package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/mnemosyne-ai-systems/mneme/pkg/logger"
	"github.com/mnemosyne-ai-systems/mneme/pkg/metrics"
	"github.com/mnemosyne-ai-systems/mneme/pkg/models"
	"github.com/mnemosyne-ai-systems/mneme/pkg/storage"
)

// ErrInserterClosed 表示写入器已关闭。
var ErrInserterClosed = errors.New("clickhouse: inserter closed")

// ErrBackpressure 表示背压拒绝。
var ErrBackpressure = errors.New("clickhouse: backpressure, queue full")

const (
	insertHistorySQL = `INSERT INTO history_message (
        message_id, tenant_id, session_id, user_id, role, content, model_name,
        total_tokens, prompt_tokens, completion_tokens, response_time_ms, metadata, created_at)`

	insertTracesSQL = `INSERT INTO agent_traces (
        request_id, tenant_id, session_id, user_id, step_type, step_name,
        input, output, metadata, duration_ms, timestamp)`

	insertMemorySQL = `INSERT INTO entity_memory (
        entity_id, tenant_id, entity_type, profile, _version, updated_at)`

	insertSummarySQL = `INSERT INTO session_summary (
        session_id, tenant_id, user_id, summary, message_count, started_at, ended_at, _version, updated_at)`

	insertAuditLogSQL = `INSERT INTO audit_log (
        id, tenant_id, actor, action, target, detail, at)`
)

// RejectStrategy 背压拒绝策略。
type RejectStrategy int

const (
	RejectStrategyBlock  RejectStrategy = iota // 阻塞等待
	RejectStrategyDrop                         // 直接丢弃
	RejectStrategyReject                       // 返回错误
)

// BatchInserter 实现 storage.BatchInserter，
// 内部按管道维护缓冲区，支持定量（batch_size）与定时（flush_interval）刷新，
// 写入失败采用指数退避重试（1s,2s,4s,8s,16s）。
// 支持自适应批次大小、背压控制、最大字节数限制。
type BatchInserter struct {
	conn          driver.Conn
	batchSize     int
	maxBatchBytes int // 批次最大字节数，防 OOM
	interval      time.Duration
	retries       int

	// 自适应批次控制
	adaptiveBatchEnabled bool
	minBatchSize         int
	maxBatchSize         int
	targetFlushLatency   time.Duration // 目标刷新延迟
	throughputEWMA       float64       // 指数加权移动平均吞吐 (rows/sec)
	throughputAlpha      float64       // EWMA 平滑因子

	// 背压控制
	rejectStrategy RejectStrategy
	semaphore      chan struct{} // 并发写入信号量
	maxQueueSize   int           // 最大队列长度（行数）

	mu        sync.Mutex
	history   []*models.HistoryMessage
	traces    []*models.TraceMessage
	memories  []*models.EntityMemory
	summaries []*models.SessionSummary
	auditLogs []*models.AuditEntry

	closed  bool
	stopCh  chan struct{}
	stopped chan struct{}
	flushCh chan struct{} // 手动触发刷新信号

	// 统计
	totalRowsWritten  int64
	totalFlushCount   int64
	totalFlushLatency time.Duration
	lastAdaptTime     time.Time
}

// NewBatchInserter 创建批量写入器，并启动后台定时刷新协程与自适应批次调整协程。
func NewBatchInserter(client *Client) *BatchInserter {
	cfg := client.Config()
	bi := &BatchInserter{
		conn:                 client.Conn(),
		batchSize:            cfg.BatchSize,
		maxBatchBytes:        cfg.MaxBatchBytes,
		interval:             cfg.FlushInterval,
		retries:              cfg.MaxRetries,
		adaptiveBatchEnabled: cfg.AdaptiveBatchEnabled,
		minBatchSize:         cfg.MinBatchSize,
		maxBatchSize:         cfg.MaxBatchSize,
		targetFlushLatency:   cfg.TargetFlushLatency,
		throughputAlpha:      0.3,
		rejectStrategy:       RejectStrategyBlock,
		semaphore:            make(chan struct{}, 10000), // 默认允许 10k 并发写入
		maxQueueSize:         1000000,                    // 最大队列 100 万行
		stopCh:               make(chan struct{}),
		stopped:              make(chan struct{}),
		flushCh:              make(chan struct{}, 1),
		lastAdaptTime:        time.Now(),
	}
	go bi.loop()
	go bi.adaptLoop()
	return bi
}

func (b *BatchInserter) loop() {
	defer close(b.stopped)
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()
	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			if err := b.Flush(); err != nil {
				logger.L().WithError(err).Error("clickhouse periodic flush failed")
			}
		case <-b.flushCh:
			if err := b.Flush(); err != nil {
				logger.L().WithError(err).Error("clickhouse manual flush failed")
			}
		}
	}
}

// InsertHistory 缓冲会话历史，达到阈值时立即刷新。
// 支持背压控制：达到 maxQueueSize 时根据策略阻塞/丢弃/拒绝。
func (b *BatchInserter) InsertHistory(ctx context.Context, messages []*models.HistoryMessage) error {
	if len(messages) == 0 {
		return nil
	}
	// 背压控制
	if !b.acquireSemaphore(ctx) {
		return ErrBackpressure
	}
	defer b.releaseSemaphore()

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrInserterClosed
	}
	if b.queueFullLocked() {
		b.mu.Unlock()
		return ErrBackpressure
	}
	b.history = append(b.history, messages...)
	ready := len(b.history) >= b.batchSize
	var batch []*models.HistoryMessage
	if ready {
		batch, b.history = b.history, nil
		// 触发手动刷新
		select {
		case b.flushCh <- struct{}{}:
		default:
		}
	}
	b.mu.Unlock()

	if batch != nil {
		return b.writeHistory(ctx, batch)
	}
	return nil
}

// acquireSemaphore 获取信号量，支持阻塞/丢弃/拒绝策略
func (b *BatchInserter) acquireSemaphore(ctx context.Context) bool {
	switch b.rejectStrategy {
	case RejectStrategyBlock:
		select {
		case b.semaphore <- struct{}{}:
			return true
		case <-ctx.Done():
			return false
		}
	case RejectStrategyDrop:
		select {
		case b.semaphore <- struct{}{}:
			return true
		default:
			return false
		}
	case RejectStrategyReject:
		select {
		case b.semaphore <- struct{}{}:
			return true
		default:
			return false
		}
	}
	return false
}

func (b *BatchInserter) releaseSemaphore() {
	select {
	case <-b.semaphore:
	default:
	}
}

// queueFullLocked 判断所有管道缓冲行数是否达到上限（调用方需持有 b.mu）。
func (b *BatchInserter) queueFullLocked() bool {
	if b.maxQueueSize <= 0 {
		return false
	}
	total := len(b.history) + len(b.traces) + len(b.memories) + len(b.summaries) + len(b.auditLogs)
	return total >= b.maxQueueSize
}

// InsertTraces 缓冲轨迹数据。
func (b *BatchInserter) InsertTraces(ctx context.Context, traces []*models.TraceMessage) error {
	if len(traces) == 0 {
		return nil
	}
	if !b.acquireSemaphore(ctx) {
		return ErrBackpressure
	}
	defer b.releaseSemaphore()

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrInserterClosed
	}
	if b.queueFullLocked() {
		b.mu.Unlock()
		return ErrBackpressure
	}
	b.traces = append(b.traces, traces...)
	var batch []*models.TraceMessage
	if len(b.traces) >= b.batchSize {
		batch, b.traces = b.traces, nil
		if batch != nil {
			select {
			case b.flushCh <- struct{}{}:
			default:
			}
		}
	}
	b.mu.Unlock()

	if batch != nil {
		return b.writeTraces(ctx, batch)
	}
	return nil
}

// InsertMemory 缓冲实体画像。
func (b *BatchInserter) InsertMemory(ctx context.Context, memories []*models.EntityMemory) error {
	if len(memories) == 0 {
		return nil
	}
	if !b.acquireSemaphore(ctx) {
		return ErrBackpressure
	}
	defer b.releaseSemaphore()

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrInserterClosed
	}
	if b.queueFullLocked() {
		b.mu.Unlock()
		return ErrBackpressure
	}
	b.memories = append(b.memories, memories...)
	var batch []*models.EntityMemory
	if len(b.memories) >= b.batchSize {
		batch, b.memories = b.memories, nil
		if batch != nil {
			select {
			case b.flushCh <- struct{}{}:
			default:
			}
		}
	}
	b.mu.Unlock()

	if batch != nil {
		return b.writeMemory(ctx, batch)
	}
	return nil
}

// InsertSessionSummary 缓冲会话总结。
func (b *BatchInserter) InsertSessionSummary(ctx context.Context, summaries []*models.SessionSummary) error {
	if len(summaries) == 0 {
		return nil
	}
	if !b.acquireSemaphore(ctx) {
		return ErrBackpressure
	}
	defer b.releaseSemaphore()

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrInserterClosed
	}
	if b.queueFullLocked() {
		b.mu.Unlock()
		return ErrBackpressure
	}
	b.summaries = append(b.summaries, summaries...)
	var batch []*models.SessionSummary
	if len(b.summaries) >= b.batchSize {
		batch, b.summaries = b.summaries, nil
		if batch != nil {
			select {
			case b.flushCh <- struct{}{}:
			default:
			}
		}
	}
	b.mu.Unlock()

	if batch != nil {
		return b.writeSessionSummary(ctx, batch)
	}
	return nil
}

// Flush 立即刷新所有缓冲区。写入失败的数据会被放回缓冲区，等待下次重试，
func (b *BatchInserter) Flush() error {
	b.mu.Lock()
	history, traces, memories, summaries, auditLogs := b.history, b.traces, b.memories, b.summaries, b.auditLogs
	b.history, b.traces, b.memories, b.summaries, b.auditLogs = nil, nil, nil, nil, nil
	b.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var errs []error
	if len(history) > 0 {
		if err := b.writeHistory(ctx, history); err != nil {
			errs = append(errs, err)
			b.requeueHistory(history)
		}
	}
	if len(traces) > 0 {
		if err := b.writeTraces(ctx, traces); err != nil {
			errs = append(errs, err)
			b.requeueTraces(traces)
		}
	}
	if len(memories) > 0 {
		if err := b.writeMemory(ctx, memories); err != nil {
			errs = append(errs, err)
			b.requeueMemories(memories)
		}
	}
	if len(summaries) > 0 {
		if err := b.writeSessionSummary(ctx, summaries); err != nil {
			errs = append(errs, err)
			b.requeueSummaries(summaries)
		}
	}
	if len(auditLogs) > 0 {
		if err := b.writeAuditLog(ctx, auditLogs); err != nil {
			errs = append(errs, err)
			b.requeueAuditLogs(auditLogs)
		}
	}

	return errors.Join(errs...)
}

// requeueHistory 把写失败的数据放回缓冲区（已关闭则丢弃并告警）。
func (b *BatchInserter) requeueHistory(rows []*models.HistoryMessage) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		logger.L().WithField("rows", len(rows)).Error("clickhouse: drop history rows after close")
		return
	}
	b.history = append(rows, b.history...)
}

// requeueTraces 把写失败的数据放回缓冲区（已关闭则丢弃并告警）。
func (b *BatchInserter) requeueTraces(rows []*models.TraceMessage) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		logger.L().WithField("rows", len(rows)).Error("clickhouse: drop trace rows after close")
		return
	}
	b.traces = append(rows, b.traces...)
}

// requeueMemories 把写失败的数据放回缓冲区（已关闭则丢弃并告警）。
func (b *BatchInserter) requeueMemories(rows []*models.EntityMemory) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		logger.L().WithField("rows", len(rows)).Error("clickhouse: drop memory rows after close")
		return
	}
	b.memories = append(rows, b.memories...)
}

// requeueSummaries 把写失败的数据放回缓冲区（已关闭则丢弃并告警）。
func (b *BatchInserter) requeueSummaries(rows []*models.SessionSummary) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		logger.L().WithField("rows", len(rows)).Error("clickhouse: drop summary rows after close")
		return
	}
	b.summaries = append(rows, b.summaries...)
}

// requeueAuditLogs 把写失败的审计日志放回缓冲区（已关闭则丢弃并告警）。
func (b *BatchInserter) requeueAuditLogs(rows []*models.AuditEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		logger.L().WithField("rows", len(rows)).Error("clickhouse: drop audit rows after close")
		return
	}
	b.auditLogs = append(rows, b.auditLogs...)
}

// InsertAuditLog 缓冲审计日志，达到阈值时立即刷新。
func (b *BatchInserter) InsertAuditLog(ctx context.Context, entry *models.AuditEntry) error {
	if entry == nil {
		return nil
	}
	if !b.acquireSemaphore(ctx) {
		return ErrBackpressure
	}
	defer b.releaseSemaphore()

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrInserterClosed
	}
	if b.queueFullLocked() {
		b.mu.Unlock()
		return ErrBackpressure
	}
	b.auditLogs = append(b.auditLogs, entry)
	var batch []*models.AuditEntry
	if len(b.auditLogs) >= b.batchSize {
		batch, b.auditLogs = b.auditLogs, nil
		if batch != nil {
			select {
			case b.flushCh <- struct{}{}:
			default:
			}
		}
	}
	b.mu.Unlock()

	if batch != nil {
		return b.writeAuditLog(ctx, batch)
	}
	return nil
}

// writeAuditLog 写入审计日志到 ClickHouse。
func (b *BatchInserter) writeAuditLog(ctx context.Context, entries []*models.AuditEntry) error {
	chunks := splitRows(entries, b.maxBatchBytes, func(e *models.AuditEntry) int {
		return len(e.TenantID) + len(e.Actor) + len(e.Action) + len(e.Target) + len(e.Detail) + 64
	})
	for _, chunk := range chunks {
		if err := b.withRetry(ctx, "audit_log", len(chunk), func(ctx context.Context) error {
			batch, err := b.conn.PrepareBatch(ctx, insertAuditLogSQL)
			if err != nil {
				return fmt.Errorf("clickhouse: prepare audit batch: %w", err)
			}
			for _, e := range chunk {
				id := uint64(time.Now().UnixNano())
				if err := batch.Append(
					id, e.TenantID, e.Actor, e.Action, e.Target, e.Detail, e.At,
				); err != nil {
					return fmt.Errorf("clickhouse: append audit: %w", err)
				}
			}
			return batch.Send()
		}); err != nil {
			return err
		}
	}
	return nil
}

// QueryAuditLog 查询审计日志（按时间倒序）。
func (b *BatchInserter) QueryAuditLog(ctx context.Context, tenant string, limit, offset int) ([]*models.AuditEntry, error) {
	if limit <= 0 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	query := `
		SELECT id, tenant_id, actor, action, target, detail, at
		FROM audit_log
		WHERE tenant_id = ?
		ORDER BY at DESC
		LIMIT ? OFFSET ?
	`
	rows, err := b.conn.Query(ctx, query, tenant, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: query audit log: %w", err)
	}
	defer rows.Close()

	var out []*models.AuditEntry
	for rows.Next() {
		var e models.AuditEntry
		if err := rows.Scan(&e.ID, &e.TenantID, &e.Actor, &e.Action, &e.Target, &e.Detail, &e.At); err != nil {
			return nil, fmt.Errorf("clickhouse: scan audit log: %w", err)
		}
		out = append(out, &e)
	}
	return out, rows.Err()
}

// adaptLoop 后台自适应批次大小调整循环。
// 根据实时吞吐量动态调整 batchSize，目标是将刷新延迟控制在 targetFlushLatency 附近。
func (b *BatchInserter) adaptLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			if !b.adaptiveBatchEnabled {
				continue
			}
			now := time.Now()
			elapsed := now.Sub(b.lastAdaptTime).Seconds()
			if elapsed <= 0 {
				continue
			}

			// 计算当前吞吐量 (rows/sec)
			currentThroughput := float64(b.totalRowsWritten) / elapsed
			if currentThroughput > 0 {
				// EWMA 平滑
				if b.throughputEWMA == 0 {
					b.throughputEWMA = currentThroughput
				} else {
					b.throughputEWMA = b.throughputAlpha*currentThroughput + (1-b.throughputAlpha)*b.throughputEWMA
				}

				// 根据吞吐量计算最优 batchSize
				// 目标：batchSize / throughput ≈ targetFlushLatency
				optimalBatch := int(b.throughputEWMA * b.targetFlushLatency.Seconds())
				optimalBatch = clamp(optimalBatch, b.minBatchSize, b.maxBatchSize)

				b.mu.Lock()
				if optimalBatch != b.batchSize {
					logger.L().Debugf("adaptive batch size: %d -> %d (throughput=%.1f rows/s)",
						b.batchSize, optimalBatch, b.throughputEWMA)
					b.batchSize = optimalBatch
				}
				b.mu.Unlock()
			}

			// 重置统计
			b.mu.Lock()
			b.totalRowsWritten = 0
			b.lastAdaptTime = now
			b.mu.Unlock()
		}
	}
}

func clamp(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}
func (b *BatchInserter) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.mu.Unlock()

	close(b.stopCh)
	<-b.stopped

	b.mu.Lock()
	history, traces, memories, summaries, auditLogs := b.history, b.traces, b.memories, b.summaries, b.auditLogs
	b.history, b.traces, b.memories, b.summaries, b.auditLogs = nil, nil, nil, nil, nil
	b.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var errs []error
	if len(history) > 0 {
		errs = append(errs, b.writeHistory(ctx, history))
	}
	if len(traces) > 0 {
		errs = append(errs, b.writeTraces(ctx, traces))
	}
	if len(memories) > 0 {
		errs = append(errs, b.writeMemory(ctx, memories))
	}
	if len(summaries) > 0 {
		errs = append(errs, b.writeSessionSummary(ctx, summaries))
	}
	if len(auditLogs) > 0 {
		errs = append(errs, b.writeAuditLog(ctx, auditLogs))
	}
	return errors.Join(errs...)
}

func (b *BatchInserter) writeHistory(ctx context.Context, rows []*models.HistoryMessage) error {
	chunks := splitRows(rows, b.maxBatchBytes, func(m *models.HistoryMessage) int {
		return len(m.MessageID) + len(m.TenantID) + len(m.SessionID) + len(m.UserID) +
			len(m.Role) + len(m.Content) + len(m.ModelName) + len(m.Metadata) + 128
	})
	for _, chunk := range chunks {
		if err := b.withRetry(ctx, storage.PipelineHistory, len(chunk), func(ctx context.Context) error {
			batch, err := b.conn.PrepareBatch(ctx, insertHistorySQL)
			if err != nil {
				return err
			}
			for _, m := range chunk {
				if err := batch.Append(
					m.MessageID, m.TenantID, m.SessionID, m.UserID, m.Role, m.Content, m.ModelName,
					m.TotalTokens, m.PromptTokens, m.CompletionTokens, m.ResponseTimeMs, m.Metadata,
					m.CreatedAt,
				); err != nil {
					_ = batch.Abort()
					return err
				}
			}
			return batch.Send()
		}); err != nil {
			return err
		}
	}
	return nil
}

func (b *BatchInserter) writeTraces(ctx context.Context, rows []*models.TraceMessage) error {
	chunks := splitRows(rows, b.maxBatchBytes, func(t *models.TraceMessage) int {
		return len(t.RequestID) + len(t.TenantID) + len(t.SessionID) + len(t.UserID) +
			len(t.StepType) + len(t.StepName) + len(t.Input) + len(t.Output) +
			metadataSize(t.Metadata) + 128
	})
	for _, chunk := range chunks {
		if err := b.withRetry(ctx, storage.PipelineTraces, len(chunk), func(ctx context.Context) error {
			batch, err := b.conn.PrepareBatch(ctx, insertTracesSQL)
			if err != nil {
				return err
			}
			for _, t := range chunk {
				meta := t.Metadata
				if meta == nil {
					meta = map[string]string{}
				}
				if err := batch.Append(
					t.RequestID, t.TenantID, t.SessionID, t.UserID, t.StepType, t.StepName,
					t.Input, t.Output, meta, t.DurationMs, t.Timestamp,
				); err != nil {
					_ = batch.Abort()
					return err
				}
			}
			return batch.Send()
		}); err != nil {
			return err
		}
	}
	return nil
}

func (b *BatchInserter) writeMemory(ctx context.Context, rows []*models.EntityMemory) error {
	chunks := splitRows(rows, b.maxBatchBytes, func(m *models.EntityMemory) int {
		return len(m.EntityID) + len(m.TenantID) + len(m.EntityType) + len(m.Profile) + 128
	})
	for _, chunk := range chunks {
		if err := b.withRetry(ctx, storage.PipelineMemory, len(chunk), func(ctx context.Context) error {
			batch, err := b.conn.PrepareBatch(ctx, insertMemorySQL)
			if err != nil {
				return err
			}
			for _, m := range chunk {
				if err := batch.Append(
					m.EntityID, m.TenantID, m.EntityType, m.Profile, m.Version, m.UpdatedAt,
				); err != nil {
					_ = batch.Abort()
					return err
				}
			}
			return batch.Send()
		}); err != nil {
			return err
		}
	}
	return nil
}

func (b *BatchInserter) writeSessionSummary(ctx context.Context, rows []*models.SessionSummary) error {
	chunks := splitRows(rows, b.maxBatchBytes, func(s *models.SessionSummary) int {
		return len(s.SessionID) + len(s.TenantID) + len(s.UserID) + len(s.Summary) + 128
	})
	for _, chunk := range chunks {
		if err := b.withRetry(ctx, storage.PipelineSession, len(chunk), func(ctx context.Context) error {
			batch, err := b.conn.PrepareBatch(ctx, insertSummarySQL)
			if err != nil {
				return err
			}
			for _, s := range chunk {
				if err := batch.Append(
					s.SessionID, s.TenantID, s.UserID, s.Summary, s.MessageCount,
					s.StartedAt, s.EndedAt, s.Version, s.UpdatedAt,
				); err != nil {
					_ = batch.Abort()
					return err
				}
			}
			return batch.Send()
		}); err != nil {
			return err
		}
	}
	return nil
}

// splitRows 按估算字节数切分批次，避免单个 INSERT 批次过大导致 OOM。
func splitRows[T any](rows []T, maxBytes int, size func(T) int) [][]T {
	if len(rows) == 0 {
		return nil
	}
	if maxBytes <= 0 {
		return [][]T{rows}
	}
	var chunks [][]T
	start, total := 0, 0
	for i, row := range rows {
		sz := size(row)
		if i > start && total+sz > maxBytes {
			chunks = append(chunks, rows[start:i])
			start = i
			total = 0
		}
		total += sz
	}
	return append(chunks, rows[start:])
}

func metadataSize(meta map[string]string) int {
	total := 0
	for k, v := range meta {
		total += len(k) + len(v)
	}
	return total
}

// withRetry 以指数退避（1s,2s,4s,8s,16s）重试写入。
func (b *BatchInserter) withRetry(ctx context.Context, pipeline string, rows int, fn func(context.Context) error) error {
	start := time.Now()
	var lastErr error
	for attempt := 0; attempt <= b.retries; attempt++ {
		if attempt > 0 {
			delay := backoffDelay(attempt)
			logger.L().WithFields(map[string]interface{}{
				"pipeline": pipeline,
				"attempt":  attempt,
				"delay":    delay.String(),
			}).Warn("retrying clickhouse write")
			select {
			case <-ctx.Done():
				metrics.ClickHouseWriteErrors.WithLabelValues(pipeline).Inc()
				return fmt.Errorf("clickhouse: write %s aborted: %w", pipeline, ctx.Err())
			case <-time.After(delay):
			}
		}
		if err := fn(ctx); err != nil {
			lastErr = err
			continue
		}
		metrics.ClickHouseWriteDuration.WithLabelValues(pipeline).Observe(time.Since(start).Seconds())
		metrics.ClickHouseRowsWritten.WithLabelValues(pipeline).Add(float64(rows))
		return nil
	}
	metrics.ClickHouseWriteErrors.WithLabelValues(pipeline).Inc()
	return fmt.Errorf("clickhouse: write %s failed after %d retries: %w", pipeline, b.retries, lastErr)
}

// backoffDelay 返回第 attempt 次重试的等待时间：1s,2s,4s,8s,16s...（上限 30s）。
func backoffDelay(attempt int) time.Duration {
	d := time.Second << (attempt - 1)
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

// QueryHistoryBySession 查询某会话的完整对话历史（按时间升序）。
func (b *BatchInserter) QueryHistoryBySession(ctx context.Context, tenant, sessionID string, limit, offset int) ([]*models.HistoryMessage, error) {
	if limit <= 0 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	query := `
		SELECT message_id, tenant_id, session_id, user_id, role, content, model_name,
		       total_tokens, prompt_tokens, completion_tokens, response_time_ms, metadata, created_at
		FROM history_message
		WHERE tenant_id = ? AND session_id = ?
		ORDER BY created_at ASC
		LIMIT ? OFFSET ?
	`
	rows, err := b.conn.Query(ctx, query, tenant, sessionID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: query history: %w", err)
	}
	defer rows.Close()

	var msgs []*models.HistoryMessage
	for rows.Next() {
		var m models.HistoryMessage
		if err := rows.Scan(
			&m.MessageID, &m.TenantID, &m.SessionID, &m.UserID,
			&m.Role, &m.Content, &m.ModelName,
			&m.TotalTokens, &m.PromptTokens, &m.CompletionTokens,
			&m.ResponseTimeMs, &m.Metadata, &m.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("clickhouse: scan history: %w", err)
		}
		msgs = append(msgs, &m)
	}
	return msgs, rows.Err()
}

var _ storage.BatchInserter = (*BatchInserter)(nil)
