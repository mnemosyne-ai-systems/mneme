package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/mnemosyne/mneme/pkg/logger"
	"github.com/mnemosyne/mneme/pkg/metrics"
	"github.com/mnemosyne/mneme/pkg/models"
	"github.com/mnemosyne/mneme/pkg/storage"
)

// ErrInserterClosed 表示写入器已关闭。
var ErrInserterClosed = errors.New("clickhouse: inserter closed")

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
)

// BatchInserter 实现 storage.BatchInserter，
// 内部按管道维护缓冲区，支持定量（batch_size）与定时（flush_interval）刷新，
// 写入失败采用指数退避重试（1s,2s,4s,8s,16s）。
type BatchInserter struct {
	conn      driver.Conn
	batchSize int
	interval  time.Duration
	retries   int

	mu        sync.Mutex
	history   []*models.HistoryMessage
	traces    []*models.TraceMessage
	memories  []*models.EntityMemory
	summaries []*models.SessionSummary

	closed  bool
	stopCh  chan struct{}
	stopped chan struct{}
}

// NewBatchInserter 创建批量写入器，并启动后台定时刷新协程。
func NewBatchInserter(client *Client) *BatchInserter {
	cfg := client.Config()
	bi := &BatchInserter{
		conn:      client.Conn(),
		batchSize: cfg.BatchSize,
		interval:  cfg.FlushInterval,
		retries:   cfg.MaxRetries,
		stopCh:    make(chan struct{}),
		stopped:   make(chan struct{}),
	}
	go bi.loop()
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
		}
	}
}

// InsertHistory 缓冲会话历史，达到阈值时立即刷新。
// 注意：阈值触发的直接写入失败时不会自动 requeue（batch 已从缓冲区取出），
// 调用方必须自行处理返回的 error 并重试（consumer 层负责放回重试），
// 否则数据会丢失。定时 Flush 路径则自带 requeue。
func (b *BatchInserter) InsertHistory(ctx context.Context, messages []*models.HistoryMessage) error {
	if len(messages) == 0 {
		return nil
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrInserterClosed
	}
	b.history = append(b.history, messages...)
	ready := len(b.history) >= b.batchSize
	var batch []*models.HistoryMessage
	if ready {
		batch, b.history = b.history, nil
	}
	b.mu.Unlock()

	if batch != nil {
		return b.writeHistory(ctx, batch)
	}
	return nil
}

// InsertTraces 缓冲轨迹数据。
// 注意：与 InsertHistory 相同，阈值触发的直接写入失败不会自动 requeue，
// 调用方必须自行处理 error 并重试。
func (b *BatchInserter) InsertTraces(ctx context.Context, traces []*models.TraceMessage) error {
	if len(traces) == 0 {
		return nil
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrInserterClosed
	}
	b.traces = append(b.traces, traces...)
	var batch []*models.TraceMessage
	if len(b.traces) >= b.batchSize {
		batch, b.traces = b.traces, nil
	}
	b.mu.Unlock()

	if batch != nil {
		return b.writeTraces(ctx, batch)
	}
	return nil
}

// InsertMemory 缓冲实体画像。
// 注意：与 InsertHistory 相同，阈值触发的直接写入失败不会自动 requeue，
// 调用方必须自行处理 error 并重试。
func (b *BatchInserter) InsertMemory(ctx context.Context, memories []*models.EntityMemory) error {
	if len(memories) == 0 {
		return nil
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrInserterClosed
	}
	b.memories = append(b.memories, memories...)
	var batch []*models.EntityMemory
	if len(b.memories) >= b.batchSize {
		batch, b.memories = b.memories, nil
	}
	b.mu.Unlock()

	if batch != nil {
		return b.writeMemory(ctx, batch)
	}
	return nil
}

// InsertSessionSummary 缓冲会话总结。
// 注意：与 InsertHistory 相同，阈值触发的直接写入失败不会自动 requeue，
// 调用方必须自行处理 error 并重试。
func (b *BatchInserter) InsertSessionSummary(ctx context.Context, summaries []*models.SessionSummary) error {
	if len(summaries) == 0 {
		return nil
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrInserterClosed
	}
	b.summaries = append(b.summaries, summaries...)
	var batch []*models.SessionSummary
	if len(b.summaries) >= b.batchSize {
		batch, b.summaries = b.summaries, nil
	}
	b.mu.Unlock()

	if batch != nil {
		return b.writeSessionSummary(ctx, batch)
	}
	return nil
}

// Flush 立即刷新所有缓冲区。写入失败的数据会被放回缓冲区，等待下次重试，
// 避免定时 flush 失败导致数据静默丢失。
func (b *BatchInserter) Flush() error {
	b.mu.Lock()
	history, traces, memories, summaries := b.history, b.traces, b.memories, b.summaries
	b.history, b.traces, b.memories, b.summaries = nil, nil, nil, nil
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

// Close 停止定时器并刷新剩余数据。
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
	history, traces, memories, summaries := b.history, b.traces, b.memories, b.summaries
	b.history, b.traces, b.memories, b.summaries = nil, nil, nil, nil
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
	return errors.Join(errs...)
}

func (b *BatchInserter) writeHistory(ctx context.Context, rows []*models.HistoryMessage) error {
	return b.withRetry(ctx, storage.PipelineHistory, len(rows), func(ctx context.Context) error {
		batch, err := b.conn.PrepareBatch(ctx, insertHistorySQL)
		if err != nil {
			return err
		}
		for _, m := range rows {
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
	})
}

func (b *BatchInserter) writeTraces(ctx context.Context, rows []*models.TraceMessage) error {
	return b.withRetry(ctx, storage.PipelineTraces, len(rows), func(ctx context.Context) error {
		batch, err := b.conn.PrepareBatch(ctx, insertTracesSQL)
		if err != nil {
			return err
		}
		for _, t := range rows {
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
	})
}

func (b *BatchInserter) writeMemory(ctx context.Context, rows []*models.EntityMemory) error {
	return b.withRetry(ctx, storage.PipelineMemory, len(rows), func(ctx context.Context) error {
		batch, err := b.conn.PrepareBatch(ctx, insertMemorySQL)
		if err != nil {
			return err
		}
		for _, m := range rows {
			if err := batch.Append(
				m.EntityID, m.TenantID, m.EntityType, m.Profile, m.Version, m.UpdatedAt,
			); err != nil {
				_ = batch.Abort()
				return err
			}
		}
		return batch.Send()
	})
}

func (b *BatchInserter) writeSessionSummary(ctx context.Context, rows []*models.SessionSummary) error {
	return b.withRetry(ctx, storage.PipelineSession, len(rows), func(ctx context.Context) error {
		batch, err := b.conn.PrepareBatch(ctx, insertSummarySQL)
		if err != nil {
			return err
		}
		for _, s := range rows {
			if err := batch.Append(
				s.SessionID, s.TenantID, s.UserID, s.Summary, s.MessageCount,
				s.StartedAt, s.EndedAt, s.Version, s.UpdatedAt,
			); err != nil {
				_ = batch.Abort()
				return err
			}
		}
		return batch.Send()
	})
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

var _ storage.BatchInserter = (*BatchInserter)(nil)
