// Package archiver 实现 ClickHouse 冷热分离与数据质量巡检。
package archiver

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/mnemosyne-labs/mneme/pkg/logger"
	"github.com/mnemosyne-labs/mneme/pkg/metrics"
)

// QualityChecker 数据质量巡检器。
type QualityChecker struct {
	chClient    driver.Conn
	redisClient interface{} // 可选，用于 Redis 一致性校验
	interval    time.Duration
	stopCh      chan struct{}
	stopped     chan struct{}
	once        sync.Once
	mu          sync.Mutex
	started     bool
}

// QualityCheckResult 单项质量检查结果。
type QualityCheckResult struct {
	Name      string
	Passed    bool
	Value     float64
	Threshold float64
	Message   string
	Severity  string // "info" | "warning" | "critical"
	Timestamp time.Time
}

// NewQualityChecker 创建质量巡检器。
func NewQualityChecker(chClient driver.Conn, redisClient interface{}, interval time.Duration) *QualityChecker {
	if interval <= 0 {
		interval = 1 * time.Hour
	}
	return &QualityChecker{
		chClient:    chClient,
		redisClient: redisClient,
		interval:    interval,
		stopCh:      make(chan struct{}),
		stopped:     make(chan struct{}),
	}
}

// Start 启动定时巡检。
func (qc *QualityChecker) Start(ctx context.Context) {
	qc.mu.Lock()
	if qc.started {
		qc.mu.Unlock()
		return
	}
	qc.started = true
	qc.mu.Unlock()
	go qc.loop(ctx)
	logger.L().WithField("interval", qc.interval).Info("quality checker started")
}

// Stop 停止巡检。未启动时直接返回，避免等待永远不会关闭的 stopped。
func (qc *QualityChecker) Stop() {
	qc.once.Do(func() {
		qc.mu.Lock()
		started := qc.started
		qc.mu.Unlock()
		close(qc.stopCh)
		if started {
			<-qc.stopped
		} else {
			close(qc.stopped)
		}
		logger.L().Info("quality checker stopped")
	})
}

func (qc *QualityChecker) loop(ctx context.Context) {
	defer close(qc.stopped)
	ticker := time.NewTicker(qc.interval)
	defer ticker.Stop()
	for {
		select {
		case <-qc.stopCh:
			return
		case <-ticker.C:
			if err := qc.RunOnce(ctx); err != nil {
				logger.L().WithError(err).Error("quality check run failed")
			}
		}
	}
}

// RunOnce 执行一次完整的质量巡检。
func (qc *QualityChecker) RunOnce(ctx context.Context) error {
	checks := []func(context.Context) ([]QualityCheckResult, error){
		qc.checkProfileCompleteness,
		qc.checkSessionSummaryQuality,
		qc.checkHistoryMessageIntegrity,
		qc.checkZSetIndexConsistency,
		qc.checkClickHousePartitionHealth,
	}

	var allResults []QualityCheckResult
	for _, check := range checks {
		results, err := check(ctx)
		if err != nil {
			logger.L().WithError(err).Error("quality check failed")
			continue
		}
		allResults = append(allResults, results...)
	}

	// 上报指标并记录日志
	for _, r := range allResults {
		metrics.QualityCheckTotal.WithLabelValues(r.Name, strconv.FormatBool(r.Passed)).Inc()
		if !r.Passed {
			logger.L().WithFields(map[string]interface{}{
				"check":     r.Name,
				"value":     r.Value,
				"threshold": r.Threshold,
				"severity":  r.Severity,
			}).Warnf("quality check failed: %s", r.Message)
		} else {
			logger.L().WithFields(map[string]interface{}{
				"check": r.Name,
				"value": r.Value,
			}).Debug("quality check passed")
		}
	}

	return nil
}

// checkProfileCompleteness 检查用户画像完整度。
func (qc *QualityChecker) checkProfileCompleteness(ctx context.Context) ([]QualityCheckResult, error) {
	if qc.chClient == nil {
		return nil, nil
	}

	const query = `
		SELECT
			count() as total,
			countIf(length(profile) > 10) as with_content,
			avg(length(profile)) as avg_size
		FROM entity_memory
	`

	rows, err := qc.chClient.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query profile stats: %w", err)
	}
	defer rows.Close()

	var total, withContent uint64
	var avgSize float64
	if rows.Next() {
		if err := rows.Scan(&total, &withContent, &avgSize); err != nil {
			return nil, fmt.Errorf("scan profile stats: %w", err)
		}
	}

	completeness := float64(0)
	if total > 0 {
		completeness = float64(withContent) / float64(total)
	}

	passed := completeness >= 0.8
	return []QualityCheckResult{
		{
			Name:      "profile_completeness",
			Passed:    passed,
			Value:     completeness,
			Threshold: 0.8,
			Message:   fmt.Sprintf("画像完整度 %.1f%% (%d/%d)", completeness*100, withContent, total),
			Severity:  severity(passed, false),
			Timestamp: time.Now(),
		},
	}, nil
}

// checkSessionSummaryQuality 检查会话总结质量。
func (qc *QualityChecker) checkSessionSummaryQuality(ctx context.Context) ([]QualityCheckResult, error) {
	if qc.chClient == nil {
		return nil, nil
	}

	const query = `
		SELECT
			count() as total,
			countIf(length(summary) > 20) as with_summary,
			avg(length(summary)) as avg_len
		FROM session_summary
	`

	rows, err := qc.chClient.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query summary stats: %w", err)
	}
	defer rows.Close()

	var total, withSummary uint64
	var avgLen float64
	if rows.Next() {
		if err := rows.Scan(&total, &withSummary, &avgLen); err != nil {
			return nil, fmt.Errorf("scan summary stats: %w", err)
		}
	}

	coverage := float64(0)
	if total > 0 {
		coverage = float64(withSummary) / float64(total)
	}

	passed := coverage >= 0.9
	return []QualityCheckResult{
		{
			Name:      "session_summary_coverage",
			Passed:    passed,
			Value:     coverage,
			Threshold: 0.9,
			Message:   fmt.Sprintf("会话总结覆盖率 %.1f%% (%d/%d)", coverage*100, withSummary, total),
			Severity:  severity(passed, false),
			Timestamp: time.Now(),
		},
	}, nil
}

// checkHistoryMessageIntegrity 检查历史消息完整性。
func (qc *QualityChecker) checkHistoryMessageIntegrity(ctx context.Context) ([]QualityCheckResult, error) {
	if qc.chClient == nil {
		return nil, nil
	}

	const query = `
		SELECT
			count() as total,
			countIf(role IN ('user','assistant','system','tool')) as valid_role,
			countIf(length(content) > 0) as has_content
		FROM history_message
	`

	rows, err := qc.chClient.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query history stats: %w", err)
	}
	defer rows.Close()

	var total, validRole, hasContent uint64
	if rows.Next() {
		if err := rows.Scan(&total, &validRole, &hasContent); err != nil {
			return nil, fmt.Errorf("scan history stats: %w", err)
		}
	}

	passed := total == 0 || (validRole == total && hasContent == total)
	return []QualityCheckResult{
		{
			Name:      "history_message_integrity",
			Passed:    passed,
			Value:     float64(validRole) / float64(max(total, 1)),
			Threshold: 1.0,
			Message: fmt.Sprintf("消息完整性: role有效率 %.1f%%, 内容非空率 %.1f%%",
				float64(validRole)/float64(max(total, 1))*100,
				float64(hasContent)/float64(max(total, 1))*100),
			Severity:  severity(passed, false),
			Timestamp: time.Now(),
		},
	}, nil
}

// checkZSetIndexConsistency 检查 ZSET 索引与 Hash 数据一致性。
func (qc *QualityChecker) checkZSetIndexConsistency(ctx context.Context) ([]QualityCheckResult, error) {
	if qc.redisClient == nil {
		return []QualityCheckResult{
			{Name: "zset_index_consistency", Passed: true, Message: "Redis 客户端未配置，跳过", Severity: "info", Timestamp: time.Now()},
		}, nil
	}

	// 由于 Redis 客户端接口不统一，这里仅作占位
	return []QualityCheckResult{
		{Name: "zset_index_consistency", Passed: true, Message: "Redis 一致性检查待实现", Severity: "info", Timestamp: time.Now()},
	}, nil
}

// checkClickHousePartitionHealth 检查 ClickHouse 分区健康度。
func (qc *QualityChecker) checkClickHousePartitionHealth(ctx context.Context) ([]QualityCheckResult, error) {
	if qc.chClient == nil {
		return nil, nil
	}

	const query = `
		SELECT
			table,
			sum(bytes_on_disk) as bytes,
			count() as parts,
			max(modification_time) as last_modified
		FROM system.parts
		WHERE active = 1 AND database = currentDatabase()
		GROUP BY table
	`

	rows, err := qc.chClient.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query parts stats: %w", err)
	}
	defer rows.Close()

	var results []QualityCheckResult
	for rows.Next() {
		var table string
		var bytes, parts uint64
		var lastMod time.Time
		if err := rows.Scan(&table, &bytes, &parts, &lastMod); err != nil {
			return nil, fmt.Errorf("clickhouse: scan parts: %w", err)
		}

		// 检查分区是否过旧（超过 48 小时未更新）
		stale := time.Since(lastMod) > 48*time.Hour
		passed := !stale
		results = append(results, QualityCheckResult{
			Name:      "ch_partition_health_" + table,
			Passed:    passed,
			Value:     time.Since(lastMod).Hours(),
			Threshold: 48,
			Message:   fmt.Sprintf("表 %s: %d 个分区, %.2f GB, 最后更新 %.1f 小时前", table, parts, float64(bytes)/1e9, time.Since(lastMod).Hours()),
			Severity:  severity(passed, true),
			Timestamp: time.Now(),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate parts: %w", err)
	}
	return results, nil
}

func severity(passed, critical bool) string {
	if passed {
		return "info"
	}
	if critical {
		return "critical"
	}
	return "warning"
}
