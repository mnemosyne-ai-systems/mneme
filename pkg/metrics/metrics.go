// Package metrics 定义 Mneme 的 Prometheus 指标。
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// 指标集合。所有指标统一以 mnemosyne_mneme_ 为前缀。
var (
	// HTTPRequestsTotal HTTP 请求总数。
	HTTPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mnemosyne_mneme_http_requests_total",
		Help: "Total number of HTTP requests handled by Mneme.",
	}, []string{"endpoint", "method", "status"})

	// HTTPRequestDuration HTTP 请求耗时。
	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mnemosyne_mneme_http_request_duration_seconds",
		Help:    "HTTP request latency in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"endpoint", "method"})

	// PulsarMessagesTotal Pulsar 消息计数（produce / consume）。
	PulsarMessagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mnemosyne_mneme_pulsar_messages_total",
		Help: "Total number of Pulsar messages produced or consumed.",
	}, []string{"topic", "direction"})

	// ClickHouseWriteDuration ClickHouse 批量写入耗时。
	ClickHouseWriteDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mnemosyne_mneme_clickhouse_write_duration_seconds",
		Help:    "ClickHouse batch write duration in seconds.",
		Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	}, []string{"pipeline"})

	// ClickHouseRowsWritten ClickHouse 写入行数。
	ClickHouseRowsWritten = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mnemosyne_mneme_clickhouse_rows_written_total",
		Help: "Total rows written to ClickHouse.",
	}, []string{"pipeline"})

	// ClickHouseWriteErrors ClickHouse 写入失败次数。
	ClickHouseWriteErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mnemosyne_mneme_clickhouse_write_errors_total",
		Help: "Total ClickHouse write errors.",
	}, []string{"pipeline"})

	// ConsumerLag 消费滞后（消息发布到消费的时间差，秒）。
	ConsumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mnemosyne_mneme_consumer_lag",
		Help: "Consumer lag in seconds per topic.",
	}, []string{"topic"})

	// ConsumerErrorsTotal 消费失败次数。
	ConsumerErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mnemosyne_mneme_consumer_errors_total",
		Help: "Total consumer processing errors.",
	}, []string{"topic", "reason"})

	// DedupFilteredTotal 被布隆过滤器拦截的重复消息数。
	DedupFilteredTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mnemosyne_mneme_dedup_filtered_total",
		Help: "Total messages filtered as duplicates.",
	}, []string{"pipeline"})

	// TraceAggregationFlushTotal 轨迹聚合刷新次数。
	TraceAggregationFlushTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mnemosyne_mneme_trace_aggregation_flush_total",
		Help: "Total trace aggregation flushes.",
	}, []string{"reason"})

	// MemoryExtractTotal 长期记忆萃取次数（含降级）。
	MemoryExtractTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mnemosyne_mneme_memory_extract_total",
		Help: "Total memory profile extractions.",
	}, []string{"mode"})

	// ArchiveRunsTotal 归档任务执行次数。
	ArchiveRunsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mnemosyne_mneme_archive_runs_total",
		Help: "Total archive job runs.",
	}, []string{"result"})

	// FileWatcherFilesTotal 文件监听处理文件数。
	FileWatcherFilesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mnemosyne_mneme_filewatcher_files_total",
		Help: "Total files processed by the file watcher.",
	}, []string{"ext", "result"})

	// AdapterPublishTotal 文件采集器批量投递结果计数（按消息数累计）。
	AdapterPublishTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mnemosyne_mneme_adapter_publish_total",
		Help: "Total history messages delivered by the adapter sink.",
	}, []string{"result"})
)

// ObserveHTTP 记录一次 HTTP 请求。
func ObserveHTTP(endpoint, method, status string, seconds float64) {
	HTTPRequestsTotal.WithLabelValues(endpoint, method, status).Inc()
	HTTPRequestDuration.WithLabelValues(endpoint, method).Observe(seconds)
}
