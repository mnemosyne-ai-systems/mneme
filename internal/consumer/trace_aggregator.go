package consumer

import (
	"sync"
	"time"

	"github.com/mnemosyne-labs/mneme/pkg/models"
)

// TraceAggregator 按 request_id 聚合 Span，超时后统一 flush。
// 采用自带过期扫描的内存缓存，避免依赖外部定时器精度。
type TraceAggregator struct {
	mu      sync.Mutex
	buffer  map[string][]*models.TraceMessage
	touched map[string]time.Time
	timeout time.Duration
	// onFlush 在聚合完成时被调用。
	onFlush func(requestID string, spans []*models.TraceMessage, reason string)
	now     func() time.Time
}

// NewTraceAggregator 创建聚合器。timeout 为聚合窗口（默认 30s）。
func NewTraceAggregator(timeout time.Duration, onFlush func(string, []*models.TraceMessage, string)) *TraceAggregator {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &TraceAggregator{
		buffer:  make(map[string][]*models.TraceMessage),
		touched: make(map[string]time.Time),
		timeout: timeout,
		onFlush: onFlush,
		now:     time.Now,
	}
}

// Add 加入一个 Span。
func (a *TraceAggregator) Add(trace *models.TraceMessage) {
	if trace == nil {
		return
	}
	key := trace.PartitionKey()
	a.mu.Lock()
	a.buffer[key] = append(a.buffer[key], trace)
	a.touched[key] = a.now()
	a.mu.Unlock()
}

// SweepExpired 刷新所有超过聚合窗口未更新的 request。
func (a *TraceAggregator) SweepExpired() int {
	deadline := a.now().Add(-a.timeout)

	a.mu.Lock()
	expired := make(map[string][]*models.TraceMessage)
	for key, at := range a.touched {
		if at.Before(deadline) {
			expired[key] = a.buffer[key]
			delete(a.buffer, key)
			delete(a.touched, key)
		}
	}
	a.mu.Unlock()

	for key, spans := range expired {
		if a.onFlush != nil && len(spans) > 0 {
			a.onFlush(key, spans, "timeout")
		}
	}
	return len(expired)
}

// FlushAll 立即刷新所有聚合中的 Span（用于优雅关闭）。
func (a *TraceAggregator) FlushAll(reason string) int {
	a.mu.Lock()
	pending := a.buffer
	a.buffer = make(map[string][]*models.TraceMessage)
	a.touched = make(map[string]time.Time)
	a.mu.Unlock()

	n := 0
	for key, spans := range pending {
		if a.onFlush != nil && len(spans) > 0 {
			a.onFlush(key, spans, reason)
			n++
		}
	}
	return n
}

// FlushRequest 立即刷新指定 request（例如收到结束标记时）。
func (a *TraceAggregator) FlushRequest(key, reason string) bool {
	a.mu.Lock()
	spans := a.buffer[key]
	delete(a.buffer, key)
	delete(a.touched, key)
	a.mu.Unlock()

	if len(spans) == 0 {
		return false
	}
	if a.onFlush != nil {
		a.onFlush(key, spans, reason)
	}
	return true
}

// PendingRequests 返回聚合中的 request 数量。
func (a *TraceAggregator) PendingRequests() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.buffer)
}

// PendingSpans 返回聚合中的 Span 总数。
func (a *TraceAggregator) PendingSpans() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, spans := range a.buffer {
		n += len(spans)
	}
	return n
}
