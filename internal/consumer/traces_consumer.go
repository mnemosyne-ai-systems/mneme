package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mnemosyne/mneme/pkg/logger"
	"github.com/mnemosyne/mneme/pkg/messagebus"
	"github.com/mnemosyne/mneme/pkg/metrics"
	"github.com/mnemosyne/mneme/pkg/models"
	"github.com/mnemosyne/mneme/pkg/storage"
)

// TracesConsumerOptions 管道二消费者配置。
type TracesConsumerOptions struct {
	Topic string
	// AggregateTimeout 聚合窗口，默认 30s。
	AggregateTimeout time.Duration
	// SweepInterval 过期扫描周期，默认为 AggregateTimeout/3。
	SweepInterval time.Duration
}

func (o TracesConsumerOptions) withDefaults() TracesConsumerOptions {
	if o.Topic == "" {
		o.Topic = "mneme-traces"
	}
	if o.AggregateTimeout <= 0 {
		o.AggregateTimeout = 30 * time.Second
	}
	if o.SweepInterval <= 0 {
		o.SweepInterval = o.AggregateTimeout / 3
		if o.SweepInterval <= 0 {
			o.SweepInterval = time.Second
		}
	}
	return o
}

// TracesConsumer 消费 mneme-traces，按 request_id 聚合后写入 ClickHouse。
type TracesConsumer struct {
	consumer   messagebus.Consumer
	inserter   storage.BatchInserter
	aggregator *TraceAggregator
	opts       TracesConsumerOptions

	stopCh  chan struct{}
	stopped chan struct{}
	once    sync.Once

	startedMu sync.Mutex
	started   bool
}

// NewTracesConsumer 创建管道二消费者。
func NewTracesConsumer(c messagebus.Consumer, inserter storage.BatchInserter, opts TracesConsumerOptions) *TracesConsumer {
	tc := &TracesConsumer{
		consumer: c,
		inserter: inserter,
		opts:     opts.withDefaults(),
		stopCh:   make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	tc.aggregator = NewTraceAggregator(tc.opts.AggregateTimeout, tc.writeSpans)
	return tc
}

// Start 订阅 Topic 并启动过期扫描。
func (t *TracesConsumer) Start(ctx context.Context) error {
	if err := t.consumer.Subscribe(ctx, t.opts.Topic, t.handle); err != nil {
		return fmt.Errorf("traces consumer subscribe: %w", err)
	}
	t.startedMu.Lock()
	t.started = true
	t.startedMu.Unlock()
	go t.loop()
	logger.L().WithField("topic", t.opts.Topic).Info("traces consumer started")
	return nil
}

func (t *TracesConsumer) loop() {
	defer close(t.stopped)
	ticker := time.NewTicker(t.opts.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-t.stopCh:
			return
		case <-ticker.C:
			t.aggregator.SweepExpired()
		}
	}
}

func (t *TracesConsumer) handle(ctx context.Context, msg *messagebus.Message) error {
	metrics.PulsarMessagesTotal.WithLabelValues(t.opts.Topic, "consume").Inc()
	if !msg.Timestamp.IsZero() {
		metrics.ConsumerLag.WithLabelValues(t.opts.Topic).Set(time.Since(msg.Timestamp).Seconds())
	}

	var span models.TraceMessage
	if err := json.Unmarshal(msg.Payload, &span); err != nil {
		metrics.ConsumerErrorsTotal.WithLabelValues(t.opts.Topic, "unmarshal").Inc()
		logger.FromContext(ctx).WithError(err).Error("traces consumer: invalid payload, dropped")
		return nil
	}
	if err := span.Validate(); err != nil {
		metrics.ConsumerErrorsTotal.WithLabelValues(t.opts.Topic, "validate").Inc()
		logger.FromContext(ctx).WithError(err).Warn("traces consumer: invalid span, dropped")
		return nil
	}
	t.aggregator.Add(&span)
	return nil
}

// writeSpans 是聚合完成后的落盘回调。
// 写入失败时把 spans 放回聚合器，等待下一个窗口重试，避免数据丢失。
func (t *TracesConsumer) writeSpans(requestKey string, spans []*models.TraceMessage, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := t.inserter.InsertTraces(ctx, spans); err != nil {
		metrics.ConsumerErrorsTotal.WithLabelValues(t.opts.Topic, "insert").Inc()
		logger.L().WithError(err).WithField("request", requestKey).Error("traces flush failed, re-buffered")
		for _, span := range spans {
			t.aggregator.Add(span)
		}
		return
	}
	metrics.TraceAggregationFlushTotal.WithLabelValues(reason).Inc()
	logger.L().WithFields(map[string]interface{}{
		"request": requestKey,
		"spans":   len(spans),
		"reason":  reason,
	}).Debug("trace aggregation flushed")
}

// Stop 优雅关闭：停止订阅，并把内存中未完成聚合的 Span 先落盘再退出。
// 未调用 Start 时直接返回，避免等待 loop 关闭 stopped 造成死锁。
func (t *TracesConsumer) Stop(ctx context.Context) error {
	var err error
	t.once.Do(func() {
		close(t.stopCh)
		t.startedMu.Lock()
		started := t.started
		t.startedMu.Unlock()
		if started {
			<-t.stopped
		} else {
			close(t.stopped)
		}
		if e := t.consumer.Stop(); e != nil {
			err = e
		}
		// 最终落盘做有界重试：writeSpans 失败会把 spans 放回聚合器，
		// 重试 FlushAll 直至聚合器清空，避免关闭后不再有 sweep 周期导致数据滞留内存丢失。
		if e := retry(3, func() error {
			t.aggregator.FlushAll("shutdown")
			if n := t.aggregator.PendingSpans(); n > 0 {
				return fmt.Errorf("traces consumer: %d spans still pending after shutdown flush", n)
			}
			return nil
		}); e != nil && err == nil {
			err = e
		}
		if e := t.inserter.Flush(); e != nil && err == nil {
			err = e
		}
	})
	return err
}

// Aggregator 暴露聚合器（测试辅助）。
func (t *TracesConsumer) Aggregator() *TraceAggregator { return t.aggregator }
