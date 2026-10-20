// Package consumer 实现三条管道的消息消费与落盘逻辑。
package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mnemosyne-labs/mneme/pkg/cache"
	"github.com/mnemosyne-labs/mneme/pkg/logger"
	"github.com/mnemosyne-labs/mneme/pkg/messagebus"
	"github.com/mnemosyne-labs/mneme/pkg/metrics"
	"github.com/mnemosyne-labs/mneme/pkg/models"
	"github.com/mnemosyne-labs/mneme/pkg/storage"
)

// HistoryConsumerOptions 管道一消费者配置。
type HistoryConsumerOptions struct {
	Topic         string
	BatchSize     int
	FlushInterval time.Duration
}

func (o HistoryConsumerOptions) withDefaults() HistoryConsumerOptions {
	if o.Topic == "" {
		o.Topic = "mneme-history"
	}
	if o.BatchSize <= 0 {
		o.BatchSize = 10000
	}
	if o.FlushInterval <= 0 {
		o.FlushInterval = 5 * time.Second
	}
	return o
}

// HistoryConsumer 消费 mneme-history，去重后攒批写入 ClickHouse。
type HistoryConsumer struct {
	consumer messagebus.Consumer
	inserter storage.BatchInserter
	deduper  cache.Deduper
	opts     HistoryConsumerOptions

	mu      sync.Mutex
	buffer  []*models.HistoryMessage
	pending map[string]struct{} // 已缓冲但尚未落盘的 message_id，用于窗口内去重

	// flushCh 容量为 1 的通知通道：batch 满时由 handle 通知 loop 执行异步 flush，
	// 避免在消费循环中同步执行可能长达数十秒的重试写入而阻塞消费进度。
	flushCh chan struct{}

	stopCh  chan struct{}
	stopped chan struct{}
	once    sync.Once

	startedMu sync.Mutex
	started   bool
}

// NewHistoryConsumer 创建管道一消费者。deduper 可为 nil（不去重）。
func NewHistoryConsumer(c messagebus.Consumer, inserter storage.BatchInserter, deduper cache.Deduper, opts HistoryConsumerOptions) *HistoryConsumer {
	return &HistoryConsumer{
		consumer: c,
		inserter: inserter,
		deduper:  deduper,
		opts:     opts.withDefaults(),
		pending:  make(map[string]struct{}),
		flushCh:  make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
		stopped:  make(chan struct{}),
	}
}

// Start 订阅 Topic 并启动定时刷新。
func (h *HistoryConsumer) Start(ctx context.Context) error {
	if err := h.consumer.Subscribe(ctx, h.opts.Topic, h.handle); err != nil {
		return fmt.Errorf("history consumer subscribe: %w", err)
	}
	h.startedMu.Lock()
	h.started = true
	h.startedMu.Unlock()
	go h.loop()
	logger.L().WithField("topic", h.opts.Topic).Info("history consumer started")
	return nil
}

func (h *HistoryConsumer) loop() {
	defer close(h.stopped)
	ticker := time.NewTicker(h.opts.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-h.stopCh:
			return
		case <-ticker.C:
			if err := h.flush(context.Background()); err != nil {
				logger.L().WithError(err).Error("history consumer periodic flush failed")
			}
		case <-h.flushCh:
			if err := h.flush(context.Background()); err != nil {
				logger.L().WithError(err).Error("history consumer batch flush failed")
			}
		}
	}
}

// handle 处理单条消息。正常路径不返回 error，避免触发 Nack 重投递：
// 写入失败的数据已由 flush 放回缓冲区，交由定时 flush 重试，
// 若再触发 Nack 会与缓冲重试叠加造成消息被重复处理。
// 仅解析/校验/去重失败等不可恢复错误才返回 error。
func (h *HistoryConsumer) handle(ctx context.Context, msg *messagebus.Message) error {
	metrics.PulsarMessagesTotal.WithLabelValues(h.opts.Topic, "consume").Inc()
	if !msg.Timestamp.IsZero() {
		metrics.ConsumerLag.WithLabelValues(h.opts.Topic).Set(time.Since(msg.Timestamp).Seconds())
	}

	var m models.HistoryMessage
	if err := json.Unmarshal(msg.Payload, &m); err != nil {
		// 解析失败属于脏数据，Ack 掉避免无限重投。
		metrics.ConsumerErrorsTotal.WithLabelValues(h.opts.Topic, "unmarshal").Inc()
		logger.FromContext(ctx).WithError(err).Error("history consumer: invalid payload, dropped")
		return nil
	}
	if err := m.Validate(); err != nil {
		metrics.ConsumerErrorsTotal.WithLabelValues(h.opts.Topic, "validate").Inc()
		logger.FromContext(ctx).WithError(err).Warn("history consumer: invalid message, dropped")
		return nil
	}

	if h.deduper != nil {
		// 只检查不标记：标记必须发生在数据交给写入器之后，
		// 否则崩溃/写入失败时消息会被误判为重复而永久丢失。
		dup, err := h.deduper.Exists(ctx, m.MessageID)
		if err != nil {
			metrics.ConsumerErrorsTotal.WithLabelValues(h.opts.Topic, "dedup").Inc()
			return fmt.Errorf("history consumer: dedup check: %w", err)
		}
		if dup {
			metrics.DedupFilteredTotal.WithLabelValues(storage.PipelineHistory).Inc()
			logger.FromContext(ctx).WithField("message_id", m.MessageID).Debug("duplicate history message filtered")
			return nil
		}
	}

	h.mu.Lock()
	if h.deduper != nil {
		if _, ok := h.pending[m.MessageID]; ok {
			h.mu.Unlock()
			metrics.DedupFilteredTotal.WithLabelValues(storage.PipelineHistory).Inc()
			logger.FromContext(ctx).WithField("message_id", m.MessageID).Debug("duplicate history message filtered")
			return nil
		}
		h.pending[m.MessageID] = struct{}{}
	}
	h.buffer = append(h.buffer, &m)
	ready := len(h.buffer) >= h.opts.BatchSize
	h.mu.Unlock()

	if ready {
		// 非阻塞通知 loop 异步 flush，避免同步写库阻塞消费循环。
		// 通知被丢弃时（已有一次 flush 在排队）由定时 flush 兜底。
		select {
		case h.flushCh <- struct{}{}:
		default:
		}
	}
	return nil
}

// flush 将缓冲区写入 ClickHouse。
func (h *HistoryConsumer) flush(ctx context.Context) error {
	h.mu.Lock()
	if len(h.buffer) == 0 {
		h.mu.Unlock()
		return nil
	}
	batch := h.buffer
	h.buffer = nil
	if h.deduper != nil {
		for _, m := range batch {
			delete(h.pending, m.MessageID)
		}
	}
	h.mu.Unlock()

	if err := h.inserter.InsertHistory(ctx, batch); err != nil {
		metrics.ConsumerErrorsTotal.WithLabelValues(h.opts.Topic, "insert").Inc()
		// 写入失败时把数据放回缓冲区，等待下次重试。
		h.mu.Lock()
		h.buffer = append(batch, h.buffer...)
		if h.deduper != nil {
			for _, m := range batch {
				h.pending[m.MessageID] = struct{}{}
			}
		}
		h.mu.Unlock()
		return fmt.Errorf("history consumer: insert: %w", err)
	}
	if h.deduper != nil {
		// 数据已交给写入器后再标记，避免先标记后失败造成消息丢失。
		for _, m := range batch {
			if _, err := h.deduper.CheckAndAdd(ctx, m.MessageID); err != nil {
				logger.L().WithError(err).WithField("message_id", m.MessageID).Warn("history consumer: mark dedup failed")
			}
		}
	}
	logger.L().WithField("rows", len(batch)).Debug("history batch flushed")
	return nil
}

// Stop 优雅关闭：停止订阅并把剩余数据落盘。
// 未调用 Start 时直接执行落盘，避免等待 loop 关闭 stopped 造成死锁。
func (h *HistoryConsumer) Stop(ctx context.Context) error {
	var err error
	h.once.Do(func() {
		close(h.stopCh)
		h.startedMu.Lock()
		started := h.started
		h.startedMu.Unlock()
		if started {
			<-h.stopped
		} else {
			close(h.stopped)
		}
		if e := h.consumer.Stop(); e != nil {
			err = e
		}
		// 最终落盘做有界重试：flush 失败会放回缓冲区，重试可再次取到数据。
		if e := retry(3, func() error { return h.flush(ctx) }); e != nil {
			err = e
		}
		logger.L().Info("history consumer stopped")
	})
	return err
}

// Buffered 返回当前缓冲区长度（测试辅助）。
func (h *HistoryConsumer) Buffered() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.buffer)
}
