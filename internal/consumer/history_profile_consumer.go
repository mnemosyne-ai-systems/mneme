package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mnemosyne/mneme/internal/service"
	"github.com/mnemosyne/mneme/pkg/logger"
	"github.com/mnemosyne/mneme/pkg/messagebus"
	"github.com/mnemosyne/mneme/pkg/metrics"
	"github.com/mnemosyne/mneme/pkg/models"
)

// HistoryProfileConsumerOptions 对话历史萃取消费者配置。
type HistoryProfileConsumerOptions struct {
	// Topic 订阅的对话历史 Topic（通常为 mneme-history）。
	Topic string
	// ExtractInterval 定时萃取周期，默认 10 分钟。
	ExtractInterval time.Duration
	// MaxMessagesPerUser 单用户最多缓冲的对话消息数，超出丢弃最旧的。
	MaxMessagesPerUser int
}

func (o HistoryProfileConsumerOptions) withDefaults() HistoryProfileConsumerOptions {
	if o.Topic == "" {
		o.Topic = "mneme-history"
	}
	if o.ExtractInterval <= 0 {
		o.ExtractInterval = 10 * time.Minute
	}
	if o.MaxMessagesPerUser <= 0 {
		o.MaxMessagesPerUser = 500
	}
	return o
}

// HistoryProfileConsumer 消费对话历史，按 user_id 缓冲并在定时窗口萃取用户画像。
// 与 MemoryConsumer 共享萃取与落库逻辑（extractEntityProfile），
// 对话内容以 event_type="message" 的 MemoryEvent 形式喂给同一个 extractor，
// 最终写入同一张 entity_memory 表——显式记忆事件与对话历史汇聚到同一用户画像。
type HistoryProfileConsumer struct {
	consumer  messagebus.Consumer
	extractor service.MemoryExtractor
	store     service.ProfileStore
	opts      HistoryProfileConsumerOptions

	mu     sync.Mutex
	buffer map[entityKey][]*models.MemoryEvent

	stopCh  chan struct{}
	stopped chan struct{}
	once    sync.Once

	startedMu sync.Mutex
	started   bool
}

// NewHistoryProfileConsumer 创建对话历史萃取消费者。
func NewHistoryProfileConsumer(c messagebus.Consumer, extractor service.MemoryExtractor, store service.ProfileStore, opts HistoryProfileConsumerOptions) *HistoryProfileConsumer {
	opts = opts.withDefaults()
	return &HistoryProfileConsumer{
		consumer:  c,
		extractor: extractor,
		store:     store,
		opts:      opts,
		buffer:    make(map[entityKey][]*models.MemoryEvent),
		stopCh:    make(chan struct{}),
		stopped:   make(chan struct{}),
	}
}

// Start 订阅 Topic 并启动定时萃取任务。
func (h *HistoryProfileConsumer) Start(ctx context.Context) error {
	if err := h.consumer.Subscribe(ctx, h.opts.Topic, h.handle); err != nil {
		return fmt.Errorf("history profile consumer subscribe: %w", err)
	}
	h.startedMu.Lock()
	h.started = true
	h.startedMu.Unlock()
	go h.loop()
	logger.L().WithFields(map[string]interface{}{
		"topic":    h.opts.Topic,
		"interval": h.opts.ExtractInterval.String(),
	}).Info("history profile consumer started")
	return nil
}

func (h *HistoryProfileConsumer) loop() {
	defer close(h.stopped)
	ticker := time.NewTicker(h.opts.ExtractInterval)
	defer ticker.Stop()
	for {
		select {
		case <-h.stopCh:
			return
		case <-ticker.C:
			if n, err := h.RunExtraction(context.Background()); err != nil {
				logger.L().WithError(err).WithField("entities", n).Error("scheduled history profile extraction failed")
			}
		}
	}
}

func (h *HistoryProfileConsumer) handle(ctx context.Context, msg *messagebus.Message) error {
	metrics.PulsarMessagesTotal.WithLabelValues(h.opts.Topic, "consume").Inc()
	if !msg.Timestamp.IsZero() {
		metrics.ConsumerLag.WithLabelValues(h.opts.Topic).Set(time.Since(msg.Timestamp).Seconds())
	}

	var hist models.HistoryMessage
	if err := json.Unmarshal(msg.Payload, &hist); err != nil {
		metrics.ConsumerErrorsTotal.WithLabelValues(h.opts.Topic, "unmarshal").Inc()
		logger.FromContext(ctx).WithError(err).Error("history profile consumer: invalid payload, dropped")
		return nil
	}
	if hist.UserID == "" {
		// 无用户归属的消息无法归因到画像，直接丢弃。
		return nil
	}

	evt := &models.MemoryEvent{
		EventID:   models.NewID(),
		EntityID:  hist.UserID,
		TenantID:  hist.TenantID,
		EntityType: models.EntityTypeUser,
		EventType: "message",
		Payload: map[string]interface{}{
			"role":       hist.Role,
			"content":    hist.Content,
			"session_id": hist.SessionID,
			"created_at": hist.CreatedAt,
		},
		Timestamp: hist.CreatedAt,
	}
	if err := evt.Validate(); err != nil {
		metrics.ConsumerErrorsTotal.WithLabelValues(h.opts.Topic, "validate").Inc()
		logger.FromContext(ctx).WithError(err).Warn("history profile consumer: invalid derived event, dropped")
		return nil
	}

	key := entityKey{Tenant: evt.TenantID, EntityType: evt.EntityType, EntityID: evt.EntityID}
	h.mu.Lock()
	list := append(h.buffer[key], evt)
	if len(list) > h.opts.MaxMessagesPerUser {
		list = list[len(list)-h.opts.MaxMessagesPerUser:]
	}
	h.buffer[key] = list
	h.mu.Unlock()
	return nil
}

// RunExtraction 立即对所有缓冲用户执行一次萃取，返回处理的实体数量。
func (h *HistoryProfileConsumer) RunExtraction(ctx context.Context) (int, error) {
	h.mu.Lock()
	pending := h.buffer
	h.buffer = make(map[entityKey][]*models.MemoryEvent)
	h.mu.Unlock()

	if len(pending) == 0 {
		return 0, nil
	}

	var firstErr error
	processed := 0
	for key, events := range pending {
		version, err := extractEntityProfile(ctx, h.extractor, h.store, h.opts.Topic, key, events)
		if err != nil {
			h.mu.Lock()
			list := append(events, h.buffer[key]...)
			if len(list) > h.opts.MaxMessagesPerUser {
				list = list[len(list)-h.opts.MaxMessagesPerUser:]
			}
			h.buffer[key] = list
			h.mu.Unlock()
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if version > 0 {
			processed++
		}
	}
	return processed, firstErr
}

// Stop 优雅关闭：停止订阅并对剩余消息执行最后一次萃取。
func (h *HistoryProfileConsumer) Stop(ctx context.Context) error {
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
		if e := retry(3, func() error {
			_, runErr := h.RunExtraction(ctx)
			return runErr
		}); e != nil && err == nil {
			err = e
		}
		logger.L().Info("history profile consumer stopped")
	})
	return err
}

// PendingEntities 返回缓冲中的用户数量（测试辅助）。
func (h *HistoryProfileConsumer) PendingEntities() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.buffer)
}
