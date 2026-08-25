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

// MemoryConsumerOptions 管道三消费者配置。
type MemoryConsumerOptions struct {
	Topic string
	// ExtractInterval 定时萃取周期，默认 10 分钟。
	ExtractInterval time.Duration
	// MaxEventsPerEntity 单实体最多缓冲事件数，超出丢弃最旧的。
	MaxEventsPerEntity int
}

func (o MemoryConsumerOptions) withDefaults() MemoryConsumerOptions {
	if o.Topic == "" {
		o.Topic = "mneme-memory"
	}
	if o.ExtractInterval <= 0 {
		o.ExtractInterval = 10 * time.Minute
	}
	if o.MaxEventsPerEntity <= 0 {
		o.MaxEventsPerEntity = 500
	}
	return o
}

// entityKey 标识一个待萃取实体。
type entityKey struct {
	Tenant     string
	EntityType string
	EntityID   string
}

// MemoryConsumer 消费 mneme-memory，按实体缓冲事件并定时萃取画像。
type MemoryConsumer struct {
	consumer  messagebus.Consumer
	extractor service.MemoryExtractor
	store     service.ProfileStore
	opts      MemoryConsumerOptions

	mu     sync.Mutex
	buffer map[entityKey][]*models.MemoryEvent

	stopCh  chan struct{}
	stopped chan struct{}
	once    sync.Once

	startedMu sync.Mutex
	started   bool
}

// NewMemoryConsumer 创建管道三消费者。
func NewMemoryConsumer(c messagebus.Consumer, extractor service.MemoryExtractor, store service.ProfileStore, opts MemoryConsumerOptions) *MemoryConsumer {
	return &MemoryConsumer{
		consumer:  c,
		extractor: extractor,
		store:     store,
		opts:      opts.withDefaults(),
		buffer:    make(map[entityKey][]*models.MemoryEvent),
		stopCh:    make(chan struct{}),
		stopped:   make(chan struct{}),
	}
}

// Start 订阅 Topic 并启动定时萃取任务。
func (m *MemoryConsumer) Start(ctx context.Context) error {
	if err := m.consumer.Subscribe(ctx, m.opts.Topic, m.handle); err != nil {
		return fmt.Errorf("memory consumer subscribe: %w", err)
	}
	m.startedMu.Lock()
	m.started = true
	m.startedMu.Unlock()
	go m.loop()
	logger.L().WithFields(map[string]interface{}{
		"topic":    m.opts.Topic,
		"interval": m.opts.ExtractInterval.String(),
	}).Info("memory consumer started")
	return nil
}

func (m *MemoryConsumer) loop() {
	defer close(m.stopped)
	ticker := time.NewTicker(m.opts.ExtractInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			if n, err := m.RunExtraction(context.Background()); err != nil {
				logger.L().WithError(err).WithField("entities", n).Error("scheduled memory extraction failed")
			}
		}
	}
}

func (m *MemoryConsumer) handle(ctx context.Context, msg *messagebus.Message) error {
	metrics.PulsarMessagesTotal.WithLabelValues(m.opts.Topic, "consume").Inc()
	if !msg.Timestamp.IsZero() {
		metrics.ConsumerLag.WithLabelValues(m.opts.Topic).Set(time.Since(msg.Timestamp).Seconds())
	}

	var evt models.MemoryEvent
	if err := json.Unmarshal(msg.Payload, &evt); err != nil {
		metrics.ConsumerErrorsTotal.WithLabelValues(m.opts.Topic, "unmarshal").Inc()
		logger.FromContext(ctx).WithError(err).Error("memory consumer: invalid payload, dropped")
		return nil
	}
	if err := evt.Validate(); err != nil {
		metrics.ConsumerErrorsTotal.WithLabelValues(m.opts.Topic, "validate").Inc()
		logger.FromContext(ctx).WithError(err).Warn("memory consumer: invalid event, dropped")
		return nil
	}

	key := entityKey{Tenant: evt.TenantID, EntityType: evt.EntityType, EntityID: evt.EntityID}
	m.mu.Lock()
	list := append(m.buffer[key], &evt)
	if len(list) > m.opts.MaxEventsPerEntity {
		list = list[len(list)-m.opts.MaxEventsPerEntity:]
	}
	m.buffer[key] = list
	m.mu.Unlock()
	return nil
}

// RunExtraction 立即对所有缓冲实体执行一次萃取，返回处理的实体数量。
func (m *MemoryConsumer) RunExtraction(ctx context.Context) (int, error) {
	m.mu.Lock()
	pending := m.buffer
	m.buffer = make(map[entityKey][]*models.MemoryEvent)
	m.mu.Unlock()

	if len(pending) == 0 {
		return 0, nil
	}

	var firstErr error
	processed := 0
	for key, events := range pending {
		profile, mode := service.ExtractWithFallback(ctx, m.extractor, events)
		if len(profile) == 0 {
			continue
		}
		version, err := m.store.SaveProfile(ctx, key.Tenant, key.EntityType, key.EntityID, profile)
		if err != nil {
			metrics.ConsumerErrorsTotal.WithLabelValues(m.opts.Topic, "save_profile").Inc()
			logger.L().WithError(err).WithField("entity", key.EntityID).Error("save profile failed")
			// 保存失败时把事件放回缓冲区，下轮重试。
			// 与 handle 一致，放回后仍需裁剪 MaxEventsPerEntity，避免持续失败时内存无限增长。
			m.mu.Lock()
			list := append(events, m.buffer[key]...)
			if len(list) > m.opts.MaxEventsPerEntity {
				list = list[len(list)-m.opts.MaxEventsPerEntity:]
			}
			m.buffer[key] = list
			m.mu.Unlock()
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		processed++
		logger.L().WithFields(map[string]interface{}{
			"tenant":  key.Tenant,
			"entity":  key.EntityID,
			"mode":    mode,
			"version": version,
			"events":  len(events),
		}).Info("entity profile updated")
	}
	return processed, firstErr
}

// Stop 优雅关闭：停止订阅并对剩余事件执行最后一次萃取。
// 未调用 Start 时直接执行萃取，避免等待 loop 关闭 stopped 造成死锁。
func (m *MemoryConsumer) Stop(ctx context.Context) error {
	var err error
	m.once.Do(func() {
		close(m.stopCh)
		m.startedMu.Lock()
		started := m.started
		m.startedMu.Unlock()
		if started {
			<-m.stopped
		} else {
			close(m.stopped)
		}
		if e := m.consumer.Stop(); e != nil {
			err = e
		}
		// 最终萃取做有界重试：保存失败会把事件放回缓冲区，重试可再次取到。
		if e := retry(3, func() error {
			_, runErr := m.RunExtraction(ctx)
			return runErr
		}); e != nil && err == nil {
			err = e
		}
		logger.L().Info("memory consumer stopped")
	})
	return err
}

// PendingEntities 返回缓冲中的实体数量（测试辅助）。
func (m *MemoryConsumer) PendingEntities() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.buffer)
}
