package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mnemosyne-labs/mneme/internal/service"
	"github.com/mnemosyne-labs/mneme/pkg/logger"
	"github.com/mnemosyne-labs/mneme/pkg/messagebus"
	"github.com/mnemosyne-labs/mneme/pkg/metrics"
	"github.com/mnemosyne-labs/mneme/pkg/models"
)

// SessionSummaryConsumerOptions 会话总结消费者配置。
type SessionSummaryConsumerOptions struct {
	// Topic 订阅的对话历史 Topic（通常为 mneme-history）。
	Topic string
	// SummarizeInterval 定时总结周期，默认 10 分钟。
	SummarizeInterval time.Duration
	// MaxMessagesPerSession 单会话最多缓冲的消息数，超出丢弃最旧的。
	MaxMessagesPerSession int
}

func (o SessionSummaryConsumerOptions) withDefaults() SessionSummaryConsumerOptions {
	if o.Topic == "" {
		o.Topic = "mneme-history"
	}
	if o.SummarizeInterval <= 0 {
		o.SummarizeInterval = 10 * time.Minute
	}
	if o.MaxMessagesPerSession <= 0 {
		o.MaxMessagesPerSession = 2000
	}
	return o
}

// sessionKey 用于按 (tenant, session) 聚合缓冲。
type sessionKey struct {
	Tenant  string
	Session string
	UserID  string
}

// sessionBuffer 会话缓冲，保留原始消息与起止时间，用于总结。
type sessionBuffer struct {
	Messages []*models.HistoryMessage
	Started  time.Time
	Ended    time.Time
}

// SessionSummaryConsumer 消费对话历史，按 session_id 缓冲并在定时窗口生成会话总结，
// 与用户画像（HistoryProfileConsumer）互补：前者描述"这次对话聊了什么"，后者描述"这个用户是谁"。
type SessionSummaryConsumer struct {
	consumer   messagebus.Consumer
	summarizer service.SessionSummarizer
	store      service.SessionSummaryStore
	opts       SessionSummaryConsumerOptions

	mu     sync.Mutex
	buffer map[sessionKey]*sessionBuffer

	stopCh  chan struct{}
	stopped chan struct{}
	once    sync.Once

	startedMu sync.Mutex
	started   bool
}

// NewSessionSummaryConsumer 创建会话总结消费者。
func NewSessionSummaryConsumer(c messagebus.Consumer, summarizer service.SessionSummarizer, store service.SessionSummaryStore, opts SessionSummaryConsumerOptions) *SessionSummaryConsumer {
	opts = opts.withDefaults()
	return &SessionSummaryConsumer{
		consumer:   c,
		summarizer: summarizer,
		store:      store,
		opts:       opts,
		buffer:     make(map[sessionKey]*sessionBuffer),
		stopCh:     make(chan struct{}),
		stopped:    make(chan struct{}),
	}
}

// Start 订阅 Topic 并启动定时总结任务。
func (s *SessionSummaryConsumer) Start(ctx context.Context) error {
	if err := s.consumer.Subscribe(ctx, s.opts.Topic, s.handle); err != nil {
		return fmt.Errorf("session summary consumer subscribe: %w", err)
	}
	s.startedMu.Lock()
	s.started = true
	s.startedMu.Unlock()
	go s.loop()
	logger.L().WithFields(map[string]interface{}{
		"topic":    s.opts.Topic,
		"interval": s.opts.SummarizeInterval.String(),
	}).Info("session summary consumer started")
	return nil
}

func (s *SessionSummaryConsumer) loop() {
	defer close(s.stopped)
	ticker := time.NewTicker(s.opts.SummarizeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			if n, err := s.RunSummarization(context.Background()); err != nil {
				logger.L().WithError(err).WithField("sessions", n).Error("scheduled session summarization failed")
			}
		}
	}
}

func (s *SessionSummaryConsumer) handle(ctx context.Context, msg *messagebus.Message) error {
	metrics.PulsarMessagesTotal.WithLabelValues(s.opts.Topic, "consume").Inc()
	if !msg.Timestamp.IsZero() {
		metrics.ConsumerLag.WithLabelValues(s.opts.Topic).Set(time.Since(msg.Timestamp).Seconds())
	}

	var hist models.HistoryMessage
	if err := json.Unmarshal(msg.Payload, &hist); err != nil {
		metrics.ConsumerErrorsTotal.WithLabelValues(s.opts.Topic, "unmarshal").Inc()
		logger.FromContext(ctx).WithError(err).Error("session summary consumer: invalid payload, dropped")
		return nil
	}
	if hist.SessionID == "" {
		// 无法归到具体会话的消息无法生成会话总结，直接丢弃。
		return nil
	}

	key := sessionKey{Tenant: hist.TenantID, Session: hist.SessionID, UserID: hist.UserID}
	s.mu.Lock()
	buf, ok := s.buffer[key]
	if !ok {
		buf = &sessionBuffer{}
		s.buffer[key] = buf
	}
	buf.Messages = append(buf.Messages, &hist)
	if len(buf.Messages) > s.opts.MaxMessagesPerSession {
		buf.Messages = buf.Messages[len(buf.Messages)-s.opts.MaxMessagesPerSession:]
	}
	if hist.CreatedAt.Before(buf.Started) || buf.Started.IsZero() {
		buf.Started = hist.CreatedAt
	}
	if hist.CreatedAt.After(buf.Ended) {
		buf.Ended = hist.CreatedAt
	}
	s.mu.Unlock()
	return nil
}

// RunSummarization 立即对所有缓冲会话执行一次总结，返回处理的会话数量。
func (s *SessionSummaryConsumer) RunSummarization(ctx context.Context) (int, error) {
	s.mu.Lock()
	pending := s.buffer
	s.buffer = make(map[sessionKey]*sessionBuffer)
	s.mu.Unlock()

	if len(pending) == 0 {
		return 0, nil
	}

	var firstErr error
	processed := 0
	for key, buf := range pending {
		if len(buf.Messages) == 0 {
			continue
		}

		// 若已有上一次总结，则在其基础上做增量更新；否则做全量总结。
		var summary *models.SessionSummary
		prev, perr := s.store.GetSummary(ctx, key.Tenant, key.Session)
		if perr != nil {
			metrics.ConsumerErrorsTotal.WithLabelValues(s.opts.Topic, "load_summary").Inc()
			s.mu.Lock()
			s.buffer[key] = buf
			s.mu.Unlock()
			if firstErr == nil {
				firstErr = perr
			}
			continue
		}
		if prev != nil {
			text, mode := service.RefineSummaryWithFallback(ctx, s.summarizer, prev.Summary, buf.Messages)
			started := prev.StartedAt
			if buf.Started.Before(started) || started.IsZero() {
				started = buf.Started
			}
			ended := prev.EndedAt
			if buf.Ended.After(ended) {
				ended = buf.Ended
			}
			summary = &models.SessionSummary{
				SessionID:    key.Session,
				TenantID:     key.Tenant,
				UserID:       key.UserID,
				Summary:      text,
				MessageCount: prev.MessageCount + len(buf.Messages),
				StartedAt:    started,
				EndedAt:      ended,
			}
			metrics.MemoryExtractTotal.WithLabelValues("session_" + mode).Inc()
		} else {
			text, mode := service.SummarizeWithFallback(ctx, s.summarizer, buf.Messages)
			summary = &models.SessionSummary{
				SessionID:    key.Session,
				TenantID:     key.Tenant,
				UserID:       key.UserID,
				Summary:      text,
				MessageCount: len(buf.Messages),
				StartedAt:    buf.Started,
				EndedAt:      buf.Ended,
			}
			metrics.MemoryExtractTotal.WithLabelValues("session_" + mode).Inc()
		}

		if err := summary.Validate(); err != nil {
			s.mu.Lock()
			s.buffer[key] = buf
			s.mu.Unlock()
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if _, err := s.store.SaveSummary(ctx, summary); err != nil {
			metrics.ConsumerErrorsTotal.WithLabelValues(s.opts.Topic, "save_summary").Inc()
			s.mu.Lock()
			s.buffer[key] = buf
			s.mu.Unlock()
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		processed++
	}
	return processed, firstErr
}

// Stop 优雅关闭：停止订阅并对剩余消息执行最后一次总结。
func (s *SessionSummaryConsumer) Stop(ctx context.Context) error {
	var err error
	s.once.Do(func() {
		close(s.stopCh)
		s.startedMu.Lock()
		started := s.started
		s.startedMu.Unlock()
		if started {
			<-s.stopped
		} else {
			close(s.stopped)
		}
		if e := s.consumer.Stop(); e != nil {
			err = e
		}
		if e := retry(3, func() error {
			_, runErr := s.RunSummarization(ctx)
			return runErr
		}); e != nil && err == nil {
			err = e
		}
		logger.L().Info("session summary consumer stopped")
	})
	return err
}

// PendingSessions 返回缓冲中的会话数量（测试辅助）。
func (s *SessionSummaryConsumer) PendingSessions() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.buffer)
}
