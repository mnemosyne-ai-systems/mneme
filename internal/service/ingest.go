// Package service 承载 Mneme 的业务逻辑。
package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mnemosyne/mneme/pkg/messagebus"
	"github.com/mnemosyne/mneme/pkg/metrics"
	"github.com/mnemosyne/mneme/pkg/models"
)

// Topics 三条管道的 Topic 配置。
type Topics struct {
	History string
	Traces  string
	Memory  string
}

// IngestService 负责把 HTTP / 文件适配器接收到的数据投递到消息总线。
type IngestService struct {
	producer messagebus.Producer
	topics   Topics
}

// NewIngestService 创建接入服务。
func NewIngestService(producer messagebus.Producer, topics Topics) *IngestService {
	return &IngestService{producer: producer, topics: topics}
}

// PublishHistory 校验并投递会话历史消息。
func (s *IngestService) PublishHistory(ctx context.Context, msg *models.HistoryMessage) error {
	msg.Normalize()
	if err := msg.Validate(); err != nil {
		return err
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal history message: %w", err)
	}
	if err := s.producer.Send(ctx, s.topics.History, msg.PartitionKey(), payload); err != nil {
		return fmt.Errorf("publish history: %w", err)
	}
	metrics.PulsarMessagesTotal.WithLabelValues(s.topics.History, "produce").Inc()
	return nil
}

// PublishHistoryBatch 批量投递会话历史消息。
func (s *IngestService) PublishHistoryBatch(ctx context.Context, msgs []*models.HistoryMessage) error {
	if len(msgs) == 0 {
		return nil
	}
	batch := make([]*messagebus.Message, 0, len(msgs))
	for _, m := range msgs {
		m.Normalize()
		if err := m.Validate(); err != nil {
			return err
		}
		payload, err := json.Marshal(m)
		if err != nil {
			return fmt.Errorf("marshal history message: %w", err)
		}
		batch = append(batch, &messagebus.Message{Key: m.PartitionKey(), Payload: payload})
	}
	if err := s.producer.SendBatch(ctx, s.topics.History, batch); err != nil {
		return fmt.Errorf("publish history batch: %w", err)
	}
	metrics.PulsarMessagesTotal.WithLabelValues(s.topics.History, "produce").Add(float64(len(batch)))
	return nil
}

// PublishTrace 校验并投递轨迹 Span。
func (s *IngestService) PublishTrace(ctx context.Context, msg *models.TraceMessage) error {
	msg.Normalize()
	if err := msg.Validate(); err != nil {
		return err
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal trace message: %w", err)
	}
	if err := s.producer.Send(ctx, s.topics.Traces, msg.PartitionKey(), payload); err != nil {
		return fmt.Errorf("publish trace: %w", err)
	}
	metrics.PulsarMessagesTotal.WithLabelValues(s.topics.Traces, "produce").Inc()
	return nil
}

// PublishMemoryEvent 校验并投递长期记忆事件。
func (s *IngestService) PublishMemoryEvent(ctx context.Context, evt *models.MemoryEvent) error {
	evt.Normalize()
	if err := evt.Validate(); err != nil {
		return err
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal memory event: %w", err)
	}
	if err := s.producer.Send(ctx, s.topics.Memory, evt.PartitionKey(), payload); err != nil {
		return fmt.Errorf("publish memory event: %w", err)
	}
	metrics.PulsarMessagesTotal.WithLabelValues(s.topics.Memory, "produce").Inc()
	return nil
}
