// Package messagebus 定义消息总线抽象（Producer / Consumer），
// 并提供 Pulsar 实现与内存实现（用于测试）。
package messagebus

import (
	"context"
	"errors"
	"time"
)

// 常见错误。
var (
	ErrClosed        = errors.New("messagebus: closed")
	ErrEmptyTopic    = errors.New("messagebus: empty topic")
	ErrNilHandler    = errors.New("messagebus: nil handler")
	ErrNotSubscribed = errors.New("messagebus: not subscribed")
)

// Message 是消息总线中传输的统一消息结构。
type Message struct {
	ID        string
	Topic     string
	Key       string
	Payload   []byte
	Headers   map[string]string
	Timestamp time.Time

	// raw 保存底层实现的原始消息句柄，用于 Ack / Nack。
	raw interface{}
}

// Raw 返回底层原始消息（实现细节，调用方一般无需关心）。
func (m *Message) Raw() interface{} { return m.raw }

// SetRaw 设置底层原始消息。
func (m *Message) SetRaw(v interface{}) { m.raw = v }

// MessageHandler 处理单条消息，返回 error 表示处理失败（触发 Nack）。
type MessageHandler func(ctx context.Context, msg *Message) error

// Producer 消息生产者抽象。
type Producer interface {
	Send(ctx context.Context, topic string, key string, payload []byte) error
	SendBatch(ctx context.Context, topic string, messages []*Message) error
	Close() error
}

// Consumer 消息消费者抽象。
type Consumer interface {
	Subscribe(ctx context.Context, topic string, handler MessageHandler) error
	Ack(ctx context.Context, msg *Message) error
	Nack(ctx context.Context, msg *Message) error
	Stop() error
}

// Config 消息总线连接配置。
type Config struct {
	URL               string
	Tenant            string
	Namespace         string
	Token             string
	OperationTimeout  time.Duration
	ConnectionTimeout time.Duration
	SubscriptionName  string
	// MaxRedeliver 消息最大重投递次数，超过后进入 DLQ。
	MaxRedeliver uint32
	// NackRedeliveryDelay 处理失败后重新投递的延迟。
	NackRedeliveryDelay time.Duration
}

// FullTopic 拼接 persistent://tenant/namespace/topic 形式的完整 Topic 名。
func (c Config) FullTopic(topic string) string {
	if topic == "" {
		return ""
	}
	// 已经是完整 URL 则直接返回。
	if len(topic) > 13 && (topic[:13] == "persistent://") {
		return topic
	}
	if len(topic) > 21 && topic[:21] == "non-persistent://" {
		return topic
	}
	if c.Tenant == "" || c.Namespace == "" {
		return topic
	}
	return "persistent://" + c.Tenant + "/" + c.Namespace + "/" + topic
}
