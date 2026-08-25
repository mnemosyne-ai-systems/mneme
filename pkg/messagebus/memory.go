package messagebus

import (
	"context"
	"sync"
	"time"

	"github.com/mnemosyne/mneme/pkg/models"
)

// MemoryBus 是一个进程内消息总线，用于单元测试与本地开发。
type MemoryBus struct {
	mu       sync.RWMutex
	topics   map[string][]*Message
	handlers map[string][]MessageHandler
	acked    map[string]int
	nacked   map[string]int
	closed   bool
}

// NewMemoryBus 创建内存消息总线。
func NewMemoryBus() *MemoryBus {
	return &MemoryBus{
		topics:   make(map[string][]*Message),
		handlers: make(map[string][]MessageHandler),
		acked:    make(map[string]int),
		nacked:   make(map[string]int),
	}
}

// Send 实现 Producer。
func (b *MemoryBus) Send(ctx context.Context, topic, key string, payload []byte) error {
	if topic == "" {
		return ErrEmptyTopic
	}
	msg := &Message{
		ID:        models.NewID(),
		Topic:     topic,
		Key:       key,
		Payload:   payload,
		Headers:   map[string]string{},
		Timestamp: time.Now().UTC(),
	}
	return b.deliver(ctx, topic, msg)
}

// SendBatch 实现 Producer。
func (b *MemoryBus) SendBatch(ctx context.Context, topic string, messages []*Message) error {
	if topic == "" {
		return ErrEmptyTopic
	}
	for _, m := range messages {
		cp := *m
		if cp.ID == "" {
			cp.ID = models.NewID()
		}
		cp.Topic = topic
		if cp.Timestamp.IsZero() {
			cp.Timestamp = time.Now().UTC()
		}
		if err := b.deliver(ctx, topic, &cp); err != nil {
			return err
		}
	}
	return nil
}

func (b *MemoryBus) deliver(ctx context.Context, topic string, msg *Message) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}
	b.topics[topic] = append(b.topics[topic], msg)
	handlers := append([]MessageHandler(nil), b.handlers[topic]...)
	b.mu.Unlock()

	for _, h := range handlers {
		if err := h(ctx, msg); err != nil {
			_ = b.Nack(ctx, msg)
			continue
		}
		_ = b.Ack(ctx, msg)
	}
	return nil
}

// Subscribe 实现 Consumer。
func (b *MemoryBus) Subscribe(ctx context.Context, topic string, handler MessageHandler) error {
	if topic == "" {
		return ErrEmptyTopic
	}
	if handler == nil {
		return ErrNilHandler
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	b.handlers[topic] = append(b.handlers[topic], handler)
	return nil
}

// Ack 实现 Consumer。
func (b *MemoryBus) Ack(_ context.Context, msg *Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.acked[msg.ID]++
	return nil
}

// Nack 实现 Consumer。
func (b *MemoryBus) Nack(_ context.Context, msg *Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nacked[msg.ID]++
	return nil
}

// Stop 实现 Consumer。
func (b *MemoryBus) Stop() error { return b.Close() }

// Close 实现 Producer。
func (b *MemoryBus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

// Messages 返回指定 Topic 已发送的消息（测试辅助）。
func (b *MemoryBus) Messages(topic string) []*Message {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return append([]*Message(nil), b.topics[topic]...)
}

// AckCount 返回 Ack 次数（测试辅助）。
func (b *MemoryBus) AckCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	n := 0
	for _, c := range b.acked {
		n += c
	}
	return n
}

// NackCount 返回 Nack 次数（测试辅助）。
func (b *MemoryBus) NackCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	n := 0
	for _, c := range b.nacked {
		n += c
	}
	return n
}

var (
	_ Producer = (*MemoryBus)(nil)
	_ Consumer = (*MemoryBus)(nil)
)
