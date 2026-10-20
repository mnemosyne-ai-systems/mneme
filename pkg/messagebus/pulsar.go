package messagebus

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/mnemosyne-ai-systems/mneme/pkg/logger"
)

// pulsarClient 封装共享的 Pulsar 连接。
type pulsarClient struct {
	client pulsar.Client
	cfg    Config
}

func newPulsarClient(cfg Config) (*pulsarClient, error) {
	opts := pulsar.ClientOptions{
		URL:               cfg.URL,
		OperationTimeout:  orDuration(cfg.OperationTimeout, 30*time.Second),
		ConnectionTimeout: orDuration(cfg.ConnectionTimeout, 10*time.Second),
	}
	if cfg.Token != "" {
		opts.Authentication = pulsar.NewAuthenticationToken(cfg.Token)
	}
	c, err := pulsar.NewClient(opts)
	if err != nil {
		return nil, fmt.Errorf("pulsar: new client: %w", err)
	}
	return &pulsarClient{client: c, cfg: cfg}, nil
}

func orDuration(d, fallback time.Duration) time.Duration {
	if d <= 0 {
		return fallback
	}
	return d
}

// PulsarProducer 是基于 Pulsar 的 Producer 实现，按 Topic 懒加载底层 producer。
type PulsarProducer struct {
	pc        *pulsarClient
	mu        sync.Mutex
	producers map[string]pulsar.Producer
	closed    bool
}

// NewProducer 创建 Pulsar Producer。
func NewProducer(cfg Config) (Producer, error) {
	pc, err := newPulsarClient(cfg)
	if err != nil {
		return nil, err
	}
	return &PulsarProducer{pc: pc, producers: make(map[string]pulsar.Producer)}, nil
}

func (p *PulsarProducer) producerFor(topic string) (pulsar.Producer, error) {
	full := p.pc.cfg.FullTopic(topic)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrClosed
	}
	if prod, ok := p.producers[full]; ok {
		return prod, nil
	}
	prod, err := p.pc.client.CreateProducer(pulsar.ProducerOptions{
		Topic:                   full,
		SendTimeout:             orDuration(p.pc.cfg.OperationTimeout, 30*time.Second),
		BatchingMaxPublishDelay: 10 * time.Millisecond,
	})
	if err != nil {
		return nil, fmt.Errorf("pulsar: create producer for %s: %w", full, err)
	}
	p.producers[full] = prod
	return prod, nil
}

// Send 发送单条消息。
func (p *PulsarProducer) Send(ctx context.Context, topic, key string, payload []byte) error {
	if topic == "" {
		return ErrEmptyTopic
	}
	prod, err := p.producerFor(topic)
	if err != nil {
		return err
	}
	_, err = prod.Send(ctx, &pulsar.ProducerMessage{Key: key, Payload: payload})
	if err != nil {
		return fmt.Errorf("pulsar: send to %s: %w", topic, err)
	}
	return nil
}

// SendBatch 异步批量发送并等待全部完成。
func (p *PulsarProducer) SendBatch(ctx context.Context, topic string, messages []*Message) error {
	if topic == "" {
		return ErrEmptyTopic
	}
	if len(messages) == 0 {
		return nil
	}
	prod, err := p.producerFor(topic)
	if err != nil {
		return err
	}

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	for _, m := range messages {
		wg.Add(1)
		prod.SendAsync(ctx, &pulsar.ProducerMessage{
			Key:        m.Key,
			Payload:    m.Payload,
			Properties: m.Headers,
		}, func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
			defer wg.Done()
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if firstErr != nil {
		return fmt.Errorf("pulsar: send batch to %s: %w", topic, firstErr)
	}
	return nil
}

// Close 关闭所有 producer 与客户端。
func (p *PulsarProducer) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	prods := make([]pulsar.Producer, 0, len(p.producers))
	for _, prod := range p.producers {
		prods = append(prods, prod)
	}
	p.producers = map[string]pulsar.Producer{}
	p.mu.Unlock()

	for _, prod := range prods {
		prod.Close()
	}
	p.pc.client.Close()
	return nil
}

// PulsarConsumer 是基于 Pulsar 的 Consumer 实现。
type PulsarConsumer struct {
	pc        *pulsarClient
	mu        sync.Mutex
	consumers map[string]pulsar.Consumer // topic(短名) -> consumer
	cancels   map[string]context.CancelFunc
	wg        sync.WaitGroup
	stopped   bool
}

// NewConsumer 创建 Pulsar Consumer。
func NewConsumer(cfg Config) (Consumer, error) {
	pc, err := newPulsarClient(cfg)
	if err != nil {
		return nil, err
	}
	return &PulsarConsumer{
		pc:        pc,
		consumers: make(map[string]pulsar.Consumer),
		cancels:   make(map[string]context.CancelFunc),
	}, nil
}

// Subscribe 订阅 Topic 并在后台循环消费。handler 返回 nil 则 Ack，否则 Nack。
func (c *PulsarConsumer) Subscribe(ctx context.Context, topic string, handler MessageHandler) error {
	if topic == "" {
		return ErrEmptyTopic
	}
	if handler == nil {
		return ErrNilHandler
	}
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return ErrClosed
	}
	c.mu.Unlock()

	full := c.pc.cfg.FullTopic(topic)
	sub := c.pc.cfg.SubscriptionName
	if sub == "" {
		sub = "mneme-sub"
	}
	opts := pulsar.ConsumerOptions{
		Topic:               full,
		SubscriptionName:    sub + "-" + topic,
		Type:                pulsar.KeyShared,
		NackRedeliveryDelay: orDuration(c.pc.cfg.NackRedeliveryDelay, time.Second),
	}
	if c.pc.cfg.MaxRedeliver > 0 {
		opts.DLQ = &pulsar.DLQPolicy{
			MaxDeliveries:   c.pc.cfg.MaxRedeliver,
			DeadLetterTopic: full + "-DLQ",
		}
	}
	consumer, err := c.pc.client.Subscribe(opts)
	if err != nil {
		return fmt.Errorf("pulsar: subscribe %s: %w", full, err)
	}

	loopCtx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	if c.stopped {
		// Stop 可能已并发执行并把 consumers/cancels 置为 nil，
		// 此时继续写入会触发 nil map panic，放弃本次订阅。
		c.mu.Unlock()
		cancel()
		consumer.Close()
		return ErrClosed
	}
	if _, exists := c.consumers[topic]; exists {
		// 同一 topic 重复订阅会覆盖并泄漏旧 consumer，直接拒绝。
		c.mu.Unlock()
		cancel()
		consumer.Close()
		return fmt.Errorf("pulsar: topic %s already subscribed", topic)
	}
	c.consumers[topic] = consumer
	c.cancels[topic] = cancel
	c.mu.Unlock()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.consumeLoop(loopCtx, consumer, topic, handler)
	}()
	return nil
}

func (c *PulsarConsumer) consumeLoop(ctx context.Context, consumer pulsar.Consumer, topic string, handler MessageHandler) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		pmsg, err := consumer.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logger.WithField("topic", topic).WithError(err).Warn("pulsar receive failed")
			time.Sleep(500 * time.Millisecond)
			continue
		}
		msg := &Message{
			ID:        pmsg.ID().String(),
			Topic:     topic,
			Key:       pmsg.Key(),
			Payload:   pmsg.Payload(),
			Headers:   pmsg.Properties(),
			Timestamp: pmsg.PublishTime(),
		}
		msg.SetRaw(pmsg)

		if err := handler(ctx, msg); err != nil {
			logger.WithField("topic", topic).WithError(err).Warn("handler failed, nack message")
			consumer.Nack(pmsg)
			continue
		}
		if err := consumer.Ack(pmsg); err != nil {
			logger.WithField("topic", topic).WithError(err).Warn("ack failed")
		}
	}
}

// Ack 显式确认消息。
func (c *PulsarConsumer) Ack(_ context.Context, msg *Message) error {
	pmsg, ok := msg.Raw().(pulsar.Message)
	if !ok {
		return ErrNotSubscribed
	}
	c.mu.Lock()
	consumer, ok := c.consumers[msg.Topic]
	c.mu.Unlock()
	if !ok {
		return ErrNotSubscribed
	}
	if err := consumer.Ack(pmsg); err != nil {
		return fmt.Errorf("pulsar: ack: %w", err)
	}
	return nil
}

// Nack 否认消息，触发重投递。
func (c *PulsarConsumer) Nack(_ context.Context, msg *Message) error {
	pmsg, ok := msg.Raw().(pulsar.Message)
	if !ok {
		return ErrNotSubscribed
	}
	c.mu.Lock()
	consumer, ok := c.consumers[msg.Topic]
	c.mu.Unlock()
	if !ok {
		return ErrNotSubscribed
	}
	consumer.Nack(pmsg)
	return nil
}

// Stop 停止所有订阅并关闭连接。
func (c *PulsarConsumer) Stop() error {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return nil
	}
	c.stopped = true
	cancels := c.cancels
	consumers := c.consumers
	c.cancels = nil
	c.consumers = nil
	c.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
	for _, consumer := range consumers {
		consumer.Close()
	}
	c.wg.Wait()
	c.pc.client.Close()
	return nil
}

var (
	_ Producer = (*PulsarProducer)(nil)
	_ Consumer = (*PulsarConsumer)(nil)
)
