package messagebus

import (
	"context"
	"errors"
	"testing"
)

func TestFullTopic(t *testing.T) {
	cfg := Config{Tenant: "mnemosyne", Namespace: "mneme"}
	if got := cfg.FullTopic("mneme-history"); got != "persistent://mnemosyne/mneme/mneme-history" {
		t.Errorf("got %s", got)
	}
	if got := cfg.FullTopic("persistent://a/b/c"); got != "persistent://a/b/c" {
		t.Errorf("got %s", got)
	}
	if got := cfg.FullTopic(""); got != "" {
		t.Errorf("got %s", got)
	}
	empty := Config{}
	if got := empty.FullTopic("t"); got != "t" {
		t.Errorf("got %s", got)
	}
}

func TestMemoryBusSendSubscribe(t *testing.T) {
	ctx := context.Background()
	bus := NewMemoryBus()

	var received [][]byte
	if err := bus.Subscribe(ctx, "topic-a", func(_ context.Context, m *Message) error {
		received = append(received, m.Payload)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := bus.Send(ctx, "topic-a", "k1", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if len(received) != 1 || string(received[0]) != "hello" {
		t.Fatalf("received = %v", received)
	}
	if bus.AckCount() != 1 {
		t.Errorf("ack = %d", bus.AckCount())
	}
	if len(bus.Messages("topic-a")) != 1 {
		t.Errorf("stored messages wrong")
	}
}

func TestMemoryBusHandlerErrorNacks(t *testing.T) {
	ctx := context.Background()
	bus := NewMemoryBus()
	_ = bus.Subscribe(ctx, "t", func(_ context.Context, _ *Message) error {
		return errors.New("boom")
	})
	_ = bus.Send(ctx, "t", "", []byte("x"))
	if bus.NackCount() != 1 {
		t.Errorf("nack = %d", bus.NackCount())
	}
}

func TestMemoryBusBatchAndErrors(t *testing.T) {
	ctx := context.Background()
	bus := NewMemoryBus()
	msgs := []*Message{{Payload: []byte("1")}, {Payload: []byte("2")}}
	if err := bus.SendBatch(ctx, "b", msgs); err != nil {
		t.Fatal(err)
	}
	if len(bus.Messages("b")) != 2 {
		t.Error("batch not stored")
	}
	if err := bus.SendBatch(ctx, "", msgs); !errors.Is(err, ErrEmptyTopic) {
		t.Error("expected ErrEmptyTopic")
	}
	if err := bus.Send(ctx, "", "", nil); !errors.Is(err, ErrEmptyTopic) {
		t.Error("expected ErrEmptyTopic")
	}
	if err := bus.Subscribe(ctx, "x", nil); !errors.Is(err, ErrNilHandler) {
		t.Error("expected ErrNilHandler")
	}
	if err := bus.Subscribe(ctx, "", func(context.Context, *Message) error { return nil }); !errors.Is(err, ErrEmptyTopic) {
		t.Error("expected ErrEmptyTopic")
	}

	if err := bus.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := bus.Send(ctx, "b", "", nil); !errors.Is(err, ErrClosed) {
		t.Error("expected ErrClosed")
	}
	if err := bus.Subscribe(ctx, "b", func(context.Context, *Message) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Error("expected ErrClosed")
	}
}

func TestMessageRaw(t *testing.T) {
	m := &Message{}
	m.SetRaw("raw")
	if m.Raw() != "raw" {
		t.Error("raw mismatch")
	}
}
