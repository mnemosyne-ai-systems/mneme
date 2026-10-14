package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/mnemosyne-labs/mneme/pkg/messagebus"
	"github.com/mnemosyne-labs/mneme/pkg/models"
)

func newTestService() (*IngestService, *messagebus.MemoryBus) {
	bus := messagebus.NewMemoryBus()
	return NewIngestService(bus, Topics{History: "h", Traces: "t", Memory: "m"}), bus
}

func TestPublishHistory(t *testing.T) {
	ctx := context.Background()
	svc, bus := newTestService()

	msg := &models.HistoryMessage{TenantID: "t1", SessionID: "s1", Role: models.RoleUser, Content: "hi"}
	if err := svc.PublishHistory(ctx, msg); err != nil {
		t.Fatal(err)
	}
	msgs := bus.Messages("h")
	if len(msgs) != 1 {
		t.Fatalf("messages = %d", len(msgs))
	}
	var out models.HistoryMessage
	if err := json.Unmarshal(msgs[0].Payload, &out); err != nil {
		t.Fatal(err)
	}
	if out.Content != "hi" || out.MessageID == "" {
		t.Errorf("payload wrong: %+v", out)
	}
	if msgs[0].Key != "t1:s1" {
		t.Errorf("key = %s", msgs[0].Key)
	}
}

func TestPublishHistoryValidation(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService()
	err := svc.PublishHistory(ctx, &models.HistoryMessage{SessionID: "s", Role: models.RoleUser, Content: "x"})
	if err == nil {
		t.Fatal("expected validation error")
	}
	if _, ok := err.(*models.ValidationError); !ok {
		t.Errorf("expected ValidationError, got %T", err)
	}
}

func TestPublishHistoryBatch(t *testing.T) {
	ctx := context.Background()
	svc, bus := newTestService()
	msgs := []*models.HistoryMessage{
		{TenantID: "t", SessionID: "s", Role: models.RoleUser, Content: "a"},
		{TenantID: "t", SessionID: "s", Role: models.RoleAssistant, Content: "b"},
	}
	if err := svc.PublishHistoryBatch(ctx, msgs); err != nil {
		t.Fatal(err)
	}
	if len(bus.Messages("h")) != 2 {
		t.Errorf("messages = %d", len(bus.Messages("h")))
	}
	if err := svc.PublishHistoryBatch(ctx, nil); err != nil {
		t.Error(err)
	}
	if err := svc.PublishHistoryBatch(ctx, []*models.HistoryMessage{{}}); err == nil {
		t.Error("expected validation error")
	}
}

func TestPublishTrace(t *testing.T) {
	ctx := context.Background()
	svc, bus := newTestService()
	tr := &models.TraceMessage{RequestID: "r", TenantID: "t", StepType: models.StepTypeToolCall, StepName: "search"}
	if err := svc.PublishTrace(ctx, tr); err != nil {
		t.Fatal(err)
	}
	if len(bus.Messages("t")) != 1 {
		t.Error("trace not published")
	}
	if err := svc.PublishTrace(ctx, &models.TraceMessage{}); err == nil {
		t.Error("expected validation error")
	}
}

func TestPublishMemoryEvent(t *testing.T) {
	ctx := context.Background()
	svc, bus := newTestService()
	e := &models.MemoryEvent{EntityID: "u", TenantID: "t", EntityType: models.EntityTypeUser, EventType: "x"}
	if err := svc.PublishMemoryEvent(ctx, e); err != nil {
		t.Fatal(err)
	}
	if len(bus.Messages("m")) != 1 {
		t.Error("event not published")
	}
	if err := svc.PublishMemoryEvent(ctx, &models.MemoryEvent{}); err == nil {
		t.Error("expected validation error")
	}
}
