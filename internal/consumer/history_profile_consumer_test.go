package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemosyne-labs/mneme/internal/service"
	"github.com/mnemosyne-labs/mneme/pkg/messagebus"
	"github.com/mnemosyne-labs/mneme/pkg/models"
	"github.com/mnemosyne-labs/mneme/pkg/storage"
)

func TestHistoryProfileConsumerExtractsPerUser(t *testing.T) {
	ctx := context.Background()
	bus := messagebus.NewMemoryBus()
	sink := storage.NewMockInserter()
	store := service.NewMemoryProfileStore(sink)
	ex := stubExtractor{profile: map[string]string{"lang": "zh"}}

	c := NewHistoryProfileConsumer(bus, ex, store, HistoryProfileConsumerOptions{
		Topic: "h", ExtractInterval: time.Hour, MaxMessagesPerUser: 500,
	})
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Stop(ctx) }()

	_ = bus.Send(ctx, "h", "", mustJSON(t, &models.HistoryMessage{
		MessageID: "m1", TenantID: "t1", SessionID: "s1", UserID: "u1",
		Role: models.RoleUser, Content: "你好", CreatedAt: time.Now().UTC(),
	}))
	_ = bus.Send(ctx, "h", "", mustJSON(t, &models.HistoryMessage{
		MessageID: "m2", TenantID: "t1", SessionID: "s1", UserID: "u2",
		Role: models.RoleAssistant, Content: "回复", CreatedAt: time.Now().UTC(),
	}))

	if c.PendingEntities() != 2 {
		t.Fatalf("pending = %d", c.PendingEntities())
	}

	n, err := c.RunExtraction(ctx)
	if err != nil || n != 2 {
		t.Fatalf("extraction: n=%d err=%v", n, err)
	}
	if p, err := store.GetProfile(ctx, "t1", models.EntityTypeUser, "u1"); err != nil || len(p) == 0 {
		t.Errorf("u1 profile missing: err=%v p=%v", err, p)
	}
	if p, err := store.GetProfile(ctx, "t1", models.EntityTypeUser, "u2"); err != nil || len(p) == 0 {
		t.Errorf("u2 profile missing: err=%v p=%v", err, p)
	}
	if sink.MemoryCount() != 2 {
		t.Errorf("clickhouse rows = %d", sink.MemoryCount())
	}
}

func TestHistoryProfileConsumerSkipsMessagesWithoutUser(t *testing.T) {
	ctx := context.Background()
	bus := messagebus.NewMemoryBus()
	store := service.NewMemoryProfileStore(storage.NewMockInserter())
	ex := stubExtractor{profile: map[string]string{"x": "1"}}

	c := NewHistoryProfileConsumer(bus, ex, store, HistoryProfileConsumerOptions{Topic: "h"})
	_ = bus.Send(ctx, "h", "", mustJSON(t, &models.HistoryMessage{
		MessageID: "m1", TenantID: "t1", SessionID: "s1", UserID: "",
		Role: models.RoleSystem, Content: "无用户", CreatedAt: time.Now().UTC(),
	}))
	if c.PendingEntities() != 0 {
		t.Fatalf("pending = %d, want 0 (no user_id dropped)", c.PendingEntities())
	}
}

func TestHistoryProfileConsumerFallbackTalliesRoles(t *testing.T) {
	ctx := context.Background()
	bus := messagebus.NewMemoryBus()
	store := service.NewMemoryProfileStore(storage.NewMockInserter())
	// Extract 失败触发 fallback，验证 role 统计
	ex := stubExtractor{err: errors.New("llm down")}

	c := NewHistoryProfileConsumer(bus, ex, store, HistoryProfileConsumerOptions{Topic: "h", MaxMessagesPerUser: 500})
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Stop(ctx) }()

	_ = bus.Send(ctx, "h", "", mustJSON(t, &models.HistoryMessage{
		MessageID: "m1", TenantID: "t1", SessionID: "s1", UserID: "u1",
		Role: models.RoleUser, Content: "问", CreatedAt: time.Now().UTC(),
	}))
	_ = bus.Send(ctx, "h", "", mustJSON(t, &models.HistoryMessage{
		MessageID: "m2", TenantID: "t1", SessionID: "s1", UserID: "u1",
		Role: models.RoleAssistant, Content: "答", CreatedAt: time.Now().UTC(),
	}))

	if _, err := c.RunExtraction(ctx); err != nil {
		t.Fatal(err)
	}
	profile, err := store.GetProfile(ctx, "t1", models.EntityTypeUser, "u1")
	if err != nil || len(profile) == 0 {
		t.Fatalf("profile missing: err=%v profile=%v", err, profile)
	}
	// fallback 应统计出 user=1, assistant=1
	if profile["roles"] != "assistant=1,user=1" {
		t.Errorf("roles = %q", profile["roles"])
	}
}
