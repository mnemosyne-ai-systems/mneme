package consumer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mnemosyne-ai-systems/mneme/internal/service"
	"github.com/mnemosyne-ai-systems/mneme/pkg/messagebus"
	"github.com/mnemosyne-ai-systems/mneme/pkg/models"
	"github.com/mnemosyne-ai-systems/mneme/pkg/storage"
)

// stubSummarizer 测试用会话总结器。
type stubSummarizer struct {
	text string
	err  error
}

func (s stubSummarizer) Summarize(_ context.Context, msgs []*models.HistoryMessage) (string, error) {
	if s.err != nil {
		return "", s.err
	}
	if s.text != "" {
		return s.text, nil
	}
	return "对话共 " + time.Now().Format("15:04") + " 条消息", nil
}

func (s stubSummarizer) SummaryFallback(_ context.Context, msgs []*models.HistoryMessage) string {
	return "（回退摘要）共 " + itoa(len(msgs)) + " 条消息"
}

func (s stubSummarizer) RefineSummary(_ context.Context, prev string, msgs []*models.HistoryMessage) (string, error) {
	if s.err != nil {
		return "", s.err
	}
	if s.text != "" {
		return s.text, nil
	}
	return prev + " | 新增 " + itoa(len(msgs)) + " 条", nil
}

func (s stubSummarizer) SummaryRefineFallback(_ context.Context, prev string, msgs []*models.HistoryMessage) string {
	if prev == "" {
		return "（回退摘要）共 " + itoa(len(msgs)) + " 条消息"
	}
	return prev + "\n（新增 " + itoa(len(msgs)) + " 条）"
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}

func TestSessionSummaryConsumerSummarizesPerSession(t *testing.T) {
	ctx := context.Background()
	bus := messagebus.NewMemoryBus()
	sink := storage.NewMockInserter()
	store := service.NewMemorySessionSummaryStore(sink)
	s := stubSummarizer{}

	c := NewSessionSummaryConsumer(bus, s, store, SessionSummaryConsumerOptions{
		Topic: "h", SummarizeInterval: time.Hour, MaxMessagesPerSession: 2000,
	})
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Stop(ctx) }()

	_ = bus.Send(ctx, "h", "", mustJSON(t, &models.HistoryMessage{
		MessageID: "m1", TenantID: "t1", SessionID: "s1", UserID: "u1",
		Role: models.RoleUser, Content: "你好", CreatedAt: time.Now().UTC().Add(-time.Minute),
	}))
	_ = bus.Send(ctx, "h", "", mustJSON(t, &models.HistoryMessage{
		MessageID: "m2", TenantID: "t1", SessionID: "s1", UserID: "u1",
		Role: models.RoleAssistant, Content: "回复", CreatedAt: time.Now().UTC(),
	}))
	_ = bus.Send(ctx, "h", "", mustJSON(t, &models.HistoryMessage{
		MessageID: "m3", TenantID: "t1", SessionID: "s2", UserID: "u1",
		Role: models.RoleUser, Content: "另一个会话", CreatedAt: time.Now().UTC(),
	}))

	if c.PendingSessions() != 2 {
		t.Fatalf("pending = %d, want 2", c.PendingSessions())
	}

	n, err := c.RunSummarization(ctx)
	if err != nil || n != 2 {
		t.Fatalf("summarization: n=%d err=%v", n, err)
	}

	sum1, err := store.GetSummary(ctx, "t1", "s1")
	if err != nil || sum1 == nil {
		t.Fatalf("s1 summary missing: err=%v sum=%v", err, sum1)
	}
	if sum1.MessageCount != 2 {
		t.Errorf("s1 message_count = %d, want 2", sum1.MessageCount)
	}
	if sum1.UserID != "u1" {
		t.Errorf("s1 user_id = %q", sum1.UserID)
	}
	if sum1.Version != 1 {
		t.Errorf("s1 version = %d, want 1", sum1.Version)
	}
	sum2, err := store.GetSummary(ctx, "t1", "s2")
	if err != nil || sum2 == nil || sum2.MessageCount != 1 {
		t.Fatalf("s2 summary wrong: err=%v sum=%v", err, sum2)
	}
	if sink.SummaryCount() != 2 {
		t.Errorf("clickhouse summary rows = %d, want 2", sink.SummaryCount())
	}
}

func TestSessionSummaryConsumerSkipsMessagesWithoutSession(t *testing.T) {
	ctx := context.Background()
	bus := messagebus.NewMemoryBus()
	store := service.NewMemorySessionSummaryStore(storage.NewMockInserter())
	s := stubSummarizer{}

	c := NewSessionSummaryConsumer(bus, s, store, SessionSummaryConsumerOptions{Topic: "h"})
	_ = bus.Send(ctx, "h", "", mustJSON(t, &models.HistoryMessage{
		MessageID: "m1", TenantID: "t1", SessionID: "", UserID: "u1",
		Role: models.RoleUser, Content: "无会话", CreatedAt: time.Now().UTC(),
	}))
	if c.PendingSessions() != 0 {
		t.Fatalf("pending = %d, want 0 (no session_id dropped)", c.PendingSessions())
	}
}

func TestSessionSummaryConsumerFallbackOnError(t *testing.T) {
	ctx := context.Background()
	bus := messagebus.NewMemoryBus()
	store := service.NewMemorySessionSummaryStore(storage.NewMockInserter())
	// Summarize 失败触发 fallback
	s := stubSummarizer{err: errors.New("llm down")}

	c := NewSessionSummaryConsumer(bus, s, store, SessionSummaryConsumerOptions{Topic: "h", MaxMessagesPerSession: 2000})
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Stop(ctx) }()

	_ = bus.Send(ctx, "h", "", mustJSON(t, &models.HistoryMessage{
		MessageID: "m1", TenantID: "t1", SessionID: "s1", UserID: "u1",
		Role: models.RoleUser, Content: "问题", CreatedAt: time.Now().UTC(),
	}))

	if _, err := c.RunSummarization(ctx); err != nil {
		t.Fatal(err)
	}
	sum, err := store.GetSummary(ctx, "t1", "s1")
	if err != nil || sum == nil {
		t.Fatalf("summary missing: err=%v sum=%v", err, sum)
	}
	if !strings.Contains(sum.Summary, "回退摘要") {
		t.Errorf("summary = %q, want fallback text", sum.Summary)
	}
}

func TestSessionSummaryConsumerIncrementalRefines(t *testing.T) {
	ctx := context.Background()
	bus := messagebus.NewMemoryBus()
	store := service.NewMemorySessionSummaryStore(storage.NewMockInserter())
	s := stubSummarizer{}

	c := NewSessionSummaryConsumer(bus, s, store, SessionSummaryConsumerOptions{Topic: "h", MaxMessagesPerSession: 2000})
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Stop(ctx) }()

	now := time.Now().UTC()
	_ = bus.Send(ctx, "h", "", mustJSON(t, &models.HistoryMessage{
		MessageID: "m1", TenantID: "t1", SessionID: "s1", UserID: "u1",
		Role: models.RoleUser, Content: "第一轮", CreatedAt: now,
	}))
	_ = bus.Send(ctx, "h", "", mustJSON(t, &models.HistoryMessage{
		MessageID: "m2", TenantID: "t1", SessionID: "s1", UserID: "u1",
		Role: models.RoleAssistant, Content: "回复一", CreatedAt: now.Add(time.Second),
	}))

	if _, err := c.RunSummarization(ctx); err != nil {
		t.Fatal(err)
	}
	first, err := store.GetSummary(ctx, "t1", "s1")
	if err != nil || first == nil || first.MessageCount != 2 {
		t.Fatalf("first summary: err=%v sum=%+v", err, first)
	}

	// 第二轮新增 3 条：应走增量 refine，MessageCount 累加为 5
	_ = bus.Send(ctx, "h", "", mustJSON(t, &models.HistoryMessage{
		MessageID: "m3", TenantID: "t1", SessionID: "s1", UserID: "u1",
		Role: models.RoleUser, Content: "第二轮", CreatedAt: now.Add(2 * time.Second),
	}))
	_ = bus.Send(ctx, "h", "", mustJSON(t, &models.HistoryMessage{
		MessageID: "m4", TenantID: "t1", SessionID: "s1", UserID: "u1",
		Role: models.RoleAssistant, Content: "回复二", CreatedAt: now.Add(3 * time.Second),
	}))
	_ = bus.Send(ctx, "h", "", mustJSON(t, &models.HistoryMessage{
		MessageID: "m5", TenantID: "t1", SessionID: "s1", UserID: "u1",
		Role: models.RoleUser, Content: "追问", CreatedAt: now.Add(4 * time.Second),
	}))

	if _, err := c.RunSummarization(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := store.GetSummary(ctx, "t1", "s1")
	if err != nil || second == nil {
		t.Fatalf("second summary: err=%v sum=%+v", err, second)
	}
	if second.MessageCount != 5 {
		t.Errorf("MessageCount = %d, want 5 (incremental accumulation)", second.MessageCount)
	}
	if second.Version != 2 {
		t.Errorf("Version = %d, want 2", second.Version)
	}
	if !strings.Contains(second.Summary, "新增 3 条") {
		t.Errorf("summary not refined: %q", second.Summary)
	}
}
