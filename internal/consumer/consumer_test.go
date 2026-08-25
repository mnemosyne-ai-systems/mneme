package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/mnemosyne/mneme/internal/service"
	"github.com/mnemosyne/mneme/pkg/cache"
	"github.com/mnemosyne/mneme/pkg/messagebus"
	"github.com/mnemosyne/mneme/pkg/models"
	"github.com/mnemosyne/mneme/pkg/storage"
)

func mustJSON(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// waitFor 轮询等待条件成立，用于异步 flush 场景下的确定性断言。
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---------- 管道一 ----------

func TestHistoryConsumerWritesAndDedups(t *testing.T) {
	ctx := context.Background()
	bus := messagebus.NewMemoryBus()
	sink := storage.NewMockInserter()
	dedup := cache.NewMemoryBloomFilter()

	c := NewHistoryConsumer(bus, sink, dedup, HistoryConsumerOptions{
		Topic:         "h",
		BatchSize:     2,
		FlushInterval: time.Hour,
	})
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Stop(ctx) }()

	msg := &models.HistoryMessage{
		MessageID: "m1", TenantID: "t", SessionID: "s", Role: models.RoleUser,
		Content: "hi", CreatedAt: time.Now().UTC(),
	}
	if err := bus.Send(ctx, "h", "", mustJSON(t, msg)); err != nil {
		t.Fatal(err)
	}
	if c.Buffered() != 1 {
		t.Fatalf("buffered = %d", c.Buffered())
	}

	// 重复 message_id 被过滤
	if err := bus.Send(ctx, "h", "", mustJSON(t, msg)); err != nil {
		t.Fatal(err)
	}
	if c.Buffered() != 1 {
		t.Errorf("duplicate not filtered, buffered = %d", c.Buffered())
	}

	// 第二条不同 ID，触发定量 flush（异步执行）
	msg2 := *msg
	msg2.MessageID = "m2"
	if err := bus.Send(ctx, "h", "", mustJSON(t, &msg2)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return sink.HistoryCount() == 2 && c.Buffered() == 0 })
	if sink.HistoryCount() != 2 {
		t.Errorf("inserted = %d, want 2", sink.HistoryCount())
	}
	if c.Buffered() != 0 {
		t.Errorf("buffer should be drained, got %d", c.Buffered())
	}
}

func TestHistoryConsumerDropsInvalid(t *testing.T) {
	ctx := context.Background()
	bus := messagebus.NewMemoryBus()
	sink := storage.NewMockInserter()
	c := NewHistoryConsumer(bus, sink, nil, HistoryConsumerOptions{Topic: "h", BatchSize: 1, FlushInterval: time.Hour})
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Stop(ctx) }()

	// 非法 JSON 被丢弃且不影响后续
	_ = bus.Send(ctx, "h", "", []byte("{not json"))
	// 校验失败被丢弃
	_ = bus.Send(ctx, "h", "", mustJSON(t, &models.HistoryMessage{MessageID: "x"}))
	if sink.HistoryCount() != 0 {
		t.Errorf("invalid data should not be written, got %d", sink.HistoryCount())
	}
	if bus.NackCount() != 0 {
		t.Errorf("invalid data should be acked, nack = %d", bus.NackCount())
	}
}

func TestHistoryConsumerRetainsOnInsertFailure(t *testing.T) {
	ctx := context.Background()
	bus := messagebus.NewMemoryBus()
	sink := storage.NewMockInserter()
	sink.Err = errors.New("ch down")

	c := NewHistoryConsumer(bus, sink, nil, HistoryConsumerOptions{Topic: "h", BatchSize: 1, FlushInterval: time.Hour})
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Stop(ctx) }()

	msg := &models.HistoryMessage{MessageID: "m1", TenantID: "t", SessionID: "s", Role: models.RoleUser, Content: "x"}
	_ = bus.Send(ctx, "h", "", mustJSON(t, msg))

	// batch 满触发异步 flush，失败后数据放回缓冲区等待重试。
	waitFor(t, func() bool { return c.Buffered() == 1 })
	if c.Buffered() != 1 {
		t.Errorf("failed batch should stay buffered, got %d", c.Buffered())
	}
	// 失败由缓冲重试兜底，不应再触发 Nack 重投递造成重复处理。
	if bus.NackCount() != 0 {
		t.Errorf("insert failure should not trigger nack, nack = %d", bus.NackCount())
	}
}

func TestHistoryConsumerStopFlushes(t *testing.T) {
	ctx := context.Background()
	bus := messagebus.NewMemoryBus()
	sink := storage.NewMockInserter()
	c := NewHistoryConsumer(bus, sink, nil, HistoryConsumerOptions{Topic: "h", BatchSize: 100, FlushInterval: time.Hour})
	_ = c.Start(ctx)

	_ = bus.Send(ctx, "h", "", mustJSON(t, &models.HistoryMessage{
		MessageID: "m1", TenantID: "t", SessionID: "s", Role: models.RoleUser, Content: "x",
	}))
	if sink.HistoryCount() != 0 {
		t.Fatal("should not flush before threshold")
	}
	if err := c.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if sink.HistoryCount() != 1 {
		t.Errorf("stop should flush, got %d", sink.HistoryCount())
	}
	// 重复 Stop 幂等
	if err := c.Stop(ctx); err != nil {
		t.Error(err)
	}
}

// ---------- 聚合器 ----------

func TestTraceAggregator(t *testing.T) {
	var flushed [][]*models.TraceMessage
	var reasons []string
	agg := NewTraceAggregator(time.Minute, func(_ string, spans []*models.TraceMessage, reason string) {
		flushed = append(flushed, spans)
		reasons = append(reasons, reason)
	})

	base := time.Now()
	agg.now = func() time.Time { return base }

	agg.Add(&models.TraceMessage{RequestID: "r1", TenantID: "t", StepType: models.StepTypeLLMCall, StepName: "a"})
	agg.Add(&models.TraceMessage{RequestID: "r1", TenantID: "t", StepType: models.StepTypeToolCall, StepName: "b"})
	agg.Add(&models.TraceMessage{RequestID: "r2", TenantID: "t", StepType: models.StepTypeReasoning, StepName: "c"})
	agg.Add(nil)

	if agg.PendingRequests() != 2 || agg.PendingSpans() != 3 {
		t.Fatalf("pending = %d/%d", agg.PendingRequests(), agg.PendingSpans())
	}

	// 未超时不刷新
	if n := agg.SweepExpired(); n != 0 {
		t.Errorf("swept = %d, want 0", n)
	}

	// 超时后全部刷新
	agg.now = func() time.Time { return base.Add(2 * time.Minute) }
	if n := agg.SweepExpired(); n != 2 {
		t.Errorf("swept = %d, want 2", n)
	}
	if len(flushed) != 2 {
		t.Fatalf("flushed groups = %d", len(flushed))
	}
	for _, r := range reasons {
		if r != "timeout" {
			t.Errorf("reason = %s", r)
		}
	}
	if agg.PendingRequests() != 0 {
		t.Error("buffer should be empty")
	}
}

func TestTraceAggregatorFlushAllAndRequest(t *testing.T) {
	count := 0
	agg := NewTraceAggregator(0, func(string, []*models.TraceMessage, string) { count++ })

	agg.Add(&models.TraceMessage{RequestID: "r1", TenantID: "t"})
	if !agg.FlushRequest("t:r1", "manual") {
		t.Error("expected flush")
	}
	if agg.FlushRequest("missing", "manual") {
		t.Error("expected no flush for missing key")
	}

	agg.Add(&models.TraceMessage{RequestID: "r2", TenantID: "t"})
	agg.Add(&models.TraceMessage{RequestID: "r3", TenantID: "t"})
	if n := agg.FlushAll("shutdown"); n != 2 {
		t.Errorf("flushed = %d", n)
	}
	if count != 3 {
		t.Errorf("callback count = %d", count)
	}
}

// ---------- 管道二 ----------

func TestTracesConsumerAggregatesAndFlushesOnStop(t *testing.T) {
	ctx := context.Background()
	bus := messagebus.NewMemoryBus()
	sink := storage.NewMockInserter()
	c := NewTracesConsumer(bus, sink, TracesConsumerOptions{Topic: "tr", AggregateTimeout: time.Hour})
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		_ = bus.Send(ctx, "tr", "", mustJSON(t, &models.TraceMessage{
			RequestID: "r1", TenantID: "t", StepType: models.StepTypeLLMCall,
			StepName: "step", Timestamp: time.Now().UTC(),
		}))
	}
	_ = bus.Send(ctx, "tr", "", mustJSON(t, &models.TraceMessage{
		RequestID: "r2", TenantID: "t", StepType: models.StepTypeToolCall, StepName: "s",
	}))
	// 脏数据
	_ = bus.Send(ctx, "tr", "", []byte("bad"))
	_ = bus.Send(ctx, "tr", "", mustJSON(t, &models.TraceMessage{RequestID: ""}))

	if c.Aggregator().PendingRequests() != 2 {
		t.Fatalf("pending requests = %d", c.Aggregator().PendingRequests())
	}
	if sink.TraceCount() != 0 {
		t.Fatal("should not write before aggregation completes")
	}

	// 优雅关闭时未完成聚合的 Span 必须先落盘
	if err := c.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if sink.TraceCount() != 4 {
		t.Errorf("traces written = %d, want 4", sink.TraceCount())
	}
	if sink.Flushes == 0 {
		t.Error("inserter flush should be called on stop")
	}
}

func TestTracesConsumerSweepWrites(t *testing.T) {
	ctx := context.Background()
	bus := messagebus.NewMemoryBus()
	sink := storage.NewMockInserter()
	c := NewTracesConsumer(bus, sink, TracesConsumerOptions{
		Topic: "tr", AggregateTimeout: 30 * time.Millisecond, SweepInterval: 10 * time.Millisecond,
	})
	_ = c.Start(ctx)
	defer func() { _ = c.Stop(ctx) }()

	_ = bus.Send(ctx, "tr", "", mustJSON(t, &models.TraceMessage{
		RequestID: "r1", TenantID: "t", StepType: models.StepTypeLLMCall, StepName: "s",
	}))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if sink.TraceCount() > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("timeout waiting for sweep flush")
}

// ---------- 管道三 ----------

type stubExtractor struct {
	profile map[string]string
	err     error
}

func (s stubExtractor) Extract(context.Context, []*models.MemoryEvent) (map[string]string, error) {
	return s.profile, s.err
}

func (s stubExtractor) Fallback(_ context.Context, events []*models.MemoryEvent) (map[string]string, error) {
	return service.BuildFallbackProfile(events), nil
}

func TestMemoryConsumerExtractionAndVersioning(t *testing.T) {
	ctx := context.Background()
	bus := messagebus.NewMemoryBus()
	sink := storage.NewMockInserter()
	store := service.NewMemoryProfileStore(sink)
	ex := stubExtractor{profile: map[string]string{"lang": "zh"}}

	c := NewMemoryConsumer(bus, ex, store, MemoryConsumerOptions{Topic: "mem", ExtractInterval: time.Hour})
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Stop(ctx) }()

	evt := &models.MemoryEvent{
		EventID: "e1", EntityID: "u1", TenantID: "t1",
		EntityType: models.EntityTypeUser, EventType: "pref",
		Payload: map[string]interface{}{"lang": "zh"}, Timestamp: time.Now().UTC(),
	}
	_ = bus.Send(ctx, "mem", "", mustJSON(t, evt))
	_ = bus.Send(ctx, "mem", "", []byte("bad"))
	_ = bus.Send(ctx, "mem", "", mustJSON(t, &models.MemoryEvent{EventID: "x"}))

	if c.PendingEntities() != 1 {
		t.Fatalf("pending = %d", c.PendingEntities())
	}

	n, err := c.RunExtraction(ctx)
	if err != nil || n != 1 {
		t.Fatalf("extraction: n=%d err=%v", n, err)
	}
	profile, _ := store.GetProfile(ctx, "t1", models.EntityTypeUser, "u1")
	if profile["lang"] != "zh" {
		t.Errorf("profile = %v", profile)
	}
	if profile["_version"] != "1" {
		t.Errorf("version = %s", profile["_version"])
	}
	if sink.MemoryCount() != 1 {
		t.Errorf("clickhouse rows = %d", sink.MemoryCount())
	}

	// 第二次萃取，版本递增
	evt2 := *evt
	evt2.EventID = "e2"
	_ = bus.Send(ctx, "mem", "", mustJSON(t, &evt2))
	if _, err := c.RunExtraction(ctx); err != nil {
		t.Fatal(err)
	}
	profile, _ = store.GetProfile(ctx, "t1", models.EntityTypeUser, "u1")
	if profile["_version"] != "2" {
		t.Errorf("version = %s, want 2", profile["_version"])
	}
	if sink.Memories[1].Version != 2 {
		t.Errorf("clickhouse version = %d", sink.Memories[1].Version)
	}
}

func TestMemoryConsumerFallbackOnLLMFailure(t *testing.T) {
	ctx := context.Background()
	bus := messagebus.NewMemoryBus()
	store := service.NewMemoryProfileStore(nil)
	ex := stubExtractor{err: errors.New("llm timeout")}

	c := NewMemoryConsumer(bus, ex, store, MemoryConsumerOptions{Topic: "mem", ExtractInterval: time.Hour})
	_ = c.Start(ctx)
	defer func() { _ = c.Stop(ctx) }()

	_ = bus.Send(ctx, "mem", "", mustJSON(t, &models.MemoryEvent{
		EventID: "e1", EntityID: "u1", TenantID: "t1",
		EntityType: models.EntityTypeUser, EventType: "pref", Timestamp: time.Now().UTC(),
	}))

	if _, err := c.RunExtraction(ctx); err != nil {
		t.Fatal(err)
	}
	profile, _ := store.GetProfile(ctx, "t1", models.EntityTypeUser, "u1")
	if profile["_extracted_by"] != "fallback" {
		t.Errorf("expected fallback profile, got %v", profile)
	}
	if profile["event_count"] != "1" {
		t.Errorf("event_count = %s", profile["event_count"])
	}
}

func TestMemoryConsumerRetriesOnStoreFailure(t *testing.T) {
	ctx := context.Background()
	bus := messagebus.NewMemoryBus()
	sink := storage.NewMockInserter()
	sink.Err = errors.New("down")
	store := service.NewMemoryProfileStore(sink)

	c := NewMemoryConsumer(bus, stubExtractor{profile: map[string]string{"a": "b"}}, store,
		MemoryConsumerOptions{Topic: "mem", ExtractInterval: time.Hour})
	_ = c.Start(ctx)
	defer func() { _ = c.Stop(ctx) }()

	_ = bus.Send(ctx, "mem", "", mustJSON(t, &models.MemoryEvent{
		EventID: "e1", EntityID: "u1", TenantID: "t1",
		EntityType: models.EntityTypeUser, EventType: "pref",
	}))

	if _, err := c.RunExtraction(ctx); err == nil {
		t.Fatal("expected error")
	}
	if c.PendingEntities() != 1 {
		t.Errorf("events should be retained for retry, pending = %d", c.PendingEntities())
	}
}

func TestMemoryConsumerMaxEventsPerEntity(t *testing.T) {
	ctx := context.Background()
	bus := messagebus.NewMemoryBus()
	store := service.NewMemoryProfileStore(nil)
	var captured int
	ex := funcExtractor(func(events []*models.MemoryEvent) map[string]string {
		captured = len(events)
		return map[string]string{"n": "1"}
	})

	c := NewMemoryConsumer(bus, ex, store, MemoryConsumerOptions{
		Topic: "mem", ExtractInterval: time.Hour, MaxEventsPerEntity: 3,
	})
	_ = c.Start(ctx)
	defer func() { _ = c.Stop(ctx) }()

	for i := 0; i < 10; i++ {
		_ = bus.Send(ctx, "mem", "", mustJSON(t, &models.MemoryEvent{
			EventID: models.NewID(), EntityID: "u1", TenantID: "t",
			EntityType: models.EntityTypeUser, EventType: "e",
		}))
	}
	if _, err := c.RunExtraction(ctx); err != nil {
		t.Fatal(err)
	}
	if captured != 3 {
		t.Errorf("captured events = %d, want 3", captured)
	}
}

type funcExtractor func([]*models.MemoryEvent) map[string]string

func (f funcExtractor) Extract(_ context.Context, e []*models.MemoryEvent) (map[string]string, error) {
	return f(e), nil
}

func (f funcExtractor) Fallback(_ context.Context, e []*models.MemoryEvent) (map[string]string, error) {
	return f(e), nil
}

func TestTracesConsumerRebuffersOnInsertFailure(t *testing.T) {
	ctx := context.Background()
	bus := messagebus.NewMemoryBus()
	sink := storage.NewMockInserter()
	sink.Err = errors.New("ch down")

	c := NewTracesConsumer(bus, sink, TracesConsumerOptions{Topic: "tr", AggregateTimeout: time.Hour})
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Stop(ctx) }()

	_ = bus.Send(ctx, "tr", "", mustJSON(t, &models.TraceMessage{
		RequestID: "r1", TenantID: "t", StepType: models.StepTypeLLMCall, StepName: "s",
	}))
	// 触发一次落盘（失败）
	c.Aggregator().FlushAll("timeout")

	if sink.TraceCount() != 0 {
		t.Fatal("insert should have failed")
	}
	if got := c.Aggregator().PendingSpans(); got != 1 {
		t.Errorf("failed spans should be re-buffered, pending = %d", got)
	}

	// 修复后重试成功
	sink.Err = nil
	c.Aggregator().FlushAll("timeout")
	if sink.TraceCount() != 1 {
		t.Errorf("re-buffered spans should flush after recovery, got %d", sink.TraceCount())
	}
}

// TestConsumersStopWithoutStart 验证未调用 Start 时 Stop 不会死锁。
func TestConsumersStopWithoutStart(t *testing.T) {
	ctx := context.Background()
	bus := messagebus.NewMemoryBus()
	sink := storage.NewMockInserter()

	done := make(chan struct{})
	go func() {
		defer close(done)
		hc := NewHistoryConsumer(bus, sink, nil, HistoryConsumerOptions{Topic: "h"})
		if err := hc.Stop(ctx); err != nil {
			t.Errorf("history stop: %v", err)
		}
		tc := NewTracesConsumer(bus, sink, TracesConsumerOptions{Topic: "t"})
		if err := tc.Stop(ctx); err != nil {
			t.Errorf("traces stop: %v", err)
		}
		mc := NewMemoryConsumer(bus, stubExtractor{}, service.NewMemoryProfileStore(nil), MemoryConsumerOptions{Topic: "m"})
		if err := mc.Stop(ctx); err != nil {
			t.Errorf("memory stop: %v", err)
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop without Start deadlocked")
	}
}

func TestConsumerOptionDefaults(t *testing.T) {
	h := HistoryConsumerOptions{}.withDefaults()
	if h.Topic != "mneme-history" || h.BatchSize != 10000 || h.FlushInterval != 5*time.Second {
		t.Errorf("history defaults wrong: %+v", h)
	}
	tr := TracesConsumerOptions{}.withDefaults()
	if tr.Topic != "mneme-traces" || tr.AggregateTimeout != 30*time.Second || tr.SweepInterval != 10*time.Second {
		t.Errorf("traces defaults wrong: %+v", tr)
	}
	m := MemoryConsumerOptions{}.withDefaults()
	if m.Topic != "mneme-memory" || m.ExtractInterval != 10*time.Minute || m.MaxEventsPerEntity != 500 {
		t.Errorf("memory defaults wrong: %+v", m)
	}
}
