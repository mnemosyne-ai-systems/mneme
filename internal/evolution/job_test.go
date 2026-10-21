package evolution

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mnemosyne-ai-systems/mneme/internal/service"
	"github.com/mnemosyne-ai-systems/mneme/pkg/models"
	"github.com/mnemosyne-ai-systems/mneme/pkg/storage"
)

type fakeQueue struct {
	due         []service.EvolutionCandidate
	scheduled   map[string]time.Time
	rescheduled map[string]time.Time
	continued   map[string]time.Time
	forgotten   map[string]bool
	rounds      map[string]int
	claimErr    error
}

func newFakeQueue() *fakeQueue {
	return &fakeQueue{
		scheduled:   make(map[string]time.Time),
		rescheduled: make(map[string]time.Time),
		continued:   make(map[string]time.Time),
		forgotten:   make(map[string]bool),
		rounds:      make(map[string]int),
	}
}

func queueKey(tenant, session string) string { return tenant + "/" + session }

func (q *fakeQueue) Schedule(_ context.Context, tenant, session string, due time.Time) error {
	q.scheduled[queueKey(tenant, session)] = due
	return nil
}

func (q *fakeQueue) Reschedule(_ context.Context, tenant, session string, due time.Time) error {
	q.rescheduled[queueKey(tenant, session)] = due
	return nil
}

func (q *fakeQueue) Continue(_ context.Context, tenant, session string, due time.Time) error {
	q.continued[queueKey(tenant, session)] = due
	return nil
}

func (q *fakeQueue) ClaimDue(_ context.Context, _ time.Time, _ int) ([]service.EvolutionCandidate, error) {
	if q.claimErr != nil {
		return nil, q.claimErr
	}
	out := q.due
	q.due = nil
	return out, nil
}

func (q *fakeQueue) Finish(_ context.Context, tenant, session string) (int, error) {
	key := queueKey(tenant, session)
	q.rounds[key]++
	return q.rounds[key], nil
}

func (q *fakeQueue) Forget(_ context.Context, tenant, session string) error {
	q.forgotten[queueKey(tenant, session)] = true
	return nil
}

type fakeReflector struct {
	text      string
	err       error
	calls     int
	seen      int
	onReflect func()
}

func (r *fakeReflector) ReflectSummary(_ context.Context, _ string, messages []*models.HistoryMessage) (string, error) {
	r.calls++
	r.seen = len(messages)
	if r.onReflect != nil {
		r.onReflect()
	}
	if r.err != nil {
		return "", r.err
	}
	return r.text, nil
}

type fakeSynthesizer struct {
	text     string
	err      error
	calls    int
	sessions int
}

func (s *fakeSynthesizer) SynthesizeProfile(_ context.Context, _ map[string]string, sessions []*models.SessionSummary) (string, error) {
	s.calls++
	s.sessions = len(sessions)
	if s.err != nil {
		return "", s.err
	}
	return s.text, nil
}

func seedHistory(t *testing.T, inserter *storage.MockInserter, tenant, session string, contents ...string) {
	t.Helper()
	base := time.Now().UTC().Add(-time.Hour)
	msgs := make([]*models.HistoryMessage, 0, len(contents))
	for i, content := range contents {
		msgs = append(msgs, &models.HistoryMessage{
			MessageID: tenant + "-" + session + "-" + content,
			TenantID:  tenant,
			SessionID: session,
			UserID:    "u1",
			Role:      models.RoleUser,
			Content:   content,
			CreatedAt: base.Add(time.Duration(i) * time.Minute),
		})
	}
	if err := inserter.InsertHistory(context.Background(), msgs); err != nil {
		t.Fatalf("seed history: %v", err)
	}
}

func newTestJob(t *testing.T, cfg Config, reflector *fakeReflector, synthesizer *fakeSynthesizer) (*Job, *fakeQueue, *service.MemorySessionSummaryStore, *service.MemoryProfileStore) {
	t.Helper()
	queue := newFakeQueue()
	sessions := service.NewMemorySessionSummaryStore(nil)
	profiles := service.NewMemoryProfileStore(nil)
	history := storage.NewMockInserter()
	job := NewJob(queue, sessions, profiles, history, reflector, synthesizer, cfg)
	return job, queue, sessions, profiles
}

func TestRunOnceEvolvesSessionAndUserSummary(t *testing.T) {
	ctx := context.Background()
	reflector := &fakeReflector{text: "改进后的会话摘要"}
	synthesizer := &fakeSynthesizer{text: "跨会话用户长期摘要"}
	job, queue, sessions, profiles := newTestJob(t, Config{
		MaxRounds: 2, HistoryLimit: 10, MaxUserSessions: 5, UserSummaryEnabled: true,
	}, reflector, synthesizer)

	now := time.Now().UTC()
	if _, err := sessions.SaveSummary(ctx, &models.SessionSummary{
		SessionID: "s1", TenantID: "t1", UserID: "u1",
		Summary: "旧摘要", MessageCount: 1, StartedAt: now, EndedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := profiles.SaveProfile(ctx, "t1", models.EntityTypeUser, "u1", map[string]string{
		"name": "Ada",
	}); err != nil {
		t.Fatal(err)
	}
	seedHistory(t, job.history.(*storage.MockInserter), "t1", "s1", "一", "二", "三")

	queue.due = []service.EvolutionCandidate{{TenantID: "t1", SessionID: "s1"}}
	n, err := job.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if n != 1 {
		t.Fatalf("processed = %d, want 1", n)
	}
	if reflector.calls != 1 || reflector.seen != 3 {
		t.Errorf("reflector calls=%d seen=%d, want 1/3", reflector.calls, reflector.seen)
	}

	got, err := sessions.GetSummary(ctx, "t1", "s1")
	if err != nil || got == nil {
		t.Fatalf("summary missing: %v", err)
	}
	if got.Summary != "改进后的会话摘要" {
		t.Errorf("summary = %q", got.Summary)
	}
	if got.Version != 2 {
		t.Errorf("version = %d, want 2", got.Version)
	}
	if _, ok := queue.continued["t1/s1"]; !ok {
		t.Error("expected next reflection round to be scheduled")
	}
	if synthesizer.calls != 1 || synthesizer.sessions != 1 {
		t.Errorf("synthesizer calls=%d sessions=%d, want 1/1", synthesizer.calls, synthesizer.sessions)
	}
	profile, err := profiles.GetProfile(ctx, "t1", models.EntityTypeUser, "u1")
	if err != nil {
		t.Fatal(err)
	}
	if profile[service.ProfileEvolvingSummaryKey] != "跨会话用户长期摘要" {
		t.Errorf("evolving summary = %q", profile[service.ProfileEvolvingSummaryKey])
	}
	if profile["name"] != "Ada" {
		t.Errorf("existing profile field lost: %v", profile)
	}
}

func TestRunOnceSkipsWhenSummaryConverged(t *testing.T) {
	ctx := context.Background()
	reflector := &fakeReflector{text: "旧摘要"}
	synthesizer := &fakeSynthesizer{text: "不应写入"}
	job, queue, sessions, _ := newTestJob(t, Config{
		MaxRounds: 2, HistoryLimit: 10, UserSummaryEnabled: true,
	}, reflector, synthesizer)

	now := time.Now().UTC()
	if _, err := sessions.SaveSummary(ctx, &models.SessionSummary{
		SessionID: "s1", TenantID: "t1", UserID: "u1",
		Summary: "旧摘要", MessageCount: 3, StartedAt: now, EndedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	seedHistory(t, job.history.(*storage.MockInserter), "t1", "s1", "一", "二")

	queue.due = []service.EvolutionCandidate{{TenantID: "t1", SessionID: "s1"}}
	n, err := job.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if n != 0 {
		t.Fatalf("processed = %d, want 0", n)
	}
	if !queue.forgotten["t1/s1"] {
		t.Error("converged session should be forgotten")
	}
	if synthesizer.calls != 0 {
		t.Errorf("synthesizer calls = %d, want 0", synthesizer.calls)
	}
	got, _ := sessions.GetSummary(ctx, "t1", "s1")
	if got.Version != 1 {
		t.Errorf("version = %d, want 1 (unchanged)", got.Version)
	}
}

func TestRunOnceDiscardsReflectionWhenSummaryChanged(t *testing.T) {
	ctx := context.Background()
	reflector := &fakeReflector{text: "基于旧摘要的改写"}
	job, queue, sessions, _ := newTestJob(t, Config{MaxRounds: 2, HistoryLimit: 10}, reflector, &fakeSynthesizer{})

	now := time.Now().UTC()
	if _, err := sessions.SaveSummary(ctx, &models.SessionSummary{
		SessionID: "s1", TenantID: "t1", UserID: "u1",
		Summary: "旧摘要", MessageCount: 2, StartedAt: now, EndedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	seedHistory(t, job.history.(*storage.MockInserter), "t1", "s1", "一", "二")

	reflector.onReflect = func() {
		if _, err := sessions.SaveSummary(ctx, &models.SessionSummary{
			SessionID: "s1", TenantID: "t1", UserID: "u1",
			Summary: "并发更新的摘要", MessageCount: 2, StartedAt: now, EndedAt: now,
		}); err != nil {
			t.Fatalf("concurrent save: %v", err)
		}
	}

	queue.due = []service.EvolutionCandidate{{TenantID: "t1", SessionID: "s1"}}
	n, err := job.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if n != 0 {
		t.Fatalf("processed = %d, want 0", n)
	}
	got, _ := sessions.GetSummary(ctx, "t1", "s1")
	if got.Summary != "并发更新的摘要" {
		t.Errorf("concurrent summary overwritten: %q", got.Summary)
	}
	if _, ok := queue.continued["t1/s1"]; !ok {
		t.Error("conflicted session should be rescheduled")
	}
	if queue.forgotten["t1/s1"] {
		t.Error("conflicted session should not be forgotten")
	}
}

func TestRunOnceReschedulesOnReflectionError(t *testing.T) {
	ctx := context.Background()
	reflector := &fakeReflector{err: errors.New("llm down")}
	job, queue, sessions, _ := newTestJob(t, Config{MaxRounds: 2, HistoryLimit: 10}, reflector, &fakeSynthesizer{})

	now := time.Now().UTC()
	if _, err := sessions.SaveSummary(ctx, &models.SessionSummary{
		SessionID: "s1", TenantID: "t1", UserID: "u1",
		Summary: "旧摘要", MessageCount: 2, StartedAt: now, EndedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	seedHistory(t, job.history.(*storage.MockInserter), "t1", "s1", "一", "二")

	queue.due = []service.EvolutionCandidate{{TenantID: "t1", SessionID: "s1"}}
	n, err := job.RunOnce(ctx)
	if err == nil || !strings.Contains(err.Error(), "reflect summary") {
		t.Fatalf("err = %v, want reflection error", err)
	}
	if n != 0 {
		t.Fatalf("processed = %d, want 0", n)
	}
	if _, ok := queue.rescheduled["t1/s1"]; !ok {
		t.Error("failed session should be rescheduled")
	}
	got, _ := sessions.GetSummary(ctx, "t1", "s1")
	if got.Summary != "旧摘要" {
		t.Errorf("summary changed on failure: %q", got.Summary)
	}
}

func TestRunOnceStopsAfterMaxRounds(t *testing.T) {
	ctx := context.Background()
	reflector := &fakeReflector{text: "新摘要"}
	job, queue, sessions, _ := newTestJob(t, Config{MaxRounds: 1, HistoryLimit: 10}, reflector, &fakeSynthesizer{})

	now := time.Now().UTC()
	if _, err := sessions.SaveSummary(ctx, &models.SessionSummary{
		SessionID: "s1", TenantID: "t1", UserID: "u1",
		Summary: "旧摘要", MessageCount: 2, StartedAt: now, EndedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	seedHistory(t, job.history.(*storage.MockInserter), "t1", "s1", "一", "二")

	queue.due = []service.EvolutionCandidate{{TenantID: "t1", SessionID: "s1"}}
	if n, err := job.RunOnce(ctx); err != nil || n != 1 {
		t.Fatalf("RunOnce: n=%d err=%v", n, err)
	}
	if !queue.forgotten["t1/s1"] {
		t.Error("session should be forgotten after max rounds")
	}
	if _, ok := queue.continued["t1/s1"]; ok {
		t.Error("session should not be rescheduled after max rounds")
	}
}
