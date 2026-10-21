package service

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/mnemosyne-ai-systems/mneme/pkg/cache"
	"github.com/mnemosyne-ai-systems/mneme/pkg/models"
)

func newTestRedisStore(t *testing.T) (*RedisSessionSummaryStore, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewRedisSessionSummaryStore(client, nil, 24*time.Hour, true), client
}

func saveRedisTestSession(t *testing.T, ctx context.Context, store *RedisSessionSummaryStore, tenant, sessionID, userID, summary string, endedAt time.Time) {
	t.Helper()
	if _, err := store.SaveSummary(ctx, &models.SessionSummary{
		SessionID: sessionID, TenantID: tenant, UserID: userID,
		Summary: summary, MessageCount: 1,
		StartedAt: endedAt.Add(-time.Minute), EndedAt: endedAt,
	}); err != nil {
		t.Fatalf("save summary: %v", err)
	}
}

func sessionIDs(list []*models.SessionSummary) []string {
	ids := make([]string, 0, len(list))
	for _, s := range list {
		ids = append(ids, s.SessionID)
	}
	return ids
}

func assertIDs(t *testing.T, got []*models.SessionSummary, want ...string) {
	t.Helper()
	ids := sessionIDs(got)
	if len(ids) != len(want) {
		t.Fatalf("session ids = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("session ids = %v, want %v", ids, want)
		}
	}
}

func TestRedisListSessionsByUserUsesIndex(t *testing.T) {
	ctx := context.Background()
	store, client := newTestRedisStore(t)
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	saveRedisTestSession(t, ctx, store, "t1", "s1", "u1", "one", base)
	saveRedisTestSession(t, ctx, store, "t1", "s2", "u2", "two", base.Add(time.Hour))
	saveRedisTestSession(t, ctx, store, "t1", "s3", "u1", "three", base.Add(2*time.Hour))

	if ok, err := client.Exists(ctx, cache.UserSessionsIndexReadyKey("t1")).Result(); err != nil || ok != 0 {
		t.Fatalf("ready marker should be absent before first read (ok=%d err=%v)", ok, err)
	}
	list, err := store.ListSessionsByUser(ctx, "t1", "u1")
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}
	assertIDs(t, list, "s3", "s1")
	if ok, err := client.Exists(ctx, cache.UserSessionsIndexReadyKey("t1")).Result(); err != nil || ok != 1 {
		t.Fatalf("ready marker should be set after backfill (ok=%d err=%v)", ok, err)
	}

	// 租户就绪标记已立后，直接往 Redis 塞一条不走索引的裸 Hash：
	// 查询空用户/新用户应直接读 ZSET，不应再触发全量 SCAN 把裸 Hash 扫出来。
	if err := client.HSet(ctx, cache.SessionKey("t1", "unindexed"), map[string]interface{}{
		"session_id": "unindexed",
		"tenant_id":  "t1",
		"user_id":    "u_empty",
		"summary":    "raw",
		"started_at": base.Format(time.RFC3339),
		"ended_at":   base.Format(time.RFC3339),
	}).Err(); err != nil {
		t.Fatal(err)
	}
	empty, err := store.ListSessionsByUser(ctx, "t1", "u_empty")
	if err != nil {
		t.Fatalf("list empty user: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("expected ZSET read (0 results without SCAN), got %v", sessionIDs(empty))
	}
}

func TestRedisListSessionsByUserBackfillsLegacyData(t *testing.T) {
	ctx := context.Background()
	store, client := newTestRedisStore(t)
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	// 模拟启用每用户索引前写入的历史会话（只有 Hash，没有索引）。
	if err := client.HSet(ctx, cache.SessionKey("t1", "legacy"), map[string]interface{}{
		"session_id":    "legacy",
		"tenant_id":     "t1",
		"user_id":       "u1",
		"summary":       "legacy summary",
		"message_count": 2,
		"started_at":    base.Add(-time.Minute).Format(time.RFC3339),
		"ended_at":      base.Format(time.RFC3339),
		"_version":      1,
	}).Err(); err != nil {
		t.Fatalf("seed legacy summary: %v", err)
	}
	// 新写入会创建索引 Key（只包含新会话），但标记未设置。
	saveRedisTestSession(t, ctx, store, "t1", "s2", "u1", "new", base.Add(time.Hour))

	list, err := store.ListSessionsByUser(ctx, "t1", "u1")
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}
	assertIDs(t, list, "s2", "legacy")
	if ok, err := client.Exists(ctx, cache.UserSessionsIndexReadyKey("t1")).Result(); err != nil || ok != 1 {
		t.Fatalf("ready marker should be set after backfill (ok=%d err=%v)", ok, err)
	}
}

func TestRedisPurgeSessionRemovesIndexes(t *testing.T) {
	ctx := context.Background()
	store, client := newTestRedisStore(t)
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	saveRedisTestSession(t, ctx, store, "t1", "s1", "u1", "one", base)
	saveRedisTestSession(t, ctx, store, "t1", "s2", "u1", "two", base.Add(time.Hour))
	if err := store.PurgeSession(ctx, "t1", "s1"); err != nil {
		t.Fatalf("purge session: %v", err)
	}
	for _, key := range []string{
		cache.SessionsIndexKey("t1"),
		cache.UserSessionsIndexKey("t1", "u1"),
	} {
		if _, err := client.ZScore(ctx, key, "s1").Result(); err != redis.Nil {
			t.Errorf("member s1 still present in %s (err=%v)", key, err)
		}
	}
	list, err := store.ListSessionsByUser(ctx, "t1", "u1")
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}
	assertIDs(t, list, "s2")

	if err := store.PurgeSessionsByUser(ctx, "t1", "u1"); err != nil {
		t.Fatalf("purge user sessions: %v", err)
	}
	if ok, err := client.Exists(ctx, cache.UserSessionsIndexKey("t1", "u1")).Result(); err != nil || ok != 0 {
		t.Errorf("user index should be removed (ok=%d err=%v)", ok, err)
	}
}

func TestRedisSearchSessionsByUserFiltersAndPaginates(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestRedisStore(t)
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	saveRedisTestSession(t, ctx, store, "t1", "s1", "u1", "alpha", base)
	saveRedisTestSession(t, ctx, store, "t1", "s2", "u1", "beta", base.Add(time.Hour))
	saveRedisTestSession(t, ctx, store, "t1", "s3", "u1", "alpha beta", base.Add(2*time.Hour))
	saveRedisTestSession(t, ctx, store, "t1", "s4", "u2", "alpha", base.Add(3*time.Hour))

	page, next, err := store.SearchSessions(ctx, "t1", SessionSearchOptions{UserID: "u1", Query: "alpha", Limit: 1})
	if err != nil {
		t.Fatalf("search sessions: %v", err)
	}
	assertIDs(t, page, "s3")
	if next == "" {
		t.Fatal("expected next cursor")
	}

	page, next, err = store.SearchSessions(ctx, "t1", SessionSearchOptions{UserID: "u1", Query: "alpha", Limit: 1, Cursor: next})
	if err != nil {
		t.Fatalf("search sessions page 2: %v", err)
	}
	assertIDs(t, page, "s1")
	if next != "" {
		t.Errorf("unexpected next cursor %q", next)
	}

	// 跨界时间过滤：s2 started_at = base+59m <= endTime(base+59m30s) < ended_at(base+1h)，应被命中。
	spanning, _, err := store.SearchSessions(ctx, "t1", SessionSearchOptions{
		UserID:    "u1",
		StartTime: base.Add(30 * time.Minute).Format(time.RFC3339),
		EndTime:   base.Add(59*time.Minute + 30*time.Second).Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("search spanning session: %v", err)
	}
	assertIDs(t, spanning, "s2")
}

func TestRedisRebuildSessionsIndexRebuildsUserIndexes(t *testing.T) {
	ctx := context.Background()
	store, client := newTestRedisStore(t)
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	saveRedisTestSession(t, ctx, store, "t1", "s1", "u1", "one", base)
	saveRedisTestSession(t, ctx, store, "t1", "s2", "u1", "two", base.Add(time.Hour))
	saveRedisTestSession(t, ctx, store, "t1", "s3", "u2", "three", base.Add(2*time.Hour))
	if err := client.ZAdd(ctx, cache.UserSessionsIndexKey("t1", "u1"), redis.Z{Score: 1, Member: "ghost"}).Err(); err != nil {
		t.Fatalf("seed ghost member: %v", err)
	}

	n, err := store.RebuildSessionsIndex(ctx, "t1")
	if err != nil {
		t.Fatalf("rebuild index: %v", err)
	}
	if n != 3 {
		t.Fatalf("rebuilt sessions = %d, want 3", n)
	}
	if card, err := client.ZCard(ctx, cache.UserSessionsIndexKey("t1", "u1")).Result(); err != nil || card != 2 {
		t.Errorf("user index card = %d, want 2 (err=%v)", card, err)
	}
	if _, err := client.ZScore(ctx, cache.UserSessionsIndexKey("t1", "u1"), "ghost").Result(); err != redis.Nil {
		t.Errorf("ghost member survived rebuild (err=%v)", err)
	}
	if ok, err := client.Exists(ctx, cache.UserSessionsIndexReadyKey("t1")).Result(); err != nil || ok != 1 {
		t.Errorf("ready marker for t1 = %d, want 1 (err=%v)", ok, err)
	}
}
