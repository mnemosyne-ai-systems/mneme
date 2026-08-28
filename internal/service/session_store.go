package service

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mnemosyne/mneme/pkg/cache"
	"github.com/mnemosyne/mneme/pkg/models"
	"github.com/mnemosyne/mneme/pkg/storage"
)

// SessionSummaryStore 负责会话总结的热存储（Redis）与冷存储（ClickHouse）。
type SessionSummaryStore interface {
	SaveSummary(ctx context.Context, summary *models.SessionSummary) (int64, error)
	GetSummary(ctx context.Context, tenant, sessionID string) (*models.SessionSummary, error)
	// ListSessionsByUser 返回某租户下属于指定用户的全部会话总结。
	ListSessionsByUser(ctx context.Context, tenant, userID string) ([]*models.SessionSummary, error)
	// SearchSessions 检索会话总结（管理后台用），支持按 user_id 与关键字过滤。
	SearchSessions(ctx context.Context, tenant string, opts SessionSearchOptions) ([]*models.SessionSummary, error)
	// DeleteSession 删除单个会话总结（数据治理）。
	DeleteSession(ctx context.Context, tenant, sessionID string) error
	// DeleteSessionsByUser 删除某用户下的全部会话总结（数据治理）。
	DeleteSessionsByUser(ctx context.Context, tenant, userID string) error
}

// SessionSearchOptions 会话检索选项。
type SessionSearchOptions struct {
	// UserID 按用户过滤（可选）。
	UserID string
	// Query 关键字，匹配 session_id 或 summary 子串（可选）。
	Query string
	// Limit 返回条数上限，<=0 表示默认 100。
	Limit int
}

// RedisSessionSummaryStore 使用 Redis Hash 保存会话总结，并把带版本号的快照写入 ClickHouse。
type RedisSessionSummaryStore struct {
	client   redis.UniversalClient
	inserter storage.BatchInserter
	ttl      time.Duration
}

// NewRedisSessionSummaryStore 创建会话总结存储。inserter 可为 nil（仅写 Redis）。
func NewRedisSessionSummaryStore(client redis.UniversalClient, inserter storage.BatchInserter, ttl time.Duration) *RedisSessionSummaryStore {
	return &RedisSessionSummaryStore{client: client, inserter: inserter, ttl: ttl}
}

// SaveSummary 写入 Redis Hash 与 ClickHouse，返回新的版本号。
func (s *RedisSessionSummaryStore) SaveSummary(ctx context.Context, summary *models.SessionSummary) (int64, error) {
	if err := summary.Validate(); err != nil {
		return 0, err
	}
	key := cache.SessionKey(summary.TenantID, summary.SessionID)
	verKey := cache.SessionVersionKey(summary.TenantID, summary.SessionID)

	version, err := s.client.Incr(ctx, verKey).Result()
	if err != nil {
		return 0, fmt.Errorf("session store: incr version: %w", err)
	}
	summary.Version = version
	summary.UpdatedAt = time.Now().UTC()

	fields := map[string]interface{}{
		"session_id":    summary.SessionID,
		"tenant_id":     summary.TenantID,
		"user_id":       summary.UserID,
		"summary":       summary.Summary,
		"message_count": summary.MessageCount,
		"started_at":    summary.StartedAt.UTC().Format(time.RFC3339),
		"ended_at":      summary.EndedAt.UTC().Format(time.RFC3339),
		"_version":      version,
		"_updated_at":   summary.UpdatedAt.UTC().Format(time.RFC3339),
	}

	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, key, fields)
	if s.ttl > 0 {
		pipe.Expire(ctx, key, s.ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return version, fmt.Errorf("session store: hset: %w", err)
	}

	if s.inserter != nil {
		if err := s.inserter.InsertSessionSummary(ctx, []*models.SessionSummary{summary}); err != nil {
			return version, fmt.Errorf("session store: insert clickhouse: %w", err)
		}
	}
	return version, nil
}

// GetSummary 从 Redis 读取会话总结。
func (s *RedisSessionSummaryStore) GetSummary(ctx context.Context, tenant, sessionID string) (*models.SessionSummary, error) {
	key := cache.SessionKey(tenant, sessionID)
	res, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("session store: hgetall: %w", err)
	}
	if len(res) == 0 {
		return nil, nil
	}
	return sessionFromHash(res), nil
}

// ListSessionsByUser 扫描该租户下的会话 Hash，按 user_id 过滤。
func (s *RedisSessionSummaryStore) ListSessionsByUser(ctx context.Context, tenant, userID string) ([]*models.SessionSummary, error) {
	iter := s.client.Scan(ctx, 0, cache.SessionKey(tenant, "*"), 0).Iterator()
	var out []*models.SessionSummary
	for iter.Next(ctx) {
		res, err := s.client.HGetAll(ctx, iter.Val()).Result()
		if err != nil {
			return nil, fmt.Errorf("session store: hgetall: %w", err)
		}
		if res["user_id"] != userID {
			continue
		}
		out = append(out, sessionFromHash(res))
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("session store: scan: %w", err)
	}
	return out, nil
}

// SearchSessions 检索会话总结（管理后台）。
func (s *RedisSessionSummaryStore) SearchSessions(ctx context.Context, tenant string, opts SessionSearchOptions) ([]*models.SessionSummary, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}
	q := strings.ToLower(opts.Query)
	iter := s.client.Scan(ctx, 0, cache.SessionKey(tenant, "*"), 0).Iterator()
	var out []*models.SessionSummary
	for iter.Next(ctx) {
		if len(out) >= limit {
			break
		}
		res, err := s.client.HGetAll(ctx, iter.Val()).Result()
		if err != nil {
			return nil, fmt.Errorf("session store: hgetall: %w", err)
		}
		if len(res) == 0 {
			continue
		}
		if opts.UserID != "" && res["user_id"] != opts.UserID {
			continue
		}
		if q != "" {
			hay := strings.ToLower(res["session_id"] + " " + res["summary"])
			if !strings.Contains(hay, q) {
				continue
			}
		}
		out = append(out, sessionFromHash(res))
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("session store: scan: %w", err)
	}
	return out, nil
}

// DeleteSession 删除单个会话总结（Hash + 版本号）。
func (s *RedisSessionSummaryStore) DeleteSession(ctx context.Context, tenant, sessionID string) error {
	key := cache.SessionKey(tenant, sessionID)
	verKey := cache.SessionVersionKey(tenant, sessionID)
	if err := s.client.Del(ctx, key, verKey).Err(); err != nil {
		return fmt.Errorf("session store: del: %w", err)
	}
	return nil
}

// DeleteSessionsByUser 删除某用户下的全部会话总结。
func (s *RedisSessionSummaryStore) DeleteSessionsByUser(ctx context.Context, tenant, userID string) error {
	list, err := s.ListSessionsByUser(ctx, tenant, userID)
	if err != nil {
		return err
	}
	for _, sum := range list {
		if err := s.DeleteSession(ctx, tenant, sum.SessionID); err != nil {
			return err
		}
	}
	return nil
}

func sessionFromHash(res map[string]string) *models.SessionSummary {
	summary := &models.SessionSummary{
		SessionID: res["session_id"],
		TenantID:  res["tenant_id"],
		UserID:    res["user_id"],
		Summary:   res["summary"],
	}
	if v, err := strconv.Atoi(res["message_count"]); err == nil {
		summary.MessageCount = v
	}
	if v, err := strconv.ParseInt(res["_version"], 10, 64); err == nil {
		summary.Version = v
	}
	if t, err := time.Parse(time.RFC3339, res["started_at"]); err == nil {
		summary.StartedAt = t
	}
	if t, err := time.Parse(time.RFC3339, res["ended_at"]); err == nil {
		summary.EndedAt = t
	}
	if t, err := time.Parse(time.RFC3339, res["_updated_at"]); err == nil {
		summary.UpdatedAt = t
	}
	return summary
}

// MemorySessionSummaryStore 是内存实现，用于测试与无 Redis 场景。
type MemorySessionSummaryStore struct {
	mu        sync.RWMutex
	summaries map[string]*models.SessionSummary
	versions  map[string]int64
	inserter  storage.BatchInserter
}

// NewMemorySessionSummaryStore 创建内存会话总结存储。
func NewMemorySessionSummaryStore(inserter storage.BatchInserter) *MemorySessionSummaryStore {
	return &MemorySessionSummaryStore{
		summaries: make(map[string]*models.SessionSummary),
		versions:  make(map[string]int64),
		inserter:  inserter,
	}
}

// SaveSummary 保存总结并递增版本号。
func (s *MemorySessionSummaryStore) SaveSummary(ctx context.Context, summary *models.SessionSummary) (int64, error) {
	if err := summary.Validate(); err != nil {
		return 0, err
	}
	key := cache.SessionKey(summary.TenantID, summary.SessionID)
	s.mu.Lock()
	s.versions[key]++
	version := s.versions[key]
	cp := *summary
	cp.Version = version
	cp.UpdatedAt = time.Now().UTC()
	s.summaries[key] = &cp
	s.mu.Unlock()

	if s.inserter != nil {
		if err := s.inserter.InsertSessionSummary(ctx, []*models.SessionSummary{&cp}); err != nil {
			return version, err
		}
	}
	return version, nil
}

// GetSummary 读取总结。
func (s *MemorySessionSummaryStore) GetSummary(_ context.Context, tenant, sessionID string) (*models.SessionSummary, error) {
	key := cache.SessionKey(tenant, sessionID)
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.summaries[key]
	if !ok {
		return nil, nil
	}
	cp := *p
	return &cp, nil
}

// ListSessionsByUser 返回属于指定用户的全部会话总结。
func (s *MemorySessionSummaryStore) ListSessionsByUser(_ context.Context, tenant, userID string) ([]*models.SessionSummary, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*models.SessionSummary
	for key, p := range s.summaries {
		if !strings.HasPrefix(key, cache.SessionKey(tenant, "")) {
			continue
		}
		if p.UserID != userID {
			continue
		}
		cp := *p
		out = append(out, &cp)
	}
	return out, nil
}

// SearchSessions 检索内存中的会话总结。
func (s *MemorySessionSummaryStore) SearchSessions(_ context.Context, tenant string, opts SessionSearchOptions) ([]*models.SessionSummary, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}
	q := strings.ToLower(opts.Query)
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*models.SessionSummary
	for key, p := range s.summaries {
		if len(out) >= limit {
			break
		}
		if !strings.HasPrefix(key, cache.SessionKey(tenant, "")) {
			continue
		}
		if opts.UserID != "" && p.UserID != opts.UserID {
			continue
		}
		if q != "" {
			hay := strings.ToLower(p.SessionID + " " + p.Summary)
			if !strings.Contains(hay, q) {
				continue
			}
		}
		cp := *p
		out = append(out, &cp)
	}
	return out, nil
}

// DeleteSession 删除内存中的单个会话总结。
func (s *MemorySessionSummaryStore) DeleteSession(_ context.Context, tenant, sessionID string) error {
	key := cache.SessionKey(tenant, sessionID)
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.summaries, key)
	delete(s.versions, key)
	return nil
}

// DeleteSessionsByUser 删除内存中某用户下的全部会话总结。
func (s *MemorySessionSummaryStore) DeleteSessionsByUser(_ context.Context, tenant, userID string) error {
	prefix := cache.SessionKey(tenant, "")
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, p := range s.summaries {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if p.UserID != userID {
			continue
		}
		delete(s.summaries, key)
		delete(s.versions, key)
	}
	return nil
}

var (
	_ SessionSummaryStore = (*RedisSessionSummaryStore)(nil)
	_ SessionSummaryStore = (*MemorySessionSummaryStore)(nil)
)
