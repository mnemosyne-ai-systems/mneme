package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mnemosyne/mneme/pkg/cache"
)

// AuditAction 管理操作审计动作类型。
const (
	AuditActionDeleteUser     = "delete_user"
	AuditActionHardDeleteUser = "hard_delete_user"
	AuditActionDeleteSession  = "delete_session"
	AuditActionHardDelete     = "hard_delete_session"
	AuditActionExportUser     = "export_user"
	AuditActionExportSession  = "export_session"
)

// AuditEntry 一条管理操作审计日志。
type AuditEntry struct {
	TenantID string    `json:"tenant_id"`
	Actor    string    `json:"actor"`   // 操作人（管理后台 token 或 "admin"）
	Action   string    `json:"action"`  // 动作类型
	Target   string    `json:"target"`  // 目标标识（user_id / session_id）
	Detail   string    `json:"detail"`  // 备注
	At       time.Time `json:"at"`
}

// AuditStore 管理操作审计日志存储。
type AuditStore interface {
	Append(ctx context.Context, entry AuditEntry) error
	List(ctx context.Context, tenant string, limit int) ([]AuditEntry, error)
}

// RedisAuditStore 使用 Redis List 存储审计日志（最新在前）。
type RedisAuditStore struct {
	client redis.UniversalClient
}

// NewRedisAuditStore 创建 Redis 审计日志存储。
func NewRedisAuditStore(client redis.UniversalClient) *RedisAuditStore {
	return &RedisAuditStore{client: client}
}

// Append 写入一条审计日志。
func (s *RedisAuditStore) Append(ctx context.Context, entry AuditEntry) error {
	if entry.At.IsZero() {
		entry.At = time.Now().UTC()
	}
	raw, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("audit: marshal: %w", err)
	}
	key := cache.AuditKey(entry.TenantID)
	if err := s.client.LPush(ctx, key, raw).Err(); err != nil {
		return fmt.Errorf("audit: lpush: %w", err)
	}
	if err := s.client.LTrim(ctx, key, 0, 9999).Err(); err != nil {
		return fmt.Errorf("audit: ltrim: %w", err)
	}
	return nil
}

// List 读取最近的审计日志（最新在前）。
func (s *RedisAuditStore) List(ctx context.Context, tenant string, limit int) ([]AuditEntry, error) {
	if limit <= 0 {
		limit = 100
	}
	raws, err := s.client.LRange(ctx, cache.AuditKey(tenant), 0, int64(limit-1)).Result()
	if err != nil {
		return nil, fmt.Errorf("audit: lrange: %w", err)
	}
	out := make([]AuditEntry, 0, len(raws))
	for _, r := range raws {
		var e AuditEntry
		if err := json.Unmarshal([]byte(r), &e); err != nil {
			continue
		}
		out = append(out, e)
	}
	return out, nil
}

// MemoryAuditStore 内存实现，用于测试与无 Redis 场景。
type MemoryAuditStore struct {
	mu     sync.RWMutex
	entries map[string][]AuditEntry
}

// NewMemoryAuditStore 创建内存审计日志存储。
func NewMemoryAuditStore() *MemoryAuditStore {
	return &MemoryAuditStore{entries: make(map[string][]AuditEntry)}
}

// Append 写入一条审计日志（最新在前）。
func (s *MemoryAuditStore) Append(_ context.Context, entry AuditEntry) error {
	if entry.At.IsZero() {
		entry.At = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[entry.TenantID] = append([]AuditEntry{entry}, s.entries[entry.TenantID]...)
	return nil
}

// List 读取最近的审计日志（最新在前）。
func (s *MemoryAuditStore) List(_ context.Context, tenant string, limit int) ([]AuditEntry, error) {
	if limit <= 0 {
		limit = 100
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	all := s.entries[tenant]
	if len(all) > limit {
		all = all[:limit]
	}
	out := make([]AuditEntry, len(all))
	copy(out, all)
	return out, nil
}

var (
	_ AuditStore = (*RedisAuditStore)(nil)
	_ AuditStore = (*MemoryAuditStore)(nil)
)
