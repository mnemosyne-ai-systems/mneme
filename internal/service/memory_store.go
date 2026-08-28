package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mnemosyne/mneme/pkg/cache"
	"github.com/mnemosyne/mneme/pkg/models"
	"github.com/mnemosyne/mneme/pkg/storage"
)

// ProfileStore 负责画像的热存储（Redis）与冷存储（ClickHouse）。
type ProfileStore interface {
	SaveProfile(ctx context.Context, tenant, entityType, entityID string, profile map[string]string) (int64, error)
	GetProfile(ctx context.Context, tenant, entityType, entityID string) (map[string]string, error)
	// ListProfiles 列出某租户下某类型实体的画像（用于管理后台浏览）。
	ListProfiles(ctx context.Context, tenant, entityType string, opts ListOptions) ([]*EntityProfile, error)
	// DeleteProfile 删除某实体画像（数据治理）。仅清理热存储；冷存储 ClickHouse 由 TTL/归档策略处理。
	DeleteProfile(ctx context.Context, tenant, entityType, entityID string) error
}

// ListOptions 列表查询选项。
type ListOptions struct {
	// Limit 返回条数上限，<=0 表示使用默认（100）。
	Limit int
}

// EntityProfile 管理后台列出的实体画像视图。
type EntityProfile struct {
	TenantID   string
	EntityType string
	EntityID   string
	Profile    map[string]string
}

// RedisProfileStore 使用 Redis Hash 保存画像，并把带版本号的快照写入 ClickHouse。
type RedisProfileStore struct {
	client   redis.UniversalClient
	inserter storage.BatchInserter
	ttl      time.Duration
}

// NewRedisProfileStore 创建画像存储。inserter 可为 nil（仅写 Redis）。
func NewRedisProfileStore(client redis.UniversalClient, inserter storage.BatchInserter, ttl time.Duration) *RedisProfileStore {
	return &RedisProfileStore{client: client, inserter: inserter, ttl: ttl}
}

// SaveProfile 写入 Redis Hash 与 ClickHouse，返回新的版本号。
func (s *RedisProfileStore) SaveProfile(ctx context.Context, tenant, entityType, entityID string, profile map[string]string) (int64, error) {
	if len(profile) == 0 {
		return 0, nil
	}
	key := cache.MemoryKey(tenant, entityType, entityID)
	verKey := cache.MemoryVersionKey(tenant, entityType, entityID)

	version, err := s.client.Incr(ctx, verKey).Result()
	if err != nil {
		return 0, fmt.Errorf("profile store: incr version: %w", err)
	}

	fields := make(map[string]interface{}, len(profile)+1)
	for k, v := range profile {
		fields[k] = v
	}
	fields["_version"] = version
	fields["_updated_at"] = time.Now().UTC().Format(time.RFC3339)

	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, key, fields)
	if s.ttl > 0 {
		pipe.Expire(ctx, key, s.ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return version, fmt.Errorf("profile store: hset: %w", err)
	}

	if s.inserter != nil {
		raw, err := json.Marshal(profile)
		if err != nil {
			return version, fmt.Errorf("profile store: marshal profile: %w", err)
		}
		row := &models.EntityMemory{
			EntityID:   entityID,
			TenantID:   tenant,
			EntityType: entityType,
			Profile:    string(raw),
			Version:    version,
			UpdatedAt:  time.Now().UTC(),
		}
		if err := s.inserter.InsertMemory(ctx, []*models.EntityMemory{row}); err != nil {
			return version, fmt.Errorf("profile store: insert clickhouse: %w", err)
		}
	}
	return version, nil
}

// GetProfile 从 Redis 读取画像。
func (s *RedisProfileStore) GetProfile(ctx context.Context, tenant, entityType, entityID string) (map[string]string, error) {
	key := cache.MemoryKey(tenant, entityType, entityID)
	res, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("profile store: hgetall: %w", err)
	}
	return res, nil
}

// ListProfiles 扫描该租户下的实体 Hash，返回画像视图（用于管理后台）。
func (s *RedisProfileStore) ListProfiles(ctx context.Context, tenant, entityType string, opts ListOptions) ([]*EntityProfile, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}
	iter := s.client.Scan(ctx, 0, cache.MemoryKey(tenant, entityType, "*"), 0).Iterator()
	var out []*EntityProfile
	for iter.Next(ctx) {
		if len(out) >= limit {
			break
		}
		res, err := s.client.HGetAll(ctx, iter.Val()).Result()
		if err != nil {
			return nil, fmt.Errorf("profile store: hgetall: %w", err)
		}
		if len(res) == 0 {
			continue
		}
		cp := make(map[string]string, len(res))
		for k, v := range res {
			cp[k] = v
		}
		out = append(out, &EntityProfile{
			TenantID:   tenant,
			EntityType: entityType,
			EntityID:   res["entity_id"],
			Profile:    cp,
		})
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("profile store: scan: %w", err)
	}
	return out, nil
}

// DeleteProfile 删除实体画像的热存储（Hash + 版本号）。
func (s *RedisProfileStore) DeleteProfile(ctx context.Context, tenant, entityType, entityID string) error {
	key := cache.MemoryKey(tenant, entityType, entityID)
	verKey := cache.MemoryVersionKey(tenant, entityType, entityID)
	if err := s.client.Del(ctx, key, verKey).Err(); err != nil {
		return fmt.Errorf("profile store: del: %w", err)
	}
	return nil
}

// MemoryProfileStore 是内存实现，用于测试与无 Redis 场景。
type MemoryProfileStore struct {
	mu       sync.RWMutex
	profiles map[string]map[string]string
	versions map[string]int64
	inserter storage.BatchInserter
}

// NewMemoryProfileStore 创建内存画像存储。
func NewMemoryProfileStore(inserter storage.BatchInserter) *MemoryProfileStore {
	return &MemoryProfileStore{
		profiles: make(map[string]map[string]string),
		versions: make(map[string]int64),
		inserter: inserter,
	}
}

// SaveProfile 保存画像并递增版本号。
func (s *MemoryProfileStore) SaveProfile(ctx context.Context, tenant, entityType, entityID string, profile map[string]string) (int64, error) {
	if len(profile) == 0 {
		return 0, nil
	}
	key := cache.MemoryKey(tenant, entityType, entityID)

	s.mu.Lock()
	s.versions[key]++
	version := s.versions[key]
	cp := make(map[string]string, len(profile))
	for k, v := range profile {
		cp[k] = v
	}
	cp["_version"] = fmt.Sprintf("%d", version)
	s.profiles[key] = cp
	s.mu.Unlock()

	if s.inserter != nil {
		raw, err := json.Marshal(profile)
		if err != nil {
			return version, err
		}
		row := &models.EntityMemory{
			EntityID:   entityID,
			TenantID:   tenant,
			EntityType: entityType,
			Profile:    string(raw),
			Version:    version,
			UpdatedAt:  time.Now().UTC(),
		}
		if err := s.inserter.InsertMemory(ctx, []*models.EntityMemory{row}); err != nil {
			return version, err
		}
	}
	return version, nil
}

// GetProfile 读取画像。
func (s *MemoryProfileStore) GetProfile(_ context.Context, tenant, entityType, entityID string) (map[string]string, error) {
	key := cache.MemoryKey(tenant, entityType, entityID)
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.profiles[key]
	if !ok {
		return map[string]string{}, nil
	}
	cp := make(map[string]string, len(p))
	for k, v := range p {
		cp[k] = v
	}
	return cp, nil
}

// ListProfiles 返回内存中该租户下该类型实体的画像。
func (s *MemoryProfileStore) ListProfiles(_ context.Context, tenant, entityType string, opts ListOptions) ([]*EntityProfile, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}
	prefix := cache.MemoryKey(tenant, entityType, "")
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*EntityProfile
	for key, p := range s.profiles {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if len(out) >= limit {
			break
		}
		entityID := strings.TrimPrefix(key, prefix)
		cp := make(map[string]string, len(p))
		for k, v := range p {
			cp[k] = v
		}
		out = append(out, &EntityProfile{
			TenantID:   tenant,
			EntityType: entityType,
			EntityID:   entityID,
			Profile:    cp,
		})
	}
	return out, nil
}

// DeleteProfile 从内存中删除实体画像。
func (s *MemoryProfileStore) DeleteProfile(_ context.Context, tenant, entityType, entityID string) error {
	key := cache.MemoryKey(tenant, entityType, entityID)
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.profiles, key)
	delete(s.versions, key)
	return nil
}

var (
	_ ProfileStore = (*RedisProfileStore)(nil)
	_ ProfileStore = (*MemoryProfileStore)(nil)
)
