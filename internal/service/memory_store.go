package service

import (
	"context"
	"encoding/json"
	"fmt"
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

var (
	_ ProfileStore = (*RedisProfileStore)(nil)
	_ ProfileStore = (*MemoryProfileStore)(nil)
)
