package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mnemosyne/mneme/pkg/cache"
	"github.com/mnemosyne/mneme/pkg/models"
	"github.com/mnemosyne/mneme/pkg/storage"
)

// decodeCursor 解析分页游标（偏移量）。
func decodeCursor(s string) int {
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// encodeCursor 编码下一页游标；无下一页时返回空串。
func encodeCursor(offset int) string {
	if offset <= 0 {
		return ""
	}
	return strconv.Itoa(offset)
}

// ProfileStore 负责画像的热存储（Redis）与冷存储（ClickHouse）。
type ProfileStore interface {
	// SaveUserInfo 在消息到达时立即写入最小用户信息（不触发 LLM），
	// 保证管理后台可以立即检索到该用户。后续 SaveProfile 会覆盖并追加完整画像。
	SaveUserInfo(ctx context.Context, tenant, userID string) error
	// CreateProfile 创建用户画像（先写 ClickHouse，再写 Redis，保证一致性）。
	CreateProfile(ctx context.Context, tenant, entityType, entityID string, profile map[string]string) error
	SaveProfile(ctx context.Context, tenant, entityType, entityID string, profile map[string]string) (int64, error)
	GetProfile(ctx context.Context, tenant, entityType, entityID string) (map[string]string, error)
	// ListProfiles 列出某租户下某类型实体的画像（用于管理后台浏览），返回结果与下一页游标。
	ListProfiles(ctx context.Context, tenant, entityType string, opts ListOptions) ([]*EntityProfile, string, error)
	// DeleteProfile 软删除某实体画像（数据治理，可恢复）。仅标记热存储 Hash 的 _deleted 字段。
	DeleteProfile(ctx context.Context, tenant, entityType, entityID string) error
	// PurgeProfile 硬删除（不可恢复），真正清理热存储。
	PurgeProfile(ctx context.Context, tenant, entityType, entityID string) error
}

// ListOptions 列表查询选项。
type ListOptions struct {
	// Cursor 分页游标（由上一页返回的 next_cursor），空表示从首页。
	Cursor string
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
	client       redis.UniversalClient
	inserter     storage.BatchInserter
	ttl          time.Duration
	useZSetIndex bool
}

// NewRedisProfileStore 创建画像存储。inserter 可为 nil（仅写 Redis）。
func NewRedisProfileStore(client redis.UniversalClient, inserter storage.BatchInserter, ttl time.Duration, useZSetIndex bool) *RedisProfileStore {
	return &RedisProfileStore{client: client, inserter: inserter, ttl: ttl, useZSetIndex: useZSetIndex}
}

// SaveUserInfo 在消息到达时立即写入最小用户信息（不触发 LLM，不写 ClickHouse），
// 保证管理后台可以立即检索到该用户。后续 SaveProfile 会覆盖并追加完整画像。
func (s *RedisProfileStore) SaveUserInfo(ctx context.Context, tenant, userID string) error {
	key := cache.MemoryKey(tenant, models.EntityTypeUser, userID)
	now := time.Now().UTC()
	updatedAt := now.Format(time.RFC3339)
	fields := map[string]interface{}{
		"entity_id":   userID,
		"entity_type": models.EntityTypeUser,
		"tenant_id":   tenant,
		"_updated_at": updatedAt,
	}
	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, key, fields)
	if s.ttl > 0 {
		pipe.Expire(ctx, key, s.ttl)
	}
	if s.useZSetIndex {
		score := float64(now.UnixMilli())
		pipe.ZAdd(ctx, cache.UsersIndexKey(tenant), redis.Z{Score: score, Member: userID})
	}
	_, err := pipe.Exec(ctx)
	return err
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

	now := time.Now().UTC()
	updatedAt := now.Format(time.RFC3339)

	fields := make(map[string]interface{}, len(profile)+1)
	for k, v := range profile {
		fields[k] = v
	}
	fields["_version"] = version
	fields["_updated_at"] = updatedAt

	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, key, fields)
	if s.ttl > 0 {
		pipe.Expire(ctx, key, s.ttl)
	}
	if s.useZSetIndex && entityType == models.EntityTypeUser {
		score := float64(now.UnixMilli())
		pipe.ZAdd(ctx, cache.UsersIndexKey(tenant), redis.Z{Score: score, Member: entityID})
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

// CreateProfile 创建用户画像（先写 ClickHouse，再写 Redis，保证一致性）。
func (s *RedisProfileStore) CreateProfile(ctx context.Context, tenant, entityType, entityID string, profile map[string]string) error {
	if len(profile) == 0 {
		return nil
	}
	// 先写 ClickHouse（源头真相）
	if s.inserter != nil {
		row := &models.EntityMemory{
			EntityID:   entityID,
			TenantID:   tenant,
			EntityType: entityType,
			Profile:    "{}", // 空画像，后续 SaveProfile 覆盖
			Version:    0,
			UpdatedAt:  time.Now().UTC(),
		}
		if err := s.inserter.InsertMemory(ctx, []*models.EntityMemory{row}); err != nil {
			return fmt.Errorf("profile store: insert clickhouse: %w", err)
		}
	}
	// 再写 Redis（热存储）
	key := cache.MemoryKey(tenant, entityType, entityID)
	verKey := cache.MemoryVersionKey(tenant, entityType, entityID)

	version, err := s.client.Incr(ctx, verKey).Result()
	if err != nil {
		return fmt.Errorf("profile store: incr version: %w", err)
	}

	now := time.Now().UTC()
	updatedAt := now.Format(time.RFC3339)

	fields := make(map[string]interface{}, len(profile)+1)
	for k, v := range profile {
		fields[k] = v
	}
	fields["_version"] = version
	fields["_updated_at"] = updatedAt

	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, key, fields)
	if s.ttl > 0 {
		pipe.Expire(ctx, key, s.ttl)
	}
	if s.useZSetIndex && entityType == models.EntityTypeUser {
		score := float64(time.Now().UTC().UnixMilli())
		pipe.ZAdd(ctx, cache.UsersIndexKey(tenant), redis.Z{Score: score, Member: entityID})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("profile store: hset: %w", err)
	}
	return nil
}

// GetProfile 从 Redis 读取画像。
func (s *RedisProfileStore) GetProfile(ctx context.Context, tenant, entityType, entityID string) (map[string]string, error) {
	key := cache.MemoryKey(tenant, entityType, entityID)
	res, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("profile store: hgetall: %w", err)
	}
	if len(res) == 0 || res[cache.DeletedField] == "1" {
		return nil, nil
	}
	return res, nil
}

// ListProfiles 扫描该租户下的实体 Hash，返回画像视图与下一页游标（用于管理后台分页）。
// 启用 ZSET 索引时走 ZREVRANGEBYSCORE，否则回退 SCAN。
func (s *RedisProfileStore) ListProfiles(ctx context.Context, tenant, entityType string, opts ListOptions) ([]*EntityProfile, string, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}

	if s.useZSetIndex && entityType == models.EntityTypeUser {
		return s.listProfilesZSet(ctx, tenant, opts)
	}

	// 回退 SCAN
	iter := s.client.Scan(ctx, 0, cache.MemoryKey(tenant, entityType, "*"), 0).Iterator()
	var keys []string
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, "", fmt.Errorf("profile store: scan: %w", err)
	}
	sort.Strings(keys)
	offset := decodeCursor(opts.Cursor)
	if offset > len(keys) {
		offset = len(keys)
	}
	end := offset + limit
	if end > len(keys) {
		end = len(keys)
	}
	out := make([]*EntityProfile, 0, end-offset)
	for _, key := range keys[offset:end] {
		res, err := s.client.HGetAll(ctx, key).Result()
		if err != nil {
			return nil, "", fmt.Errorf("profile store: hgetall: %w", err)
		}
		if len(res) == 0 || res[cache.DeletedField] == "1" {
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
	next := ""
	if end < len(keys) {
		next = encodeCursor(end)
	}
	return out, next, nil
}

// listProfilesZSet 使用 ZSET 索引查询用户画像（按 updated_at 倒序）。
func (s *RedisProfileStore) listProfilesZSet(ctx context.Context, tenant string, opts ListOptions) ([]*EntityProfile, string, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}
	offset := decodeCursor(opts.Cursor)
	idxKey := cache.UsersIndexKey(tenant)

	// ZREVRANGEBYSCORE 返回 member 列表（倒序）
	members, err := s.client.ZRevRangeByScore(ctx, idxKey, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    "+inf",
		Offset: int64(offset),
		Count:  int64(limit + 1), // 多取 1 个判断是否有下一页
	}).Result()
	if err != nil {
		return nil, "", fmt.Errorf("profile store: zrevrangebyscore: %w", err)
	}

	// 超出 limit 说明有下一页
	next := ""
	if len(members) > limit {
		members = members[:limit]
		next = encodeCursor(offset + limit)
	}

	out := make([]*EntityProfile, 0, len(members))
	for _, userID := range members {
		key := cache.MemoryKey(tenant, models.EntityTypeUser, userID)
		res, err := s.client.HGetAll(ctx, key).Result()
		if err != nil {
			return nil, "", fmt.Errorf("profile store: hgetall: %w", err)
		}
		if len(res) == 0 || res[cache.DeletedField] == "1" {
			continue
		}
		cp := make(map[string]string, len(res))
		for k, v := range res {
			cp[k] = v
		}
		out = append(out, &EntityProfile{
			TenantID:   tenant,
			EntityType: models.EntityTypeUser,
			EntityID:   userID,
			Profile:    cp,
		})
	}
	return out, next, nil
}

// DeleteProfile 软删除：标记热存储 Hash 的 _deleted 字段（可恢复）。
func (s *RedisProfileStore) DeleteProfile(ctx context.Context, tenant, entityType, entityID string) error {
	key := cache.MemoryKey(tenant, entityType, entityID)
	if err := s.client.HSet(ctx, key, cache.DeletedField, "1", "_updated_at", time.Now().UTC().Format(time.RFC3339)).Err(); err != nil {
		return fmt.Errorf("profile store: soft delete: %w", err)
	}
	return nil
}

// PurgeProfile 硬删除：真正清理热存储 Hash 与版本号（不可恢复）。
func (s *RedisProfileStore) PurgeProfile(ctx context.Context, tenant, entityType, entityID string) error {
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
	cp["_updated_at"] = time.Now().UTC().Format(time.RFC3339)
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

// CreateProfile 创建用户画像（先写 ClickHouse，再写内存，保证一致性）。
func (s *MemoryProfileStore) CreateProfile(ctx context.Context, tenant, entityType, entityID string, profile map[string]string) error {
	if len(profile) == 0 {
		return nil
	}
	// 先写 ClickHouse
	if s.inserter != nil {
		row := &models.EntityMemory{
			EntityID:   entityID,
			TenantID:   tenant,
			EntityType: entityType,
			Profile:    "{}",
			Version:    0,
			UpdatedAt:  time.Now().UTC(),
		}
		if err := s.inserter.InsertMemory(ctx, []*models.EntityMemory{row}); err != nil {
			return err
		}
	}
	// 再写内存
	key := cache.MemoryKey(tenant, entityType, entityID)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.versions[key]++
	version := s.versions[key]
	cp := make(map[string]string, len(profile)+1)
	for k, v := range profile {
		cp[k] = v
	}
	cp["_version"] = fmt.Sprintf("%d", version)
	cp["_updated_at"] = time.Now().UTC().Format(time.RFC3339)
	s.profiles[key] = cp
	return nil
}

// SaveUserInfo 在消息到达时立即写入最小用户信息（不触发 LLM）。
func (s *MemoryProfileStore) SaveUserInfo(_ context.Context, tenant, userID string) error {
	key := cache.MemoryKey(tenant, models.EntityTypeUser, userID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.profiles[key]; !ok {
		s.profiles[key] = map[string]string{
			"entity_id":   userID,
			"entity_type": models.EntityTypeUser,
			"tenant_id":   tenant,
		}
	}
	return nil
}

// GetProfile 读取画像。
func (s *MemoryProfileStore) GetProfile(_ context.Context, tenant, entityType, entityID string) (map[string]string, error) {
	key := cache.MemoryKey(tenant, entityType, entityID)
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.profiles[key]
	if !ok || p[cache.DeletedField] == "1" {
		return nil, nil
	}
	cp := make(map[string]string, len(p))
	for k, v := range p {
		cp[k] = v
	}
	return cp, nil
}

// ListProfiles 返回内存中该租户下该类型实体的画像，并给出下一页游标。
func (s *MemoryProfileStore) ListProfiles(_ context.Context, tenant, entityType string, opts ListOptions) ([]*EntityProfile, string, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}
	prefix := cache.MemoryKey(tenant, entityType, "")
	s.mu.RLock()
	defer s.mu.RUnlock()
	var keys []string
	for key := range s.profiles {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	offset := decodeCursor(opts.Cursor)
	if offset > len(keys) {
		offset = len(keys)
	}
	end := offset + limit
	if end > len(keys) {
		end = len(keys)
	}
	out := make([]*EntityProfile, 0, end-offset)
	for _, key := range keys[offset:end] {
		p := s.profiles[key]
		if p[cache.DeletedField] == "1" {
			continue
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
	next := ""
	if end < len(keys) {
		next = encodeCursor(end)
	}
	return out, next, nil
}

// DeleteProfile 软删除：标记 _deleted（可恢复）。
func (s *MemoryProfileStore) DeleteProfile(_ context.Context, tenant, entityType, entityID string) error {
	key := cache.MemoryKey(tenant, entityType, entityID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if p, ok := s.profiles[key]; ok {
		p[cache.DeletedField] = "1"
		p["_updated_at"] = time.Now().UTC().Format(time.RFC3339)
	}
	return nil
}

// PurgeProfile 硬删除：真正清理内存中的画像。
func (s *MemoryProfileStore) PurgeProfile(_ context.Context, tenant, entityType, entityID string) error {
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