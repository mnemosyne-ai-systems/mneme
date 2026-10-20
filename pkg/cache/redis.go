// Package cache 提供 Redis 客户端封装与布隆过滤器实现。
package cache

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config Redis 连接配置。
type Config struct {
	Addr            string
	Password        string
	DB              int
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	PoolSize        int
	MinIdleConns    int
	ConnMaxIdleTime time.Duration
	ConnMaxLifetime time.Duration
}

// NewRedisClient 创建并探活 Redis 客户端。
func NewRedisClient(ctx context.Context, cfg Config) (*redis.Client, error) {
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 3 * time.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 3 * time.Second
	}
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = runtime.NumCPU() * 4
	}
	if cfg.MinIdleConns <= 0 {
		cfg.MinIdleConns = cfg.PoolSize / 2
	}
	if cfg.ConnMaxIdleTime <= 0 {
		cfg.ConnMaxIdleTime = 30 * time.Minute
	}
	if cfg.ConnMaxLifetime <= 0 {
		cfg.ConnMaxLifetime = time.Hour
	}
	client := redis.NewClient(&redis.Options{
		Addr:            cfg.Addr,
		Password:        cfg.Password,
		DB:              cfg.DB,
		DialTimeout:     cfg.DialTimeout,
		ReadTimeout:     cfg.ReadTimeout,
		WriteTimeout:    cfg.WriteTimeout,
		PoolSize:        cfg.PoolSize,
		MinIdleConns:    cfg.MinIdleConns,
		ConnMaxIdleTime: cfg.ConnMaxIdleTime,
		ConnMaxLifetime: cfg.ConnMaxLifetime,
	})
	pingCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis: ping %s: %w", cfg.Addr, err)
	}
	return client, nil
}

// KeyPrefix 是 Mnemosyne 统一的 Redis Key 前缀根。
const KeyPrefix = "mnemosyne:mneme"

// MemoryKey 返回实体画像的 Redis Hash Key：
// mnemosyne:mneme:memory:{tenant}:{entity_type}:{entity_id}
func MemoryKey(tenant, entityType, entityID string) string {
	return fmt.Sprintf("%s:memory:%s:%s:%s", KeyPrefix, tenant, entityType, entityID)
}

// BloomKey 返回布隆过滤器 Key：mnemosyne:mneme:bloom:{pipeline}:{tenant}
func BloomKey(pipeline, tenant string) string {
	return fmt.Sprintf("%s:bloom:%s:%s", KeyPrefix, pipeline, tenant)
}

// MemoryVersionKey 返回实体画像版本号计数器 Key。
func MemoryVersionKey(tenant, entityType, entityID string) string {
	return fmt.Sprintf("%s:memver:%s:%s:%s", KeyPrefix, tenant, entityType, entityID)
}

// SessionKey 返回会话总结的 Redis Hash Key：
// mnemosyne:mneme:session:{tenant}:{session_id}
func SessionKey(tenant, sessionID string) string {
	return fmt.Sprintf("%s:session:%s:%s", KeyPrefix, tenant, sessionID)
}

// SessionVersionKey 返回会话总结版本号计数器 Key（独立前缀，避免与 SessionKey 的 SCAN 模式冲突）：
// mnemosyne:mneme:sessver:{tenant}:{session_id}
func SessionVersionKey(tenant, sessionID string) string {
	return fmt.Sprintf("%s:sessver:%s:%s", KeyPrefix, tenant, sessionID)
}

// DeletedField 是软删除标记字段（写入 Hash，读取时跳过）。
const DeletedField = "_deleted"

// AuditKey 返回管理操作审计日志的 Redis List Key：
// mnemosyne:mneme:audit:{tenant}
func AuditKey(tenant string) string {
	return fmt.Sprintf("%s:audit:%s", KeyPrefix, tenant)
}

// UsersIndexKey 返回用户索引 ZSET Key（按 updated_at 排序）：
// mnemosyne:mneme:users:{tenant}
// member = entity_id, score = updated_at unix ms
func UsersIndexKey(tenant string) string {
	return fmt.Sprintf("%s:users:%s", KeyPrefix, tenant)
}

// SessionsIndexKey 返回会话索引 ZSET Key（按 ended_at 排序）：
// mnemosyne:mneme:sessions:{tenant}
// member = session_id, score = ended_at unix ms
func SessionsIndexKey(tenant string) string {
	return fmt.Sprintf("%s:sessions:%s", KeyPrefix, tenant)
}

// L2Cache 本地二级缓存（内存级，带 TTL 和 LRU 淘汰）。
type L2Cache struct {
	mu      sync.RWMutex
	data    map[string]*l2Entry
	maxSize int
	ttl     time.Duration
	onEvict func(key string, value interface{})
}

type l2Entry struct {
	value      interface{}
	expiration time.Time
	accessed   time.Time
}

// NewL2Cache 创建本地二级缓存。
func NewL2Cache(maxSize int, ttl time.Duration, onEvict func(key string, value interface{})) *L2Cache {
	if maxSize <= 0 {
		maxSize = 10000
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	c := &L2Cache{
		data:    make(map[string]*l2Entry, maxSize),
		maxSize: maxSize,
		ttl:     ttl,
		onEvict: onEvict,
	}
	go c.gcLoop()
	return c
}

func (c *L2Cache) Get(key string) (interface{}, bool) {
	// accessed 会被更新，必须使用写锁；RLock 下写入会造成数据竞争。
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.data[key]
	if !ok {
		return nil, false
	}
	if time.Now().After(entry.expiration) {
		return nil, false
	}
	entry.accessed = time.Now()
	return entry.value, true
}

func (c *L2Cache) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// LRU 淘汰
	if len(c.data) >= c.maxSize {
		c.evictLRU(1)
	}

	c.data[key] = &l2Entry{
		value:      value,
		expiration: time.Now().Add(c.ttl),
		accessed:   time.Now(),
	}
}

func (c *L2Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.data[key]; ok {
		if c.onEvict != nil {
			c.onEvict(key, entry.value)
		}
		delete(c.data, key)
	}
}

func (c *L2Cache) evictLRU(n int) {
	if len(c.data) == 0 {
		return
	}
	type kv struct {
		key      string
		accessed time.Time
	}
	entries := make([]kv, 0, len(c.data))
	for k, v := range c.data {
		entries = append(entries, kv{k, v.accessed})
	}
	for i := 0; i < n && len(entries) > 0; i++ {
		oldestIdx := 0
		for j := 1; j < len(entries); j++ {
			if entries[j].accessed.Before(entries[oldestIdx].accessed) {
				oldestIdx = j
			}
		}
		delete(c.data, entries[oldestIdx].key)
		entries = append(entries[:oldestIdx], entries[oldestIdx+1:]...)
	}
}

func (c *L2Cache) gcLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		c.mu.Lock()
		now := time.Now()
		for k, v := range c.data {
			if now.After(v.expiration) {
				delete(c.data, k)
			}
		}
		c.mu.Unlock()
	}
}

func (c *L2Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.data)
}

func (c *L2Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data = make(map[string]*l2Entry, c.maxSize)
}
