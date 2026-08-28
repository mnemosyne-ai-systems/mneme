// Package cache 提供 Redis 客户端封装与布隆过滤器实现。
package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config Redis 连接配置。
type Config struct {
	Addr        string
	Password    string
	DB          int
	DialTimeout time.Duration
	PoolSize    int
}

// NewRedisClient 创建并探活 Redis 客户端。
func NewRedisClient(ctx context.Context, cfg Config) (*redis.Client, error) {
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = 20
	}
	client := redis.NewClient(&redis.Options{
		Addr:        cfg.Addr,
		Password:    cfg.Password,
		DB:          cfg.DB,
		DialTimeout: cfg.DialTimeout,
		PoolSize:    cfg.PoolSize,
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
