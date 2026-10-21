package service

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mnemosyne-ai-systems/mneme/pkg/cache"
)

// evolutionMemberSep 分隔 ZSET member 中的 tenant 与 session_id，
// 使用不可见字符避免与业务 ID 冲突。
const evolutionMemberSep = "\x1f"

// evolutionClaimLease 是领取后的租约：处理期间把 member 放回队列并推迟到租约之后，
// 进程崩溃时任务仍会被下一轮扫描重新领取（至少一次语义）。
const evolutionClaimLease = 10 * time.Minute

// ProfileEvolvingSummaryKey 是用户画像中保存跨会话长期摘要的保留字段。
const ProfileEvolvingSummaryKey = "evolving_summary"

// ProfileEvolvingUpdatedKey 记录跨会话长期摘要的更新时间。
const ProfileEvolvingUpdatedKey = "evolving_summary_updated_at"

// EvolutionCandidate 标识一个待自我反思的会话。
type EvolutionCandidate struct {
	TenantID  string
	SessionID string
}

// EvolutionScheduler 由会话总结消费者使用：每次摘要更新后安排一次反思。
type EvolutionScheduler interface {
	Schedule(ctx context.Context, tenantID, sessionID string, due time.Time) error
}

// EvolutionQueue 维护待反思会话的调度队列与反思轮次状态。
type EvolutionQueue interface {
	EvolutionScheduler
	// Reschedule 更新下次反思时间（覆盖已有时间），不重置轮次。用于失败重试。
	Reschedule(ctx context.Context, tenantID, sessionID string, due time.Time) error
	// Continue 续排下一轮反思：保留更早的到期时间，避免覆盖更紧急的新调度。
	Continue(ctx context.Context, tenantID, sessionID string, due time.Time) error
	// ClaimDue 领取已到期的会话（原子移除，返回领取成功的候选）。
	ClaimDue(ctx context.Context, now time.Time, limit int) ([]EvolutionCandidate, error)
	// Finish 记录一轮反思完成并返回累计轮次。
	Finish(ctx context.Context, tenantID, sessionID string) (int, error)
	// Forget 从队列与状态中移除会话。
	Forget(ctx context.Context, tenantID, sessionID string) error
}

// RedisEvolutionQueue 基于 Redis ZSET（到期时间）+ Hash（轮次）实现进化队列。
type RedisEvolutionQueue struct {
	client redis.UniversalClient
	ttl    time.Duration
}

// NewRedisEvolutionQueue 创建 Redis 进化队列。ttl <= 0 表示不设置过期时间。
func NewRedisEvolutionQueue(client redis.UniversalClient, ttl time.Duration) *RedisEvolutionQueue {
	return &RedisEvolutionQueue{client: client, ttl: ttl}
}

// Schedule 安排一次反思并重置轮次（新摘要代表新的演化周期）。
func (q *RedisEvolutionQueue) Schedule(ctx context.Context, tenantID, sessionID string, due time.Time) error {
	pipe := q.client.TxPipeline()
	pipe.ZAdd(ctx, cache.EvolutionQueueKey(), redis.Z{
		Score:  float64(due.UnixMilli()),
		Member: encodeEvolutionMember(tenantID, sessionID),
	})
	pipe.Del(ctx, cache.EvolutionStateKey(tenantID, sessionID))
	q.expire(ctx, pipe, cache.EvolutionQueueKey(), cache.EvolutionStateKey(tenantID, sessionID))
	_, err := pipe.Exec(ctx)
	return err
}

// Reschedule 更新下次反思时间（覆盖已有时间），保留已完成的轮次。
func (q *RedisEvolutionQueue) Reschedule(ctx context.Context, tenantID, sessionID string, due time.Time) error {
	pipe := q.client.TxPipeline()
	pipe.ZAdd(ctx, cache.EvolutionQueueKey(), redis.Z{
		Score:  float64(due.UnixMilli()),
		Member: encodeEvolutionMember(tenantID, sessionID),
	})
	q.expire(ctx, pipe, cache.EvolutionQueueKey(), cache.EvolutionStateKey(tenantID, sessionID))
	_, err := pipe.Exec(ctx)
	return err
}

// Continue 续排下一轮反思：只在当前到期时间更晚（或不存在）时写入，保留更早的调度。
func (q *RedisEvolutionQueue) Continue(ctx context.Context, tenantID, sessionID string, due time.Time) error {
	queueKey := cache.EvolutionQueueKey()
	member := encodeEvolutionMember(tenantID, sessionID)
	current, err := q.client.ZScore(ctx, queueKey, member).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("evolution queue: zscore: %w", err)
	}
	if err == nil && current <= float64(due.UnixMilli()) {
		// 已有更早的到期时间，保留它。
		return nil
	}
	pipe := q.client.TxPipeline()
	pipe.ZAdd(ctx, queueKey, redis.Z{Score: float64(due.UnixMilli()), Member: member})
	q.expire(ctx, pipe, queueKey, cache.EvolutionStateKey(tenantID, sessionID))
	_, err = pipe.Exec(ctx)
	return err
}

// ClaimDue 领取到期会话。通过 ZRem 的返回值保证多实例下同一会话只会被一个实例处理。
func (q *RedisEvolutionQueue) ClaimDue(ctx context.Context, now time.Time, limit int) ([]EvolutionCandidate, error) {
	if limit <= 0 {
		limit = 1
	}
	queueKey := cache.EvolutionQueueKey()
	members, err := q.client.ZRangeByScore(ctx, queueKey, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    strconv.FormatInt(now.UnixMilli(), 10),
		Offset: 0,
		Count:  int64(limit),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("evolution queue: zrangebyscore: %w", err)
	}

	out := make([]EvolutionCandidate, 0, len(members))
	for _, member := range members {
		claimed, cerr := q.client.ZRem(ctx, queueKey, member).Result()
		if cerr != nil {
			return out, fmt.Errorf("evolution queue: claim: %w", cerr)
		}
		if claimed == 0 {
			continue
		}
		candidate, derr := decodeEvolutionMember(member)
		if derr != nil {
			continue
		}
		// 重新放回并设置租约：任务成功/失败后由 Reschedule/Forget 覆盖，崩溃则租约到期后重试。
		if lerr := q.client.ZAdd(ctx, queueKey, redis.Z{
			Score:  float64(now.Add(evolutionClaimLease).UnixMilli()),
			Member: member,
		}).Err(); lerr != nil {
			return out, fmt.Errorf("evolution queue: lease: %w", lerr)
		}
		out = append(out, candidate)
	}
	return out, nil
}

// Finish 记录一轮完成并返回新的轮次。
func (q *RedisEvolutionQueue) Finish(ctx context.Context, tenantID, sessionID string) (int, error) {
	stateKey := cache.EvolutionStateKey(tenantID, sessionID)
	pipe := q.client.TxPipeline()
	incr := pipe.HIncrBy(ctx, stateKey, "round", 1)
	if q.ttl > 0 {
		pipe.Expire(ctx, stateKey, q.ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("evolution queue: finish round: %w", err)
	}
	return int(incr.Val()), nil
}

// Forget 移除会话的调度与状态。
func (q *RedisEvolutionQueue) Forget(ctx context.Context, tenantID, sessionID string) error {
	pipe := q.client.TxPipeline()
	pipe.ZRem(ctx, cache.EvolutionQueueKey(), encodeEvolutionMember(tenantID, sessionID))
	pipe.Del(ctx, cache.EvolutionStateKey(tenantID, sessionID))
	_, err := pipe.Exec(ctx)
	return err
}

func (q *RedisEvolutionQueue) expire(ctx context.Context, pipe redis.Pipeliner, queueKey, stateKey string) {
	if q.ttl <= 0 {
		return
	}
	pipe.Expire(ctx, queueKey, q.ttl)
	pipe.Expire(ctx, stateKey, q.ttl)
}

func encodeEvolutionMember(tenantID, sessionID string) string {
	return tenantID + evolutionMemberSep + sessionID
}

func decodeEvolutionMember(member string) (EvolutionCandidate, error) {
	parts := strings.SplitN(member, evolutionMemberSep, 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return EvolutionCandidate{}, fmt.Errorf("evolution queue: invalid member %q", member)
	}
	return EvolutionCandidate{TenantID: parts[0], SessionID: parts[1]}, nil
}

var _ EvolutionQueue = (*RedisEvolutionQueue)(nil)
