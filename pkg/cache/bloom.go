package cache

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/redis/go-redis/v9"
)

// BloomFilter 幂等去重抽象。
type BloomFilter interface {
	Add(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)
}

// RedisBloomFilter 基于 Redis Bitmap 实现的布隆过滤器。
// 若 Redis 已安装 RedisBloom 模块，可通过 UseModule 切换到 BF.ADD/BF.EXISTS。
type RedisBloomFilter struct {
	client    redis.UniversalClient
	key       string
	bits      uint64
	hashCount uint64
	useModule bool
}

// BloomOptions 布隆过滤器参数。
type BloomOptions struct {
	// Key 是完整的 Redis Key（建议使用 BloomKey 生成）。
	Key string
	// Bits 位图大小，默认 1<<24（2MB，约千万级容量）。
	Bits uint64
	// HashCount 哈希函数个数，默认 7。
	HashCount uint64
	// UseModule 为 true 时使用 RedisBloom 模块命令。
	UseModule bool
}

// NewRedisBloomFilter 创建布隆过滤器。
func NewRedisBloomFilter(client redis.UniversalClient, opts BloomOptions) *RedisBloomFilter {
	if opts.Bits == 0 {
		opts.Bits = 1 << 24
	}
	if opts.HashCount == 0 {
		opts.HashCount = 7
	}
	return &RedisBloomFilter{
		client:    client,
		key:       opts.Key,
		bits:      opts.Bits,
		hashCount: opts.HashCount,
		useModule: opts.UseModule,
	}
}

// Add 将 key 标记为已存在。
func (b *RedisBloomFilter) Add(ctx context.Context, key string) error {
	if b.useModule {
		if err := b.client.Do(ctx, "BF.ADD", b.key, key).Err(); err != nil {
			return fmt.Errorf("bloom: BF.ADD: %w", err)
		}
		return nil
	}
	pipe := b.client.Pipeline()
	for _, offset := range b.offsets(key) {
		pipe.SetBit(ctx, b.key, int64(offset), 1)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("bloom: setbit: %w", err)
	}
	return nil
}

// Exists 判断 key 是否可能已存在（存在假阳性，无假阴性）。
func (b *RedisBloomFilter) Exists(ctx context.Context, key string) (bool, error) {
	if b.useModule {
		v, err := b.client.Do(ctx, "BF.EXISTS", b.key, key).Int64()
		if err != nil {
			return false, fmt.Errorf("bloom: BF.EXISTS: %w", err)
		}
		return v == 1, nil
	}
	pipe := b.client.Pipeline()
	cmds := make([]*redis.IntCmd, 0, b.hashCount)
	for _, offset := range b.offsets(key) {
		cmds = append(cmds, pipe.GetBit(ctx, b.key, int64(offset)))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return false, fmt.Errorf("bloom: getbit: %w", err)
	}
	for _, cmd := range cmds {
		if cmd.Val() == 0 {
			return false, nil
		}
	}
	return true, nil
}

// checkAndAddScript 原子执行"检查全部位 → 若均已置 1 返回 1（已存在），
// 否则写入全部位并返回 0（新添加）"，消除 Exists/Add 两步之间的竞态窗口。
var checkAndAddScript = redis.NewScript(`
local key = KEYS[1]
for i = 1, #ARGV do
    if redis.call('GETBIT', key, tonumber(ARGV[i])) == 0 then
        for j = 1, #ARGV do
            redis.call('SETBIT', key, tonumber(ARGV[j]), 1)
        end
        return 0
    end
end
return 1
`)

// CheckAndAdd 原子"检查后写入"，返回 true 表示此前已存在（重复）。
func (b *RedisBloomFilter) CheckAndAdd(ctx context.Context, key string) (bool, error) {
	if b.useModule {
		// BF.ADD 返回 1 表示元素已存在（未写入），0 表示新添加。
		v, err := b.client.Do(ctx, "BF.ADD", b.key, key).Int64()
		if err != nil {
			return false, fmt.Errorf("bloom: BF.ADD: %w", err)
		}
		return v == 1, nil
	}
	args := make([]interface{}, 0, b.hashCount)
	for _, offset := range b.offsets(key) {
		args = append(args, offset)
	}
	v, err := checkAndAddScript.Run(ctx, b.client, []string{b.key}, args...).Int64()
	if err != nil {
		return false, fmt.Errorf("bloom: check-and-add: %w", err)
	}
	return v == 1, nil
}

// offsets 使用双重哈希生成 hashCount 个位偏移。
func (b *RedisBloomFilter) offsets(key string) []uint64 {
	h1 := fnv.New64a()
	_, _ = h1.Write([]byte(key))
	a := h1.Sum64()

	h2 := fnv.New64()
	_, _ = h2.Write([]byte("salt:" + key))
	c := h2.Sum64()
	if c == 0 {
		c = 0x9e3779b97f4a7c15
	}

	offsets := make([]uint64, 0, b.hashCount)
	for i := uint64(0); i < b.hashCount; i++ {
		offsets = append(offsets, (a+i*c)%b.bits)
	}
	return offsets
}

// MemoryBloomFilter 是进程内布隆过滤器（精确集合），用于测试与无 Redis 场景。
type MemoryBloomFilter struct {
	mu   sync.RWMutex
	seen map[string]struct{}
}

// NewMemoryBloomFilter 创建内存去重器。
func NewMemoryBloomFilter() *MemoryBloomFilter {
	return &MemoryBloomFilter{seen: make(map[string]struct{})}
}

// Add 添加 key。
func (m *MemoryBloomFilter) Add(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seen[key] = struct{}{}
	return nil
}

// Exists 判断 key 是否存在。
func (m *MemoryBloomFilter) Exists(_ context.Context, key string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.seen[key]
	return ok, nil
}

// CheckAndAdd 检查并添加。
func (m *MemoryBloomFilter) CheckAndAdd(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.seen[key]; ok {
		return true, nil
	}
	m.seen[key] = struct{}{}
	return false, nil
}

// Len 返回已记录的 key 数量。
func (m *MemoryBloomFilter) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.seen)
}

// Deduper 是消费者使用的去重能力（Redis / 内存实现均满足）。
type Deduper interface {
	CheckAndAdd(ctx context.Context, key string) (bool, error)
}

var (
	_ BloomFilter = (*RedisBloomFilter)(nil)
	_ BloomFilter = (*MemoryBloomFilter)(nil)
	_ Deduper     = (*RedisBloomFilter)(nil)
	_ Deduper     = (*MemoryBloomFilter)(nil)
)
