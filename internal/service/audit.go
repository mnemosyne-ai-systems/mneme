package service

import (
	"context"
	"sync"
	"time"

	"github.com/mnemosyne/mneme/pkg/models"
	"github.com/mnemosyne/mneme/pkg/storage"
)

// AuditAction 管理操作审计动作类型。
const (
	AuditActionDeleteUser     = "delete_user"
	AuditActionHardDeleteUser = "hard_delete_user"
	AuditActionDeleteSession  = "delete_session"
	AuditActionHardDelete     = "hard_delete_session"
	AuditActionExportUser     = "export_user"
	AuditActionExportSession  = "export_session"
	AuditActionSummarize      = "summarize_sessions"
)

// AuditStore 管理操作审计日志存储。
type AuditStore interface {
	Append(ctx context.Context, entry models.AuditEntry) error
	List(ctx context.Context, tenant string, limit int) ([]models.AuditEntry, error)
}

// ClickHouseAuditStore 使用 ClickHouse 存储审计日志（不再依赖 Redis）。
type ClickHouseAuditStore struct {
	inserter storage.BatchInserter
}

// NewClickHouseAuditStore 创建 ClickHouse 审计日志存储。
func NewClickHouseAuditStore(inserter storage.BatchInserter) *ClickHouseAuditStore {
	return &ClickHouseAuditStore{inserter: inserter}
}

// Append 写入一条审计日志。
func (s *ClickHouseAuditStore) Append(ctx context.Context, entry models.AuditEntry) error {
	if entry.At.IsZero() {
		entry.At = time.Now().UTC()
	}
	return s.inserter.InsertAuditLog(ctx, &entry)
}

// List 读取最近的审计日志（最新在前）。
func (s *ClickHouseAuditStore) List(ctx context.Context, tenant string, limit int) ([]models.AuditEntry, error) {
	if limit <= 0 {
		limit = 100
	}
	entries, err := s.inserter.QueryAuditLog(ctx, tenant, limit, 0)
	if err != nil {
		return nil, err
	}
	// 转换 []*AuditEntry 为 []AuditEntry
	out := make([]models.AuditEntry, len(entries))
	for i, e := range entries {
		if e != nil {
			out[i] = *e
		}
	}
	return out, nil
}

// MemoryAuditStore 内存实现，用于测试与无 ClickHouse 场景。
type MemoryAuditStore struct {
	mu     sync.RWMutex
	entries map[string][]models.AuditEntry
}

func (s *MemoryAuditStore) Append(_ context.Context, entry models.AuditEntry) error {
	if entry.At.IsZero() {
		entry.At = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[entry.TenantID] = append([]models.AuditEntry{entry}, s.entries[entry.TenantID]...)
	return nil
}

func (s *MemoryAuditStore) List(_ context.Context, tenant string, limit int) ([]models.AuditEntry, error) {
	if limit <= 0 {
		limit = 100
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	all := s.entries[tenant]
	if len(all) > limit {
		all = all[:limit]
	}
	out := make([]models.AuditEntry, len(all))
	copy(out, all)
	return out, nil
}

// NewMemoryAuditStore 创建内存审计日志存储（用于测试/无 ClickHouse 场景）。
func NewMemoryAuditStore() *MemoryAuditStore {
	return &MemoryAuditStore{entries: make(map[string][]models.AuditEntry)}
}

var (
	_ AuditStore = (*ClickHouseAuditStore)(nil)
	_ AuditStore = (*MemoryAuditStore)(nil)
)