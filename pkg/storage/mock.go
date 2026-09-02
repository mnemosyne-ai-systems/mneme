package storage

import (
	"context"
	"sync"

	"github.com/mnemosyne/mneme/pkg/models"
)

// MockInserter 是 BatchInserter 的内存实现，用于单元测试。
type MockInserter struct {
	mu       sync.Mutex
	History  []*models.HistoryMessage
	Traces   []*models.TraceMessage
	Memories []*models.EntityMemory
	Flushes  int
	Closed   bool
	// Err 非空时所有写入返回该错误。
	Err error

	Summaries  []*models.SessionSummary
	AuditLogs []*models.AuditEntry
}

// NewMockInserter 创建 Mock 写入器。
func NewMockInserter() *MockInserter { return &MockInserter{} }

// InsertHistory 记录写入的历史消息。
func (m *MockInserter) InsertHistory(_ context.Context, messages []*models.HistoryMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	m.History = append(m.History, messages...)
	return nil
}

// InsertTraces 记录写入的轨迹。
func (m *MockInserter) InsertTraces(_ context.Context, traces []*models.TraceMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	m.Traces = append(m.Traces, traces...)
	return nil
}

// InsertMemory 记录写入的画像。
func (m *MockInserter) InsertMemory(_ context.Context, memories []*models.EntityMemory) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	m.Memories = append(m.Memories, memories...)
	return nil
}

// InsertSessionSummary 记录写入的会话总结。
func (m *MockInserter) InsertSessionSummary(_ context.Context, summaries []*models.SessionSummary) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	m.Summaries = append(m.Summaries, summaries...)
	return nil
}

// InsertAuditLog 记录写入的审计日志。
func (m *MockInserter) InsertAuditLog(_ context.Context, entry *models.AuditEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	m.AuditLogs = append(m.AuditLogs, entry)
	return nil
}

// QueryAuditLog 返回审计日志（按时间倒序）。
func (m *MockInserter) QueryAuditLog(_ context.Context, tenant string, limit, offset int) ([]*models.AuditEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*models.AuditEntry
	for _, e := range m.AuditLogs {
		if e.TenantID == tenant {
			out = append(out, e)
		}
	}
	// 倒序
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	if offset > len(out) {
		offset = len(out)
	}
	end := offset + limit
	if end > len(out) {
		end = len(out)
	}
	return out[offset:end], nil
}

// Flush 记录刷新次数。
func (m *MockInserter) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Flushes++
	return nil
}

// Close 标记关闭。
func (m *MockInserter) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Closed = true
	return nil
}

// HistoryCount 返回已写入历史条数。
func (m *MockInserter) HistoryCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.History)
}

// TraceCount 返回已写入轨迹条数。
func (m *MockInserter) TraceCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.Traces)
}

// MemoryCount 返回已写入画像条数。
func (m *MockInserter) MemoryCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.Memories)
}

// SummaryCount 返回已写入会话总结条数。
func (m *MockInserter) SummaryCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.Summaries)
}

// QueryHistoryBySession 返回指定会话的历史消息（内存过滤）。
func (m *MockInserter) QueryHistoryBySession(_ context.Context, tenant, sessionID string, limit, offset int) ([]*models.HistoryMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*models.HistoryMessage
	for _, m := range m.History {
		if m.TenantID == tenant && m.SessionID == sessionID {
			out = append(out, m)
		}
	}
	if offset > len(out) {
		offset = len(out)
	}
	end := offset + limit
	if end > len(out) {
		end = len(out)
	}
	return out[offset:end], nil
}

var _ BatchInserter = (*MockInserter)(nil)
