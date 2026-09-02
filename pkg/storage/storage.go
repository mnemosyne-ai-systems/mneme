// Package storage 定义 Mneme 的持久化抽象。
package storage

import (
	"context"

	"github.com/mnemosyne/mneme/pkg/models"
)

// BatchInserter 是各管道共用的批量写入抽象。
type BatchInserter interface {
	InsertHistory(ctx context.Context, messages []*models.HistoryMessage) error
	InsertTraces(ctx context.Context, traces []*models.TraceMessage) error
	InsertMemory(ctx context.Context, memories []*models.EntityMemory) error
	InsertSessionSummary(ctx context.Context, summaries []*models.SessionSummary) error
	// QueryHistoryBySession 查询某会话的完整对话历史（按时间升序）。
	QueryHistoryBySession(ctx context.Context, tenant, sessionID string, limit, offset int) ([]*models.HistoryMessage, error)
	Flush() error
	Close() error
}

// Pipeline 名称常量，用于指标标签。
const (
	PipelineHistory = "history"
	PipelineTraces  = "traces"
	PipelineMemory  = "memory"
	PipelineSession = "session"
)
