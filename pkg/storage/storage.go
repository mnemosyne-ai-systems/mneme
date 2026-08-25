// Package storage 定义 Mneme 的持久化抽象。
package storage

import (
	"context"

	"github.com/mnemosyne/mneme/pkg/models"
)

// BatchInserter 是三条管道共用的批量写入抽象。
type BatchInserter interface {
	InsertHistory(ctx context.Context, messages []*models.HistoryMessage) error
	InsertTraces(ctx context.Context, traces []*models.TraceMessage) error
	InsertMemory(ctx context.Context, memories []*models.EntityMemory) error
	Flush() error
	Close() error
}

// Pipeline 名称常量，用于指标标签。
const (
	PipelineHistory = "history"
	PipelineTraces  = "traces"
	PipelineMemory  = "memory"
)
