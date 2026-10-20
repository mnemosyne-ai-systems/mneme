package service

import (
	"context"

	"github.com/mnemosyne-ai-systems/mneme/pkg/models"
)

// HistorySink 抽象会话历史消息的投递目标。
// IngestService（进程内直投 Pulsar）与 HTTPHistorySink（推送到服务端批量 API）
// 均实现此接口，文件采集器等接入方只依赖该抽象，与消息总线拓扑解耦。
type HistorySink interface {
	PublishHistoryBatch(ctx context.Context, msgs []*models.HistoryMessage) error
}

// historyBatchPayload 是批量投递的传输结构，
// 与服务端 POST /api/v1/history/batch 的请求体保持一致。
type historyBatchPayload struct {
	Messages []*models.HistoryMessage `json:"messages"`
}

var (
	_ HistorySink = (*IngestService)(nil)
	_ HistorySink = (*HTTPHistorySink)(nil)
)
