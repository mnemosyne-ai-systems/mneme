package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/mnemosyne-labs/mneme/internal/service"
	"github.com/mnemosyne-labs/mneme/pkg/logger"
	"github.com/mnemosyne-labs/mneme/pkg/models"
)

// maxBodyBytes 限制单次请求体大小（4MB）。
const maxBodyBytes = 4 << 20

// maxBatchMessages 限制批量请求的消息数。
const maxBatchMessages = 1000

// HistoryHandler 管道一 HTTP 接入。
type HistoryHandler struct {
	ingest   *service.IngestService
	profiles service.ProfileStore
	sessions service.SessionSummaryStore
}

// NewHistoryHandler 创建 handler。profiles/sessions 可为 nil（跳过即时记录）。
func NewHistoryHandler(ingest *service.IngestService, profiles service.ProfileStore, sessions service.SessionSummaryStore) *HistoryHandler {
	return &HistoryHandler{ingest: ingest, profiles: profiles, sessions: sessions}
}

// historyRequest 是 POST /api/v1/history 的请求体。
type historyRequest struct {
	MessageID        string    `json:"message_id"`
	SessionID        string    `json:"session_id"`
	UserID           string    `json:"user_id"`
	Role             string    `json:"role"`
	Content          string    `json:"content"`
	ModelName        string    `json:"model_name"`
	TotalTokens      int32     `json:"total_tokens"`
	PromptTokens     int32     `json:"prompt_tokens"`
	CompletionTokens int32     `json:"completion_tokens"`
	ResponseTimeMs   int32     `json:"response_time_ms"`
	Metadata         string    `json:"metadata"`
	TenantID         string    `json:"tenant_id"`
	CreatedAt        time.Time `json:"created_at"`
}

func (r historyRequest) toModel(tenant string) *models.HistoryMessage {
	return &models.HistoryMessage{
		MessageID:        r.MessageID,
		TenantID:         tenant,
		SessionID:        r.SessionID,
		UserID:           r.UserID,
		Role:             r.Role,
		Content:          r.Content,
		ModelName:        r.ModelName,
		TotalTokens:      r.TotalTokens,
		PromptTokens:     r.PromptTokens,
		CompletionTokens: r.CompletionTokens,
		ResponseTimeMs:   r.ResponseTimeMs,
		Metadata:         r.Metadata,
		CreatedAt:        r.CreatedAt,
	}
}

// historyBatchRequest 是 POST /api/v1/history/batch 的请求体。
type historyBatchRequest struct {
	Messages []historyRequest `json:"messages"`
}

// Create 处理 POST /api/v1/history，异步投递后立即返回 202。
func (h *HistoryHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req historyRequest
	if err := decodeJSON(w, r, &req); err != nil {
		badRequest(w, CodeInvalidJSON, "invalid json body: "+err.Error())
		return
	}

	tenant := tenantFrom(r)
	if tenant == "" {
		tenant = req.TenantID
	}
	if tenant == "" {
		badRequest(w, CodeMissingTenant, "missing tenant: set X-Tenant-ID header or tenant_id field")
		return
	}

	msg := req.toModel(tenant)
	if err := h.ingest.PublishHistory(r.Context(), msg); err != nil {
		if ve, ok := err.(*models.ValidationError); ok {
			badRequest(w, CodeMissingField, ve.Error())
			return
		}
		logger.FromContext(r.Context()).WithError(err).Error("publish history failed")
		writeError(w, http.StatusServiceUnavailable, CodeUpstream, "failed to publish message")
		return
	}
	h.recordSessionInfo(r.Context(), tenant, msg)
	h.recordUserInfo(r.Context(), tenant, msg.UserID)

	writeAccepted(w, map[string]string{"message_id": msg.MessageID})
}

// CreateBatch 处理 POST /api/v1/history/batch，整批校验后原子投递。
// 租户解析规则与单条接口一致：X-Tenant-ID 头优先，其次各消息自带 tenant_id。
func (h *HistoryHandler) CreateBatch(w http.ResponseWriter, r *http.Request) {
	var req historyBatchRequest
	if err := decodeJSON(w, r, &req); err != nil {
		badRequest(w, CodeInvalidJSON, "invalid json body: "+err.Error())
		return
	}
	if len(req.Messages) == 0 {
		badRequest(w, CodeMissingField, "messages must not be empty")
		return
	}
	if len(req.Messages) > maxBatchMessages {
		badRequest(w, CodeBadRequest, fmt.Sprintf("messages count %d exceeds limit %d", len(req.Messages), maxBatchMessages))
		return
	}

	headerTenant := tenantFrom(r)
	msgs := make([]*models.HistoryMessage, 0, len(req.Messages))
	for i := range req.Messages {
		tenant := headerTenant
		if tenant == "" {
			tenant = req.Messages[i].TenantID
		}
		if tenant == "" {
			badRequest(w, CodeMissingTenant,
				fmt.Sprintf("messages[%d]: missing tenant: set X-Tenant-ID header or tenant_id field", i))
			return
		}
		msgs = append(msgs, req.Messages[i].toModel(tenant))
	}

	if err := h.ingest.PublishHistoryBatch(r.Context(), msgs); err != nil {
		if ve, ok := err.(*models.ValidationError); ok {
			badRequest(w, CodeMissingField, ve.Error())
			return
		}
		logger.FromContext(r.Context()).WithError(err).Error("publish history batch failed")
		writeError(w, http.StatusServiceUnavailable, CodeUpstream, "failed to publish messages")
		return
	}
	for _, m := range msgs {
		h.recordSessionInfo(r.Context(), m.TenantID, m)
		h.recordUserInfo(r.Context(), m.TenantID, m.UserID)
	}

	writeAccepted(w, map[string]int{"accepted": len(msgs)})
}

// decodeJSON 解析请求体，限制大小并禁止未知字段以外的宽松处理。
func decodeJSON(w http.ResponseWriter, r *http.Request, dst interface{}) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(dst); err != nil {
		return err
	}
	// 拒绝多余的 JSON 文档
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return errMultipleJSONObjects
	}
	return nil
}

// recordSessionInfo 在消息到达时立即记录会话基本信息（不阻塞主流程，忽略错误）。
func (h *HistoryHandler) recordSessionInfo(ctx context.Context, tenant string, msg *models.HistoryMessage) {
	if h.sessions == nil || msg.SessionID == "" {
		return
	}
	now := time.Now().UTC()
	ts := now
	if !msg.CreatedAt.IsZero() {
		ts = msg.CreatedAt
	}
	_ = h.sessions.SaveSessionInfo(ctx, tenant, msg.SessionID, msg.UserID, ts, ts)
}

// recordUserInfo 在消息到达时立即记录最小用户信息（不阻塞主流程，忽略错误）。
func (h *HistoryHandler) recordUserInfo(ctx context.Context, tenant, userID string) {
	if h.profiles == nil || userID == "" {
		return
	}
	_ = h.profiles.SaveUserInfo(ctx, tenant, userID)
}
