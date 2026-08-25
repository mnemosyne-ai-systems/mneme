package api

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/mnemosyne/mneme/internal/service"
	"github.com/mnemosyne/mneme/pkg/logger"
	"github.com/mnemosyne/mneme/pkg/models"
)

// maxBodyBytes 限制单次请求体大小（4MB）。
const maxBodyBytes = 4 << 20

// HistoryHandler 管道一 HTTP 接入。
type HistoryHandler struct {
	ingest *service.IngestService
}

// NewHistoryHandler 创建 handler。
func NewHistoryHandler(ingest *service.IngestService) *HistoryHandler {
	return &HistoryHandler{ingest: ingest}
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

	writeAccepted(w, map[string]string{"message_id": msg.MessageID})
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
