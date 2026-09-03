package api

import (
	"context"
	"net/http"
	"time"

	"github.com/gorilla/mux"

	"github.com/mnemosyne/mneme/pkg/logger"
	"github.com/mnemosyne/mneme/pkg/models"
)

// SessionSummaryReader 用于读取会话总结。
type SessionSummaryReader interface {
	GetSummary(ctx context.Context, tenant, sessionID string) (*models.SessionSummary, error)
}

// SessionSummaryWriter 用于创建会话。
type SessionSummaryWriter interface {
	CreateSession(ctx context.Context, summary *models.SessionSummary) error
}

// SessionsHandler 会话级总结查询与创建接口。
type SessionsHandler struct {
	reader SessionSummaryReader
	writer SessionSummaryWriter
}

// NewSessionsHandler 创建 handler。reader/writer 可为 nil。
func NewSessionsHandler(reader SessionSummaryReader, writer SessionSummaryWriter) *SessionsHandler {
	return &SessionsHandler{reader: reader, writer: writer}
}

// GetSummary 处理 GET /api/v1/sessions/{session_id}。
func (h *SessionsHandler) GetSummary(w http.ResponseWriter, r *http.Request) {
	tenant := tenantFrom(r)
	if tenant == "" {
		badRequest(w, CodeMissingTenant, "missing X-Tenant-ID header")
		return
	}
	vars := mux.Vars(r)
	sessionID := vars["session_id"]
	if sessionID == "" {
		badRequest(w, CodeMissingField, "missing session_id")
		return
	}

	if h.reader == nil {
		writeError(w, http.StatusNotFound, CodeNotFound, "session summary store not configured")
		return
	}
	summary, err := h.reader.GetSummary(r.Context(), tenant, sessionID)
	if err != nil {
		logger.FromContext(r.Context()).WithError(err).Error("get session summary failed")
		internalError(w, "failed to read session summary")
		return
	}
	if summary == nil {
		writeError(w, http.StatusNotFound, CodeNotFound, "session summary not found")
		return
	}
	writeOK(w, summary)
}

// CreateSession 处理 POST /api/v1/sessions。
func (h *SessionsHandler) CreateSession(w http.ResponseWriter, r *http.Request) {
	tenant := tenantFrom(r)
	if tenant == "" {
		badRequest(w, CodeMissingTenant, "missing X-Tenant-ID header")
		return
	}
	if h.writer == nil {
		writeError(w, http.StatusNotFound, CodeNotFound, "session writer not configured")
		return
	}

	var req struct {
		SessionID string `json:"session_id"`
		UserID    string `json:"user_id"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		badRequest(w, CodeInvalidJSON, "invalid json body: "+err.Error())
		return
	}
	if req.SessionID == "" {
		badRequest(w, CodeMissingField, "missing session_id")
		return
	}
	if req.UserID == "" {
		badRequest(w, CodeMissingField, "missing user_id")
		return
	}

	now := time.Now().UTC()
	summary := &models.SessionSummary{
		SessionID:    req.SessionID,
		TenantID:     tenant,
		UserID:       req.UserID,
		Summary:      "",
		MessageCount: 0,
		StartedAt:    now,
		EndedAt:      now,
	}

	if err := h.writer.CreateSession(r.Context(), summary); err != nil {
		logger.FromContext(r.Context()).WithError(err).Error("create session failed")
		internalError(w, "failed to create session")
		return
	}
	writeOK(w, map[string]interface{}{"session_id": req.SessionID, "created_at": now})
}
