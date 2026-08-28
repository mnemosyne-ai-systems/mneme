package api

import (
	"context"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/mnemosyne/mneme/pkg/logger"
	"github.com/mnemosyne/mneme/pkg/models"
)

// SessionSummaryReader 用于读取会话总结。
type SessionSummaryReader interface {
	GetSummary(ctx context.Context, tenant, sessionID string) (*models.SessionSummary, error)
}

// SessionsHandler 会话级总结查询接口。
type SessionsHandler struct {
	store SessionSummaryReader
}

// NewSessionsHandler 创建 handler。store 可为 nil（此时查询返回 404）。
func NewSessionsHandler(store SessionSummaryReader) *SessionsHandler {
	return &SessionsHandler{store: store}
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

	if h.store == nil {
		writeError(w, http.StatusNotFound, CodeNotFound, "session summary store not configured")
		return
	}
	summary, err := h.store.GetSummary(r.Context(), tenant, sessionID)
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
