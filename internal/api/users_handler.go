package api

import (
	"net/http"
	"time"

	"github.com/gorilla/mux"

	"github.com/mnemosyne/mneme/internal/service"
	"github.com/mnemosyne/mneme/pkg/logger"
	"github.com/mnemosyne/mneme/pkg/models"
)

// UsersHandler 聚合查询接口：同一用户的画像 + 其所有会话总结。
type UsersHandler struct {
	profiles service.ProfileStore
	sessions service.SessionSummaryStore
}

// NewUsersHandler 创建 handler。任一 store 可为 nil（对应部分返回空/404）。
func NewUsersHandler(profiles service.ProfileStore, sessions service.SessionSummaryStore) *UsersHandler {
	return &UsersHandler{profiles: profiles, sessions: sessions}
}

// GetContext 处理 GET /api/v1/users/{user_id}。
func (h *UsersHandler) GetContext(w http.ResponseWriter, r *http.Request) {
	tenant := tenantFrom(r)
	if tenant == "" {
		badRequest(w, CodeMissingTenant, "missing X-Tenant-ID header")
		return
	}
	vars := mux.Vars(r)
	userID := vars["user_id"]
	if userID == "" {
		badRequest(w, CodeMissingField, "missing user_id")
		return
	}

	var profile map[string]string
	if h.profiles != nil {
		p, err := h.profiles.GetProfile(r.Context(), tenant, models.EntityTypeUser, userID)
		if err != nil {
			logger.FromContext(r.Context()).WithError(err).Error("get profile failed")
			internalError(w, "failed to read profile")
			return
		}
		profile = p
	}

	var sessions []*models.SessionSummary
	if h.sessions != nil {
		list, err := h.sessions.ListSessionsByUser(r.Context(), tenant, userID)
		if err != nil {
			logger.FromContext(r.Context()).WithError(err).Error("list sessions failed")
			internalError(w, "failed to read sessions")
			return
		}
		sessions = list
	}

	if len(profile) == 0 && len(sessions) == 0 {
		writeError(w, http.StatusNotFound, CodeNotFound, "user context not found")
		return
	}
	writeOK(w, map[string]interface{}{
		"user_id":  userID,
		"tenant_id": tenant,
		"profile":  profile,
		"sessions": sessions,
	})
}

// CreateUser 处理 POST /api/v1/users。
func (h *UsersHandler) CreateUser(w http.ResponseWriter, r *http.Request) {
	tenant := tenantFrom(r)
	if tenant == "" {
		badRequest(w, CodeMissingTenant, "missing X-Tenant-ID header")
		return
	}
	if h.profiles == nil {
		writeError(w, http.StatusNotFound, CodeNotFound, "profile store not configured")
		return
	}

	var req struct {
		UserID  string                 `json:"user_id"`
		Profile map[string]string      `json:"profile"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		badRequest(w, CodeInvalidJSON, "invalid json body: "+err.Error())
		return
	}
	if req.UserID == "" {
		badRequest(w, CodeMissingField, "missing user_id")
		return
	}

	if err := h.profiles.CreateProfile(r.Context(), tenant, models.EntityTypeUser, req.UserID, req.Profile); err != nil {
		logger.FromContext(r.Context()).WithError(err).Error("create user failed")
		internalError(w, "failed to create user")
		return
	}
	writeOK(w, map[string]interface{}{"user_id": req.UserID, "created_at": time.Now().UTC()})
}
