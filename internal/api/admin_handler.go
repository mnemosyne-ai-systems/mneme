package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"

	"github.com/mnemosyne/mneme/internal/service"
	"github.com/mnemosyne/mneme/pkg/logger"
	"github.com/mnemosyne/mneme/pkg/models"
)

// AdminHandler 轻量后台管理接口：用户/画像浏览、会话总结检索、数据治理（删除/导出）。
// 不做复杂鉴权，仅支持可选静态令牌（Token）薄保护。
type AdminHandler struct {
	profiles service.ProfileStore
	sessions service.SessionSummaryStore
	token    string
}

// NewAdminHandler 创建后台管理 handler。profiles/sessions 可为 nil（对应功能返回 404）。
func NewAdminHandler(profiles service.ProfileStore, sessions service.SessionSummaryStore, token string) *AdminHandler {
	return &AdminHandler{profiles: profiles, sessions: sessions, token: token}
}

// Register 注册后台管理路由到给定子路由（前缀 /admin/api）。
func (h *AdminHandler) Register(r *mux.Router) {
	r.HandleFunc("/users", h.ListUsers).Methods(http.MethodGet)
	r.HandleFunc("/users/{user_id}", h.GetUser).Methods(http.MethodGet)
	r.HandleFunc("/users/{user_id}/export", h.ExportUser).Methods(http.MethodGet)
	r.HandleFunc("/users/{user_id}", h.DeleteUser).Methods(http.MethodDelete)
	r.HandleFunc("/sessions", h.SearchSessions).Methods(http.MethodGet)
	r.HandleFunc("/sessions/{session_id}", h.GetSession).Methods(http.MethodGet)
	r.HandleFunc("/sessions/{session_id}/export", h.ExportSession).Methods(http.MethodGet)
	r.HandleFunc("/sessions/{session_id}", h.DeleteSession).Methods(http.MethodDelete)
}

func (h *AdminHandler) authorize(r *http.Request) bool {
	if h.token == "" {
		return true
	}
	if r.Header.Get("Authorization") == "Bearer "+h.token {
		return true
	}
	// 导出类 GET 链接无法带请求头，允许通过查询参数附带令牌。
	q := r.URL.Query()
	return (q.Get("token") == h.token) || (q.Get("__t") == h.token)
}

func (h *AdminHandler) tenant(r *http.Request) string {
	return tenantFrom(r)
}

// ListUsers 处理 GET /admin/api/users。
func (h *AdminHandler) ListUsers(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(r) {
		writeError(w, http.StatusUnauthorized, CodeUnauthorized, "invalid admin token")
		return
	}
	tenant := h.tenant(r)
	if tenant == "" {
		badRequest(w, CodeMissingTenant, "missing X-Tenant-ID header")
		return
	}
	if h.profiles == nil {
		writeError(w, http.StatusNotFound, CodeNotFound, "profile store not configured")
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	list, err := h.profiles.ListProfiles(r.Context(), tenant, models.EntityTypeUser, service.ListOptions{Limit: limit})
	if err != nil {
		logger.FromContext(r.Context()).WithError(err).Error("admin list users failed")
		internalError(w, "failed to list users")
		return
	}
	out := make([]map[string]interface{}, 0, len(list))
	for _, e := range list {
		out = append(out, map[string]interface{}{
			"user_id": e.EntityID,
			"profile": e.Profile,
		})
	}
	writeOK(w, map[string]interface{}{"tenant_id": tenant, "users": out, "count": len(out)})
}

// GetUser 处理 GET /admin/api/users/{user_id}（聚合画像 + 会话列表）。
func (h *AdminHandler) GetUser(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(r) {
		writeError(w, http.StatusUnauthorized, CodeUnauthorized, "invalid admin token")
		return
	}
	tenant := h.tenant(r)
	if tenant == "" {
		badRequest(w, CodeMissingTenant, "missing X-Tenant-ID header")
		return
	}
	userID := mux.Vars(r)["user_id"]

	var profile map[string]string
	if h.profiles != nil {
		p, err := h.profiles.GetProfile(r.Context(), tenant, models.EntityTypeUser, userID)
		if err != nil {
			logger.FromContext(r.Context()).WithError(err).Error("admin get profile failed")
			internalError(w, "failed to read profile")
			return
		}
		profile = p
	}
	var sessions []*models.SessionSummary
	if h.sessions != nil {
		list, err := h.sessions.ListSessionsByUser(r.Context(), tenant, userID)
		if err != nil {
			logger.FromContext(r.Context()).WithError(err).Error("admin list sessions failed")
			internalError(w, "failed to read sessions")
			return
		}
		sessions = list
	}
	if len(profile) == 0 && len(sessions) == 0 {
		writeError(w, http.StatusNotFound, CodeNotFound, "user not found")
		return
	}
	writeOK(w, map[string]interface{}{"user_id": userID, "tenant_id": tenant, "profile": profile, "sessions": sessions})
}

// ExportUser 处理 GET /admin/api/users/{user_id}/export（JSON 导出）。
func (h *AdminHandler) ExportUser(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(r) {
		writeError(w, http.StatusUnauthorized, CodeUnauthorized, "invalid admin token")
		return
	}
	tenant := h.tenant(r)
	if tenant == "" {
		badRequest(w, CodeMissingTenant, "missing X-Tenant-ID header")
		return
	}
	userID := mux.Vars(r)["user_id"]
	profile, _ := h.profiles.GetProfile(r.Context(), tenant, models.EntityTypeUser, userID)
	sessions, _ := h.sessions.ListSessionsByUser(r.Context(), tenant, userID)
	body := map[string]interface{}{"user_id": userID, "tenant_id": tenant, "profile": profile, "sessions": sessions}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", "attachment; filename=user_"+userID+".json")
	_ = json.NewEncoder(w).Encode(body)
}

// DeleteUser 处理 DELETE /admin/api/users/{user_id}（数据治理：删画像 + 其全部会话）。
func (h *AdminHandler) DeleteUser(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(r) {
		writeError(w, http.StatusUnauthorized, CodeUnauthorized, "invalid admin token")
		return
	}
	tenant := h.tenant(r)
	if tenant == "" {
		badRequest(w, CodeMissingTenant, "missing X-Tenant-ID header")
		return
	}
	userID := mux.Vars(r)["user_id"]
	if h.profiles != nil {
		if err := h.profiles.DeleteProfile(r.Context(), tenant, models.EntityTypeUser, userID); err != nil {
			logger.FromContext(r.Context()).WithError(err).Error("admin delete profile failed")
			internalError(w, "failed to delete profile")
			return
		}
	}
	if h.sessions != nil {
		if err := h.sessions.DeleteSessionsByUser(r.Context(), tenant, userID); err != nil {
			logger.FromContext(r.Context()).WithError(err).Error("admin delete sessions failed")
			internalError(w, "failed to delete sessions")
			return
		}
	}
	writeOK(w, map[string]interface{}{"deleted": userID})
}

// SearchSessions 处理 GET /admin/api/sessions?user_id=&q=&limit=。
func (h *AdminHandler) SearchSessions(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(r) {
		writeError(w, http.StatusUnauthorized, CodeUnauthorized, "invalid admin token")
		return
	}
	tenant := h.tenant(r)
	if tenant == "" {
		badRequest(w, CodeMissingTenant, "missing X-Tenant-ID header")
		return
	}
	if h.sessions == nil {
		writeError(w, http.StatusNotFound, CodeNotFound, "session store not configured")
		return
	}
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	opts := service.SessionSearchOptions{
		UserID: q.Get("user_id"),
		Query:  strings.TrimSpace(q.Get("q")),
		Limit:  limit,
	}
	list, err := h.sessions.SearchSessions(r.Context(), tenant, opts)
	if err != nil {
		logger.FromContext(r.Context()).WithError(err).Error("admin search sessions failed")
		internalError(w, "failed to search sessions")
		return
	}
	writeOK(w, map[string]interface{}{"tenant_id": tenant, "sessions": list, "count": len(list)})
}

// GetSession 处理 GET /admin/api/sessions/{session_id}。
func (h *AdminHandler) GetSession(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(r) {
		writeError(w, http.StatusUnauthorized, CodeUnauthorized, "invalid admin token")
		return
	}
	tenant := h.tenant(r)
	if tenant == "" {
		badRequest(w, CodeMissingTenant, "missing X-Tenant-ID header")
		return
	}
	sessionID := mux.Vars(r)["session_id"]
	if h.sessions == nil {
		writeError(w, http.StatusNotFound, CodeNotFound, "session store not configured")
		return
	}
	sum, err := h.sessions.GetSummary(r.Context(), tenant, sessionID)
	if err != nil {
		logger.FromContext(r.Context()).WithError(err).Error("admin get session failed")
		internalError(w, "failed to read session")
		return
	}
	if sum == nil {
		writeError(w, http.StatusNotFound, CodeNotFound, "session not found")
		return
	}
	writeOK(w, sum)
}

// ExportSession 处理 GET /admin/api/sessions/{session_id}/export。
func (h *AdminHandler) ExportSession(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(r) {
		writeError(w, http.StatusUnauthorized, CodeUnauthorized, "invalid admin token")
		return
	}
	tenant := h.tenant(r)
	if tenant == "" {
		badRequest(w, CodeMissingTenant, "missing X-Tenant-ID header")
		return
	}
	sessionID := mux.Vars(r)["session_id"]
	if h.sessions == nil {
		writeError(w, http.StatusNotFound, CodeNotFound, "session store not configured")
		return
	}
	sum, err := h.sessions.GetSummary(r.Context(), tenant, sessionID)
	if err != nil {
		internalError(w, "failed to read session")
		return
	}
	if sum == nil {
		writeError(w, http.StatusNotFound, CodeNotFound, "session not found")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", "attachment; filename=session_"+sessionID+".json")
	_ = json.NewEncoder(w).Encode(sum)
}

// DeleteSession 处理 DELETE /admin/api/sessions/{session_id}。
func (h *AdminHandler) DeleteSession(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(r) {
		writeError(w, http.StatusUnauthorized, CodeUnauthorized, "invalid admin token")
		return
	}
	tenant := h.tenant(r)
	if tenant == "" {
		badRequest(w, CodeMissingTenant, "missing X-Tenant-ID header")
		return
	}
	sessionID := mux.Vars(r)["session_id"]
	if h.sessions == nil {
		writeError(w, http.StatusNotFound, CodeNotFound, "session store not configured")
		return
	}
	if err := h.sessions.DeleteSession(r.Context(), tenant, sessionID); err != nil {
		logger.FromContext(r.Context()).WithError(err).Error("admin delete session failed")
		internalError(w, "failed to delete session")
		return
	}
	writeOK(w, map[string]interface{}{"deleted": sessionID})
}
