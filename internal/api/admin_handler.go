package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"github.com/mnemosyne/mneme/internal/service"
	"github.com/mnemosyne/mneme/pkg/logger"
	"github.com/mnemosyne/mneme/pkg/models"
	"github.com/mnemosyne/mneme/pkg/storage"
)

// SessionSummarizer 可触发即时会话总结的接口（对应 SessionSummaryConsumer.RunSummarization）。
type SessionSummarizer interface {
	RunSummarization(ctx context.Context) (int, error)
}

// AdminHandler 轻量后台管理接口：用户/画像浏览、会话总结检索、数据治理（软删除/硬删除/导出）。
// 不做复杂鉴权，仅支持可选静态令牌（Token）薄保护；所有变更操作记录审计日志。
type AdminHandler struct {
	profiles   service.ProfileStore
	sessions   service.SessionSummaryStore
	summarizer SessionSummarizer
	audit      service.AuditStore
	inserter   storage.BatchInserter
	token      string
}

// NewAdminHandler 创建后台管理 handler。profiles/sessions/audit/summarizer/inserter 可为 nil。
func NewAdminHandler(profiles service.ProfileStore, sessions service.SessionSummaryStore, audit service.AuditStore, token string, summarizer SessionSummarizer, inserter storage.BatchInserter) *AdminHandler {
	return &AdminHandler{profiles: profiles, sessions: sessions, audit: audit, token: token, summarizer: summarizer, inserter: inserter}
}

// Register 注册后台管理路由到给定子路由（前缀 /admin/api）。
func (h *AdminHandler) Register(r *mux.Router) {
	r.HandleFunc("/users", h.ListUsers).Methods(http.MethodGet)
	r.HandleFunc("/users/{user_id}", h.GetUser).Methods(http.MethodGet)
	r.HandleFunc("/users/{user_id}/export", h.ExportUser).Methods(http.MethodGet)
	r.HandleFunc("/users/{user_id}", h.DeleteUser).Methods(http.MethodDelete)
	r.HandleFunc("/sessions", h.SearchSessions).Methods(http.MethodGet)
	r.HandleFunc("/sessions/{session_id}", h.GetSession).Methods(http.MethodGet)
	r.HandleFunc("/sessions/{session_id}/history", h.GetSessionHistory).Methods(http.MethodGet)
	r.HandleFunc("/sessions/{session_id}/export", h.ExportSession).Methods(http.MethodGet)
	r.HandleFunc("/sessions/{session_id}", h.DeleteSession).Methods(http.MethodDelete)
	r.HandleFunc("/sessions/summarize", h.SummarizeSessions).Methods(http.MethodPost)
	r.HandleFunc("/index/rebuild", h.RebuildIndex).Methods(http.MethodPost)
	r.HandleFunc("/audit", h.ListAudit).Methods(http.MethodGet)
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

// actor 返回操作人标识：优先取 Bearer 令牌，否则记为 "admin"。
func (h *AdminHandler) actor(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimPrefix(auth, "Bearer ")
	}
	return "admin"
}

// recordAudit 记录一条管理操作审计日志（audit 为 nil 时安全跳过）。
func (h *AdminHandler) recordAudit(r *http.Request, action, target, detail string) {
	if h.audit == nil {
		return
	}
	if err := h.audit.Append(r.Context(), models.AuditEntry{
		TenantID: h.tenant(r),
		Actor:    h.actor(r),
		Action:   action,
		Target:   target,
		Detail:   detail,
	}); err != nil {
		logger.FromContext(r.Context()).WithError(err).Warn("admin audit append failed")
	}
}

func (h *AdminHandler) tenant(r *http.Request) string {
	return tenantFrom(r)
}

// ListUsers 处理 GET /admin/api/users?limit=&cursor=。
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
	list, next, err := h.profiles.ListProfiles(r.Context(), tenant, models.EntityTypeUser, service.ListOptions{
		Limit:  limit,
		Cursor: r.URL.Query().Get("cursor"),
	})
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
	writeOK(w, map[string]interface{}{"tenant_id": tenant, "users": out, "count": len(out), "next_cursor": next})
}

// GetUser 处理 GET /admin/api/users/{user_id}（聚合画像 + 会话列表，会话按时间倒序）。
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
	var sessions []*models.SessionSummary
	if h.sessions != nil {
		sessions, _ = h.sessions.ListSessionsByUser(r.Context(), tenant, userID)
	}
	body := map[string]interface{}{"user_id": userID, "tenant_id": tenant, "profile": profile, "sessions": sessions}
	h.recordAudit(r, models.AuditActionExportUser, userID, "")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", "attachment; filename=user_"+userID+".json")
	_ = json.NewEncoder(w).Encode(body)
}

// DeleteUser 处理 DELETE /admin/api/users/{user_id}?hard=。默认软删除（可恢复），hard=true 为硬删除。
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
	hard := r.URL.Query().Get("hard") == "true"

	if h.profiles != nil {
		var err error
		if hard {
			err = h.profiles.PurgeProfile(r.Context(), tenant, models.EntityTypeUser, userID)
		} else {
			err = h.profiles.DeleteProfile(r.Context(), tenant, models.EntityTypeUser, userID)
		}
		if err != nil {
			logger.FromContext(r.Context()).WithError(err).Error("admin delete profile failed")
			internalError(w, "failed to delete profile")
			return
		}
	}
	if h.sessions != nil {
		var err error
		if hard {
			err = h.sessions.PurgeSessionsByUser(r.Context(), tenant, userID)
		} else {
			err = h.sessions.DeleteSessionsByUser(r.Context(), tenant, userID)
		}
		if err != nil {
			logger.FromContext(r.Context()).WithError(err).Error("admin delete sessions failed")
			internalError(w, "failed to delete sessions")
			return
		}
	}
	action := models.AuditActionDeleteUser
	if hard {
		action = models.AuditActionHardDeleteUser
	}
	h.recordAudit(r, action, userID, "")
	writeOK(w, map[string]interface{}{"deleted": userID, "hard": hard})
}

// SearchSessions 处理 GET /admin/api/sessions?user_id=&q=&limit=&cursor=。
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
		Cursor: q.Get("cursor"),
	}
	list, next, err := h.sessions.SearchSessions(r.Context(), tenant, opts)
	if err != nil {
		logger.FromContext(r.Context()).WithError(err).Error("admin search sessions failed")
		internalError(w, "failed to search sessions")
		return
	}
	writeOK(w, map[string]interface{}{"tenant_id": tenant, "sessions": list, "count": len(list), "next_cursor": next})
}

// SummarizeSessions 处理 POST /admin/api/sessions/summarize（立即触发一次会话总结，无需等待定时窗口）。
func (h *AdminHandler) SummarizeSessions(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(r) {
		writeError(w, http.StatusUnauthorized, CodeUnauthorized, "invalid admin token")
		return
	}
	if h.summarizer == nil {
		writeError(w, http.StatusNotFound, CodeNotFound, "session summarizer not configured")
		return
	}
	n, err := h.summarizer.RunSummarization(r.Context())
	if err != nil {
		logger.FromContext(r.Context()).WithError(err).Error("admin summarize sessions failed")
		internalError(w, "failed to summarize sessions")
		return
	}
	h.recordAudit(r, "summarize_sessions", "", fmt.Sprintf("processed %d sessions", n))
	writeOK(w, map[string]interface{}{"processed": n})
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

// GetSessionHistory 处理 GET /admin/api/sessions/{session_id}/history（获取会话完整对话历史）。
func (h *AdminHandler) GetSessionHistory(w http.ResponseWriter, r *http.Request) {
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
	// 从 ClickHouse 读取完整历史消息
	var history []*models.HistoryMessage
	if h.inserter != nil {
		limit := 500
		offset := 0
		if l := r.URL.Query().Get("limit"); l != "" {
			if v, err := strconv.Atoi(l); err == nil && v > 0 {
				limit = v
			}
		}
		if o := r.URL.Query().Get("offset"); o != "" {
			if v, err := strconv.Atoi(o); err == nil && v >= 0 {
				offset = v
			}
		}
		history, err = h.inserter.QueryHistoryBySession(r.Context(), tenant, sessionID, limit, offset)
		if err != nil {
			logger.FromContext(r.Context()).WithError(err).Error("admin query session history failed")
			internalError(w, "failed to query history")
			return
		}
	}
	writeOK(w, map[string]interface{}{
		"session_id":    sum.SessionID,
		"user_id":       sum.UserID,
		"summary":       sum.Summary,
		"message_count": sum.MessageCount,
		"started_at":    sum.StartedAt,
		"ended_at":      sum.EndedAt,
		"history":       history,
	})
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
	h.recordAudit(r, models.AuditActionExportSession, sessionID, "")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", "attachment; filename=session_"+sessionID+".json")
	_ = json.NewEncoder(w).Encode(sum)
}

// DeleteSession 处理 DELETE /admin/api/sessions/{session_id}?hard=。默认软删除，hard=true 为硬删除。
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
	hard := r.URL.Query().Get("hard") == "true"
	var err error
	if hard {
		err = h.sessions.PurgeSession(r.Context(), tenant, sessionID)
	} else {
		err = h.sessions.DeleteSession(r.Context(), tenant, sessionID)
	}
	if err != nil {
		logger.FromContext(r.Context()).WithError(err).Error("admin delete session failed")
		internalError(w, "failed to delete session")
		return
	}
	action := models.AuditActionDeleteSession
	if hard {
		action = models.AuditActionHardDelete
	}
	h.recordAudit(r, action, sessionID, "")
	writeOK(w, map[string]interface{}{"deleted": sessionID, "hard": hard})
}

// ListAudit 处理 GET /admin/api/audit?limit=，返回管理操作审计日志（最新在前）。
func (h *AdminHandler) ListAudit(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(r) {
		writeError(w, http.StatusUnauthorized, CodeUnauthorized, "invalid admin token")
		return
	}
	tenant := h.tenant(r)
	if tenant == "" {
		badRequest(w, CodeMissingTenant, "missing X-Tenant-ID header")
		return
	}
	if h.audit == nil {
		writeError(w, http.StatusNotFound, CodeNotFound, "audit store not configured")
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	entries, err := h.audit.List(r.Context(), tenant, limit)
	if err != nil {
		logger.FromContext(r.Context()).WithError(err).Error("admin list audit failed")
		internalError(w, "failed to list audit")
		return
	}
	writeOK(w, map[string]interface{}{"tenant_id": tenant, "audit": entries, "count": len(entries)})
}

// RebuildIndex 处理 POST /admin/api/index/rebuild（重建 ZSET 索引）。
func (h *AdminHandler) RebuildIndex(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(r) {
		writeError(w, http.StatusUnauthorized, CodeUnauthorized, "invalid admin token")
		return
	}
	tenant := h.tenant(r)
	if tenant == "" {
		badRequest(w, CodeMissingTenant, "missing X-Tenant-ID header")
		return
	}

	type rebuildResult struct {
		UsersRebuilt    int64 `json:"users_rebuilt"`
		SessionsRebuilt int64 `json:"sessions_rebuilt"`
		DurationMs      int64 `json:"duration_ms"`
	}

	start := time.Now()
	res := rebuildResult{}

	if h.profiles != nil {
		if n, err := h.profiles.RebuildUsersIndex(r.Context(), tenant); err != nil {
			logger.FromContext(r.Context()).WithError(err).Error("admin rebuild users index failed")
			internalError(w, "failed to rebuild users index")
			return
		} else {
			res.UsersRebuilt = n
		}
	}
	if h.sessions != nil {
		if n, err := h.sessions.RebuildSessionsIndex(r.Context(), tenant); err != nil {
			logger.FromContext(r.Context()).WithError(err).Error("admin rebuild sessions index failed")
			internalError(w, "failed to rebuild sessions index")
			return
		} else {
			res.SessionsRebuilt = n
		}
	}

	res.DurationMs = time.Since(start).Milliseconds()
	h.recordAudit(r, "rebuild_index", "", fmt.Sprintf("users=%d sessions=%d", res.UsersRebuilt, res.SessionsRebuilt))
	writeOK(w, res)
}
