package api

import (
	"context"
	"net/http"
	"time"

	"github.com/gorilla/mux"

	"github.com/mnemosyne/mneme/internal/service"
	"github.com/mnemosyne/mneme/pkg/logger"
	"github.com/mnemosyne/mneme/pkg/models"
)

// ProfileReader 用于读取实体画像（由 memory 服务实现）。
type ProfileReader interface {
	GetProfile(ctx context.Context, tenant, entityType, entityID string) (map[string]string, error)
}

// MemoryHandler 管道三 HTTP 接入。
type MemoryHandler struct {
	ingest *service.IngestService
	reader ProfileReader
}

// NewMemoryHandler 创建 handler。reader 可为 nil（此时画像查询返回 404）。
func NewMemoryHandler(ingest *service.IngestService, reader ProfileReader) *MemoryHandler {
	return &MemoryHandler{ingest: ingest, reader: reader}
}

// memoryEventRequest 是 POST /api/v1/memory/events 的请求体。
type memoryEventRequest struct {
	EventID    string                 `json:"event_id"`
	EntityID   string                 `json:"entity_id"`
	TenantID   string                 `json:"tenant_id"`
	EntityType string                 `json:"entity_type"`
	EventType  string                 `json:"event_type"`
	Payload    map[string]interface{} `json:"payload"`
	Timestamp  time.Time              `json:"timestamp"`
}

func (r memoryEventRequest) toModel(tenant string) *models.MemoryEvent {
	return &models.MemoryEvent{
		EventID:    r.EventID,
		EntityID:   r.EntityID,
		TenantID:   tenant,
		EntityType: r.EntityType,
		EventType:  r.EventType,
		Payload:    r.Payload,
		Timestamp:  r.Timestamp,
	}
}

// CreateEvent 处理 POST /api/v1/memory/events。
func (h *MemoryHandler) CreateEvent(w http.ResponseWriter, r *http.Request) {
	var req memoryEventRequest
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

	evt := req.toModel(tenant)
	if err := h.ingest.PublishMemoryEvent(r.Context(), evt); err != nil {
		if ve, ok := err.(*models.ValidationError); ok {
			badRequest(w, CodeMissingField, ve.Error())
			return
		}
		logger.FromContext(r.Context()).WithError(err).Error("publish memory event failed")
		writeError(w, http.StatusServiceUnavailable, CodeUpstream, "failed to publish memory event")
		return
	}

	writeAccepted(w, map[string]string{"event_id": evt.EventID})
}

// GetProfile 处理 GET /api/v1/memory/{entity_type}/{entity_id}。
func (h *MemoryHandler) GetProfile(w http.ResponseWriter, r *http.Request) {
	tenant := tenantFrom(r)
	if tenant == "" {
		badRequest(w, CodeMissingTenant, "missing X-Tenant-ID header")
		return
	}
	vars := mux.Vars(r)
	entityType, entityID := vars["entity_type"], vars["entity_id"]

	if h.reader == nil {
		writeError(w, http.StatusNotFound, CodeNotFound, "profile store not configured")
		return
	}
	profile, err := h.reader.GetProfile(r.Context(), tenant, entityType, entityID)
	if err != nil {
		logger.FromContext(r.Context()).WithError(err).Error("get profile failed")
		internalError(w, "failed to read profile")
		return
	}
	if len(profile) == 0 {
		writeError(w, http.StatusNotFound, CodeNotFound, "profile not found")
		return
	}
	writeOK(w, profile)
}
