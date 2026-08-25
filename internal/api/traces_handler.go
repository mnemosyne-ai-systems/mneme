package api

import (
	"net/http"
	"time"

	"github.com/mnemosyne/mneme/internal/service"
	"github.com/mnemosyne/mneme/pkg/logger"
	"github.com/mnemosyne/mneme/pkg/models"
)

// TracesHandler 管道二 HTTP 接入。
type TracesHandler struct {
	ingest *service.IngestService
}

// NewTracesHandler 创建 handler。
func NewTracesHandler(ingest *service.IngestService) *TracesHandler {
	return &TracesHandler{ingest: ingest}
}

// traceRequest 是 POST /api/v1/traces 的请求体。
type traceRequest struct {
	RequestID  string            `json:"request_id"`
	TenantID   string            `json:"tenant_id"`
	SessionID  string            `json:"session_id"`
	UserID     string            `json:"user_id"`
	StepType   string            `json:"step_type"`
	StepName   string            `json:"step_name"`
	Input      string            `json:"input"`
	Output     string            `json:"output"`
	Metadata   map[string]string `json:"metadata"`
	DurationMs int32             `json:"duration_ms"`
	Timestamp  time.Time         `json:"timestamp"`
}

func (r traceRequest) toModel(tenant string) *models.TraceMessage {
	return &models.TraceMessage{
		RequestID:  r.RequestID,
		TenantID:   tenant,
		SessionID:  r.SessionID,
		UserID:     r.UserID,
		StepType:   r.StepType,
		StepName:   r.StepName,
		Input:      r.Input,
		Output:     r.Output,
		Metadata:   r.Metadata,
		DurationMs: r.DurationMs,
		Timestamp:  r.Timestamp,
	}
}

// Create 处理 POST /api/v1/traces。
func (h *TracesHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req traceRequest
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
	if err := h.ingest.PublishTrace(r.Context(), msg); err != nil {
		if ve, ok := err.(*models.ValidationError); ok {
			badRequest(w, CodeMissingField, ve.Error())
			return
		}
		logger.FromContext(r.Context()).WithError(err).Error("publish trace failed")
		writeError(w, http.StatusServiceUnavailable, CodeUpstream, "failed to publish trace")
		return
	}

	writeAccepted(w, map[string]string{"request_id": msg.RequestID})
}
