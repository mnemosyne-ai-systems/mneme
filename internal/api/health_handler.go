package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// HealthChecker 表示一个可探活的依赖。
type HealthChecker interface {
	Name() string
	Check(ctx context.Context) error
}

// CheckerFunc 把函数适配为 HealthChecker。
type CheckerFunc struct {
	CheckName string
	Fn        func(ctx context.Context) error
}

// Name 返回依赖名称。
func (c CheckerFunc) Name() string { return c.CheckName }

// Check 执行探活。
func (c CheckerFunc) Check(ctx context.Context) error { return c.Fn(ctx) }

// HealthHandler 提供 /health 与 /ready 端点。
type HealthHandler struct {
	checkers []HealthChecker
	timeout  time.Duration
}

// NewHealthHandler 创建健康检查 handler。
func NewHealthHandler(checkers ...HealthChecker) *HealthHandler {
	return &HealthHandler{checkers: checkers, timeout: 3 * time.Second}
}

// Health 处理 GET /health，仅表示进程存活。
func (h *HealthHandler) Health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// Ready 处理 GET /ready，逐个探活依赖。
func (h *HealthHandler) Ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	details := make(map[string]string, len(h.checkers))
	healthy := true
	for _, c := range h.checkers {
		if err := c.Check(ctx); err != nil {
			details[c.Name()] = "error: " + err.Error()
			healthy = false
			continue
		}
		details[c.Name()] = "ok"
	}

	status := http.StatusOK
	state := "ok"
	if !healthy {
		status = http.StatusServiceUnavailable
		state = "degraded"
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":       state,
		"dependencies": details,
	})
}
