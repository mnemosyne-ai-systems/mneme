package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"

	"github.com/mnemosyne/mneme/pkg/logger"
	"github.com/mnemosyne/mneme/pkg/metrics"
	"github.com/mnemosyne/mneme/pkg/models"
	"github.com/mnemosyne/mneme/pkg/tracing"
)

// HeaderTenantID 租户标识请求头。
const HeaderTenantID = "X-Tenant-ID"

// HeaderRequestID 请求标识请求头。
const HeaderRequestID = "X-Request-ID"

type statusRecorder struct {
	http.ResponseWriter
	status int
	size   int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.size += n
	return n, err
}

// Hijack 透传给底层，以支持 WebSocket 等升级场景。
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// LoggingMiddleware 记录访问日志、注入 trace_id / tenant_id 并上报指标。
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		requestID := r.Header.Get(HeaderRequestID)
		if requestID == "" {
			requestID = models.NewID()
		}
		if traceID := tracing.TraceIDFromContext(r.Context()); traceID != "" {
			requestID = traceID
		}
		tenantID := r.Header.Get(HeaderTenantID)

		ctx := logger.WithTenantID(logger.WithTraceID(r.Context(), requestID), tenantID)
		r = r.WithContext(ctx)
		w.Header().Set(HeaderRequestID, requestID)

		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)

		if rec.status == 0 {
			rec.status = http.StatusOK
		}
		endpoint := routeTemplate(r)
		elapsed := time.Since(start)
		metrics.ObserveHTTP(endpoint, r.Method, strconv.Itoa(rec.status), elapsed.Seconds())

		logger.FromContext(ctx).WithFields(map[string]interface{}{
			"method":   r.Method,
			"path":     r.URL.Path,
			"endpoint": endpoint,
			"status":   rec.status,
			"bytes":    rec.size,
			"duration": elapsed.String(),
		}).Info("http request")
	})
}

// RecoverMiddleware 捕获 panic，返回 500 而非中断进程。
func RecoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				logger.FromContext(r.Context()).WithField("panic", rec).Error("panic recovered")
				internalError(w, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// routeTemplate 返回路由模板（用于指标标签，避免高基数）。
func routeTemplate(r *http.Request) string {
	if route := mux.CurrentRoute(r); route != nil {
		if tpl, err := route.GetPathTemplate(); err == nil {
			return tpl
		}
	}
	return r.URL.Path
}

// tenantFrom 从请求头解析租户；缺失时返回空串。
func tenantFrom(r *http.Request) string {
	return r.Header.Get(HeaderTenantID)
}
