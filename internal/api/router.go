package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/mnemosyne/mneme/pkg/models"
)

// RouterOptions 组装路由所需的依赖。
type RouterOptions struct {
	History        *HistoryHandler
	Traces         *TracesHandler
	Memory         *MemoryHandler
	Sessions       *SessionsHandler
	Users          *UsersHandler
	Health         *HealthHandler
	OpenAPI        *OpenAPIHandler
	Admin          *AdminHandler
	AdminUI        *AdminUIHandler
	MetricsEnabled bool
	TracingEnabled bool
}

// NewRouter 构建 HTTP 路由。
func NewRouter(opts RouterOptions) http.Handler {
	r := mux.NewRouter()
	r.Use(RecoverMiddleware, LoggingMiddleware)

	health := opts.Health
	if health == nil {
		health = NewHealthHandler()
	}
	r.HandleFunc("/health", health.Health).Methods(http.MethodGet)
	r.HandleFunc("/ready", health.Ready).Methods(http.MethodGet)

	if opts.MetricsEnabled {
		r.Handle("/metrics", promhttp.Handler()).Methods(http.MethodGet)
	}

	v1 := r.PathPrefix("/api/v1").Subrouter()
	if opts.History != nil {
		v1.HandleFunc("/history", opts.History.Create).Methods(http.MethodPost)
		v1.HandleFunc("/history/batch", opts.History.CreateBatch).Methods(http.MethodPost)
	}
	if opts.Traces != nil {
		v1.HandleFunc("/traces", opts.Traces.Create).Methods(http.MethodPost)
	}
	if opts.Memory != nil {
		v1.HandleFunc("/memory/events", opts.Memory.CreateEvent).Methods(http.MethodPost)
		v1.HandleFunc("/memory/{entity_type}/{entity_id}", opts.Memory.GetProfile).Methods(http.MethodGet)
	}
	if opts.Sessions != nil {
		v1.HandleFunc("/sessions/{session_id}", opts.Sessions.GetSummary).Methods(http.MethodGet)
	}
	if opts.Users != nil {
		v1.HandleFunc("/users/{user_id}", opts.Users.GetContext).Methods(http.MethodGet)
	}

	if opts.OpenAPI != nil {
		r.HandleFunc("/openapi.json", opts.OpenAPI.Spec).Methods(http.MethodGet)
		r.HandleFunc("/docs", opts.OpenAPI.Docs).Methods(http.MethodGet)
	}

	if opts.Admin != nil {
		adminSub := r.PathPrefix("/admin/api").Subrouter()
		opts.Admin.Register(adminSub)
	}
	if opts.AdminUI != nil {
		r.Handle("/admin", opts.AdminUI).Methods(http.MethodGet)
		r.Handle("/admin/", opts.AdminUI).Methods(http.MethodGet)
	}

	r.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusNotFound, models.Err(CodeNotFound, "resource not found"))
	})
	r.MethodNotAllowedHandler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusMethodNotAllowed, models.Err(CodeBadRequest, "method not allowed"))
	})

	if opts.TracingEnabled {
		return otelhttp.NewHandler(r, "mneme-http")
	}
	return r
}
