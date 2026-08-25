package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mnemosyne/mneme/internal/service"
	"github.com/mnemosyne/mneme/pkg/messagebus"
	"github.com/mnemosyne/mneme/pkg/models"
)

type stubReader struct {
	profile map[string]string
	err     error
}

func (s stubReader) GetProfile(context.Context, string, string, string) (map[string]string, error) {
	return s.profile, s.err
}

func newTestServer(reader ProfileReader) (http.Handler, *messagebus.MemoryBus) {
	bus := messagebus.NewMemoryBus()
	ingest := service.NewIngestService(bus, service.Topics{History: "h", Traces: "t", Memory: "m"})
	return NewRouter(RouterOptions{
		History:        NewHistoryHandler(ingest),
		Traces:         NewTracesHandler(ingest),
		Memory:         NewMemoryHandler(ingest, reader),
		Health:         NewHealthHandler(),
		MetricsEnabled: true,
	}), bus
}

func doJSON(t *testing.T, h http.Handler, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func TestHealthEndpoint(t *testing.T) {
	h, _ := newTestServer(nil)
	rr := doJSON(t, h, http.MethodGet, "/health", "", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	var body map[string]string
	_ = json.Unmarshal(rr.Body.Bytes(), &body)
	if body["status"] != "ok" {
		t.Errorf("body = %v", body)
	}
}

func TestReadyEndpoint(t *testing.T) {
	okChecker := CheckerFunc{CheckName: "redis", Fn: func(context.Context) error { return nil }}
	badChecker := CheckerFunc{CheckName: "ch", Fn: func(context.Context) error { return errors.New("down") }}

	h := NewRouter(RouterOptions{Health: NewHealthHandler(okChecker)})
	if rr := doJSON(t, h, http.MethodGet, "/ready", "", nil); rr.Code != http.StatusOK {
		t.Errorf("status = %d", rr.Code)
	}

	h2 := NewRouter(RouterOptions{Health: NewHealthHandler(okChecker, badChecker)})
	rr := doJSON(t, h2, http.MethodGet, "/ready", "", nil)
	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d", rr.Code)
	}
	var body map[string]interface{}
	_ = json.Unmarshal(rr.Body.Bytes(), &body)
	if body["status"] != "degraded" {
		t.Errorf("body = %v", body)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	h, _ := newTestServer(nil)
	rr := doJSON(t, h, http.MethodGet, "/metrics", "", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "mnemosyne_mneme_") {
		t.Error("metrics output missing mneme metrics")
	}
}

func TestPostHistoryAccepted(t *testing.T) {
	h, bus := newTestServer(nil)
	body := `{"session_id":"s1","user_id":"u1","role":"user","content":"hello","model_name":"gpt"}`
	rr := doJSON(t, h, http.MethodPost, "/api/v1/history", body, map[string]string{HeaderTenantID: "t1"})
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d body = %s", rr.Code, rr.Body.String())
	}
	msgs := bus.Messages("h")
	if len(msgs) != 1 {
		t.Fatalf("published = %d", len(msgs))
	}
	var out models.HistoryMessage
	_ = json.Unmarshal(msgs[0].Payload, &out)
	if out.TenantID != "t1" || out.Content != "hello" {
		t.Errorf("message = %+v", out)
	}
}

func TestPostHistoryValidationErrors(t *testing.T) {
	h, _ := newTestServer(nil)

	// 缺少租户
	rr := doJSON(t, h, http.MethodPost, "/api/v1/history", `{"session_id":"s","role":"user","content":"x"}`, nil)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("missing tenant status = %d", rr.Code)
	}

	// 非法 JSON
	rr = doJSON(t, h, http.MethodPost, "/api/v1/history", `{bad`, map[string]string{HeaderTenantID: "t"})
	if rr.Code != http.StatusBadRequest {
		t.Errorf("bad json status = %d", rr.Code)
	}

	// 缺少字段
	rr = doJSON(t, h, http.MethodPost, "/api/v1/history", `{"role":"user","content":"x"}`, map[string]string{HeaderTenantID: "t"})
	if rr.Code != http.StatusBadRequest {
		t.Errorf("missing session status = %d", rr.Code)
	}
	var resp models.APIResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp.Code != CodeMissingField {
		t.Errorf("code = %d", resp.Code)
	}

	// 非法角色
	rr = doJSON(t, h, http.MethodPost, "/api/v1/history", `{"session_id":"s","role":"bot","content":"x"}`, map[string]string{HeaderTenantID: "t"})
	if rr.Code != http.StatusBadRequest {
		t.Errorf("bad role status = %d", rr.Code)
	}
}

func TestPostHistoryTenantFromBody(t *testing.T) {
	h, bus := newTestServer(nil)
	body := `{"tenant_id":"tb","session_id":"s","role":"user","content":"x"}`
	rr := doJSON(t, h, http.MethodPost, "/api/v1/history", body, nil)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d", rr.Code)
	}
	if len(bus.Messages("h")) != 1 {
		t.Error("not published")
	}
}

func TestPostTraces(t *testing.T) {
	h, bus := newTestServer(nil)
	body := `{"request_id":"r1","session_id":"s","step_type":"tool_call","step_name":"search","duration_ms":12}`
	rr := doJSON(t, h, http.MethodPost, "/api/v1/traces", body, map[string]string{HeaderTenantID: "t1"})
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d body = %s", rr.Code, rr.Body.String())
	}
	if len(bus.Messages("t")) != 1 {
		t.Error("trace not published")
	}

	rr = doJSON(t, h, http.MethodPost, "/api/v1/traces", `{"request_id":"r","step_type":"bad","step_name":"n"}`, map[string]string{HeaderTenantID: "t"})
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d", rr.Code)
	}
	rr = doJSON(t, h, http.MethodPost, "/api/v1/traces", `{"request_id":"r"}`, nil)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("missing tenant status = %d", rr.Code)
	}
	rr = doJSON(t, h, http.MethodPost, "/api/v1/traces", `{`, map[string]string{HeaderTenantID: "t"})
	if rr.Code != http.StatusBadRequest {
		t.Errorf("bad json status = %d", rr.Code)
	}
}

func TestPostMemoryEvents(t *testing.T) {
	h, bus := newTestServer(nil)
	body := `{"entity_id":"u1","entity_type":"user","event_type":"pref","payload":{"lang":"zh"}}`
	rr := doJSON(t, h, http.MethodPost, "/api/v1/memory/events", body, map[string]string{HeaderTenantID: "t1"})
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d body = %s", rr.Code, rr.Body.String())
	}
	if len(bus.Messages("m")) != 1 {
		t.Error("event not published")
	}

	rr = doJSON(t, h, http.MethodPost, "/api/v1/memory/events", `{"entity_id":"u"}`, map[string]string{HeaderTenantID: "t"})
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d", rr.Code)
	}
	rr = doJSON(t, h, http.MethodPost, "/api/v1/memory/events", `{"entity_id":"u"}`, nil)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("missing tenant status = %d", rr.Code)
	}
	rr = doJSON(t, h, http.MethodPost, "/api/v1/memory/events", `{`, map[string]string{HeaderTenantID: "t"})
	if rr.Code != http.StatusBadRequest {
		t.Errorf("bad json status = %d", rr.Code)
	}
}

func TestGetProfile(t *testing.T) {
	h, _ := newTestServer(stubReader{profile: map[string]string{"summary": "likes go"}})
	rr := doJSON(t, h, http.MethodGet, "/api/v1/memory/user/u1", "", map[string]string{HeaderTenantID: "t1"})
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rr.Code, rr.Body)
	}
	var resp models.APIResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	data, _ := resp.Data.(map[string]interface{})
	if data["summary"] != "likes go" {
		t.Errorf("data = %v", resp.Data)
	}

	// 缺少租户
	if rr := doJSON(t, h, http.MethodGet, "/api/v1/memory/user/u1", "", nil); rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d", rr.Code)
	}

	// 未配置 reader
	h2, _ := newTestServer(nil)
	if rr := doJSON(t, h2, http.MethodGet, "/api/v1/memory/user/u1", "", map[string]string{HeaderTenantID: "t"}); rr.Code != http.StatusNotFound {
		t.Errorf("status = %d", rr.Code)
	}

	// 空画像
	h3, _ := newTestServer(stubReader{profile: map[string]string{}})
	if rr := doJSON(t, h3, http.MethodGet, "/api/v1/memory/user/u1", "", map[string]string{HeaderTenantID: "t"}); rr.Code != http.StatusNotFound {
		t.Errorf("status = %d", rr.Code)
	}

	// 读取错误
	h4, _ := newTestServer(stubReader{err: errors.New("boom")})
	if rr := doJSON(t, h4, http.MethodGet, "/api/v1/memory/user/u1", "", map[string]string{HeaderTenantID: "t"}); rr.Code != http.StatusInternalServerError {
		t.Errorf("status = %d", rr.Code)
	}
}

func TestNotFoundAndMethodNotAllowed(t *testing.T) {
	h, _ := newTestServer(nil)
	if rr := doJSON(t, h, http.MethodGet, "/api/v1/nope", "", nil); rr.Code != http.StatusNotFound {
		t.Errorf("status = %d", rr.Code)
	}
	// 顶层路由（非子路由）上的方法不匹配返回 405
	rr := doJSON(t, h, http.MethodPost, "/health", "", nil)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d", rr.Code)
	}
	var resp models.APIResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp.Code != CodeBadRequest {
		t.Errorf("code = %d", resp.Code)
	}
}

func TestRecoverMiddleware(t *testing.T) {
	r := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") })
	h := RecoverMiddleware(LoggingMiddleware(r))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/x", nil))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status = %d", rr.Code)
	}
}
