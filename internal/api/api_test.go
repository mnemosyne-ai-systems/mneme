package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"

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

func TestPostHistoryBatch(t *testing.T) {
	h, bus := newTestServer(nil)

	body := `{"messages":[
		{"session_id":"s1","role":"user","content":"a"},
		{"tenant_id":"t2","session_id":"s2","role":"assistant","content":"b"}
	]}`
	rr := doJSON(t, h, http.MethodPost, "/api/v1/history/batch", body, map[string]string{HeaderTenantID: "t1"})
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d body = %s", rr.Code, rr.Body.String())
	}
	var resp models.APIResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	data, _ := resp.Data.(map[string]interface{})
	if data["accepted"] != float64(2) {
		t.Errorf("data = %v", resp.Data)
	}

	msgs := bus.Messages("h")
	if len(msgs) != 2 {
		t.Fatalf("published = %d", len(msgs))
	}
	var first, second models.HistoryMessage
	_ = json.Unmarshal(msgs[0].Payload, &first)
	_ = json.Unmarshal(msgs[1].Payload, &second)
	if first.TenantID != "t1" || first.Content != "a" {
		t.Errorf("first = %+v", first)
	}
	// 头部租户优先于消息自带 tenant_id（与单条接口语义一致）
	if second.TenantID != "t1" || second.SessionID != "s2" {
		t.Errorf("second = %+v", second)
	}

	// 无头部时使用各消息自带的 tenant_id
	h2, bus2 := newTestServer(nil)
	rr2 := doJSON(t, h2, http.MethodPost, "/api/v1/history/batch",
		`{"messages":[{"tenant_id":"ta","session_id":"s","role":"user","content":"a"},{"tenant_id":"tb","session_id":"s","role":"user","content":"b"}]}`,
		nil)
	if rr2.Code != http.StatusAccepted {
		t.Fatalf("no-header status = %d", rr2.Code)
	}
	msgs2 := bus2.Messages("h")
	if len(msgs2) != 2 {
		t.Fatalf("published = %d", len(msgs2))
	}
	var bare models.HistoryMessage
	_ = json.Unmarshal(msgs2[1].Payload, &bare)
	if bare.TenantID != "tb" {
		t.Errorf("bare = %+v", bare)
	}

	// 空批次
	rr = doJSON(t, h, http.MethodPost, "/api/v1/history/batch", `{"messages":[]}`, map[string]string{HeaderTenantID: "t"})
	if rr.Code != http.StatusBadRequest {
		t.Errorf("empty batch status = %d", rr.Code)
	}

	// 非法 JSON
	rr = doJSON(t, h, http.MethodPost, "/api/v1/history/batch", `{bad`, nil)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("bad json status = %d", rr.Code)
	}

	// 缺少租户（无头且消息未带 tenant_id）
	rr = doJSON(t, h, http.MethodPost, "/api/v1/history/batch",
		`{"messages":[{"session_id":"s","role":"user","content":"x"}]}`, nil)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("missing tenant status = %d", rr.Code)
	}

	// 批内校验失败整批拒绝
	before := len(bus.Messages("h"))
	rr = doJSON(t, h, http.MethodPost, "/api/v1/history/batch",
		`{"messages":[{"session_id":"s","role":"user","content":"x"},{"session_id":"s","role":"bot","content":"y"}]}`,
		map[string]string{HeaderTenantID: "t"})
	if rr.Code != http.StatusBadRequest {
		t.Errorf("invalid role status = %d", rr.Code)
	}
	if len(bus.Messages("h")) != before {
		t.Error("rejected batch must not be partially published")
	}

	// 超过批量上限
	over := historyBatchRequest{}
	for i := 0; i < maxBatchMessages+1; i++ {
		over.Messages = append(over.Messages, historyRequest{SessionID: "s", Role: "user", Content: "x"})
	}
	raw, _ := json.Marshal(over)
	rr = doJSON(t, h, http.MethodPost, "/api/v1/history/batch", string(raw), map[string]string{HeaderTenantID: "t"})
	if rr.Code != http.StatusBadRequest {
		t.Errorf("over limit status = %d", rr.Code)
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

type stubSummaryReader struct {
	summary *models.SessionSummary
	err     error
}

func (s stubSummaryReader) GetSummary(_ context.Context, _, _ string) (*models.SessionSummary, error) {
	return s.summary, s.err
}

func newSessionsServer(reader SessionSummaryReader) http.Handler {
	bus := messagebus.NewMemoryBus()
	ingest := service.NewIngestService(bus, service.Topics{History: "h", Traces: "t", Memory: "m"})
	return NewRouter(RouterOptions{
		History:  NewHistoryHandler(ingest),
		Memory:   NewMemoryHandler(ingest, nil),
		Sessions: NewSessionsHandler(reader),
		Health:   NewHealthHandler(),
	})
}

func TestGetSessionSummary(t *testing.T) {
	summary := &models.SessionSummary{
		SessionID: "s1", TenantID: "t1", UserID: "u1",
		Summary: "讨论了部署方案", MessageCount: 4, Version: 2,
	}
	h := newSessionsServer(stubSummaryReader{summary: summary})
	rr := doJSON(t, h, http.MethodGet, "/api/v1/sessions/s1", "", map[string]string{HeaderTenantID: "t1"})
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rr.Code, rr.Body)
	}
	var resp models.APIResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	data, _ := resp.Data.(map[string]interface{})
	if data["summary"] != "讨论了部署方案" {
		t.Errorf("data = %v", resp.Data)
	}

	// 缺少租户
	if rr := doJSON(t, h, http.MethodGet, "/api/v1/sessions/s1", "", nil); rr.Code != http.StatusBadRequest {
		t.Errorf("missing tenant status = %d", rr.Code)
	}
	// 未配置 reader
	h2 := NewRouter(RouterOptions{})
	rr2 := doJSON(t, h2, http.MethodGet, "/api/v1/sessions/s1", "", map[string]string{HeaderTenantID: "t"})
	if rr2.Code != http.StatusNotFound {
		t.Errorf("unconfigured status = %d", rr2.Code)
	}
	// 空总结
	h3 := newSessionsServer(stubSummaryReader{summary: nil})
	if rr := doJSON(t, h3, http.MethodGet, "/api/v1/sessions/s1", "", map[string]string{HeaderTenantID: "t"}); rr.Code != http.StatusNotFound {
		t.Errorf("not found status = %d", rr.Code)
	}
	// 读取错误
	h4 := newSessionsServer(stubSummaryReader{err: errors.New("boom")})
	if rr := doJSON(t, h4, http.MethodGet, "/api/v1/sessions/s1", "", map[string]string{HeaderTenantID: "t"}); rr.Code != http.StatusInternalServerError {
		t.Errorf("error status = %d", rr.Code)
	}
}

type stubUserStore struct {
	profile  map[string]string
	profileErr error
	sessions []*models.SessionSummary
	sessErr  error
}

func (s stubUserStore) GetProfile(_ context.Context, _, _, _ string) (map[string]string, error) {
	return s.profile, s.profileErr
}

func (s stubUserStore) SaveProfile(_ context.Context, _, _, _ string, _ map[string]string) (int64, error) {
	return 1, nil
}

func (s stubUserStore) ListProfiles(_ context.Context, _, _ string, _ service.ListOptions) ([]*service.EntityProfile, error) {
	return nil, nil
}

func (s stubUserStore) DeleteProfile(_ context.Context, _, _, _ string) error {
	return nil
}

func (s stubUserStore) ListSessionsByUser(_ context.Context, _, _ string) ([]*models.SessionSummary, error) {
	return s.sessions, s.sessErr
}

func (s stubUserStore) SaveSummary(_ context.Context, _ *models.SessionSummary) (int64, error) {
	return 1, nil
}

func (s stubUserStore) SearchSessions(_ context.Context, _ string, _ service.SessionSearchOptions) ([]*models.SessionSummary, error) {
	return s.sessions, nil
}

func (s stubUserStore) DeleteSession(_ context.Context, _, _ string) error {
	return nil
}

func (s stubUserStore) DeleteSessionsByUser(_ context.Context, _, _ string) error {
	return nil
}

func (s stubUserStore) GetSummary(_ context.Context, _, _ string) (*models.SessionSummary, error) {
	if len(s.sessions) > 0 {
		return s.sessions[0], nil
	}
	return nil, nil
}

func newUsersServer(store stubUserStore) http.Handler {
	return NewRouter(RouterOptions{
		Users:  NewUsersHandler(store, store),
		Health: NewHealthHandler(),
	})
}

func TestGetUserContext(t *testing.T) {
	sessions := []*models.SessionSummary{
		{SessionID: "s1", TenantID: "t1", UserID: "u1", Summary: "部署方案", MessageCount: 4},
		{SessionID: "s2", TenantID: "t1", UserID: "u1", Summary: "排障", MessageCount: 2},
	}
	h := newUsersServer(stubUserStore{profile: map[string]string{"lang": "zh"}, sessions: sessions})
	rr := doJSON(t, h, http.MethodGet, "/api/v1/users/u1", "", map[string]string{HeaderTenantID: "t1"})
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rr.Code, rr.Body)
	}
	var resp models.APIResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	data, _ := resp.Data.(map[string]interface{})
	if data["user_id"] != "u1" {
		t.Errorf("user_id = %v", data["user_id"])
	}
	prof, _ := data["profile"].(map[string]interface{})
	if prof["lang"] != "zh" {
		t.Errorf("profile = %v", data["profile"])
	}
	sess, _ := data["sessions"].([]interface{})
	if len(sess) != 2 {
		t.Errorf("sessions = %v", data["sessions"])
	}

	// 缺少租户
	if rr := doJSON(t, h, http.MethodGet, "/api/v1/users/u1", "", nil); rr.Code != http.StatusBadRequest {
		t.Errorf("missing tenant status = %d", rr.Code)
	}
	// 既无画像也无会话 → 404
	h2 := newUsersServer(stubUserStore{profile: map[string]string{}, sessions: nil})
	if rr := doJSON(t, h2, http.MethodGet, "/api/v1/users/u1", "", map[string]string{HeaderTenantID: "t"}); rr.Code != http.StatusNotFound {
		t.Errorf("empty status = %d", rr.Code)
	}
}

func TestOpenAPISpecAndDocs(t *testing.T) {
	h := NewRouter(RouterOptions{OpenAPI: NewOpenAPIHandler(), Health: NewHealthHandler()})
	rr := doJSON(t, h, http.MethodGet, "/openapi.json", "", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("spec status = %d", rr.Code)
	}
	var spec map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &spec); err != nil {
		t.Fatalf("spec not json: %v", err)
	}
	if spec["openapi"] != "3.0.3" {
		t.Errorf("openapi version = %v", spec["openapi"])
	}
	rr = doJSON(t, h, http.MethodGet, "/docs", "", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("docs status = %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "swagger-ui") {
		t.Error("docs missing swagger-ui")
	}
}

func newAdminServer(h *AdminHandler) http.Handler {
	r := mux.NewRouter()
	h.Register(r)
	return r
}

func TestAdminUserAndSessionLifecycle(t *testing.T) {
	ctx := context.Background()
	profiles := service.NewMemoryProfileStore(nil)
	sessions := service.NewMemorySessionSummaryStore(nil)
	if _, err := profiles.SaveProfile(ctx, "t1", models.EntityTypeUser, "u1", map[string]string{"lang": "zh"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sessions.SaveSummary(ctx, &models.SessionSummary{SessionID: "s1", TenantID: "t1", UserID: "u1", Summary: "部署讨论", MessageCount: 2}); err != nil {
		t.Fatal(err)
	}

	h := newAdminServer(NewAdminHandler(profiles, sessions, ""))

	// 列表用户
	rr := doJSON(t, h, http.MethodGet, "/users", "", map[string]string{HeaderTenantID: "t1"})
	if rr.Code != http.StatusOK {
		t.Fatalf("list users status = %d", rr.Code)
	}
	var listResp models.APIResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &listResp)
	if listResp.Data.(map[string]interface{})["count"].(float64) != 1 {
		t.Errorf("user count = %v", listResp.Data)
	}

	// 用户聚合视图
	rr = doJSON(t, h, http.MethodGet, "/users/u1", "", map[string]string{HeaderTenantID: "t1"})
	if rr.Code != http.StatusOK {
		t.Fatalf("get user status = %d", rr.Code)
	}
	var userResp models.APIResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &userResp)
	ud := userResp.Data.(map[string]interface{})
	if ud["profile"].(map[string]interface{})["lang"] != "zh" {
		t.Errorf("profile = %v", ud["profile"])
	}
	if float64(len(ud["sessions"].([]interface{}))) != 1 {
		t.Errorf("sessions = %v", ud["sessions"])
	}

	// 会话检索
	rr = doJSON(t, h, http.MethodGet, "/sessions?user_id=u1", "", map[string]string{HeaderTenantID: "t1"})
	if rr.Code != http.StatusOK {
		t.Fatalf("search sessions status = %d", rr.Code)
	}

	// 导出用户（带 Content-Disposition）
	rr = doJSON(t, h, http.MethodGet, "/users/u1/export", "", map[string]string{HeaderTenantID: "t1"})
	if rr.Code != http.StatusOK || !strings.Contains(rr.Header().Get("Content-Disposition"), "attachment") {
		t.Fatalf("export status = %d hdr = %v", rr.Code, rr.Header().Get("Content-Disposition"))
	}

	// 删除用户 → 其画像与会话均清空
	rr = doJSON(t, h, http.MethodDelete, "/users/u1", "", map[string]string{HeaderTenantID: "t1"})
	if rr.Code != http.StatusOK {
		t.Fatalf("delete user status = %d", rr.Code)
	}
	rr = doJSON(t, h, http.MethodGet, "/users/u1", "", map[string]string{HeaderTenantID: "t1"})
	if rr.Code != http.StatusNotFound {
		t.Errorf("after delete status = %d, want 404", rr.Code)
	}
	rr = doJSON(t, h, http.MethodGet, "/sessions/s1", "", map[string]string{HeaderTenantID: "t1"})
	if rr.Code != http.StatusNotFound {
		t.Errorf("session after user delete status = %d, want 404", rr.Code)
	}
}

func TestAdminTokenGuard(t *testing.T) {
	profiles := service.NewMemoryProfileStore(nil)
	sessions := service.NewMemorySessionSummaryStore(nil)
	h := newAdminServer(NewAdminHandler(profiles, sessions, "secret"))

	// 无令牌 → 401
	rr := doJSON(t, h, http.MethodGet, "/users", "", map[string]string{HeaderTenantID: "t1"})
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("no token status = %d, want 401", rr.Code)
	}
	// 带令牌 → 200
	rr = doJSON(t, h, http.MethodGet, "/users", "", map[string]string{HeaderTenantID: "t1", "Authorization": "Bearer secret"})
	if rr.Code != http.StatusOK {
		t.Errorf("with token status = %d, want 200", rr.Code)
	}

	// 导出链接携带查询令牌
	rr = doJSON(t, h, http.MethodGet, "/users/u1/export?token=secret", "", map[string]string{HeaderTenantID: "t1"})
	if rr.Code != http.StatusOK {
		t.Errorf("export with query token status = %d, want 200", rr.Code)
	}
}
