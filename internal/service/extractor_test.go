package service

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mnemosyne-ai-systems/mneme/pkg/models"
)

func testEvents() []*models.MemoryEvent {
	ts := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	return []*models.MemoryEvent{
		{EntityID: "u1", TenantID: "t1", EntityType: models.EntityTypeUser, EventType: "preference", Timestamp: ts, Payload: map[string]interface{}{"lang": "go"}},
		{EntityID: "u1", TenantID: "t1", EntityType: models.EntityTypeUser, EventType: "focus", Timestamp: ts.Add(time.Hour), Payload: map[string]interface{}{"topic": "distributed"}},
	}
}

var errBoom = errors.New("boom")

func TestNewLLMExtractorDefaults(t *testing.T) {
	e := NewLLMExtractor(LLMConfig{})
	if e.cfg.Timeout != 20*time.Second {
		t.Errorf("default timeout = %v", e.cfg.Timeout)
	}
	if e.cfg.MaxEvents != 200 {
		t.Errorf("default maxEvents = %d", e.cfg.MaxEvents)
	}
	if e.client == nil {
		t.Error("client not initialized")
	}
}

func TestExtractDisabled(t *testing.T) {
	e := NewLLMExtractor(LLMConfig{Enabled: false})
	_, err := e.Extract(context.Background(), testEvents())
	if err == nil {
		t.Fatal("expected disabled error")
	}
	if !strings.Contains(err.Error(), "disabled") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestExtractEmptyEvents(t *testing.T) {
	e := NewLLMExtractor(LLMConfig{Enabled: true})
	out, err := e.Extract(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Errorf("expected empty profile, got %v", out)
	}
}

func TestExtractHTTPError(t *testing.T) {
	// 端点直接返回 500，验证错误传播
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	e := NewLLMExtractor(LLMConfig{Enabled: true, Endpoint: srv.URL, Model: "m", Timeout: 5 * time.Second})
	_, err := e.Extract(context.Background(), testEvents())
	if err == nil {
		t.Fatal("expected error from 500")
	}
	if !strings.Contains(err.Error(), "status 500") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestExtractSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer key" {
			t.Errorf("missing bearer token, got %q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"{\"lang\":\"go\",\"level\":5}"}}]}`))
	}))
	defer srv.Close()

	e := NewLLMExtractor(LLMConfig{Enabled: true, Endpoint: srv.URL, Model: "m", APIKey: "key", Timeout: 5 * time.Second})
	profile, err := e.Extract(context.Background(), testEvents())
	if err != nil {
		t.Fatal(err)
	}
	if profile["lang"] != "go" {
		t.Errorf("lang = %q", profile["lang"])
	}
	if profile["level"] != "5" {
		t.Errorf("level = %q", profile["level"])
	}
	if profile["_extracted_by"] != "llm:m" {
		t.Errorf("extracted_by = %q", profile["_extracted_by"])
	}
}

func TestExtractNoChoices(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[]}`))
	}))
	defer srv.Close()

	e := NewLLMExtractor(LLMConfig{Enabled: true, Endpoint: srv.URL, Model: "m", Timeout: 5 * time.Second})
	_, err := e.Extract(context.Background(), testEvents())
	if err == nil {
		t.Fatal("expected no-choices error")
	}
}

func TestExtractBadJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"not json"}}]}`))
	}))
	defer srv.Close()

	e := NewLLMExtractor(LLMConfig{Enabled: true, Endpoint: srv.URL, Model: "m", Timeout: 5 * time.Second})
	_, err := e.Extract(context.Background(), testEvents())
	if err == nil {
		t.Fatal("expected parse error")
	}
}

func TestFallback(t *testing.T) {
	e := NewLLMExtractor(LLMConfig{Enabled: false})
	profile, err := e.Fallback(context.Background(), testEvents())
	if err != nil {
		t.Fatal(err)
	}
	if profile["_extracted_by"] != "fallback" {
		t.Errorf("extracted_by = %q", profile["_extracted_by"])
	}
	if profile["event_count"] != "2" {
		t.Errorf("event_count = %q", profile["event_count"])
	}
	// 两种事件类型
	if !strings.Contains(profile["event_types"], "preference=1") || !strings.Contains(profile["event_types"], "focus=1") {
		t.Errorf("event_types = %q", profile["event_types"])
	}
	if _, ok := profile["recent_events"]; !ok {
		t.Error("expected recent_events digest")
	}
}

func TestBuildFallbackProfileEmpty(t *testing.T) {
	profile := BuildFallbackProfile(nil)
	if profile["event_count"] != "0" {
		t.Errorf("event_count = %q", profile["event_count"])
	}
	if _, ok := profile["first_event_at"]; ok {
		t.Error("empty profile should not have first_event_at")
	}
}

func TestEventDigests(t *testing.T) {
	events := testEvents()
	digests := eventDigests(events)
	if len(digests) != 2 {
		t.Fatalf("digests = %d", len(digests))
	}
	if digests[0]["event_type"] != "preference" {
		t.Errorf("event_type = %v", digests[0]["event_type"])
	}
}

func TestParseFlatJSON(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantKey string
		wantVal string
		wantErr bool
	}{
		{name: "plain", input: `{"a":"b"}`, wantKey: "a", wantVal: "b"},
		{name: "codefence", input: "```json\n{\"a\":\"b\"}\n```", wantKey: "a", wantVal: "b"},
		{name: "nested", input: `{"a":{"x":1}}`, wantKey: "a", wantVal: `{"x":1}`},
		{name: "number", input: `{"a":5}`, wantKey: "a", wantVal: "5"},
		{name: "noobject", input: "hello", wantErr: true},
		{name: "badjson", input: `{a:b}`, wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := parseFlatJSON(c.input)
			if c.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if out[c.wantKey] != c.wantVal {
				t.Errorf("%s = %q, want %q", c.wantKey, out[c.wantKey], c.wantVal)
			}
		})
	}
}

type mockExtractor struct {
	extractErr error
	fallback   map[string]string
	fallbackOK bool
	extracted  map[string]string
}

func (m *mockExtractor) Extract(context.Context, []*models.MemoryEvent) (map[string]string, error) {
	return m.extracted, m.extractErr
}

func (m *mockExtractor) Fallback(context.Context, []*models.MemoryEvent) (map[string]string, error) {
	if m.fallbackOK {
		return m.fallback, nil
	}
	return nil, errBoom
}

func TestExtractWithFallbackLLM(t *testing.T) {
	ex := &mockExtractor{extracted: map[string]string{"a": "b"}}
	profile, method := ExtractWithFallback(context.Background(), ex, testEvents())
	if method != "llm" {
		t.Errorf("method = %s", method)
	}
	if profile["a"] != "b" {
		t.Errorf("profile = %v", profile)
	}
}

func TestExtractWithFallbackLLMFallback(t *testing.T) {
	ex := &mockExtractor{extractErr: errBoom, fallback: map[string]string{"_extracted_by": "fallback"}, fallbackOK: true}
	profile, method := ExtractWithFallback(context.Background(), ex, testEvents())
	if method != "fallback" {
		t.Errorf("method = %s", method)
	}
	if profile["_extracted_by"] != "fallback" {
		t.Errorf("profile = %v", profile)
	}
}

func TestExtractWithFallbackBothFail(t *testing.T) {
	ex := &mockExtractor{extractErr: errBoom, fallbackOK: false}
	profile, method := ExtractWithFallback(context.Background(), ex, testEvents())
	if method != "fallback" {
		t.Errorf("method = %s", method)
	}
	// 兜底到 BuildFallbackProfile
	if profile["event_count"] != "2" {
		t.Errorf("profile = %v", profile)
	}
}

func TestExtractWithFallbackEmptyProfile(t *testing.T) {
	// LLM 成功但返回空，也应降级
	ex := &mockExtractor{extracted: map[string]string{}, fallback: map[string]string{"_extracted_by": "fallback"}, fallbackOK: true}
	_, method := ExtractWithFallback(context.Background(), ex, testEvents())
	if method != "fallback" {
		t.Errorf("method = %s", method)
	}
}
