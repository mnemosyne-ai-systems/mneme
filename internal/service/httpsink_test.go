package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mnemosyne-labs/mneme/pkg/models"
)

func testMessages(n int) []*models.HistoryMessage {
	msgs := make([]*models.HistoryMessage, 0, n)
	for i := 0; i < n; i++ {
		msgs = append(msgs, &models.HistoryMessage{
			MessageID: "m" + models.NewID(),
			TenantID:  "t1",
			SessionID: "s1",
			Role:      models.RoleUser,
			Content:   "hello",
		})
	}
	return msgs
}

func TestHTTPSinkPostPayload(t *testing.T) {
	var gotAuth, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		var body historyBatchPayload
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode: %v", err)
		}
		if len(body.Messages) != 2 || body.Messages[0].TenantID != "t1" {
			t.Errorf("body = %+v", body)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	sink := NewHTTPHistorySink(HTTPSinkConfig{Endpoint: srv.URL + "/api/v1/history/batch", Token: "secret"})
	if err := sink.PublishHistoryBatch(context.Background(), testMessages(2)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if gotPath != "/api/v1/history/batch" {
		t.Errorf("path = %s", gotPath)
	}
	if gotAuth != "Bearer secret" {
		t.Errorf("auth = %q", gotAuth)
	}
}

func TestHTTPSinkRetryThenSuccess(t *testing.T) {
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&attempts, 1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	sink := NewHTTPHistorySink(HTTPSinkConfig{
		Endpoint:    srv.URL,
		MaxRetries:  3,
		BackoffBase: time.Millisecond,
	})
	if err := sink.PublishHistoryBatch(context.Background(), testMessages(1)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if n := atomic.LoadInt32(&attempts); n != 2 {
		t.Errorf("attempts = %d, want 2", n)
	}
}

func TestHTTPSinkPermanentErrorNoRetry(t *testing.T) {
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	sink := NewHTTPHistorySink(HTTPSinkConfig{Endpoint: srv.URL, BackoffBase: time.Millisecond})
	err := sink.PublishHistoryBatch(context.Background(), testMessages(1))
	if err == nil {
		t.Fatal("expected error")
	}
	var statusErr *httpStatusError
	if !errors.As(err, &statusErr) || statusErr.Status != http.StatusBadRequest {
		t.Errorf("err = %v", err)
	}
	if n := atomic.LoadInt32(&attempts); n != 1 {
		t.Errorf("4xx should not retry, attempts = %d", n)
	}
}

func TestHTTPSinkChunking(t *testing.T) {
	var requests int32
	var total int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		var body historyBatchPayload
		_ = json.NewDecoder(r.Body).Decode(&body)
		atomic.AddInt32(&total, int32(len(body.Messages)))
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	sink := NewHTTPHistorySink(HTTPSinkConfig{Endpoint: srv.URL, MaxBatchSize: 2})
	if err := sink.PublishHistoryBatch(context.Background(), testMessages(5)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if n := atomic.LoadInt32(&requests); n != 3 {
		t.Errorf("requests = %d, want 3", n)
	}
	if n := atomic.LoadInt32(&total); n != 5 {
		t.Errorf("total = %d, want 5", n)
	}
}

func TestHTTPSinkValidation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("server should not be reached on validation failure")
	}))
	defer srv.Close()

	sink := NewHTTPHistorySink(HTTPSinkConfig{Endpoint: srv.URL})
	bad := []*models.HistoryMessage{{TenantID: "t", Role: "user", Content: "x"}}
	if err := sink.PublishHistoryBatch(context.Background(), bad); err == nil {
		t.Error("expected validation error for missing session_id")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sink.PublishHistoryBatch(ctx, nil); err != nil {
		t.Errorf("empty batch should be no-op, got %v", err)
	}
}

func TestHTTPSinkContextCanceled(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		w.WriteHeader(http.StatusAccepted)
	}))
	defer func() { close(release); srv.Close() }()

	sink := NewHTTPHistorySink(HTTPSinkConfig{Endpoint: srv.URL, MaxRetries: 3, BackoffBase: time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := sink.PublishHistoryBatch(ctx, testMessages(1)); err == nil {
		t.Fatal("expected context error")
	}
}

func TestHTTPSinkBackoffCap(t *testing.T) {
	sink := NewHTTPHistorySink(HTTPSinkConfig{BackoffBase: 100 * time.Millisecond})
	if d := sink.backoff(1); d > 200*time.Millisecond {
		t.Errorf("first backoff = %v", d)
	}
	if d := sink.backoff(20); d > 30*time.Second {
		t.Errorf("backoff not capped: %v", d)
	}
}
