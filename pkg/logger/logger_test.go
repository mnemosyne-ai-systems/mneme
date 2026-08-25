package logger

import (
	"context"
	"testing"
)

func TestInitAndContext(t *testing.T) {
	Init("debug", "text")
	if L().Level.String() != "debug" {
		t.Errorf("level = %s", L().Level)
	}
	Init("bogus", "json")
	if L().Level.String() != "info" {
		t.Errorf("fallback level = %s", L().Level)
	}

	ctx := WithTenantID(WithTraceID(context.Background(), "tr-1"), "tn-1")
	if TraceIDFrom(ctx) != "tr-1" {
		t.Error("trace id missing")
	}
	if TenantIDFrom(ctx) != "tn-1" {
		t.Error("tenant id missing")
	}
	e := FromContext(ctx)
	if e.Data["trace_id"] != "tr-1" || e.Data["tenant_id"] != "tn-1" {
		t.Errorf("fields = %+v", e.Data)
	}
	if TraceIDFrom(context.Background()) != "" {
		t.Error("expected empty trace id")
	}
	if WithField("a", 1).Data["a"] != 1 {
		t.Error("WithField failed")
	}
	if WithFields(map[string]interface{}{"b": 2}).Data["b"] != 2 {
		t.Error("WithFields failed")
	}
}
