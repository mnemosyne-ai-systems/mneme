// Package logger 提供结构化日志封装（基于 logrus），
// 支持从 context 中继承 trace_id / tenant_id 等字段。
package logger

import (
	"context"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
)

type ctxKey string

const (
	ctxKeyTraceID  ctxKey = "trace_id"
	ctxKeyTenantID ctxKey = "tenant_id"
)

var std = logrus.New()

// Init 根据配置初始化全局日志器。
func Init(level, format string) {
	lv, err := logrus.ParseLevel(strings.ToLower(level))
	if err != nil {
		lv = logrus.InfoLevel
	}
	std.SetLevel(lv)
	std.SetOutput(os.Stdout)
	if strings.EqualFold(format, "text") {
		std.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	} else {
		std.SetFormatter(&logrus.JSONFormatter{TimestampFormat: "2006-01-02T15:04:05.000Z07:00"})
	}
}

// L 返回全局 logger。
func L() *logrus.Logger { return std }

// WithTraceID 把 trace_id 放入 context。
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, ctxKeyTraceID, traceID)
}

// WithTenantID 把 tenant_id 放入 context。
func WithTenantID(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, ctxKeyTenantID, tenantID)
}

// TraceIDFrom 从 context 提取 trace_id。
func TraceIDFrom(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyTraceID).(string); ok {
		return v
	}
	return ""
}

// TenantIDFrom 从 context 提取 tenant_id。
func TenantIDFrom(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyTenantID).(string); ok {
		return v
	}
	return ""
}

// FromContext 返回带 trace_id / tenant_id 字段的日志条目。
func FromContext(ctx context.Context) *logrus.Entry {
	fields := logrus.Fields{}
	if v := TraceIDFrom(ctx); v != "" {
		fields["trace_id"] = v
	}
	if v := TenantIDFrom(ctx); v != "" {
		fields["tenant_id"] = v
	}
	return std.WithFields(fields)
}

// WithField 返回带单个字段的日志条目。
func WithField(key string, value interface{}) *logrus.Entry {
	return std.WithField(key, value)
}

// WithFields 返回带多个字段的日志条目。
func WithFields(fields map[string]interface{}) *logrus.Entry {
	return std.WithFields(fields)
}
