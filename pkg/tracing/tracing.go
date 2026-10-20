// Package tracing 提供基于 OpenTelemetry 的链路追踪初始化与工具函数。
package tracing

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

// Config 链路追踪配置。
type Config struct {
	Enabled      bool
	ServiceName  string
	OTLPEndpoint string
	SampleRatio  float64
}

// ShutdownFunc 用于优雅关闭 TracerProvider。
type ShutdownFunc func(context.Context) error

// Init 初始化全局 TracerProvider。未启用时返回 no-op shutdown。
func Init(ctx context.Context, cfg Config) (ShutdownFunc, error) {
	if !cfg.Enabled {
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{}, propagation.Baggage{}))
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(cfg.OTLPEndpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("tracing: create otlp exporter: %w", err)
	}

	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(cfg.ServiceName),
		attribute.String("service.namespace", "mnemosyne"),
	))
	if err != nil {
		return nil, fmt.Errorf("tracing: build resource: %w", err)
	}

	ratio := cfg.SampleRatio
	if ratio <= 0 {
		ratio = 1
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))

	return tp.Shutdown, nil
}

// Tracer 返回 Mneme 的命名 tracer。
func Tracer(name string) trace.Tracer {
	return otel.Tracer("github.com/mnemosyne-ai-systems/mneme/" + name)
}

// Start 开启一个 span。
func Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return Tracer("mneme").Start(ctx, name, trace.WithAttributes(attrs...))
}

// TraceIDFromContext 返回当前 span 的 trace id（无则返回空串）。
func TraceIDFromContext(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}
