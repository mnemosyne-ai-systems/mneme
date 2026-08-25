package models

import "time"

// 轨迹步骤类型常量。
const (
	StepTypeLLMCall      = "llm_call"
	StepTypeToolCall     = "tool_call"
	StepTypeRAGRetrieval = "rag_retrieval"
	StepTypeReasoning    = "reasoning"
)

// TraceMessage 管道二：Agent 执行轨迹的单个 Span。
type TraceMessage struct {
	RequestID  string            `json:"request_id"`
	TenantID   string            `json:"tenant_id"`
	SessionID  string            `json:"session_id"`
	UserID     string            `json:"user_id"`
	StepType   string            `json:"step_type"`
	StepName   string            `json:"step_name"`
	Input      string            `json:"input"`
	Output     string            `json:"output"`
	Metadata   map[string]string `json:"metadata"`
	DurationMs int32             `json:"duration_ms"`
	Timestamp  time.Time         `json:"timestamp"`
}

// Validate 校验必填字段与取值范围。
func (t *TraceMessage) Validate() error {
	if t == nil {
		return NewValidationError("trace_message", "message is nil")
	}
	if t.RequestID == "" {
		return NewValidationError("request_id", "must not be empty")
	}
	if t.TenantID == "" {
		return NewValidationError("tenant_id", "must not be empty")
	}
	if !isValidStepType(t.StepType) {
		return NewValidationError("step_type", "must be one of llm_call|tool_call|rag_retrieval|reasoning")
	}
	if t.StepName == "" {
		return NewValidationError("step_name", "must not be empty")
	}
	if t.DurationMs < 0 {
		return NewValidationError("duration_ms", "must not be negative")
	}
	return nil
}

// Normalize 补齐可推导的默认值。
func (t *TraceMessage) Normalize() {
	if t.Timestamp.IsZero() {
		t.Timestamp = time.Now().UTC()
	} else {
		t.Timestamp = t.Timestamp.UTC()
	}
	if t.Metadata == nil {
		t.Metadata = map[string]string{}
	}
}

// PartitionKey 返回分区键，保证同一 request 的 Span 落在同一消费者。
func (t *TraceMessage) PartitionKey() string {
	return t.TenantID + ":" + t.RequestID
}

func isValidStepType(s string) bool {
	switch s {
	case StepTypeLLMCall, StepTypeToolCall, StepTypeRAGRetrieval, StepTypeReasoning:
		return true
	default:
		return false
	}
}
