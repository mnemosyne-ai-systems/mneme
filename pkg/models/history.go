package models

import (
	"encoding/json"
	"time"
)

// 会话角色常量。
const (
	RoleUser      = "user"
	RoleAssistant = "assistant"
	RoleSystem    = "system"
	RoleTool      = "tool"
)

// HistoryMessage 管道一：会话历史消息。
type HistoryMessage struct {
	MessageID        string    `json:"message_id"`
	TenantID         string    `json:"tenant_id"`
	SessionID        string    `json:"session_id"`
	UserID           string    `json:"user_id"`
	Role             string    `json:"role"`
	Content          string    `json:"content"`
	ModelName        string    `json:"model_name"`
	TotalTokens      int32     `json:"total_tokens"`
	PromptTokens     int32     `json:"prompt_tokens"`
	CompletionTokens int32     `json:"completion_tokens"`
	ResponseTimeMs   int32     `json:"response_time_ms"`
	Metadata         string    `json:"metadata"`
	CreatedAt        time.Time `json:"created_at"`
}

// Validate 校验必填字段与取值范围。
func (m *HistoryMessage) Validate() error {
	if m == nil {
		return NewValidationError("history_message", "message is nil")
	}
	if m.MessageID == "" {
		return NewValidationError("message_id", "must not be empty")
	}
	if m.TenantID == "" {
		return NewValidationError("tenant_id", "must not be empty")
	}
	if m.SessionID == "" {
		return NewValidationError("session_id", "must not be empty")
	}
	if !isValidRole(m.Role) {
		return NewValidationError("role", "must be one of user|assistant|system|tool")
	}
	if m.Content == "" {
		return NewValidationError("content", "must not be empty")
	}
	if m.TotalTokens < 0 || m.PromptTokens < 0 || m.CompletionTokens < 0 {
		return NewValidationError("tokens", "must not be negative")
	}
	if m.ResponseTimeMs < 0 {
		return NewValidationError("response_time_ms", "must not be negative")
	}
	if m.Metadata != "" && !json.Valid([]byte(m.Metadata)) {
		return NewValidationError("metadata", "must be a valid JSON string")
	}
	return nil
}

// Normalize 补齐可推导的默认值。
func (m *HistoryMessage) Normalize() {
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now().UTC()
	} else {
		m.CreatedAt = m.CreatedAt.UTC()
	}
	if m.MessageID == "" {
		m.MessageID = NewID()
	}
	if m.TotalTokens == 0 && (m.PromptTokens > 0 || m.CompletionTokens > 0) {
		m.TotalTokens = m.PromptTokens + m.CompletionTokens
	}
}

// PartitionKey 返回消息在 MQ 中的分区键，保证同一会话有序。
func (m *HistoryMessage) PartitionKey() string {
	return m.TenantID + ":" + m.SessionID
}

func isValidRole(role string) bool {
	switch role {
	case RoleUser, RoleAssistant, RoleSystem, RoleTool:
		return true
	default:
		return false
	}
}
