package models

import "time"

// SessionSummary 是单场会话的概括，独立存储于 session_summary 表，
// 与跨会话的用户画像（entity_memory）区分：它描述"这次对话聊了什么"。
type SessionSummary struct {
	SessionID    string    `json:"session_id"`
	TenantID     string    `json:"tenant_id"`
	UserID       string    `json:"user_id"`
	Summary      string    `json:"summary"`
	MessageCount int       `json:"message_count"`
	StartedAt    time.Time `json:"started_at"`
	EndedAt      time.Time `json:"ended_at"`
	Version      int64     `json:"_version"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// Validate 校验必填字段。
func (s *SessionSummary) Validate() error {
	if s == nil {
		return NewValidationError("session_summary", "summary is nil")
	}
	if s.SessionID == "" {
		return NewValidationError("session_id", "must not be empty")
	}
	if s.TenantID == "" {
		return NewValidationError("tenant_id", "must not be empty")
	}
	if s.Summary == "" {
		return NewValidationError("summary", "must not be empty")
	}
	return nil
}
