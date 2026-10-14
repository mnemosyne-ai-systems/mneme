package models

import "time"

// AuditEntry 审计日志条目。
type AuditEntry struct {
	ID       uint64    `json:"id"`
	TenantID string    `json:"tenant_id"`
	Actor    string    `json:"actor"`
	Action   string    `json:"action"`
	Target   string    `json:"target"`
	Detail   string    `json:"detail"`
	At       time.Time `json:"at"`
}

// AuditAction 审计动作类型常量。
const (
	AuditActionDeleteUser     = "delete_user"
	AuditActionHardDeleteUser = "hard_delete_user"
	AuditActionDeleteSession  = "delete_session"
	AuditActionHardDelete     = "hard_delete_session"
	AuditActionExportUser     = "export_user"
	AuditActionExportSession  = "export_session"
	AuditActionSummarize      = "summarize_sessions"
)
