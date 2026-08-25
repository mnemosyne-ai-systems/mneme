package models

import "time"

// 实体类型常量。
const (
	EntityTypeUser       = "user"
	EntityTypeEnterprise = "enterprise"
)

// MemoryEvent 管道三：长期记忆事件。
type MemoryEvent struct {
	EventID    string                 `json:"event_id"`
	EntityID   string                 `json:"entity_id"`
	TenantID   string                 `json:"tenant_id"`
	EntityType string                 `json:"entity_type"`
	EventType  string                 `json:"event_type"`
	Payload    map[string]interface{} `json:"payload"`
	Timestamp  time.Time              `json:"timestamp"`
}

// Validate 校验必填字段。
func (e *MemoryEvent) Validate() error {
	if e == nil {
		return NewValidationError("memory_event", "event is nil")
	}
	if e.EventID == "" {
		return NewValidationError("event_id", "must not be empty")
	}
	if e.EntityID == "" {
		return NewValidationError("entity_id", "must not be empty")
	}
	if e.TenantID == "" {
		return NewValidationError("tenant_id", "must not be empty")
	}
	if !isValidEntityType(e.EntityType) {
		return NewValidationError("entity_type", "must be one of user|enterprise")
	}
	if e.EventType == "" {
		return NewValidationError("event_type", "must not be empty")
	}
	return nil
}

// Normalize 补齐可推导的默认值。
func (e *MemoryEvent) Normalize() {
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now().UTC()
	} else {
		e.Timestamp = e.Timestamp.UTC()
	}
	if e.EventID == "" {
		e.EventID = NewID()
	}
	if e.Payload == nil {
		e.Payload = map[string]interface{}{}
	}
}

// PartitionKey 返回分区键，保证同一实体事件有序。
func (e *MemoryEvent) PartitionKey() string {
	return e.TenantID + ":" + e.EntityType + ":" + e.EntityID
}

// EntityMemory 是萃取后的实体画像，落地到 ClickHouse entity_memory 表。
type EntityMemory struct {
	EntityID   string    `json:"entity_id"`
	TenantID   string    `json:"tenant_id"`
	EntityType string    `json:"entity_type"`
	Profile    string    `json:"profile"`
	Version    int64     `json:"_version"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// Validate 校验实体画像。
func (m *EntityMemory) Validate() error {
	if m == nil {
		return NewValidationError("entity_memory", "memory is nil")
	}
	if m.EntityID == "" {
		return NewValidationError("entity_id", "must not be empty")
	}
	if m.TenantID == "" {
		return NewValidationError("tenant_id", "must not be empty")
	}
	if !isValidEntityType(m.EntityType) {
		return NewValidationError("entity_type", "must be one of user|enterprise")
	}
	return nil
}

func isValidEntityType(t string) bool {
	switch t {
	case EntityTypeUser, EntityTypeEnterprise:
		return true
	default:
		return false
	}
}
