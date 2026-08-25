package models

import (
	"encoding/json"
	"testing"
	"time"
)

func validHistory() *HistoryMessage {
	return &HistoryMessage{
		MessageID: "m1",
		TenantID:  "t1",
		SessionID: "s1",
		UserID:    "u1",
		Role:      RoleUser,
		Content:   "hello",
		Metadata:  `{"k":"v"}`,
		CreatedAt: time.Now().UTC(),
	}
}

func TestHistoryMessageValidate(t *testing.T) {
	if err := validHistory().Validate(); err != nil {
		t.Fatalf("expect valid, got %v", err)
	}

	cases := map[string]func(*HistoryMessage){
		"empty message_id": func(m *HistoryMessage) { m.MessageID = "" },
		"empty tenant":     func(m *HistoryMessage) { m.TenantID = "" },
		"empty session":    func(m *HistoryMessage) { m.SessionID = "" },
		"bad role":         func(m *HistoryMessage) { m.Role = "robot" },
		"empty content":    func(m *HistoryMessage) { m.Content = "" },
		"negative tokens":  func(m *HistoryMessage) { m.TotalTokens = -1 },
		"negative rt":      func(m *HistoryMessage) { m.ResponseTimeMs = -5 },
		"bad metadata":     func(m *HistoryMessage) { m.Metadata = "{not json" },
	}
	for name, mut := range cases {
		m := validHistory()
		mut(m)
		if err := m.Validate(); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}

	var nilMsg *HistoryMessage
	if err := nilMsg.Validate(); err == nil {
		t.Error("nil message should be invalid")
	}
}

func TestHistoryMessageNormalize(t *testing.T) {
	m := &HistoryMessage{PromptTokens: 3, CompletionTokens: 4}
	m.Normalize()
	if m.CreatedAt.IsZero() {
		t.Error("created_at should be filled")
	}
	if m.MessageID == "" {
		t.Error("message_id should be generated")
	}
	if m.TotalTokens != 7 {
		t.Errorf("total_tokens = %d, want 7", m.TotalTokens)
	}
	if got := m.PartitionKey(); got != ":" {
		t.Logf("partition key: %s", got)
	}
}

func TestHistoryMessageJSONRoundTrip(t *testing.T) {
	m := validHistory()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var out HistoryMessage
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.MessageID != m.MessageID || out.Content != m.Content {
		t.Errorf("round trip mismatch: %+v", out)
	}
}

func validTrace() *TraceMessage {
	return &TraceMessage{
		RequestID: "r1",
		TenantID:  "t1",
		SessionID: "s1",
		StepType:  StepTypeLLMCall,
		StepName:  "chat",
		Timestamp: time.Now().UTC(),
	}
}

func TestTraceMessageValidate(t *testing.T) {
	if err := validTrace().Validate(); err != nil {
		t.Fatalf("expect valid: %v", err)
	}
	cases := map[string]func(*TraceMessage){
		"empty request": func(m *TraceMessage) { m.RequestID = "" },
		"empty tenant":  func(m *TraceMessage) { m.TenantID = "" },
		"bad step type": func(m *TraceMessage) { m.StepType = "x" },
		"empty name":    func(m *TraceMessage) { m.StepName = "" },
		"negative dur":  func(m *TraceMessage) { m.DurationMs = -1 },
	}
	for name, mut := range cases {
		m := validTrace()
		mut(m)
		if err := m.Validate(); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	var nilT *TraceMessage
	if err := nilT.Validate(); err == nil {
		t.Error("nil trace should be invalid")
	}
}

func TestTraceMessageNormalizeAndKey(t *testing.T) {
	m := &TraceMessage{RequestID: "r1", TenantID: "t1"}
	m.Normalize()
	if m.Timestamp.IsZero() || m.Metadata == nil {
		t.Error("normalize failed")
	}
	if m.PartitionKey() != "t1:r1" {
		t.Errorf("key = %s", m.PartitionKey())
	}
}

func TestTraceJSONRoundTrip(t *testing.T) {
	m := validTrace()
	m.Metadata = map[string]string{"a": "b"}
	b, _ := json.Marshal(m)
	var out TraceMessage
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Metadata["a"] != "b" {
		t.Error("metadata lost")
	}
}

func validEvent() *MemoryEvent {
	return &MemoryEvent{
		EventID:    "e1",
		EntityID:   "u1",
		TenantID:   "t1",
		EntityType: EntityTypeUser,
		EventType:  "preference_updated",
		Payload:    map[string]interface{}{"lang": "zh"},
		Timestamp:  time.Now().UTC(),
	}
}

func TestMemoryEventValidate(t *testing.T) {
	if err := validEvent().Validate(); err != nil {
		t.Fatalf("expect valid: %v", err)
	}
	cases := map[string]func(*MemoryEvent){
		"empty event id":  func(m *MemoryEvent) { m.EventID = "" },
		"empty entity id": func(m *MemoryEvent) { m.EntityID = "" },
		"empty tenant":    func(m *MemoryEvent) { m.TenantID = "" },
		"bad entity type": func(m *MemoryEvent) { m.EntityType = "team" },
		"empty type":      func(m *MemoryEvent) { m.EventType = "" },
	}
	for name, mut := range cases {
		m := validEvent()
		mut(m)
		if err := m.Validate(); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	var nilE *MemoryEvent
	if err := nilE.Validate(); err == nil {
		t.Error("nil event should be invalid")
	}
}

func TestMemoryEventNormalize(t *testing.T) {
	e := &MemoryEvent{EntityID: "u1", TenantID: "t1", EntityType: EntityTypeUser}
	e.Normalize()
	if e.EventID == "" || e.Timestamp.IsZero() || e.Payload == nil {
		t.Error("normalize failed")
	}
	if e.PartitionKey() != "t1:user:u1" {
		t.Errorf("key = %s", e.PartitionKey())
	}
}

func TestMemoryEventJSONRoundTrip(t *testing.T) {
	e := validEvent()
	b, _ := json.Marshal(e)
	var out MemoryEvent
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Payload["lang"] != "zh" {
		t.Error("payload lost")
	}
}

func TestEntityMemoryValidate(t *testing.T) {
	m := &EntityMemory{EntityID: "u1", TenantID: "t1", EntityType: EntityTypeUser, Profile: "{}", Version: 1}
	if err := m.Validate(); err != nil {
		t.Fatalf("expect valid: %v", err)
	}
	m.EntityType = "bad"
	if err := m.Validate(); err == nil {
		t.Error("expected error")
	}
	var nilM *EntityMemory
	if err := nilM.Validate(); err == nil {
		t.Error("nil should be invalid")
	}
	m2 := &EntityMemory{TenantID: "t"}
	if err := m2.Validate(); err == nil {
		t.Error("expected error for empty entity id")
	}
	m3 := &EntityMemory{EntityID: "u"}
	if err := m3.Validate(); err == nil {
		t.Error("expected error for empty tenant")
	}
}

func TestAPIResponse(t *testing.T) {
	r := OK(map[string]string{"a": "b"})
	if r.Code != 0 || r.Message != "ok" {
		t.Error("OK response wrong")
	}
	e := Err(400, "bad")
	if e.Code != 400 || e.Data != nil {
		t.Error("Err response wrong")
	}
	b, _ := json.Marshal(e)
	if string(b) != `{"code":400,"message":"bad"}` {
		t.Errorf("json = %s", b)
	}
}

func TestValidationError(t *testing.T) {
	err := NewValidationError("f", "r")
	if err.Error() != `invalid field "f": r` {
		t.Errorf("msg = %s", err.Error())
	}
}

func TestNewID(t *testing.T) {
	a, b := NewID(), NewID()
	if a == "" || a == b || len(a) != 32 {
		t.Errorf("bad id: %s %s", a, b)
	}
}
