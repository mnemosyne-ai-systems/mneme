package storage

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mnemosyne/mneme/pkg/models"
)

func TestSchemaStatements(t *testing.T) {
	stmts := SchemaStatements()
	if len(stmts) != 5 {
		t.Fatalf("want 5 statements, got %d", len(stmts))
	}
	if !strings.Contains(stmts[0], "history_message") {
		t.Error("missing history_message DDL")
	}
	if !strings.Contains(stmts[1], "INTERVAL 7 DAY") {
		t.Error("traces DDL missing TTL")
	}
	if !strings.Contains(stmts[2], "ReplacingMergeTree(_version)") {
		t.Error("memory DDL missing ReplacingMergeTree")
	}
	if !strings.Contains(stmts[3], "session_summary") {
		t.Error("missing session_summary DDL")
	}
	if !strings.Contains(stmts[4], "audit_log") {
		t.Error("missing audit_log DDL")
	}
	if DatabaseDDL("db") != "CREATE DATABASE IF NOT EXISTS db" {
		t.Error("database ddl wrong")
	}
}

func TestMockInserter(t *testing.T) {
	ctx := context.Background()
	m := NewMockInserter()
	if err := m.InsertHistory(ctx, []*models.HistoryMessage{{MessageID: "1"}}); err != nil {
		t.Fatal(err)
	}
	if err := m.InsertTraces(ctx, []*models.TraceMessage{{RequestID: "r"}}); err != nil {
		t.Fatal(err)
	}
	if err := m.InsertMemory(ctx, []*models.EntityMemory{{EntityID: "e"}}); err != nil {
		t.Fatal(err)
	}
	if err := m.InsertSessionSummary(ctx, []*models.SessionSummary{{SessionID: "s", TenantID: "t", Summary: "x"}}); err != nil {
		t.Fatal(err)
	}
	if m.HistoryCount() != 1 || m.TraceCount() != 1 || m.MemoryCount() != 1 || m.SummaryCount() != 1 {
		t.Error("counts wrong")
	}
	_ = m.Flush()
	if m.Flushes != 1 {
		t.Error("flush not recorded")
	}
	_ = m.Close()
	if !m.Closed {
		t.Error("close not recorded")
	}

	m.Err = errors.New("x")
	if err := m.InsertHistory(ctx, []*models.HistoryMessage{{}}); err == nil {
		t.Error("expected error")
	}
	if err := m.InsertTraces(ctx, []*models.TraceMessage{{}}); err == nil {
		t.Error("expected error")
	}
	if err := m.InsertMemory(ctx, []*models.EntityMemory{{}}); err == nil {
		t.Error("expected error")
	}
	if err := m.InsertSessionSummary(ctx, []*models.SessionSummary{{}}); err == nil {
		t.Error("expected error")
	}
}
