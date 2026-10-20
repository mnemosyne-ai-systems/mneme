package filewatcher

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mnemosyne-ai-systems/mneme/internal/service"
	"github.com/mnemosyne-ai-systems/mneme/pkg/messagebus"
	"github.com/mnemosyne-ai-systems/mneme/pkg/models"
)

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestMarkdownParser(t *testing.T) {
	dir := t.TempDir()
	p := writeFile(t, dir, "test.md", `---
session_id: sess-1
user_id: u1
role: assistant
model_name: gpt-4
total_tokens: 42
created_at: 2025-01-02T03:04:05Z
metadata:
  source: notes
---
# Hello

This is the body.`)

	msgs, err := (&MarkdownParser{}).Parse(p, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("messages = %d", len(msgs))
	}
	m := msgs[0]
	if m.SessionID != "sess-1" || m.Role != models.RoleAssistant || m.TenantID != "tenant-a" {
		t.Errorf("message = %+v", m)
	}
	if m.ModelName != "gpt-4" || m.TotalTokens != 42 {
		t.Errorf("meta = %+v", m)
	}
	if m.CreatedAt.Year() != 2025 {
		t.Errorf("created_at = %v", m.CreatedAt)
	}
	var meta map[string]string
	_ = json.Unmarshal([]byte(m.Metadata), &meta)
	if meta["source"] != "notes" {
		t.Errorf("metadata = %s", m.Metadata)
	}
	if m.Content != "# Hello\n\nThis is the body." {
		t.Errorf("content = %q", m.Content)
	}
}

func TestMarkdownParserNoFrontmatter(t *testing.T) {
	dir := t.TempDir()
	p := writeFile(t, dir, "plain.md", "just some text")
	msgs, err := (&MarkdownParser{}).Parse(p, "t")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].SessionID != "plain" || msgs[0].Role != models.RoleUser {
		t.Errorf("msgs = %+v", msgs)
	}

	empty := writeFile(t, dir, "empty.md", "   \n  ")
	msgs, err = (&MarkdownParser{}).Parse(empty, "t")
	if err != nil || len(msgs) != 0 {
		t.Errorf("empty file: %v %v", msgs, err)
	}

	bad := writeFile(t, dir, "bad.md", "---\n: : :\nnot yaml\n---\nbody")
	if _, err := (&MarkdownParser{}).Parse(bad, "t"); err == nil {
		t.Error("expected frontmatter parse error")
	}
}

func TestJSONLParser(t *testing.T) {
	dir := t.TempDir()
	p := writeFile(t, dir, "conv.jsonl", `{"message_id":"m1","session_id":"s1","role":"user","content":"hi","model":"gpt-4"}
{"id":"m2","conversation_id":"s1","role":"assistant","text":"hello","total_tokens":10}

{"role":"user","content":"third"}
`)
	msgs, err := (&JSONLParser{}).Parse(p, "tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 3 {
		t.Fatalf("messages = %d", len(msgs))
	}
	if msgs[0].MessageID != "m1" || msgs[0].ModelName != "gpt-4" {
		t.Errorf("msg0 = %+v", msgs[0])
	}
	if msgs[1].MessageID != "m2" || msgs[1].Content != "hello" || msgs[1].SessionID != "s1" {
		t.Errorf("msg1 = %+v", msgs[1])
	}
	if msgs[2].SessionID != "conv" {
		t.Errorf("msg2 session = %s", msgs[2].SessionID)
	}
	for _, m := range msgs {
		if m.TenantID != "tenant-b" {
			t.Errorf("tenant = %s", m.TenantID)
		}
	}

	bad := writeFile(t, dir, "bad.jsonl", "{not json}")
	if _, err := (&JSONLParser{}).Parse(bad, "t"); err == nil {
		t.Error("expected parse error")
	}
}

func TestTextParser(t *testing.T) {
	dir := t.TempDir()
	p := writeFile(t, dir, "chat.txt", "user: hello\nassistant: hi there\nplain line\n\n")
	msgs, err := (&TextParser{}).Parse(p, "t")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 3 {
		t.Fatalf("messages = %d", len(msgs))
	}
	if msgs[0].Role != models.RoleUser || msgs[0].Content != "hello" {
		t.Errorf("msg0 = %+v", msgs[0])
	}
	if msgs[1].Role != models.RoleAssistant || msgs[1].Content != "hi there" {
		t.Errorf("msg1 = %+v", msgs[1])
	}
	if msgs[2].Content != "plain line" {
		t.Errorf("msg2 = %+v", msgs[2])
	}

	empty := writeFile(t, dir, "empty.txt", "")
	msgs, err = (&TextParser{}).Parse(empty, "t")
	if err != nil || len(msgs) != 0 {
		t.Errorf("empty: %v %v", msgs, err)
	}
}

func TestRegistryDispatch(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry()
	p := writeFile(t, dir, "a.md", "content")
	msgs, err := r.Parse(p, "t")
	if err != nil || len(msgs) != 1 {
		t.Fatalf("parse: %v %v", msgs, err)
	}
	unsupported := writeFile(t, dir, "a.pdf", "x")
	if _, err := r.Parse(unsupported, "t"); err == nil {
		t.Error("expected unsupported error")
	}
	if _, ok := r.Get(".JSONL"); !ok {
		t.Error("case-insensitive lookup failed")
	}
}

func TestState(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	p := writeFile(t, dir, "a.md", "one")

	s, err := LoadState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	should, mark, err := s.ShouldProcess(p)
	if err != nil || !should {
		t.Fatalf("should process: %v %v", should, err)
	}
	s.Commit(p, mark)
	if err := s.Save(); err != nil {
		t.Fatal(err)
	}

	// 内容未变，不重复处理
	s2, err := LoadState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	should, _, _ = s2.ShouldProcess(p)
	if should {
		t.Error("unchanged file should be skipped")
	}
	if s2.Len() != 1 {
		t.Errorf("len = %d", s2.Len())
	}

	// 内容改变，重新处理
	writeFile(t, dir, "a.md", "two")
	should, _, _ = s2.ShouldProcess(p)
	if !should {
		t.Error("changed file should be processed")
	}

	// 不存在的文件
	if _, _, err := s2.ShouldProcess(filepath.Join(dir, "nope.md")); err == nil {
		t.Error("expected stat error")
	}

	// 空路径状态
	s3, err := LoadState("")
	if err != nil || s3.Save() != nil {
		t.Error("empty state path should be a no-op")
	}
}

func newTestWatcher(t *testing.T, dir string) (*Watcher, *messagebus.MemoryBus) {
	t.Helper()
	bus := messagebus.NewMemoryBus()
	ingest := service.NewIngestService(bus, service.Topics{History: "h"})
	w, err := New(Config{
		Dirs:      []string{dir},
		Debounce:  20 * time.Millisecond,
		StateFile: filepath.Join(dir, ".state.json"),
		TenantID:  "tenant-x",
	}, ingest)
	if err != nil {
		t.Fatal(err)
	}
	return w, bus
}

func TestWatcherProcessFileAndDedup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	w, bus := newTestWatcher(t, dir)
	defer func() { _ = w.Stop() }()

	p := writeFile(t, dir, "note.md", "---\nsession_id: s1\n---\nhello world")
	if err := w.ProcessFile(ctx, p); err != nil {
		t.Fatal(err)
	}
	if len(bus.Messages("h")) != 1 {
		t.Fatalf("published = %d", len(bus.Messages("h")))
	}

	// 内容未变，不重复发送
	if err := w.ProcessFile(ctx, p); err != nil {
		t.Fatal(err)
	}
	if len(bus.Messages("h")) != 1 {
		t.Errorf("duplicate processing, published = %d", len(bus.Messages("h")))
	}

	// 内容变化后重新发送
	writeFile(t, dir, "note.md", "---\nsession_id: s1\n---\nupdated content")
	if err := w.ProcessFile(ctx, p); err != nil {
		t.Fatal(err)
	}
	if len(bus.Messages("h")) != 2 {
		t.Errorf("changed file not republished, published = %d", len(bus.Messages("h")))
	}
}

func TestWatcherLiveEvents(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	w, bus := newTestWatcher(t, dir)
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Stop() }()

	writeFile(t, dir, "test.md", "---\nsession_id: live\n---\nauto detected")

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(bus.Messages("h")) > 0 {
			var out models.HistoryMessage
			_ = json.Unmarshal(bus.Messages("h")[0].Payload, &out)
			if out.SessionID != "live" || out.TenantID != "tenant-x" {
				t.Errorf("message = %+v", out)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timeout waiting for file event")
}

func TestWatcherScanAllAndSupported(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	writeFile(t, dir, "a.md", "aaa")
	writeFile(t, dir, "b.jsonl", `{"role":"user","content":"bbb"}`)
	writeFile(t, dir, "c.txt", "ccc")
	writeFile(t, dir, "ignore.pdf", "xxx")
	writeFile(t, dir, ".hidden.md", "hidden")

	w, bus := newTestWatcher(t, dir)
	defer func() { _ = w.Stop() }()

	if err := w.ScanAll(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(bus.Messages("h")); n != 3 {
		t.Errorf("published = %d, want 3", n)
	}

	if w.supported(filepath.Join(dir, "x.tmp")) || w.supported(filepath.Join(dir, ".x.md")) {
		t.Error("temp/hidden files should be unsupported")
	}
	if !w.supported(filepath.Join(dir, "x.MD")) {
		t.Error("extension match should be case-insensitive")
	}
}

func TestConfigDefaults(t *testing.T) {
	c := Config{}.withDefaults()
	if len(c.Extensions) != 3 || c.Debounce != 2*time.Second || c.TenantID != "default" {
		t.Errorf("defaults = %+v", c)
	}
}

func TestHelpers(t *testing.T) {
	if firstNonEmpty("", "", "x") != "x" || firstNonEmpty() != "" {
		t.Error("firstNonEmpty wrong")
	}
	if deriveID("a", "b") == deriveID("a", "c") {
		t.Error("deriveID should differ")
	}
	if deriveID("a", "b") != deriveID("a", "b") {
		t.Error("deriveID should be stable")
	}
	if deriveSessionID("/tmp/foo/bar.md") != "bar" {
		t.Error("deriveSessionID wrong")
	}
	if got := parseTimeOrFileMtime("2025-01-02", "/nonexistent"); got.Year() != 2025 {
		t.Errorf("parseTime = %v", got)
	}
	if got := parseTimeOrFileMtime("", "/nonexistent"); got.IsZero() {
		t.Error("should fall back to now")
	}
}

func TestSplitFrontmatter(t *testing.T) {
	fm, body := splitFrontmatter("---\nkey: v\n---\nbody text")
	if fm != "key: v" || body != "body text" {
		t.Errorf("fm=%q body=%q", fm, body)
	}
	fm, body = splitFrontmatter("no frontmatter")
	if fm != "" || body != "no frontmatter" {
		t.Errorf("fm=%q body=%q", fm, body)
	}
	fm, body = splitFrontmatter("---\nunterminated")
	if fm != "" || body == "" {
		t.Errorf("fm=%q body=%q", fm, body)
	}
}
