package filewatcher

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemosyne-ai-systems/mneme/pkg/models"
)

func parseFrameworkFile(t *testing.T, name, content string) []*models.HistoryMessage {
	t.Helper()
	dir := t.TempDir()
	path := writeFile(t, dir, name, content)
	msgs, err := NewRegistry().Parse(path, "tenant-fw")
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return msgs
}

func frameworkName(t *testing.T, m *models.HistoryMessage) string {
	t.Helper()
	var meta map[string]interface{}
	if err := json.Unmarshal([]byte(m.Metadata), &meta); err != nil {
		t.Fatalf("metadata %q: %v", m.Metadata, err)
	}
	name, _ := meta["framework"].(string)
	return name
}

func TestClaudeCodeAdapter(t *testing.T) {
	msgs := parseFrameworkFile(t, "transcript.jsonl", strings.Join([]string{
		`{"type":"summary","summary":"meta","leafUuid":"x"}`,
		`{"type":"user","uuid":"u1","sessionId":"sess-cc","timestamp":"2025-03-01T10:00:00Z","cwd":"/work","message":{"role":"user","content":"please refactor the parser"}}`,
		`{"type":"assistant","uuid":"a1","sessionId":"sess-cc","timestamp":"2025-03-01T10:00:05Z","message":{"id":"msg_1","role":"assistant","model":"claude-sonnet-4","content":[{"type":"thinking","thinking":"hmm"},{"type":"text","text":"I will update it."},{"type":"tool_use","id":"t1","name":"Edit","input":{}}],"usage":{"input_tokens":12,"output_tokens":7}}}`,
		`{"type":"user","uuid":"u2","sessionId":"sess-cc","timestamp":"2025-03-01T10:00:06Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":[{"type":"text","text":"file updated"}]}]}}`,
		`{"type":"system","subtype":"hook","content":"noise","sessionId":"sess-cc"}`,
		`not-json-line`,
	}, "\n"))

	if len(msgs) != 3 {
		t.Fatalf("messages = %d: %+v", len(msgs), msgs)
	}
	if msgs[0].MessageID != "u1" || msgs[0].Role != models.RoleUser ||
		msgs[0].Content != "please refactor the parser" || msgs[0].SessionID != "sess-cc" {
		t.Errorf("msg0 = %+v", msgs[0])
	}
	if msgs[1].MessageID != "a1" || msgs[1].Role != models.RoleAssistant ||
		msgs[1].Content != "I will update it." || msgs[1].ModelName != "claude-sonnet-4" {
		t.Errorf("msg1 = %+v", msgs[1])
	}
	if msgs[1].PromptTokens != 12 || msgs[1].CompletionTokens != 7 {
		t.Errorf("usage = %d/%d", msgs[1].PromptTokens, msgs[1].CompletionTokens)
	}
	if msgs[2].Role != models.RoleTool || msgs[2].Content != "file updated" {
		t.Errorf("msg2 = %+v", msgs[2])
	}
	if got := frameworkName(t, msgs[0]); got != "claude-code" {
		t.Errorf("framework = %q", got)
	}
	if msgs[0].TenantID != "tenant-fw" {
		t.Errorf("tenant = %q", msgs[0].TenantID)
	}
}

func TestCodexAdapter(t *testing.T) {
	msgs := parseFrameworkFile(t, "rollout.jsonl", strings.Join([]string{
		`{"timestamp":"2025-03-02T09:00:00Z","type":"session_meta","payload":{"id":"sess-cx","cwd":"/repo","originator":"codex_cli_rs"}}`,
		`{"timestamp":"2025-03-02T09:00:01Z","type":"turn_context","payload":{"model":"gpt-5-codex"}}`,
		`{"timestamp":"2025-03-02T09:00:02Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"add tests"}]}}`,
		`{"timestamp":"2025-03-02T09:00:02Z","type":"event_msg","payload":{"type":"user_message","message":"add tests"}}`,
		`{"timestamp":"2025-03-02T09:00:03Z","type":"response_item","payload":{"type":"function_call","name":"shell","arguments":"{}"}}`,
		`{"timestamp":"2025-03-02T09:00:04Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Done."}]}}`,
		`{"timestamp":"2025-03-02T09:00:04Z","type":"event_msg","payload":{"type":"agent_message","message":"Done."}}`,
		`{"timestamp":"2025-03-02T09:00:05Z","type":"event_msg","payload":{"type":"user_message","message":"thanks"}}`,
		`{"timestamp":"2025-03-02T09:00:06Z","type":"event_msg","payload":{"type":"token_count","info":{"total":100}}}`,
	}, "\n"))

	if len(msgs) != 3 {
		t.Fatalf("messages = %d: %+v", len(msgs), msgs)
	}
	if msgs[0].Role != models.RoleUser || msgs[0].Content != "add tests" || msgs[0].SessionID != "sess-cx" {
		t.Errorf("msg0 = %+v", msgs[0])
	}
	if msgs[1].Role != models.RoleAssistant || msgs[1].Content != "Done." || msgs[1].ModelName != "gpt-5-codex" {
		t.Errorf("msg1 = %+v", msgs[1])
	}
	if msgs[2].Role != models.RoleUser || msgs[2].Content != "thanks" {
		t.Errorf("msg2 = %+v", msgs[2])
	}
	if got := frameworkName(t, msgs[0]); got != "codex-cli" {
		t.Errorf("framework = %q", got)
	}
	if msgs[0].MessageID == msgs[1].MessageID {
		t.Error("message ids should differ")
	}

	// 旧版仅含 event_msg 的 rollout 仍需按原顺序保留
	msgs = parseFrameworkFile(t, "legacy.jsonl", strings.Join([]string{
		`{"timestamp":"2025-03-02T10:00:00Z","type":"session_meta","payload":{"id":"sess-legacy"}}`,
		`{"timestamp":"2025-03-02T10:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"one"}}`,
		`{"timestamp":"2025-03-02T10:00:02Z","type":"event_msg","payload":{"type":"agent_message","message":"two"}}`,
	}, "\n"))
	if len(msgs) != 2 || msgs[0].Content != "one" || msgs[1].Content != "two" || msgs[0].SessionID != "sess-legacy" {
		t.Errorf("legacy = %+v", msgs)
	}
}

func TestGeminiAdapter(t *testing.T) {
	msgs := parseFrameworkFile(t, "session-1.json", `{
  "sessionId": "sess-gm",
  "projectHash": "abc123",
  "startTime": "2025-03-03T08:00:00Z",
  "messages": [
    {"id":"g1","timestamp":"2025-03-03T08:00:01Z","type":"user","content":"explain this repo"},
    {"id":"g2","timestamp":"2025-03-03T08:00:02Z","type":"gemini","content":"It is a memory service.","model":"gemini-2.5-pro","tokens":{"input":10,"output":20,"total":30}},
    {"id":"g3","timestamp":"2025-03-03T08:00:03Z","type":"info","content":"tip"}
  ]
}`)

	if len(msgs) != 2 {
		t.Fatalf("messages = %d: %+v", len(msgs), msgs)
	}
	if msgs[0].Role != models.RoleUser || msgs[0].Content != "explain this repo" || msgs[0].SessionID != "sess-gm" {
		t.Errorf("msg0 = %+v", msgs[0])
	}
	if msgs[1].Role != models.RoleAssistant || msgs[1].ModelName != "gemini-2.5-pro" {
		t.Errorf("msg1 = %+v", msgs[1])
	}
	if msgs[1].PromptTokens != 10 || msgs[1].CompletionTokens != 20 || msgs[1].TotalTokens != 30 {
		t.Errorf("tokens = %d/%d/%d", msgs[1].PromptTokens, msgs[1].CompletionTokens, msgs[1].TotalTokens)
	}
	if got := frameworkName(t, msgs[1]); got != "gemini-cli" {
		t.Errorf("framework = %q", got)
	}

	// 顶层数组形式（无 sessionId）
	msgs = parseFrameworkFile(t, "session-2.json", `[
	  {"id":"g4","timestamp":"2025-03-03T09:00:00Z","type":"user","content":"hi"},
	  {"id":"g5","timestamp":"2025-03-03T09:00:01Z","type":"gemini","content":"hello"}
	]`)
	if len(msgs) != 2 || msgs[0].SessionID != "session-2" {
		t.Errorf("array form = %+v", msgs)
	}
}

func TestClaudeExportAdapter(t *testing.T) {
	msgs := parseFrameworkFile(t, "claude-conversations.json", `[
  {"uuid":"conv-1","name":"Planning","chat_messages":[
    {"uuid":"m1","sender":"human","text":"hi","created_at":"2025-03-04T07:00:00Z"},
    {"uuid":"m2","sender":"assistant","text":"","content":[{"type":"text","text":"hello"}],"created_at":"2025-03-04T07:00:01Z"}
  ]}
]`)

	if len(msgs) != 2 {
		t.Fatalf("messages = %d: %+v", len(msgs), msgs)
	}
	if msgs[0].Role != models.RoleUser || msgs[0].Content != "hi" || msgs[0].SessionID != "conv-1" {
		t.Errorf("msg0 = %+v", msgs[0])
	}
	if msgs[1].Role != models.RoleAssistant || msgs[1].Content != "hello" {
		t.Errorf("msg1 = %+v", msgs[1])
	}
	if got := frameworkName(t, msgs[1]); got != "claude-export" {
		t.Errorf("framework = %q", got)
	}

	// {"conversations": [...]} 包装
	msgs = parseFrameworkFile(t, "wrapped.json", `{"conversations":[
	  {"uuid":"conv-2","name":"Wrapped","chat_messages":[{"uuid":"m3","sender":"human","text":"wrapped"}]}
	]}`)
	if len(msgs) != 1 || msgs[0].Content != "wrapped" || msgs[0].SessionID != "conv-2" {
		t.Errorf("wrapper = %+v", msgs)
	}
}

func TestChatGPTExportAdapter(t *testing.T) {
	const conversation = `[
  {"id":"conv-gpt","title":"Refactor","create_time":1741000000,"current_node":"n3","mapping":{
    "root":{"id":"root","parent":null,"children":["n1"],"message":null},
    "n1":{"id":"n1","parent":"root","children":["n2","alt"],"message":{"id":"m1","author":{"role":"user"},"create_time":1741000001,"content":{"content_type":"text","parts":["please refactor"]}}},
    "n2":{"id":"n2","parent":"n1","children":["n3"],"message":{"id":"m2","author":{"role":"assistant"},"create_time":1741000002,"content":{"content_type":"text","parts":["done"]}}},
    "alt":{"id":"alt","parent":"n1","children":[],"message":{"id":"m-alt","author":{"role":"assistant"},"create_time":1741000005,"content":{"content_type":"text","parts":["alternative"]}}},
    "n3":{"id":"n3","parent":"n2","children":[],"message":{"id":"m3","author":{"role":"user"},"create_time":1741000003,"content":{"content_type":"text","parts":["thanks"]}}}
  }}
]`

	msgs := parseFrameworkFile(t, "chatgpt-conversations.json", conversation)
	if len(msgs) != 3 {
		t.Fatalf("messages = %d: %+v", len(msgs), msgs)
	}
	if msgs[0].MessageID != "m1" || msgs[0].Role != models.RoleUser || msgs[0].Content != "please refactor" {
		t.Errorf("msg0 = %+v", msgs[0])
	}
	if msgs[1].MessageID != "m2" || msgs[1].Content != "done" {
		t.Errorf("msg1 = %+v", msgs[1])
	}
	if msgs[2].MessageID != "m3" || msgs[2].Content != "thanks" || msgs[2].SessionID != "conv-gpt" {
		t.Errorf("msg2 = %+v", msgs[2])
	}
	for _, m := range msgs {
		if m.Content == "alternative" {
			t.Error("branch outside current_node should be skipped")
		}
	}
	if got := frameworkName(t, msgs[0]); got != "chatgpt-export" {
		t.Errorf("framework = %q", got)
	}

	// 无 current_node 时 DFS 覆盖所有分支
	msgs = parseFrameworkFile(t, "chatgpt-branches.json", strings.Replace(conversation, `"current_node":"n3",`, "", 1))
	if len(msgs) != 4 {
		t.Fatalf("branch messages = %d: %+v", len(msgs), msgs)
	}
	if msgs[len(msgs)-1].Content != "alternative" {
		t.Errorf("last message = %+v", msgs[len(msgs)-1])
	}
}

func TestQwenCodeAdapter(t *testing.T) {
	msgs := parseFrameworkFile(t, "sess-qw.jsonl", strings.Join([]string{
		`{"uuid":"q0","parentUuid":null,"sessionId":"sess-qw","timestamp":"2026-01-02T03:04:00Z","type":"system","subtype":"session_model","systemPayload":{"modelId":"qwen3-coder"}}`,
		`{"uuid":"q1","parentUuid":"q0","sessionId":"sess-qw","timestamp":"2026-01-02T03:04:01.500Z","type":"user","cwd":"/repo","message":{"role":"user","parts":[{"text":"how do I build this"}]}}`,
		`{"uuid":"q2","parentUuid":"q1","sessionId":"sess-qw","timestamp":"2026-01-02T03:04:02Z","type":"assistant","model":"qwen3-coder","message":{"role":"model","parts":[{"text":"internal reasoning","thought":true},{"text":"run make build"},{"functionCall":{"name":"shell","args":{"command":"make build"}}}]},"usageMetadata":{"promptTokenCount":21,"candidatesTokenCount":8,"totalTokenCount":29}}`,
		`{"uuid":"q3","parentUuid":"q2","sessionId":"sess-qw","timestamp":"2026-01-02T03:04:03Z","type":"tool_result","message":{"role":"user","parts":[{"functionResponse":{"name":"shell","response":{"output":"ok"}}}]}}`,
	}, "\n"))

	if len(msgs) != 2 {
		t.Fatalf("messages = %d: %+v", len(msgs), msgs)
	}
	if msgs[0].MessageID != "q1" || msgs[0].Role != models.RoleUser ||
		msgs[0].Content != "how do I build this" || msgs[0].SessionID != "sess-qw" {
		t.Errorf("msg0 = %+v", msgs[0])
	}
	if msgs[1].MessageID != "q2" || msgs[1].Role != models.RoleAssistant ||
		msgs[1].Content != "run make build" || msgs[1].ModelName != "qwen3-coder" {
		t.Errorf("msg1 = %+v", msgs[1])
	}
	if msgs[1].PromptTokens != 21 || msgs[1].CompletionTokens != 8 || msgs[1].TotalTokens != 29 {
		t.Errorf("usage = %d/%d/%d", msgs[1].PromptTokens, msgs[1].CompletionTokens, msgs[1].TotalTokens)
	}
	if got := frameworkName(t, msgs[0]); got != "qwen-code" {
		t.Errorf("framework = %q", got)
	}

	// 旧版整文件 JSON（role user / model + parts）
	msgs = parseFrameworkFile(t, "legacy-qwen.json", `{
  "sessionId": "sess-qw-old",
  "projectHash": "proj-1",
  "startTime": "2026-01-03T08:00:00Z",
  "lastUpdated": "2026-01-03T08:01:00Z",
  "messages": [
    {"role":"user","parts":[{"text":"legacy question"}]},
    {"role":"model","parts":[{"text":"legacy answer"}]}
  ]
}`)
	if len(msgs) != 2 || msgs[0].SessionID != "sess-qw-old" || msgs[1].Role != models.RoleAssistant {
		t.Fatalf("legacy = %+v", msgs)
	}
	if got := frameworkName(t, msgs[1]); got != "qwen-code" {
		t.Errorf("legacy framework = %q", got)
	}
}

func TestKimiAdapter(t *testing.T) {
	dir := t.TempDir()
	sessionDir := filepath.Join(dir, "sess-kimi")
	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := writeFile(t, sessionDir, "context.jsonl", strings.Join([]string{
		`{"role":"_system_prompt","content":"You are Kimi CLI."}`,
		`{"role":"_checkpoint","id":0}`,
		`{"role":"user","content":"hello kimi"}`,
		`{"role":"assistant","content":[{"type":"think","think":"hidden"},{"type":"text","text":"hi there"}],"tool_calls":[{"type":"function","id":"c1","function":{"name":"shell","arguments":"{}"}}]}`,
		`{"role":"_usage","token_count":64}`,
		`{"role":"tool","content":[{"type":"text","text":"command output"}],"tool_call_id":"c1"}`,
	}, "\n"))

	msgs, err := NewRegistry().Parse(path, "tenant-fw")
	if err != nil {
		t.Fatalf("parse kimi: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("messages = %d: %+v", len(msgs), msgs)
	}
	if msgs[0].Role != models.RoleUser || msgs[0].Content != "hello kimi" || msgs[0].SessionID != "sess-kimi" {
		t.Errorf("msg0 = %+v", msgs[0])
	}
	if msgs[1].Role != models.RoleAssistant || msgs[1].Content != "hi there" {
		t.Errorf("msg1 = %+v", msgs[1])
	}
	if msgs[2].Role != models.RoleTool || msgs[2].Content != "command output" {
		t.Errorf("msg2 = %+v", msgs[2])
	}
	if got := frameworkName(t, msgs[0]); got != "kimi-cli" {
		t.Errorf("framework = %q", got)
	}
}

func TestFrameworkFallbackToGenericJSON(t *testing.T) {
	msgs := parseFrameworkFile(t, "plain.jsonl", strings.Join([]string{
		`{"role":"user","content":"plain jsonl"}`,
		`{"role":"assistant","content":"reply"}`,
	}, "\n"))
	if len(msgs) != 2 || msgs[0].Content != "plain jsonl" || msgs[1].Role != models.RoleAssistant {
		t.Errorf("jsonl fallback = %+v", msgs)
	}
	if msgs[0].Metadata != "" {
		t.Errorf("generic parser should not set framework metadata: %q", msgs[0].Metadata)
	}

	msgs = parseFrameworkFile(t, "records.json", `[{"role":"user","content":"a"},{"role":"assistant","content":"b"}]`)
	if len(msgs) != 2 || msgs[0].Content != "a" || msgs[1].Content != "b" {
		t.Errorf("json fallback = %+v", msgs)
	}

	msgs = parseFrameworkFile(t, "wrapped.json", `{"messages":[{"session_id":"s1","role":"user","content":"wrapped"}]}`)
	if len(msgs) != 1 || msgs[0].SessionID != "s1" || msgs[0].Content != "wrapped" {
		t.Errorf("wrapped json = %+v", msgs)
	}
}
