package filewatcher

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/mnemosyne-ai-systems/mneme/pkg/models"
)

const frameworkCodex = "codex-cli"

// codexAdapter 解析 Codex CLI 的 rollout 记录（~/.codex/sessions/**/rollout-*.jsonl）。
type codexAdapter struct{}

func (a *codexAdapter) Name() string { return frameworkCodex }

func (a *codexAdapter) Match(ext string, head []byte) bool {
	if ext != ".jsonl" {
		return false
	}
	c := compactJSON(head)
	if !strings.Contains(c, `"payload":`) {
		return false
	}
	return matchesAny(c, `"session_meta"`, `"response_item"`, `"event_msg"`, `"turn_context"`)
}

type codexRecord struct {
	Timestamp string          `json:"timestamp"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
}

type codexPayload struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Role       string          `json:"role"`
	Model      string          `json:"model"`
	Message    string          `json:"message"`
	Content    json.RawMessage `json:"content"`
	Cwd        string          `json:"cwd"`
	Originator string          `json:"originator"`
}

func (a *codexAdapter) Parse(path, tenantID string) ([]*models.HistoryMessage, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("filewatcher: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	sessionID := deriveSessionID(path)
	var cwd, originator, model string
	var out []*models.HistoryMessage
	type pendingMessage struct {
		role string
		text string
		msg  *models.HistoryMessage
	}
	var events []pendingMessage
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var rec codexRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			continue
		}
		if len(rec.Payload) == 0 {
			continue
		}
		var payload codexPayload
		if err := json.Unmarshal(rec.Payload, &payload); err != nil {
			continue
		}

		switch rec.Type {
		case "session_meta":
			sessionID = firstNonEmpty(payload.ID, sessionID)
			cwd = firstNonEmpty(payload.Cwd, cwd)
			originator = firstNonEmpty(payload.Originator, originator)
		case "turn_context":
			model = firstNonEmpty(payload.Model, model)
		case "response_item":
			if payload.Type != "message" {
				continue
			}
			role := normalizeRole(payload.Role)
			text, _ := contentText(payload.Content)
			if role == "" || text == "" {
				continue
			}
			out = append(out, codexMessage(path, tenantID, sessionID, role, text, model, cwd, originator, rec.Timestamp, lineNo))
		case "event_msg":
			role := ""
			switch payload.Type {
			case "user_message":
				role = models.RoleUser
			case "agent_message":
				role = models.RoleAssistant
			default:
				continue
			}
			text := strings.TrimSpace(payload.Message)
			if text == "" {
				text, _ = contentText(payload.Content)
			}
			if text == "" {
				continue
			}
			events = append(events, pendingMessage{
				role: role,
				text: text,
				msg:  codexMessage(path, tenantID, sessionID, role, text, model, cwd, originator, rec.Timestamp, lineNo),
			})
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("filewatcher: scan %s: %w", path, err)
	}

	// 较新的 rollout 同时记录 response_item 与 event_msg，后者是前者的 UI 事件副本。
	// 若 event_msg 内容已由 response_item 提供则丢弃；只有 event_msg 的旧文件保留原顺序。
	seen := make(map[string]bool, len(out))
	for _, msg := range out {
		seen[msg.Role+"\x00"+msg.Content] = true
	}
	for _, e := range events {
		if !seen[e.role+"\x00"+e.text] {
			out = append(out, e.msg)
		}
	}
	return validateFrameworkMessages(path, out)
}

func codexMessage(path, tenantID, sessionID, role, text, model, cwd, originator, timestamp string, lineNo int) *models.HistoryMessage {
	msg := &models.HistoryMessage{
		MessageID: deriveID(path, strconv.Itoa(lineNo)+role+text),
		TenantID:  tenantID,
		SessionID: sessionID,
		Role:      role,
		Content:   text,
		ModelName: model,
		CreatedAt: parseTimeOrFileMtime(timestamp, path),
	}
	setFrameworkMetadata(msg, frameworkCodex, map[string]interface{}{
		"cwd":        cwd,
		"originator": originator,
	})
	return msg
}
