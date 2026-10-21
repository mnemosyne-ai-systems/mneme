package filewatcher

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/mnemosyne-ai-systems/mneme/pkg/models"
)

const frameworkQwenCode = "qwen-code"

// qwenCodeAdapter 解析 Qwen Code 的会话记录。
// 新版为 ~/.qwen/tmp/<project_id>/chats/<sessionId>.jsonl（uuid/parentUuid 树），
// 旧版为 <sessionId>.json（sessionId/projectHash/messages）。
type qwenCodeAdapter struct{}

func (a *qwenCodeAdapter) Name() string { return frameworkQwenCode }

func (a *qwenCodeAdapter) Match(ext string, head []byte) bool {
	c := compactJSON(head)
	switch ext {
	case ".jsonl":
		// Claude Code 同样使用 parentUuid，靠 message.parts 区分（Claude 用 message.content）。
		return strings.Contains(c, `"parentUuid":`) &&
			matchesAny(c, `"message":{"role":"user","parts":[`, `"message":{"role":"model","parts":[`)
	case ".json":
		return strings.Contains(c, `"projectHash":`) &&
			strings.Contains(c, `"messages":[`) &&
			strings.Contains(c, `"parts":[`)
	default:
		return false
	}
}

type qwenRecord struct {
	UUID          string          `json:"uuid"`
	SessionID     string          `json:"sessionId"`
	Timestamp     string          `json:"timestamp"`
	Type          string          `json:"type"`
	Subtype       string          `json:"subtype"`
	Cwd           string          `json:"cwd"`
	Model         string          `json:"model"`
	IsSidechain   bool            `json:"isSidechain"`
	Message       *qwenContent    `json:"message"`
	UsageMetadata *qwenUsageStats `json:"usageMetadata"`
}

type qwenContent struct {
	ID      string          `json:"id"`
	Role    string          `json:"role"`
	Parts   []qwenPart      `json:"parts"`
	Content json.RawMessage `json:"content"`
}

type qwenPart struct {
	Text    string `json:"text"`
	Thought bool   `json:"thought"`
}

type qwenUsageStats struct {
	PromptTokens     int32 `json:"promptTokenCount"`
	CandidatesTokens int32 `json:"candidatesTokenCount"`
	TotalTokens      int32 `json:"totalTokenCount"`
}

type qwenLegacySession struct {
	SessionID   string        `json:"sessionId"`
	ProjectHash string        `json:"projectHash"`
	StartTime   string        `json:"startTime"`
	LastUpdated string        `json:"lastUpdated"`
	Messages    []qwenContent `json:"messages"`
}

func (a *qwenCodeAdapter) Parse(path, tenantID string) ([]*models.HistoryMessage, error) {
	if strings.EqualFold(filepath.Ext(path), ".json") {
		return a.parseSession(path, tenantID)
	}
	return a.parseTranscript(path, tenantID)
}

// parseTranscript 解析新版 JSONL：每条 user / assistant / tool_result 记录携带完整的 Content。
func (a *qwenCodeAdapter) parseTranscript(path, tenantID string) ([]*models.HistoryMessage, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("filewatcher: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	sessionFallback := deriveSessionID(path)
	var out []*models.HistoryMessage
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var rec qwenRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			// 会话文件可能正在追加，跳过不完整的行。
			continue
		}
		if rec.Message == nil || rec.Subtype != "" {
			continue
		}
		switch rec.Type {
		case "user", "assistant", "tool_result":
		default:
			continue
		}
		text := qwenPartsText(rec.Message.Parts)
		if text == "" {
			continue
		}
		role := normalizeRole(rec.Message.Role)
		if role == "" {
			role = normalizeRole(rec.Type)
		}
		if role == "" {
			continue
		}
		if rec.Type == "tool_result" {
			role = models.RoleTool
		}

		msg := &models.HistoryMessage{
			MessageID: firstNonEmpty(rec.UUID, rec.Message.ID, deriveID(path, strconv.Itoa(lineNo)+text)),
			TenantID:  tenantID,
			SessionID: firstNonEmpty(rec.SessionID, sessionFallback),
			Role:      role,
			Content:   text,
			ModelName: rec.Model,
			CreatedAt: frameworkTime(rec.Timestamp, 0, path),
		}
		if rec.UsageMetadata != nil {
			msg.PromptTokens = rec.UsageMetadata.PromptTokens
			msg.CompletionTokens = rec.UsageMetadata.CandidatesTokens
			msg.TotalTokens = rec.UsageMetadata.TotalTokens
		}
		setFrameworkMetadata(msg, frameworkQwenCode, map[string]interface{}{
			"cwd":       rec.Cwd,
			"sidechain": rec.IsSidechain,
		})
		out = append(out, msg)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("filewatcher: scan %s: %w", path, err)
	}
	return validateFrameworkMessages(path, out)
}

// parseSession 解析旧版整文件 JSON。
func (a *qwenCodeAdapter) parseSession(path, tenantID string) ([]*models.HistoryMessage, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("filewatcher: read %s: %w", path, err)
	}
	raw = bytes.TrimPrefix(raw, []byte("\xef\xbb\xbf"))

	var session qwenLegacySession
	if err := json.Unmarshal(raw, &session); err != nil {
		return nil, fmt.Errorf("filewatcher: parse %s: %w", path, err)
	}
	sessionID := firstNonEmpty(session.SessionID, deriveSessionID(path))

	var out []*models.HistoryMessage
	for i, m := range session.Messages {
		role := normalizeRole(m.Role)
		if role == "" {
			continue
		}
		text := qwenPartsText(m.Parts)
		if text == "" {
			text, _ = contentText(m.Content)
		}
		if text == "" {
			continue
		}

		msg := &models.HistoryMessage{
			MessageID: firstNonEmpty(m.ID, deriveID(path, strconv.Itoa(i)+role+text)),
			TenantID:  tenantID,
			SessionID: sessionID,
			Role:      role,
			Content:   text,
			CreatedAt: frameworkTime(session.StartTime, 0, path),
		}
		setFrameworkMetadata(msg, frameworkQwenCode, map[string]interface{}{
			"project_hash": session.ProjectHash,
		})
		out = append(out, msg)
	}
	return validateFrameworkMessages(path, out)
}

// qwenPartsText 提取 Google GenAI Content.parts 中的可见文本，跳过思维链与工具调用。
func qwenPartsText(parts []qwenPart) string {
	var out []string
	for _, p := range parts {
		if p.Thought {
			continue
		}
		if t := strings.TrimSpace(p.Text); t != "" {
			out = append(out, t)
		}
	}
	return strings.TrimSpace(strings.Join(out, "\n\n"))
}
