package filewatcher

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/mnemosyne-ai-systems/mneme/pkg/models"
)

const frameworkGemini = "gemini-cli"

// geminiAdapter 解析 Gemini CLI 的会话文件（~/.gemini/tmp/*/chats/session-*.json）。
type geminiAdapter struct{}

func (a *geminiAdapter) Name() string { return frameworkGemini }

func (a *geminiAdapter) Match(ext string, head []byte) bool {
	if ext != ".json" {
		return false
	}
	c := compactJSON(head)
	if strings.Contains(c, `"sessionId":`) && matchesAny(c, `"messages":[`, `"history":[`) {
		return true
	}
	return strings.HasPrefix(c, "[{") && strings.Contains(c, `"type":"gemini"`)
}

type geminiSession struct {
	SessionID   string          `json:"sessionId"`
	ProjectHash string          `json:"projectHash"`
	StartTime   string          `json:"startTime"`
	Messages    []geminiMessage `json:"messages"`
	History     []geminiMessage `json:"history"`
}

type geminiMessage struct {
	ID        string          `json:"id"`
	Timestamp string          `json:"timestamp"`
	Type      string          `json:"type"`
	Model     string          `json:"model"`
	Content   json.RawMessage `json:"content"`
	Tokens    *geminiTokens   `json:"tokens"`
}

type geminiTokens struct {
	Input  int32 `json:"input"`
	Output int32 `json:"output"`
	Total  int32 `json:"total"`
}

func (a *geminiAdapter) Parse(path, tenantID string) ([]*models.HistoryMessage, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("filewatcher: read %s: %w", path, err)
	}
	raw = bytes.TrimPrefix(raw, []byte("\xef\xbb\xbf"))

	var session geminiSession
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		if err := json.Unmarshal(trimmed, &session.Messages); err != nil {
			return nil, fmt.Errorf("filewatcher: parse %s: %w", path, err)
		}
		session.SessionID = deriveSessionID(path)
	} else {
		if err := json.Unmarshal(trimmed, &session); err != nil {
			return nil, fmt.Errorf("filewatcher: parse %s: %w", path, err)
		}
		if len(session.Messages) == 0 {
			session.Messages = session.History
		}
		if session.SessionID == "" {
			session.SessionID = deriveSessionID(path)
		}
	}

	var out []*models.HistoryMessage
	for _, m := range session.Messages {
		var role string
		switch strings.ToLower(strings.TrimSpace(m.Type)) {
		case "user":
			role = models.RoleUser
		case "gemini", "model":
			role = models.RoleAssistant
		default:
			continue
		}
		text, _ := contentText(m.Content)
		if text == "" {
			continue
		}

		msg := &models.HistoryMessage{
			MessageID: firstNonEmpty(m.ID, deriveID(path, session.SessionID+role+text)),
			TenantID:  tenantID,
			SessionID: session.SessionID,
			Role:      role,
			Content:   text,
			ModelName: m.Model,
			CreatedAt: parseTimeOrFileMtime(firstNonEmpty(m.Timestamp, session.StartTime), path),
		}
		if m.Tokens != nil {
			msg.PromptTokens = m.Tokens.Input
			msg.CompletionTokens = m.Tokens.Output
			msg.TotalTokens = m.Tokens.Total
		}
		setFrameworkMetadata(msg, frameworkGemini, map[string]interface{}{
			"project_hash": session.ProjectHash,
		})
		out = append(out, msg)
	}
	return validateFrameworkMessages(path, out)
}
