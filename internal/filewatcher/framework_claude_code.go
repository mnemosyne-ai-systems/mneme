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

const frameworkClaudeCode = "claude-code"

// claudeCodeAdapter 解析 Claude Code 的会话记录（~/.claude/projects/**/*.jsonl）。
type claudeCodeAdapter struct{}

func (a *claudeCodeAdapter) Name() string { return frameworkClaudeCode }

func (a *claudeCodeAdapter) Match(ext string, head []byte) bool {
	if ext != ".jsonl" {
		return false
	}
	c := compactJSON(head)
	if matchesAny(c, `"parentUuid":`, `"isSidechain":`, `"userType":`) {
		return true
	}
	return strings.Contains(c, `"sessionId":`) && strings.Contains(c, `"message":{`)
}

// claudeCodeRecord 是 Claude Code JSONL 的一行。
type claudeCodeRecord struct {
	Type        string             `json:"type"`
	UUID        string             `json:"uuid"`
	SessionID   string             `json:"sessionId"`
	Timestamp   string             `json:"timestamp"`
	Cwd         string             `json:"cwd"`
	IsSidechain bool               `json:"isSidechain"`
	Message     *claudeCodeMessage `json:"message"`
}

type claudeCodeMessage struct {
	ID      string           `json:"id"`
	Role    string           `json:"role"`
	Model   string           `json:"model"`
	Content json.RawMessage  `json:"content"`
	Usage   *claudeCodeUsage `json:"usage"`
}

type claudeCodeUsage struct {
	InputTokens  int32 `json:"input_tokens"`
	OutputTokens int32 `json:"output_tokens"`
}

func (a *claudeCodeAdapter) Parse(path, tenantID string) ([]*models.HistoryMessage, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("filewatcher: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	fallbackSession := deriveSessionID(path)
	var out []*models.HistoryMessage
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var rec claudeCodeRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			// 会话文件可能正在追加，跳过不完整的行。
			continue
		}
		if rec.Message == nil || (rec.Type != "user" && rec.Type != "assistant") {
			continue
		}
		text, hasToolResult := contentText(rec.Message.Content)
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
		if hasToolResult && role == models.RoleUser {
			role = models.RoleTool
		}

		msg := &models.HistoryMessage{
			MessageID: firstNonEmpty(rec.UUID, rec.Message.ID, deriveID(path, strconv.Itoa(lineNo)+text)),
			TenantID:  tenantID,
			SessionID: firstNonEmpty(rec.SessionID, fallbackSession),
			Role:      role,
			Content:   text,
			ModelName: rec.Message.Model,
			CreatedAt: parseTimeOrFileMtime(rec.Timestamp, path),
		}
		if rec.Message.Usage != nil {
			msg.PromptTokens = rec.Message.Usage.InputTokens
			msg.CompletionTokens = rec.Message.Usage.OutputTokens
		}
		setFrameworkMetadata(msg, frameworkClaudeCode, map[string]interface{}{
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
