package filewatcher

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/mnemosyne-ai-systems/mneme/pkg/models"
)

const frameworkKimiCLI = "kimi-cli"

// kimiAdapter 解析 Kimi CLI 的 context.jsonl
// （~/.kimi/sessions/<work-dir-hash>/<session-id>/context.jsonl）。
// 首行为 _system_prompt，其间可能插入 _usage / _checkpoint 控制记录。
type kimiAdapter struct{}

func (a *kimiAdapter) Name() string { return frameworkKimiCLI }

func (a *kimiAdapter) Match(ext string, head []byte) bool {
	if ext != ".jsonl" {
		return false
	}
	c := compactJSON(head)
	return matchesAny(c, `"role":"_system_prompt"`, `"role":"_checkpoint"`, `"role":"_usage"`)
}

type kimiRecord struct {
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content"`
	ToolCallID string          `json:"tool_call_id"`
}

type kimiPart struct {
	Type  string `json:"type"`
	Text  string `json:"text"`
	Think string `json:"think"`
}

func (a *kimiAdapter) Parse(path, tenantID string) ([]*models.HistoryMessage, error) {
	if isKimiSubagentPath(path) {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("filewatcher: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	sessionID := kimiSessionID(path)
	createdAt := parseTimeOrFileMtime("", path)
	var out []*models.HistoryMessage
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var rec kimiRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			// 会话文件可能正在追加，跳过不完整的行。
			continue
		}
		if rec.Role == "" || strings.HasPrefix(rec.Role, "_") {
			continue
		}
		role := normalizeRole(rec.Role)
		if role == "" {
			continue
		}
		text := kimiContentText(rec.Content)
		if text == "" {
			continue
		}

		msg := &models.HistoryMessage{
			MessageID: deriveID(path, strconv.Itoa(lineNo)+role+text),
			TenantID:  tenantID,
			SessionID: sessionID,
			Role:      role,
			Content:   text,
			CreatedAt: createdAt,
		}
		setFrameworkMetadata(msg, frameworkKimiCLI, nil)
		out = append(out, msg)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("filewatcher: scan %s: %w", path, err)
	}
	return validateFrameworkMessages(path, out)
}

// kimiContentText 提取 Message.content（字符串或内容块数组），跳过思维链。
func kimiContentText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var single string
	if json.Unmarshal(raw, &single) == nil {
		return strings.TrimSpace(single)
	}
	var parts []kimiPart
	if json.Unmarshal(raw, &parts) != nil {
		return ""
	}
	var out []string
	for _, p := range parts {
		if p.Type == "think" {
			continue
		}
		if t := strings.TrimSpace(p.Text); t != "" {
			out = append(out, t)
		}
	}
	return strings.TrimSpace(strings.Join(out, "\n\n"))
}

// kimiSessionID 使用会话目录名（新版 context.jsonl）或文件名（旧版）。
func kimiSessionID(path string) string {
	if strings.EqualFold(filepath.Base(path), "context.jsonl") {
		return filepath.Base(filepath.Dir(path))
	}
	return deriveSessionID(path)
}

// isKimiSubagentPath 跳过 subagents 目录下的子代理会话，避免与主会话重复。
func isKimiSubagentPath(path string) bool {
	for _, part := range strings.Split(filepath.ToSlash(path), "/") {
		if part == "subagents" {
			return true
		}
	}
	return false
}
