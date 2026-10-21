// Package filewatcher 实现文件驱动的历史消息接入：
// 监听目录变更，解析 .md / .jsonl / .json / .txt 文件并转换为 HistoryMessage。
// JSON / JSONL 会先尝试识别 Claude Code、Qwen Code、Kimi CLI、Codex CLI、Gemini CLI、
// Claude/ChatGPT 导出等框架格式。
package filewatcher

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/mnemosyne-ai-systems/mneme/pkg/models"
)

// Parser 把文件内容解析为一批 HistoryMessage。
type Parser interface {
	// Ext 返回支持的扩展名（含点，小写）。
	Ext() string
	Parse(path string, tenantID string) ([]*models.HistoryMessage, error)
}

// Registry 按扩展名分发解析器；json / jsonl 先做框架格式嗅探。
type Registry struct {
	parsers  map[string]Parser
	adapters []frameworkAdapter
}

// NewRegistry 创建解析器注册表，内置 md / jsonl / json / txt 解析器。
func NewRegistry() *Registry {
	r := &Registry{
		parsers: map[string]Parser{},
		adapters: []frameworkAdapter{
			&kimiAdapter{},
			&qwenCodeAdapter{},
			&claudeCodeAdapter{},
			&codexAdapter{},
			&geminiAdapter{},
			&claudeExportAdapter{},
			&chatGPTExportAdapter{},
		},
	}
	r.Register(&MarkdownParser{})
	r.Register(&JSONLParser{})
	r.Register(&JSONParser{})
	r.Register(&TextParser{})
	return r
}

// Register 注册解析器。
func (r *Registry) Register(p Parser) { r.parsers[strings.ToLower(p.Ext())] = p }

// Get 按扩展名查找解析器。
func (r *Registry) Get(ext string) (Parser, bool) {
	p, ok := r.parsers[strings.ToLower(ext)]
	return p, ok
}

// Parse 按文件扩展名解析；json / jsonl 优先交给内容嗅探匹配到的框架适配器。
func (r *Registry) Parse(path, tenantID string) ([]*models.HistoryMessage, error) {
	ext := strings.ToLower(filepath.Ext(path))
	if ext == ".json" || ext == ".jsonl" {
		if adapter := detectFramework(r.adapters, path, ext); adapter != nil {
			msgs, err := adapter.Parse(path, tenantID)
			if err != nil {
				return nil, fmt.Errorf("filewatcher: %s (%s): %w", path, adapter.Name(), err)
			}
			return msgs, nil
		}
	}
	p, ok := r.Get(ext)
	if !ok {
		return nil, fmt.Errorf("filewatcher: unsupported extension %q", ext)
	}
	return p.Parse(path, tenantID)
}

// frontmatter 是 Markdown 文件头部的 YAML 元数据。
type frontmatter struct {
	SessionID        string            `yaml:"session_id"`
	MessageID        string            `yaml:"message_id"`
	UserID           string            `yaml:"user_id"`
	TenantID         string            `yaml:"tenant_id"`
	Role             string            `yaml:"role"`
	ModelName        string            `yaml:"model_name"`
	TotalTokens      int32             `yaml:"total_tokens"`
	PromptTokens     int32             `yaml:"prompt_tokens"`
	CompletionTokens int32             `yaml:"completion_tokens"`
	ResponseTimeMs   int32             `yaml:"response_time_ms"`
	CreatedAt        string            `yaml:"created_at"`
	Metadata         map[string]string `yaml:"metadata"`
}

// MarkdownParser 解析 YAML Frontmatter + 正文。
type MarkdownParser struct{}

// Ext 返回 .md。
func (p *MarkdownParser) Ext() string { return ".md" }

// Parse 解析 Markdown 文件为单条 HistoryMessage。
func (p *MarkdownParser) Parse(path, tenantID string) ([]*models.HistoryMessage, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("filewatcher: read %s: %w", path, err)
	}
	fm, body := splitFrontmatter(string(raw))

	var meta frontmatter
	if fm != "" {
		if err := yaml.Unmarshal([]byte(fm), &meta); err != nil {
			return nil, fmt.Errorf("filewatcher: parse frontmatter of %s: %w", path, err)
		}
	}

	body = strings.TrimSpace(body)
	if body == "" {
		return nil, nil
	}

	tenant := firstNonEmpty(meta.TenantID, tenantID)
	msg := &models.HistoryMessage{
		MessageID:        firstNonEmpty(meta.MessageID, deriveID(path, body)),
		TenantID:         tenant,
		SessionID:        firstNonEmpty(meta.SessionID, deriveSessionID(path)),
		UserID:           meta.UserID,
		Role:             firstNonEmpty(meta.Role, models.RoleUser),
		Content:          body,
		ModelName:        meta.ModelName,
		TotalTokens:      meta.TotalTokens,
		PromptTokens:     meta.PromptTokens,
		CompletionTokens: meta.CompletionTokens,
		ResponseTimeMs:   meta.ResponseTimeMs,
		CreatedAt:        parseTimeOrFileMtime(meta.CreatedAt, path),
	}
	if len(meta.Metadata) > 0 {
		if b, err := json.Marshal(meta.Metadata); err == nil {
			msg.Metadata = string(b)
		}
	}
	msg.Normalize()
	if err := msg.Validate(); err != nil {
		return nil, fmt.Errorf("filewatcher: %s: %w", path, err)
	}
	return []*models.HistoryMessage{msg}, nil
}

// splitFrontmatter 分离 YAML Frontmatter 与正文。
func splitFrontmatter(content string) (string, string) {
	content = strings.TrimLeft(content, "\ufeff \t\r\n")
	if !strings.HasPrefix(content, "---") {
		return "", content
	}
	rest := content[3:]
	rest = strings.TrimPrefix(rest, "\r")
	rest = strings.TrimPrefix(rest, "\n")

	idx := strings.Index(rest, "\n---")
	if idx < 0 {
		return "", content
	}
	fm := rest[:idx]
	body := rest[idx+4:]
	body = strings.TrimPrefix(body, "\r")
	body = strings.TrimPrefix(body, "\n")
	return fm, body
}

// JSONLParser 逐行解析 JSON 对象。
type JSONLParser struct{}

// Ext 返回 .jsonl。
func (p *JSONLParser) Ext() string { return ".jsonl" }

// jsonlRecord 兼容 OpenAI 风格与 Mneme 原生字段。
type jsonlRecord struct {
	MessageID        string                 `json:"message_id"`
	ID               string                 `json:"id"`
	TenantID         string                 `json:"tenant_id"`
	SessionID        string                 `json:"session_id"`
	Conversation     string                 `json:"conversation_id"`
	UserID           string                 `json:"user_id"`
	Role             string                 `json:"role"`
	Content          string                 `json:"content"`
	Text             string                 `json:"text"`
	ModelName        string                 `json:"model_name"`
	Model            string                 `json:"model"`
	TotalTokens      int32                  `json:"total_tokens"`
	PromptTokens     int32                  `json:"prompt_tokens"`
	CompletionTokens int32                  `json:"completion_tokens"`
	ResponseTimeMs   int32                  `json:"response_time_ms"`
	CreatedAt        string                 `json:"created_at"`
	Metadata         map[string]interface{} `json:"metadata"`
}

// Parse 解析 JSONL 文件。
func (p *JSONLParser) Parse(path, tenantID string) ([]*models.HistoryMessage, error) {
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
		if line == "" || strings.HasPrefix(line, "//") {
			continue
		}
		var rec jsonlRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			return nil, fmt.Errorf("filewatcher: %s line %d: %w", path, lineNo, err)
		}
		msg := recordMessage(path, tenantID, rec, strconv.Itoa(lineNo)+firstNonEmpty(rec.Content, rec.Text), sessionFallback)
		if msg == nil {
			continue
		}
		if err := msg.Validate(); err != nil {
			return nil, fmt.Errorf("filewatcher: %s line %d: %w", path, lineNo, err)
		}
		out = append(out, msg)
	}
	if err := scanner.Err(); err != nil && err != io.EOF {
		return nil, fmt.Errorf("filewatcher: scan %s: %w", path, err)
	}
	return out, nil
}

// JSONParser 解析通用 JSON：消息数组、单条消息，或 {"messages": [...]} 包装。
type JSONParser struct{}

// Ext 返回 .json。
func (p *JSONParser) Ext() string { return ".json" }

// Parse 解析通用 JSON 文件。
func (p *JSONParser) Parse(path, tenantID string) ([]*models.HistoryMessage, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("filewatcher: read %s: %w", path, err)
	}
	trimmed := bytes.TrimSpace(bytes.TrimPrefix(raw, []byte("\xef\xbb\xbf")))
	if len(trimmed) == 0 {
		return nil, nil
	}

	var records []jsonlRecord
	if trimmed[0] == '[' {
		if err := json.Unmarshal(trimmed, &records); err != nil {
			return nil, fmt.Errorf("filewatcher: parse %s: %w", path, err)
		}
	} else {
		var wrapper struct {
			Messages []jsonlRecord `json:"messages"`
			Records  []jsonlRecord `json:"records"`
			Data     []jsonlRecord `json:"data"`
		}
		if err := json.Unmarshal(trimmed, &wrapper); err == nil {
			switch {
			case len(wrapper.Messages) > 0:
				records = wrapper.Messages
			case len(wrapper.Records) > 0:
				records = wrapper.Records
			case len(wrapper.Data) > 0:
				records = wrapper.Data
			}
		}
		if records == nil {
			var one jsonlRecord
			if err := json.Unmarshal(trimmed, &one); err != nil {
				return nil, fmt.Errorf("filewatcher: parse %s: %w", path, err)
			}
			records = []jsonlRecord{one}
		}
	}

	sessionFallback := deriveSessionID(path)
	var out []*models.HistoryMessage
	for i, rec := range records {
		msg := recordMessage(path, tenantID, rec, strconv.Itoa(i)+firstNonEmpty(rec.Content, rec.Text), sessionFallback)
		if msg == nil {
			continue
		}
		if err := msg.Validate(); err != nil {
			return nil, fmt.Errorf("filewatcher: %s record %d: %w", path, i+1, err)
		}
		out = append(out, msg)
	}
	return out, nil
}

// recordMessage 把通用 JSON / JSONL 记录转换为消息；content 为空时返回 nil。
func recordMessage(path, tenantID string, rec jsonlRecord, key, sessionFallback string) *models.HistoryMessage {
	content := firstNonEmpty(rec.Content, rec.Text)
	if content == "" {
		return nil
	}
	msg := &models.HistoryMessage{
		MessageID:        firstNonEmpty(rec.MessageID, rec.ID, deriveID(path, key)),
		TenantID:         firstNonEmpty(rec.TenantID, tenantID),
		SessionID:        firstNonEmpty(rec.SessionID, rec.Conversation, sessionFallback),
		UserID:           rec.UserID,
		Role:             firstNonEmpty(rec.Role, models.RoleUser),
		Content:          content,
		ModelName:        firstNonEmpty(rec.ModelName, rec.Model),
		TotalTokens:      rec.TotalTokens,
		PromptTokens:     rec.PromptTokens,
		CompletionTokens: rec.CompletionTokens,
		ResponseTimeMs:   rec.ResponseTimeMs,
		CreatedAt:        parseTimeOrFileMtime(rec.CreatedAt, path),
	}
	if len(rec.Metadata) > 0 {
		if b, err := json.Marshal(rec.Metadata); err == nil {
			msg.Metadata = string(b)
		}
	}
	msg.Normalize()
	return msg
}

// TextParser 按规则提取纯文本：
// 支持 "role: content" 前缀行，否则整篇作为一条 user 消息。
type TextParser struct{}

// Ext 返回 .txt。
func (p *TextParser) Ext() string { return ".txt" }

// Parse 解析纯文本文件。
func (p *TextParser) Parse(path, tenantID string) ([]*models.HistoryMessage, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("filewatcher: read %s: %w", path, err)
	}
	content := strings.TrimSpace(string(raw))
	if content == "" {
		return nil, nil
	}

	sessionID := deriveSessionID(path)
	createdAt := parseTimeOrFileMtime("", path)

	var out []*models.HistoryMessage
	for i, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		role, text := models.RoleUser, line
		if idx := strings.Index(line, ":"); idx > 0 && idx < 20 {
			candidate := strings.ToLower(strings.TrimSpace(line[:idx]))
			switch candidate {
			case models.RoleUser, models.RoleAssistant, models.RoleSystem, models.RoleTool:
				role = candidate
				text = strings.TrimSpace(line[idx+1:])
			}
		}
		if text == "" {
			continue
		}
		msg := &models.HistoryMessage{
			MessageID: deriveID(path, strconv.Itoa(i)+text),
			TenantID:  tenantID,
			SessionID: sessionID,
			Role:      role,
			Content:   text,
			CreatedAt: createdAt,
		}
		msg.Normalize()
		if err := msg.Validate(); err != nil {
			return nil, fmt.Errorf("filewatcher: %s: %w", path, err)
		}
		out = append(out, msg)
	}
	return out, nil
}

// deriveID 由路径和内容生成稳定的确定性 ID，保证重复处理时幂等。
func deriveID(path, content string) string {
	sum := sha256.Sum256([]byte(path + "|" + content))
	return hex.EncodeToString(sum[:16])
}

// deriveSessionID 用文件名（去扩展名）作为默认会话 ID。
func deriveSessionID(path string) string {
	base := filepath.Base(path)
	return strings.TrimSuffix(base, filepath.Ext(base))
}

// parseTimeOrFileMtime 解析时间字符串，失败时回退到文件修改时间。
func parseTimeOrFileMtime(value, path string) time.Time {
	if value != "" {
		for _, layout := range []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"} {
			if t, err := time.Parse(layout, value); err == nil {
				return t.UTC()
			}
		}
	}
	if info, err := os.Stat(path); err == nil {
		return info.ModTime().UTC()
	}
	return time.Now().UTC()
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
