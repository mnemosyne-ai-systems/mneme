package filewatcher

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/mnemosyne-ai-systems/mneme/pkg/models"
)

const (
	frameworkClaudeExport  = "claude-export"
	frameworkChatGPTExport = "chatgpt-export"
)

// claudeExportAdapter 解析 claude.ai 导出的 conversations.json（chat_messages）。
type claudeExportAdapter struct{}

func (a *claudeExportAdapter) Name() string { return frameworkClaudeExport }

func (a *claudeExportAdapter) Match(ext string, head []byte) bool {
	return ext == ".json" && strings.Contains(compactJSON(head), `"chat_messages":[`)
}

type claudeExportConversation struct {
	UUID         string                `json:"uuid"`
	Name         string                `json:"name"`
	ChatMessages []claudeExportMessage `json:"chat_messages"`
}

type claudeExportMessage struct {
	UUID      string              `json:"uuid"`
	Text      string              `json:"text"`
	Sender    string              `json:"sender"`
	CreatedAt string              `json:"created_at"`
	Content   []claudeExportBlock `json:"content"`
}

type claudeExportBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func (a *claudeExportAdapter) Parse(path, tenantID string) ([]*models.HistoryMessage, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("filewatcher: read %s: %w", path, err)
	}
	trimmed := bytes.TrimSpace(bytes.TrimPrefix(raw, []byte("\xef\xbb\xbf")))

	var convs []claudeExportConversation
	if len(trimmed) > 0 && trimmed[0] == '[' {
		if err := json.Unmarshal(trimmed, &convs); err != nil {
			return nil, fmt.Errorf("filewatcher: parse %s: %w", path, err)
		}
	} else {
		var wrapper struct {
			Conversations []claudeExportConversation `json:"conversations"`
		}
		if err := json.Unmarshal(trimmed, &wrapper); err == nil && len(wrapper.Conversations) > 0 {
			convs = wrapper.Conversations
		} else {
			var one claudeExportConversation
			if err := json.Unmarshal(trimmed, &one); err != nil {
				return nil, fmt.Errorf("filewatcher: parse %s: %w", path, err)
			}
			convs = []claudeExportConversation{one}
		}
	}

	var out []*models.HistoryMessage
	for i, conv := range convs {
		sessionID := firstNonEmpty(conv.UUID, fmt.Sprintf("%s-%d", deriveSessionID(path), i))
		for _, m := range conv.ChatMessages {
			role := normalizeRole(m.Sender)
			if role == "" {
				continue
			}
			text := strings.TrimSpace(m.Text)
			if text == "" {
				text = claudeExportText(m.Content)
			}
			if text == "" {
				continue
			}

			msg := &models.HistoryMessage{
				MessageID: firstNonEmpty(m.UUID, deriveID(path, sessionID+role+text)),
				TenantID:  tenantID,
				SessionID: sessionID,
				Role:      role,
				Content:   text,
				CreatedAt: parseTimeOrFileMtime(m.CreatedAt, path),
			}
			setFrameworkMetadata(msg, frameworkClaudeExport, map[string]interface{}{
				"title": conv.Name,
			})
			out = append(out, msg)
		}
	}
	return validateFrameworkMessages(path, out)
}

func claudeExportText(blocks []claudeExportBlock) string {
	var parts []string
	for _, b := range blocks {
		if b.Type != "" && b.Type != "text" {
			continue
		}
		if t := strings.TrimSpace(b.Text); t != "" {
			parts = append(parts, t)
		}
	}
	return strings.TrimSpace(strings.Join(parts, "\n\n"))
}

// chatGPTExportAdapter 解析 ChatGPT 导出的 conversations.json（mapping 树）。
type chatGPTExportAdapter struct{}

func (a *chatGPTExportAdapter) Name() string { return frameworkChatGPTExport }

func (a *chatGPTExportAdapter) Match(ext string, head []byte) bool {
	if ext != ".json" {
		return false
	}
	c := compactJSON(head)
	return strings.Contains(c, `"mapping":{`) && matchesAny(c, `"current_node":`, `"title":`)
}

type chatGPTExport struct {
	ID          string                 `json:"id"`
	Title       string                 `json:"title"`
	CreateTime  float64                `json:"create_time"`
	CurrentNode string                 `json:"current_node"`
	Mapping     map[string]chatGPTNode `json:"mapping"`
}

type chatGPTNode struct {
	ID       string          `json:"id"`
	Parent   string          `json:"parent"`
	Children []string        `json:"children"`
	Message  *chatGPTMessage `json:"message"`
}

type chatGPTMessage struct {
	ID         string  `json:"id"`
	CreateTime float64 `json:"create_time"`
	Author     struct {
		Role string `json:"role"`
		Name string `json:"name"`
	} `json:"author"`
	Content struct {
		ContentType string            `json:"content_type"`
		Parts       []json.RawMessage `json:"parts"`
		Text        string            `json:"text"`
	} `json:"content"`
	Metadata map[string]interface{} `json:"metadata"`
}

func (a *chatGPTExportAdapter) Parse(path, tenantID string) ([]*models.HistoryMessage, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("filewatcher: read %s: %w", path, err)
	}
	trimmed := bytes.TrimSpace(bytes.TrimPrefix(raw, []byte("\xef\xbb\xbf")))

	var convs []chatGPTExport
	if len(trimmed) > 0 && trimmed[0] == '[' {
		if err := json.Unmarshal(trimmed, &convs); err != nil {
			return nil, fmt.Errorf("filewatcher: parse %s: %w", path, err)
		}
	} else {
		var root map[string]json.RawMessage
		if err := json.Unmarshal(trimmed, &root); err != nil {
			return nil, fmt.Errorf("filewatcher: parse %s: %w", path, err)
		}
		if wrapper, ok := root["conversations"]; ok {
			if err := json.Unmarshal(wrapper, &convs); err != nil {
				return nil, fmt.Errorf("filewatcher: parse %s: %w", path, err)
			}
		} else {
			var one chatGPTExport
			if err := json.Unmarshal(trimmed, &one); err != nil {
				return nil, fmt.Errorf("filewatcher: parse %s: %w", path, err)
			}
			convs = []chatGPTExport{one}
		}
	}

	var out []*models.HistoryMessage
	for i, conv := range convs {
		sessionID := firstNonEmpty(conv.ID, fmt.Sprintf("%s-%d", deriveSessionID(path), i))
		for _, nodeID := range chatGPTOrderedNodes(conv) {
			node := conv.Mapping[nodeID]
			if node.Message == nil {
				continue
			}
			if chatGPTHidden(node.Message) {
				continue
			}
			role := normalizeRole(node.Message.Author.Role)
			if role == "" {
				continue
			}
			text := chatGPTMessageText(node.Message)
			if text == "" {
				continue
			}

			msg := &models.HistoryMessage{
				MessageID: firstNonEmpty(node.Message.ID, nodeID, deriveID(path, sessionID+role+text)),
				TenantID:  tenantID,
				SessionID: sessionID,
				Role:      role,
				Content:   text,
				CreatedAt: frameworkTime("", firstNonZero(node.Message.CreateTime, conv.CreateTime), path),
			}
			setFrameworkMetadata(msg, frameworkChatGPTExport, map[string]interface{}{
				"title": conv.Title,
			})
			out = append(out, msg)
		}
	}
	return validateFrameworkMessages(path, out)
}

func chatGPTHidden(m *chatGPTMessage) bool {
	hidden, ok := m.Metadata["is_visually_hidden_from_conversation"].(bool)
	return ok && hidden
}

func chatGPTMessageText(m *chatGPTMessage) string {
	switch m.Content.ContentType {
	case "thoughts", "reasoning_recap", "model_editable_context", "user_editable_context":
		return ""
	}

	var parts []string
	for _, raw := range m.Content.Parts {
		var single string
		if json.Unmarshal(raw, &single) == nil {
			if t := strings.TrimSpace(single); t != "" {
				parts = append(parts, t)
			}
			continue
		}
		var block struct {
			Text string `json:"text"`
		}
		if json.Unmarshal(raw, &block) == nil {
			if t := strings.TrimSpace(block.Text); t != "" {
				parts = append(parts, t)
			}
		}
	}
	if len(parts) == 0 {
		return strings.TrimSpace(m.Content.Text)
	}
	return strings.TrimSpace(strings.Join(parts, "\n\n"))
}

// chatGPTOrderedNodes 返回对话节点顺序：优先沿 current_node 回溯到根，否则按时间 DFS。
func chatGPTOrderedNodes(conv chatGPTExport) []string {
	if conv.CurrentNode != "" {
		if _, ok := conv.Mapping[conv.CurrentNode]; ok {
			var ids []string
			seen := map[string]bool{}
			for id := conv.CurrentNode; id != "" && !seen[id]; {
				seen[id] = true
				ids = append(ids, id)
				node, ok := conv.Mapping[id]
				if !ok {
					break
				}
				id = node.Parent
			}
			for i, j := 0, len(ids)-1; i < j; i, j = i+1, j-1 {
				ids[i], ids[j] = ids[j], ids[i]
			}
			return ids
		}
	}

	var roots []string
	for id, node := range conv.Mapping {
		if node.Parent == "" {
			roots = append(roots, id)
			continue
		}
		if _, ok := conv.Mapping[node.Parent]; !ok {
			roots = append(roots, id)
		}
	}
	sort.Slice(roots, func(i, j int) bool {
		return chatGPTNodeTime(conv.Mapping[roots[i]]) < chatGPTNodeTime(conv.Mapping[roots[j]])
	})

	var out []string
	seen := map[string]bool{}
	var walk func(string)
	walk = func(id string) {
		if seen[id] {
			return
		}
		seen[id] = true
		out = append(out, id)
		for _, child := range conv.Mapping[id].Children {
			if _, ok := conv.Mapping[child]; ok {
				walk(child)
			}
		}
	}
	for _, root := range roots {
		walk(root)
	}
	return out
}

func chatGPTNodeTime(node chatGPTNode) float64 {
	if node.Message == nil {
		return 0
	}
	return node.Message.CreateTime
}

func firstNonZero(values ...float64) float64 {
	for _, v := range values {
		if v > 0 {
			return v
		}
	}
	return 0
}
