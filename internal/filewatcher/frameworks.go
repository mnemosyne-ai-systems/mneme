package filewatcher

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mnemosyne-ai-systems/mneme/pkg/models"
)

// frameworkAdapter 识别并解析主流 AI 编码工具 / 聊天导出的会话文件。
// Match 只依赖扩展名与文件头部内容，避免把普通 JSONL / JSON 误判为框架格式。
type frameworkAdapter interface {
	Name() string
	Match(ext string, head []byte) bool
	Parse(path, tenantID string) ([]*models.HistoryMessage, error)
}

// detectHeadSize 是格式嗅探读取的最大字节数。
const detectHeadSize = 256 * 1024

// detectFramework 读取文件头部，返回第一个匹配的框架适配器。
func detectFramework(adapters []frameworkAdapter, path, ext string) frameworkAdapter {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer func() { _ = f.Close() }()

	head := make([]byte, detectHeadSize)
	n, err := f.Read(head)
	if n == 0 && err != nil {
		return nil
	}
	head = head[:n]
	for _, a := range adapters {
		if a.Match(ext, head) {
			return a
		}
	}
	return nil
}

// compactJSON 去掉 JSON 中的所有空白，便于用固定子串做格式嗅探。
func compactJSON(head []byte) string {
	return strings.Join(strings.Fields(string(head)), "")
}

func matchesAny(compact string, markers ...string) bool {
	for _, m := range markers {
		if strings.Contains(compact, m) {
			return true
		}
	}
	return false
}

// contentBlock 兼容 OpenAI / Claude 风格的内容块。
type contentBlock struct {
	Type    string          `json:"type"`
	Text    string          `json:"text"`
	Content json.RawMessage `json:"content"`
}

// contentText 从字符串或内容块数组中提取纯文本；toolResult 表示包含工具结果块。
func contentText(raw json.RawMessage) (text string, toolResult bool) {
	if len(raw) == 0 {
		return "", false
	}
	var single string
	if err := json.Unmarshal(raw, &single); err == nil {
		return strings.TrimSpace(single), false
	}
	var blocks []contentBlock
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return "", false
	}
	var parts []string
	for _, b := range blocks {
		switch b.Type {
		case "text", "input_text", "output_text":
			if t := strings.TrimSpace(b.Text); t != "" {
				parts = append(parts, t)
			}
		case "tool_result":
			toolResult = true
			if t := blockContentText(b.Content); t != "" {
				parts = append(parts, t)
			}
		}
	}
	return strings.TrimSpace(strings.Join(parts, "\n\n")), toolResult
}

// blockContentText 提取 tool_result.content（字符串或文本块数组）。
func blockContentText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var single string
	if json.Unmarshal(raw, &single) == nil {
		return strings.TrimSpace(single)
	}
	var blocks []contentBlock
	if json.Unmarshal(raw, &blocks) != nil {
		return ""
	}
	var parts []string
	for _, b := range blocks {
		if t := strings.TrimSpace(b.Text); t != "" {
			parts = append(parts, t)
		}
	}
	return strings.TrimSpace(strings.Join(parts, "\n\n"))
}

// normalizeRole 把各框架的角色名归一为 Mneme 角色；未知角色返回空串。
func normalizeRole(role string) string {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "user", "human", "input":
		return models.RoleUser
	case "assistant", "gemini", "model", "ai", "output":
		return models.RoleAssistant
	case "system":
		return models.RoleSystem
	case "tool", "function", "tool_result":
		return models.RoleTool
	default:
		return ""
	}
}

// setFrameworkMetadata 写入 metadata.framework 及来源附加信息。
func setFrameworkMetadata(msg *models.HistoryMessage, framework string, extra map[string]interface{}) {
	meta := map[string]interface{}{"framework": framework}
	for k, v := range extra {
		switch value := v.(type) {
		case string:
			if value != "" {
				meta[k] = value
			}
		case bool:
			if value {
				meta[k] = value
			}
		case nil:
		default:
			meta[k] = v
		}
	}
	if raw, err := json.Marshal(meta); err == nil {
		msg.Metadata = string(raw)
	}
}

// epochToTime 把 Unix 秒（可含小数）转为 UTC 时间。
func epochToTime(sec float64) time.Time {
	if sec <= 0 {
		return time.Time{}
	}
	whole := int64(sec)
	return time.Unix(whole, int64((sec-float64(whole))*1e9)).UTC()
}

// frameworkTime 优先使用导出中的时间，缺失时回退到文件修改时间。
func frameworkTime(value string, epoch float64, path string) time.Time {
	if epoch > 0 {
		return epochToTime(epoch)
	}
	return parseTimeOrFileMtime(value, path)
}

// validateFrameworkMessages 统一补齐默认值并校验。
func validateFrameworkMessages(path string, msgs []*models.HistoryMessage) ([]*models.HistoryMessage, error) {
	for _, m := range msgs {
		m.Normalize()
		if err := m.Validate(); err != nil {
			return nil, fmt.Errorf("filewatcher: %s: %w", path, err)
		}
	}
	return msgs, nil
}
