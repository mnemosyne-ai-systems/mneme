package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mnemosyne-labs/mneme/pkg/logger"
	"github.com/mnemosyne-labs/mneme/pkg/metrics"
	"github.com/mnemosyne-labs/mneme/pkg/models"
)

// MemoryExtractor 把一批记忆事件萃取为结构化画像。
type MemoryExtractor interface {
	Extract(ctx context.Context, events []*models.MemoryEvent) (map[string]string, error)
	// Fallback 在 LLM 不可用时至少保留原始事件摘要。
	Fallback(ctx context.Context, events []*models.MemoryEvent) (map[string]string, error)
}

// SessionSummarizer 把一场会话的多条消息浓缩为一段自然语言概括。
type SessionSummarizer interface {
	// Summarize 调用 LLM 生成会话摘要；未启用或失败时返回错误，由调用方决定是否降级。
	Summarize(ctx context.Context, messages []*models.HistoryMessage) (string, error)
	// SummaryFallback 在 LLM 不可用时退化为首尾消息拼接的轻量摘要。
	SummaryFallback(ctx context.Context, messages []*models.HistoryMessage) string
	// RefineSummary 在已有摘要基础上，结合新增消息进行增量更新；未启用或失败时返回错误。
	RefineSummary(ctx context.Context, prevSummary string, newMessages []*models.HistoryMessage) (string, error)
	// SummaryRefineFallback 在 LLM 不可用时把旧摘要与新消息拼接为轻量更新。
	SummaryRefineFallback(ctx context.Context, prevSummary string, newMessages []*models.HistoryMessage) string
}

// LLMConfig LLM 萃取配置。
type LLMConfig struct {
	Enabled   bool
	Endpoint  string
	Model     string
	APIKey    string
	Timeout   time.Duration
	MaxEvents int
}

// LLMExtractor 调用 OpenAI 兼容接口进行画像萃取，失败时自动降级。
type LLMExtractor struct {
	cfg    LLMConfig
	client *http.Client
}

// NewLLMExtractor 创建 LLM 萃取器。
func NewLLMExtractor(cfg LLMConfig) *LLMExtractor {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 20 * time.Second
	}
	if cfg.MaxEvents <= 0 {
		cfg.MaxEvents = 200
	}
	return &LLMExtractor{cfg: cfg, client: &http.Client{Timeout: cfg.Timeout}}
}

const extractPrompt = `你是一个用户画像萃取器。请阅读以下 Agent 记忆事件与对话消息，
输出一个扁平的 JSON 对象（键值均为字符串），概括该实体的稳定偏好、属性与近期关注点。
只输出 JSON，不要输出任何解释。

事件与对话列表：
`

const summaryPrompt = `你是一个对话总结助手。请阅读以下完整会话（按时间顺序排列），
用 2-4 句中文概括本次会话的主题、关键决策与待办事项。直接输出概括文本，不要使用 JSON、列表或任何额外解释。

会话消息：
`

const refinePrompt = `你是一个对话总结助手。以下是某场会话已有的摘要，以及该会话在之后新增的消息。
请基于新增消息对已有摘要做增量更新，保留仍然有效的内容，补充新进展，并纠正已变化的信息。
直接输出更新后的完整摘要文本（2-4 句中文），不要输出 JSON、列表或任何额外解释。

已有摘要：
%s

新增消息：
`

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

// Extract 调用 LLM 萃取画像；未启用或失败时返回错误，由调用方决定是否降级。
func (e *LLMExtractor) Extract(ctx context.Context, events []*models.MemoryEvent) (map[string]string, error) {
	if !e.cfg.Enabled {
		return nil, fmt.Errorf("llm extractor disabled")
	}
	if len(events) == 0 {
		return map[string]string{}, nil
	}
	if len(events) > e.cfg.MaxEvents {
		events = events[len(events)-e.cfg.MaxEvents:]
	}

	summary, err := json.Marshal(eventDigests(events))
	if err != nil {
		return nil, fmt.Errorf("marshal events: %w", err)
	}

	reqBody := map[string]interface{}{
		"model": e.cfg.Model,
		"messages": []map[string]string{
			{"role": "system", "content": "You extract structured profiles as flat JSON."},
			{"role": "user", "content": extractPrompt + string(summary)},
		},
		"temperature": 0.1,
	}
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal llm request: %w", err)
	}

	callCtx, cancel := context.WithTimeout(ctx, e.cfg.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(callCtx, http.MethodPost, e.cfg.Endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build llm request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if e.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+e.cfg.APIKey)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call llm: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("llm returned status %d", resp.StatusCode)
	}

	var out struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode llm response: %w", err)
	}
	if len(out.Choices) == 0 {
		return nil, fmt.Errorf("llm returned no choices")
	}

	profile, err := parseFlatJSON(out.Choices[0].Message.Content)
	if err != nil {
		return nil, err
	}
	profile["_extracted_by"] = "llm:" + e.cfg.Model
	profile["_extracted_at"] = time.Now().UTC().Format(time.RFC3339)
	return profile, nil
}

// Fallback 生成不依赖 LLM 的事件摘要画像，确保数据不丢失。
func (e *LLMExtractor) Fallback(_ context.Context, events []*models.MemoryEvent) (map[string]string, error) {
	return BuildFallbackProfile(events), nil
}

// BuildFallbackProfile 由原始事件构建摘要画像。
func BuildFallbackProfile(events []*models.MemoryEvent) map[string]string {
	profile := map[string]string{
		"_extracted_by": "fallback",
		"_extracted_at": time.Now().UTC().Format(time.RFC3339),
		"event_count":   strconv.Itoa(len(events)),
	}
	if len(events) == 0 {
		return profile
	}

	counts := map[string]int{}
	roleCounts := map[string]int{}
	var first, last time.Time
	for i, e := range events {
		counts[e.EventType]++
		if r, ok := e.Payload["role"].(string); ok && r != "" {
			roleCounts[r]++
		}
		if i == 0 || e.Timestamp.Before(first) {
			first = e.Timestamp
		}
		if e.Timestamp.After(last) {
			last = e.Timestamp
		}
	}

	types := make([]string, 0, len(counts))
	for k := range counts {
		types = append(types, k)
	}
	sort.Strings(types)
	parts := make([]string, 0, len(types))
	for _, k := range types {
		parts = append(parts, fmt.Sprintf("%s=%d", k, counts[k]))
	}
	profile["event_types"] = strings.Join(parts, ",")

	if len(roleCounts) > 0 {
		roles := make([]string, 0, len(roleCounts))
		for k := range roleCounts {
			roles = append(roles, k)
		}
		sort.Strings(roles)
		roleParts := make([]string, 0, len(roles))
		for _, k := range roles {
			roleParts = append(roleParts, fmt.Sprintf("%s=%d", k, roleCounts[k]))
		}
		profile["roles"] = strings.Join(roleParts, ",")
	}
	profile["first_event_at"] = first.UTC().Format(time.RFC3339)
	profile["last_event_at"] = last.UTC().Format(time.RFC3339)

	// 保留最近若干条事件的原始摘要，避免信息丢失。
	digests := eventDigests(events)
	if len(digests) > 20 {
		digests = digests[len(digests)-20:]
	}
	if raw, err := json.Marshal(digests); err == nil {
		profile["recent_events"] = string(raw)
	}
	return profile
}

// eventDigests 把事件压缩为轻量摘要，避免超长 prompt。
func eventDigests(events []*models.MemoryEvent) []map[string]interface{} {
	digests := make([]map[string]interface{}, 0, len(events))
	for _, e := range events {
		digests = append(digests, map[string]interface{}{
			"event_type": e.EventType,
			"timestamp":  e.Timestamp.UTC().Format(time.RFC3339),
			"payload":    e.Payload,
		})
	}
	return digests
}

// parseFlatJSON 从 LLM 输出中提取扁平 JSON 对象。
func parseFlatJSON(content string) (map[string]string, error) {
	content = strings.TrimSpace(content)
	content = strings.TrimPrefix(content, "```json")
	content = strings.TrimPrefix(content, "```")
	content = strings.TrimSuffix(content, "```")
	content = strings.TrimSpace(content)

	start := strings.Index(content, "{")
	end := strings.LastIndex(content, "}")
	if start < 0 || end <= start {
		return nil, fmt.Errorf("llm output is not a json object")
	}
	content = content[start : end+1]

	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(content), &raw); err != nil {
		return nil, fmt.Errorf("parse llm json: %w", err)
	}
	profile := make(map[string]string, len(raw))
	for k, v := range raw {
		switch val := v.(type) {
		case string:
			profile[k] = val
		default:
			b, err := json.Marshal(val)
			if err != nil {
				continue
			}
			profile[k] = string(b)
		}
	}
	return profile, nil
}

// ExtractWithFallback 先尝试 LLM 萃取，失败或超时自动降级。
func ExtractWithFallback(ctx context.Context, ex MemoryExtractor, events []*models.MemoryEvent) (map[string]string, string) {
	profile, err := ex.Extract(ctx, events)
	if err == nil && len(profile) > 0 {
		metrics.MemoryExtractTotal.WithLabelValues("llm").Inc()
		return profile, "llm"
	}
	if err != nil {
		logger.FromContext(ctx).WithError(err).Warn("memory extraction failed, falling back")
	}
	fallback, ferr := ex.Fallback(ctx, events)
	if ferr != nil {
		logger.FromContext(ctx).WithError(ferr).Error("fallback extraction failed")
		fallback = BuildFallbackProfile(events)
	}
	metrics.MemoryExtractTotal.WithLabelValues("fallback").Inc()
	return fallback, "fallback"
}

var _ MemoryExtractor = (*LLMExtractor)(nil)

// Summarize 调用 LLM 生成会话摘要；未启用或失败时返回错误，由调用方决定是否降级。
func (e *LLMExtractor) Summarize(ctx context.Context, messages []*models.HistoryMessage) (string, error) {
	if !e.cfg.Enabled {
		return "", fmt.Errorf("llm disabled")
	}
	if len(messages) == 0 {
		return "", fmt.Errorf("no messages to summarize")
	}
	msgs := messages
	if len(msgs) > e.cfg.MaxEvents {
		msgs = msgs[len(msgs)-e.cfg.MaxEvents:]
	}
	var b strings.Builder
	b.WriteString(summaryPrompt)
	for i, m := range msgs {
		role := m.Role
		if role == "" {
			role = "unknown"
		}
		b.WriteString(fmt.Sprintf("[%d] %s: %s\n", i+1, role, m.Content))
	}
	return e.llmSummarize(ctx, "你是专业的对话总结助手，只输出概括文本。", b.String())
}

// llmSummarize 执行一次 OpenAI 兼容的总结请求。
func (e *LLMExtractor) llmSummarize(ctx context.Context, system, user string) (string, error) {
	if !e.cfg.Enabled {
		return "", fmt.Errorf("llm disabled")
	}
	reqBody, err := json.Marshal(chatRequest{
		Model: e.cfg.Model,
		Messages: []chatMessage{
			{Role: "system", Content: system},
			{Role: "user", Content: user},
		},
	})
	if err != nil {
		return "", fmt.Errorf("marshal summary request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.cfg.Endpoint, bytes.NewReader(reqBody))
	if err != nil {
		return "", fmt.Errorf("build summary request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if e.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+e.cfg.APIKey)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("llm summary request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("llm summary http %d: %s", resp.StatusCode, string(body))
	}
	var out chatResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("decode summary response: %w", err)
	}
	if len(out.Choices) == 0 {
		return "", fmt.Errorf("llm returned no choices")
	}
	summary := strings.TrimSpace(out.Choices[0].Message.Content)
	if summary == "" {
		return "", fmt.Errorf("llm returned empty summary")
	}
	return summary, nil
}

// RefineSummary 在已有摘要基础上结合新增消息做增量更新。
func (e *LLMExtractor) RefineSummary(ctx context.Context, prevSummary string, newMessages []*models.HistoryMessage) (string, error) {
	if !e.cfg.Enabled {
		return "", fmt.Errorf("llm disabled")
	}
	if len(newMessages) == 0 {
		return "", fmt.Errorf("no new messages to refine")
	}
	msgs := newMessages
	if len(msgs) > e.cfg.MaxEvents {
		msgs = msgs[len(msgs)-e.cfg.MaxEvents:]
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf(refinePrompt, prevSummary))
	for i, m := range msgs {
		role := m.Role
		if role == "" {
			role = "unknown"
		}
		b.WriteString(fmt.Sprintf("[%d] %s: %s\n", i+1, role, m.Content))
	}
	return e.llmSummarize(ctx, "你是专业的对话总结助手，只对已有摘要做增量更新并输出完整新摘要。", b.String())
}

// SummaryRefineFallback 退化增量更新：把旧摘要与新消息拼接。
func (e *LLMExtractor) SummaryRefineFallback(_ context.Context, prevSummary string, newMessages []*models.HistoryMessage) string {
	if prevSummary == "" {
		return e.SummaryFallback(context.Background(), newMessages)
	}
	if len(newMessages) == 0 {
		return prevSummary
	}
	var b strings.Builder
	b.WriteString(prevSummary)
	b.WriteString("\n\n（新增消息，未启用智能总结）\n")
	for i, m := range newMessages {
		role := m.Role
		if role == "" {
			role = "unknown"
		}
		b.WriteString(fmt.Sprintf("[%d] %s: %s\n", i+1, role, truncate(m.Content, 120)))
	}
	return strings.TrimSpace(b.String())
}

// SummaryFallback 退化摘要：拼接首尾若干条消息，不依赖 LLM。
func (e *LLMExtractor) SummaryFallback(_ context.Context, messages []*models.HistoryMessage) string {
	if len(messages) == 0 {
		return "（空会话）"
	}
	const keep = 6
	head := messages
	if len(head) > keep {
		head = head[:keep]
	}
	tail := messages
	if len(tail) > keep {
		tail = tail[len(tail)-keep:]
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf("（共 %d 条消息，未启用智能总结）\n", len(messages)))
	for i, m := range head {
		b.WriteString(fmt.Sprintf("[%d] %s: %s\n", i+1, m.Role, truncate(m.Content, 120)))
	}
	if len(messages) > keep*2 {
		b.WriteString("...\n")
		start := len(messages) - len(tail)
		for i, m := range tail {
			b.WriteString(fmt.Sprintf("[%d] %s: %s\n", start+i+1, m.Role, truncate(m.Content, 120)))
		}
	}
	return strings.TrimSpace(b.String())
}

// SummarizeWithFallback 先尝试 LLM 总结，失败或超时自动降级。
func SummarizeWithFallback(ctx context.Context, s SessionSummarizer, messages []*models.HistoryMessage) (string, string) {
	summary, err := s.Summarize(ctx, messages)
	if err == nil && summary != "" {
		metrics.MemoryExtractTotal.WithLabelValues("session_llm").Inc()
		return summary, "llm"
	}
	if err != nil {
		logger.FromContext(ctx).WithError(err).Warn("session summary failed, falling back")
	}
	fallback := s.SummaryFallback(ctx, messages)
	metrics.MemoryExtractTotal.WithLabelValues("session_fallback").Inc()
	return fallback, "fallback"
}

// RefineSummaryWithFallback 先尝试 LLM 增量更新，失败或超时自动降级。
func RefineSummaryWithFallback(ctx context.Context, s SessionSummarizer, prevSummary string, newMessages []*models.HistoryMessage) (string, string) {
	summary, err := s.RefineSummary(ctx, prevSummary, newMessages)
	if err == nil && summary != "" {
		metrics.MemoryExtractTotal.WithLabelValues("session_llm").Inc()
		return summary, "llm"
	}
	if err != nil {
		logger.FromContext(ctx).WithError(err).Warn("session summary refine failed, falling back")
	}
	fallback := s.SummaryRefineFallback(ctx, prevSummary, newMessages)
	metrics.MemoryExtractTotal.WithLabelValues("session_fallback").Inc()
	return fallback, "fallback"
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}

var _ SessionSummarizer = (*LLMExtractor)(nil)
