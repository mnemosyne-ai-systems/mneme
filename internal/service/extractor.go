package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mnemosyne/mneme/pkg/logger"
	"github.com/mnemosyne/mneme/pkg/metrics"
	"github.com/mnemosyne/mneme/pkg/models"
)

// MemoryExtractor 把一批记忆事件萃取为结构化画像。
type MemoryExtractor interface {
	Extract(ctx context.Context, events []*models.MemoryEvent) (map[string]string, error)
	// Fallback 在 LLM 不可用时至少保留原始事件摘要。
	Fallback(ctx context.Context, events []*models.MemoryEvent) (map[string]string, error)
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

const extractPrompt = `你是一个用户画像萃取器。请阅读以下 Agent 记忆事件，
输出一个扁平的 JSON 对象（键值均为字符串），概括该实体的稳定偏好、属性与近期关注点。
只输出 JSON，不要输出任何解释。

事件列表：
`

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
	var first, last time.Time
	for i, e := range events {
		counts[e.EventType]++
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
