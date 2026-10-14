package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/mnemosyne-labs/mneme/pkg/logger"
	"github.com/mnemosyne-labs/mneme/pkg/metrics"
	"github.com/mnemosyne-labs/mneme/pkg/models"
)

// HTTPSinkConfig HTTPHistorySink 配置。
type HTTPSinkConfig struct {
	// Endpoint 服务端批量接入地址（POST /api/v1/history/batch）。
	Endpoint string
	// Token 可选的 Bearer 鉴权令牌。
	Token string
	// Timeout 单次请求超时。
	Timeout time.Duration
	// MaxRetries 可重试失败后的最大重试次数。
	MaxRetries int
	// BackoffBase 指数退避基数，第 n 次重试等待约 BackoffBase * 2^(n-1)，含随机抖动。
	BackoffBase time.Duration
	// MaxBatchSize 单次请求携带的最大消息数，超出自动分片。
	MaxBatchSize int
}

func (c HTTPSinkConfig) withDefaults() HTTPSinkConfig {
	if c.Timeout <= 0 {
		c.Timeout = 10 * time.Second
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = 5
	}
	if c.BackoffBase <= 0 {
		c.BackoffBase = 500 * time.Millisecond
	}
	if c.MaxBatchSize <= 0 {
		c.MaxBatchSize = 500
	}
	return c
}

// HTTPHistorySink 通过服务端批量接口投递历史消息，
// 是文件采集器等边缘接入方的默认通道：只依赖 mneme 的 HTTP API，
// 无需感知消息总线拓扑。投递为同步语义——返回 nil 表示服务端已受理。
type HTTPHistorySink struct {
	cfg    HTTPSinkConfig
	client *http.Client
}

// NewHTTPHistorySink 创建 HTTP 投递 sink。
func NewHTTPHistorySink(cfg HTTPSinkConfig) *HTTPHistorySink {
	cfg = cfg.withDefaults()
	return &HTTPHistorySink{cfg: cfg, client: &http.Client{Timeout: cfg.Timeout}}
}

// httpStatusError 表示服务端返回的非 2xx 状态码。
type httpStatusError struct {
	Status int
	Body   string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("unexpected status %d: %s", e.Status, e.Body)
}

// retryable 判断是否值得重试：4xx 是请求本身的问题不重试；5xx 可重试。
func (e *httpStatusError) retryable() bool { return e.Status >= http.StatusInternalServerError }

// PublishHistoryBatch 校验并同步投递一批消息：先分片再逐片 POST，
// 失败时按指数退避重试。返回错误表示该批消息未送达；
// 由于 MessageID 幂等（文件采集器派生自内容哈希），调用方可安全整批重试。
func (s *HTTPHistorySink) PublishHistoryBatch(ctx context.Context, msgs []*models.HistoryMessage) error {
	if len(msgs) == 0 {
		return nil
	}
	for _, m := range msgs {
		m.Normalize()
		if err := m.Validate(); err != nil {
			return err
		}
	}
	for start := 0; start < len(msgs); start += s.cfg.MaxBatchSize {
		end := min(start+s.cfg.MaxBatchSize, len(msgs))
		if err := s.post(ctx, msgs[start:end]); err != nil {
			return fmt.Errorf("http sink: %w", err)
		}
	}
	return nil
}

// post 投递单个分片，带指数退避重试。
func (s *HTTPHistorySink) post(ctx context.Context, msgs []*models.HistoryMessage) error {
	payload, err := json.Marshal(historyBatchPayload{Messages: msgs})
	if err != nil {
		return fmt.Errorf("marshal %d messages: %w", len(msgs), err)
	}

	var lastErr error
	for attempt := 0; attempt <= s.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := s.backoff(attempt)
			logger.FromContext(ctx).WithError(lastErr).
				WithField("attempt", attempt).WithField("delay", delay).
				Warn("history batch post failed, retrying")
			select {
			case <-ctx.Done():
				return fmt.Errorf("post %d messages: %w", len(msgs), ctx.Err())
			case <-time.After(delay):
			}
		}
		err := s.doOnce(ctx, payload)
		if err == nil {
			metrics.AdapterPublishTotal.WithLabelValues("ok").Add(float64(len(msgs)))
			return nil
		}
		lastErr = err
		var statusErr *httpStatusError
		if errors.As(err, &statusErr) && !statusErr.retryable() {
			break
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			break
		}
	}
	metrics.AdapterPublishTotal.WithLabelValues("error").Add(float64(len(msgs)))
	return fmt.Errorf("post %d messages: %w", len(msgs), lastErr)
}

func (s *HTTPHistorySink) doOnce(ctx context.Context, payload []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.Endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if s.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.cfg.Token)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return &httpStatusError{Status: resp.StatusCode, Body: strings.TrimSpace(string(body))}
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// backoff 计算第 attempt 次重试的等待时长：基数指数增长并加抖动，上限 30s。
func (s *HTTPHistorySink) backoff(attempt int) time.Duration {
	const (
		maxShift = 6 // BackoffBase * 64 已达秒级，避免移位溢出
		maxDelay = 30 * time.Second
	)
	shift := attempt - 1
	if shift > maxShift {
		shift = maxShift
	}
	d := s.cfg.BackoffBase << uint(shift)
	if d > maxDelay {
		d = maxDelay
	}
	jitter := time.Duration(rand.Int64N(int64(d / 2)))
	return d/2 + jitter
}
