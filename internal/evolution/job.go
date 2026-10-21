// Package evolution 实现会话摘要的定时自我反思（B）与跨会话用户摘要演化（C）。
package evolution

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mnemosyne-ai-systems/mneme/internal/service"
	"github.com/mnemosyne-ai-systems/mneme/pkg/logger"
	"github.com/mnemosyne-ai-systems/mneme/pkg/metrics"
	"github.com/mnemosyne-ai-systems/mneme/pkg/models"
	"github.com/mnemosyne-ai-systems/mneme/pkg/storage"
)

// Config 自我进化任务配置。
type Config struct {
	// Interval 队列扫描周期，默认 1 小时。
	Interval time.Duration
	// MaxRounds 同一份摘要最多连续反思的轮次，默认 2。
	MaxRounds int
	// BatchSize 每轮最多处理的会话数，默认 10。
	BatchSize int
	// HistoryLimit 反思时读取的最近消息数，默认 200。
	HistoryLimit int
	// MaxUserSessions 跨会话整合时使用的最新会话摘要数，默认 10。
	MaxUserSessions int
	// MaxUsersPerRun 单轮最多做跨会话整合的用户数，默认 10（限制 LLM 调用量）。
	MaxUsersPerRun int
	// UserSummaryEnabled 是否把演化结果整合进用户画像的跨会话长期摘要。
	UserSummaryEnabled bool
}

func (c Config) withDefaults() Config {
	if c.Interval <= 0 {
		c.Interval = time.Hour
	}
	if c.MaxRounds <= 0 {
		c.MaxRounds = 2
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 10
	}
	if c.HistoryLimit <= 0 {
		c.HistoryLimit = 200
	}
	if c.MaxUserSessions <= 0 {
		c.MaxUserSessions = 10
	}
	if c.MaxUsersPerRun <= 0 {
		c.MaxUsersPerRun = 10
	}
	return c
}

// Job 定时消费进化队列：先对会话摘要做自我反思，再按用户整合跨会话长期摘要。
type Job struct {
	queue       service.EvolutionQueue
	sessions    service.SessionSummaryStore
	profiles    service.ProfileStore
	history     storage.BatchInserter
	reflector   service.SessionReflector
	synthesizer service.ProfileSynthesizer
	cfg         Config

	stopCh  chan struct{}
	stopped chan struct{}
	once    sync.Once
	mu      sync.Mutex
	started bool
}

// NewJob 创建自我进化任务。
func NewJob(
	queue service.EvolutionQueue,
	sessions service.SessionSummaryStore,
	profiles service.ProfileStore,
	history storage.BatchInserter,
	reflector service.SessionReflector,
	synthesizer service.ProfileSynthesizer,
	cfg Config,
) *Job {
	return &Job{
		queue:       queue,
		sessions:    sessions,
		profiles:    profiles,
		history:     history,
		reflector:   reflector,
		synthesizer: synthesizer,
		cfg:         cfg.withDefaults(),
		stopCh:      make(chan struct{}),
		stopped:     make(chan struct{}),
	}
}

// Start 启动定时进化任务。
func (j *Job) Start(ctx context.Context) {
	j.mu.Lock()
	if j.started {
		j.mu.Unlock()
		return
	}
	j.started = true
	j.mu.Unlock()
	go j.loop(ctx)
	logger.L().WithFields(map[string]interface{}{
		"interval":   j.cfg.Interval.String(),
		"max_rounds": j.cfg.MaxRounds,
		"batch_size": j.cfg.BatchSize,
	}).Info("summary evolution job started")
}

// Stop 停止任务。未启动时直接返回，避免等待永远不会关闭的 stopped。
func (j *Job) Stop() {
	j.once.Do(func() {
		j.mu.Lock()
		started := j.started
		j.mu.Unlock()
		close(j.stopCh)
		if started {
			<-j.stopped
		} else {
			close(j.stopped)
		}
		logger.L().Info("summary evolution job stopped")
	})
}

func (j *Job) loop(ctx context.Context) {
	defer close(j.stopped)
	ticker := time.NewTicker(j.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-j.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n, err := j.RunOnce(ctx); err != nil {
				logger.L().WithError(err).WithField("sessions", n).Error("scheduled summary evolution failed")
			}
		}
	}
}

type userRef struct {
	tenant string
	userID string
}

// RunOnce 执行一轮进化：领取到期会话，逐个反思，再按用户整合跨会话摘要。
func (j *Job) RunOnce(ctx context.Context) (int, error) {
	if j.sessions == nil || j.history == nil || j.queue == nil {
		return 0, fmt.Errorf("evolution job: missing dependencies")
	}
	candidates, err := j.queue.ClaimDue(ctx, time.Now().UTC(), j.cfg.BatchSize)
	if err != nil {
		metrics.EvolutionTotal.WithLabelValues("session", "error").Inc()
		return 0, err
	}

	var firstErr error
	processed := 0
	users := make(map[userRef]struct{})
	for _, candidate := range candidates {
		evolved, userID, rerr := j.reflectSession(ctx, candidate)
		if rerr != nil {
			metrics.EvolutionTotal.WithLabelValues("session", "error").Inc()
			if firstErr == nil {
				firstErr = rerr
			}
			j.retryLater(ctx, candidate)
			continue
		}
		if !evolved {
			metrics.EvolutionTotal.WithLabelValues("session", "skipped").Inc()
			continue
		}
		metrics.EvolutionTotal.WithLabelValues("session", "evolved").Inc()
		processed++
		if userID != "" && len(users) < j.cfg.MaxUsersPerRun {
			users[userRef{tenant: candidate.TenantID, userID: userID}] = struct{}{}
		}
	}

	if j.cfg.UserSummaryEnabled {
		for ref := range users {
			changed, serr := j.synthesizeUser(ctx, ref.tenant, ref.userID)
			if serr != nil {
				metrics.EvolutionTotal.WithLabelValues("user", "error").Inc()
				logger.L().WithError(serr).WithFields(map[string]interface{}{
					"tenant": ref.tenant,
					"user":   ref.userID,
				}).Warn("cross-session summary synthesis failed")
				if firstErr == nil {
					firstErr = serr
				}
				continue
			}
			if !changed {
				metrics.EvolutionTotal.WithLabelValues("user", "skipped").Inc()
				continue
			}
			metrics.EvolutionTotal.WithLabelValues("user", "evolved").Inc()
		}
	}
	return processed, firstErr
}

// reflectSession 对单个会话执行一次自我反思；返回摘要是否被改写。
func (j *Job) reflectSession(ctx context.Context, candidate service.EvolutionCandidate) (bool, string, error) {
	prev, err := j.sessions.GetSummary(ctx, candidate.TenantID, candidate.SessionID)
	if err != nil {
		return false, "", fmt.Errorf("load summary: %w", err)
	}
	if prev == nil || strings.TrimSpace(prev.Summary) == "" {
		// 摘要已删除或尚未生成：停止该会话的演化，等待下一次总结重新调度。
		_ = j.queue.Forget(ctx, candidate.TenantID, candidate.SessionID)
		return false, "", nil
	}

	messages, err := j.history.QueryRecentHistoryBySession(ctx, candidate.TenantID, candidate.SessionID, j.cfg.HistoryLimit)
	if err != nil {
		return false, "", fmt.Errorf("load history: %w", err)
	}
	if len(messages) < 2 {
		_ = j.queue.Forget(ctx, candidate.TenantID, candidate.SessionID)
		return false, "", nil
	}

	text, err := j.reflector.ReflectSummary(ctx, prev.Summary, messages)
	if err != nil {
		return false, "", fmt.Errorf("reflect summary: %w", err)
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return false, "", fmt.Errorf("reflect summary returned empty text")
	}
	if normalizeSummary(text) == normalizeSummary(prev.Summary) {
		// 已收敛：继续反思不会带来改进。
		_ = j.queue.Forget(ctx, candidate.TenantID, candidate.SessionID)
		return false, prev.UserID, nil
	}

	// LLM 调用期间摘要可能已被消费者更新：只在版本未变化时落库，避免覆盖新摘要。
	latest, err := j.sessions.GetSummary(ctx, candidate.TenantID, candidate.SessionID)
	if err != nil {
		return false, "", fmt.Errorf("reload summary: %w", err)
	}
	if latest == nil {
		_ = j.queue.Forget(ctx, candidate.TenantID, candidate.SessionID)
		return false, "", nil
	}
	if latest.Version != prev.Version {
		// 让更新后的摘要重新开启演化周期，本轮结果作废。
		if err := j.queue.Continue(ctx, candidate.TenantID, candidate.SessionID, time.Now().UTC().Add(j.cfg.Interval)); err != nil {
			logger.L().WithError(err).WithField("session", candidate.SessionID).Warn("reschedule evolution failed")
		}
		return false, prev.UserID, nil
	}

	evolved := *prev
	evolved.Summary = text
	if len(messages) > evolved.MessageCount {
		evolved.MessageCount = len(messages)
	}
	if evolved.StartedAt.IsZero() {
		evolved.StartedAt = messages[0].CreatedAt
	}
	if evolved.EndedAt.IsZero() {
		evolved.EndedAt = messages[len(messages)-1].CreatedAt
	}
	if _, err := j.sessions.SaveSummary(ctx, &evolved); err != nil {
		return false, "", fmt.Errorf("save evolved summary: %w", err)
	}

	round, err := j.queue.Finish(ctx, candidate.TenantID, candidate.SessionID)
	if err != nil {
		logger.L().WithError(err).WithField("session", candidate.SessionID).Warn("record evolution round failed")
		return true, prev.UserID, nil
	}
	if round < j.cfg.MaxRounds {
		// 续排下一轮：保留更早的到期时间，避免覆盖更紧急的新调度。
		if err := j.queue.Continue(ctx, candidate.TenantID, candidate.SessionID, time.Now().UTC().Add(j.cfg.Interval)); err != nil {
			logger.L().WithError(err).WithField("session", candidate.SessionID).Warn("reschedule evolution failed")
		}
	} else {
		_ = j.queue.Forget(ctx, candidate.TenantID, candidate.SessionID)
	}
	return true, prev.UserID, nil
}

// synthesizeUser 将用户画像与最近会话摘要整合为跨会话长期摘要。
// 返回是否真的写入了新的长期摘要（内容未变化时返回 false，不写库）。
func (j *Job) synthesizeUser(ctx context.Context, tenant, userID string) (bool, error) {
	profile, err := j.profiles.GetProfile(ctx, tenant, models.EntityTypeUser, userID)
	if err != nil {
		return false, fmt.Errorf("load profile: %w", err)
	}
	sessions, err := j.sessions.ListSessionsByUser(ctx, tenant, userID)
	if err != nil {
		return false, fmt.Errorf("list user sessions: %w", err)
	}

	// 取最近 max_user_sessions 场会话；启用 ZSET 索引时走每用户会话索引，避免全量 SCAN。
	recent := make([]*models.SessionSummary, 0, j.cfg.MaxUserSessions)
	for _, s := range sessions {
		if s == nil || strings.TrimSpace(s.Summary) == "" {
			continue
		}
		recent = append(recent, s)
		if len(recent) >= j.cfg.MaxUserSessions {
			break
		}
	}
	if len(recent) == 0 && len(profile) == 0 {
		return false, nil
	}

	text, err := j.synthesizer.SynthesizeProfile(ctx, profile, recent)
	if err != nil {
		return false, fmt.Errorf("synthesize profile: %w", err)
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return false, nil
	}
	if profile == nil {
		profile = make(map[string]string)
	}
	if profile[service.ProfileEvolvingSummaryKey] == text {
		return false, nil
	}

	merged := make(map[string]string, len(profile)+2)
	for k, v := range profile {
		if service.IsInternalProfileKey(k) {
			continue
		}
		merged[k] = v
	}
	merged[service.ProfileEvolvingSummaryKey] = text
	merged[service.ProfileEvolvingUpdatedKey] = time.Now().UTC().Format(time.RFC3339)
	if _, err := j.profiles.SaveProfile(ctx, tenant, models.EntityTypeUser, userID, merged); err != nil {
		return false, fmt.Errorf("save evolved profile: %w", err)
	}
	return true, nil
}

func (j *Job) retryLater(ctx context.Context, candidate service.EvolutionCandidate) {
	due := time.Now().UTC().Add(j.cfg.Interval)
	if err := j.queue.Reschedule(ctx, candidate.TenantID, candidate.SessionID, due); err != nil {
		logger.L().WithError(err).WithFields(map[string]interface{}{
			"tenant":  candidate.TenantID,
			"session": candidate.SessionID,
		}).Warn("reschedule failed evolution task")
	}
}

func normalizeSummary(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
