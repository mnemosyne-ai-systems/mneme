// Package app 负责组装 Mneme 各组件并管理生命周期。
package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mnemosyne-labs/mneme/internal/api"
	"github.com/mnemosyne-labs/mneme/internal/archiver"
	"github.com/mnemosyne-labs/mneme/internal/config"
	"github.com/mnemosyne-labs/mneme/internal/consumer"
	"github.com/mnemosyne-labs/mneme/internal/filewatcher"
	"github.com/mnemosyne-labs/mneme/internal/service"
	"github.com/mnemosyne-labs/mneme/pkg/cache"
	"github.com/mnemosyne-labs/mneme/pkg/logger"
	"github.com/mnemosyne-labs/mneme/pkg/messagebus"
	chstore "github.com/mnemosyne-labs/mneme/pkg/storage/clickhouse"
	"github.com/mnemosyne-labs/mneme/pkg/tracing"
)

// App 持有 Mneme 运行期的全部组件。
type App struct {
	cfg *config.Config

	server   *http.Server
	producer messagebus.Producer
	chClient *chstore.Client
	inserter *chstore.BatchInserter
	redis    *redis.Client

	historyConsumer *consumer.HistoryConsumer
	tracesConsumer  *consumer.TracesConsumer
	memoryConsumer  *consumer.MemoryConsumer
	profileConsumer *consumer.HistoryProfileConsumer
	sessionConsumer *consumer.SessionSummaryConsumer
	sessionStore    service.SessionSummaryStore
	adminHandler    *api.AdminHandler
	archiveJob      *archiver.Job
	fileWatcher     *filewatcher.Watcher

	shutdownTracing tracing.ShutdownFunc
}

// New 根据配置构建应用。
func New(ctx context.Context, cfg *config.Config) (*App, error) {
	app := &App{cfg: cfg}

	shutdownTracing, err := tracing.Init(ctx, tracing.Config{
		Enabled:      cfg.Telemetry.TracingEnabled,
		ServiceName:  cfg.Telemetry.ServiceName,
		OTLPEndpoint: cfg.Telemetry.OTLPEndpoint,
		SampleRatio:  cfg.Telemetry.SampleRatio,
	})
	if err != nil {
		return nil, err
	}
	app.shutdownTracing = shutdownTracing

	// ClickHouse
	chClient, err := chstore.NewClient(ctx, chstore.Config{
		Hosts:         cfg.ClickHouse.Hosts,
		Database:      cfg.ClickHouse.Database,
		Username:      cfg.ClickHouse.Username,
		Password:      cfg.ClickHouse.Password,
		MaxOpenConns:  cfg.ClickHouse.MaxOpenConns,
		MaxIdleConns:  cfg.ClickHouse.MaxIdleConns,
		DialTimeout:   cfg.ClickHouse.DialTimeout,
		BatchSize:     cfg.ClickHouse.BatchSize,
		FlushInterval: cfg.ClickHouse.FlushInterval,
		MaxRetries:    cfg.ClickHouse.MaxRetries,
		AutoMigrate:   cfg.ClickHouse.AutoMigrate,
	})
	if err != nil {
		app.cleanup(ctx)
		return nil, err
	}
	app.chClient = chClient
	if cfg.ClickHouse.AutoMigrate {
		if err := chClient.Migrate(ctx); err != nil {
			app.cleanup(ctx)
			return nil, err
		}
	}
	app.inserter = chstore.NewBatchInserter(chClient)

	// Redis
	redisClient, err := cache.NewRedisClient(ctx, cache.Config{
		Addr:        cfg.Redis.Addr,
		Password:    cfg.Redis.Password,
		DB:          cfg.Redis.DB,
		DialTimeout: cfg.Redis.DialTimeout,
		PoolSize:    cfg.Redis.PoolSize,
	})
	if err != nil {
		app.cleanup(ctx)
		return nil, err
	}
	app.redis = redisClient

	// Pulsar producer
	busCfg := messagebus.Config{
		URL:                 cfg.Pulsar.URL,
		Tenant:              cfg.Pulsar.Tenant,
		Namespace:           cfg.Pulsar.Namespace,
		Token:               cfg.Pulsar.Token,
		OperationTimeout:    cfg.Pulsar.OperationTimeout,
		ConnectionTimeout:   cfg.Pulsar.ConnectionTimeout,
		SubscriptionName:    cfg.Pulsar.SubscriptionName,
		MaxRedeliver:        10,
		NackRedeliveryDelay: time.Second,
	}
	producer, err := messagebus.NewProducer(busCfg)
	if err != nil {
		app.cleanup(ctx)
		return nil, err
	}
	app.producer = producer

	topics := service.Topics{
		History: cfg.Pulsar.Topics.History,
		Traces:  cfg.Pulsar.Topics.Traces,
		Memory:  cfg.Pulsar.Topics.Memory,
	}
	ingest := service.NewIngestService(producer, topics)
	profileStore := service.NewRedisProfileStore(redisClient, app.inserter, cfg.Cache.ProfileTTL, cfg.Cache.UseZSetIndex)
	extractor := service.NewLLMExtractor(service.LLMConfig{
		Enabled:   cfg.Memory.LLM.Enabled,
		Endpoint:  cfg.Memory.LLM.Endpoint,
		Model:     cfg.Memory.LLM.Model,
		APIKey:    cfg.Memory.LLM.APIKey,
		Timeout:   cfg.Memory.LLM.Timeout,
		MaxEvents: cfg.Memory.LLM.MaxEvents,
	})

	// 消费者
	if cfg.Consumer.HistoryEnabled {
		c, err := messagebus.NewConsumer(busCfg)
		if err != nil {
			app.cleanup(ctx)
			return nil, err
		}
		bloom := cache.NewRedisBloomFilter(redisClient, cache.BloomOptions{
			Key: cache.BloomKey("history", "global"),
		})
		app.historyConsumer = consumer.NewHistoryConsumer(c, app.inserter, bloom, consumer.HistoryConsumerOptions{
			Topic:         topics.History,
			BatchSize:     cfg.ClickHouse.BatchSize,
			FlushInterval: cfg.ClickHouse.FlushInterval,
		})
	}
	if cfg.Consumer.TracesEnabled {
		c, err := messagebus.NewConsumer(busCfg)
		if err != nil {
			app.cleanup(ctx)
			return nil, err
		}
		app.tracesConsumer = consumer.NewTracesConsumer(c, app.inserter, consumer.TracesConsumerOptions{
			Topic:            topics.Traces,
			AggregateTimeout: cfg.Consumer.TraceAggregateTimeout,
		})
	}
	if cfg.Consumer.MemoryEnabled {
		c, err := messagebus.NewConsumer(busCfg)
		if err != nil {
			app.cleanup(ctx)
			return nil, err
		}
		app.memoryConsumer = consumer.NewMemoryConsumer(c, extractor, profileStore, consumer.MemoryConsumerOptions{
			Topic:           topics.Memory,
			ExtractInterval: cfg.Consumer.MemoryExtractInterval,
		})
	}

	// 从对话历史中萃取用户画像：复用同一个 extractor 与 profileStore，
	// 让显式记忆事件与对话内容汇聚到同一张 entity_memory 表。
	if cfg.Consumer.HistoryProfileEnabled {
		c, err := messagebus.NewConsumer(busCfg)
		if err != nil {
			app.cleanup(ctx)
			return nil, err
		}
		app.profileConsumer = consumer.NewHistoryProfileConsumer(c, extractor, profileStore, consumer.HistoryProfileConsumerOptions{
			Topic:              topics.History,
			ExtractInterval:    cfg.Consumer.HistoryProfileInterval,
			MaxMessagesPerUser: cfg.Consumer.HistoryProfileMaxMessages,
		})
	}

	// 会话级总结：按 session_id 缓冲，定时生成"本次对话聊了什么"的自然语言概括，
	// 与跨会话的用户画像互补。
	if cfg.Consumer.SessionSummaryEnabled {
		c, err := messagebus.NewConsumer(busCfg)
		if err != nil {
			app.cleanup(ctx)
			return nil, err
		}
		app.sessionStore = service.NewRedisSessionSummaryStore(redisClient, app.inserter, cfg.Cache.SessionTTL, cfg.Cache.UseZSetIndex)
		app.sessionConsumer = consumer.NewSessionSummaryConsumer(c, extractor, app.sessionStore, consumer.SessionSummaryConsumerOptions{
			Topic:                 topics.History,
			SummarizeInterval:     cfg.Consumer.SessionSummaryInterval,
			MaxMessagesPerSession: cfg.Consumer.SessionSummaryMaxMessages,
		})
	}

	// 文件监听适配器
	if cfg.FileWatcher.Enabled {
		fw, err := filewatcher.New(filewatcher.Config{
			Dirs:        cfg.FileWatcher.Dirs,
			Extensions:  cfg.FileWatcher.Extensions,
			Debounce:    cfg.FileWatcher.Debounce,
			StateFile:   cfg.FileWatcher.StateFile,
			TenantID:    cfg.FileWatcher.TenantID,
			ScanOnStart: true,
		}, ingest)
		if err != nil {
			app.cleanup(ctx)
			return nil, err
		}
		app.fileWatcher = fw
	}

	// 归档任务
	if cfg.Archiver.Enabled {
		app.archiveJob = archiver.NewJob(chClient.Conn(), archiver.Config{
			CronHour:     cfg.Archiver.CronHour,
			RetainMonths: cfg.Archiver.RetainMonths,
			Tables:       cfg.Archiver.Tables,
			ExportDir:    cfg.Archiver.ExportDir,
			HTTPAddr:     deriveClickHouseHTTPAddr(cfg.Archiver.HTTPAddr, cfg.ClickHouse.Hosts),
			Username:     cfg.ClickHouse.Username,
			Password:     cfg.ClickHouse.Password,
			OSS: archiver.OSSConfig{
				Endpoint:  cfg.Archiver.OSS.Endpoint,
				Region:    cfg.Archiver.OSS.Region,
				Bucket:    cfg.Archiver.OSS.Bucket,
				AccessKey: cfg.Archiver.OSS.AccessKey,
				SecretKey: cfg.Archiver.OSS.SecretKey,
				Prefix:    cfg.Archiver.OSS.Prefix,
			},
		})
	}

	// HTTP
	routerOpts := api.RouterOptions{
		History:        api.NewHistoryHandler(ingest, profileStore, app.sessionStore),
		Traces:         api.NewTracesHandler(ingest),
		Memory:         api.NewMemoryHandler(ingest, profileStore),
		Sessions:       api.NewSessionsHandler(app.sessionStore, app.sessionStore),
		Users:          api.NewUsersHandler(profileStore, app.sessionStore),
		OpenAPI:        api.NewOpenAPIHandler(),
		Health:         api.NewHealthHandler(app.healthCheckers()...),
		MetricsEnabled: cfg.Telemetry.MetricsEnabled,
		TracingEnabled: cfg.Telemetry.TracingEnabled,
	}

	// 后台管理界面（可选，默认关闭）：用户/画像浏览、会话检索、数据治理（软删除 + 审计）。
	if cfg.Admin.Enabled {
		var auditStore service.AuditStore
		if app.inserter != nil {
			auditStore = service.NewClickHouseAuditStore(app.inserter)
		} else {
			auditStore = service.NewMemoryAuditStore()
		}
		app.adminHandler = api.NewAdminHandler(profileStore, app.sessionStore, auditStore, cfg.Admin.Token, app.sessionConsumer, app.inserter)
		routerOpts.Admin = app.adminHandler
		routerOpts.AdminUI = api.NewAdminUIHandler(cfg.Admin.PageTitle)
	}

	handler := api.NewRouter(routerOpts)
	app.server = &http.Server{
		Addr:         cfg.Server.Addr(),
		Handler:      handler,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}
	return app, nil
}

func (a *App) healthCheckers() []api.HealthChecker {
	var checkers []api.HealthChecker
	if a.chClient != nil {
		checkers = append(checkers, api.CheckerFunc{
			CheckName: "clickhouse",
			Fn:        func(ctx context.Context) error { return a.chClient.Conn().Ping(ctx) },
		})
	}
	if a.redis != nil {
		checkers = append(checkers, api.CheckerFunc{
			CheckName: "redis",
			Fn:        func(ctx context.Context) error { return a.redis.Ping(ctx).Err() },
		})
	}
	return checkers
}

// Run 启动消费者与 HTTP 服务，阻塞直到 ctx 结束。
func (a *App) Run(ctx context.Context) error {
	if a.historyConsumer != nil {
		if err := a.historyConsumer.Start(ctx); err != nil {
			return err
		}
	}
	if a.tracesConsumer != nil {
		if err := a.tracesConsumer.Start(ctx); err != nil {
			return err
		}
	}
	if a.memoryConsumer != nil {
		if err := a.memoryConsumer.Start(ctx); err != nil {
			return err
		}
	}
	if a.profileConsumer != nil {
		if err := a.profileConsumer.Start(ctx); err != nil {
			return err
		}
	}
	if a.sessionConsumer != nil {
		if err := a.sessionConsumer.Start(ctx); err != nil {
			return err
		}
	}
	if a.fileWatcher != nil {
		if err := a.fileWatcher.Start(ctx); err != nil {
			return err
		}
	}
	if a.archiveJob != nil {
		a.archiveJob.Start(ctx)
	}

	errCh := make(chan error, 1)
	go func() {
		logger.L().WithField("addr", a.cfg.Server.Addr()).Info("mneme http server listening")
		if err := a.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

// Shutdown 优雅关闭所有组件。
func (a *App) Shutdown(ctx context.Context) error {
	logger.L().Info("shutting down mneme")

	if a.server != nil {
		if err := a.server.Shutdown(ctx); err != nil {
			logger.L().WithError(err).Warn("http server shutdown error")
		}
	}
	if a.fileWatcher != nil {
		if err := a.fileWatcher.Stop(); err != nil {
			logger.L().WithError(err).Warn("file watcher stop error")
		}
	}
	if a.archiveJob != nil {
		a.archiveJob.Stop()
	}
	// 消费者先落盘再退出
	if a.historyConsumer != nil {
		if err := a.historyConsumer.Stop(ctx); err != nil {
			logger.L().WithError(err).Warn("history consumer stop error")
		}
	}
	if a.tracesConsumer != nil {
		if err := a.tracesConsumer.Stop(ctx); err != nil {
			logger.L().WithError(err).Warn("traces consumer stop error")
		}
	}
	if a.memoryConsumer != nil {
		if err := a.memoryConsumer.Stop(ctx); err != nil {
			logger.L().WithError(err).Warn("memory consumer stop error")
		}
	}
	if a.profileConsumer != nil {
		if err := a.profileConsumer.Stop(ctx); err != nil {
			logger.L().WithError(err).Warn("history profile consumer stop error")
		}
	}
	if a.sessionConsumer != nil {
		if err := a.sessionConsumer.Stop(ctx); err != nil {
			logger.L().WithError(err).Warn("session summary consumer stop error")
		}
	}
	a.cleanup(ctx)
	logger.L().Info("mneme stopped")
	return nil
}

func (a *App) cleanup(ctx context.Context) {
	if a.producer != nil {
		_ = a.producer.Close()
		a.producer = nil
	}
	if a.inserter != nil {
		if err := a.inserter.Close(); err != nil {
			logger.L().WithError(err).Warn("clickhouse inserter close error")
		}
		a.inserter = nil
	}
	if a.chClient != nil {
		_ = a.chClient.Close()
		a.chClient = nil
	}
	if a.redis != nil {
		_ = a.redis.Close()
		a.redis = nil
	}
	if a.shutdownTracing != nil {
		_ = a.shutdownTracing(ctx)
		a.shutdownTracing = nil
	}
}

// deriveClickHouseHTTPAddr 推导 ClickHouse HTTP 接口地址：
// 优先使用显式配置；否则把第一个 native 地址的端口替换为 8123。
func deriveClickHouseHTTPAddr(configured string, hosts []string) string {
	if configured != "" {
		return configured
	}
	if len(hosts) == 0 {
		return ""
	}
	host, _, err := net.SplitHostPort(hosts[0])
	if err != nil {
		// 无端口形式，整体当作主机名
		host = hosts[0]
	}
	return "http://" + net.JoinHostPort(host, "8123")
}
