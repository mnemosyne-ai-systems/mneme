// Command file-adapter 监听本地目录，把 .md/.jsonl/.txt 文件解析为
// HistoryMessage 并投递到 mneme，是 Mneme 的独立文件采集器。
// 默认通过 HTTP 推送到服务端批量接口（与消息总线拓扑解耦），
// 高吞吐可信环境可切换 pulsar 模式直连总线。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/mnemosyne/mneme/internal/config"
	"github.com/mnemosyne/mneme/internal/filewatcher"
	"github.com/mnemosyne/mneme/internal/service"
	"github.com/mnemosyne/mneme/pkg/logger"
	"github.com/mnemosyne/mneme/pkg/messagebus"
)

var version = "dev"

func main() {
	configPath := flag.String("config", "", "path to config file")
	dir := flag.String("dir", "", "override watch directory")
	scan := flag.Bool("scan", true, "scan existing files on start")
	mode := flag.String("mode", "", "delivery mode: http (default) or pulsar, overrides adapter.mode")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Init("info", "json")
		logger.L().WithError(err).Fatal("failed to load config")
	}
	logger.Init(cfg.Log.Level, cfg.Log.Format)
	logger.L().WithField("version", version).Info("starting mneme file-adapter")

	dirs := cfg.FileWatcher.Dirs
	if *dir != "" {
		dirs = []string{*dir}
	}
	if len(dirs) == 0 {
		logger.L().Fatal("no watch directory configured (set filewatcher.dirs or --dir)")
	}

	deliveryMode := cfg.Adapter.Mode
	if *mode != "" {
		deliveryMode = *mode
	}
	sink, cleanup, err := buildSink(cfg, deliveryMode)
	if err != nil {
		logger.L().WithError(err).Fatal("failed to create history sink")
	}
	defer cleanup()

	watcher, err := filewatcher.New(filewatcher.Config{
		Dirs:        dirs,
		Extensions:  cfg.FileWatcher.Extensions,
		Debounce:    cfg.FileWatcher.Debounce,
		StateFile:   cfg.FileWatcher.StateFile,
		TenantID:    cfg.FileWatcher.TenantID,
		ScanOnStart: *scan,
	}, sink)
	if err != nil {
		logger.L().WithError(err).Fatal("failed to create file watcher")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := watcher.Start(ctx); err != nil {
		logger.L().WithError(err).Fatal("failed to start file watcher")
	}

	<-ctx.Done()
	if err := watcher.Stop(); err != nil {
		logger.L().WithError(err).Error("file watcher stop error")
		os.Exit(1)
	}
}

// buildSink 根据模式构造投递通道，返回的 cleanup 在进程退出前调用。
func buildSink(cfg *config.Config, mode string) (sink service.HistorySink, cleanup func(), err error) {
	cleanup = func() {}
	switch mode {
	case "http":
		httpCfg := cfg.Adapter.HTTP
		sink = service.NewHTTPHistorySink(service.HTTPSinkConfig{
			Endpoint:     httpCfg.Endpoint,
			Token:        httpCfg.Token,
			Timeout:      httpCfg.Timeout,
			MaxRetries:   httpCfg.MaxRetries,
			BackoffBase:  httpCfg.BackoffBase,
			MaxBatchSize: httpCfg.MaxBatchSize,
		})
		logger.L().WithField("endpoint", httpCfg.Endpoint).Info("delivery via http api")
	case "pulsar":
		producer, perr := messagebus.NewProducer(messagebus.Config{
			URL:               cfg.Pulsar.URL,
			Tenant:            cfg.Pulsar.Tenant,
			Namespace:         cfg.Pulsar.Namespace,
			Token:             cfg.Pulsar.Token,
			OperationTimeout:  cfg.Pulsar.OperationTimeout,
			ConnectionTimeout: cfg.Pulsar.ConnectionTimeout,
		})
		if perr != nil {
			return nil, cleanup, perr
		}
		sink = service.NewIngestService(producer, service.Topics{History: cfg.Pulsar.Topics.History})
		cleanup = func() { _ = producer.Close() }
		logger.L().WithField("url", cfg.Pulsar.URL).Info("delivery via pulsar")
	default:
		return nil, cleanup, fmt.Errorf("unknown delivery mode %q (want http or pulsar)", mode)
	}
	return sink, cleanup, nil
}
