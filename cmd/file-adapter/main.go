// Command file-adapter 监听本地目录，把 .md/.jsonl/.txt 文件解析为
// HistoryMessage 并发送到 Pulsar，是 Mneme 的独立文件驱动接入端。
package main

import (
	"context"
	"flag"
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

	producer, err := messagebus.NewProducer(messagebus.Config{
		URL:               cfg.Pulsar.URL,
		Tenant:            cfg.Pulsar.Tenant,
		Namespace:         cfg.Pulsar.Namespace,
		Token:             cfg.Pulsar.Token,
		OperationTimeout:  cfg.Pulsar.OperationTimeout,
		ConnectionTimeout: cfg.Pulsar.ConnectionTimeout,
	})
	if err != nil {
		logger.L().WithError(err).Fatal("failed to connect pulsar")
	}
	defer func() { _ = producer.Close() }()

	ingest := service.NewIngestService(producer, service.Topics{
		History: cfg.Pulsar.Topics.History,
		Traces:  cfg.Pulsar.Topics.Traces,
		Memory:  cfg.Pulsar.Topics.Memory,
	})

	watcher, err := filewatcher.New(filewatcher.Config{
		Dirs:        dirs,
		Extensions:  cfg.FileWatcher.Extensions,
		Debounce:    cfg.FileWatcher.Debounce,
		StateFile:   cfg.FileWatcher.StateFile,
		TenantID:    cfg.FileWatcher.TenantID,
		ScanOnStart: *scan,
	}, ingest)
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
