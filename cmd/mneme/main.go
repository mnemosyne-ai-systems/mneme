// Command mneme 是 Mnemosyne 记忆存储服务的入口。
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mnemosyne/mneme/internal/app"
	"github.com/mnemosyne/mneme/internal/config"
	"github.com/mnemosyne/mneme/pkg/logger"
)

// version 由构建时通过 -ldflags 注入。
var version = "dev"

func main() {
	configPath := flag.String("config", "", "path to config file (default: ./configs/config.yaml)")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		os.Stdout.WriteString("mneme " + version + "\n")
		return
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Init("info", "json")
		logger.L().WithError(err).Fatal("failed to load config")
	}
	logger.Init(cfg.Log.Level, cfg.Log.Format)
	logger.L().WithField("version", version).Info("starting mneme")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	initCtx, cancelInit := context.WithTimeout(ctx, 60*time.Second)
	application, err := app.New(initCtx, cfg)
	cancelInit()
	if err != nil {
		logger.L().WithError(err).Fatal("failed to initialize mneme")
	}

	if err := application.Run(ctx); err != nil {
		logger.L().WithError(err).Error("mneme runtime error")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel()
	if err := application.Shutdown(shutdownCtx); err != nil {
		logger.L().WithError(err).Error("shutdown error")
		os.Exit(1)
	}
}
