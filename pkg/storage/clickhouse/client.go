// Package clickhouse 提供 ClickHouse 连接、Schema 初始化与批量写入实现。
package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/mnemosyne-labs/mneme/pkg/logger"
	"github.com/mnemosyne-labs/mneme/pkg/storage"
)

// Config ClickHouse 连接配置。
type Config struct {
	Hosts         []string
	Database      string
	Username      string
	Password      string
	MaxOpenConns  int
	MaxIdleConns  int
	DialTimeout   time.Duration
	BatchSize     int
	MaxBatchBytes int // 批次最大字节数，防 OOM
	FlushInterval time.Duration
	MaxRetries    int
	AutoMigrate   bool

	// 自适应批次控制
	AdaptiveBatchEnabled bool
	MinBatchSize         int
	MaxBatchSize         int
	TargetFlushLatency   time.Duration // 目标刷新延迟
}

func (c Config) withDefaults() Config {
	if c.MaxOpenConns <= 0 {
		c.MaxOpenConns = 10
	}
	if c.MaxIdleConns <= 0 {
		c.MaxIdleConns = 5
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 10 * time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 10000
	}
	if c.MaxBatchBytes <= 0 {
		c.MaxBatchBytes = 10 * 1024 * 1024 // 10MB
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = 5 * time.Second
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = 5
	}
	if c.Database == "" {
		c.Database = "mnemosyne"
	}
	if c.AdaptiveBatchEnabled == false {
		c.AdaptiveBatchEnabled = true
	}
	if c.MinBatchSize <= 0 {
		c.MinBatchSize = 1000
	}
	if c.MaxBatchSize <= 0 {
		c.MaxBatchSize = 100000
	}
	if c.TargetFlushLatency <= 0 {
		c.TargetFlushLatency = 100 * time.Millisecond
	}
	return c
}

// Client 封装 ClickHouse 连接。
type Client struct {
	conn driver.Conn
	cfg  Config
}

// NewClient 建立 ClickHouse 连接（不含建库）。
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	cfg = cfg.withDefaults()

	// 先以空 database 连接用于建库。
	if cfg.AutoMigrate {
		bootstrap, err := open(cfg, "")
		if err != nil {
			return nil, err
		}
		if err := bootstrap.Exec(ctx, storage.DatabaseDDL(cfg.Database)); err != nil {
			_ = bootstrap.Close()
			return nil, fmt.Errorf("clickhouse: create database: %w", err)
		}
		_ = bootstrap.Close()
	}

	conn, err := open(cfg, cfg.Database)
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("clickhouse: ping: %w", err)
	}
	return &Client{conn: conn, cfg: cfg}, nil
}

func open(cfg Config, database string) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: cfg.Hosts,
		Auth: clickhouse.Auth{
			Database: database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout:     cfg.DialTimeout,
		MaxOpenConns:    cfg.MaxOpenConns,
		MaxIdleConns:    cfg.MaxIdleConns,
		ConnMaxLifetime: time.Hour,
		Compression:     &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse: open: %w", err)
	}
	return conn, nil
}

// Conn 返回底层连接。
func (c *Client) Conn() driver.Conn { return c.conn }

// Config 返回生效的配置。
func (c *Client) Config() Config { return c.cfg }

// Migrate 幂等执行建表语句。
func (c *Client) Migrate(ctx context.Context) error {
	for _, stmt := range storage.SchemaStatements() {
		if err := c.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("clickhouse: migrate: %w", err)
		}
	}
	logger.L().Info("clickhouse schema ensured")
	return nil
}

// Close 关闭连接。
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
