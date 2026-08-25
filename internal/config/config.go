// Package config 负责 Mneme 服务的配置加载（YAML 文件 + 环境变量覆盖）。
package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config 是 Mneme 的顶层配置。
type Config struct {
	Server      ServerConfig      `mapstructure:"server"`
	Pulsar      PulsarConfig      `mapstructure:"pulsar"`
	ClickHouse  ClickHouseConfig  `mapstructure:"clickhouse"`
	Redis       RedisConfig       `mapstructure:"redis"`
	Consumer    ConsumerConfig    `mapstructure:"consumer"`
	Memory      MemoryConfig      `mapstructure:"memory"`
	FileWatcher FileWatcherConfig `mapstructure:"filewatcher"`
	Archiver    ArchiverConfig    `mapstructure:"archiver"`
	Telemetry   TelemetryConfig   `mapstructure:"telemetry"`
	Log         LogConfig         `mapstructure:"log"`
}

// ServerConfig HTTP 服务配置。
type ServerConfig struct {
	Port            int           `mapstructure:"port"`
	ReadTimeout     time.Duration `mapstructure:"read_timeout"`
	WriteTimeout    time.Duration `mapstructure:"write_timeout"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
}

// Addr 返回监听地址。
func (s ServerConfig) Addr() string {
	return fmt.Sprintf(":%d", s.Port)
}

// PulsarTopics 三条管道对应的 Topic 名称。
type PulsarTopics struct {
	History string `mapstructure:"history"`
	Traces  string `mapstructure:"traces"`
	Memory  string `mapstructure:"memory"`
}

// PulsarConfig Pulsar 连接配置。
type PulsarConfig struct {
	URL               string        `mapstructure:"url"`
	Tenant            string        `mapstructure:"tenant"`
	Namespace         string        `mapstructure:"namespace"`
	Token             string        `mapstructure:"token"`
	OperationTimeout  time.Duration `mapstructure:"operation_timeout"`
	ConnectionTimeout time.Duration `mapstructure:"connection_timeout"`
	SubscriptionName  string        `mapstructure:"subscription_name"`
	Topics            PulsarTopics  `mapstructure:"topics"`
}

// ClickHouseConfig ClickHouse 连接与批量写入配置。
type ClickHouseConfig struct {
	Hosts         []string      `mapstructure:"hosts"`
	Database      string        `mapstructure:"database"`
	Username      string        `mapstructure:"username"`
	Password      string        `mapstructure:"password"`
	MaxOpenConns  int           `mapstructure:"max_open_conns"`
	MaxIdleConns  int           `mapstructure:"max_idle_conns"`
	DialTimeout   time.Duration `mapstructure:"dial_timeout"`
	BatchSize     int           `mapstructure:"batch_size"`
	FlushInterval time.Duration `mapstructure:"flush_interval"`
	MaxRetries    int           `mapstructure:"max_retries"`
	AutoMigrate   bool          `mapstructure:"auto_migrate"`
}

// RedisConfig Redis 连接配置。
type RedisConfig struct {
	Addr        string        `mapstructure:"addr"`
	Password    string        `mapstructure:"password"`
	DB          int           `mapstructure:"db"`
	DialTimeout time.Duration `mapstructure:"dial_timeout"`
	PoolSize    int           `mapstructure:"pool_size"`
}

// ConsumerConfig 消费者开关与窗口配置。
type ConsumerConfig struct {
	HistoryEnabled        bool          `mapstructure:"history_enabled"`
	TracesEnabled         bool          `mapstructure:"traces_enabled"`
	MemoryEnabled         bool          `mapstructure:"memory_enabled"`
	TraceAggregateTimeout time.Duration `mapstructure:"trace_aggregate_timeout"`
	MemoryExtractInterval time.Duration `mapstructure:"memory_extract_interval"`
}

// LLMConfig 长期记忆萃取所使用的 LLM 配置。
type LLMConfig struct {
	Enabled   bool          `mapstructure:"enabled"`
	Endpoint  string        `mapstructure:"endpoint"`
	Model     string        `mapstructure:"model"`
	APIKey    string        `mapstructure:"api_key"`
	Timeout   time.Duration `mapstructure:"timeout"`
	MaxEvents int           `mapstructure:"max_events"`
}

// MemoryConfig 管道三配置。
type MemoryConfig struct {
	LLM LLMConfig `mapstructure:"llm"`
}

// FileWatcherConfig 文件监听适配器配置。
type FileWatcherConfig struct {
	Enabled    bool          `mapstructure:"enabled"`
	Dirs       []string      `mapstructure:"dirs"`
	Extensions []string      `mapstructure:"extensions"`
	Debounce   time.Duration `mapstructure:"debounce"`
	StateFile  string        `mapstructure:"state_file"`
	TenantID   string        `mapstructure:"tenant_id"`
}

// OSSConfig 对象存储配置。
type OSSConfig struct {
	Endpoint  string `mapstructure:"endpoint"`
	Region    string `mapstructure:"region"`
	Bucket    string `mapstructure:"bucket"`
	AccessKey string `mapstructure:"access_key"`
	SecretKey string `mapstructure:"secret_key"`
	Prefix    string `mapstructure:"prefix"`
}

// ArchiverConfig 冷热分离归档配置。
type ArchiverConfig struct {
	Enabled      bool      `mapstructure:"enabled"`
	CronHour     int       `mapstructure:"cron_hour"`
	RetainMonths int       `mapstructure:"retain_months"`
	Tables       []string  `mapstructure:"tables"`
	ExportDir    string    `mapstructure:"export_dir"`
	// HTTPAddr 是 ClickHouse 的 HTTP 接口地址（如 http://clickhouse:8123），
	// 用于客户端侧 Parquet 导出；为空时从 clickhouse.hosts 推导（端口换成 8123）。
	HTTPAddr string    `mapstructure:"http_addr"`
	OSS      OSSConfig `mapstructure:"oss"`
}

// TelemetryConfig 指标与链路追踪配置。
type TelemetryConfig struct {
	MetricsEnabled bool    `mapstructure:"metrics_enabled"`
	TracingEnabled bool    `mapstructure:"tracing_enabled"`
	ServiceName    string  `mapstructure:"service_name"`
	OTLPEndpoint   string  `mapstructure:"otlp_endpoint"`
	SampleRatio    float64 `mapstructure:"sample_ratio"`
}

// LogConfig 日志配置。
type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

// Load 从指定路径加载配置；path 为空时按默认搜索路径查找 config.yaml。
// 环境变量以 MNEME_ 为前缀，例如 MNEME_SERVER_PORT=9090。
func Load(path string) (*Config, error) {
	v := viper.New()
	setDefaults(v)

	if path != "" {
		v.SetConfigFile(path)
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath("./configs")
		v.AddConfigPath(".")
		v.AddConfigPath("/etc/mneme")
	}

	v.SetEnvPrefix("MNEME")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("read config: %w", err)
		}
		// 配置文件缺失时退化为默认值 + 环境变量。
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Validate 校验关键配置项。
func (c *Config) Validate() error {
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid server port: %d", c.Server.Port)
	}
	if c.ClickHouse.BatchSize <= 0 {
		return fmt.Errorf("clickhouse.batch_size must be positive")
	}
	if c.ClickHouse.FlushInterval <= 0 {
		return fmt.Errorf("clickhouse.flush_interval must be positive")
	}
	if len(c.ClickHouse.Hosts) == 0 {
		return fmt.Errorf("clickhouse.hosts must not be empty")
	}
	return nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("server.port", 8080)
	v.SetDefault("server.read_timeout", 30*time.Second)
	v.SetDefault("server.write_timeout", 30*time.Second)
	v.SetDefault("server.shutdown_timeout", 15*time.Second)

	v.SetDefault("pulsar.url", "pulsar://localhost:6650")
	v.SetDefault("pulsar.tenant", "mnemosyne")
	v.SetDefault("pulsar.namespace", "mneme")
	v.SetDefault("pulsar.operation_timeout", 30*time.Second)
	v.SetDefault("pulsar.connection_timeout", 10*time.Second)
	v.SetDefault("pulsar.subscription_name", "mneme-sub")
	v.SetDefault("pulsar.topics.history", "mneme-history")
	v.SetDefault("pulsar.topics.traces", "mneme-traces")
	v.SetDefault("pulsar.topics.memory", "mneme-memory")

	v.SetDefault("clickhouse.hosts", []string{"localhost:9000"})
	v.SetDefault("clickhouse.database", "mnemosyne")
	v.SetDefault("clickhouse.username", "default")
	v.SetDefault("clickhouse.max_open_conns", 10)
	v.SetDefault("clickhouse.max_idle_conns", 5)
	v.SetDefault("clickhouse.dial_timeout", 10*time.Second)
	v.SetDefault("clickhouse.batch_size", 10000)
	v.SetDefault("clickhouse.flush_interval", 5*time.Second)
	v.SetDefault("clickhouse.max_retries", 5)
	v.SetDefault("clickhouse.auto_migrate", true)

	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("redis.db", 0)
	v.SetDefault("redis.dial_timeout", 5*time.Second)
	v.SetDefault("redis.pool_size", 20)

	v.SetDefault("consumer.history_enabled", true)
	v.SetDefault("consumer.traces_enabled", true)
	v.SetDefault("consumer.memory_enabled", true)
	v.SetDefault("consumer.trace_aggregate_timeout", 30*time.Second)
	v.SetDefault("consumer.memory_extract_interval", 10*time.Minute)

	v.SetDefault("memory.llm.enabled", false)
	v.SetDefault("memory.llm.timeout", 20*time.Second)
	v.SetDefault("memory.llm.max_events", 200)

	v.SetDefault("filewatcher.enabled", false)
	v.SetDefault("filewatcher.extensions", []string{".md", ".jsonl", ".txt"})
	v.SetDefault("filewatcher.debounce", 2*time.Second)
	v.SetDefault("filewatcher.state_file", "./data/.filewatcher_state.json")
	v.SetDefault("filewatcher.tenant_id", "default")

	v.SetDefault("archiver.enabled", false)
	v.SetDefault("archiver.cron_hour", 3)
	v.SetDefault("archiver.retain_months", 3)
	v.SetDefault("archiver.tables", []string{"history_message"})
	v.SetDefault("archiver.export_dir", "./data/archive")
	v.SetDefault("archiver.http_addr", "")

	v.SetDefault("telemetry.metrics_enabled", true)
	v.SetDefault("telemetry.tracing_enabled", false)
	v.SetDefault("telemetry.service_name", "mneme")
	v.SetDefault("telemetry.otlp_endpoint", "localhost:4318")
	v.SetDefault("telemetry.sample_ratio", 1.0)

	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")
}
