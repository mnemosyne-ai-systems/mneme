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
	Cache       CacheConfig       `mapstructure:"cache"`
	Consumer    ConsumerConfig    `mapstructure:"consumer"`
	Evolution   EvolutionConfig   `mapstructure:"evolution"`
	Memory      MemoryConfig      `mapstructure:"memory"`
	FileWatcher FileWatcherConfig `mapstructure:"filewatcher"`
	Adapter     AdapterConfig     `mapstructure:"adapter"`
	Admin       AdminConfig       `mapstructure:"admin"`
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
	MaxBatchBytes int           `mapstructure:"max_batch_bytes"`
	FlushInterval time.Duration `mapstructure:"flush_interval"`
	MaxRetries    int           `mapstructure:"max_retries"`
	AutoMigrate   bool          `mapstructure:"auto_migrate"`

	// 自适应批次控制
	AdaptiveBatchEnabled bool          `mapstructure:"adaptive_batch_enabled"`
	MinBatchSize         int           `mapstructure:"min_batch_size"`
	MaxBatchSize         int           `mapstructure:"max_batch_size"`
	TargetFlushLatency   time.Duration `mapstructure:"target_flush_latency"`
}

// RedisConfig Redis 连接配置。
type RedisConfig struct {
	Addr            string        `mapstructure:"addr"`
	Password        string        `mapstructure:"password"`
	DB              int           `mapstructure:"db"`
	DialTimeout     time.Duration `mapstructure:"dial_timeout"`
	ReadTimeout     time.Duration `mapstructure:"read_timeout"`
	WriteTimeout    time.Duration `mapstructure:"write_timeout"`
	PoolSize        int           `mapstructure:"pool_size"`
	MinIdleConns    int           `mapstructure:"min_idle_conns"`
	ConnMaxIdleTime time.Duration `mapstructure:"conn_max_idle_time"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
}

// CacheConfig 热存储 TTL 配置（0 = 永不过期）。
type CacheConfig struct {
	SessionTTL time.Duration `mapstructure:"session_ttl"`
	ProfileTTL time.Duration `mapstructure:"profile_ttl"`
	// UseZSetIndex 是否启用 ZSET 索引替代 SCAN（实验性，默认关闭）。
	UseZSetIndex bool `mapstructure:"use_zset_index"`
}

// ConsumerConfig 消费者开关与窗口配置。
type ConsumerConfig struct {
	HistoryEnabled        bool          `mapstructure:"history_enabled"`
	TracesEnabled         bool          `mapstructure:"traces_enabled"`
	MemoryEnabled         bool          `mapstructure:"memory_enabled"`
	TraceAggregateTimeout time.Duration `mapstructure:"trace_aggregate_timeout"`
	MemoryExtractInterval time.Duration `mapstructure:"memory_extract_interval"`
	// HistoryProfileEnabled 是否从对话历史中萃取用户画像。
	HistoryProfileEnabled bool `mapstructure:"history_profile_enabled"`
	// HistoryProfileInterval 对话历史萃取周期。
	HistoryProfileInterval time.Duration `mapstructure:"history_profile_interval"`
	// HistoryProfileMaxMessages 单用户最多缓冲的对话消息数，超出丢弃最旧的。
	HistoryProfileMaxMessages int `mapstructure:"history_profile_max_messages"`
	// SessionSummaryEnabled 是否从对话历史中生成会话级总结。
	SessionSummaryEnabled bool `mapstructure:"session_summary_enabled"`
	// SessionSummaryInterval 会话总结生成周期。
	SessionSummaryInterval time.Duration `mapstructure:"session_summary_interval"`
	// SessionSummaryMaxMessages 单会话最多缓冲的消息数，超出丢弃最旧的。
	SessionSummaryMaxMessages int `mapstructure:"session_summary_max_messages"`
}

// EvolutionConfig 会话摘要自我进化（定时反思 + 跨会话整合）配置。
// 依赖 memory.llm.enabled 与 consumer.session_summary_enabled。
type EvolutionConfig struct {
	Enabled bool `mapstructure:"enabled"`
	// Interval 进化队列扫描周期。
	Interval time.Duration `mapstructure:"interval"`
	// InitialDelay 摘要生成后到首次反思的延迟。
	InitialDelay time.Duration `mapstructure:"initial_delay"`
	// MaxRounds 同一份摘要最多连续反思的轮次。
	MaxRounds int `mapstructure:"max_rounds"`
	// BatchSize 每轮最多处理的会话数。
	BatchSize int `mapstructure:"batch_size"`
	// HistoryLimit 反思时读取的最近消息数。
	HistoryLimit int `mapstructure:"history_limit"`
	// MaxUserSessions 跨会话整合时使用的最新会话摘要数。
	MaxUserSessions int `mapstructure:"max_user_sessions"`
	// MaxUsersPerRun 单轮最多做跨会话整合的用户数（限制 LLM 调用量）。
	MaxUsersPerRun int `mapstructure:"max_users_per_run"`
	// UserSummaryEnabled 是否把演化结果整合进用户画像的跨会话长期摘要。
	UserSummaryEnabled bool `mapstructure:"user_summary_enabled"`
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

// AdapterConfig 独立文件采集器 file-adapter 的投递配置。
// 主服务内嵌 filewatcher 时不使用本节。
type AdapterConfig struct {
	// Mode 投递模式：http 推送到服务端批量 API（默认，推荐）；
	// pulsar 直连消息总线（高吞吐可信环境）。
	Mode string            `mapstructure:"mode"`
	HTTP AdapterHTTPConfig `mapstructure:"http"`
}

// AdapterHTTPConfig HTTP 投递模式配置。
type AdapterHTTPConfig struct {
	// Endpoint 服务端批量接入地址。
	Endpoint string `mapstructure:"endpoint"`
	// Token 可选的 Bearer 鉴权令牌。
	Token string `mapstructure:"token"`
	// Timeout 单次请求超时。
	Timeout time.Duration `mapstructure:"timeout"`
	// MaxRetries 可重试失败后的最大重试次数。
	MaxRetries int `mapstructure:"max_retries"`
	// BackoffBase 指数退避基数。
	BackoffBase time.Duration `mapstructure:"backoff_base"`
	// MaxBatchSize 单次请求携带的最大消息数，超出自动分片。
	MaxBatchSize int `mapstructure:"max_batch_size"`
}

// AdminConfig 轻量后台管理界面（只读浏览 + 数据治理）配置。
// 不做复杂鉴权：仅通过可选静态令牌（Token）做一层薄保护，生产请配合网关鉴权。
type AdminConfig struct {
	// Enabled 是否启用后台管理界面与接口（默认关闭）。
	Enabled bool `mapstructure:"enabled"`
	// Token 可选的保护令牌：非空时要求 `Authorization: Bearer <token>`。
	Token string `mapstructure:"token"`
	// PageTitle 管理界面标题（可选）。
	PageTitle string `mapstructure:"page_title"`
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
	Enabled      bool     `mapstructure:"enabled"`
	CronHour     int      `mapstructure:"cron_hour"`
	RetainMonths int      `mapstructure:"retain_months"`
	Tables       []string `mapstructure:"tables"`
	ExportDir    string   `mapstructure:"export_dir"`
	// HTTPAddr 是 ClickHouse 的 HTTP 接口地址（如 http://clickhouse:8123），
	// 用于客户端侧 Parquet 导出；为空时从 clickhouse.hosts 推导（端口换成 8123）。
	HTTPAddr string    `mapstructure:"http_addr"`
	OSS      OSSConfig `mapstructure:"oss"`
	// QualityCheckInterval 数据质量巡检周期（启用归档任务时生效）。
	QualityCheckInterval time.Duration `mapstructure:"quality_check_interval"`
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
	switch c.Adapter.Mode {
	case "", "http", "pulsar":
	default:
		return fmt.Errorf("adapter.mode must be http or pulsar, got %q", c.Adapter.Mode)
	}
	if c.Adapter.HTTP.MaxBatchSize < 0 {
		return fmt.Errorf("adapter.http.max_batch_size must not be negative")
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

	v.SetDefault("clickhouse.max_batch_bytes", 10*1024*1024)
	v.SetDefault("clickhouse.adaptive_batch_enabled", true)
	v.SetDefault("clickhouse.min_batch_size", 1000)
	v.SetDefault("clickhouse.max_batch_size", 100000)
	v.SetDefault("clickhouse.target_flush_latency", 100*time.Millisecond)

	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("redis.db", 0)
	v.SetDefault("redis.dial_timeout", 5*time.Second)
	v.SetDefault("redis.read_timeout", 3*time.Second)
	v.SetDefault("redis.write_timeout", 3*time.Second)
	v.SetDefault("redis.pool_size", 80)
	v.SetDefault("redis.min_idle_conns", 40)
	v.SetDefault("redis.conn_max_idle_time", 30*time.Minute)
	v.SetDefault("redis.conn_max_lifetime", time.Hour)

	v.SetDefault("cache.session_ttl", 720*time.Hour)
	v.SetDefault("cache.profile_ttl", 720*time.Hour)
	v.SetDefault("cache.use_zset_index", true)

	v.SetDefault("consumer.history_enabled", true)
	v.SetDefault("consumer.traces_enabled", true)
	v.SetDefault("consumer.memory_enabled", true)
	v.SetDefault("consumer.trace_aggregate_timeout", 30*time.Second)
	v.SetDefault("consumer.memory_extract_interval", 10*time.Minute)
	v.SetDefault("consumer.history_profile_enabled", true)
	v.SetDefault("consumer.history_profile_interval", 10*time.Minute)
	v.SetDefault("consumer.history_profile_max_messages", 500)
	v.SetDefault("consumer.session_summary_enabled", true)
	v.SetDefault("consumer.session_summary_interval", 10*time.Minute)
	v.SetDefault("consumer.session_summary_max_messages", 2000)

	v.SetDefault("evolution.enabled", false)
	v.SetDefault("evolution.interval", time.Hour)
	v.SetDefault("evolution.initial_delay", 10*time.Minute)
	v.SetDefault("evolution.max_rounds", 2)
	v.SetDefault("evolution.batch_size", 10)
	v.SetDefault("evolution.history_limit", 200)
	v.SetDefault("evolution.max_user_sessions", 10)
	v.SetDefault("evolution.max_users_per_run", 10)
	v.SetDefault("evolution.user_summary_enabled", true)

	v.SetDefault("memory.llm.enabled", false)
	v.SetDefault("memory.llm.timeout", 20*time.Second)
	v.SetDefault("memory.llm.max_events", 200)

	v.SetDefault("filewatcher.enabled", false)
	v.SetDefault("filewatcher.extensions", []string{".md", ".jsonl", ".json", ".txt"})
	v.SetDefault("filewatcher.debounce", 2*time.Second)
	v.SetDefault("filewatcher.state_file", "./data/.filewatcher_state.json")
	v.SetDefault("filewatcher.tenant_id", "default")

	v.SetDefault("adapter.mode", "http")
	v.SetDefault("adapter.http.endpoint", "http://localhost:8080/api/v1/history/batch")
	v.SetDefault("adapter.http.timeout", 10*time.Second)
	v.SetDefault("adapter.http.max_retries", 5)
	v.SetDefault("adapter.http.backoff_base", 500*time.Millisecond)
	v.SetDefault("adapter.http.max_batch_size", 500)
	v.SetDefault("admin.enabled", false)
	v.SetDefault("admin.token", "")
	v.SetDefault("admin.page_title", "Mneme 管理后台")

	v.SetDefault("archiver.enabled", false)
	v.SetDefault("archiver.cron_hour", 3)
	v.SetDefault("archiver.retain_months", 3)
	v.SetDefault("archiver.tables", []string{"history_message"})
	v.SetDefault("archiver.export_dir", "./data/archive")
	v.SetDefault("archiver.http_addr", "")
	v.SetDefault("archiver.quality_check_interval", time.Hour)

	v.SetDefault("telemetry.metrics_enabled", true)
	v.SetDefault("telemetry.tracing_enabled", false)
	v.SetDefault("telemetry.service_name", "mneme")
	v.SetDefault("telemetry.otlp_endpoint", "localhost:4318")
	v.SetDefault("telemetry.sample_ratio", 1.0)

	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")
}
