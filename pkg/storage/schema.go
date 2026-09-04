package storage

// DDL 语句：服务启动时幂等执行，确保数据库结构就绪。
const (
	// DDLHistoryMessage 会话历史表。
	DDLHistoryMessage = `
CREATE TABLE IF NOT EXISTS history_message (
    message_id String,
    tenant_id  String,
    session_id String,
    user_id    String,
    role       String,
    content    String,
    model_name String,
    total_tokens Int32,
    prompt_tokens Int32,
    completion_tokens Int32,
    response_time_ms Int32,
    metadata   String,
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY (tenant_id, session_id, created_at)
PARTITION BY toYYYYMM(created_at)
TTL created_at + INTERVAL 730 DAY
SETTINGS index_granularity = 8192`

	// DDLAgentTraces 短期轨迹表（TTL 7 天）。
	DDLAgentTraces = `
CREATE TABLE IF NOT EXISTS agent_traces (
    request_id String,
    tenant_id  String,
    session_id String,
    user_id    String,
    step_type  String,
    step_name  String,
    input      String,
    output     String,
    metadata   Map(String, String),
    duration_ms Int32,
    timestamp  DateTime
) ENGINE = MergeTree()
ORDER BY (tenant_id, request_id, timestamp)
TTL timestamp + INTERVAL 7 DAY
SETTINGS index_granularity = 8192`

	// DDLEntityMemory 实体记忆表（ReplacingMergeTree 支持版本更新）。
	DDLEntityMemory = `
CREATE TABLE IF NOT EXISTS entity_memory (
    entity_id   String,
    tenant_id   String,
    entity_type String,
    profile     String CODEC(ZSTD(3)),
    _version    Int64 CODEC(ZSTD(3)),
    updated_at  DateTime CODEC(Delta(4), ZSTD(3))
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (tenant_id, entity_type, entity_id)
SETTINGS index_granularity = 8192`

	// DDLSessionSummary 会话总结表（ReplacingMergeTree 按版本更新）。
	DDLSessionSummary = `
CREATE TABLE IF NOT EXISTS session_summary (
    session_id    String,
    tenant_id     String,
    user_id       String,
    summary       String CODEC(ZSTD(3)),
    message_count Int32 CODEC(ZSTD(3)),
    started_at    DateTime CODEC(Delta(4), ZSTD(3)),
    ended_at      DateTime CODEC(Delta(4), ZSTD(3)),
    _version      Int64 CODEC(ZSTD(3)),
    updated_at    DateTime CODEC(Delta(4), ZSTD(3))
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (tenant_id, session_id)
SETTINGS index_granularity = 8192`

	// DDLSessionStatsHourly 会话统计按小时预聚合（用于管理后台快速聚合查询）。
	DDLSessionStatsHourly = `
CREATE TABLE IF NOT EXISTS session_stats_hourly (
    tenant_id     String,
    user_id       String,
    hour          DateTime,
    session_count UInt64,
    msg_count     UInt64,
    first_msg     DateTime,
    last_msg      DateTime
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (tenant_id, user_id, hour)
TTL hour + INTERVAL 730 DAY
SETTINGS index_granularity = 8192`

	// DDLAuditLog 审计日志表（只追加，按时间倒序查询）。
	DDLAuditLog = `
CREATE TABLE IF NOT EXISTS audit_log (
    id          UInt64,
    tenant_id   String,
    actor       String CODEC(ZSTD(3)),
    action      String CODEC(ZSTD(3)),
    target      String CODEC(ZSTD(3)),
    detail      String CODEC(ZSTD(3)),
    at          DateTime CODEC(Delta(4), ZSTD(3))
) ENGINE = MergeTree()
ORDER BY (tenant_id, at)
PARTITION BY toYYYYMM(at)
TTL at + INTERVAL 90 DAY
SETTINGS index_granularity = 8192`

	// DDLUserProfileStatsHourly 用户画像统计按小时预聚合（用于管理后台用户画像质量监控）。
	DDLUserProfileStatsHourly = `
CREATE TABLE IF NOT EXISTS user_profile_stats_hourly (
    tenant_id       String,
    entity_type     String,
    entity_id       String,
    hour            DateTime,
    profile_count   UInt64,
    profile_size    UInt64,
    version_max     UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (tenant_id, entity_type, hour)
TTL hour + INTERVAL 730 DAY
SETTINGS index_granularity = 8192`
)

// SchemaStatements 返回按顺序执行的建表语句。
func SchemaStatements() []string {
	return []string{DDLHistoryMessage, DDLAgentTraces, DDLEntityMemory, DDLSessionSummary, DDLSessionStatsHourly, DDLUserProfileStatsHourly, DDLAuditLog}
}

// DatabaseDDL 返回创建数据库的语句。
func DatabaseDDL(db string) string {
	return "CREATE DATABASE IF NOT EXISTS " + db
}
