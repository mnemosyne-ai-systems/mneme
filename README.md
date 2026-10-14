# Mneme

[![Go](https://img.shields.io/badge/Go-1.25-blue.svg)](https://go.dev)
[![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)

**Mneme** 是一个面向 AI Agent 的**长期记忆服务**：把对话历史、Agent 轨迹、显式记忆事件汇聚成
**用户画像（跨会话稳定属性）** 与 **会话总结（单次对话内容）**，并通过统一的 HTTP 接口对外提供低延迟查询与数据治理能力。

> 名字取自希腊语 *μνήμη*（记忆）。架构上采用「消息总线解耦 + 冷热分离存储」：
> Pulsar 承接写入峰值，Redis 承载毫秒级热数据，ClickHouse 承载长期留存与分析。

---

## 目录

- [特性](#特性)
- [架构](#架构)
- [快速开始](#快速开始)
- [API 一览](#api-一览)
- [配置](#配置)
- [数据存储](#数据存储)
- [可观测性](#可观测性)
- [部署](#部署)
- [文件采集器 file-adapter](#文件采集器-file-adapter)
- [项目结构](#项目结构)
- [路线图](#路线图)
- [参与贡献](#参与贡献)

---

## 特性

| 能力 | 说明 |
|------|------|
| **三条写入管道** | 会话历史 `history`、Agent 轨迹 `traces`、长期记忆事件 `memory`，经 Pulsar 异步落库，削峰填谷 |
| **双存储模型** | Redis 热数据（Hash + ZSET 索引 + TTL）毫秒级读；ClickHouse 冷数据（压缩 + TTL）长期留存 |
| **画像 / 总结自动萃取** | 对话历史按用户聚合成画像、按会话生成总结；内置规则实现，开箱即用 |
| **可选 LLM 增强** | 开启 `memory.llm.enabled` 后画像与总结改由 LLM 精炼；关闭时**零 LLM 成本** |
| **数据治理** | 软删除 / 硬删除、游标分页、时间范围过滤、单用户与会话导出，全部操作写审计日志（ClickHouse） |
| **管理后台** | 内置零依赖单页后台 `/admin`，画像浏览、会话检索、折叠长消息、一键重建索引 |
| **自动批写** | `BatchInserter` 按行数 / 时间 / 字节数三阈值刷写，失败自动回队与重试，进程退出前强制落盘 |
| **可观测性** | Prometheus 指标、OpenTelemetry Trace、结构化 JSON 日志、健康 / 就绪探针 |
| **弹性伸缩** | 提供 HPA + ServiceMonitor + PrometheusRule，多实例水平扩容 |
| **水平扩展** | Consumer 与 HTTP 层无状态，多副本部署；Pulsar 订阅保证同一会话内消息有序 |

---

## 架构

```
              客户端 / Agent / 采集器
                        │  HTTP
             ┌──────────▼───────────┐
             │   internal/api       │  校验 → 发布
             └──────────┬───────────┘
                        │  Publish
             ┌──────────▼───────────┐
             │  Pulsar（3 个 Topic） │
             └──────────┬───────────┘
                        │  Subscribe
   ┌────────────┬───────┴───────┬────────────────┬──────────────────┐
History     Traces          Memory         HistoryProfile   SessionSummary
Consumer    Consumer        Consumer          Consumer          Consumer
   │            │               │                 │                  │
   └────────────┴───────┬───────┴─────────────────┴──────────────────┘
                        │
        ┌───────────────┴────────────────┐
        │                                │
┌───────▼─────────┐          ┌───────────▼────────────┐
│ ClickHouse（冷） │          │  Redis（热，带 TTL）    │
│ history_message  │          │  画像 Hash / 会话 Hash  │
│ agent_traces     │          │  ZSET 时间索引 / 版本号 │
│ entity_memory    │          │  布隆去重              │
│ session_summary  │          └───────────┬────────────┘
│ audit_log        │                      │  未命中回源 ClickHouse
└───────────────────┘                      │
                             ┌────────────▼────────────┐
                             │ 查询接口 + 管理后台       │
                             └─────────────────────────┘
```

详细设计见 [ARCHITECTURE.md](ARCHITECTURE.md)。

---

## 快速开始

### 1. 环境要求

- Go 1.25+
- Docker / Docker Compose（用于启动 Pulsar、ClickHouse、Redis）
- 可选：本地 LLM 服务（Ollama / 任意 OpenAI 兼容端点）用于增强萃取

### 2. 启动依赖

```bash
make deps-up        # docker compose 起 Pulsar + ClickHouse + Redis
```

> 首次启动时 `pulsar-init` 会自动创建租户 `mnemosyne`、命名空间 `mneme` 与三个 Topic。

### 3. 启动服务

```bash
make run            # 等价于 go run ./cmd/mneme（默认读取 ./configs/config.yaml）
```

| 地址 | 说明 |
|------|------|
| <http://localhost:8080/health> | 健康检查 |
| <http://localhost:8080/ready> | 就绪检查 |
| <http://localhost:8080/docs> | Swagger UI 接口文档 |
| <http://localhost:8080/admin> | 管理后台（需 `admin.enabled: true`） |
| <http://localhost:8080/metrics> | Prometheus 指标 |

### 4. 验证链路

```bash
curl -X POST http://localhost:8080/api/v1/history/batch \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-ID: t1' \
  -d '{"messages":[
        {"session_id":"s1","user_id":"u1","role":"user","content":"我习惯用 Go 写服务"},
        {"session_id":"s1","user_id":"u1","role":"assistant","content":"好的，已记录"}
      ]}'

# 读取聚合后的用户上下文（画像 + 会话总结）
curl -H 'X-Tenant-ID: t1' http://localhost:8080/api/v1/users/u1
```

完整可执行的请求集合见 [mneme.http](mneme.http)（IntelliJ / VS Code REST Client 可直接运行）。

> **提示**：写入经 Pulsar 异步落库，拉取式萃取按 `consumer.session_summary_interval` / `history_profile_interval` 周期触发。
> 需立即生成总结时调用 `POST /admin/api/sessions/summarize`。

---

## API 一览

### 业务接口（`/api/v1`，租户由 `X-Tenant-ID` 头指定）

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/v1/history` | 写入单条会话历史消息 |
| `POST` | `/api/v1/history/batch` | 批量写入会话历史（采集器推荐） |
| `POST` | `/api/v1/traces` | 写入 Agent 轨迹步骤 |
| `POST` | `/api/v1/memory/events` | 写入显式长期记忆事件 |
| `GET` | `/api/v1/memory/{entity_type}/{entity_id}` | 读取实体画像 |
| `POST` | `/api/v1/sessions` | 显式创建会话 |
| `GET` | `/api/v1/sessions/{session_id}` | 读取会话总结 |
| `POST` | `/api/v1/users` | 显式创建用户及其初始画像 |
| `GET` | `/api/v1/users/{user_id}` | 聚合读取用户上下文（画像 + 会话列表） |

### 管理接口（`/admin/api`，可选 `Authorization: Bearer <token>`）

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/admin/api/users` | 用户/画像列表（游标分页 `limit` + `cursor`） |
| `GET` | `/admin/api/users/{user_id}` | 用户画像详情 |
| `GET` | `/admin/api/users/{user_id}/export` | 导出用户全量数据 |
| `DELETE` | `/admin/api/users/{user_id}` | 删除用户（`?hard=true` 硬删除） |
| `GET` | `/admin/api/sessions` | 会话总结检索（`user_id` / `q` / `start_time` / `end_time` / 游标） |
| `GET` | `/admin/api/sessions/{session_id}` | 会话详情 |
| `GET` | `/admin/api/sessions/{session_id}/history` | 从 ClickHouse 拉取完整会话历史 |
| `GET` | `/admin/api/sessions/{session_id}/export` | 导出会话数据 |
| `DELETE` | `/admin/api/sessions/{session_id}` | 删除会话（`?hard=true` 硬删除） |
| `POST` | `/admin/api/sessions/summarize` | 立即触发一轮会话总结 |
| `POST` | `/admin/api/index/rebuild` | 重建 Redis ZSET 时间索引 |
| `GET` | `/admin/api/audit` | 查询审计日志 |

### 运维接口

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/health` | 存活探针 |
| `GET` | `/ready` | 就绪探针 |
| `GET` | `/metrics` | Prometheus 指标 |
| `GET` | `/openapi.json` | OpenAPI 3.0 规范 |

完整字段说明见运行时的 `/docs`。

---

## 配置

配置文件：`configs/config.yaml`（Viper 加载，支持环境变量覆盖，如 `MNEME_SERVER_PORT`）。
常用项：

| 配置段 | 关键项 | 默认值 | 说明 |
|--------|--------|--------|------|
| `server` | `port` | `8080` | HTTP 监听端口 |
| `pulsar` | `url` / `token` | `pulsar://localhost:6650` | 消息总线连接 |
| `clickhouse` | `auto_migrate` | `true` | 启动时自动建表 |
| `clickhouse` | `batch_size` / `flush_interval` | `10000` / `5s` | 批量写入阈值 |
| `clickhouse` | `adaptive_batch_enabled` | `true` | 按吞吐自适应调整批大小 |
| `clickhouse` | `max_batch_bytes` | `10485760` | 单批最大字节数，防 OOM |
| `redis` | `pool_size` | `80` | 连接池大小 |
| `cache` | `session_ttl` / `profile_ttl` | `720h` | 热数据 TTL（**0 表示永不过期**） |
| `cache` | `use_zset_index` | `true` | ZSET 时间索引替代 SCAN |
| `consumer` | `session_summary_interval` | `10m` | 会话总结周期 |
| `consumer` | `history_profile_interval` | `10m` | 画像萃取周期 |
| `memory.llm` | `enabled` | `false` | 开启后使用 LLM 精炼画像/总结 |
| `telemetry` | `metrics_enabled` | `true` | 暴露 `/metrics` |
| `telemetry` | `tracing_enabled` | `false` | OTLP 上报链路 |
| `admin` | `enabled` / `token` | `true` / 空 | 管理后台开关与静态令牌 |
| `archiver` | `enabled` / `retain_months` | `false` / `3` | ClickHouse → Parquet → 对象存储归档 |

> **生产建议**：`admin.token` 必须设置非空值；对外暴露时在网关层补齐租户级鉴权与限流。

---

## 数据存储

### ClickHouse（冷存储 / 长期留存）

| 表 | 引擎 | 用途 |
|----|------|------|
| `history_message` | `MergeTree` | 原始会话历史，按天分区，TTL 730 天 |
| `agent_traces` | `MergeTree` | Agent 轨迹步骤 |
| `entity_memory` | `ReplacingMergeTree` | 实体画像（按 `_version` 去重） |
| `session_summary` | `ReplacingMergeTree` | 会话总结（按 `_version` 去重） |
| `audit_log` | `MergeTree` | 治理操作审计 |
| `session_stats_hourly` | `SummingMergeTree` | 会话维度小时级预聚合 |
| `user_profile_stats_hourly` | `SummingMergeTree` | 画像维度小时级预聚合 |

列族统一使用 `ZSTD` + `Delta` 编码；`history_message` 额外使用 `DoubleDelta` 以压缩时间戳。

### Redis（热存储 / 低延迟读）

| Key | 类型 | 说明 |
|-----|------|------|
| `mneme:memory:{type}:{id}` | Hash | 实体画像 |
| `mneme:session:{session_id}` | Hash | 会话总结 |
| `mneme:memver:{type}:{id}` | String | 画像版本号（乐观并发） |
| `mneme:sessver:{session_id}` | String | 总结版本号 |
| `mneme:users:{tenant}` | ZSET | 用户时间索引（游标分页） |
| `mneme:sessions:{tenant}` | ZSET | 会话时间索引（游标分页） |
| `mneme:bloom:*` | 布隆 | 消息去重 |
| `mneme:audit:{tenant}` | List | 审计日志（ClickHouse 为权威源） |

热数据读取未命中时自动回源 ClickHouse 并回填 Redis。

---

## 可观测性

**指标**（前缀 `mnemosyne_mneme_`，`GET /metrics`）

| 指标 | 类型 | 说明 |
|------|------|------|
| `http_requests_total` / `http_request_duration_seconds` | Counter / Histogram | HTTP QPS 与延迟 |
| `pulsar_messages_total` | Counter | 消息发布 / 消费计数 |
| `clickhouse_write_duration_seconds` / `rows_written_total` / `write_errors_total` | Histogram / Counter | 落库吞吐与失败 |
| `consumer_lag` / `consumer_errors_total` | Gauge / Counter | 消费积压与错误 |
| `dedup_filtered_total` | Counter | 布隆去重命中 |
| `memory_extract_total` / `trace_aggregation_flush_total` | Counter | 萃取与聚合次数 |
| `quality_check_total` | Counter | 数据质量巡检结果 |
| `mneme_write_queue_size` / `mneme_write_duration_seconds` | Gauge / Histogram | 写入积压与写延迟 |

**链路追踪**：开启 `telemetry.tracing_enabled` 后经 OTLP HTTP 上报（`otelhttp` 注入 trace_id，同时写入响应头 `X-Request-ID`）。

**日志**：Logrus JSON 输出，自动注入 `trace_id` / `tenant_id` 字段。

**告警与扩缩容**：`deploy/hpa.yaml` 提供 HPA、ServiceMonitor 与 PrometheusRule 样例。

---

## 部署

### Docker

```bash
make docker                       # 构建 mnemosyne-labs/mneme:latest
docker run --rm -p 8080:8080 \
  -v $(pwd)/configs:/app/configs \
  mnemosyne-labs/mneme:latest
```

### Docker Compose（依赖 + 服务）

```bash
make deps-up
docker compose -f deploy/docker-compose.yaml up -d mneme   # 或自行编写编排
```

### Kubernetes

```bash
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/deployment.yaml
kubectl apply -f deploy/k8s/service.yaml
kubectl apply -f deploy/hpa.yaml          # HPA + ServiceMonitor + PrometheusRule
```

---

## 文件采集器 file-adapter

把本地 Markdown / JSONL / 文本文件变成记忆事件，支持增量监听（fsnotify + 去抖 + 断点续传）。

```bash
./bin/file-adapter -config ./configs/config.yaml -dir ./data/inbox -mode http
```

| 参数 | 说明 |
|------|------|
| `-config` | 配置文件路径 |
| `-dir` | 监听目录（覆盖配置） |
| `-scan` | 启动时扫描存量文件，默认 `true` |
| `-mode` | 投递模式：`http`（默认，推荐）或 `pulsar` |

---

## 项目结构

```
.
├── cmd/
│   ├── mneme/            # 主服务入口
│   └── file-adapter/     # 文件采集器入口
├── internal/
│   ├── api/              # HTTP 路由、handler、中间件、管理后台、OpenAPI
│   ├── app/              # 依赖装配与生命周期
│   ├── archiver/         # ClickHouse → Parquet → 对象存储归档、质量巡检
│   ├── config/           # Viper 配置加载与默认值
│   ├── consumer/         # Pulsar 消费、轨迹聚合、画像/总结萃取
│   ├── filewatcher/      # 文件监听与解析
│   └── service/          # 摄取、画像与会话存储、审计、HTTP sink
├── pkg/
│   ├── cache/            # Redis 客户端与 L2 缓存
│   ├── logger/           # 结构化日志
│   ├── messagebus/       # Pulsar 封装
│   ├── metrics/          # Prometheus 指标定义
│   ├── models/           # 领域模型与校验
│   ├── storage/          # 存储抽象与 ClickHouse 实现
│   └── tracing/          # OpenTelemetry 封装
├── configs/config.yaml   # 配置样例
├── deploy/               # docker-compose / k8s / HPA
├── tools/yamlcheck/      # YAML 语法与重复键校验（make yaml-lint）
└── mneme.http            # 可执行请求集合
```

常用命令：`make build`、`make test`、`make lint`、`make yaml-lint`、`make deps-up`、`make docker`。

---

## 路线图

- [ ] ClickHouse 物化视图自动创建与增量刷新（当前提供预聚合目标表）
- [ ] Redis Cluster 模式与租户级 hash tag
- [ ] L2 本地缓存接入查询链路
- [ ] 质量巡检结果接入 PrometheusRule 告警闭环
- [ ] 多租户配额与限流

---

## 参与贡献

请阅读 [CONTRIBUTING.md](CONTRIBUTING.md)、[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) 与 [SECURITY.md](SECURITY.md)。

## 许可证

[Apache License 2.0](LICENSE)
