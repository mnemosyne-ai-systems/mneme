# Mneme 架构说明

Mneme 是一个面向 Agent 的**长期记忆服务**：把对话历史、Agent 轨迹、显式记忆事件汇聚成
**用户画像（跨会话稳定属性）** 与 **会话总结（单次对话内容）**，并提供统一查询接口。

模块路径：`github.com/mnemosyne/mneme`（Go 1.25，单仓库单模块）。

---

## 1. 整体数据流

```
                          ┌──────────────────────────────┐
   文件 / Agent 客户端 ──▶ │  HTTP API  (internal/api)     │
   POST /api/v1/history   │  /history /history/batch      │
   /memory/events /traces │  /memory/events /traces       │
                          └───────────┬──────────────────┘
                                      │  Publish
                                      ▼
                          ┌──────────────────────────────┐
                          │  Pulsar  (3 个 Topic)          │
                          │  mneme-history / mneme-traces  │
                          │  / mneme-memory                │
                          └───────────┬──────────────────┘
                                      │  Subscribe
                 ┌────────────────────┼────────────────────────┐
                 ▼                    ▼                         ▼
        HistoryConsumer       TracesConsumer           MemoryConsumer
        (原始历史落库)         (轨迹聚合落库)            (记忆事件 → 画像)
                 │                    │                    │  extractEntityProfile
                 │                    │                    ▼
                 │                    │            HistoryProfileConsumer
                 │                    │            (对话历史 → 同一张画像表)
                 │                    │                    │
                 │                    │        SessionSummaryConsumer
                 │                    │        (对话历史 → 会话总结，增量 refine)
                 └─────────┬──────────┴─────────────────────┘
                           ▼
            ┌──────────────────────────────────────────┐
            │  ClickHouse                               │
            │  history_message                          │
            │  agent_traces   (TTL 7 天)                 │
            │  entity_memory  (ReplacingMergeTree)       │
            │  session_summary (ReplacingMergeTree)      │
            └──────────────────────────────────────────┘
                           ▲
            Redis (热数据)  │  画像 Hash / 会话 Hash / 版本号计数器
            mneme:memory:*  │  mneme:session:*  mneme:sessver:*
                           └──────────────────────────────────┘
                                      │
                                      ▼
                         查询接口 (读 Redis 热数据)
                         GET /api/v1/memory/{type}/{id}
                         GET /api/v1/sessions/{session_id}
                         GET /api/v1/users/{user_id}   ← 聚合：画像 + 会话列表
```

**设计要点**：所有写入先经 Pulsar 解耦，consumer 异步落库；Redis 作为热存储支撑低延迟查询，
ClickHouse 作为冷存储支撑分析与长期保留。两条萃取管线（用户画像 / 会话总结）复用同一个
`LLMExtractor`，因此 `memory.llm.enabled` 一旦开启，二者会**同时**升级为 LLM 萃取；
未开启时全部走 fallback，**零额外 LLM 成本**。

---

## 2. 关键组件

| 组件 | 路径 | 职责 |
|------|------|------|
| API 接入层 | `internal/api` | 校验、发布到 Pulsar、查询路由 |
| IngestService | `internal/service` | 封装 `Publish*` 与 Topic 路由 |
| HTTPHistorySink | `internal/service/httpsink.go` | file-adapter 默认投递目标（HTTP 批量） |
| HistoryConsumer | `internal/consumer` | 原始对话历史批量写入 `history_message`（布隆去重） |
| TracesConsumer | `internal/consumer` | Agent 轨迹按 `request_id` 聚合后写入 `agent_traces` |
| MemoryConsumer | `internal/consumer` | 记忆事件 → `extractEntityProfile` → `entity_memory` |
| HistoryProfileConsumer | `internal/consumer` | 对话历史（含 user_id）按用户缓冲 → 同一张画像表 |
| SessionSummaryConsumer | `internal/consumer` | 对话历史按 `session_id` 缓冲 → `session_summary`，**增量 refine** |
| LLMExtractor | `internal/service/extractor.go` | 同时实现 `MemoryExtractor` 与 `SessionSummarizer` |
| ProfileStore | `internal/service` | Redis Hash + ClickHouse，版本号 `Incr` |
| SessionSummaryStore | `internal/service/session_store.go` | Redis Hash + ClickHouse，版本号 `Incr`，按用户 `SCAN` 列出 |
| BatchInserter | `pkg/storage/clickhouse` | 批量写入 + 定时刷新 + 指数退避重试 |

---

## 3. 两条萃取管线

### 3.1 用户画像（HistoryProfileConsumer）
- 订阅 `mneme-history`，按 `(tenant, user_id)` 缓冲（`MaxMessagesPerUser`，默认 500，超出丢最旧）
- 每条消息包装为 `event_type="message"` 的 `MemoryEvent`，喂给与 `MemoryConsumer` **相同**的 `extractor`
- 定时窗口（`history_profile_interval`，默认 10m）触发，写入 **同一张 `entity_memory` 表**
- 效果：显式记忆事件与对话内容汇聚到同一用户画像，描述"这个用户是谁"

### 3.2 会话总结（SessionSummaryConsumer）
- 订阅 `mneme-history`，按 `(tenant, session_id)` 缓冲（`MaxMessagesPerSession`，默认 2000）
- 定时窗口（`session_summary_interval`，默认 10m）触发，生成自然语言总结，写入独立 `session_summary` 表，描述"这次对话聊了什么"
- **增量更新**：每轮先从 store 读取上一轮总结 `prev`；若有则只把本轮新增消息交给
  `RefineSummary` 合并（避免每次全量重算），`MessageCount` 累加；无旧摘要才全量 `Summarize`
- 若开启 LLM：`Summarize` / `RefineSummary` 走模型；否则 `SummaryFallback` / `SummaryRefineFallback`
  退化为首尾消息拼接

---

## 4. 配置（`configs/config.yaml`）

| 配置项 | 默认 | 说明 |
|--------|------|------|
| `consumer.history_profile_enabled` | `true` | 对话历史萃取用户画像 |
| `consumer.history_profile_interval` | `10m` | 画像萃取周期 |
| `consumer.history_profile_max_messages` | `500` | 单用户缓冲上限 |
| `consumer.session_summary_enabled` | `true` | 会话级总结 |
| `consumer.session_summary_interval` | `10m` | 总结生成周期 |
| `consumer.session_summary_max_messages` | `2000` | 单会话缓冲上限 |
| `memory.llm.enabled` | `false` | 开启后画像与总结均走 LLM |
| `adapter.mode` | `http` | file-adapter 投递方式：`http`（默认）或 `pulsar` |

---

## 5. 对外接口

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/v1/history` | 单条对话历史 |
| POST | `/api/v1/history/batch` | 批量对话历史（file-adapter 默认走这条） |
| POST | `/api/v1/traces` | Agent 轨迹 |
| POST | `/api/v1/memory/events` | 显式记忆事件 |
| GET | `/api/v1/memory/{entity_type}/{entity_id}` | 读取实体画像 |
| GET | `/api/v1/sessions/{session_id}` | 读取会话总结 |
| GET | `/api/v1/users/{user_id}` | **聚合**：用户画像 + 其全部会话列表 |
| GET | `/openapi.json` | OpenAPI 3 文档 |
| GET | `/docs` | 轻量管理界面（Swagger UI，可在线试用） |
| GET | `/metrics` | Prometheus 指标 |
| GET | `/health` `/ready` | 健康检查 / 就绪检查 |

所有受租户隔离的接口需带 `X-Tenant-ID` 请求头（缺失时部分接口回退到消息体内的 `tenant_id`）。

---

## 6. 部署（`deploy/docker-compose.yaml`）

- `pulsar`：消息总线；`pulsar-init` 一次性容器负责建租户 / 命名空间 / Topic
- `clickhouse`：冷存储，`AutoMigrate` 自动建表（含 `session_summary`）
- `redis`：热存储（画像 Hash / 会话 Hash / 版本号计数器）
- `mneme`：主服务；`file-adapter`：独立二进制，扫描本地文件经 HTTP 批量写入历史

清除 Pulsar 损坏数据：`docker compose down -v` 并移除 `deploy_pulsar-data` 卷后重启。

---

## 7. 管理界面

### 7.1 开发联调（Swagger UI）
浏览器打开 `/docs` 即进入 Swagger UI，从 `/openapi.json` 加载规格，可在页面内对每个接口
直接 **Try it out**（已预填示例请求体）。配合 `X-Tenant-ID` 头即可联调全部查询与写入接口。

### 7.2 后台管理系统（admin）
由 `internal/api/admin_handler.go` + 内嵌页面 `admin_ui.html` 提供，**默认关闭**，需在
`configs/config.yaml` 中开启：

```yaml
admin:
  enabled: true      # 默认 false
  token: ""          # 可选静态令牌；非空时要求 Authorization: Bearer <token>
  page_title: "Mneme 管理后台"
```

开启后提供：

- 页面入口 `GET /admin`（内嵌 HTML，无需独立前端构建）
- 用户与画像浏览：`GET /admin/api/users?limit=&cursor=`、`GET /admin/api/users/{user_id}`
- 会话总结检索：`GET /admin/api/sessions?user_id=&q=&limit=&cursor=`（按结束时间倒序）
- 数据治理：
  - 软删除用户及其全部会话：`DELETE /admin/api/users/{user_id}`（可恢复，标记 `_deleted`）
  - 硬删除用户及其全部会话：`DELETE /admin/api/users/{user_id}?hard=true`（不可恢复，彻底清理）
  - 软删除单个会话：`DELETE /admin/api/sessions/{session_id}`
  - 硬删除单个会话：`DELETE /admin/api/sessions/{session_id}?hard=true`
  - 导出用户（画像 + 会话 JSON）：`GET /admin/api/users/{user_id}/export`
  - 导出单个会话（JSON）：`GET /admin/api/sessions/{session_id}/export`
- 审计日志：`GET /admin/api/audit?limit=`（记录删除/导出等变更操作，最新在前）

列表与检索支持**游标分页**：请求返回 `next_cursor`，将其作为下一页的 `cursor` 参数即可翻页。
会话检索与用户聚合视图中的会话列表均**按结束时间倒序**（最近的在前）。

鉴权说明：仅做薄保护——若配置了 `token`，所有 `/admin/api/*` 需带
`Authorization: Bearer <token>`（导出类 GET 链接也支持 `?token=` / `?__t=` 携带）。
生产环境建议将 `/admin` 置于网关或反向代理之后做真正的访问控制。

后端支撑方法：`ProfileStore.ListProfiles/DeleteProfile/PurgeProfile`、`SessionSummaryStore.
SearchSessions/DeleteSession/PurgeSession/DeleteSessionsByUser/PurgeSessionsByUser`，
以及 `AuditStore.Append/List`（Redis 与内存双实现）。软删除通过热存储 Hash 的 `_deleted`
标记实现，读取时自动跳过；冷存储 ClickHouse 仍保留原始快照（由 TTL/归档策略处理）。
