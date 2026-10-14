# Mneme Architecture

Mneme is a **long-term memory service for AI agents**. It aggregates conversation history, agent
traces, and explicit memory events into **user profiles** (stable attributes across sessions) and
**session summaries** (what a single conversation covered), and serves them through a unified API.

Module path: `github.com/mnemosyne-labs/mneme` (Go 1.25, single repository, single module).

> **Note on language**: this document and the other Markdown files are written in English for open
> source contributions. Code comments, the admin console UI, and the OpenAPI descriptions remain in
> Chinese, since those are consumed by the original team.

---

## 1. End-to-end data flow

```
                           ┌──────────────────────────────┐
   Files / Agent clients ─▶│  HTTP API  (internal/api)     │
   POST /api/v1/history    │  /history /history/batch      │
   /memory/events /traces  │  /memory/events /traces       │
                           └───────────┬──────────────────┘
                                       │  publish
                                       ▼
                           ┌──────────────────────────────┐
                           │  Pulsar  (3 topics)          │
                           │  mneme-history / mneme-traces │
                           │  / mneme-memory               │
                           └───────────┬──────────────────┘
                                       │  subscribe
                  ┌────────────────────┼────────────────────────┐
                  ▼                    ▼                         ▼
         HistoryConsumer       TracesConsumer           MemoryConsumer
         (raw history → CH)    (trace agg → CH)        (events → profile)
                  │                    │                    │  extractEntityProfile
                  │                    │                    ▼
                  │                    │            HistoryProfileConsumer
                  │                    │            (history → same profile table)
                  │                    │                    │
                  │                    │        SessionSummaryConsumer
                  │                    │        (history → summary, incremental refine)
                  └─────────┬──────────┴─────────────────────┘
                            ▼
             ┌──────────────────────────────────────────┐
             │  ClickHouse                               │
             │  history_message   (TTL 730 days)         │
             │  agent_traces      (TTL 7 days)           │
             │  entity_memory     (ReplacingMergeTree)   │
             │  session_summary   (ReplacingMergeTree)   │
             │  audit_log         (TTL 90 days)          │
             └──────────────────────────────────────────┘
                            ▲
             Redis (hot)    │  profile Hash / session Hash / version counters
             mneme:memory:* │  mneme:session:*  mneme:sessver:*
                            └──────────────────────────────────┘
                                       │
                                       ▼
                          Query API (reads Redis first)
                          GET /api/v1/memory/{type}/{id}
                          GET /api/v1/sessions/{session_id}
                          GET /api/v1/users/{user_id}   ← aggregate: profile + sessions
```

**Key design points**

- All writes are decoupled through Pulsar; consumers persist asynchronously.
- Redis serves low-latency reads, ClickHouse serves analytics and long retention.
- Reads hit Redis first and fall through to ClickHouse on a miss, repopulating Redis.
- The two extraction pipelines (user profile, session summary) share the same `LLMExtractor`.
  Enabling `memory.llm.enabled` therefore upgrades **both** to LLM extraction at once; when it is
  disabled everything falls back to rules, i.e. **zero additional LLM cost**.

---

## 2. Core components

| Component | Path | Responsibility |
|-----------|------|----------------|
| API layer | `internal/api` | Validation, publishing to Pulsar, query routing |
| IngestService | `internal/service` | Wraps `Publish*` and topic routing |
| HTTPHistorySink | `internal/service/httpsink.go` | Default delivery target for file-adapter (batched HTTP) |
| HistoryConsumer | `internal/consumer` | Batched writes of raw history into `history_message` (bloom dedup) |
| TracesConsumer | `internal/consumer` | Aggregates traces by `request_id`, writes `agent_traces` |
| MemoryConsumer | `internal/consumer` | Memory events → `extractEntityProfile` → `entity_memory` |
| HistoryProfileConsumer | `internal/consumer` | Buffers history per `(tenant, user_id)` → the same profile table |
| SessionSummaryConsumer | `internal/consumer` | Buffers history per `session_id` → `session_summary`, **incremental refine** |
| LLMExtractor | `internal/service/extractor.go` | Implements both `MemoryExtractor` and `SessionSummarizer` |
| ProfileStore | `internal/service/memory_store.go` | Redis Hash + ClickHouse, version via `Incr` |
| SessionSummaryStore | `internal/service/session_store.go` | Redis Hash + ClickHouse, version via `Incr`, ZSET or `SCAN` listing |
| AuditStore | `internal/service/audit.go` | Audit log writes; ClickHouse is the source of truth |
| BatchInserter | `pkg/storage/clickhouse/inserter.go` | Batched inserts + timed flush + exponential-backoff retry + adaptive batch sizing |
| QualityChecker | `internal/archiver/quality_check.go` | Data quality checks (profile completeness, summary coverage, message integrity, ZSET consistency, partition health) |

---

## 3. The two extraction pipelines

### 3.1 User profile (`HistoryProfileConsumer`)

- Subscribes to `mneme-history`, buffers per `(tenant, user_id)`
  (`history_profile_max_messages`, default 500; oldest entries are dropped when full).
- Wraps each message into a `MemoryEvent` with `event_type="message"` and feeds the **same**
  `extractor` used by `MemoryConsumer`.
- A timer (`history_profile_interval`, default `10m`) triggers extraction into the **same**
  `entity_memory` table.
- Net effect: explicit memory events and conversation content converge into a single profile that
  answers "who is this user".

### 3.2 Session summary (`SessionSummaryConsumer`)

- Subscribes to `mneme-history`, buffers per `(tenant, session_id)`
  (`session_summary_max_messages`, default 2000).
- A timer (`session_summary_interval`, default `10m`) generates a natural-language summary into the
  separate `session_summary` table — "what did this conversation cover".
- **Incremental update**: each round first reads the previous summary; if one exists, only the new
  messages are passed to `RefineSummary` for merging (instead of recomputing everything), and
  `MessageCount` accumulates. Without a previous summary it falls back to a full `Summarize`.
- With an LLM enabled, `Summarize` / `RefineSummary` call the model; otherwise
  `SummaryFallback` / `SummaryRefineFallback` degrade to first/last message concatenation.

---

## 4. Configuration (`configs/config.yaml`)

| Key | Default | Description |
|-----|---------|-------------|
| `consumer.history_profile_enabled` | `true` | Extract user profiles from conversation history |
| `consumer.history_profile_interval` | `10m` | Profile extraction cycle |
| `consumer.history_profile_max_messages` | `500` | Per-user buffer cap |
| `consumer.session_summary_enabled` | `true` | Session-level summarization |
| `consumer.session_summary_interval` | `10m` | Summary generation cycle |
| `consumer.session_summary_max_messages` | `2000` | Per-session buffer cap |
| `memory.llm.enabled` | `false` | Route profiles and summaries through an LLM |
| `adapter.mode` | `http` | file-adapter delivery: `http` (default) or `pulsar` |
| `cache.session_ttl` | `720h` | Session cache TTL (`0` = never expire; 720h recommended) |
| `cache.profile_ttl` | `720h` | Profile cache TTL (`0` = never expire; 720h recommended) |
| `cache.use_zset_index` | `true` | Use ZSET indexes instead of `SCAN` |

---

## 5. HTTP surface

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/history` | Single history message |
| POST | `/api/v1/history/batch` | Batched history (default path used by file-adapter) |
| POST | `/api/v1/traces` | Agent traces |
| POST | `/api/v1/memory/events` | Explicit memory events |
| POST | `/api/v1/sessions` | Create a session (ClickHouse first, then Redis) |
| POST | `/api/v1/users` | Create a user (ClickHouse first, then Redis) |
| GET | `/api/v1/memory/{entity_type}/{entity_id}` | Read an entity profile |
| GET | `/api/v1/sessions/{session_id}` | Read a session summary |
| GET | `/api/v1/users/{user_id}` | **Aggregate**: user profile + all its session summaries |
| GET | `/openapi.json` | OpenAPI 3 document |
| GET | `/docs` | Swagger UI for trying requests |
| GET | `/metrics` | Prometheus metrics |
| GET | `/health`, `/ready` | Liveness / readiness probes |

All tenant-scoped endpoints require the `X-Tenant-ID` header (some endpoints fall back to the
`tenant_id` field in the request body when the header is absent).

---

## 6. Deployment (`deploy/docker-compose.yaml`)

- `pulsar` — message bus; the one-shot `pulsar-init` container creates tenant / namespace / topics.
- `clickhouse` — cold storage; `AutoMigrate` creates the tables on startup.
- `redis` — hot storage (profile Hash / session Hash / version counters).
- `mneme` — the main service; `file-adapter` — a standalone binary that scans local files and
  delivers them as batched history over HTTP.

To purge corrupted Pulsar state: `docker compose down -v`, remove the `deploy_pulsar-data` volume,
then restart.

---

## 7. Admin console

### 7.1 Swagger UI (development)

Open `/docs` for Swagger UI, loaded from `/openapi.json`. Every endpoint supports **Try it out**
with prefilled example bodies; add an `X-Tenant-ID` header to exercise tenant-scoped routes.

### 7.2 Admin console (`/admin`)

Provided by `internal/api/admin_handler.go` plus the embedded `admin_ui.html` page. It is **disabled
by default**; enable it in `configs/config.yaml`:

```yaml
admin:
  enabled: true      # default false
  token: ""          # optional static token; when set, requires Authorization: Bearer <token>
  page_title: "Mneme Admin"
```

Once enabled:

- Entry point `GET /admin` (embedded HTML, no separate frontend build).
- User and profile browsing: `GET /admin/api/users?limit=&cursor=`, `GET /admin/api/users/{user_id}`.
- Session summary search: `GET /admin/api/sessions?user_id=&q=&limit=&cursor=` (sorted by end time,
  descending).
- Data governance:
  - Soft-delete a user and all their sessions: `DELETE /admin/api/users/{user_id}` (recoverable,
    marks `_deleted`).
  - Hard-delete a user and all their sessions: `DELETE /admin/api/users/{user_id}?hard=true`
    (irreversible).
  - Soft-delete a session: `DELETE /admin/api/sessions/{session_id}`.
  - Hard-delete a session: `DELETE /admin/api/sessions/{session_id}?hard=true`.
  - Export a user (profile + session JSON): `GET /admin/api/users/{user_id}/export`.
  - Export a session (JSON): `GET /admin/api/sessions/{session_id}/export`.
- Audit log: `GET /admin/api/audit?limit=` (deletes/exports are recorded, newest first).
- Trigger summarization immediately: `POST /admin/api/sessions/summarize` (no need to wait for the
  timer).
- Rebuild the Redis ZSET time index: `POST /admin/api/index/rebuild`.

Lists and search results use **cursor pagination**: responses return `next_cursor`, which you pass
back as the `cursor` parameter of the next page. Session search and the session list in the
aggregated user view are sorted by end time descending (newest first).

**Authorization** is intentionally thin: if `token` is configured, all `/admin/api/*` requests must
carry `Authorization: Bearer <token>` (export GET links may also pass `?token=` or `?__t=`). In
production, place `/admin` behind a gateway or reverse proxy that performs real access control.

Supporting methods: `ProfileStore.SaveUserInfo/ListProfiles/DeleteProfile/PurgeProfile`,
`SessionSummaryStore.SaveSessionInfo/SearchSessions/DeleteSession/PurgeSession/
DeleteSessionsByUser/PurgeSessionsByUser`, and `AuditStore.Append/List`.

**Immediate records**: when a message arrives (`HistoryHandler`), `SaveUserInfo` and
`SaveSessionInfo` immediately write a minimal record to Redis so the console can list users and
sessions without waiting for the LLM extraction/summary windows. Later extraction runs overwrite
those minimal records with the full profile/summary.

Soft deletion is implemented with an `_deleted` marker in the hot Hash and is skipped on read;
ClickHouse keeps the original snapshot (handled by TTL / archival policy).

---

## 8. Data retention

| Table | TTL | Notes |
|-------|-----|-------|
| `history_message` | 730 days | `TTL created_at + INTERVAL 730 DAY`, partitioned daily |
| `agent_traces` | 7 days | High volume, short-lived |
| `entity_memory` | none | Durable profiles, deduplicated by `_version` |
| `session_summary` | none | Durable summaries |
| `audit_log` | 90 days | Governance trail |
| `session_stats_hourly`, `user_profile_stats_hourly` | 730 days | Pre-aggregation targets |

`archiver` can export Parquet snapshots to object storage before TTL expiry.

> `CREATE TABLE IF NOT EXISTS` does not alter existing tables. Schema changes such as new TTLs or
> codecs require explicit `ALTER TABLE` migrations on deployments that already have data.

---

## 9. Redis compatibility (standalone / Dragonfly / Cluster)

### 9.1 Current state

`pkg/cache.NewRedisClient` builds a **single-node** client (`redis.NewClient`) from a comma
separated `addr`. This works with:

- a standalone Redis server,
- a Redis-compatible server such as **Dragonfly** (drop-in replacement, higher throughput).

### 9.2 Redis Cluster: not supported yet

Cluster mode requires three changes, tracked in the roadmap:

1. **Client type** — switch to `redis.NewUniversalClient` so cluster addresses are handled.
2. **Hash tags** — keys are tenant-prefixed (`mneme:memory:{tenant}:{type}:{id}`,
   `mneme:session:{tenant}:{session}`) but not hash-tagged with braces, so keys of the same tenant
   do **not** share a slot. Multi-key pipelines/transactions would fail with `CROSSSLOT`; wrap the
   tenant in braces (`{tenant}`) to fix this.
3. **Index rebuild** — `POST /admin/api/index/rebuild` currently iterates a single node; in cluster
   mode it must fan out over all masters.

Until then, run a standalone Redis (or Dragonfly). Enabling `cache.use_zset_index: true` is still
recommended because it replaces `SCAN`-based listing with `ZREVRANGEBYSCORE` cursor pagination.

### 9.3 Key layout

| Key | Type | Purpose |
|-----|------|---------|
| `mneme:memory:{tenant}:{entity_type}:{entity_id}` | Hash | Entity profile |
| `mneme:memver:{tenant}:{entity_type}:{entity_id}` | String | Profile version |
| `mneme:session:{tenant}:{session_id}` | Hash | Session summary |
| `mneme:sessver:{tenant}:{session_id}` | String | Summary version |
| `mneme:users:{tenant}` | ZSET | User time index |
| `mneme:sessions:{tenant}` | ZSET | Session time index |
| `mneme:bloom:{pipeline}:{tenant}` | Bloom | Deduplication |
| `mneme:audit:{tenant}` | List | Audit entries (ClickHouse is authoritative) |
