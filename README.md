# Mneme

[![Go](https://img.shields.io/badge/Go-1.25-blue.svg)](https://go.dev)
[![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)

**Mneme** is a **long-term memory service for AI agents**. It turns conversation history, agent
traces, and explicit memory events into two durable artifacts:

- **User profiles** — stable attributes that persist across sessions
- **Session summaries** — what a single conversation was about

Both are served through a unified HTTP API with low-latency reads and first-class data governance.

> Named after the Greek *μνήμη* (memory). The architecture follows **write decoupling + hot/cold
> storage split**: Pulsar absorbs write peaks, Redis serves millisecond reads, ClickHouse retains
> data long-term and powers analytics.

---

## Contents

- [Features](#features)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [API](#api)
- [Configuration](#configuration)
- [Storage Model](#storage-model)
- [Observability](#observability)
- [Deployment](#deployment)
- [File Adapter](#file-adapter)
- [Project Layout](#project-layout)
- [Roadmap](#roadmap)
- [Contributing](#contributing)

---

## Features

| Capability | Description |
|------------|-------------|
| **Three ingestion pipelines** | Conversation history, agent traces, and explicit memory events are published to Pulsar and written asynchronously, absorbing traffic spikes |
| **Hot/cold storage split** | Redis hot data (Hash + ZSET indexes + TTL) for millisecond reads; ClickHouse cold data (compression + TTL) for retention and analytics |
| **Automatic profile & summary extraction** | History is aggregated into per-user profiles and per-session summaries by a rule-based extractor that works out of the box |
| **Optional LLM refinement** | Set `memory.llm.enabled: true` to refine profiles and summaries with an LLM; leave it off for **zero LLM cost** |
| **Self-evolving summaries** | An optional background job re-reads recent history to rewrite session summaries over bounded rounds and folds recent sessions into a cross-session `evolving_summary` on the user profile |
| **Data governance** | Soft/hard delete, cursor pagination, time-range filtering, per-user and per-session export; every mutation is written to an audit log in ClickHouse |
| **Admin console** | Dependency-free single-page console at `/admin` for profile browsing, session search, collapsible long messages, and one-click index rebuild |
| **Batched writes** | `BatchInserter` flushes on row count, time, or byte thresholds; failed batches are requeued and retried; buffers are flushed on shutdown |
| **Observability** | Prometheus metrics, OpenTelemetry tracing, structured JSON logs, health and readiness probes |
| **Autoscaling** | Shipped HPA + ServiceMonitor + PrometheusRule examples for multi-replica deployments |
| **Horizontal scale-out** | Consumers and the HTTP tier are stateless; Pulsar subscriptions preserve per-session message ordering |

---

## Architecture

```
              Clients / Agents / Adapters
                        │  HTTP
             ┌──────────▼───────────┐
             │    internal/api      │  validate → publish
             └──────────┬───────────┘
                        │  publish
             ┌──────────▼───────────┐
             │  Pulsar (3 topics)   │
             └──────────┬───────────┘
                        │  subscribe
   ┌────────────┬───────┴───────┬────────────────┬──────────────────┐
History     Traces          Memory         HistoryProfile   SessionSummary
Consumer    Consumer        Consumer          Consumer          Consumer
   │            │               │                 │                  │
   └────────────┴───────┬───────┴─────────────────┴──────────────────┘
                        │
        ┌───────────────┴────────────────┐
        │                                │
┌───────▼─────────┐          ┌───────────▼────────────┐
│ ClickHouse(cold)│          │   Redis (hot, TTL)      │
│ history_message │          │  profile / session Hash │
│ agent_traces    │          │  ZSET time index / ver  │
│ entity_memory   │          │  bloom filter          │
│ session_summary │          └───────────┬────────────┘
│ audit_log       │                      │  miss → read-through CH
└─────────────────┘                      │
                             ┌────────────▼────────────┐
                             │  Query API + /admin      │
                             └─────────────────────────┘
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for the detailed design.

### Self-evolving summaries

With `evolution.enabled: true` (requires `memory.llm.enabled` and `consumer.session_summary_enabled`),
a background job keeps summaries improving instead of freezing them at write time:

1. A successful summary write schedules the session in a Redis ZSET; the first reflection runs after
   `evolution.initial_delay` (default `10m`).
2. Each scan (`evolution.interval`, default `1h`) re-reads the session's recent ClickHouse history and
   rewrites the summary until it converges or `evolution.max_rounds` (default `2`) is reached; failed
   rounds are retried on the next scan.
3. With `evolution.user_summary_enabled: true`, up to `evolution.max_user_sessions` recent session
   summaries are synthesized with the profile and stored as the profile's `evolving_summary` field,
   giving a cross-session narrative that evolves over time.

Reflections never overwrite a summary that changed while the LLM call was in flight (version check),
and `evolution.max_users_per_run` bounds cross-session synthesis per scan.

---

## Quick Start

### 1. Requirements

- Go 1.26+
- Docker / Docker Compose (to run Pulsar, ClickHouse, Redis)
- Optional: a local LLM endpoint (Ollama or any OpenAI-compatible server) for refined extraction

### 2. Start dependencies

```bash
make deps-up        # docker compose up Pulsar + ClickHouse + Redis
```

> On first start, the `pulsar-init` service creates tenant `mnemosyne`, namespace `mneme`, and the three topics.

### 3. Run the service

```bash
make run            # equivalent to: go run ./cmd/mneme (reads ./configs/config.yaml)
```

| Endpoint | Purpose |
|----------|---------|
| <http://localhost:8080/health> | Liveness probe |
| <http://localhost:8080/ready> | Readiness probe |
| <http://localhost:8080/docs> | Swagger UI |
| <http://localhost:8080/admin> | Admin console (requires `admin.enabled: true`) |
| <http://localhost:8080/metrics> | Prometheus metrics |

### 4. Verify the pipeline

```bash
curl -X POST http://localhost:8080/api/v1/history/batch \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-ID: t1' \
  -d '{"messages":[
        {"session_id":"s1","user_id":"u1","role":"user","content":"I prefer writing services in Go"},
        {"session_id":"s1","user_id":"u1","role":"assistant","content":"Noted."}
      ]}'

# Read back the aggregated user context (profile + session summaries)
curl -H 'X-Tenant-ID: t1' http://localhost:8080/api/v1/users/u1
```

A ready-to-run collection of requests lives in [mneme.http](mneme.http) (IntelliJ / VS Code REST Client).

> **Note**: writes are published to Pulsar and persisted asynchronously; extraction runs on the
> `consumer.session_summary_interval` / `consumer.history_profile_interval` cycles. To generate
> summaries immediately, call `POST /admin/api/sessions/summarize`.

---

## API

### Data plane (`/api/v1`, tenant supplied via the `X-Tenant-ID` header)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/history` | Write a single history message |
| `POST` | `/api/v1/history/batch` | Write a batch of history messages (recommended) |
| `POST` | `/api/v1/traces` | Write an agent trace step |
| `POST` | `/api/v1/memory/events` | Write an explicit long-term memory event |
| `GET` | `/api/v1/memory/{entity_type}/{entity_id}` | Read an entity profile |
| `POST` | `/api/v1/sessions` | Explicitly create a session |
| `GET` | `/api/v1/sessions/{session_id}` | Read a session summary |
| `POST` | `/api/v1/users` | Explicitly create a user with an initial profile |
| `GET` | `/api/v1/users/{user_id}` | Read aggregated user context (profile + session list) |

### Control plane (`/admin/api`, optional `Authorization: Bearer <token>`)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/admin/api/users` | List users/profiles (cursor pagination via `limit` + `cursor`) |
| `GET` | `/admin/api/users/{user_id}` | User profile detail |
| `GET` | `/admin/api/users/{user_id}/export` | Export all data for a user |
| `DELETE` | `/admin/api/users/{user_id}` | Delete a user (`?hard=true` for a permanent delete) |
| `GET` | `/admin/api/sessions` | Search session summaries (`user_id`, `q`, `start_time`, `end_time`, cursor) |
| `GET` | `/admin/api/sessions/{session_id}` | Session detail |
| `GET` | `/admin/api/sessions/{session_id}/history` | Full session history read from ClickHouse |
| `GET` | `/admin/api/sessions/{session_id}/export` | Export session data |
| `DELETE` | `/admin/api/sessions/{session_id}` | Delete a session (`?hard=true` for a permanent delete) |
| `POST` | `/admin/api/sessions/summarize` | Trigger a summary pass immediately |
| `POST` | `/admin/api/index/rebuild` | Rebuild the Redis ZSET session and per-user indexes |
| `GET` | `/admin/api/audit` | Query the audit log |

### Operations

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness probe |
| `GET` | `/ready` | Readiness probe |
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/openapi.json` | OpenAPI 3.0 specification |

Request/response schemas are served by the running instance at `/docs`.

---

## Configuration

Configuration lives in `configs/config.yaml` (loaded via Viper; environment variables override file
values, e.g. `MNEME_SERVER_PORT`). Frequently used keys:

| Section | Key | Default | Description |
|---------|-----|---------|-------------|
| `server` | `port` | `8080` | HTTP listen port |
| `pulsar` | `url` / `token` | `pulsar://localhost:6650` | Message bus connection |
| `clickhouse` | `auto_migrate` | `true` | Create tables on startup |
| `clickhouse` | `batch_size` / `flush_interval` | `10000` / `5s` | Write batch thresholds |
| `clickhouse` | `adaptive_batch_enabled` | `true` | Adapt batch size to observed throughput |
| `clickhouse` | `max_batch_bytes` | `10485760` | Max bytes per batch (protects against OOM) |
| `redis` | `pool_size` | `80` | Connection pool size |
| `cache` | `session_ttl` / `profile_ttl` | `720h` | Hot-data TTL (**`0` means never expire**) |
| `cache` | `use_zset_index` | `true` | Use ZSET time indexes instead of `SCAN` |
| `consumer` | `session_summary_interval` | `10m` | Session summarization cycle |
| `consumer` | `history_profile_interval` | `10m` | Profile extraction cycle |
| `evolution` | `enabled` | `false` | Periodic summary reflection (needs `memory.llm.enabled`) |
| `evolution` | `interval` / `max_rounds` | `1h` / `2` | Queue scan cycle and reflection bound |
| `evolution` | `max_users_per_run` | `10` | Users synthesized per scan (bounds LLM calls) |
| `evolution` | `user_summary_enabled` | `true` | Fold results into the cross-session `evolving_summary` |
| `memory.llm` | `enabled` | `false` | Use an LLM to refine profiles/summaries |
| `telemetry` | `metrics_enabled` | `true` | Expose `/metrics` |
| `telemetry` | `tracing_enabled` | `false` | Export traces over OTLP |
| `admin` | `enabled` / `token` | `true` / empty | Admin console switch and static token |
| `archiver` | `enabled` / `retain_months` | `false` / `3` | ClickHouse → Parquet → object storage |

> **Production checklist**: always set a non-empty `admin.token`, and put tenant-level authn/authz
> and rate limiting in front of the service at the gateway.

---

## Storage Model

### ClickHouse (cold storage, long retention)

| Table | Engine | Purpose |
|-------|--------|---------|
| `history_message` | `MergeTree` | Raw conversation history, partitioned daily, TTL 730 days |
| `agent_traces` | `MergeTree` | Agent trace steps |
| `entity_memory` | `ReplacingMergeTree` | Entity profiles, deduplicated by `_version` |
| `session_summary` | `ReplacingMergeTree` | Session summaries, deduplicated by `_version` |
| `audit_log` | `MergeTree` | Governance audit trail |
| `session_stats_hourly` | `SummingMergeTree` | Hourly pre-aggregation per session |
| `user_profile_stats_hourly` | `SummingMergeTree` | Hourly pre-aggregation per profile |

Columns use `ZSTD` + `Delta` compression; `history_message` additionally uses `DoubleDelta` for its
timestamp columns.

### Redis (hot storage, low-latency reads)

| Key | Type | Purpose |
|-----|------|---------|
| `mnemosyne:mneme:memory:{tenant}:{type}:{id}` | Hash | Entity profile |
| `mnemosyne:mneme:memver:{tenant}:{type}:{id}` | String | Profile version (optimistic concurrency) |
| `mnemosyne:mneme:session:{tenant}:{session_id}` | Hash | Session summary |
| `mnemosyne:mneme:sessver:{tenant}:{session_id}` | String | Summary version |
| `mnemosyne:mneme:users:{tenant}` | ZSET | User time index (cursor pagination) |
| `mnemosyne:mneme:sessions:{tenant}` | ZSET | Session time index (cursor pagination) |
| `mnemosyne:mneme:sessions:{tenant}:user:{user_id}` | ZSET | Per-user session index (newest first) |
| `mnemosyne:mneme:sessions:{tenant}:user_ready` | String | Marker: tenant per-user indexes initialized |
| `mnemosyne:mneme:evolution:due` | ZSET | Sessions awaiting summary reflection |
| `mnemosyne:mneme:evolution:state:{tenant}:{session_id}` | Hash | Reflection round state |
| `mnemosyne:mneme:bloom:{pipeline}:{tenant}` | Bloom | Message deduplication |
| `mnemosyne:mneme:audit:{tenant}` | List | Audit log (ClickHouse is the source of truth) |

Session listings for a user read the per-user ZSET instead of scanning the global index; each tenant
is backfilled once on first use (2,000 sessions per pipeline batch), and purge/rebuild maintain both
indexes.

Hot reads fall through to ClickHouse on a miss and repopulate Redis.

---

## Observability

**Metrics** (prefixed `mnemosyne_mneme_`, exposed at `GET /metrics`):

| Metric | Type | Description |
|--------|------|-------------|
| `http_requests_total` / `http_request_duration_seconds` | Counter / Histogram | HTTP QPS and latency |
| `pulsar_messages_total` | Counter | Messages published / consumed |
| `clickhouse_write_duration_seconds` / `rows_written_total` / `write_errors_total` | Histogram / Counter | Write throughput and failures |
| `consumer_lag` / `consumer_errors_total` | Gauge / Counter | Consumer backlog and errors |
| `dedup_filtered_total` | Counter | Bloom-filter dedup hits |
| `memory_extract_total` / `trace_aggregation_flush_total` | Counter | Extraction and aggregation runs |
| `evolution_total` | Counter | Summary evolution outcomes (`stage=session/user`, `result=evolved/skipped/error`) |
| `quality_check_total` | Counter | Data quality check results |
| `mneme_write_queue_size` / `mneme_write_duration_seconds` | Gauge / Histogram | Write backlog and write latency |

**Tracing**: set `telemetry.tracing_enabled: true` to export over OTLP HTTP. `otelhttp` injects the
trace id, which is also returned in the `X-Request-ID` response header.

**Logs**: Logrus JSON output with `trace_id` / `tenant_id` fields injected automatically.

**Alerting & autoscaling**: `deploy/hpa.yaml` provides HPA, ServiceMonitor, and PrometheusRule examples.

---

## Deployment

### Docker

```bash
make docker                       # builds mnemosyne-ai-systems/mneme:latest
docker run --rm -p 8080:8080 \
  -v $(pwd)/configs:/app/configs \
  mnemosyne-ai-systems/mneme:latest
```

### Docker Compose (dependencies + service)

```bash
make deps-up
docker compose -f deploy/docker-compose.yaml up -d mneme
```

### Kubernetes

```bash
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/deployment.yaml
kubectl apply -f deploy/k8s/service.yaml
kubectl apply -f deploy/hpa.yaml          # HPA + ServiceMonitor + PrometheusRule
```

---

## File Adapter

`file-adapter` turns local Markdown / JSONL / text files into memory events, with incremental
watching (fsnotify + debounce + resumable state).

```bash
./bin/file-adapter -config ./configs/config.yaml -dir ./data/inbox -mode http
```

| Flag | Description |
|------|-------------|
| `-config` | Path to the configuration file |
| `-dir` | Watch directory (overrides config) |
| `-scan` | Scan pre-existing files on startup (default `true`) |
| `-mode` | Delivery mode: `http` (default, recommended) or `pulsar` |

---

## Project Layout

```
.
├── cmd/
│   ├── mneme/            # main service entrypoint
│   └── file-adapter/     # file collector entrypoint
├── internal/
│   ├── api/              # HTTP routing, handlers, middleware, admin console, OpenAPI
│   ├── app/              # dependency wiring and lifecycle
│   ├── archiver/         # ClickHouse → Parquet → object storage, quality checks
│   ├── config/           # Viper configuration loading and defaults
│   ├── consumer/         # Pulsar consumers, trace aggregation, profile/summary extraction
│   ├── evolution/        # scheduled summary reflection and cross-session synthesis
│   ├── filewatcher/      # file watching and parsing
│   └── service/          # ingestion, profile/session stores, audit, HTTP sink
├── pkg/
│   ├── cache/            # Redis client and L2 cache
│   ├── logger/           # structured logging
│   ├── messagebus/       # Pulsar wrapper
│   ├── metrics/          # Prometheus metric definitions
│   ├── models/           # domain models and validation
│   ├── storage/          # storage abstractions and the ClickHouse implementation
│   └── tracing/          # OpenTelemetry wrapper
├── tools/yamlcheck/      # YAML syntax and duplicate-key checker (make yaml-lint)
├── configs/config.yaml   # sample configuration
├── deploy/               # docker-compose / k8s / HPA
└── mneme.http            # runnable request collection
```

Common commands: `make build`, `make test`, `make lint`, `make yaml-lint`, `make deps-up`, `make docker`.

---

## Roadmap

- [ ] Auto-create ClickHouse materialized views for the pre-aggregation tables (target tables ship today)
- [ ] Redis Cluster support (universal client + `{tenant}` hash tags + master-fanout index rebuild)
- [ ] Wire the L2 cache into the query path
- [x] Feed quality-check results into PrometheusRule alerts (enable `archiver.enabled`)
- [ ] Per-tenant quotas and rate limiting

---

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md), [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md), and
[SECURITY.md](SECURITY.md).

## License

[Apache License 2.0](LICENSE)
