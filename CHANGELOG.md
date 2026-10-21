# Changelog

All notable changes to Mneme are documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versioning follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- **README.md** — overview, features, architecture, quick start, API reference, configuration
  table, storage model, observability, and deployment guide.
- **Open source governance files** — `LICENSE` (Apache-2.0), `CONTRIBUTING.md`, `SECURITY.md`,
  `CODE_OF_CONDUCT.md`.
- **CI** — GitHub Actions running gofmt checks, `go vet`, YAML validation, race-enabled tests with
  coverage, builds, and a Docker build; tagged releases build multi-architecture images and binaries
  and publish a GitHub Release automatically.
- **Issue and PR templates**, plus Dependabot for Go modules, GitHub Actions, and Docker.
- **Self-evolving summaries** (optional) — a background `evolution` job periodically re-reads a
  session's recent history and rewrites its summary for up to `evolution.max_rounds` rounds, then
  folds recent session summaries into a cross-session `evolving_summary` field on the user profile.
  Requires `memory.llm.enabled` and `consumer.session_summary_enabled`. New metric:
  `mnemosyne_mneme_evolution_total{stage,result}`.

### Changed

- **Module path**: `github.com/mnemosyne/mneme` → `github.com/mnemosyne-ai-systems/mneme` (breaking —
  import paths must be updated).
- **Image name**: `mnemosyne/mneme` → `mnemosyne-ai-systems/mneme`.
- **Documentation is now English-first**; code comments, the admin console UI, and OpenAPI
  descriptions remain in Chinese.
- **User session listing** now uses a per-user ZSET index
  (`mnemosyne:mneme:sessions:{tenant}:user:{user_id}`) instead of scanning every session hash;
  missing indexes are backfilled on demand and by the admin "rebuild indexes" action.

### Fixed

- `deploy/hpa.yaml`: removed duplicate `annotations` / `severity` keys in the PrometheusRule (which
  prevented Prometheus from loading the rule) and corrected the quality-check and consumer-lag
  alert expressions to match the real metric names.
- Added `tools/yamlcheck` (wired into `make yaml-lint` and CI) to catch YAML duplicate keys.
- **Code review fixes**:
  - `POST /api/v1/sessions` now creates empty sessions (summary is filled later by the summarizer).
  - Session-summary and history-profile consumers no longer overwrite messages buffered while a
    summarization/extraction run is in flight; history dedup marks IDs only after handing data to
    the writer, with in-buffer duplicate filtering preserved.
  - `BatchInserter`: buffered audit logs are flushed on shutdown, `max_queue_size` and
    `max_batch_bytes` are now enforced, and the adaptive-batch configuration is actually wired
    through; `clickhouse.adaptive_batch_enabled: false` is respected.
  - Archiver refuses to drop a partition when neither upload nor a locally visible export succeeded.
  - Admin API: constant-time token comparison and no token material in audit `actor`; no nil
    summarizer/session-store panics when features are disabled.
  - Session summary storage clears `_deleted` on re-summarize, refreshes ZSET index TTL, removes
    index members on purge, falls back to SCAN for filtered searches, and keeps session time
    ranges monotonic.
  - Redis bloom dedup rotates its bitmap daily (48h TTL) and fixed inverted RedisBloom semantics.
  - Quality checker: removed non-existent `_deleted` predicates, propagates context, emits
    `mnemosyne_mneme_quality_check_total`, no longer deadlocks when stopped without start, and is
    wired into the app (`archiver.quality_check_interval`).
  - LLM fallback summaries now include the tail messages; `L2Cache.Get` no longer races on the LRU
    timestamp; creating a user with an empty profile persists the entity.

## [0.1.0] - 2026-10-14

First usable release.

### Core capabilities

- Three ingestion pipelines — conversation history, agent traces, and explicit memory events —
  decoupled through Pulsar and persisted asynchronously.
- Hot/cold storage split: Redis (Hash, ZSET indexes, TTL) for reads, ClickHouse (compression, TTL)
  for retention and analytics.
- Automatic profile and summary extraction, rule-based out of the box with optional LLM refinement
  (`memory.llm.enabled`).
- Unified query API: entity profiles, session summaries, and aggregated user context.
- Data governance: soft/hard delete, cursor pagination, time filtering, export, and an audit log in
  ClickHouse.
- Admin console at `/admin` and Swagger UI at `/docs`.
- Observability: Prometheus metrics, OpenTelemetry tracing, structured JSON logs, and
  health/readiness probes.
- `file-adapter` for incremental ingestion of local files as memory events.
- Operations: ClickHouse → Parquet → object storage archival, data quality checks, and HPA/alerting
  examples.

[Unreleased]: https://github.com/mnemosyne-ai-systems/mneme/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/mnemosyne-ai-systems/mneme/releases/tag/v0.1.0
