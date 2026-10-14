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

### Changed

- **Module path**: `github.com/mnemosyne/mneme` → `github.com/mnemosyne-labs/mneme` (breaking —
  import paths must be updated).
- **Image name**: `mnemosyne/mneme` → `mnemosyne-labs/mneme`.
- **Documentation is now English-first**; code comments, the admin console UI, and OpenAPI
  descriptions remain in Chinese.

### Fixed

- `deploy/hpa.yaml`: removed duplicate `annotations` / `severity` keys in the PrometheusRule (which
  prevented Prometheus from loading the rule) and corrected the quality-check and consumer-lag
  alert expressions to match the real metric names.
- Added `tools/yamlcheck` (wired into `make yaml-lint` and CI) to catch YAML duplicate keys.

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

[Unreleased]: https://github.com/mnemosyne-labs/mneme/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/mnemosyne-labs/mneme/releases/tag/v0.1.0
