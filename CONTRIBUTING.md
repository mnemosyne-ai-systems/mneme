# Contributing to Mneme

Thanks for your interest in improving Mneme. This document covers the development setup, the
submission workflow, and what we look for in each kind of contribution.

## Development environment

| Dependency | Version |
|------------|---------|
| Go | 1.25+ |
| Docker / Docker Compose | runs Pulsar, ClickHouse, and Redis locally |
| ClickHouse | 24.3+ (tables are created by `auto_migrate`) |
| Redis | 7.x |
| Pulsar | 3.x |

### Start dependencies

```bash
make deps-up      # start Pulsar + ClickHouse + Redis and initialize topics
make deps-down    # stop them and remove the volumes
```

### Everyday commands

```bash
make build        # build into ./bin/mneme
make run          # run locally
make test         # go test ./... -race -count=1
make test-cover   # coverage report
make lint         # gofmt -s -w . && go vet ./... && make yaml-lint
make yaml-lint    # validate YAML under deploy/configs/.github (syntax + duplicate keys)
make tidy         # go mod tidy
```

Before submitting, make sure all of the following pass:

```bash
gofmt -l .              # must print nothing
go vet ./...
go test ./... -race -count=1
go run ./tools/yamlcheck    # required when touching deploy/ or configs/
```

CI runs the same checks and will reject unformatted code or failing tests.

## Code style

- Follow [Effective Go](https://go.dev/doc/effective_go) and the surrounding code style:
  consistency with the existing code beats personal preference.
- Exported identifiers (types, functions, constants) need doc comments that start with the
  identifier name; package comments live in `doc.go` or above the package clause.
- Prefer the standard library. Every new third-party dependency must be justified in the PR
  description.
- Log through `pkg/logger` with structured fields; never `fmt.Println`. Never log secrets, tokens,
  or raw user content.
- Wrap errors with context: `fmt.Errorf("clickhouse: flush history: %w", err)`.
- Keep the `mneme:*` key prefix for Redis and database objects so they never collide with other
  systems.

## Layering

| Directory | May depend on |
|-----------|---------------|
| `internal/api` | `internal/service`, `pkg/*` — must not talk to ClickHouse / Redis directly |
| `internal/service` | `pkg/storage`, `pkg/cache` — must stay HTTP-agnostic |
| `internal/consumer` | `pkg/messagebus`, `internal/service` |
| `pkg/*` | usable by any layer; must never import `internal/*` |

## Commit messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <subject>

[body]

[footer]
```

`type` is one of `feat`, `fix`, `perf`, `refactor`, `docs`, `test`, `build`, `ci`, `chore`.

Examples:

```
feat(cache): support tenant-scoped ZSET index keys
fix(inserter): panic when requeueing after close
perf(consumer): lower trace aggregation window from 30s to 10s
```

One concern per commit; do not mix formatting or renames with logic changes.

## Pull requests

1. Branch off `main`: `git checkout -b feat/xxx`.
2. Run `make lint && make test` before pushing.
3. The PR description must cover:
   - what changed and why,
   - the key implementation approach,
   - the blast radius (config keys, database DDL, API contract),
   - how you verified it (commands, sample requests, screenshots).
4. New configuration keys must also update `configs/config.yaml` and the tables in `README.md`.
5. DDL changes must explain how **existing** data is migrated — `CREATE TABLE IF NOT EXISTS` does
   not modify tables that already exist.
6. Link the issue: `Fixes #123`.

## Issues

Search existing issues first. A useful report includes reproduction steps, expected behavior,
actual behavior, log excerpts, version / commit, and the deployment mode (Docker / Kubernetes).

## Discussions

Design questions, performance trade-offs, and architecture changes should start as a Discussion
with background, alternatives, and your preferred direction.

## License

By contributing you agree that your contribution is licensed under the
[Apache-2.0](LICENSE) license.
