## Summary

<!-- What changed and why. Link the issue: Fixes #123 -->

## Type

- [ ] Feature (`feat`)
- [ ] Fix (`fix`)
- [ ] Performance (`perf`)
- [ ] Refactor (no behavior change)
- [ ] Documentation (`docs`)
- [ ] Build / CI

## Impact

- [ ] No API contract change
- [ ] API contract change (describe it and update `/openapi.json`)
- [ ] ClickHouse DDL change (explain how **existing** data is migrated — `CREATE TABLE IF NOT EXISTS`
      does not modify existing tables)
- [ ] New or changed configuration keys (update `configs/config.yaml` and the README config table)
- [ ] Concurrency or consistency sensitive (describe locking, transactions, idempotency)

## Verification

<!-- Commands you ran, plus sample requests where relevant -->

```bash
gofmt -l .
go vet ./...
go test ./... -race -count=1
go run ./tools/yamlcheck
```

- [ ] `gofmt -l .` prints nothing
- [ ] `go vet ./...` passes
- [ ] `go test ./... -race -count=1` passes
- [ ] Started the service locally and exercised the affected endpoints

## Screenshots / logs

<!-- Screenshots for console changes; redact secrets in logs -->
