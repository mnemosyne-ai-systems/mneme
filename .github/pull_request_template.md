## 变更说明

<!-- 简要描述这次改动做了什么、为什么。关联 Issue：Fixes #123 -->

## 变更类型

- [ ] 功能（feat）
- [ ] 修复（fix）
- [ ] 性能（perf）
- [ ] 重构（refactor，不改变外部行为）
- [ ] 文档（docs）
- [ ] 构建 / CI

## 影响面

- [ ] 不涉及 API 契约变更
- [ ] 涉及 API 契约变更（请在下方说明，并同步 `/openapi.json`）
- [ ] 涉及 ClickHouse DDL（请说明**存量数据**如何迁移，`CREATE TABLE IF NOT EXISTS` 不会修改已有表）
- [ ] 新增 / 修改配置项（请同步 `configs/config.yaml` 与 `README.md` 配置表）
- [ ] 涉及并发 / 数据一致性（请说明锁、事务、幂等策略）

## 验证方式

<!-- 贴出你实际执行的命令与结果，以及必要的请求示例 -->

```bash
gofmt -l .
go vet ./...
go test ./... -race -count=1
```

- [ ] `gofmt -l .` 无输出
- [ ] `go vet ./...` 通过
- [ ] `go test ./... -race -count=1` 通过
- [ ] 已在本地起服务并实际验证接口（如涉及 HTTP 变更）

## 截图 / 日志

<!-- 后台页面变更请附截图；日志请脱敏 -->
