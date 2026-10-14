# 贡献指南

感谢你愿意为 Mneme 贡献代码。本文档说明开发环境、提交流程与各类贡献的期望。

## 开发环境

| 依赖 | 版本 |
|------|------|
| Go | 1.25+ |
| Docker / Docker Compose | 用于本地拉起 Pulsar、ClickHouse、Redis |
| ClickHouse | 24.3+（`auto_migrate` 会自动建表） |
| Redis | 7.x |
| Pulsar | 3.x |

### 拉起依赖

```bash
make deps-up      # 启动 Pulsar + ClickHouse + Redis 并初始化 Topic
make deps-down    # 停止并清理数据卷
```

### 日常开发

```bash
make build        # 构建到 ./bin/mneme
make run          # 本地运行
make test         # go test ./... -race -count=1
make test-cover   # 覆盖率报告
make lint         # gofmt -s -w . && go vet ./... && make yaml-lint
make yaml-lint    # 校验 deploy/configs/.github 下的 YAML（语法 + 重复键）
make tidy         # go mod tidy
```

提交前请确保以下三项全绿：

```bash
gofmt -l .        # 应无输出
go vet ./...
go test ./... -race -count=1
go run ./tools/yamlcheck   # 修改了 deploy/ 或 configs/ 时必跑
```

CI 会自动执行同样的检查，格式不通过或测试失败都会被拦截。

## 代码风格

- 遵循 [Effective Go](https://go.dev/doc/effective_go) 与现有代码风格，保持一致优先于个人偏好。
- 导出的标识符（类型、函数、常量）必须有以标识符名开头的注释；包注释写在 `doc.go` 或包声明上方。
- 优先使用标准库能力，新增第三方依赖需在 PR 描述中说明理由。
- 日志使用 `pkg/logger`（结构化字段），不要直接 `fmt.Println`；不要打印密钥、令牌、用户原始内容。
- 错误需包装上下文：`fmt.Errorf("clickhouse: flush history: %w", err)`。
- 数据库与缓存的 Key 命名沿用 `mneme:*` 前缀，避免与其他系统冲突。

## 目录职责

| 目录 | 可否新增依赖方向 |
|------|------------------|
| `internal/api` | 依赖 `internal/service`、`pkg/*`；不得直接访问 ClickHouse / Redis |
| `internal/service` | 依赖 `pkg/storage`、`pkg/cache`；不感知 HTTP |
| `internal/consumer` | 依赖 `pkg/messagebus`、`internal/service` |
| `pkg/*` | 可被任意层依赖，不得反向依赖 `internal/*` |

## 提交信息

遵循 [Conventional Commits](https://www.conventionalcommits.org/)：

```
<type>(<scope>): <subject>

[body]

[footer]
```

`type` 取值：`feat`、`fix`、`perf`、`refactor`、`docs`、`test`、`build`、`ci`、`chore`。

示例：

```
feat(cache): 支持按租户隔离的 ZSET 索引键
fix(inserter): 关闭时回队数据导致 panic
perf(consumer): 轨迹聚合窗口从 30s 降到 10s
```

一个提交只做一件事；不要把格式化、重命名与逻辑改动混在同一个提交中。

## Pull Request

1. 从 `main` 切出特性分支：`git checkout -b feat/xxx`。
2. 提交前跑通 `make lint && make test`。
3. PR 描述需说明：
   - 改动目的与背景
   - 关键实现思路
   - 影响面（是否涉及配置项、数据库 DDL、API 契约）
   - 验证方式（命令、请求示例、截图）
4. 若新增配置项，同步更新 `configs/config.yaml` 与 `README.md` 配置表。
5. 若涉及 DDL 变更，说明存量数据的迁移方式（`CREATE TABLE IF NOT EXISTS` 不会修改已有表）。
6. 关联 Issue：`Fixes #123`。

## Issue

提交前请搜索是否已有同类问题。Issue 请包含：复现步骤、期望行为、实际行为、日志片段、版本 / commit、部署方式（Docker / K8s）。

## 提问与讨论

功能设计、性能权衡、架构变更请先开 Discussion，附上背景、备选方案与你的倾向，便于讨论。

## 许可

提交即表示你同意你的贡献以 [Apache-2.0](LICENSE) 协议授权。
