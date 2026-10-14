# 更新日志

本文件记录 Mneme 的重要变更，格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [未发布]

### 新增

- **README.md**：项目介绍、快速开始、API 一览、配置说明、存储模型与部署指南。
- **开源治理文件**：`LICENSE`（Apache-2.0）、`CONTRIBUTING.md`、`SECURITY.md`、`CODE_OF_CONDUCT.md`。
- **CI**：GitHub Actions（gofmt / vet / race 测试 / Docker 构建），Tag 发布自动构建多架构镜像与二进制。
- **Issue 与 PR 模板**、Dependabot 依赖升级配置。

### 变更

- **模块路径**：`github.com/mnemosyne/mneme` → `github.com/mnemosyne-labs/mneme`（不兼容变更，导入路径需同步）。
- **镜像名**：`mnemosyne/mneme` → `mnemosyne-labs/mneme`。

## [0.1.0] - 2026-10-14

首个可用版本。

### 核心能力

- 三条写入管道：会话历史、Agent 轨迹、长期记忆事件，经 Pulsar 解耦异步落库。
- 双存储模型：Redis 热数据（Hash / ZSET 索引 / TTL）+ ClickHouse 冷数据（压缩 + TTL）。
- 画像与总结自动萃取：规则实现开箱即用，可选 LLM 精炼（`memory.llm.enabled`）。
- 统一查询接口：实体画像、会话总结、用户聚合上下文。
- 数据治理：软/硬删除、游标分页、时间过滤、数据导出、审计日志（ClickHouse）。
- 内置管理后台 `/admin` 与 Swagger UI `/docs`。
- 可观测性：Prometheus 指标、OpenTelemetry Trace、结构化 JSON 日志、健康/就绪探针。
- 文件采集器 `file-adapter`：增量监听本地文件并投递为记忆事件。
- 运维能力：ClickHouse → Parquet → 对象存储归档、数据质量巡检、HPA 与告警规则样例。

[未发布]: https://github.com/mnemosyne-labs/mneme/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/mnemosyne-labs/mneme/releases/tag/v0.1.0
