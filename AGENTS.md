# Ferry

- 任务跟踪、分支命名（`<type>/<id>-<slug>`）与 `docs/specs/` 规划边界：`docs/agents/issue-tracker.md`；Triage 标签词表：`docs/agents/triage-labels.md`。
- 领域术语、架构设计与 ADR 约束：`docs/agents/domain.md`（词表 `GLOSSARY.md`、架构 `docs/ARCHITECTURE.md`、决策 `docs/adr/`）。
- 本地工具安装、测试分层归属、真容器隔离与项目专属编码约定：`CONTRIBUTING.md`。
- 任何代码改动先读 `CODING_STANDARDS.md`，再按场景读对应规范：
  - Go 业务代码（`internal/`、`cmd/`）：`docs/standards/go/code.md`；包分层与深模块边界见 `docs/ARCHITECTURE.md` 与 `docs/adr/0007-tactical-ddd-and-deep-module-seams.md`。
  - Go 测试与集成流水线：`docs/standards/go/testing.md`。
  - PostgreSQL 表结构与 Goose 迁移（`migrations/`、`internal/infra/pg/`）：`docs/standards/database.md`。
  - 变更对外行为/契约、新增 ADR 或维护 `docs/`：`docs/standards/records.md`。
  - 严苛可维护性审查（Thermo-nuclear review）：`.agents/skills/thermo-nuclear-code-quality-review/SKILL.md`。
- GitHub PR、源分支更新/重开与合入 `main` 校验：`docs/agents/github.md`（PR 正文模板 `.github/pull_request_template.md`）。
