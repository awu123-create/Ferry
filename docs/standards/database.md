# 数据库与迁移规范（Database Schema Standard）

在 `migrations/` 或 `internal/infra/pg/` 新增或修改表、列、索引、约束、触发器或迁移脚本时阅读本文件。通用分级与调用方原则见 [CODING_STANDARDS.md](../../CODING_STANDARDS.md)；DML 读写与并发围栏见 [docs/standards/go/code.md](./go/code.md)；真库与迁移测试见 [docs/standards/go/testing.md](./go/testing.md)。

表只存储代码真实读取的**业务事实**，Schema 用数据库约束守住不变量。

## 表、列、索引与触发器

- **DDL 归 Goose，DML 归 GORM**：一切建表、改列、唯一约束、`CHECK` 约束、部分索引和 `pg_notify` 触发器一律写在 `migrations/NNNN_<slug>.sql`，**严禁启用 GORM `AutoMigrate`**（见 `docs/adr/0006-tech-stack-and-engineering-tooling.md`）。
- **调用方原则（Callers）**：任何表、列、索引或枚举值必须在本 PR 中有生产代码读或写，禁止为后续 Milestone 提前预埋未使用的字段或索引（见 [CODING_STANDARDS.md](../../CODING_STANDARDS.md)）。
- **数据库约束兜底**：
  - 表间引用显式声明 `REFERENCES` 外键，并按生命周期决定是否级联删除（如 `task_attempts`、`outbox` 随 `tasks` 级联 `ON DELETE CASCADE`，`tasks.endpoint_id` 不级联删除历史任务）；
  - 封闭状态枚举与数值上下限在库层加 `CHECK` 约束（如 `tasks.status` 五态枚举、`timeout_ms BETWEEN 100 AND 60000`、`rate_limit_qps > 0`）。
- **主键、时间与载荷类型**：
  - 核心聚合表（`endpoints`、`tasks`）主键采用 Go 端生成的 `UUIDv7`（`uuid.NewV7()`），按时间递增防止随机 `UUIDv4` 打碎 PG B-Tree 索引页；只追加流水/队列表（`task_attempts`、`outbox`）采用 `BIGSERIAL`；
  - 所有时间戳列使用 `TIMESTAMPTZ`，默认值写 `now()`；
  - 需参与幂等哈希比对和 HMAC 签名的原始报文（`tasks.payload`）必须用 `BYTEA` 原样存储，**禁止用 `JSONB`**（防止键序与空白重排破坏签名，见 `docs/adr/0008-concurrency-fencing-and-flow-control-hardening.md`）。
- **无瞬态 `RUNNING` 与部分索引对齐**：不在热路径向 `tasks.status` 写瞬态 `RUNNING`（见 `docs/adr/0007-tactical-ddd-and-deep-module-seams.md`）；部分索引的 `WHERE` 谓词必须与查询代码条件严格一致，确保查询可命中索引。

**Judgement.**

- 定义与执行记录分表：`endpoints` 存配置契约、`tasks` 存任务、`task_attempts` 只追加投递明细，不把多次执行覆盖进主表。
- 能由其他列推导出的状态不单独加冗余列，减少状态不一致风险。
- 每个查询都会回退成固定默认值的列，直接设 `NOT NULL DEFAULT <值>`（如 `signing_secret TEXT NOT NULL DEFAULT ''`、`last_error TEXT NOT NULL DEFAULT ''`），避免 Go 层反复处理可空指针；仅当“缺失本身具有独立业务语义”时才允许 `NULL`（如尚未结束任务的 `completed_at`）。

## Goose 迁移规范（Migrations）

- 迁移脚本存放于 `migrations/NNNN_<slug>.sql`（4 位递增编号，如 `0001_init.sql`），通过 `migrations/embed.go` 嵌入二进制，经 `pg.Store.RunMigrations` 与 `make migrate` 统一执行。
- 每个脚本必须同时提供 `-- +goose Up` 与 `-- +goose Down`；函数与触发器块用 `-- +goose StatementBegin` / `-- +goose StatementEnd` 包裹；`Down` 必须完整撤销 `Up` 变更（涉及数据搬迁或列拆分时，`Down` 须还原回上一版二进制可读回的形态）。
- **版本号必须高于 `main`**：当前分支新增的迁移编号必须严格高于 `main` 上最新版本号。拉齐 `main`（`git rebase origin/main`）后如发生撞号，**保留 `main` 已有版本号不动**，用 `git mv` 将本分支新增的全部迁移按原顺序整体往后顺延（例如分支的 `0002_create` 与 `0003_alter` 在 `main` 新增 `0002` 后，必须整体挪为 `0003` 与 `0004`，不能只挪前一个导致建表与改表顺序倒挂），同步更新测试中引用的版本号并重跑 `make test-integration`（联动 [docs/agents/github.md](../agents/github.md)）。

**Judgement.** 同一尚未合入 `main` 的 PR 内，若对本 PR 新写的迁移发现遗漏或错误，直接原地修改该脚本，不额外追加第二个补丁迁移文件。
