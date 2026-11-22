# 记录同步规范（Records Standard）

当改动改变系统对外行为或契约（REST API、错误码、环境变量、CLI 参数、数据库表结构、测试或 CI 门禁），或新增 ADR 与 `docs/` 文档时阅读本文件。通用分级见 [CODING_STANDARDS.md](../../CODING_STANDARDS.md)。

## 权威记录与同 PR 同步

改变系统行为的改动，必须在**同一个 PR** 中同步更新所有声明该行为的权威记录，不留“后续再补文档”的尾巴。

Ferry 的权威记录清单：

- [docs/ARCHITECTURE.md](../ARCHITECTURE.md)：系统架构、核心数据流、状态机与当前代码树边界；
- `docs/adr/NNNN-*.md`：决定具体设计取舍的架构决策记录；
- [GLOSSARY.md](../../GLOSSARY.md)：统一领域术语与 `_Avoid_` 禁用同义词；
- [README.md](../../README.md)：项目概览、当前仓库树、开发与校验命令；
- `docs/standards/*.md` 与 `docs/agents/*.md`：编码、测试、数据库、记录与协作规范。

## 无双规并存

- 被取代的规则直接改写或删除；在 ADR 中如需保留演进背景，必须改用**过去时态**说明初版做法并链接取代它的新 ADR。
- 禁止只在文档顶部挂一句“已被修订”，而正文仍以现行规范口吻保留旧规则，造成两套规则同时生效。

## 以当前代码树为真相

- 权威记录只描述当前分支磁盘上实际存在的代码树与已落地契约。现行目录树、路由表、迁移号与可执行命令不得预先填入尚未落地的目录或文件；先改代码（或在同一次 PR 中同步改代码与记录）。
- 跨阶段路线图、未来里程碑拆分与验收规划放在 `docs/specs/`（如 [docs/specs/roadmap.md](../specs/roadmap.md)），仅作规划上下文，不冒充现行实现，也不在其中镜像活工单状态（见 [docs/agents/issue-tracker.md](../agents/issue-tracker.md)）。

## 延后落地的架构提案（Proposed ADR）

- 若某项架构决策的代码将在后续 PR 中落地，新增 ADR 状态行标为 `Proposed（提案中）`，并在影响章节列出实现 PR 合入时需要同步改写的记录清单。
- 代码真正合入 `main` 之前，`docs/ARCHITECTURE.md`、`README.md` 与既有 ADR 继续描述现行代码树；实现 PR 合入时同步将提案 ADR 改为 `Accepted（已采纳）` 并改写对应记录。若 `GLOSSARY.md` 提前收录尚未落地的新概念，必须显式标注为提案态。

**Judgement.**

- 新文档如果只是把另一份权威记录已拥有的内容重抄一遍（重复目录树、重复状态表、重复命令说明），应删除并改为指向权威文档的链接。
- 某次 PR 的拆解过程、踩坑日志、评审讨论纪要属于 PR 正文或 Issue 评论，不落盘 `docs/`；只有后续维护者需要、且代码本身无法表达的长期知识，才值得写入 `docs/`。
