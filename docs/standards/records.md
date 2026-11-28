# 记录同步规范（Records Standard）

当改动改变系统对外行为或契约（REST API、错误码、环境变量、CLI 参数、数据库表结构、测试或 CI 门禁），或新增 ADR 与维护 `docs/` 文档时阅读本文件。通用分级见 [CODING_STANDARDS.md](../../CODING_STANDARDS.md)。

## 文档职责与单真相源

每类信息只保留唯一权威落点，改变系统现行行为时在**同一个 PR** 中同步对应活文档，不留“后续再补文档”的尾巴：

- [docs/ARCHITECTURE.md](../ARCHITECTURE.md)：唯一随系统演进持续更新的**活架构文档**，承载组件职责、Redis Key 拓扑、核心数据流、状态机、目标表结构与模块分层边界；
- [CONTRIBUTING.md](../../CONTRIBUTING.md)：本地工具安装、日常校验命令、测试分层归属、真容器测试隔离与项目专属编码约定；
- [GLOSSARY.md](../../GLOSSARY.md)：统一领域术语与 `_Avoid_` 禁用同义词（不含基础设施实现细节）；
- [README.md](../../README.md)：面向使用者与初访者的项目简介、核心特性、快速启动命令与文档入口；
- `docs/standards/{database.md,go/*.md}`：跨项目通用的编码、测试与数据库规范（机械检查项直接交由 `.golangci.yml`、`.pre-commit-config.yaml`、`Makefile` 与自动化测试执行，不在文档中重复抄写插件或钩子清单）；
- `docs/standards/records.md` 与 `docs/agents/*.md`：本仓库的文档同步、工单跟踪与分支协作规约；
- `docs/adr/NNNN-*.md`：**冻结的历史架构决策日志**（记录决策当时的背景、备选方案与取舍）。

## ADR 冻结与演进原则

- **按需新建 ADR**：仅当一项决策同时满足“逆转成本高、脱离背景令人困惑、存在真实备选方案权衡”三条时才新建 ADR；常规功能迭代、参数调整、超时补齐或门禁加固不新建 ADR。
- **已采纳 ADR 正文冻结**：ADR 一旦状态定为“已采纳”，正文作为当时的历史决策记录保持冻结；后续日常参数、内部函数名、Redis Key 细则或工具链演进只更新活文档 [docs/ARCHITECTURE.md](../ARCHITECTURE.md) 与 [CONTRIBUTING.md](../../CONTRIBUTING.md)，除修复链接格式或校准首次建档遗留的时态与造词外，不再回头改写旧 ADR 正文。
- **重大路线取代**：只有未来某项新架构决策推翻了旧 ADR 的核心方案时，才新增一篇新编号 ADR，并在旧 ADR 顶部状态行补注“已被 `[ADR-NNNN](./NNNN-xxx.md)` 取代（或部分取代）”。
- **提案态 ADR（Proposed）**：若某项重大架构决策的代码将在后续 PR 落地，新增 ADR 状态标为 `Proposed（提案中）`，待实现 PR 合入 `main` 时同步改为 `Accepted（已采纳）` 并更新 [docs/ARCHITECTURE.md](../ARCHITECTURE.md)。

## 以当前代码树为真相

- 活文档（`docs/ARCHITECTURE.md`、`CONTRIBUTING.md`、`README.md`）中的现行目录树、路由、迁移号与可执行命令只描述当前分支磁盘上实际存在的内容，不预先填入尚未落地的目录或文件。
- 跨阶段路线图、未来里程碑拆分与验收规划放在 `docs/specs/`（如 [docs/specs/roadmap.md](../specs/roadmap.md)），仅作规划上下文，不冒充现行实现，也不在其中镜像活工单状态（见 [docs/agents/issue-tracker.md](../agents/issue-tracker.md)）。

**Judgement.**

- 新段落如果只是把另一份权威文件或工具配置已有的内容重抄一遍（重复目录树、重复状态表、重复 linter 列表），应删除并改为指向权威文件的链接。
- 单次 PR 的拆解过程、踩坑日志与评审讨论纪要留在 PR 正文或 Issue 评论中，不写入 `docs/`。
