# 领域文档消费规则

规定各工程 skill 在探索本仓库代码时如何读取与遵循领域文档。

本仓库采用**单上下文（single-context）**结构。

## 探索代码前先读这些文件

- 根目录 **`GLOSSARY.md`**：统一语言（Ubiquitous Language）与禁用同义词表。
- **`docs/adr/`**：阅读与当前改动区域相关的架构决策记录（ADR）。

若其中某个文件暂不存在，**静默继续**，不要提醒缺失，也不要主动建议提前补全。`/domain-modeling`（经由 `/grill-with-docs` 与 `/improve-codebase-architecture` 触发）会在术语或决策真正敲定时按需创建。

## 目录结构

单上下文仓库结构：

```
/
├── GLOSSARY.md
├── docs/adr/
│   ├── 0001-unified-http-task-delivery-model.md
│   └── 0002-transactional-outbox-batch-relay.md
├── cmd/
└── go.mod
```

## 遵守词汇表用语

在输出中涉及领域概念（Issue 标题、重构方案、问题假设、测试命名等）时，使用 `GLOSSARY.md` 中定义的标准术语，不要漂移到词汇表显式排除（`_Avoid_`）的同义词。

若所需的领域概念尚未出现在 `GLOSSARY.md` 中，说明可能正在引入项目未约定的新词（需先审视是否复用已有概念），或确实存在领域建模空白（记录下来交由 `/domain-modeling` 处理）。

## 显式标注 ADR 冲突

如果输出方案与已有 ADR 相矛盾，必须显式标出而非静默覆盖：

> _与 ADR-0001（统一 HTTP 任务投递模型）冲突，值得重新讨论的原因：…_
