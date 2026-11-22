# Triage 标签映射

工程 skill 使用 5 个标准 triage 状态角色。本表将这 5 个角色映射到本仓库 Issue tracker 实际使用的标签名。

| mattpocock/skills 角色 | 本仓库实际标签    | 含义                             |
| ---------------------- | ----------------- | -------------------------------- |
| `needs-triage`         | `needs-triage`    | 待维护者评估                     |
| `needs-info`           | `needs-info`      | 信息不足，等待提交者补充         |
| `ready-for-agent`      | `ready-for-agent` | 规格明确，可交由 AFK Agent 实现  |
| `ready-for-human`      | `ready-for-human` | 需要人工决策或人工实现           |
| `wontfix`              | `wontfix`         | 不处理并关闭                     |

当 skill 提及某个角色（例如“应用 AFK-ready triage 标签”）时，使用上表“本仓库实际标签”列对应的字符串。

若后续调整仓库标签词表，直接修改上表第二列即可。
