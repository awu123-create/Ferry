# Pull Requests 与分支协作（GitHub）

本仓库采用单主干（`main`）开发模式，所有交付改动通过 Pull Request 合入 `main`，统一使用 `gh` CLI 操作。

## 目标分支（Target）

```bash
gh pr create --base main --title "..." --body "..."
```

- `main` 受 GitHub Ruleset `Protect main` 保护：禁止直推、禁止 force-push、禁止删除分支。
- 单个 PR 只覆盖一个可独立验收的小切片；标题遵循 Conventional Commits（如 `feat(m1): ...`、`chore: ...`）。
- PR 正文遵循 `.github/pull_request_template.md` 模板填写。
- 合入 `main` 默认使用 **Squash merge**（保持 `main` 单切片线性历史）。合入后及时删除远端源分支、本地分支与对应 `.worktrees/` 目录。

## 源分支：更新与重开规则（Source branch）

操作前先确认当前分支对应的远端源分支 `<source>`：

```bash
git status -sb
```

`git status -sb` 输出形如 `local...origin/<source>` 时，`<source>` 为 PR 源分支；若没有 `...`，说明尚未建立上游跟踪，`<source>` 即当前本地分支名。查询已有 PR 时以 `<source>` 为准：

```bash
gh pr list --head "<source>" --state all
```

- **存在 `OPEN` PR**：在原分支继续提交并运行 `git push` 更新 PR；若刚执行过 `rebase`，仅允许使用 `git push --force-with-lease`（严禁裸 `--force`）。
- **尚无远端分支**：首次发布时使用 `git push -u origin HEAD`。
- **仅存在 `MERGED` 或 `CLOSED` PR**：禁止复用旧分支继续提交。`main` 上的 Squash merge 提交即文件真相，必须从最新 `origin/main` 切出新的 `<type>/<id>-<slug>` 分支承载新改动，并清理旧分支与对应 worktree。
- **跨切片或无关改动**：不混入当前分支，从最新 `origin/main` 另开新分支（并行开发使用独立 `.worktrees/<slug>`）。

## 拉齐 `main` 与合入校验清单

当 `main` 推进后，个人 / 单 Agent 分支通过 `rebase` 拉齐最新基线：

```bash
git fetch origin
git rebase origin/main
git push --force-with-lease
```

（若存在多人或多 Agent 共享同一远程分支，改用 `git merge origin/main` + 普通 `git push`，避免改写共享历史。）

拉齐 `main` 后仅解决冲突不算完成；在推送更新或合入 `main` 前，必须确认以下清单全部满足：

1. **工作区与基础门禁**
   - `git status` 干净，无遗留临时文件或跨切片越界改动；
   - 改动文件通过 `pre-commit run --files <changed-files>`（或 `pre-commit run --all-files`，覆盖尾部空白、EOF 换行、YAML、冲突标记、私钥检测）。
2. **Go 静态检查、单测与构建**（涉及 Go 代码或 lint 配置时）
   - `make fmt`
   - `make lint`（`golangci-lint config verify` + `golangci-lint run ./...`，含 `depguard` 分层与 `json_v2` 门禁）
   - `make test`（`go test -race -short ./...`）
   - `make build`
3. **集成与黑盒验收**（涉及数据库迁移、存储适配器、中间件、调度或投递链路时）
   - 启动容器 `make up` 后跑通 `make test-integration`（`go test -race -count=1 ./...`）；
   - 涉及里程碑验收时补跑对应 `scripts/verify_*.sh`。
4. **Migration 编号顺序检查**（涉及 `migrations/` 时）
   - 分支新增的 `migrations/NNNN_*.sql` 编号必须严格大于 `main` 上最新迁移版本号；
   - 拉齐 `main` 后如发生撞号或倒挂，保留 `main` 已有版本号不动，用 `git mv` 按序顺延本分支所有新增迁移文件，同步修正测试中写死的版本号后重跑 `make test-integration`；
   - 同一未合入 PR 内对新增迁移的修正直接原地修改，不追加补丁迁移文件。
5. **ADR 与领域词表一致性**
   - 涉及 `docs/adr/` 时确保 `docs/adr/NNNN-*.md` 编号唯一（`adr-number-unique`）；
   - 术语遵守 `GLOSSARY.md`（无禁用同义词），改动未静默违背既有 ADR。
6. **GitHub PR 门禁与单向门确认**
   - 分支已与最新 `origin/main` 同步（满足 Ruleset `strict_required_status_checks_policy`）；
   - `Protect main` 必需检查全绿：`Lint, Test & Build`、`Integration Tests (PG + Kafka + Redis)`；
   - 涉及 `migrations/`、密钥/配置、删除逻辑等单向门改动时，明确说明影响范围与回滚/补救方案，经人工最终确认后再执行 Squash merge。
