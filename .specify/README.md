# 在 Krim 中使用 Spec Kit

Spec Kit 1.3.2 用于开发 Krim，不是产品的运行时依赖。项目技能位于 `.claude/skills/`；原生事件处理器关闭，只由用户手动调用，不安装 Git 扩展。

## 开始一个功能

先阅读根目录 `CLAUDE.md` 和 [.specify 项目原则](memory/constitution.md)，检查现有实现。适合使用本流程的是范围明确的新功能或跨模块变更；简单缺陷修复、局部调整可以直接处理。

以下命令在聊天输入框中逐条发送，不是在终端执行。将第一条中的功能描述换成实际需求，每一步检查产物后再继续。

```text
/speckit-specify 描述需要新增或修改的功能、现有行为和验收要求。
```

```text
/speckit-plan 沿用 Krim 现有架构、接口和组件；明确修改范围、兼容要求、必要验证，以及仍需用户批准的事项。
```

```text
/speckit-tasks 按文件、接口和依赖拆分任务，明确验收方式；只并行互不依赖且文件不重叠的任务。
```

```text
/speckit-implement
```

需要澄清需求时，在计划前用 `/speckit-clarify`；计划和任务完成后可用 `/speckit-analyze` 检查一致性，用 `/speckit-checklist` 检查需求质量。实现后可用 `/speckit-converge` 核对差距。`/speckit-constitution` 用于明确要求的项目原则调整。

规格、计划和任务通过不代表已批准修改 UI。涉及既有控件的调整仍须按 `CLAUDE.md` 单独确认，实际打开受影响界面验收。

## 文件与 Git

- `specs/<功能目录>/`：功能规格、方案、任务及相关资料，默认仅留本地，不发布；`docs/` 的既有忽略规则不变。
- `.specify/feature.json`：当前 checkout 的功能指针，仅留本地；它不依赖 Git 分支名称。
- `.specify/memory/constitution.md`：Krim 项目原则。
- `.specify/templates/`、`.specify/scripts/`、`.specify/integrations/`：官方模板、脚本和安装清单，保留原文件及校验值。

继续已有功能前，确认功能资料和当前指针指向正确目录。同一 checkout 不同时推进两个会改写该指针的功能；并行实现同一功能时引用同一份已确认任务。

新 worktree 不会自动包含未提交的项目技能，也不会带入被忽略的 `specs/` 或功能指针。接续时先明确需要使用哪份资料，再按用户批准的范围复制；不要静默重建或把缺失资料当作已完成。需要共享或发布规格时，先确认位置、跟踪范围及敏感信息。

沿用 qa 开发与 worktree 流程。Spec Kit 不自动建分支、提交、推送或合并，Git 写操作按用户授权执行。

## 检查安装

Windows PowerShell 5.1 可运行项目附带的脚本。以下检查不调用模型、不安装依赖，也不启动 Krim：

```bash
powershell -NoProfile -File scripts/check-speckit.ps1
```

额外检查临时规格的路径解析、计划初始化、任务模板与实现前置检查：

```bash
powershell -NoProfile -File scripts/check-speckit.ps1 -SmokeTest
```

冒烟检查在 `.cache/` 下创建独立目录，结束后清理，不修改实际功能资料或当前指针。若执行策略明确阻止脚本，可以仅为这次命令加上 `-ExecutionPolicy Bypass`，不修改用户级或系统级策略。

技能文件需要随代码提交后才会进入后续 worktree；是否提交、推送由用户决定。若当前会话的技能菜单尚未刷新，可在同一项目目录新开会话，再查找 `/speckit-` 开头的名称。
