# Skills、长期记忆与子代理：分期实施方案

**基线**：远端 `qa`，实现分支 `claude/agent-extensions-impl-20261112` 从 `7db603e9a` 开始。本分支已获批并正在实施 Skills 与 Principal 私有记忆后端；Explore 延期，UI 不在本阶段范围。

## 建议结论

Skills 使用通行 SKILL.md 格式；各产品对子代理配置和记忆存储没有共同 schema，所以 Krim 自行定义。共同约束是：技能/记忆/子代理结果都按不可信上下文处理，真正的权限边界仍由 host policy 与 sandbox 决定。

把能力分成共享安全约定与三项可分开验收的模块，依序推进：

1. **共同契约**：确定资源主体、作用域、审批、版本快照、审计、保留/删除语义。
2. **Skills**：先做显式选择、只读加载、版本固定的任务指引。不执行其中脚本，不允许 Skill 扩大工具权限。
3. **长期记忆**：跨会话的 Principal 私有记忆，由本人管理；首版不允许模型写入。先明确旧运行记录与备份保留范围，再做 CRUD 和受限检索。
4. **子代理**：最后做一层、只读 Explore 委派。先把父子工具权限、预算扣减和取消语义设计清楚；首版复用 RunService，崩溃中的只读 child 转为 abandoned，不自动重跑。

Skills 与记忆可以并行做接口设计，但不共用内容存储；记忆不依赖 Skills 才能实现。子代理不应等待 Skills/记忆的功能实现，但会受益于其工具 allowlist 与上下文快照约定。高风险的递归委派、自动写记忆、共享工作区记忆都不纳入第一版。

## 现状与关键约束

- Agent 主循环在 [`runtime/engine.py`](src/krim_agent/runtime/engine.py:75)，工具统一经过 [`tools/executor.py`](src/krim_agent/tools/executor.py:74) 的指纹、参数、策略和审批流程。`ask` 会停靠并写恢复点，审批后恢复。
- [`tools/registry.py`](src/krim_agent/tools/registry.py:9) 每个 binding 最多 16 个工具。账号沙箱 binding 已占 10 个，新增 Skill/Memory/Delegate 工具必须核算目标工具表容量，不能假设每个工具集都有空位。
- Sessions/turns 按 `workspace_id`、`owner_id` 隔离，turn 持久化 context snapshot 和 transcript，见 [`storage/models.py`](src/krim_agent/storage/models.py:101)。上下文构建器只投影完整历史组，并超限报错、不静默截断，见 [`context/builder.py`](src/krim_agent/context/builder.py:28) 和 `:91`。
- `context/manifest.py` 有 `memory` 类别标签，但没有记忆表、读写 API 或注入路径。Compaction 是同一 session 的历史摘要，不是跨会话用户记忆。
- Principal 具有账号 generation、workspace role 和 approve 权限，见 [`security.py`](src/krim_agent/security.py:84)；SessionService 每次请求重验主体。沙箱还把授权绑定到 principal generation、workspace、sandbox project 和 session binding revision。sandbox project 是项目资源 id，不等同于 workspace 身份。
- 数据库 schema version 为 10，迁移使用追加式 `create_all` 与每版本 table set 校验，见 [`storage/sqlite.py`](src/krim_agent/storage/sqlite.py:56)。session close 只是不可逆的逻辑关闭，turn/transcript 仍留存；账号停用、key 吊销也不删除历史数据。当前没有通用数据清除或 retention 服务。
- 技能/记忆前端应作为新增区域，沿用已有 `details`、`ToolCatalog`、`Dialog`、request intent 模式。不得顺带改现有导航、控件或聊天输入框；账号设置 UI 先后端与迁移验证，再独立 HTML 样板并取得单独审批。

## 模块 1：Skills

**目标**：让用户可复用有限、可追溯的任务说明；默认不加载、不执行代码、不赋予权限。

**第一版设计**

- 沿用 Agent Skills 格式：必需 `name`、`description`、Markdown 正文，可选 `license`、`compatibility`、字符串 `metadata`、相对资源路径。Krim 在导入时分配内部 id，并从正文/资源生成版本摘要；`version` 不是规范字段，未知扩展字段按可忽略 metadata 处理，实验性的 `allowed-tools` 不能作为通用互操作权限机制。危险路径、解析歧义、重复名称和超限内容须明确报错。MVP 不自动执行安装钩子或脚本，Skill 附带脚本仍只能通过已授权的沙箱命令工具运行。
- 第一版来源限于随包内置资源与数据库中账户/Principal 私有导入资源；当前代码没有通用账号私有目录或 workspace 文件目录可供安全扫描。workspace 共享 Skill 暂缓，若未来支持，使用 workspace 授权的数据库资源，不能直接扫描宿主或沙箱文件树。
- session 仅固定被选择的 Skill 内部 id/version/content hash；版本行保存在新表或受信任包资源，不向已有 session/turn schema 加列。内置索引先只列 `name`/`description`；用户显式选用后才渐进披露正文及按需资源，分别受 context/manifest 限额约束。技能内容可采用规范化的不可信内容标记，但不注入 `Message(role=system)` 历史中间位置（当前 builder 校验会拒绝）。它不能覆盖 system、安全策略或用户要求。
- Skill 声明的工具意图只用来提示和过滤可见工具，不可创建工具或放宽父级策略。若多个 Skill 的 allowlist 互相冲突，取已授权工具集的交集；无 allowlist 表示不新增限制。
- 每个 turn 固定 Skill 快照及来源 hash；运行中的资源更新不改变当前 turn。禁用 Skill 只影响后续 turn。旧运行可能把 Skill 正文保存在 context snapshot、tool result 或 transcript；若用户要求彻底删除，须把这些副本和备份纳入同一清除策略，否则只能承诺后续不再加载。
- 注入需分别处理 session `SessionContext` builder、workspace `WorkFrame`/Compactor 与 sandbox 上下文。三个 target 的 run binding 互斥；仅接 `context/builder.py` 不等于所有 target 生效。修改 builder 会改变 `builder_fingerprint`，故新快照带新版本、恢复仍按存储指纹与快照版本验证。若 manifest 展示 Skill 来源，新增 `skill` 类别并计入 manifest 自身 30 KiB 限额与每项 source-ref 上限。

**涉及区域**：新 `src/krim_agent/skills/`（解析、catalog）；`context/types.py`、`context/builder.py`、`context/compaction.py`、`context/manifest.py`（session/workspace/sandbox 三管线分别接入快照、预算与来源；如需观测来源，增加 `skill` manifest category 并计入限额）；`sessions/service.py`、`storage/models.py`、`storage/sessions.py`、`storage/sqlite.py`（新表保存 session 选择与 catalog revision，不改旧表；同步 `storage/sessions.py` 所有 schema-version gate）；若做数据写入则加 `storage/skill_models.py`，必要时用现有 `script_assets.py`/manifest 做 wheel 内置资源；无 host-directory 扫描；`composition.py`、`api/server.py`（服务与路由装配）；MVP 将已选择 Skill 的正文作为本轮不可信上下文，不新增 `skill_read` ToolSpec，从而不触碰 16 工具上限；若后续增加读取工具，须改 `tools/core.py` reserved-name/capability/risk/policy_version 映射并核算每种 registry 容量；`runtime/service.py` binding fingerprints；`pyproject.toml` wheel allowlist；`tests/agent_core`。

**验收**：未选择时不注入；选择后上下文可见且超预算在首次模型调用前失败；变更 Skill 不影响已接纳 turn；恶意指令无法改变 tool policy；账户、workspace、viewer/member/admin 边界及迁移恢复均有测试。逐目标核实 `target-http`、`target-workspace`、sandbox binding 的 context 管线不同，不假定一次接线覆盖所有目标。

## 模块 2：长期记忆

**目标**：在同一 Principal（由当前账号身份表示）的个人工作区多个 session 间复用用户认可的信息，且支持查看、更正和移除。若产品未来允许一个账号由多人共用，则需新增独立 user identity，不能继续把 Principal 私有记忆描述成自然人私有。

**推荐第一版作用域**：Principal 私有，默认仅在该 Principal 的个人工作区会话间使用。session 私有存储不满足跨会话记忆目标；workspace 共享记忆先延期。共享工作区的会话可能被有权管理员读取，个人记忆一旦注入就可能进入该会话历史，因此 MVP 禁止在其他 workspace 自动召回个人记忆。后续允许时须由用户明确选择共享条目并确认目标可见范围。每次检索从当前 PrincipalContext 获取身份并实时验证 generation；执行者与 session owner 不一致时，不得代入执行者或 owner 的私有记忆。

**第一版设计**

- 记忆是独立资源，不是 transcript 的别名。条目包含 id、principal_id、正文、来源 turn/ref、hash、revision、创建/更新/删除时间与状态。所有查询都强制 `principal_id` 条件，并用来源/正文 hash 校验后才注入。
- 个人记忆的第一版只允许用户本人通过明确的设置/API 操作创建、更新和删除。暂不开放模型记忆写工具：ToolObservation 的 arguments/value 会持久化进 run_events，无法在当前 append-only tool pipeline 中承诺不留正文；现有 session approval 还允许有 approve 权限的 workspace admin 代批，不适合作为 user_private memory 的确认门。以后若加模型提议，须先有 owner-specific consent 与不把正文写入不可变事件的安全协议，不能直接复用普通 workspace approval。
- 记忆检索默认只在启用该功能且 session owner 与当前主体相同的个人工作区会话发生，数量、字节及上下文预算有硬上限。所有写操作时重查主体 generation、目标 revision、scope 和正文 hash；人工管理操作遵循 request_key 幂等模式。
- 建议、活动条目与上下文中的检索快照分别建模；删除的条目不进入未来 context。记录来源和来源 turn 可供用户判断记忆依据；记忆永远是不可信内容，不提升为 system 指令。
- **数据删除分层且不能混称**：用户主动“移除记忆”应硬删账户私有 memory 主表/索引及其派生缓存，使其不再被检索；历史 run events、turn context_snapshot/transcript 可能留有此前注入的正文，按现行追加式运行记录策略保留。该 API 只能标注“已从未来记忆中移除，历史运行记录仍按保留策略保存”，不能称为全库彻底擦除。备份过期与清理时点另列为平台保留策略，不在第一版用户界面承诺立即擦除备份。若未来要求连历史正文也擦除，必须单独设计可验证的 history purge 或 per-user 可销毁密钥加密，并处理 hash、审计和备份恢复语义。
- `context/manifest.py` 的 metadata 不承载原文；但实际 turn context_snapshot/transcript 和 tool run_events 中的模型可见内容/工具参数可能被保留，须纳入用户告知和验收。Tools executor 的输入/输出是冻结审计事实，不能靠 hook 静默脱敏，因此第一版不开放模型发起的记忆写工具。

**涉及区域**：新 `src/krim_agent/memory/`；新增 `storage/memory_models.py` 等表与 schema 迁移；更新 `storage/sqlite.py` expected-table ladder、`storage/sessions.py` 和 `storage/work.py` 版本 gate；`context/builder.py`、`context/compaction.py`、`context/manifest.py` 的三管线检索/预算；当前不新增模型写工具或普通 session-approval flow；账户私有 CRUD API（列举/人工创建/编辑/移除）要求实时 principal 等于资源 owner；后端与前端 contract tests。

**验收**：账号 A/B 无交叉读写；只有 memory owner 可以修改/移除，workspace admin 的通用 `approve` 不能替 owner 确认账户私有记忆变更；拒绝、重试、并发冲突可预测；停用后新 turn 不再召回；用户移除后 memory 主表和派生索引清除且不被未来检索，明确记录的 context snapshots/transcripts/run events 与备份保留载体保持可说明、不可误称为物理清除。

## 模块 3：子代理（本轮延期）

批准方案原先将独立只读 Explore 作为第三模块。但实现必须触及 `runtime/service.py`、`runtime/engine.py`、`composition.py`、`tools/core.py` 与 schema ladder 等共享责任文件；主会话当前将这些文件分配给其他模块，且本分支工作树里没有保留可继续的子代理实现。因此本轮不写任何未接线子代理模块，不声称这项能力已实现。待共享文件责任区统一开放后，再按下列已批准安全边界拆成独立计划/实现：

- 单层、独立 child run，默认 Explore；工具必须同时属于父级 allowlist 与硬编码 child-safe 清单，且逐个审定为 `risk == "read"`、无 write/exec capability。命令、外部写工具、MCP 写工具、记忆修改与递归委派均默认拒绝；子任务不得等待 ask 审批。
- child 继承可信 Principal/workspace/session/binding 范围；不接收模型提供的身份或资源路径；父取消/超时/权限 generation 变化时取消 child。子输出视为不可信、限长，并作为普通 tool-result 返回。
- RunService 共享容量要显式预留一个 child slot，预算由父 run 限定。重启时未完成只读 child 转为 interrupted/abandoned，不自动重跑；长后台任务与子审批/写入留后续设计。
- 验收要覆盖 read/write/exec、MCP source 与 capability 的风险矩阵；父策略 allow 也不能把非 child-safe 工具放入子目录，并验证取消、崩溃与容量无死锁。

## 阶段和依赖

| 阶段 | 范围 | 依赖 / 决策门 |
|---|---|---|
| 0 | 共同资源约定、威胁/作用域模型、memory deletion contract、context snapshot 格式与 schema migration 设计 | 先批准保留/删除承诺；不实现 UI |
| 1 | Skills 静态 catalog、显式选择、预算化 context 注入、冻结来源 | 阶段 0；不依赖 Memory/Subagent |
| 2 | Principal 私有 Memory、检索和人工管理；第一版不提供模型写入/代批 | 阶段 0；可复用阶段 1 的 snapshot/manifest plumbing，但不共享内容表；先约定 run_events/snapshots/备份的保留告知。实际开发需等共享 schema/context/API 责任区分配 |
| 3 | 子代理 Explore 暂缓，不在本轮实现 | 需主会话统一开放共享 runtime/engine/composition/tool-core/schema 责任区，并另行排期 |
| 4 | API/前端与评测收敛 | 后端与迁移先验收；前端另走 UI 审批和 HTML 样板流程 |

数据库迁移只能增加新表，**不向既有 sessions/turns 表加列**：SQLiteStore 会以当前 metadata 对旧库逐列强校验，而 `create_all` 不执行 ALTER；须更新 `storage/sqlite.py` 的版本梯子/expected table set、`storage/sessions.py` 和 `storage/work.py` 的 `require()` 版本白名单，并覆盖 readonly open。session 的 Skill 选择、记忆绑定和 child-task 关联使用新表或已有 JSON snapshot，不破坏现存数据库。复用 `_eval_write(operation, confirm)` 的幂等写模式。审批可以复用 ADR-0015，但不能将账户私有数据的确认主体混同 workspace-admin 的通用 session approval。

## UI 与评测

- 首版可以先只提供 API/CLI 和工具审批，不要求改动现有导航。若增加 Skills/Memory 管理面板，应作为新的可撤销区域复用现有组件；不触碰输入框、按钮状态/尺寸/颜色、导航和当前布局。
- 正式 UI 按项目规则：先完成后端与迁移验证，再交独立 HTML 样板并等待明确批准；审批不自动授权更广泛 UI 改版。弹窗必须实际检查长文案、错误态、窄屏和 Portal 样式。
- 测试先用 FakeProvider：Skill 未选/被注入/版本快照；Memory owner CRUD、跨 workspace 拒绝、移除后不再召回、历史副本保留事实；若未来引入模型提议写入，再单独覆盖 owner-scoped consent 与 park/recovery；Subagent allowlist 降权、预算耗尽、父取消、权限撤销、重启恢复及伪造子代理输出注入。确认 `pytest` 收集目录；老架构遗留的 `tests/test_skills.py`、`frontend/e2e/skills.spec.ts` 不代表当前实现，须先适配当前 API/测试 allowlist 才能作为证据。
- 质量评测每阶段固定任务集，记录成功率、误召回/误写、完成时间、调用数和模型费用。真实模型评测调用前单独批准费用；FakeProvider 测试不产生供应商费用。

## 风险与延期项

- Skills、记忆、对话历史及子代理结果均是潜在 prompt injection，不得改变系统指令和工具策略。
- SQLite 单写者决定了子代理首版应串行并限制并发，不能在事务锁内等待模型。
- 工作区 `target-workspace`、普通 `target-http` 和 sandbox 会话上下文管线不同；每个 target 必须分别设计 context 注入、审批与验收，不能只验证一种 binding。
- 共享 workspace memory、自动学习/自动批准、递归子代理、任意 Skill 脚本执行和子代理写工具自动继承均延期。
- 本方案不包含 shell/runtime、网页/MCP 工具整合、浏览器和编辑体验重构；子代理首版不依赖未规划的通用长任务模块，只扩展当前 RunService/Run 状态支持 child run；若后续要做自动重试、任意长后台任务或可审批写入，届时另行设计通用长任务与子任务审批模块。
