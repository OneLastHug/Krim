# Krim Agent

Krim Agent 是 Python Agent 工作台：在持久对话中运行任务、审批工具、查看执行事实与文件产物，再用人工参考构建数据集、执行评测并比较版本。

后端使用 Python 3.12、SQLite 和单个运行服务；前端使用 React、TypeScript、Tailwind 与 shadcn/Base UI。当前源码版本为 `0.4.0a1`。

## 开始使用

运行后端需要 Linux 和 Python 3.12；构建前端需要 Node.js 24.15 以上。WSL 2 可提供 Linux 环境。以下命令在项目目录执行。

用 uv 同步锁定依赖；`sync` 表示同步环境，`--frozen` 表示不改写锁文件：

```bash
uv sync --frozen
```

启动本机 API（Application Programming Interface，应用程序接口）与同包网页；`serve` 表示提供服务：

```bash
uv run --frozen python -m krim_agent serve --database .data/krim.sqlite --port 8765
```

浏览器打开 `http://127.0.0.1:8765/ui/`。默认不需要模型密钥，可在“对话”中新建会话，用 `/add 2 3`、`/echo 你好`、`/note welcome` 或 `/slow` 体验工具、审批与停止。

HTTP 模型地址、凭据和预算由可信宿主设置，网页不接收供应商地址或密钥。网页搜索/读取与 MCP 工具通过独立宿主配置启用，支持 stdio、Streamable HTTP 和显式覆盖；工具凭据与模型凭据分离。工作区文件任务还需要有效的 Linux 隔离配置；只提供普通对话服务时，不展示为可执行文件环境。SQLite 按单进程单写端运行；无访问策略的本机模式只供回环访问。

HTTP 模型接入使用 Vercel Labs Python AI SDK `ai[openai]`，固定为 `0.9.0`，通过 Chat Completions 协议直连宿主配置的兼容端点，不要求使用 Vercel AI Gateway。模型协议与流式事件由 SDK 处理；Krim 保留请求和响应上限、严格工具参数校验、预算与工具执行，完整响应通过检查后才释放工具调用。

## 工作流

- **任务与对话**：持久 Session/Turn、多轮上下文、有界队列、审批、取消、原请求键找回与执行事件。
- **文件与结果**：工作区输入快照、受控命令、完成条件核验、不可变产物修订与分块读取。
- **数据集与评测**：来源候选、人工参考确认、冻结版本、按工作区执行与分页报告。模型版本比较与工具/权限条件变化分开处理。
- **Python SDK**：同进程与 HTTP 客户端复用同一业务服务；创建回程未知时先按原键读取，不自动重放。

新工作区评测使用 v4 API 与 `WorkspaceEvaluationSDK`；旧 v1 评测接口保留兼容。

## 账号管理

账号管理继续使用密钥登录，支持账号状态、平台角色、个人工作区、密钥签发/轮换/吊销和管理事件。登录密钥只保存摘要，明文仅在签发时返回一次；账号显示名和授权版本分别维护，换密钥不改变历史会话归属。

查看账号命令：

```bash
python -m krim_agent account --help
```

首次初始化、旧身份导入和紧急恢复由可信宿主离线执行，必须先停止同一数据库的服务；服务运行中的管理走带认证的 HTTP API。旧策略导入保留 principal、workspace、角色和 legacy 标记，原本机数据需要显式认领，不会自动分给首个新账号。

默认 `KRIM_AGENT_IDENTITY_MODE=auto` 会识别已经初始化的账号数据库；`managed` 要求数据库已初始化。托管模式不能同时配置旧全局 `KRIM_AGENT_API_KEY` 或访问策略文件，也不会在认证失败时回退到本机身份。`KRIM_AGENT_GATEWAY_DISPLAY_NAME` 只设置公开网关名称，不改变模型路由或凭据。

管理入口为 `/v2/admin/accounts`，个人资料为 `/v2/me/profile`；写操作需要 `request_key`，资料和成员修改另需预期版本。原 `/v2/me` 结构不变。平台管理员不自动获得其他账号会话和文件的读取权限，仍需满足工作区授权。

网页账号菜单可打开个人资料、管理员账号管理、切换账号和退出登录；设置中可查看管理记录。密钥轮换后先保存一次性明文再关闭对话框，当前页面随后使用新密钥。退出登录只清除当前页面认证，不吊销密钥；账号权限与密钥变更的服务端执行结果仍按实际记录呈现。

## 沙箱管理后端（开发中）

设置 `KRIM_AGENT_SANDBOX_CONFIG_FILE` 指向权限为 `0600` 的可信 JSON 文件可启用管理接口；未配置时保持旧行为，且需要已初始化的托管账号。配置结构为 `quotas.defaults`、`quotas.profiles`，管理员再通过 `PUT /v2/admin/accounts/{principal_id}/sandbox/quota` 设置 `profile_name` 和 `overrides`，依次覆盖全局默认与规格模板。未准备环境的写操作要求 `request_key` 和 `expected_revision`；首次配置版本为 0。已准备环境采用在线预览/确认流程，目标配置和实际限额分开。该 PUT 完整替换账号配额选择：省略或传 `profile_name: null` 使用全局默认，省略 `overrides` 清空账号覆盖；更新前先读取当前配置并明确提交需要保留的字段。

额度可调整项目环境字节/inode、检查点容量/数量/期限及独立冻结预算；`frozen_bytes: null` 表示关闭冻结能力。`GET /v2/me/sandbox` 返回合并后的配置，`/v2/me/sandbox/projects` 提供账号私有项目管理。未接通 Broker 时，状态为 `unprovisioned`，用量未知，保存配额不表示硬限制已生效。

配置 `broker_socket` 可连接独立的本机存储 Broker。控制面不持有 Docker socket；Broker 校验 Unix 对端 UID，管理 XFS 项目硬配额与内部 ID 对应的持久目录。通过 `POST /v2/me/sandbox/projects/{project_id}/provision`（携带 `request_key`）准备目录后，`/files` 提供有界分页列表，`/file?path=...` 提供分块读取，`/search?query=...` 提供有界文本搜索；用量与已应用限额从 Broker 查询。

存储管理默认不开放命令。可信配置显式增加 `command_image`（已安装镜像的固定 `sha256` ID），并在 Broker 用 `--image` 指定同一镜像后，`target-http` 会切为 `sandbox/1` 命令工具集；旧会话绑定不会静默改写。该目标只能通过持久会话使用，模型调用 `sandbox_command_run` 必须先人工批准，再取得项目租约、捕获执行前检查点、验证实际 cgroup 后启动 runsc。当前命令只支持受限 Python，默认20秒、网络关闭，拒绝任意宿主路径或 Docker 参数。新版文件工具包括项目查询、列表、读取、搜索、写文件、建目录和删单文件；写操作同样需审批、租约和修改前检查点。

失败和取消保留已经发生的文件修改，原操作只核实、不自动重跑；回执标记 `verification=not_evaluated`，退出0不是项目完成证明。检查点独立持有者引用、两阶段 GC、多文件恢复和在线调额现已接入后端；依赖安装、受控联网、个人 MCP 与独立冻结核验仍未完成。实际镜像匹配且 Broker 可用时才报告 `runtime_available=true`。


`GET/PUT /v2/me/sandbox/sessions/{session_id}/project` 管理会话项目绑定，独立于旧会话响应。改绑需 `request_key`、绑定 `expected_revision`、`expected_session_revision`；仅允许所有者在会话空闲时选择同一工作区的自有项目。改绑更新会话版本，旧版本提交不能入队；停靠批准后再次核对绑定。绑定后文件/命令工具可省略 `project_id`，不能用显式参数越过绑定访问另一个项目。

恢复先创建检查点恢复预览，再用计划摘要、项目 generation、明确选中的步骤及确认请求执行。后来新增文件默认保留；预览后变化则冲突，恢复中断保留逐文件进度并阻止新写者，允许核实、继续或放弃剩余步骤。运行和恢复引用分别保护，用户取消固定不会解除作业保护；GC报告逻辑删除和实际物理回收两种结果。

管理员在线调额使用 `POST /v2/admin/accounts/{principal}/sandbox/quota-plans` 及对应确认接口，查询 `quota-operations/{operation}`。目标低于用量时显示 `pending_reclaim`，不删文件；等待任务收束和部分应用状态都可查询。字节和 inode 的区域重分配先缩后扩，未核实释放的额度不提前分配给别人。已有检查点区设置为0不会隐式销毁历史或变成无限额度，旧实际限额保留并等待明确清理。

## 开发与验证

普通回归入口会保存命令输出和 JUnit（测试结果交换格式）：

```bash
python scripts/validate.py --suite ci
```

完整测试另外需要实际隔离、外部目标或阶段材料；运行前会列出所需材料变量：

```bash
python scripts/validate.py --suite full
```

前端类型、契约、组件与构建命令见 [frontend/package.json](frontend/package.json)。端到端演练使用真实本机服务，并按页面步骤保存截图。

