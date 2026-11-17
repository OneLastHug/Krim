# 开发工作台 QA 验证

当前版本包含开发 Agent、通用会话工具和正式工作台接入，数据库版本为 schema 18。计划、用户问题、目标、上下文整理和项目记忆独立持久保存；侧栏布局仍按账号和工作区保存，展示分组与项目绑定相互独立。

## 启动

运行使用 Linux / WSL、Python 3.12 和项目锁定依赖。前端构建资源已随源码提交，手动检查页面无需先运行前端构建。升级前停止使用同一数据库的服务并备份数据库及尚未合并的 WAL；不要并行启动两个写入同一数据库的进程。

从仓库根目录启动普通本机工作台：

```bash
uv run --frozen python -m krim_agent serve --database .data/krim.sqlite --port 8765
```

打开 `http://127.0.0.1:8765/ui/`。普通本机模式可验证会话、分组、置顶和排序；完整项目环境、Secret 与文件功能需使用已有托管账号及沙箱配置。页面中的 Code 新会话可选择服务端已配置的目标，未配置的目标不会出现。

开发环境沿用现有可信沙箱配置和开发配置；双 TLS 入口见 [开发预览说明](development-preview/README.md)，不要将本机普通 HTTP 入口当成项目预览的 HTTPS 控制站。

```bash
uv run --frozen python -m krim_agent serve-development --database .data/krim.sqlite --development-config /absolute/path/to/development.json
```

`KRIM_AGENT_SANDBOX_CONFIG_FILE` 继续由可信宿主提供。模型与工具凭据使用原配置，不写入页面或提交到仓库。

## 正式聊天中的 Shell

Shell 需要宿主同时配置 `KRIM_AGENT_SANDBOX_CONFIG_FILE` 和 `KRIM_AGENT_DEVELOPMENT_CONFIG_FILE`，并设置 `KRIM_AGENT_PROVIDER=http`、`KRIM_AGENT_HTTP_MODE=agent`。模型地址、模型名和密钥分别通过 `KRIM_AGENT_HTTP_BASE_URL`、`KRIM_AGENT_HTTP_MODEL`、`KRIM_AGENT_HTTP_API_KEY` 提供；密钥不得写入仓库、前端或启动参数。

使用 Responses 供应商时显式设置 `KRIM_AGENT_HTTP_WIRE_API=responses`；该协议复用锁定 SDK 的原生实现并发送 `store=false`。普通 `serve` 可以提供不含项目网页预览的开发聊天；`serve-development` 另要求配置独立的双 TLS listener。项目容器仍不开放网络或宿主执行。

历史 Luna 解题实例使用以下宿主显式预算。当前普通/开发运行的累计步骤、工具调用、事件、输出和运行时长默认不设应用额度；下表不代表当前默认配置：

| 环境变量 | 值 |
|---|---:|
| `KRIM_AGENT_MODEL_TIMEOUT` | 90 |
| `KRIM_AGENT_RUN_TIMEOUT` | 300 |
| `KRIM_AGENT_HTTP_CONNECT_TIMEOUT_SECONDS` | 10 |
| `KRIM_AGENT_HTTP_READ_TIMEOUT_SECONDS` | 90 |
| `KRIM_AGENT_HTTP_MAX_OUTPUT_TOKENS` | 4096 |
| `KRIM_AGENT_HTTP_MAX_REQUEST_BYTES` | 131072 |
| `KRIM_AGENT_HTTP_MAX_RESPONSE_BYTES` | 1048576 |
| `KRIM_AGENT_HTTP_MAX_EVENT_BYTES` | 1048576 |
| `KRIM_AGENT_HTTP_MAX_EVENTS` | 16384 |
| `KRIM_AGENT_HTTP_MAX_ARGUMENTS_BYTES` | 32768 |
| `KRIM_AGENT_HTTP_MAX_TOOL_CALLS` | 1 |

正式用户路径：登录 → 新建对话时选择“开发项目” → 在 Environment 绑定项目并准备 Linux 环境 → 获得项目 `shell.execute` 授权 → 在聊天中发任务。授权管理界面暂未接线，可信管理端通过 `PUT /v2/me/development/projects/{project_id}/grant` 提供有期限、限定动作的授权；不要手工改数据库或默认为所有项目开放执行。已有普通目标会话不会因为绑定项目自动变成开发目标。

`GET /v2/me` 的 `target_keys` 应包含 `target-development`，其 `/v2/targets/target-development/tools` 应包含 `shell`。Shell 返回的是进程句柄：`wait_seconds` 最多20秒；启动成功不等于命令成功，须继续用 `process_status` 检查状态和退出码，用 `read_output` 读取输出。工具参数或模型流被拒绝时，不把未释放的工具当成已执行。

通用文件工具为 `read`、`list`、`find`、`search`、`write`、`edit`。`read` 返回UTF-8文本、完整文件SHA-256和下一字节偏移；目录与搜索使用有界分页。`write` 默认只新建；覆盖已有文件必须提供读取时的 `expected_sha256`。`edit` 接受 `old_string`、`new_string` 和 `expected_sha256`，默认要求唯一匹配；多处替换须显式 `replace_all=true`。正文通过带摘要的分块 stdin 传入，不再设置16KiB写入或1MiB文件总量上限；父目录须已存在。写入在原隔离执行环境内完成，沿用 `shell.execute` 授权、逐次审批和持久操作记录。版本冲突/找不到文本/匹配不唯一会返回具体错误，不覆盖文件。

模型目录及工具卡片不再使用开发前缀。旧运行的原始事件与指纹保留，界面将已知旧工具名显示为新名；未知外部工具名称不变。升级后已有会话或待批任务如遇 `target_binding_changed`，新建开发会话并绑定原项目继续；不要改写旧审批指纹或自动重放旧写入。

## 手动检查

- 左栏：新建两个分组与多个会话；选择、发送、重命名和继续会话后位置不变。左键拖分组或同级会话，刷新后顺序保留；新会话追加到当前或指定分组末尾。跨组或跨父级拖动取消，不改变项目绑定。
- 会话菜单：置顶、取消置顶、重命名、只读辅助会话；关闭会话后确认归档或移入回收站，再取消归档或恢复。检查操作针对菜单所在会话，不误操作当前选中会话。
- Environment：绑定与切换项目、准备和停止环境；改绑后能继续发送消息。项目未配置、权限不足及版本冲突显示实际结果。
- Secret：使用一次性测试值添加引用；轮换需输入新值并确认，撤销需确认。凭据值不回显、不进入本机请求账本；切换账号或项目后不出现旧引用的回程结果。
- 信息栏：工具目录与账号 MCP 连接、上下文策略、Memory files 和 Outputs。长列表在区内滚动，Context 与 Outputs 随内容排列；窄屏可关闭并重新打开。
- 右侧工作区：文件目录分页、文档和图片、标签关闭及上下分区、辅助会话记录。已注册的开发预览从成功工具结果进入页面，使用短效票据在独立窗口打开。
- 输入框：空白时单行，随内容增长，到上限后内部滚动，清空后收回。检查桌面及 375px 窗口，实际打开弹窗核对长文字、按钮、滚动和错误状态。

计划、待答问题和计划确认显示在现有对话历史区域。原 Compact session 按钮已连接服务端整理请求：成功排队后在下一安全模型请求边界处理，断网时重试原 request_key。未读标记、Fork 执行、辅助会话继续、顶栏终端控制和整项目打包下载等未接通动作仍保留“敬请期待”；页面不显示虚构的上下文用量或记忆载入来源。

## 2026-11-17 通用工具集成验收

- 默认CI清单225个文件均有完整分组通过报告，去重2858项通过、0跳过；不是一次单进程全量CI。最后存储/Docker/恢复/通用工具组合240项通过；原条件跳过的runsc网络失权观察已显式启用，其完整文件11项通过。该实机用例使用内存授权夹具，不请求外部网络；容器与目录均清理。分组过程中的失败、原因和修复后报告保留在本地验收记录。
- Node24.22.2下TypeScript、契约、33文件/388项组件测试及生产构建通过。正式本机网页已操作创建会话、四轮工具执行、Compact排队与后续运行；连续发送期间没有出现清空历史的空态。输入框保持单行、自适应增高、上限后内部滚动、清空收回。
- 375px组件夹具已操作计划展开/分页、空答复校验、选择提交、计划确认与读取失败重试。它使用真实App和内存API，不代表后端或远端模型验收。
- wheel与sdist的显式清单、来源字节、元数据、依赖、入口和静态资源核对通过；48个新增模块在源码树外导入通过。发行预检旧规则已修正，字体/清单回归34项通过。
- 已用现有可信宿主的 root 控制路径完成真实 Docker/runsc 终端验收：命令幂等、stdin/EOF、应用重开后服务存活、进程树取消、环境停止均通过。Docker 29 将无限 PID 序列化为 null、并只在 HostConfig.Tmpfs 列出 tmpfs 的兼容问题已修复；身份、项目挂载和安全选项核验保留，相关回归81项通过。
- 真实 XFS 配额迁移已通过：经预览/确认把数据区与检查点区从有限额改为无限额，内核读回均为0/0，文件摘要保持不变，临时配额和目录已清理。
- 真实 Broker/XFS/runsc 联合验收通过：1,176,147 字节文件写入、容器内 unittest、持久问题跨应用重开、计划确认、4次工具审批、官方 MCP 工具与资源、项目记忆、图片请求、Goal 顺序续跑及手动上下文压缩。16次工具执行均成功；22次回环模型请求中有5次携带图像，1次压缩摘要已持久保存。监督进程正常退出，3个关联容器删除或确认不存在，临时目录已清理。
- 实测发现并修复了运行事件存储残留的64 KiB单条上限；大于1 MiB的工具参数/结果现可完整保存、幂等重放和重开读取，JSON有效性与键冲突检查保留。该回归纳入默认CI。
- 外部 Web/MCP/视觉模型没有本轮可用接入配置；本地回环模型和官方 MCP 协议验证不替代远端模型与服务实测。

### 可重复的真实基础设施验收

`tests/coding_agent/verify_general_tools_real.py` 是显式启用的宿主验收脚本，不在默认 pytest 中启动容器。它要求已有 `/srv/krim-sandbox` XFS project quota 挂载、runsc、脚本中批准的镜像及可信 root 操作上下文；不会安装依赖或拉取镜像。模型使用确定性回环 HTTP，MCP 使用官方 SDK 的本地 stdio 服务。

从仓库根目录、在已具备上述权限的环境执行：

```bash
PYTHONPATH=src:. python -m tests.coding_agent.verify_general_tools_real --run-real --evidence /srv/krim-sandbox/general-tools-evidence-$(date +%s)
```

脚本创建独立账号、数据库、项目和容器，测试问题停靠重开、计划确认、逐工具审批、大文件、命令输出、MCP、图片请求、记忆、Goal 续跑与压缩。`result.json` 保存逐工具结果、配额读回和清理状态；容器只按已记录 ID、镜像和项目挂载核验后清理。回环模型收到图像不等于外部视觉模型完成识图。

## 历史 Shell 与通用工具验证

- 正式网页聊天使用真实模型分轮完成 Hot 100 三题，自行写入实现、测试和 README；真实沙箱执行29项 unittest 全部通过、退出码0，并由模型读取日志。
- 通用 `write/read/edit/list/find/search` 均经正式聊天实际调用；最终 `shell/read_output` 验证输出 `GENERIC_TOOLS_OK`、退出码0。过程中遇到的参数错误和超时保留在运行记录中。
- 本次后端相关组合回归138项通过；完整前端组件341项通过；TypeScript、生产构建、7个相关生产源类型检查及改动Python lint通过。本次未重跑全仓后端CI或重建wheel。
- 已实际检查窄屏登录、工具目录、工具详情弹窗和旧聊天工具标题；不代表所有页面交互均已验收。
- 可选实测辅助脚本位于 `tests/coding_agent/`。`serve_chat_shell.py` 和 `collect_chat_shell.py` 从 `KRIM_TEST_ACCESS_TOKEN` 读取一次性测试账号凭据，至少32个可打印ASCII字符；不得使用真实账号或将凭据写入仓库。供应商凭据仍通过私有套接字在内存中交接。

## 前一版工作台验证记录

以下为前一版整合结果，不替代本次改动的回归：

- 完整后端自供 CI：2043 通过、0 失败、1 项显式 Docker/runsc 实机跳过，共 2044 项。
- 完整前端组件回归：335 通过、0 失败；TypeScript 检查通过。
- 最后构建后资源路由、字体、脚本、Broker 与侧栏组合：48 项通过；该组与完整 CI 有重叠，不累加。
- 生产构建与包内资源 hash 一致；离线 wheel 的 292 个 Python 文件和 131 个网页资源逐字节核对，7 个模块在源码树外导入通过。

以上为历史版本记录。当前已验证范围与尚缺的真实环境联合验收以本页“2026-11-17 通用工具集成验收”为准。
