# Krim Agent

Krim Agent 是以 Python 为核心的本地 Agent 项目，提供多轮模型/工具执行、版本化配置、持久会话与任务、审批问答、实时事件、恢复、评估以及浏览器/SDK/CLI 管理。TypeScript只负责浏览器界面，静态产物由Python服务托管。

当前在持续完成项目，完整顺序见 [P01–P10接续计划](docs/plan/2026-10-08-project-completion.md)。Workflow放后期；受控工作区读取、条件修改、持久回执、原生修改清单、检查点捕获/恢复及SDK/REST/CLI/浏览器已接通，实际验证范围见[P03证据审计](docs/testing/reports/2026-10-09-workspace-completion.md)。P04命令隔离、持久记录、Agent/SDK、完整输入、文本产物提案/条件发布与REST/client/CLI已接通，浏览器命令面板和Profile执行资源配置已接通，原键恢复/输出与成果分页/取消和提案审阅见[报告](docs/testing/reports/2026-10-10-browser-commands.md)；目录/二进制及更广文件类型继续；Skills文件包、完整Host/多模态及生产运行能力按后续阶段推进。

## 快速开始

需要Python 3.12–3.14与uv。以下命令在仓库根目录执行：

```bash
# sync（同步）按锁文件准备项目环境；dev是development（开发依赖）。
uv sync --locked --group dev

# serve（提供服务）启动Python HTTP/SSE；run（运行）不再次同步环境。
uv run --no-sync krim serve
```

浏览器打开 <http://127.0.0.1:8765/ui/>。默认使用离线FakeProvider，可以输入`/add 2 3`或`/echo hello`验证工具执行；普通文本为确定性回显，不代表模型推理能力。现有服务必须单工作进程运行。

Linux/macOS使用`.venv/bin/python`，Windows使用`.venv\Scripts\python.exe`。当前机器已有环境时可直接执行：

```bash
.venv/bin/python -m krim_agent serve
```

访问密钥通过`KRIM_AGENT_API_KEY`设置，模型端点密钥通过`KRIM_AGENT_PROVIDER_API_KEY`设置，两者独立。`.env.example`是模板，不会自动加载；明确加载时用`uv run --env-file .env --no-sync krim serve`。详细说明见 [安装与本地试运行](docs/user-guide/local-agent.md)。

## 可运行的演练

```bash
# 使用临时数据库与真实CLI/TCP，验证两轮执行、任务、服务重启、历史和eval。
.venv/bin/python examples/local_trial.py

# SDK会话示例：关闭/重开、独立Run与原事件读取。
.venv/bin/python examples/python_agent_sessions.py

# Linux工作区演练：SDK准备、CLI恢复、服务重启与原操作零重放。
.venv/bin/python examples/workspace_trial.py

# Linux隔离命令基础：需bubblewrap 0.11.0与系统Python，验证副本、产物和取消。
.venv/bin/python examples/command_trial.py

# 持久命令演练：实际执行、保存产物、关闭服务、新数据库连接读取原记录。
.venv/bin/python examples/durable_command_trial.py

# Agent命令演练：Profile资源、SDK审批、完整只读输入、原产物和冷恢复。
.venv/bin/python examples/agent_command_trial.py

# 命令成果演练：真实差异、生成提案、独立批准发布、撤销恢复原文件。
.venv/bin/python examples/command_artifact_trial.py

# 命令管理演练：真实CLI服务、同步客户端、重启、原任务与文件回执零重放。
.venv/bin/python examples/command_management_trial.py

# 显式离线兼容端点模拟：真实Provider适配/工具循环，不发商业请求。
.venv/bin/python examples/python_openai_provider.py
```

`-m`是module（模块入口），SDK是Software Development Kit（软件开发工具包），CLI是Command-Line Interface（命令行界面），SSE是Server-Sent Events（服务端事件流）。

## 已有能力

| 模块 | 当前可用行为 |
|---|---|
| Agent | Profile历史版本/CAS、可信模型与工具选择、agent/ask/plan/auto边界、父子权限/预算、persistent恢复、后台start/wait/stop与资源清理 |
| 会话与任务 | 独立session/topic/turn、绝对TTL、关闭/重开/销毁、0023计划/待办、CAS/幂等/历史和会话隔离 |
| 对话与流 | 消息树/编辑/重生成/分支、持久事件/独立游标、断开重连、后台多话题读取 |
| SDK/API/CLI/UI | PythonAI/AgentSession、类型化生成/结构化结果、REST/OpenAPI、命令行、Agent任务卡片与任务面板 |
| 工具与交互 | Pydantic输入、输出Schema、权限/Hook/审计、工具按需发现、真实审批与Question答案 |
| 工作区 | 宿主根目录授权、Profile/资源冻结、读取/检索；可写授权下的Diff提案、条件应用/撤销/保留、持久回执/核实/清理、原生清单与批次、完整检查点捕获和条件恢复；SDK/REST/CLI/浏览器消费同一事实 |
| 隔离执行与命令 | 资源/Profile授权、模型与SDK同审批/Hook/审计、完整受控输入、非root/独立网络、输出/资源上限、取消/超时/后代回收；0030原结果、0031文本产物提案、REST/client/CLI管理；浏览器命令管理/原键恢复/输出与成果分页/取消及文件审阅已接通；目录/二进制继续 |
| 上下文与预算 | 持久摘要/提案确认、离线BPE/请求窗口、Provider usage、累计预留/结算与未知结果核实 |
| Eval | 数据集/真实Agent目标、多轮/规则/显式评委、持久进度/暂停/取消/恢复、报告/统计与版本比较 |
| 资源集成 | MCP stdio/HTTP与显式OAuth、按话题Skills目录、同源Host回程、受限附件/引用/拖放、Change List/Checkpoint |

商业模型质量、真实账单、目录/二进制条件发布、生产多worker/租户、PostgreSQL/Redis与部署演练尚未完成。隔离执行当前仅有Linux本机证据，rlimit不代表cgroup整棵树总配额。旧M1/M2工作记录保留历史事实；当前状态以接续计划和最新模块报告为准。

工作区的目录文件描述符边界与条件发布目前仅Linux实测；Windows不能使用该文件能力，普通Agent/SDK/eval入口仍可用。文件路径约束不等于操作系统进程沙箱。

## 设计、进度与验证

- [Agent架构与审阅顺序](docs/architecture/agent-design.md)
- [任务状态架构与理由](docs/architecture/task-state-design.md)
- [工作区架构与设计理由](docs/architecture/workspace-design.md)
- [工作区API、SDK与CLI使用](docs/api/workspace.md)
- [工作区浏览器与命令行修改指南](docs/user-guide/workspace-changes.md)
- [浏览器文件修改架构与理由](docs/architecture/workspace-browser-design.md)
- [连续修改保留与兼容设计](docs/architecture/workspace-effect-chain-design.md)
- [原生修改清单与批次设计](docs/architecture/native-workspace-list-design.md)
- [浏览器统一审阅修改](docs/user-guide/native-change-list.md)
- [原生检查点浏览器与命令行指南](docs/user-guide/native-checkpoints.md)
- [原生检查点REST与客户端合约](docs/api/native-checkpoints.md)
- [检查点条件恢复架构](docs/architecture/workspace-checkpoint-restore-design.md)
- [工作区整体演练与证据组织](docs/architecture/workspace-trial-design.md)
- [P03完整证据审计](docs/testing/reports/2026-10-09-workspace-completion.md)
- [隔离命令架构与设计理由](docs/architecture/isolated-command-design.md)
- [可信Python执行接口与试运行](docs/api/isolated-execution.md)
- [持久命令架构与原结果恢复](docs/architecture/durable-command-design.md)
- [持久命令Python合约](docs/api/durable-commands.md)
- [Agent命令架构与授权输入](docs/architecture/agent-command-design.md)
- [Agent命令SDK使用](docs/api/agent-commands.md)
- [命令产物提案与条件发布架构](docs/architecture/command-artifact-design.md)
- [命令成果SDK合约](docs/api/command-artifacts.md)
- [命令管理REST、客户端与CLI](docs/api/command-management.md)
- [浏览器命令与产物审阅指南](docs/user-guide/commands.md)
- [本会话接续计划与授权](docs/plan/2026-10-09-project-resumption.md)
- [命令后台接受与管理架构](docs/architecture/command-management-api-design.md)
- [SDK与Eval架构](docs/architecture/sdk-evaluation-design.md)
- [完整项目计划](docs/plan/2026-10-08-project-completion.md)
- [需求追踪](docs/requirements/requirements-traceability.md)
- [当前进度](docs/progress/README.md)
- [Agent已验证基线：Python972/浏览器58](docs/testing/reports/2026-10-07-agent-completion.md)
- [P01任务模块测试报告](docs/testing/reports/2026-10-08-agent-tasks.md)
- [文档中心](docs/README.md)

每个完成并验证的功能独立使用中文“模块：功能”提交并普通推送到既有GitHub仓库；架构、来源、测试和限制随功能同步。未完成阶段不记为项目目标完成，人工验收和生产上线事实单独记录。
