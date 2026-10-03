# Krim Agent

Krim Agent 是一个以 Python 为核心的本地 Agent 运行时。项目内部名称已统一为 `Krim Agent`；前端只保留协议向量所需的 TypeScript 校验薄层，当前不包含完整浏览器 UI。

## 当前状态

- M1 已在用户授权范围内实现：协议桥、工具执行链、离线 fake provider、SQLite 持久化、HTTP/OpenAPI/SSE、Python 客户端、CLI 和宿主门面。
- M1 验收证据见 `docs/testing/reports/2026-10-02-m1-test-report.md`；实际结果以报告中的最后一轮记录为准。
- AUTH-004 已批准继续开发 M2/M3，并要求每个已验证功能单独本地提交；消息树、跨进程续传、SubAgent、Workflow、MCP 和完整前端的实际完成状态见需求追踪。
- M2 消息模块已接通消息树、活动分支、编辑重问、重新生成和 M1 数据迁移；[分支 API 与命令](docs/api/message-tree.md)说明实际范围。可靠流恢复、Agent、上下文、Workflow、MCP 和完整 UI 持续开发。
- M2 已实现 [持久流事件与续传](docs/api/durable-streams.md)：SQLite 保存 SSE 帧，重启可按原序号继续读取，CLI 提供 `resume`；不重复执行旧工具。
- 已实现 [Python 生成 SDK](docs/sdk/python-generation.md)：参考 Vercel AI SDK 的文本/完整流、步骤与结果语义，复用现有运行时与工具链，支持最终文本、严格结构化读取和显式取消。
- 默认 ToolRegistry 已接入 plan/todo/question/skills 内建包；这些工具仅写入宿主内存并受现有 write 权限/审计链约束。
- 已实现 [Python OpenAI-compatible Provider](docs/api/openai-compatible-provider.md)：显式配置后 SDK、HTTP 和 CLI 可调用兼容文本/函数工具端点；本地 TCP、取消/断流与错误持久化已验证，真实商业供应商矩阵尚未验证。
- 已实现 [OAuth 持久密文与刷新租约](docs/api/mcp-oauth.md)：配置/主体隔离、SQLite 0005、单飞刷新、CAS 删除保护和密钥轮换；真实本机双进程刷新已验证，MCP transport 自动授权尚未完成。
- 已实现 OAuth metadata/DCR 组件：issuer/S256/endpoint 安全校验、path-aware discovery、动态注册和 PKCE exchange 消费；本机 TCP 已验证，401 自动 transport 与浏览器回调仍未完成。

## 快速开始

```powershell
# uv 是 Python 项目管理工具；sync 表示按 uv.lock 创建/同步项目虚拟环境。
uv sync --locked --group dev

# krim 是 CLI（Command-Line Interface，命令行界面）；serve 表示启动本地 HTTP/SSE 服务。
uv run --no-sync krim serve
```

默认只绑定 `127.0.0.1:8765`。无 `KRIM_AGENT_API_KEY` 时拒绝非本机 Host；配置 API Key 后，除健康检查外需要 `Authorization: Bearer <key>`。

也可以直接使用内置解释器和项目环境：

```powershell
.venv\Scripts\python.exe -m krim_agent version
.venv\Scripts\python.exe -m pytest -q
```

## M1 入口

- `src/krim_agent/protocol`：v2 内部信封和进程内 `emit`、`notify`、`stream`、`on`。
- `src/krim_agent/tools`：Pydantic 输入模型、JSON Schema、权限、Hook、超时、取消、嵌套限制和审计。
- `src/krim_agent/runtime`：模型→工具→模型循环、后台运行、幂等、stop、SSE 事件。
- `src/krim_agent/persistence`：SQLAlchemy 2 + SQLite 的话题、消息、运行和工具审计。
- `src/krim_agent/api`：FastAPI、OpenAPI、HTTP/SSE 和本地安全边界。
- `src/krim_agent/client.py`、`src/krim_agent/cli.py`：Python 客户端和 `krim` 命令。
- `src/krim_agent/app_sdk`、`src/krim_agent/app.py`：最小宿主语义门面和进程内桥。

## 技术边界

- 后端、Agent 运行时、协议、数据库、API 和 CLI 使用 Python。
- 前端只使用必要的 TypeScript；当前仅有 `frontend/contracts/protocol.test.ts`，用于同一 JSON 向量校验。
- `FakeProvider` 是离线确定性演示，不是真实模型、不调用商业服务，也不执行网络、Shell 或文件工具。
- 默认 Provider 为 fake；仅当宿主配置 `KRIM_AGENT_PROVIDER=openai-compatible` 和模型端点时按配置发请求。模型端点密钥与 Krim API 访问密钥独立。
- 当前 SQLite 保存消息、运行终态、审计和 SSE 帧；持久事件支持重启与读进程恢复，活跃生产仍要求单进程，不承诺多 worker 并发生产。
- 物理工作区仍可能位于旧目录名下；项目文本、Python 包、CLI 和环境变量统一使用 `Krim Agent` / `krim_agent` / `krim` / `KRIM_AGENT_`。

## 文档入口

- [文档总目录](docs/README.md)
- [实施计划](docs/plan/2026-10-02-krim-agent-implementation-plan.md)
- [授权与里程碑](docs/plan/milestones.md)
- [需求追踪](docs/requirements/requirements-traceability.md)
- [M1 本地运行指南](docs/user-guide/m1-local.md)
- [M1 HTTP API](docs/api/m1-http.md)
- [测试报告](docs/testing/reports/2026-10-02-m1-test-report.md)
- [实施工作记录](docs/progress/2026-10-02-m1-implementation.md)

## Git

仓库已有 M1 提交。AUTH-004 要求每个已完成并验证的功能单独 `commit`（本地提交），说明首行以精炼中文写明“模块：功能”供审计；尚未授权 `push`（推送远程）。查看状态使用：

```powershell
# git status 的 status 是“状态”，--short 是简短格式，--branch 显示分支信息。
git -c "safe.directory=$($PWD.Path -replace '\\', '/')" status --short --branch
```
