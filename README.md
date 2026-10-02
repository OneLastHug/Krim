# Krim Agent

Krim Agent 是一个以 Python 为核心的本地 Agent 运行时。项目内部名称已统一为 `Krim Agent`；前端只保留协议向量所需的 TypeScript 校验薄层，当前不包含完整浏览器 UI。

## 当前状态

- M1 已在用户授权范围内实现：协议桥、工具执行链、离线 fake provider、SQLite 持久化、HTTP/OpenAPI/SSE、Python 客户端、CLI 和宿主门面。
- M1 验收证据见 `docs/testing/reports/2026-10-02-m1-test-report.md`；实际结果以报告中的最后一轮记录为准。
- M2/M3 尚未批准，不把消息树、跨进程续传、SubAgent、Workflow、MCP 和完整前端写成已完成能力。

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
- M1 的流事件缓冲只在单进程内存；SQLite 保存消息、运行终态和审计，不承诺跨进程流续传。
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

仓库已初始化并保留可审查工作区。按项目规则，本轮不自动执行 `commit`（提交）或 `push`（推送远程）；查看状态使用：

```powershell
# git status 的 status 是“状态”，--short 是简短格式，--branch 显示分支信息。
git -c "safe.directory=$($PWD.Path -replace '\\', '/')" status --short --branch
```
