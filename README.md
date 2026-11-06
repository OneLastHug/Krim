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

