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

