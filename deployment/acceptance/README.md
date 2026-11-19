# 外部 Web / MCP / 视觉模型联合实测

入口：`tests/coding_agent/verify_external_services_real.py`。它复用正式配置加载器、Provider、Web/MCP 适配器和 Session API；完整模式使用已有 Broker、XFS project quota 和 runsc。模型自主选择调用和编写程序，没有预设工具调用序列。

## 准备配置

将本目录两份 example JSON 复制到仓库外的私有配置目录。它们是结构样板，`mcp.example.com` 和 `read_reference` 必须改为实际已授权服务、工具及参数。工具配置文件须当前运行用户所有、权限0600、无符号链接，父目录不能被其他用户写入；这是生产加载器原有要求。

- `tool_config_file` 相对于场景配置所在目录解析，也接受绝对路径。
- `web` 使用 Tavily 或兼容 API；密钥由 `api_key_env` 指定的环境变量提供。
- MCP 只接受 HTTPS Streamable HTTP；`allowed_hosts` 为明确允许的主机。`selected_tools` 必须全部列入 `readonly_tools`，不能给测试账号写入真实外部数据的权限。
- Bearer 认证用 `bearer_env`；其他静态请求头用 `headers_env`，值为环境变量名。工具配置不接受明文 `headers` / `env`。本入口不新增 OAuth 登录流程。
- `mcp_arguments`、`mcp_task` 只放可发送给模型及 MCP 的合成查询，不放密钥、私有仓库内容或个人资料。主任务要求调用同一工具并记录实际参考内容；不强制任意服务实现特定CSV schema。
- `mcp_resource_uri` 默认 null，报告明确记为未请求；服务确实支持资源时填一个允许读取的 URI，届时必须完成资源列表、模板列表和读取。空资源目录不是通过读取的替代。
- `mcp_scope` 为 `host` 或 `account`，一次只测试一种。建议先host成功，再换一个新的证据目录跑account。后者经正式账号API加密保存凭据并选择连接，最后检查另一测试账号不可见和删除后不能重新选择；不会撤销真实上游密钥。

模型沿用生产 OpenAI 兼容接口，从当前可信宿主进程环境读取：

| 变量 | 要求或建议 |
| --- | --- |
| `KRIM_AGENT_HTTP_BASE_URL` | 已授权模型接口，含实际API前缀；允许生产支持的本机网关 |
| `KRIM_AGENT_HTTP_MODEL` | 用户指定、同时支持图片与工具调用的实际模型名 |
| `KRIM_AGENT_HTTP_API_KEY` | 由已有安全渠道注入，不写入JSON或命令行 |
| `KRIM_AGENT_HTTP_WIRE_API` | `chat_completions` 或 `responses` |
| `KRIM_AGENT_HTTP_ALLOW_IMAGES` | `true` |
| `KRIM_AGENT_HTTP_MAX_REQUEST_BYTES` | 建议 `8000000`，须符合供应商容量 |
| `KRIM_AGENT_HTTP_MAX_OUTPUT_TOKENS` | 建议 `8192`，须符合供应商容量 |
| `KRIM_AGENT_HTTP_READ_TIMEOUT_SECONDS` | 建议 `90` |
| `KRIM_AGENT_HTTP_CONNECT_TIMEOUT_SECONDS` | 建议 `10` |

可空供应商上限沿用生产 `none` / `unlimited` 解析，不序列化成环境变量字符串 `None`。场景里的900秒、60步、120次调用是这次实测的显式停止条件，不修改产品默认额度。停止后记录失败，不自动重复付费请求。服务适配器自身已有的网络重试行为保持。

## 三种模式

所有命令从仓库根目录在已有 Python 3.12 + 锁定依赖环境执行。下列 `/private/...` 是需要换成实际配置/证据目录的路径，密钥应已由可信宿主环境提供；不要把真实密钥粘入终端命令或文档。

### 1. 只检查配置

```bash
PYTHONPATH=src:. python -m tests.coding_agent.verify_external_services_real --config /private/external-services.json --check-config
```

不连接网络、不创建数据库或容器，只输出不含URL/凭据的配置摘要。它不能证明服务可用。

### 2. 最少量接入预检

```bash
PYTHONPATH=src:. python -m tests.coding_agent.verify_external_services_real --config /private/external-services.json --preflight --evidence /private/evidence/preflight-01
```

这会真实调用外部服务：Web搜索一次、提取参考页一次、从本地结果引用再次读取；MCP初始化并调用配置的只读工具一次（请求资源时另列目录、模板并读取）；视觉模型接收一张随机颜色图片，返回结果与宿主答案比较。各阶段顺序执行，失败即停止后续阶段并关闭客户端。图片顺序不写入提示、文件名或PNG元数据。仅三块颜色的小题不能当作视觉模型能力评测。

### 3. 完整联合任务

```bash
PYTHONPATH=src:. python -m tests.coding_agent.verify_external_services_real --config /private/external-services.json --run-real --evidence /private/evidence/joint-host-01
```

该模式先做预检，然后执行真实任务；启动前要求可信Linux宿主root操作上下文、已有`/srv/krim-sandbox` XFS挂载、runsc和现有验收镜像。批准镜像与平台检查复用 `verify_development_broker.py`，不会安装依赖、拉取镜像、启动Docker daemon或连接已有用户数据库。

任务使用独立账号、数据库、项目和容器：

1. 模型建立计划和Goal、提出文字确认；脚本等到持久停靠后关闭并重开应用，再通过正式API回答。计划确认和本项目write/edit/shell审批也通过正式API处理；不批准Git、下载、Secret或project_service。
2. 模型真实调用Web及远端MCP并记录来源，读取项目内随机图片，将图片中左/中/右色块解释为三种CSV错误码，实现`validator.py`及测试并在容器运行。
3. 第一轮保留活动Goal，宿主顺序续跑后再完成；再做一次状态续问以保留生产keep_turns=2策略，手动压缩后继续一轮，核对计划、记忆和完成状态。
4. 宿主收集成功工具事件、图像请求计数、实际持久摘要、Goal和计划状态。最后在独立runsc容器执行生成的代码，使用重排后的CSV与宿主答案评分；既不在宿主import生成代码，也不以模型声称成功判通过。

这验证工具实际被调用、引用被保存及产物按视觉规则运行；Web/MCP参考内容对代码的语义贡献仍应结合具体服务人工审阅，不用一个模糊的模型评分冒充事实检查。

## 结果与清理

证据目录必须是全新绝对路径，不覆盖既有证据；默认0700。`result.json` 中 `passed=true` 且退出码0才算本模式通过；预检通过不等于完整模式通过。

报告保存每阶段摘要/内容hash、工具证据检查、图像/摘要请求计数、供应商返回的usage、产物hash、独立程序评分和清理结果。不保存模型提示、完整服务正文、密钥或原始上游异常消息；没有供应商计价信息时不虚构费用。临时数据库包含正常运行历史，成功清理时一并移除；清理失败则报告保留根目录位置，需按精确资源核查。

容器只按本次记录ID及镜像、所有者、项目挂载核验后删除；应用、模型客户端、MCP/Broker和监督进程均关闭。清理失败会使整次验收失败。生成代码保留在隔离容器内执行，宿主只读取常规产物的有界数据。不要公开上传原始证据目录；对外只使用审读后的结论。

## 离线回归

```bash
PYTHONPATH=src:. python -m pytest -q tests/coding_agent/test_external_acceptance.py tests/coding_agent/test_external_challenge.py
```

这组使用测试替身和本地数据，不访问真实模型/Web/MCP，也不创建Docker容器。真实外部调用必须显式选择 `--preflight` 或 `--run-real`。

本次入口准备的验证：配置CLI已实际运行且未发起外部请求；新入口/图片标准答案/账号MCP组合149项通过，Ruff与账号MCP源码类型检查通过；独立评分容器在现有runsc上实跑6/6行通过并清理。真实外部服务尚未运行，完整模式的端到端结果需在接入后确认。
