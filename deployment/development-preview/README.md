# 独立开发预览部署接口

模块 E 提供独立 ASGI `PreviewApp`，消费 C0、C 持久进程和 B 的受管 socket 路由。I 已在 `DevelopmentService` 装配 schema16 SQLite 仓储、当前授权源、B 路由和 E HTTP/WS 代理；认证控制 API、模型预览工具及 `serve-development` 双 TLS 监听入口已实现。B/runsc 传输与双 TLS 入口分别经过实测，真实 Broker/XFS、项目容器、浏览器的完整组合仍待验收。E 不启动项目命令、解析模型 URL 或创建目标连接。

## 启动与控制入口

主配置的 `environment.development.preview` 可选配置 `control_origin`、`control_cookie_domain` 和 `preview_domain`，并使用独立站点。未配置时不注册预览工具。独立 TLS listener 在同一 `environment.development.preview.listener` 中设置，包含 `control_host/control_port`、`preview_host/preview_port`、`control_certificate/control_private_key` 和 `preview_certificate/preview_private_key`；证书与私钥路径相对主配置文件定位，文件属于进程用户且不可被 group/other 写入，私钥不可被 group/other 读取。

用 `krim serve-development --config <krim.yaml>` 启动开发服务。它只打开一个 Application，控制 API 与预览分别使用 TLS listener；绑定只允许 loopback，默认端口8765/8766，禁用 forwarded-header 信任和访问日志，控制 listener 不接受 WebSocket。公共 HTTPS 域名须由部署方通过 TLS 透传连接对应 listener，或为本地测试配置对应独立 loopback 地址/443；代理不能通过添加 forwarded headers 伪造 HTTPS。此命令不会安装证书、配置 DNS 或发布外网服务。

模型图片通过主配置 `model.allow_images: true` 开启；模型请求上限设置于 `model.max_request_bytes`，通用运行输入预算设置于 `execution.max_request_bytes`，两者均有值时取较小值。`model.max_tool_calls` 限制单次响应的工具调用数，`execution.max_tools_per_step` 和 `execution.max_tool_calls` 分别配置每步与整轮预算。模型或执行预算未配置时不施加对应的应用层限额。

`scripts/check_development_startup.py` 是组件级静态预检，不启动服务也不覆盖主配置。服务运行时只用 `serve-development --config` 指定的主文件；预检脚本仍接受 `--project-root`、`--development-config`、`--sandbox-config`、`--provider-bounds`、`--host-max-request-bytes` 和 `--prompt`，用于核对 TLS 证书加载、browser CA hash/seccomp 文件、媒体开关与请求预算。provider-bounds文件仅含 `allow_images`、`max_output_tokens`、`max_request_bytes`、`max_tool_calls` 四个非秘密字段，不接受供应商地址、模型名或Key。该工具只输出状态码、预算和提示词SHA，不会启动应用、连接Broker或模型。`static_ready` 表示静态检查通过，真实服务、网络和应用流程另验收。

认证控制接口：

- `POST /v2/me/development/projects/{project_id}/previews`，JSON `{request_key, process_id, port}`。从真实持久进程注册；同主体/项目/request_key重放返回原绑定，换目标或同服务端口冲突拒绝。
- `GET /v2/me/development/previews/{preview_id}?wait_seconds=0..20`：当前授权下读状态或等待就绪。
- `POST /v2/me/development/previews/{preview_id}/stop`：先关闭预览，再停止服务。
- `POST /api/development/previews/{preview_id}/open`：认证 Bearer、精确配置的 HTTPS Host/Origin；存在 Sec-Fetch-Site 时必须 same-origin。返回 `{action,ticket,expires_at}`，禁止缓存。客户端在新顶层标签页以表单 POST 兑换，不能把票据交给模型。

模型工具为 `preview_register`、`preview_status`、`preview_stop`，只操作当前项目/环境，返回 PreviewHandle，不返回票据。应用关闭时关闭自身路由/连接，保留部署所有的项目服务与 supervisor。

## I 组合入口

```python
service = PreviewService(previews, processes, environments, process_backend, authorizer, routes)
app = PreviewApp(service, credentials, origins, WebsocketsConnector(routes))
```

`routes` 唯一合同来自 I 的 `krim_agent.development.preview_ports`。E 不复制 RouteBinding、RouteLease、ServiceInstance、PreviewRoutes 或 PreviewRouteError。共享路由错误保留固定code：unavailable/capacity返回503，invalidated返回410，denied返回403；WS相应使用1011或1008。I/B 路由应提供 `observe`、`acquire(binding, principal)`、`connect(lease)`、`check`、`ready`、`aclose`。应用停止时由 composition 调 routes.aclose；关闭预览连接不停止项目服务。

`register(preview_id, ProcessHandle, port, PrincipalContext)` 绑定已经 running 的 project_service。service_id 等于持久 process_id。B.observe按共享合同先调用I source.registration校验当前权限，再观察实际execution_id与精确State.StartedAt；E核对canonical ProcessRecord后持久保存返回的ServiceInstance；若观察后发生重启，之后 acquire 必须拒绝，不刷新旧记录。启动仍走 I/C 的 CommandRequest，E不另建服务调度器。

PreviewHandle 不扩展，access_path 为主 API 认证入口 `/api/development/previews/{id}/open`。I 在已有身份认证、CSRF 和项目授权之后调用 issue_bootstrap。30 秒单次票据仅从控制站以表单 POST body 传到 Bootstrap.action，不进入 URL、日志、模型或工具结果。响应禁止缓存。兑换后重定向预览根路径，设置专用 `__Host-Krim-Preview` HttpOnly/Secure/SameSite=Lax cookie，最长一小时；票据和会话只存 SHA256 摘要。

支持新顶层标签页，CSP 拒绝 iframe；不依赖第三方 cookie。Service Worker 注册被禁止，避免该 origin 的 worker 截获后续票据。预览 ID/host 永不复用，已有 worker 的旧域不得直接迁入。UI入口另由I安排审批/接入。

## 存储与授权

`PreviewRecord` 字段：handle、process、authorization、instance: ServiceInstance、request_digest、route_epoch=1、revision=1。execution_id/started_at仅从共享instance读取，不保存重复字段。

- PreviewRepository.create 拒绝复用 ID 和相同有效 service/port 绑定；host key = SHA256(preview_id)[:40] 唯一。save 严格 CAS、revision 显式+1；ready观察不改epoch，stop/revoke显式route_epoch+1。StartedAt和目标不可变，重启/换目标须新preview ID。I实现SQLite，不新建业务DB。
- PreviewCredentials 原子消费票据，先校验 preview/过期再删除；保存摘要、preview_id、PrincipalContext投影和expires_at。会话不是永久授权。
- PreviewAuthorizer.check 每次请求、长连接周期、响应块/WS消息前检查创建授权及当前主体 generation/project grant。principal=None只用于宿主readiness，不是匿名访问。
- B 通过 I 的 PreviewRouteSource 再读持久记录和原始viewer。该source不得递归调用B/E路由，不跨网络IO持数据库事务。
- ProcessRepository/EnvironmentRepository/ProcessBackend 直接消费C0；严格核对owner/project/environment/generation/process/operation/ownership/log/lifecycle/execution/command digest/authorization，要求持久running和环境ready。
- stop先持久stopped+epoch，再C.cancel；失败不重新开放访问，允许重试。revoke关闭访问，资源回收依旧归I/B/C宿主路径。

## B socket 路由与 HTTP/WS

共享 RouteBinding 包含preview/owner/project/environment/generation/service/process/operation/request_digest/execution/StartedAt/port/route_epoch。RouteLease仅有binding和opaque ID，没有地址。B维持network none，经固定docker exec字节中继连接已登记的容器127.0.0.1端口；具体实现和回收由B负责，E不推断IP或开默认网络。

B.connect每次交付非阻塞SOCK_STREAM socket（允许socketpair），交付前后核验真实容器实例和持久授权。正常/异常退出时E先关闭HTTP/WS协议传输和socket，再退出B.connect上下文；上下文enter/exit在同一task，长流不持项目锁。B保持独立到期机制、连接限额、中继回收；控制面故障不能因E忙于drain而续租。

HTTP使用已有httpx 0.28.1 + httpcore 1.0.9公开AsyncNetworkBackend薄适配。B socket在外层提供，后端connect_tcp只核对公共host/80并一次性交出该socket，绝不调用DNS、sock_connect或默认backend。禁Unix路径/TLS fallback、HTTP2、重试和keepalive；每请求一个socket，断流不重放请求。协议解析仍由httpcore/h11完成。httpcore==1.0.9已声明为直接依赖并纳入锁文件，原有依赖版本保持不变。

WS使用锁定的websockets==15.0.1客户端，传B预连接socket、公共ws URI供Host/path、HTTPS Origin、proxy=None；消息1MiB、队列16、关闭原始日志和压缩。URI不是拨号地址。真实AF_UNIX socketpair已验证握手、文本/二进制往返、关闭、拒绝重定向和握手取消。容器中继上的完整WS与浏览器HMR仍需组合验收；缺库时明确拒绝。

每0.5秒复核（可配0.01–2秒），每响应块/WS消息前再检查；控制面复核5秒超时则关闭，B独立lease到期仍是必要兜底。已发送的数据无法追回；stream error中断响应，不伪装下载成功。

## 浏览器边界

- 独立于Krim的注册域，每preview独立host；部署方提供真实cookie domain，不用简单后缀测试冒充Public Suffix List。
- 专用TLS listener保留精确Host与可信https/wss scheme，后端不可被不可信客户端直连。不能挂主站路径，也不能套用只支持JSON且未鉴权WS的LocalBoundary。入站Forwarded/X-Forwarded-*不作为信任来源。
- HTTP状态变更和WS精确Origin，拒绝sibling preview；跨站导航仅允许无query的根GET，其他动作路径拒绝。
- HTTP上传和响应保留raw编码/长度，支持Range和条件请求；body EOF之后断连监听才接管receive。独立client cookie jar、trust_env=False、不跟随redirect。
- 主应用Authorization、Proxy-*、任意自定义凭据头和Referrer不转发；专用预览会话cookie剥离，应用自己的cookie保留。上游Set-Cookie去Domain、强制Secure、屏蔽专用会话名。Connection指定头、Alt-Svc、Reporting、Service-Worker-Allowed和Link不转发。
- Location仅允许相对路径、当前公共https origin或当前公共host的http origin（改写https）；其他地址拒绝。正文不做HTML/JS重写，开发服务自行配置公共host/HMR URL。
- WS允许已授权服务内应用WS与HMR，不提供任意目标host/port或另一执行单元。应用登录可用自己host-only cookie。
- 所有开发预览请求需Krim预览会话。图床成品公开下载属其应用部署需求，不能据此让开发基础设施匿名开放。

## 验证入口与证据范围

`check.py --contracts-root <I提供的src>` 明确加载I的development包，E仍从自己的src加载；不复制DTO或硬编码机器路径。前后输出/比较__init__.py、contracts.py、ports.py、preview_ports.py的SHA256并禁止pyc。`verify.py`统一运行pytest/Ruff/mypy并保存verification.txt。

- protocol测试：内存持久化/权限/路由控制面替身；HTTP实际经过socketpair、h11 fixture peer、httpcore和ASGI；WS消息桥使用替身。
- transport测试：真实socketpair分块、无DNS/dial、无重连/换目标；WS客户端参数和取消路径使用fake库。
- websockets测试：真实库socketpair握手、文本/二进制、重定向拒绝、握手取消；缺库时明确skip，不计通过。

安装获批的websockets==15.0.1后，预览协议、HTTP传输、真实WS库与SQLite票据入口组合51项通过，无跳过。B中继14项真实runsc检查和HTTP GET/POST各512000字节传输已验证。SQLite身份与票据测试仍使用socketpair路由；正式API装配、TLS/浏览器/HMR全组合尚未验收。专用预览服务通过preview运行器启动和检查。
