# 独立开发预览部署接口

模块 E 提供独立 ASGI `PreviewApp`，消费 C0、C 持久进程和 B 的受管 socket 路由。B 中继与 E HTTP 适配器已完成真实 runsc 传输验证；schema16 的 SQLite 预览记录、票据和权限源已实现并测试。正式应用入口、认证 API 与独立 TLS 预览服务的装配尚未完成。E 不发布网络、启动项目命令、解析模型 URL 或创建目标连接。

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
