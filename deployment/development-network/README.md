# 受控开发下载

容器始终使用 Docker/runsc `--network=none`。依赖管理器通过容器内 `127.0.0.1:39119` HTTPS 下载代理访问可信宿主；代理只在 Docker exec 的 stdin/stdout 上发送结构化下载消息，不监听宿主端口、不挂载 Docker socket、不添加 capability、不修改宿主网络或信任库。

宿主复用已有 `ProjectEgressPolicy` 与 `PinnedAsyncTransport`：准确匹配授权 DNS host/端口，DNS 全部地址通过公共地址校验后钉扎连接，TLS 使用原域名 SNI 并验证证书。只允许 GET/HEAD，不接收调用者的 Host、Authorization、Proxy、Upgrade、SNI 配置或请求正文。宿主不跟随重定向；Location 先按同一策略校验，客户端的下一请求重新解析 DNS、复核策略及 TLS。Registry/CDN 必须同时存在于 D 的现有 NetworkGrant。

CONNECT 在容器本地终止 TLS，不能建立任意 TCP 隧道。临时 CA/证书只进入该容器 tmpfs；私钥加载到容器 TLS context 后删除，宿主不信任该 CA。容器被视为敌对：即使进程替换代理、读取证书或自行伪造 stdin/stdout 帧，宿主只会执行通过策略复核的 HTTPS GET/HEAD。原始网络字节、任意地址与 Docker 管理能力不会被转交。

## I 接线

```python
from krim_agent.dev_network.manager import DownloadNetworks

async def resolve_policy(spec):
    # 首版仅受理 lifecycle=run 且原授权显式选择策略的命令。
    # I 从 canonical ProcessRecord 原 ref + command_intent 复核当前run、
    # 账号与grant；不信任调用者提交的policy ID或环境ID作为能力。
    ref, intent = await trusted_mapping.lookup_canonical_command_intent(spec)
    return await project_access.network_policy(ref, intent)

downloads = DownloadNetworks(resolve_policy, runner=runner)
backend = DevelopmentBackend(
    environment_repository, project_locks, authorizer,
    runner=runner, processes=process_repository, downloads=downloads,
)
```

生产首版 `EnvironmentRecord.network_id=None, network_policy_ref=None`，空闲环境始终断网，不建立idle授权映射。执行容器的有效策略仅取原 `CommandRequest.authorization.network_policy_ref`；命令无ref就断网，绝不继承环境策略。兼容既有环境记录：无request时仍使用record字段；有request且record策略非空时，必须与命令ref相同（命令缺ref也算不一致），否则拒绝。非法ref及任何非空Docker network ID均拒绝。有效策略进入容器指纹，并决定代理环境变量及bootstrap等待；不同命令策略不会修改空闲环境指纹。I service只允许lifecycle=run的显式下载授权，project_service不隐式获得长期网络。本增量不修改共享合同、存储或composition。

每个新 session 固定 D 返回的 NetworkGrant 全文与 revision；每次请求、DNS 前后、响应头和每个响应块重新调用 D 权威入口。连接监视器每 0.25 秒复核，resolver 超时 5 秒后失败。I 的撤权、轮换、actor/membership/approval 失效事务必须先建立阻新受理的围栏，持久化变更并等待：

```python
await downloads.invalidate(owner_id, project_id, policy_id=policy_id)
```

该调用关闭匹配 session 和现有响应流后才返回；不依赖被撤销用户权限清理。actor 级失效应对相关项目调用不带 policy_id 的 invalidate。多宿主/多 worker 必须由 I 在所有持有 lease 的 worker 广播并确认；单进程方法和轮询不是跨进程撤权围栏。授予新 revision 不恢复旧 session，应重新创建 execution/container 并取得新 lease。应用退出 `await downloads.close()`。

B 已在 prepare/create/start、取消、release、stop 和 recreate 中接入私有 manager；执行仍先 create，C 持久 execution ID 后才 attach 启动。固定 bootstrap 最多等待代理 15 秒，失败不运行项目命令。独立下载 session 最长 900 秒、最多 8 个并发请求，单下载 512 MiB、单 session 累计 2 GiB、单请求 120 秒；超时和异常均关闭出口，长任务需在新 execution 取新 lease。断开管理进程或 pipe 后代理停止，Docker 网络仍为 none。

## 现成组件与安装

复用 Docker exec、gVisor network none、Python asyncio/ssl、项目锁定 httpx/httpcore 和现有 pinned transport；X.509 生成用项目已安装的 cryptography extra。缺 cryptography 时 session 拒绝，不自动安装。没有新增 pyproject/lock 依赖。未选择 internal bridge + Squid/Tinyproxy：internal bridge 本身仍允许同网 peer 和宿主网关，额外防火墙/代理部署会扩大主机配置面。Unix socket 方案涉及 runsc host UDS 配置，stdio 不需要该变化。

## wheel 打包与验收结果

I 的 `pyproject.toml` 采用 wheel `only-include` 显式白名单，必须加入本补丁的 `src/krim_agent/dev_network/{__init__,broker,policy,manager,shim,tls,lifecycle}.py`。B 按写入范围未修改该共享文件。cryptography 沿用现有 extra，不增加包版本或自动安装；发布环境必须已装该 extra，否则证书生成拒绝。

命令显式策略增量检查：132 passed；Ruff通过；mypy11源码通过，检查前后源码哈希相同。新增19项覆盖环境/命令策略不一致、非法值、无策略断网、有效策略指纹及bootstrap。`verify.py --command-only --evidence <目录>` 的真实Docker/runsc单容器16步通过：环境记录无策略，原CommandRequest显式策略，registry下载成功；确切ID完整绑定清理，目录删除。检查工具为 `check.py --evidence <目录>`，记录真实解释器/UID、命令、完整输出及检查前后源码SHA256。

最终真实 Docker/runsc `verify.py` 验收90步通过：registry HEAD/GET，esbuild0.25.12 postinstall实际执行、TypeScript转换成功，公网TCP/宿主gateway/UDP DNS/清proxy直连拒绝，offlist/metadata/Host混淆/POST403，撤权/过期/manager关闭后连接拒绝，CommandRequest真实bootstrap下载成功。4个本批确切ID逐一核对完整绑定后删除，测试目录删除。使用批准镜像内置Node24.22.3/npm11.19.1，未安装新Node；Grant resolver为明确测试替身，生产SQLite/actor/审批guard及多worker广播需I组合验收。CDN重定向只在mock测试中验证，未访问未批准的新来源。

## 验收

`tests/agent_core/test_dev_network_*.py` 包括 mock HTTP、真实本地 TLS/stdin/stdout 和生命周期/撤权竞态；这些不等同真实 Docker 或互联网安装。`verify.py` 为单独实机入口，只使用已有镜像及指定 registry/npm 授权来源，证据按运行目录输出。运行前协调 Docker/runsc 资源；以可信 root 管理 Docker，普通测试使用已有 krim Python。

下载方法限制不支持 Git smart HTTP POST/push 或私有 registry 凭据。D2 的 Git 凭据通道必须配单独授权出站，不能借此下载通道打开 Git。缓存与安装产物仅写项目 `/workspace/.cache` 与项目内容目录（F 盘 WSL）。
