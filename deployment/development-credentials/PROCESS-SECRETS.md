# 通用项目 Secret 进程投递

## 使用约定

命令只携带 `AuthorizationRef.secret_refs`，不接受明文、目的路径或自定义投递回调。当前项目 grant 必须同时具有 `shell.execute` 和 `secret.deliver`，普通 Shell 权限不会自动得到 Secret。项目 Secret 记录复用 `SecretRecord(purpose="project.delivery")`、`AuthenticatedSecretVault` 和既有 CAS 版本；Git 凭据用途不能作为通用进程 Secret。

每条带 Secret 的命令必须有有限 `timeout_seconds`，最多引用8项；每项最多8192字节，总计16384字节。授权进程通过 `KRIM_SECRET_REFS`（JSON引用列表）及 `KRIM_SECRET_FD_0`、`KRIM_SECRET_FD_1` 等只读 FD 编号读取各项。环境变量只含引用和编号，不含 Secret 值。

带 Secret 的命令 stdout、stderr 和 PTY echo 不进入终端日志或模型工具输出；退出状态仍按原流程保存。不要将 Secret 拼接到命令文本，也不要把它打印到文件作为调试手段。

当前带 Secret 的 `project_service` 同样绑定创建run的活跃授权和有限deadline，run结束会回收，不提供跨run长期Secret服务租约。普通无Secret的项目服务生命周期不变。

## 管理入口

`PUT /v2/me/development/projects/{project_id}/secrets/{secret_id}` 由当前项目具有写入和批准权限的主体管理。请求字段为 `expected_revision`、`expires_at`、`value`、`revoked`；值按UTF-8最多8192字节，过期时间最多24小时。响应只有Secret引用、当前revision和撤销状态。值不进入模型会话、工具参数或响应；同一引用不能在Git凭据和项目投递用途之间切换。

模型调用 `dev_shell` 时只传 `secret_refs: ["已配置的引用"]` 及有限 `timeout_seconds`，当前grant还必须明确包含两项动作。API没有配置的vault时，Secret管理和命令投递保持关闭。

轮换或撤销提交后，管理路径释放项目锁，再在更新围栏内等待精确清理；停止未确认会报失败并保留可重试绑定。不要将Secret取消放在持有项目锁的子任务中，以免终端取消再次取锁。

## 宿主装配

以下接口已由I/H3接入正式DevelopmentService、SQLite仓储、API和wheel。无完整当前证明、vault或生命周期接点时仍拒绝投递。

1. 复用既有 vault，不读取用户本地环境猜测密钥；管理入口只允许当前项目写权限，Secret值不进入会话/审计正文。轮换与撤销沿原 `SQLiteAccessRepository.put` CAS。
2. 创建 `CommandSecretRouter(vault)`，将它作为 `ProjectAccessService(..., command_sink=router)`。已有 `secret_sink`、`git_sink` 不变。不能把 `deliver_command_secrets` 注册为模型工具。
3. 创建 `CommandSecretLifecycle(access, router, workspace=resolve_workspace, stop=stop_execution)` 并注入 `TerminalBackend(..., secrets=lifecycle)`。workspace从原项目归属解析，不能信任模型给的owner/workspace。stop是宿主清理权限，必须按绑定的process/operation/run/environment/execution精确终止、回收容器；不要要求已经撤销的主体重新授权清理。若调用 `TerminalBackend.cancel`，使用既有host cleanup context。
4. `SQLiteAccessRepository.guard`在同一`BEGIN IMMEDIATE`内，为`shell.execute`且`secret_refs`非空投影`AccessSnapshot.command`：
   - `CommandAccessState.operation`：当前operation receipt，完整`OperationIdentity`；
   - `command_digest`：operation行已通过`bind_command`写入的实际命令摘要，不是SDK arguments digest；
   - `process`：同operation的当前`ProcessRecord`，完整canonical `CommandRequest`、handle、execution_id、request_digest；
   - `environment`：当前环境handle；
   - `ownership`：由run/session记录独立连接得到的`RunOwnership`；
   - `run_status`、`stop_requested`：当前run记录。
   无法证明任何连接时返回None或拒绝。不得从传入binding复制这些事实。D会核主体代际/成员/项目grant/approval/current secret versions，并检查run运行中、未停止、environment ready、request/process/execution/op全绑定；初投递只允许accepted。运行监测还允许有原call_id结果回执的completed launch operation：Shell工具返回running句柄后，SDK会完成launch operation，但进程仍可运行；这不允许再次投递。unknown/failed/cancelled operation均拒绝。
5. 每次管理轮换、撤销、成员/账号/项目授权变化提交后调用 `await lifecycle.invalidate(owner_id, project_id)`；当前实现仅影响该完整scope的已注册执行，不批量按Docker label删除。watcher周期复核是通知失败的补偿，不代替持久撤销事实。
6. 服务关闭调用`await lifecycle.close()`，它会尝试全部绑定并关闭密文路由器，再报告失败；某个execution停止失败时，先终止/回收它的writer，保留该execution绑定供重试。批量invalidate同样不会因首个失败跳过后续绑定。恢复时宿主枚举原ProcessRepository中仍active/unknown且带secret_refs的精确记录，按既有host cleanup authority取消旧execution并保存结果；无法确认停止则保留unknown。TerminalBackend的refresh也会检查`secrets.tracks(record)`，没有匹配活跃watcher的旧Secret执行只做精确cancel，不继续inspect为运行中。不要为旧记录构造新的Secret版本或调用`launched`重投；旧unknown、running、accepted操作没有新的reservation执行许可。
7. 打包纳入新`development/secret_delivery.py`、`project_access/process_secrets.py`、`project_access/process_bootstrap.py`。不需要新Python包、宿主权限、Docker挂载、能力或网络配置。

## 临时通道与清理

投递包先经现有`SealedDeliveryQueue`密文暂存。服务在当前授权围栏内解密一次，最终spawn之前再次核授权与expiry。包通过匿名`memfd`（Linux可用）或OS匿名临时文件作为固定writer的stdin；argv只有固定代码、精确execution ID与非敏感绑定，不含值。固定writer使用Docker Unix socket、`exec --interactive --user=65532:65532`，stdout/stderr丢弃，不读取Docker ambient配置。

用户命令的原bootstrap在执行用户代码前等待本命令容器`/tmp` tmpfs中的一次性包；校验token/refs/大小/权限后，将每项打开为只读FD，unlink包及临时目录，再执行原命令。到期限制取当前grant、approval、所有Secret、network grant与命令deadline的最早时间，容器内已有`timeout`独立执行，宿主API失联仍有限期。没有包或包不匹配时不会执行用户代码。

投递和writer启动失败视为已消费，不重试该投递。清理先回收writer和待投密文包，再停止整个精确执行单元；停止无法确认时保留绑定和unknown状态供重试，不将其算作成功。容器移除后其`/tmp` tmpfs随之销毁。普通workspace与Git不存放投递值。

## 安全与验收范围

被授权进程能读取、复制自己的Secret，撤销不能抹除已被它复制到别处的数据。这里限制投递路径、普通输出和生命周期，不承诺阻止任意恶意程序泄漏；既有networknone与受控出口策略保持。

D3本地合成Secret/真实SQLite/AESGCM/子进程组合154项通过。I/H3正式仓储与管理入口回归、整合组合以及真实Broker/XFS/runsc/SDK验收已执行：32MiB/1000inode实际配额、UID65532、默认断网和只读根目录保持；进程经FD读出合成值，日志正文0字节；轮换后精确取消命令容器，环境和命令容器、临时目录、配额均已清理。实机记录位于 `.data/coding-agent/IH3-secret-native/`；这是基础设施投递验收，图床应用及真实供应商请求另行执行。
