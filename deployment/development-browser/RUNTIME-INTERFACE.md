# F 当前运行时接线

本入口用于 I 的 `development/browser_worker.py` 控制器。F 不另建控制器；当前正式包导入不依赖 deployment 路径、runpy 或 Playwright 安装。`F-final-manifest.json`列单次接收的全部当前src、tests和部署输入；证据单独列在`F-final-evidence-manifest.json`。

## 固定镜像和profile

- 当前下载/窄屏镜像：`sha256:392fdb1ba2a7d9c4853a9483ceecad25db2d7db6253497b38c3ba5ce9d0ec7b4`。本地tag仅便于识别：`krim-development-browser:i-download-1.61.0`，运行使用准确ID。它在已批准的resize镜像上离线更新应用源码，不新增依赖。
- 已验收浏览器版本：Playwright1.61.0 / Chromium Headless Shell149.0.7827.55；迁移镜像沿用同一固定base/wheels/NSS/browser层，只改模块路径和host socket权限。离线构建检查应用import、NSS和browser revision通过。
- 已批准profile：`/opt/krim-development-browser/v1.61.0/seccomp-chroot.json`，SHA256 `f922c7c9bfc1ece0b72b244e56dc85756ded002c837fb46054cfd8b275c74b6b`，root0644。源码部署输入`seccomp-chroot.json`与其逐字节一致。
- 所有代码和容器数据仍在F盘承载的专用WSL发行版内；host原Python venv、Docker权限、host网络/信任及runsc项目容器不改。

## 可调用host接口

```python
from krim_agent.dev_browser.host_bridge import PreviewConnectBridge
from krim_agent.dev_browser.launcher import DOCKER, WorkerSpec, SECCOMP_SHA256
```

`PreviewConnectBridge(socket_path: str, expected_host: str, authorize: Callable[[], Awaitable[None]], *, expected_uid: int, expected_gid: int)` 是 async context manager。`WorkerSpec` 字段保持：`image_id, name, origin, control_origin, socket_path: Path, ca_path: Path, ca_sha256, seccomp_path: Path`。`create_args()`返回固定Docker create参数元组；`remove_args(container_id)`只接受64位完整ID，返回`('rm','--force',id)`。`DOCKER=('/usr/bin/docker','--host=unix:///var/run/docker.sock')`。所有参数来自可信host，不是模型工具输入。

最小调用关系：

```python
async with service.route(handle.preview_id, original_principal) as (record, lease):
    # I在进入后先核对run/project与record环境、服务、端口；主身份不进IPC。
    async def authorize():
        await service.recheck(record, original_principal, lease)

    async with PreviewConnectBridge(
        str(socket_path), origins.host(handle.preview_id), authorize,
        expected_uid=1000, expected_gid=1000,
    ):
        spec = WorkerSpec(
            image_id=approved_image_id,
            name='krim-browser-' + unique_session_hex,
            origin=origins.origin(handle.preview_id),
            control_origin=origins.control_origin,
            socket_path=socket_path,
            ca_path=preview_ca_public_path,
            ca_sha256=preview_ca_sha256,
            seccomp_path=approved_profile_path,
        )
        create_argv = (*DOCKER, *spec.create_args())
        # I受信管理入口执行create，保存返回container ID并核对实际绑定/配置。
        # 然后以同一owner task启动 (*DOCKER,'start','--attach','--interactive',CID)。
        # 不传--tty；stdin/stdout私有、串行，保留原record/principal/lease。
        bootstrap = await preview_app.issue_bootstrap(handle.preview_id, original_principal)
        # 校验bootstrap.action等于origins.origin(id)+'/.krim-preview/session'。
        # 私有stdin写JSON行 {'op':'bootstrap','ticket':bootstrap.ticket}。
        # ready之后串行动作；动作前后authorize。票据/原始IPC不记录到日志或模型。
        # I在finally停止并rm准确CID、确认回收后才退出bridge和route。
```

`service.route`必须由同一个task enter/exit。传入的`original_principal`固定，不允许每个动作重新选择viewer。bridge的authorize closure捕获相同`record/principal/lease`；每次CHECK调用一次，CONNECT拨号前后各一次，活跃隧道每0.5秒一次，检查限时3秒。worker的BrowserSession动作前后、请求以及0.5秒watcher通过UDS CHECK调用同一个closure。I控制器还需独立周期recheck；撤权/取消即终止当前IPC及准确容器，不依赖下一次网络请求。

## 桥生命周期和权限

I先创建 `/run/krim-browser/<随机会话>/`，新目录、可信owner、0700，无symlink；socket路径此前不存在。Bridge enter创建节点，记录device/inode，exit停止监听、取消所有连接任务、回收上游并只删除同inode的socket。会话目录由I确认空及身份后删除。

当前root管理进程模式已实测：父目录root:root0700保留，bridge只对刚创建的socket节点设置root:1000/0660；容器1000:1000只读bind该单节点，因此能连接。宿主UID1000不能直接穿越父目录；SO_PEERCRED仍精确UID/GID1000/1000。没有修改Docker socket或其他host路径的权限。非root模式仅允许expected UID/GID等于自身uid/gid，否则在创建socket前拒绝。

Bridge只允许：

- `KRIM-PREVIEW/1 CHECK <固定preview-host> 443\n`：授权检查，不TCP拨号。
- `KRIM-PREVIEW/1 CONNECT <固定preview-host> 443\n`：仅拨literal `127.0.0.1:443`，先后检查授权后才返回OK并转发TLS字节。

不DNS、不接任意目标，不解TLS。Chromium与driver都在network=none容器里，统一使用容器loopback18080固定CONNECT代理。I的真实preview TLS监听器必须能从宿主127.0.0.1:443访问，证书SAN匹配E派生hostname；控制origin可带8765，仅作为票据Origin头，不给予控制站网络出口。跨preview内部Host/cookie仍由E校验，不能将CONNECT封包校验当成TLS终止代理。

## CA公钥与启动argv

I提供专用preview CA公钥PEM路径和SHA256，不用用户profile或自造授权。路径绝对、无symlink、≤64KiB；文件必须可被容器UID1000读取。推荐公钥root:root0644或0444（它不包含私钥），也接受owner1000可读或group1000可读；宿主父目录可继续私有，因为只bind单文件inode。launcher在create前检查hash及read位，worker启动后再核同hash。

唯一两个host bind：socket→`/run/krim-preview/bridge.sock`，CA公钥→`/run/krim-ca/preview-ca.pem`，均readonly。`KRIM_PREVIEW_ORIGIN`、`KRIM_CONTROL_ORIGIN`、`KRIM_CA_SHA256`来自WorkerSpec。镜像固定`NODE_EXTRA_CA_CERTS=/run/krim-ca/preview-ca.pem`；entrypoint在临时HOME中创建NSS SQL库并导入同一公钥。不开ignore_https_errors或证书忽略flag，CA私钥/站点私钥不进入worker。

create固定runc、1000:1000、capdropALL/NNP、批准seccomp、networknone、readonly、2CPU/1GiB/128PID、privateIPC/shm128MiB/tmp128MiB/HOME32MiB、nofile256、lognone、pullnever、无任意命令。host以create结果CID执行无TTY的start/attach。worker内部120秒总deadline、entrypoint130秒背板；I在异常/取消时更早清理容器。start可能在票据30秒期内花掉时间，建议attach后立即issue_bootstrap并发送，不复用已用或过期票据。

## IPC与产物

每行最多1,500,000 bytes；主机reader也必须设置相同上限（asyncio默认64KiB不足以接PNG）。一次只有一个请求在途，没有request-id复用、自动重试或恢复重放。bootstrap成功返回`{"ok":true,"ready":true}`，之后响应`{"ok":true,"result":...}`或固定error。私有IPC中的票据、截图字节和应用cookie描述符不直接进入模型、日志或checkpoint。

动作：goto(path默认/)、click(selector)、fill(selector,value)、wait(selector,state)、assert(selector,text和/或count)、upload(selector,name,media_type,data_base64)、screenshot、diagnostics、close。未知字段拒绝。console/network只输出固定类别计数，无页面原始日志/headers/secret。PNG返回临时artifact_id、kind、media_type、size、sha256、data_base64；host解码后校验PNG/hash/size，重新authorize，再持久化绑定owner/run/environment交A2；不把临时id当授权材料ref。A2/provider更严大小限制依然适用。

`resize`只接受preset=desktop(1024×768)或mobile(375×812)，不改变context/UA，不自动reload。

模型动作`download`必须带相对path和cookie_mode=context或preview_only。宿主将其转换为私有worker操作`download_context`，经BrowserSession原预算/授权检查读取当前URL适用的应用cookies；描述符只含path/cookie_mode/cookie_header，不发网络请求。context模式最多64个cookie/8192bytes，拒绝重复/非法字符并剔除预览基础设施cookie；preview_only不读取或发送应用cookies，也不改变真实context。

宿主固定原viewer/record/lease，复用E/B已连接socket和HTTPX流作GET；不DNS/dial、redirect/retry或解压，最多1MiB，逐chunk检查授权并计算SHA256。只返回status/media_type/size/sha256，任何cookies、文件名和正文都不进入工具结果。应用401可作为检查结果返回；重定向、编码、超限或不完整正文不会返回成功材料。取消/撤权中断流并关闭准确会话。Playwright的全量response.body/save_as不用于该限额路径。

## 冻结接收与验证范围

`F-final-manifest.json`保留为初次接收基线；后续resize和私有download_context增量分别核对，不能再拿旧manifest覆盖当前源码。发行包包含dev_browser运行模块及development/browser_worker.py、browser_downloads.py、browser_artifacts.py；deployment目录仍是镜像构建与部署输入，不作为宿主运行时导入。锁定依赖和profile保持原版本。

`F-final-evidence-manifest.json`单独指向验证报告、实机摘要/截图、root UDS证据与最终image receipt。root UDS测试只证明传输权限/peer和拒绝机制，回调明确为fixture；真正原principal/record/lease由I提供。现有真实Chromium无网络fixture与profile/TTL已通过，迁移后当前c418264镜像已再次通过真实Chromiumprobe。root桥UID/GID1000容器单UDS权限实测通过并清理。当前F专项140测试、最终Ruff、10运行时源mypy通过；140不含前轮已跑的52项旧browser/E协议。完整TLS/CA/原viewer预览仍待本次I组合验收。服务启动使用preview_*；没有图床应用业务测试。
