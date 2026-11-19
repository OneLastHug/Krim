# 单后端开发兼容探针

本目录保留 Docker + gVisor/runsc 的初期工具链兼容探针。当前开发目标已通过 `krim_agent.development.service` 接入 SDK、会话、审批和持久进程；C0 位于 `krim_agent.development.contracts`。这些探针脚本不作为生产运行入口。

## 环境要求

可信 Linux 宿主已有 Docker、runsc 和启用 project quota 的 XFS 挂载。本目录的历史探针以镜像用户 UID 1000 运行；生产 `dev_environment` 明确使用 UID/GID 65532。两者均移除 capabilities、启用 no-new-privileges、只读根目录，源码与依赖保留在独立项目挂载，Docker socket 不进入容器。

`probe.py` 只运行已安装镜像，不联网、不拉取、不安装；退出码 2 表示工具链不足。`compatibility.py` 会在新建的 `coding-agent-probe-*` 目录中安装真实 npm 依赖、执行安装脚本、构建、额外运行时安装、Git CLI、停止/重建和磁盘额度测试，需要安装及临时联网授权。该脚本只在临时测试目录中操作 Git index，不创建提交或推送。

运行 `compatibility.py` 时使用项目锁定 Python 环境，设置 `PYTHONPATH` 为本工作树 `src`。镜像参数使用构建后的不可变 image ID，证据目录须显式传入。Shell 请求经过同一 C0 CommandRequest 校验；脚本中的 probe 授权引用只标明测试作用域，不调用现有生产授权适配。

## 真实输出

每个步骤落独立日志、命令和退出码，汇总为 `compatibility-results.json`。失败时移除本批容器，保留项目及日志用于诊断；成功后仅保留一个空闲开发容器供预览。`preview-binding.json` 记录服务所属容器、镜像和项目。

`serve_probe.py` 由预览运行器调用，核验容器名称与 image ID 后用 Docker exec 启动 Node 服务。服务只映射到宿主回环端口 5189。预览后停止服务与本批容器，保留证据；它不是 E 模块的正式预览代理。

安装、构建、浏览器访问与跨会话恢复分别记录结果。这个固定页面只用于验证后端能力；最终图床管理系统必须由 Krim Agent 从空项目自行计划、开发和测试，详见 `tests/coding_agent/README.md`。
