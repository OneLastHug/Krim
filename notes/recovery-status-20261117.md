# 2026-11-17 通用工具集成收尾

本文件是当前接续入口。旧过程笔记已归档到本地 `.cache/recovery/archived-notes-qa-20261117/`；旧 RUN-STATUS、handoff-A..H 和会话树不再代表当前后台任务或文件所有权。

## 外部实测入口接续

qa已于2026-11-17推送提交 `b7d9328d1`。之后按用户“准备实测入口”新增 `tests/coding_agent/verify_external_services_real.py`，操作说明和无密钥样板位于 [deployment/acceptance](../deployment/acceptance/README.md)。默认必须显式选择模式：`--check-config`不联网，`--preflight`调用真实服务，`--run-real`另创建隔离容器；本次尚未发起真实外部请求。

新增离线测试文件已纳入默认CI（清单由225增至227文件）；下文2858项、前端和分发包结果是qa发布前验证快照，不包含此次入口。正式HTTP回归发现并修复个人MCP严格tuple字段拒绝JSON数组的问题，仅对创建/更新/会话选择字段接受数组并转tuple，保留元素类型、重复项和权限校验。新改动尚未提交推送，旧分发包不包含此修复。

入口验证：无外部请求的CLI配置模式exit0，缺少模式exit2；新入口/图片标准答案/账号MCP最终组合149项通过，之前含通用流程与发行清单的组合166项通过（集合重叠，不累加）。7个变动Python文件Ruff通过，账号MCP服务mypy通过；最后清理用例2项复测通过。真实runsc独立评分6/6行通过，容器和临时目录已清理。证据见 `.cache/recovery/external-entry-final-r3.xml`、`external-entry-cli-result.json`、`external-grader-runsc-result.json`。本轮未运行完整227文件CI，也未重建分发包。

## qa发布前验证快照

本地代码已接入计划、结构化提问、计划模式、工具发现、MCP 资源、私有/项目记忆、图片输入、Goal 顺序续跑与统一上下文压缩；默认取消开发运行和内容的累计应用配额。恢复后的修复、前端生产构建、正式页面交互、wheel/sdist 构建均已执行。

默认 CI 清单的 225 个文件均有本轮完整分组通过报告；去重汇总为 2858 passed、0 skipped，无未覆盖文件。各分组报告及去重节点索引在 `.cache/recovery/final-ci-inventory.json` 和 `final-ci-node-index.json`。这些是分组执行结果，不代表一次单进程全量 CI 运行。最后存储/Docker/恢复/通用工具组合240 passed / 199.27s；原实机条件跳过已显式启用，网络失权观察完整文件11 passed / 7.61s。

真实 Docker/runsc 终端、XFS 有限额→无限额迁移、通用工具真实 Broker/容器联合流程均已通过，现有可信宿主 root 路径不再视为环境阻塞。实测暴露的 Docker 29 序列化差异和事件存储 64 KiB 残留上限已修复并完成回归。远端 Web/MCP/视觉模型没有本轮可用的接入配置，仍未验证。

## 原始目标与工作位置

原会话的实际用户要求已从本地导出中提取到 `.cache/recovery/original-human-requests.json`：取消本版硬编码的小配额；将开发能力、Web/MCP、Todo、提问、计划模式、发现、资源、图片、记忆、Goal、上下文压缩接入同一 Agent 运行链路。产品内子代理/多 Agent 编排不在范围。

- 当前分支 `claude/general-tools-recovery-20261117`，基线 `b342ba7aa679b626fdb38175a1ea6e2f5fc96f06`。
- 远端已只读核对：qa 为 b342ba7aa，main 为 ca43bacfb；未从恢复会话的旧 main 基线继续实现。
- 原集成分支 `claude/general-tools-integration-qa-20261116`、原工作树完整保留。
- 原有 156 个已跟踪文件的改动与 129 个未跟踪文件已接入本工作树，接入时完整二进制补丁比对一致。原始副本、未跟踪文件 SHA 和补丁位于 `.cache/recovery/original-work/`。
- 原主会话及 A–G 均已停止。本轮没有跨会话派工、没有新建子代理；测试使用本会话后台进程。
- 验证快照生成时尚未提交或推送。用户于2026-11-17授权提交并推送远端qa；main不在此次发布范围。未安装依赖或执行部署。

## 代码交付

`composition.py` 创建 SessionToolHost 与 Planning/Interaction/Memory/Goal/Context/Media 服务，公共 bundle 接普通/HTTP/开发目标；Web 与 MCP 通过同一 run 的 RunTools 装配。API 已注册；集中数据库 schema18 包含新服务数据和媒体存储。

- 同一会话计划跨轮次持久保存、版本冲突和固定版本分页。
- 问题/计划确认停靠后从原调用恢复，新用户消息优先，未知副作用不重放。
- Goal 记录完成条件和证据，串行续跑；暂停、取消、待答/待批、失权时停止；重复无进展失败转 blocked。
- Web 完整正文引用和 MCP resources/templates/read；tool_search 按需选择 schema。派生目录保留来源目录冻结校验。
- 私有/项目记忆沿用用户开关、作用域和版本，普通与开发会话共用模型工具。
- 授权图像原图 SQLite 持久保存、真实 MIME、请求内选择、refs-only checkpoint 与成功请求确认。
- 普通上下文 schema4、开发 schema3，共用压缩和独立 plan/goal/memory 投影；原历史保存，手动 Compact 在安全请求边界执行。
- 开发配置 schema2 默认 unlimited；累计运行/步骤/调用/内容可为 null；文件正文经 stdin 分块，不进入 argv；新日志完整追加，可继续读取。
- 旧限额环境通过显式迁移和内核读回变更；旧冻结 WorkspaceProfile、审批和结果继续保持原版本语义，不能宣称所有历史路径已无限额。

记忆管理 `Memory.tsx` 仍是独立组件，原实施约定明确其账号 UI 未批准，故未直接挂进正式 App。Goal 本轮主要通过工具/API/调度提供；没有借收尾新增常驻入口或重新设计 UI。

## 恢复后实际修复

1. 旧账号运行测试替身补齐新增可选服务字段；评测来源夹具显式登记宿主身份，不放宽产品身份解析。
2. 修正迁移/schema 的过期断言、跨目录同名 conftest 导入和 CSP 测试预期；保留只读、回滚、数据与权限断言。
3. 移除非超时测试自行设置的短整轮期限，调整账号测试的 HTTP 屏障释放顺序；真实工具预算、凭据期限和产品权限保持。
4. Goal 暂停测试明确等待运行中续跑取消，消除 release 与取消竞争。
5. 命令与维护路径读取旧 limits 改用 limits_from_record，避免缺 policy 的旧记录被解释为 unlimited。
6. 检查点分页取消 offset1000 总量上限；维护扫描不再在8192条后中止或漏项。新增旧记录/后续页/后续扫描回归。
7. SessionSDK 取消历史分页 offset100000 上限，保留整型、非负和单页窗口验证。
8. agent_tools 与 SDKAdapter 的同步工具声明批次复用已校验目录和 schema，避免每个工具重复校验完整目录。装配前后均校验，实际执行中的 get/fence/权限检查不变。原3秒/10秒启动用例未延长，专项60项与批量声明7项通过。
9. QA闪跳修复已局部接入：同一会话刷新保留历史，仅真正切换会话才清空；读取未完成时禁止旧 revision 提交。独立原工作树保留，原生成 bundle 未复制，当前已重新构建。
10. 默认验证入口纳入所有60个新增 Python 测试文件、记忆契约/UI、闪跳测试、请求预算和发行清单测试；记忆测试此前漏检的类型错误已修正。
11. wheel 明确列出脚本/样式并只展开受管理字体目录；sdist 核对规则与现有 deployment/browser 来源对齐。原过期的发行预检回归34项通过。
12. 实测 Docker 29 将无限 PID 返回为 null、并在 Mounts 中省略 tmpfs，只在 HostConfig.Tmpfs 返回完整配置。验证器现在接受这两种等价表示；有限 PID 不接受 null，完整 tmpfs 配置和精确项目挂载仍必须匹配。相关81项回归与真实 runsc 终端通过。
13. 真实联合任务暴露 SQLiteStore._event 的64 KiB残留总量上限；已取消该字节额度，保留JSON类型/结构验证、事件键幂等和冲突拒绝。新增超过1 MiB的参数与结果持久化/重放/只读重开测试，连同原存储测试纳入默认CI。74项存储/请求预算/文件专项通过。
14. 新增显式启用的 `tests/coding_agent/verify_general_tools_real.py`，通过正式API与真实Broker/XFS/runsc串起通用工具；首次问题落库与停靠完成的时序差异已修正，脚本等待awaiting再重启，不修改产品的关闭取消语义。

## 验证结果与证据

Python 3.12.3、现成锁定依赖。前端复用现有 Node24.22.2，满足 >=24.15.0；没有安装新工具。下列集合有重叠，不累加。

| 组 | 结果 | `.cache/recovery/` 报告 |
| --- | --- | --- |
| SDK/凭据 CI 前段 | 206 passed | ci-completion-0-result.json |
| 凭据/环境/预览 | 262 passed | ci-completion-1-result.json |
| 预览/存储/浏览器 | 206 passed | ci-completion-2-result.json |
| 浏览器桥接/SDK上下文/Git | 401 passed | ci-completion-3-result.json |
| 终端/会话/迁移/账号早期快照 | 302 passed，1条件skip已在最终实跑替代 | ci-completion-4-result.json |
| 网络/配额退役/MCP/Web | 209 passed | ci-completion-6-result.json |
| 通用能力/Goal/Planning/Interaction/Memory | 155 passed | capabilities-final-result.json |
| 会话工具与无限额 | 171 passed | tail-session-result.json |
| 上下文及通用任务恢复 | 76 passed | tail-context-result.json |
| 媒体/文件/模型协议 | 133 passed | tail-media-result.json |
| 只读/上下文记录/安全评测 | 116 passed | tail-core-result.json |
| 维护旧限额和后续分页/扫描 | 47 passed | maintenance-final-result.json |
| SDK与服务分页 | 47 passed | sdk-pagination-result.json |
| 请求预算 | 15 passed | request-budgets-result.json |
| Web持久化/sandbox schema4 | 8 passed | coverage-additions-result.json |
| 发行清单/字体 | 34 passed | distribution-result.json |
| SDK启动、审批及冻结专项 | 60 passed | sdk-startup-final-result.json |
| SDK批量声明与宿主桥接 | 7 passed | sdk-batch-declarations-result.json |
| 沙箱存储/检查点 | 173 passed | sandbox-storage-final-result.json |
| 项目绑定/调额/文件变更 | 104 passed | sandbox-binding-final-result.json |
| 个人MCP/后台任务/浏览器策略 | 89 passed | sandbox-private-final-result.json |
| 命令与停靠取消完整组 | 43 passed | sandbox-command-final-result.json |
| 最后取消阶段精确诊断 | 2 passed | cancel-probe-result.json |
| 前端类型/契约/生产构建 | passed | frontend-final-check.log / frontend-contracts-node24.log / frontend-final-build.log |
| 前端组件 | 33文件 / 388 passed | frontend-final-components.log |
| 变动Python ruff | 255文件，0问题 | static-result.json |
| 关键源码 mypy | 14文件，0问题 | static-result.json |
| 最终存储/Docker/恢复/通用工具 | 240 passed | real-closeout-final-result.json |
| 实机网络失权观察完整文件 | 11 passed，0 skipped | real-network-final-result.json |

长 CI 曾因整体超时、临时目录过长导致 Unix socket 超长，以及多组并行下短启动窗失败；这些结果保留在对应旧报告。已按短目录和分组重验，不把进度点当通过。全树额外 lint 曾发现未改旧材料/旧测试问题，没有全局自动格式化。

## 真实宿主与联合验收

使用现有可信Linux宿主、Docker 29、runsc及XFS project quota；只创建独立临时资源，没有安装依赖或拉取镜像。

- `.cache/recovery/real-terminal-result.json`：真实命令幂等、stdin/EOF、应用重开保留服务、取消进程树、运行取消不误停项目服务、环境停止覆盖服务均通过。5个跟踪容器已删除或确认不存在，监督进程退出0；该探针的项目内容按原设计保留。
- `.cache/recovery/real-xfs-result.json`：经真实预览/确认，将数据48 MiB与检查点16 MiB及各自inode限额改为0/0；内核读回确认，文件SHA不变。两个临时配额清零，临时目录移除。
- `.cache/recovery/general-tools-real-final/result.json`：联合验收exit0。真实项目写入1,176,147字节app.py并在容器内执行unittest，正式API完成2次交互答复和4次工具审批。用户问题跨应用重开后从原调用恢复，ask_user仅执行一次。16次工具执行均为ok，涵盖计划模式/计划、Goal、提问、记忆、工具发现、MCP资源和工具、write/shell/read_output、图片与压缩；Goal在顺序续跑后completed。
- 同一联合任务22次回环HTTP模型请求中有5次携带图像，手动压缩生成1份持久摘要，随后3轮请求均完成；内核配额读回0/0。监督进程退出0，3个关联容器已删除或确认不存在，临时根目录移除，fixture_errors与cleanup_blockers均为空。

模型是确定性回环HTTP夹具，MCP为官方SDK的本地stdio服务。它证明真实基础设施和协议链路，不证明远端模型规划或识图能力。失败的r2/r3/r4及修复过程保留；最终有效入口只指向general-tools-real-final。

## 浏览器实际验收

使用 preview_start 启动真实 CLI/API 及随包正式网页，操作创建会话、真实 FakeProvider/SDK 工具运行，四轮对话结果持久读回。实际按 Compact session 成功排队，下一轮继续运行。它证明真实本机服务路径，不代表远端模型实测。

正式构建已合入闪跳修复。MutationObserver 观察连续发送期间 chat-empty 出现次数为0；历史四轮保留，结果正确，草稿清空。输入框 rows1，空白20px，多行增高至约213px后内部滚动，清空恢复20px；未改控件外观。

真实 App + 内存API组件夹具验收：375px 时 fonts=loaded、scrollWidth=innerWidth；计划展开与2→4固定版本分页、空答复提示、选择提交、计划确认、错误重试均操作。该夹具不冒充后端恢复/外部模型。实际新建弹窗和长文字/窄屏截图已检查。所有本轮预览服务已停止。

## 当前分发包

以 `.cache/recovery/final-distribution-result.json` 为准：

- wheel SHA256 `87faaee2340cf81371bec49522001547959936981d7b154017dd8c7913c92962`。
- sdist SHA256 `7af18c0ff2786c482173f954f94c7d8877321651ff133c671c053b84745bbf7b`。
- 475 wheel payload / 608 sdist source 文件，精确清单、元数据、依赖、入口和来源字节核对通过。
- 48模块源码树外 -I 导入通过，errors={}、wrong_origin={}；隐私与禁入扫描0命中。
- 3脚本、123字体内容/大小/hash正确。`.cache/recovery/release-consistency.json` 确认当前 wheel payload 和 sdist source 漂移均为空。
- 构建复用既有 hatchling 环境；只解包离线导入，没有安装发行包。

当前包目录 `.cache/recovery/final-distribution-1794899055671531588/dist/`。旧c36、084、df235、de6候选均保留为历史，不能作为当前最终包。

## 环境阻塞与最后收口

1. 普通 WSL 账号对 Docker socket 无权访问，但现有可信宿主 root 控制路径可用。本轮已用该路径运行真实 Docker 29/runsc/XFS 验收；未改组、安装或启动 daemon，容器内未暴露宿主 Docker socket。真实终端与配额迁移通过，联合任务结果另见实机验收记录。
2. 本轮没有配置可用的远端Web/MCP凭据或视觉模型。官方MCP SDK的真实本地stdio/HTTP与真实SDK协议已经跑过，不能当作远端服务和真实识图结果。
3. 记忆账号管理页面保持独立，因为原计划要求UI批准。通用记忆工具/HTTP及组件测试已交付。
4. 默认CI增加核心存储文件后共225文件，均有完整通过报告；去重2858项通过、0跳过。最后两处产品修复的240项回归通过，原实机条件跳过已实跑并通过。旧的超时/中断结果保留，但不纳入完整通过报告；统计以最新final-ci-inventory.json为准。当前没有遗留测试后台进程或预览服务。
5. 本地实现、正式页面交互、真实基础设施联合验收已完成；外部服务/视觉模型验收没有可用配置，因此仍单列未验证。账号UI接入另遵守原审批约定。提交与推送按2026-11-17用户授权仅面向qa，发布结果以Git远端记录为准，不合并main。
