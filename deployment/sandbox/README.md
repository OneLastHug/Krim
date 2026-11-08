# 沙箱部署模板（F1 dry-run）

本目录是参考模板。正式安装涉及root常驻服务、数据盘挂载、密钥生成与真实备份，须单独按授权执行；本目录不含任何已安装配置。所有机器相关值用`@KRIM_*@`占位符表达，安装时由运维提供实际值。

## 占位符

| 占位符 | 含义 | 示例形态 |
| --- | --- | --- |
| `@KRIM_INSTALL_ROOT@` | admin自有安装前缀，代码root所有、业务用户不可写 | `/opt/krim` |
| `@KRIM_STORAGE_ROOT@` | XFS数据根挂载点（prjquota） | `/srv/krim-sandbox` |
| `@KRIM_STORAGE_DISK@` | XFS镜像文件（位于F盘承载的发行版原生fs内） | `/var/lib/krim-sandbox/disks/accounts.xfs` |
| `@KRIM_STATE_ROOT@` | Broker state目录（root自有，0700） | `/var/lib/krim-sandbox/state` |
| `@KRIM_BROKER_SOCKET@` | Broker unix socket完整路径 | `/run/krim-sandbox/broker.sock` |
| `@KRIM_SOCKET_DIR@` | 上一项的父目录 | `/run/krim-sandbox` |
| `@KRIM_CONTROL_UID@` / `@KRIM_CONTROL_USER@` / `@KRIM_CONTROL_GROUP@` | 非root控制面用户 | 专用系统账号 |
| `@KRIM_SOCKET_GID@` | socket 0660的组gid | 控制面专用组 |
| `@KRIM_COMMAND_IMAGE@` | 固定镜像 `sha256:…` 或 `repo@sha256:…` | 不用浮动tag |
| `@KRIM_DB_PATH@` / `@KRIM_DB_DIR@` | 控制面SQLite库及所在目录 | admin自有目录 |
| `@KRIM_API_PORT@` | API监听端口 | 防火墙内端口 |
| `@KRIM_WORKSPACE_ROOT@` | 控制面可写工作区根 | admin自有目录 |

当前开发机样例（仅文档记录，不是模板默认值）：发行版vhdx为`F:\WSL\Krim-Ubuntu-24.04\ext4.vhdx`，安装结果见 `docs/proposals/sandbox-research/installation-results.md`。真实值一律以用户提供的占位符替换为准。

## 部署要点

- **数据根只在F盘承载的原生Linux文件系统**：不把镜像层或容器数据放`/mnt/f`的Windows文件系统；不格式化任何现有磁盘。承载关系用`scripts/sandbox_admin.py`核对：Windows上`host-evidence --wsl-distro <名称>`读注册表Lxss BasePath生成证据JSON，Linux上`preflight --evidence <该文件>`比对findmnt结果。
- **root Broker与非root API分离**：Broker以root运行（XFS配额管理），控制面API以专用非root用户运行，仅经socket的SO_PEERCRED uid校验接入；业务用户与模型不获得Docker socket。
- **不用systemd .socket单元**：Broker自行创建并校验socket（拒绝已存在路径、O_NOFOLLOW、创建后chown root:gid 0660），systemd预创建会与该检查冲突。
- **配额强制**：数据根必须XFS且挂载带`prjquota`；Broker启动与`preflight`都核对`xfs_quota state`的Accounting/Enforcement为ON。

## 启用前预检清单（全部可在不启动常驻服务的情况下验证）

1. `python scripts/sandbox_admin.py preflight --config <沙箱配置> --install-root <前缀> --state-root <state> --storage-root <数据根> --broker-socket <socket路径> --control-uid <uid> --control-gid <gid> --broker-uid 0 --command-image <固定镜像> --evidence <证据JSON>`：无fail即退出码0且`passed:true`，但`passed`只表示"无fail"；`confirmed:true`要求unknown也为0，即所有检查项都实际核验通过。安装前必须达到`confirmed:true`：unknown项逐条补齐证据重跑，不得静默放过。安装后运行时模板目录不在脚本同级，用`--template-dir <安装前缀>/share/krim/deployment/sandbox`指向。
2. `systemd-analyze verify krim-sandbox-data.mount krim-sandbox-broker.service krim-sandbox-api.service`：语法与依赖通过。
3. 配额工具在服务沙箱内可用（ProtectSystem=strict + ReadWritePaths的正确性不能只看文件，须实跑）：`systemd-run --property=ProtectSystem=strict "--property=ReadWritePaths=<数据根> <state> <socket目录>" --property=PrivateDevices=yes --property=CapabilityBoundingSet=CAP_SYS_ADMIN ... /usr/sbin/xfs_quota -x -c "state -p" <数据根>`。返回Accounting/Enforcement ON才算通过；若实际发行版的xfs_quota需要更多访问，必须先放宽该项并在交付记录说明原因，不允许静默失效。
4. `systemd-analyze security krim-sandbox-broker.service krim-sandbox-api.service`：与上文声明的capability/权限一致，无意外暴露。
5. 备份/恢复只做dry-run：`backup-plan`与`restore-preview`仅输出计划与预览，本CLI不存在`--yes`或任何执行恢复的开关；`restore-preview`直接接受`backup-plan`的完整输出JSON（按内层plan计算digest，信封digest与内层不符时报问题），发现任何问题（含`--expect-digest`不符、计划记录根链审计问题）即退出码2。`render --output-dir`要求输出目录已存在，且遇到已有同名文件时整体拒绝，不写任何文件。合成fixture的执行校验只用于测试（`execute_synthetic_backup`拒绝真实数据根前缀，且目标已有manifest时拒绝执行）。

## 备份/恢复授权流程（正式执行须单独批准）

1. **quiesce**：coordinator停止Broker并阻塞API写入；逐个取得旧writer已停止的确认。租约/状态"期满"不等于写者已停止，必须有显式stopped proof。
2. **一致快照**：SQLite一律走`Connection.backup()`；活动WAL下禁止普通cp。
3. **树复制**：projects（含影子Git）、state按不跟随symlink方式复制，逐文件sha256入manifest。
4. **秘密密封**：凭据密文独立0600归档；主密钥不入任何备份，独立保管、恢复时单独录入。日志与manifest不含秘密内容或秘密文件名。
5. **恢复**：先`restore-preview`校验manifest摘要与计划digest；重新走quiesce；逐条核对后才能应用。恢复旧凭据可能复活已撤销/轮换的key，恢复后必须核对撤销台账。遇到比当前认知更新或未知的schema/数据版本，先冻结确认兼容，不直接应用。
