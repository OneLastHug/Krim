#!/usr/bin/env python3
"""沙箱部署/备份dry-run CLI：只读预检、单元渲染、备份计划与恢复预览。

默认只读取与输出计划，不执行主机变更、真实备份或恢复。生产恢复没有单开关
授权：本CLI不存在`--yes`，也不提供任何直接执行恢复的子命令。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sqlite3
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_DIR = REPO_ROOT / "deployment" / "sandbox"
IMAGE_PATTERN = re.compile(r"(?:sha256:[0-9a-f]{64}|[A-Za-z0-9._/:-]+@sha256:[0-9a-f]{64})\Z")
PLACEHOLDER_PATTERN = re.compile(r"@KRIM_[A-Z0-9_]+@")
SECRET_KEY_PATTERN = re.compile(
    r"(secret|password|token|passphrase|api_?key|master|credential)", re.IGNORECASE
)
# 密封凭据归档里拒绝出现的文件名形态；主密钥必须走独立保管，不入任何备份。
MASTER_KEY_HINT = re.compile(r"(master|[.]key$)", re.IGNORECASE)
LXSS_KEY = r"HKCU\Software\Microsoft\Windows\CurrentVersion\Lxss"
REGISTRY_VALUE = re.compile(r"^\s+(\S+)\s+REG_(?:SZ|EXPAND_SZ)\s+(.+?)\s*$")
NATIVE_LINUX_FILESYSTEMS = frozenset({"ext2", "ext3", "ext4", "xfs", "btrfs", "zfs"})
WINDOWS_OR_NETWORK_FILESYSTEMS = frozenset(
    {"9p", "drvfs", "cifs", "nfs", "fuse", "fuseblk", "ntfs", "vfat", "exfat"}
)
SQLITE_SUFFIXES = (".db", ".sqlite", ".sqlite3")
# 合成fixture执行适配拒绝的真实数据根前缀；正式数据永远不走本机测试适配器。
REAL_DATA_PREFIXES = (
    "/srv/krim-sandbox",
    "/var/lib/krim-sandbox",
    "/var/lib/docker",
    "/var/lib/containerd",
)

PASS = "pass"
FAIL = "fail"
UNKNOWN = "unknown"


def verdict(name: str, status: str, detail: str = "") -> dict[str, str]:
    return {"check": name, "status": status, "detail": detail}


def mask_secrets(value: Any) -> Any:
    """输出前递归掩码：字典键命中秘密命名且值为字符串/字节时替换为占位符。"""
    if isinstance(value, dict):
        masked = {}
        for key, item in value.items():
            if SECRET_KEY_PATTERN.search(str(key)) and isinstance(item, (str, bytes)):
                masked[key] = "<redacted>"
            else:
                masked[key] = mask_secrets(item)
        return masked
    if isinstance(value, list):
        return [mask_secrets(item) for item in value]
    return value


def emit(result: dict[str, Any]) -> None:
    print(json.dumps(mask_secrets(result), ensure_ascii=False, indent=2))


# ---------------------------------------------------------------- 路径与权限审计


def inspect_parent_chain(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    """lstat整条父链；出现symlink或非目录时记为问题，绝不跟随。"""
    problems: list[str] = []
    entries: list[dict[str, Any]] = []
    if not path.is_absolute():
        problems.append(f"{path}: 路径必须绝对")
        return entries, problems
    for item in reversed([path, *path.parents]):
        try:
            info = item.lstat()
        except OSError as error:
            problems.append(f"{item}: {error.strerror or 'lstat失败'}")
            continue
        entries.append(
            {
                "path": str(item),
                "uid": info.st_uid,
                "gid": info.st_gid,
                "mode": format(stat.S_IMODE(info.st_mode), "04o"),
            }
        )
        if stat.S_ISLNK(info.st_mode):
            problems.append(f"{item}: symlink（必须拒绝，不跟随）")
        elif not stat.S_ISDIR(info.st_mode):
            problems.append(f"{item}: 不是目录")
    return entries, problems


def writable_by(info: Any, uid: int, gids: frozenset[int]) -> bool:
    """该stat条目是否可被业务用户写入（其他写、属主写或业务组写）。"""
    mode = stat.S_IMODE(info.st_mode)
    if mode & 0o002:
        return True
    if info.st_uid == uid:
        return bool(mode & 0o200)
    if info.st_gid in gids:
        return bool(mode & 0o020)
    return False


def audit_path(
    path: Path,
    *,
    owner_uid: int | None,
    private_mode: bool,
    business_uid: int | None,
    business_gids: frozenset[int],
) -> list[str]:
    """admin自有代码/state审计：symlink拒绝、owner校验、业务用户不可写、可选私有权限。"""
    _, problems = inspect_parent_chain(path)
    chain = [path, *path.parents]
    if business_uid is not None:
        for item in chain:
            try:
                info = item.lstat()
            except OSError:
                continue
            if writable_by(info, business_uid, business_gids):
                problems.append(f"{item}: 业务用户(uid={business_uid})可写")
    if owner_uid is not None:
        for item in chain:
            try:
                info = item.lstat()
            except OSError:
                continue
            if info.st_uid != owner_uid:
                problems.append(f"{item}: 属主uid={info.st_uid}，应为{owner_uid}")
    if private_mode:
        try:
            info = path.lstat()
            if stat.S_IMODE(info.st_mode) & 0o077:
                problems.append(
                    f"{path}: 权限{format(stat.S_IMODE(info.st_mode), '04o')}过宽，要求仅属主"
                )
        except OSError:
            pass
    return problems


# ---------------------------------------------------------------- 文件系统与承载证据


def findmnt_target(path: Path) -> dict[str, Any] | None:
    if os.name != "posix":
        return None
    try:
        result = subprocess.run(
            ["findmnt", "-n", "-o", "FSTYPE,OPTIONS,SOURCE", "-T", str(path)],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    # findmnt对含空格路径输出\x20转义，按空白切分安全。
    parts = result.stdout.strip().split()
    if result.returncode or len(parts) < 3:
        return None
    return {"fstype": parts[0], "options": parts[1].split(","), "source": parts[2]}


def classify_mount(mount: dict[str, Any]) -> dict[str, Any]:
    source = mount["source"]
    return {
        "fstype": mount["fstype"],
        "native_linux_fs": mount["fstype"] in NATIVE_LINUX_FILESYSTEMS,
        "windows_or_network_fs": mount["fstype"] in WINDOWS_OR_NETWORK_FILESYSTEMS
        or source.startswith("/mnt/"),
        "prjquota": "prjquota" in mount["options"],
        "source": source,
    }


def check_native_mount(name: str, root: Path, *, require_prjquota: bool) -> dict[str, str]:
    mount = findmnt_target(root)
    if mount is None:
        reason = "路径不存在" if not Path(root).exists() else "findmnt不可用或无结果"
        return verdict(f"{name}_filesystem", UNKNOWN, f"{root}: {reason}（dry-run不提权）")
    cls = classify_mount(mount)
    detail = f"fstype={cls['fstype']} prjquota={cls['prjquota']} source={cls['source']}"
    if cls["windows_or_network_fs"]:
        return verdict(f"{name}_filesystem", FAIL, f"{root}: 位于Windows/网络文件系统，{detail}")
    if require_prjquota and not cls["prjquota"]:
        return verdict(f"{name}_filesystem", FAIL, f"{root}: {detail}；配额强制需要XFS prjquota")
    status = PASS if cls["native_linux_fs"] else FAIL
    return verdict(f"{name}_filesystem", status, f"{root}: {detail}")


def parse_lxss_registry(text: str) -> dict[str, str]:
    """解析`reg query .../Lxss /s`输出，返回发行版名→BasePath。"""
    mapping: dict[str, str] = {}
    pending: str | None = None
    for line in text.splitlines():
        if line.startswith("HKEY_"):
            pending = None
            continue
        match = REGISTRY_VALUE.match(line)
        if match is None:
            continue
        name, value = match.group(1), match.group(2).strip()
        if name == "DistributionName":
            pending = value
        elif name == "BasePath" and pending is not None:
            mapping[pending] = value
    return mapping


def validate_host_evidence(data: Any) -> list[str]:
    if not isinstance(data, dict):
        return ["evidence必须是JSON对象"]
    problems = []
    for key in ("distro_name", "base_path_windows", "drive_letter", "source"):
        if not isinstance(data.get(key), str) or not data[key]:
            problems.append(f"缺少字段 {key}")
    if problems:
        return problems
    if data["source"] != "windows-registry":
        problems.append("source必须为windows-registry")
    if data["drive_letter"].upper() != "F":
        problems.append(f"盘符{data['drive_letter']}不是F：数据根只允许F盘承载")
    prefix = data["drive_letter"].upper() + ":"
    if not data["base_path_windows"].upper().startswith(prefix):
        problems.append("base_path_windows与drive_letter不一致")
    return problems


def check_hosting(
    classifications: dict[str, dict[str, Any] | None],
    evidence: dict[str, Any] | None,
    evidence_problems: list[str],
) -> dict[str, str]:
    if evidence_problems:
        return verdict("f_drive_hosting", FAIL, "；".join(evidence_problems))
    if evidence is None:
        return verdict(
            "f_drive_hosting",
            UNKNOWN,
            "缺少Windows侧证据：在Windows运行host-evidence子命令并用--evidence注入，不在Linux猜测",
        )
    resolvable = {name: cls for name, cls in classifications.items() if cls is not None}
    if not resolvable:
        return verdict(
            "f_drive_hosting",
            UNKNOWN,
            "证据已接受但没有可分类的数据根：提供--storage-root/--docker-root/--containerd-root后重跑",
        )
    for name, cls in resolvable.items():
        if not cls["native_linux_fs"] or cls["source"].startswith("/mnt/"):
            return verdict(
                "f_drive_hosting", FAIL, f"{name}不在原生Linux文件系统或位于/mnt挂载"
            )
    return verdict(
        "f_drive_hosting",
        PASS,
        f"已核验{len(resolvable)}个数据根均为原生Linux fs且无/mnt挂载；"
        "发行版vhdx由注册表证据承载于"
        f" {evidence['base_path_windows']}",
    )


def check_xfs_quota_state(storage_root: Path) -> dict[str, str]:
    if os.name != "posix" or not hasattr(os, "geteuid") or os.geteuid() != 0:
        return verdict("xfs_quota_state", UNKNOWN, "查询xfs_quota需要root（dry-run不提权）")
    xfs_quota = Path("/usr/sbin/xfs_quota")
    if not xfs_quota.is_file():
        return verdict("xfs_quota_state", UNKNOWN, "未安装/usr/sbin/xfs_quota")
    try:
        result = subprocess.run(
            [str(xfs_quota), "-x", "-c", "state -p", str(storage_root)],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return verdict("xfs_quota_state", UNKNOWN, f"查询失败: {error}")
    if result.returncode:
        return verdict("xfs_quota_state", UNKNOWN, "xfs_quota查询失败（路径可能不在XFS上）")
    accounting = "Accounting: ON" in result.stdout
    enforcement = "Enforcement: ON" in result.stdout
    status = PASS if accounting and enforcement else FAIL
    return verdict("xfs_quota_state", status, f"accounting={accounting} enforcement={enforcement}")


def check_capacity(root: Path, min_free_bytes: int) -> dict[str, str]:
    try:
        usage = os.statvfs(root)
    except OSError as error:
        return verdict("capacity", UNKNOWN, f"statvfs失败: {error}")
    free = usage.f_bavail * usage.f_frsize
    total = usage.f_blocks * usage.f_frsize
    status = FAIL if free < min_free_bytes else PASS
    return verdict("capacity", status, f"total={total} free={free} min_required={min_free_bytes}")


def check_sandbox_configuration(config_path: Path) -> dict[str, str]:
    if os.name != "posix":
        return verdict("sandbox_configuration", UNKNOWN, "配置校验仅支持posix")
    try:
        from krim_agent.contracts import ServiceError
        from krim_agent.sandbox.configuration import SandboxConfiguration
    except ImportError:
        return verdict("sandbox_configuration", UNKNOWN, "krim_agent不可用（PYTHONPATH需指向src）")
    try:
        config = SandboxConfiguration.load(config_path)
    except ServiceError:
        return verdict(
            "sandbox_configuration",
            FAIL,
            "load拒绝：sandbox_config_invalid（类型/权限/大小/内容不符）",
        )
    except OSError as error:
        return verdict("sandbox_configuration", UNKNOWN, f"读取失败: {error}")
    problems = []
    if config.broker_owner_uid != 0:
        problems.append(f"broker_owner_uid={config.broker_owner_uid}，Broker必须root(0)")
    if config.command_image is not None and not IMAGE_PATTERN.fullmatch(config.command_image):
        problems.append("command_image不是固定digest或本地image ID")
    status = FAIL if problems else PASS
    detail = "；".join(problems) if problems else "已加载且与root Broker语义一致"
    return verdict("sandbox_configuration", status, detail)


def check_templates(template_dir: Path | None = None) -> dict[str, str]:
    base = TEMPLATE_DIR if template_dir is None else template_dir
    expected = (
        "krim-sandbox-data.mount",
        "krim-sandbox-broker.service",
        "krim-sandbox-api.service",
        "README.md",
    )
    missing = [name for name in expected if not (base / name).is_file()]
    if missing:
        return verdict("deployment_templates", FAIL, "缺少: " + ", ".join(missing))
    return verdict("deployment_templates", PASS, "模板齐全: " + ", ".join(expected))


def run_preflight(args: argparse.Namespace) -> dict[str, Any]:
    checks: list[dict[str, str]] = []
    business_gids = frozenset({args.control_gid}) if args.control_gid is not None else frozenset()

    def audit(name: str, path: Path, *, owner_uid: int, private_mode: bool, ok: str) -> None:
        """路径未创建按unknown报告（尚未部署）；无法检查也按unknown，已存在才审计。"""
        if os.name != "posix":
            checks.append(verdict(name, UNKNOWN, "路径归属/权限审计需要posix语义"))
            return
        try:
            path.lstat()
        except FileNotFoundError:
            checks.append(verdict(name, UNKNOWN, f"{path}: 尚未创建（安装后重跑）"))
            return
        except OSError as error:
            checks.append(
                verdict(name, UNKNOWN, f"{path}: 无法检查（{error.strerror or 'lstat失败'}）")
            )
            return
        problems = audit_path(
            path,
            owner_uid=owner_uid,
            private_mode=private_mode,
            business_uid=args.control_uid,
            business_gids=business_gids,
        )
        checks.append(verdict(name, FAIL if problems else PASS, "；".join(problems) or ok))

    if args.config is not None:
        checks.append(check_sandbox_configuration(args.config))
    if args.install_root is not None:
        audit(
            "admin_owned_code",
            args.install_root,
            owner_uid=0,
            private_mode=False,
            ok="父链无symlink、root自有、业务用户不可写",
        )
    if args.state_root is not None:
        audit(
            "admin_owned_state",
            args.state_root,
            owner_uid=0,
            private_mode=True,
            ok="root自有且权限私有",
        )

    if args.broker_socket is not None:
        audit(
            "broker_socket_parent",
            args.broker_socket.parent,
            owner_uid=0,
            private_mode=True,
            ok="socket父目录root自有且私有",
        )
        try:
            args.broker_socket.lstat()
            checks.append(
                verdict(
                    "broker_socket_absent",
                    FAIL,
                    "socket路径已存在；Broker拒绝占用路径，遗留路径须运维显式核实清理",
                )
            )
        except FileNotFoundError:
            checks.append(verdict("broker_socket_absent", PASS, "socket路径不存在，可由Broker创建"))
        except OSError as error:
            checks.append(
                verdict(
                    "broker_socket_absent",
                    UNKNOWN,
                    f"{args.broker_socket}: 无法检查（{error.strerror or 'lstat失败'}）",
                )
            )

    if args.control_uid is not None:
        status = PASS if args.control_uid != 0 else FAIL
        detail = (
            "控制面用户非root，通过socket uid(0o660 root:gid)接入Broker"
            if args.control_uid != 0
            else "控制面用户不能是root；root Broker与业务控制面必须分离"
        )
        checks.append(verdict("control_plane_user", status, detail))
    else:
        checks.append(verdict("control_plane_user", UNKNOWN, "未提供--control-uid"))

    if args.broker_uid is not None:
        checks.append(
            verdict(
                "broker_runs_as_root",
                PASS if args.broker_uid == 0 else FAIL,
                "XFS配额管理要求Broker以root运行（broker.py启动强制校验）"
                if args.broker_uid == 0
                else f"broker uid={args.broker_uid}，配额管理要求root",
            )
        )
    else:
        checks.append(verdict("broker_runs_as_root", UNKNOWN, "未提供--broker-uid"))
    if args.command_image is not None:
        checks.append(
            verdict(
                "command_image_fixed",
                PASS if IMAGE_PATTERN.fullmatch(args.command_image) else FAIL,
                "固定digest/本地image ID"
                if IMAGE_PATTERN.fullmatch(args.command_image)
                else "镜像必须固定为sha256 digest或本地image ID，不用浮动tag",
            )
        )
    else:
        checks.append(verdict("command_image_fixed", UNKNOWN, "未提供--command-image"))

    classifications: dict[str, dict[str, Any] | None] = {}
    if args.storage_root is not None:
        checks.append(check_native_mount("storage", args.storage_root, require_prjquota=True))
        mount = findmnt_target(args.storage_root)
        classifications["storage_root"] = classify_mount(mount) if mount else None
        checks.append(check_xfs_quota_state(args.storage_root))
        checks.append(check_capacity(args.storage_root, args.min_free_bytes))
    for name, _ in (
        ("docker", Path("/var/lib/docker")),
        ("containerd", Path("/var/lib/containerd")),
    ):
        root = getattr(args, f"{name}_root")
        if root is not None:
            checks.append(check_native_mount(name, root, require_prjquota=False))
            mount = findmnt_target(root)
            classifications[f"{name}_root"] = classify_mount(mount) if mount else None

    evidence: dict[str, Any] | None = None
    evidence_problems: list[str] = []
    if args.evidence is not None:
        try:
            evidence = json.loads(args.evidence.read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            evidence_problems = [f"evidence读取失败: {error}"]
        else:
            evidence_problems = validate_host_evidence(evidence)
    checks.append(check_hosting(classifications, evidence, evidence_problems))
    checks.append(check_templates(args.template_dir))

    failures = [entry["check"] for entry in checks if entry["status"] == FAIL]
    unknowns = [entry["check"] for entry in checks if entry["status"] == UNKNOWN]
    return {
        "command": "preflight",
        # passed只表示"无fail"；confirmed还要求unknown为0，即所有检查项都实际核验通过。
        # 安装前必须confirmed=true：unknown项逐条补齐证据重跑，不得静默放过。
        "passed": not failures,
        "confirmed": not failures and not unknowns,
        "checks": checks,
        "warnings": [
            f"{name}: 未确认（{next(e['detail'] for e in checks if e['check'] == name)}）"
            for name in unknowns
        ],
        "execution": "只读预检；本命令不做任何主机变更",
    }


# ---------------------------------------------------------------- 备份计划与恢复预览


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def plan_digest(plan: dict[str, Any]) -> str:
    canonical = json.dumps(plan, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def inventory_tree(root: Path, limit: int) -> dict[str, Any]:
    files = 0
    total_bytes = 0
    symlinks: list[str] = []
    truncated = False
    for path in sorted(root.rglob("*")):
        try:
            info = path.lstat()
        except OSError:
            continue
        if stat.S_ISLNK(info.st_mode):
            symlinks.append(str(path.relative_to(root)))
        elif stat.S_ISREG(info.st_mode):
            files += 1
            total_bytes += info.st_size
            if files > limit:
                truncated = True
    return {
        "files": files,
        "bytes": total_bytes,
        "truncated_inventory": truncated,
        "symlink_count": len(symlinks),
        "symlinks": symlinks[:50],
    }


def scan_sqlite_files(root: Path) -> list[Path]:
    found: list[Path] = []
    for path in sorted(root.rglob("*")):
        if path.suffix not in SQLITE_SUFFIXES:
            continue
        try:
            if stat.S_ISREG(path.lstat().st_mode):
                found.append(path)
        except OSError:
            continue
    return found


def create_backup_plan(
    *,
    data_root: Path,
    state_root: Path,
    secrets_root: Path,
    backup_root: Path,
    extra_sqlite: list[Path] | None = None,
    inventory_limit: int = 2000,
    schema_version: int | None = None,
    fixture: bool = False,
) -> dict[str, Any]:
    """只读盘点并产出备份计划；不复制、不写任何目标。"""
    missing = [str(p) for p in (data_root, state_root, secrets_root) if not p.is_dir()]
    if missing:
        raise ValueError("根目录不存在: " + ", ".join(missing))
    chain_problems = {
        "data": inspect_parent_chain(data_root)[1],
        "state": inspect_parent_chain(state_root)[1],
        "secrets": inspect_parent_chain(secrets_root)[1],
    }
    sqlite_sources = sorted(
        {
            *scan_sqlite_files(data_root),
            *scan_sqlite_files(state_root),
            *(extra_sqlite or []),
        }
    )
    sqlite_targets = [
        {"source": str(path), "bytes": path.stat().st_size} for path in sqlite_sources
    ]
    sealed_files = 0
    sealed_bytes = 0
    master_excluded = 0
    secret_symlinks = 0
    for path in sorted(secrets_root.rglob("*")):
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode):
            secret_symlinks += 1
            continue
        if not stat.S_ISREG(info.st_mode):
            continue
        if MASTER_KEY_HINT.search(path.name):
            master_excluded += 1
            continue
        sealed_files += 1
        sealed_bytes += info.st_size
    plan = {
        "plan_version": 1,
        "kind": "backup",
        "fixture": fixture,
        "data_versions": {"control_schema": schema_version},
        "targets": [
            {
                "kind": "sqlite",
                "method": "sqlite_backup_api",
                "files": sqlite_targets,
                "note": "用sqlite3.Connection.backup()做一致快照；活动WAL下禁止普通cp",
            },
            {
                "kind": "projects",
                "source": str(data_root),
                "method": "copy_nofollow",
                **inventory_tree(data_root, inventory_limit),
            },
            {
                "kind": "state",
                "source": str(state_root),
                "method": "copy_nofollow",
                **inventory_tree(state_root, inventory_limit),
            },
            {
                "kind": "secrets",
                "source": str(secrets_root),
                "method": "sealed_archive_0600",
                "files": sealed_files,
                "bytes": sealed_bytes,
                "master_key_files_excluded": master_excluded,
                "symlink_count": secret_symlinks,
                "names_listed": False,
            },
        ],
        "destination": {
            "root": str(backup_root),
            "manifest": "manifest.json",
            "sealed_archive": "secrets/（0600）",
            "wal_sidecars_excluded": True,
        },
        "quiesce": {
            "required": [
                "coordinator停止并阻写（暂停API写入入口）",
                "Broker已停止且所有写租约释放",
                "逐个取得旧writer已停止的确认（stopped proof）；期满不自动视为已停止",
            ],
        },
        "steps": [
            {"order": 1, "action": "quiesce", "detail": "停Broker、阻API写、收集writer停止确认"},
            {"order": 2, "action": "sqlite_backup", "detail": "逐库用Connection.backup()一致快照"},
            {"order": 3, "action": "copy_trees", "detail": "不跟随symlink复制并算sha256"},
            {"order": 4, "action": "seal_secrets", "detail": "凭据密文独立0600归档，主密钥不入包"},
            {"order": 5, "action": "write_manifest", "detail": "manifest记录条目sha256与整体哈希"},
            {"order": 6, "action": "verify", "detail": "回读校验，不一致即中止且不标记完成"},
        ],
        "root_chain_problems": chain_problems,
        "warnings": [
            "恢复旧备份可能复活已轮换/撤销的密钥：恢复后必须核对撤销台账并按需重签发",
            "schema版本未知或比本工具认知更新时，先冻结并确认兼容再恢复",
            "主密钥不在任何备份归档中；由运维独立保管，恢复时单独密封录入",
            "日志与manifest不含秘密内容和秘密文件名",
        ],
    }
    return plan


def restore_preview(
    plan: dict[str, Any],
    *,
    known_schema_versions: frozenset[int] | None = None,
    expected_digest: str | None = None,
) -> dict[str, Any]:
    """恢复预览：只校验与列步骤，不执行；授权必须离线显式给出。"""
    problems = []
    if plan.get("kind") != "backup" or plan.get("plan_version") != 1:
        problems.append("不是受支持的备份计划（kind/plan_version不符）")
    digest = plan_digest(plan)
    if expected_digest is not None and expected_digest != digest:
        problems.append("plan digest与期望值不一致（计划可能被改动）")
    targets = plan.get("targets", [])
    if not isinstance(targets, list) or any(not isinstance(t, dict) for t in targets):
        problems.append("targets字段类型异常（应为对象数组），拒绝按其内容恢复")
        targets = []
    chain = plan.get("root_chain_problems")
    if isinstance(chain, dict):
        for name, chain_problems in sorted(chain.items()):
            if chain_problems:
                joined = "；".join(str(item) for item in chain_problems)
                problems.append(f"计划记录{name}根链审计问题，须先核实解决: {joined}")
    data_versions = plan.get("data_versions")
    schema = data_versions.get("control_schema") if isinstance(data_versions, dict) else None
    warnings = [
        "恢复时必须重新走quiesce流程并取得新的stopped proof，不沿用备份时的旧确认",
        "恢复前逐条校验manifest sha256；任一不一致即拒绝应用",
        "恢复旧凭据/secret可能复活已撤销或已轮换的旧key：恢复后核对撤销台账并按需重签发",
    ]
    if schema is None:
        warnings.append("计划未记录schema版本：按未知/更新数据处理，恢复前先冻结确认兼容")
    elif known_schema_versions is not None and schema not in known_schema_versions:
        warnings.append(f"schema版本{schema}不在已知范围：按更新数据确认兼容后再恢复")
    if any(target.get("kind") == "secrets" for target in targets):
        warnings.append("密封secret归档须单独授权解封，不混入公开归档或日志")
    return {
        "plan_version": plan.get("plan_version"),
        "plan_digest": digest,
        "targets": targets,
        "steps": plan.get("steps", []),
        "warnings": warnings,
        "problems": problems,
        "authorization": {
            "mode": "offline-explicit",
            "accepted": False,
            "cli_bypass": None,
            "required_confirmations": [
                "quiesce proof（全部旧writer已停止）",
                "manifest摘要与物理归档一致",
                "secret归档单独确认并密封录入主密钥",
                "恢复执行前重申plan digest",
            ],
        },
        "execution": "restore-preview只输出预览；本CLI没有执行恢复的开关",
    }


# ---------------------------------------------------------------- 合成fixture执行适配


def sqlite_backup(source: Path, destination: Path) -> None:
    """一致快照：sqlite3 backup API，只读打开源，不普通cp活动WAL。"""
    if destination.exists():
        destination.unlink()
    src = sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True)
    try:
        dst = sqlite3.connect(destination)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()


def copy_tree_nofollow(
    source: Path, destination: Path, entries: dict[str, Any], *, prefix: str
) -> list[str]:
    destination.mkdir(parents=True, exist_ok=True)
    skipped: list[str] = []
    for path in sorted(source.rglob("*")):
        relative = path.relative_to(source)
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode) or (
            not stat.S_ISREG(info.st_mode) and not stat.S_ISDIR(info.st_mode)
        ):
            skipped.append(str(relative))
            continue
        if stat.S_ISDIR(info.st_mode):
            (destination / relative).mkdir(parents=True, exist_ok=True)
            continue
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target, follow_symlinks=False)
        entries[f"{prefix}/{relative.as_posix()}"] = {
            "sha256": sha256_file(target),
            "bytes": info.st_size,
        }
    return skipped


def execute_synthetic_backup(plan: dict[str, Any], *, backup_root: Path) -> dict[str, Any]:
    """只允许对合成fixture执行；拒绝真实部署数据根，正式备份走运维流程。"""
    if not plan.get("fixture"):
        raise ValueError("仅合成fixture计划允许执行校验")
    if plan.get("kind") != "backup":
        raise ValueError("plan kind不符")
    sources = [str(t.get("source", "")) for t in plan.get("targets", [])]
    sources.append(str(plan.get("destination", {}).get("root", "")))
    for source in sources:
        if any(source.startswith(prefix) for prefix in REAL_DATA_PREFIXES):
            raise ValueError(f"拒绝真实数据根走合成适配器: {source}")
    manifest_path = backup_root / "manifest.json"
    if manifest_path.exists() or manifest_path.is_symlink():
        raise ValueError(f"拒绝覆盖已有manifest，请改用空目录: {manifest_path}")
    backup_root.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {
        "plan_digest": plan_digest(plan),
        "entries": {},
        "secrets": {
            "files": 0,
            "master_key_excluded": 0,
            "names_omitted": True,
            "sealed": True,
        },
    }
    skipped_symlinks: list[str] = []
    for target in plan["targets"]:
        kind = target["kind"]
        if kind == "sqlite":
            for entry in target["files"]:
                source = Path(entry["source"])
                destination = backup_root / "sqlite" / source.name
                destination.parent.mkdir(parents=True, exist_ok=True)
                sqlite_backup(source, destination)
                manifest["entries"][f"sqlite/{source.name}"] = {
                    "sha256": sha256_file(destination),
                    "bytes": destination.stat().st_size,
                }
        elif kind in {"projects", "state"}:
            skipped = copy_tree_nofollow(
                Path(target["source"]), backup_root / kind, manifest["entries"], prefix=kind
            )
            skipped_symlinks.extend(skipped)
        elif kind == "secrets":
            destination = backup_root / "secrets"
            destination.mkdir(parents=True, exist_ok=True)
            for path in sorted(Path(target["source"]).rglob("*")):
                info = path.lstat()
                if not stat.S_ISREG(info.st_mode):
                    continue
                if MASTER_KEY_HINT.search(path.name):
                    manifest["secrets"]["master_key_excluded"] += 1
                    continue
                copied = destination / path.name
                shutil.copyfile(path, copied, follow_symlinks=False)
                os.chmod(copied, 0o600)
                manifest["secrets"]["files"] += 1
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8"
    )
    os.chmod(manifest_path, 0o600)
    return {
        "manifest": str(manifest_path),
        "entries": len(manifest["entries"]),
        "skipped_symlinks": skipped_symlinks,
        "secrets": manifest["secrets"],
    }


# ---------------------------------------------------------------- 模板渲染与主机证据


def render_units(
    values: dict[str, str], names: list[str] | None = None, template_dir: Path | None = None
) -> dict[str, str]:
    base = TEMPLATE_DIR if template_dir is None else template_dir
    if not (base / "README.md").is_file():
        raise ValueError(f"模板目录不存在: {base}")
    units = names or sorted(
        path.name for path in base.iterdir() if path.suffix in {".service", ".mount", ".socket"}
    )
    rendered: dict[str, str] = {}
    for name in units:
        text = (base / name).read_text(encoding="utf-8")
        missing = sorted(set(PLACEHOLDER_PATTERN.findall(text)) - values.keys())
        if missing:
            raise ValueError(f"{name}: 缺少替换值 {', '.join(missing)}")
        rendered[name] = PLACEHOLDER_PATTERN.sub(lambda match: values[match.group(0)], text)
    return rendered


def run_render(args: argparse.Namespace) -> dict[str, Any]:
    values: dict[str, str] = {}
    for pair in args.set:
        name, separator, value = pair.partition("=")
        if not separator or not PLACEHOLDER_PATTERN.fullmatch(name):
            return {
                "command": "render",
                "error": f"--set需要@KRIM_*@=值形式: {pair}",
            }
        values[name] = value
    try:
        rendered = render_units(values, list(args.unit) or None, template_dir=args.template_dir)
    except ValueError as error:
        return {"command": "render", "error": str(error)}
    result: dict[str, Any] = {"command": "render", "units": sorted(rendered)}
    if args.output_dir is not None:
        # 误覆盖保护：输出目录必须已存在（防typo静默创建），且已有同名文件（含悬空
        # symlink）时整体拒绝，不写任何文件。
        if not args.output_dir.is_dir():
            return {"command": "render", "error": f"输出目录不存在: {args.output_dir}"}
        existing = sorted(
            name
            for name in rendered
            if (args.output_dir / name).exists() or (args.output_dir / name).is_symlink()
        )
        if existing:
            return {
                "command": "render",
                "error": "目标已存在，拒绝覆盖（请改用空目录）: " + ", ".join(existing),
            }
        for name, text in rendered.items():
            target = args.output_dir / name
            target.write_text(text, encoding="utf-8")
        result["written_to"] = str(args.output_dir)
    else:
        result["content"] = rendered
        result["note"] = "仅输出；未写主机。--output-dir需显式给出目标目录"
    return result


def run_host_evidence(args: argparse.Namespace) -> dict[str, Any]:
    if os.name != "nt":
        return {
            "command": "host-evidence",
            "error": "该子命令只在Windows主机运行（读取Lxss注册表）；Linux侧用--evidence注入",
        }
    try:
        result = subprocess.run(
            ["reg.exe", "query", LXSS_KEY, "/s"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"command": "host-evidence", "error": str(error)}
    if result.returncode:
        message = (result.stderr or result.stdout).strip()
        return {"command": "host-evidence", "error": message[-500:]}
    mapping = parse_lxss_registry(result.stdout)
    base = mapping.get(args.wsl_distro)
    if base is None:
        return {
            "command": "host-evidence",
            "error": f"未找到发行版 {args.wsl_distro} 的BasePath",
            "distros": sorted(mapping),
        }
    record = {
        "distro_name": args.wsl_distro,
        "base_path_windows": base,
        "drive_letter": base[:1],
        "source": "windows-registry",
        "registry_key": LXSS_KEY,
        "vhdx_present": (Path(base) / "ext4.vhdx").is_file(),
    }
    result_payload: dict[str, Any] = {
        "command": "host-evidence",
        "evidence": record,
        "problems": validate_host_evidence(record),
    }
    if args.output is not None:
        # 由本命令直接写JSON，避免手工转义把Windows路径写坏。
        args.output.write_text(
            json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        result_payload["written_to"] = str(args.output)
    return result_payload


def run_backup_plan(args: argparse.Namespace) -> dict[str, Any]:
    try:
        plan = create_backup_plan(
            data_root=args.data_root,
            state_root=args.state_root,
            secrets_root=args.secrets_root,
            backup_root=args.backup_root,
            extra_sqlite=list(args.sqlite) or None,
            inventory_limit=args.inventory_limit,
            schema_version=args.schema_version,
            fixture=args.fixture,
        )
    except ValueError as error:
        return {"command": "backup-plan", "error": str(error)}
    return {
        "command": "backup-plan",
        "plan": plan,
        "plan_digest": plan_digest(plan),
        "execution": "未执行：dry-run只输出计划；执行须按quiesce与授权流程由运维进行",
    }


def run_restore_preview(args: argparse.Namespace) -> dict[str, Any]:
    try:
        data = json.loads(args.plan.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        return {"command": "restore-preview", "error": f"计划读取失败: {error}"}
    # backup-plan输出的完整JSON（command/plan/plan_digest信封）与其内层plan等效：
    # 信封原样可预览，digest始终按内层plan计算，避免操作员手工拆包出错。
    if (
        isinstance(data, dict)
        and "plan" in data
        and isinstance(data["plan"], dict)
        and data.get("command") == "backup-plan"
    ):
        plan = data["plan"]
        envelope_digest = data.get("plan_digest")
    else:
        plan = data
        envelope_digest = None
    preview = restore_preview(plan, expected_digest=args.expect_digest)
    if envelope_digest is not None and envelope_digest != preview["plan_digest"]:
        preview["problems"].append("信封记录的plan_digest与内层plan不一致（计划可能被改动）")
    return {"command": "restore-preview", "preview": preview}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sandbox_admin", description="沙箱部署/备份dry-run工具；默认只读"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    pre = sub.add_parser("preflight", help="只读安装预检：归属/权限/文件系统/承载证据/容量")
    pre.add_argument("--config", type=Path, help="沙箱配置文件（SandboxConfiguration.load校验）")
    pre.add_argument("--install-root", type=Path, help="admin自有代码安装前缀")
    pre.add_argument("--state-root", type=Path, help="Broker state目录（root自有且0700）")
    pre.add_argument("--storage-root", type=Path, help="数据根（XFS prjquota挂载点）")
    pre.add_argument("--broker-socket", type=Path, help="Broker socket路径")
    pre.add_argument("--docker-root", type=Path, default=Path("/var/lib/docker"))
    pre.add_argument("--containerd-root", type=Path, default=Path("/var/lib/containerd"))
    pre.add_argument("--control-uid", type=int, help="非root控制面用户uid（业务用户）")
    pre.add_argument("--control-gid", type=int, help="控制面用户主组gid（组写检查）")
    pre.add_argument("--broker-uid", type=int, help="Broker运行uid期望值（必须0）")
    pre.add_argument("--command-image", help="固定镜像digest或本地image ID")
    pre.add_argument("--evidence", type=Path, help="Windows侧host-evidence输出的JSON文件")
    pre.add_argument("--min-free-bytes", type=int, default=1 << 30)
    pre.add_argument(
        "--template-dir",
        type=Path,
        default=None,
        help="模板目录（默认脚本同级deployment/sandbox；安装后指向安装前缀内模板目录）",
    )

    render = sub.add_parser("render", help="渲染systemd模板（默认stdout；不写主机）")
    render.add_argument("--unit", action="append", default=[], help="只渲染指定单元，可重复")
    render.add_argument(
        "--set", action="append", default=[], metavar="@KRIM_*@=值", help="占位符替换，可重复"
    )
    render.add_argument("--output-dir", type=Path, help="显式给出才写文件；已有同名文件整体拒绝")
    render.add_argument(
        "--template-dir",
        type=Path,
        default=None,
        help="模板目录（默认脚本同级deployment/sandbox；安装后指向安装前缀内模板目录）",
    )

    evidence = sub.add_parser("host-evidence", help="Windows侧读取Lxss注册表生成承载证据JSON")
    evidence.add_argument("--wsl-distro", required=True)
    evidence.add_argument("--output", type=Path, help="同时把证据JSON写到该文件，避免手工转义")

    backup = sub.add_parser("backup-plan", help="只读盘点并输出备份计划")
    backup.add_argument("--data-root", type=Path, required=True)
    backup.add_argument("--state-root", type=Path, required=True)
    backup.add_argument("--secrets-root", type=Path, required=True)
    backup.add_argument("--backup-root", type=Path, required=True)
    backup.add_argument("--sqlite", type=Path, action="append", default=[], help="额外sqlite文件")
    backup.add_argument("--inventory-limit", type=int, default=2000)
    backup.add_argument("--schema-version", type=int, default=None)
    backup.add_argument(
        "--fixture", action="store_true", help="标记合成fixture计划（仅测试执行器可用）"
    )

    restore = sub.add_parser("restore-preview", help="校验备份计划并输出恢复预览；不执行")
    restore.add_argument("--plan", type=Path, required=True)
    restore.add_argument("--expect-digest", default=None, help="期望的plan digest，校验篡改")
    return parser


RUNNERS = {
    "preflight": run_preflight,
    "render": run_render,
    "host-evidence": run_host_evidence,
    "backup-plan": run_backup_plan,
    "restore-preview": run_restore_preview,
}


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = RUNNERS[args.command](args)
    emit(result)
    if result.get("error"):
        return 1
    if args.command == "preflight":
        failed = any(entry["status"] == FAIL for entry in result.get("checks", []))
        return 2 if failed else 0
    if args.command == "restore-preview":
        # 预览发现任何问题（含digest不符、计划被改、根链问题）即非零退出，不静默。
        return 2 if result.get("preview", {}).get("problems") else 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
