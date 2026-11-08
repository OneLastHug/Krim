"""Broker的XFS账号配额；配额状态必须由宿主查询，不能只保存配置。"""

from __future__ import annotations

import hashlib
import os
import stat
import subprocess
import uuid
from pathlib import Path
from typing import Any

from krim_agent.contracts import ServiceError
from krim_agent.sandbox.quota import QuotaLimits


def internal_id(value: str) -> str:
    try:
        if type(value) is not str or str(uuid.UUID(value)) != value:
            raise ValueError("内部ID无效")
        return value
    except (ValueError, AttributeError):
        raise ServiceError("invalid_input", 422) from None


def private_directory(path: Path) -> Path:
    if not path.is_absolute():
        raise ServiceError("sandbox_config_invalid", 422)
    for parent in reversed([path, *path.parents]):
        if not stat.S_ISDIR(parent.lstat().st_mode):
            raise ServiceError("sandbox_config_invalid", 422)
    return path


class XfsAccounts:
    def __init__(self, mount: Path, root: Path) -> None:
        self.mount = private_directory(mount)
        self.root = private_directory(root)
        if self.root.parent != self.mount or any(
            c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/_-"
            for c in str(root)
        ):
            raise ServiceError("sandbox_config_invalid", 422)
        self._identity = (self.root.stat().st_dev, self.root.stat().st_ino)
        target = subprocess.run(
            ["findmnt", "-n", "-o", "FSTYPE,OPTIONS", "-T", str(root)],
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        ).stdout
        if not target.startswith("xfs ") or "prjquota" not in target:
            raise ServiceError("sandbox_backend_unavailable", 503)
        state = self.command("state -p")
        if "Accounting: ON" not in state or "Enforcement: ON" not in state:
            raise ServiceError("sandbox_backend_unavailable", 503)

    def current(self) -> None:
        private_directory(self.root)
        info = self.root.stat()
        if (info.st_dev, info.st_ino) != self._identity:
            raise ServiceError("sandbox_backend_unavailable", 503)
        state = self.command("state -p")
        if "Accounting: ON" not in state or "Enforcement: ON" not in state:
            raise ServiceError("sandbox_backend_unavailable", 503)

    def command(self, expression: str) -> str:
        result = subprocess.run(
            ["/usr/sbin/xfs_quota", "-x", "-c", expression, str(self.mount)],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode or result.stderr.strip():
            raise ServiceError("sandbox_backend_unavailable", 503)
        return result.stdout

    @staticmethod
    def project_number(environment_id: str) -> int:
        internal_id(environment_id)
        return 100_000 + int(hashlib.sha256(environment_id.encode()).hexdigest()[:7], 16)

    def usage(self, number: int) -> dict[str, int]:
        self.current()
        output = self.command(f"quota -p -b -i -n -N {number}")
        rows = [line.split() for line in output.splitlines() if line.startswith("/dev/")]
        if len(rows) != 1 or len(rows[0]) < 10:
            raise ServiceError("sandbox_backend_unavailable", 503)
        row = rows[0]
        try:
            return {
                "environment_bytes": int(row[1]) * 1024,
                "byte_limit": int(row[3]) * 1024,
                "environment_inodes": int(row[6]),
                "inode_limit": int(row[8]),
            }
        except (ValueError, IndexError):
            raise ServiceError("sandbox_backend_unavailable", 503) from None

    def provision(self, environment_id: str, limits: QuotaLimits) -> dict[str, Any]:
        self.current()
        number = self.project_number(environment_id)
        path = self.root / environment_id
        if path.exists():
            if not stat.S_ISDIR(path.lstat().st_mode):
                raise ServiceError("artifact_invalid")
            with os.scandir(path) as entries:
                if next(entries, None) is not None:
                    # 无已确认ready回执却有内容时，不能递归重标记未知租户文件。
                    raise ServiceError("sandbox_cleanup_required")
        else:
            path.mkdir(mode=0o700)
        # 父目录由Broker持有，不挂给租户；初始化project标志后子目录自动继承。
        self.command(f"project -s -p {path} {number}")
        kib = limits.environment_bytes // 1024
        self.command(f"limit -p bhard={kib}k ihard={limits.environment_inodes} {number}")
        usage = self.usage(number)
        if usage["byte_limit"] != kib * 1024 or usage["inode_limit"] != limits.environment_inodes:
            raise ServiceError("sandbox_backend_unavailable", 503)
        return {
            "project_number": number,
            "limits": limits.model_dump(mode="json"),
            "usage": usage,
            "enforcement": "xfs_project_quota",
        }

    def checkpoint_partition(self, environment_id: str, limits: QuotaLimits) -> dict[str, Any]:
        """从账号总额内拆分独立检查点预算；project quota不假装具有父子叠加。"""
        self.current()
        number = self.project_number(environment_id)
        checkpoint_number = number + 0x40000000
        path = self.root / environment_id / "checkpoints"
        if path.exists():
            private_directory(path)
        else:
            path.mkdir(mode=0o700)
        if not limits.checkpoint_bytes or not limits.checkpoint_count:
            raise ServiceError("sandbox_limit")
        ck_inodes = min(limits.environment_inodes // 4, max(128, limits.checkpoint_count * 64))
        data_bytes = (limits.environment_bytes - limits.checkpoint_bytes) // 1024 * 1024
        data_inodes = limits.environment_inodes - ck_inodes
        # 下调数据区不删除已有文件；不满足数据区新预算时拒绝开始写任务。
        used = self.usage(number)
        if used["environment_bytes"] > data_bytes or used["environment_inodes"] > data_inodes:
            raise ServiceError("sandbox_limit")
        self.command(f"project -s -p {path} {checkpoint_number}")
        ck_kib = limits.checkpoint_bytes // 1024
        self.command(f"limit -p bhard={ck_kib}k ihard={ck_inodes} {checkpoint_number}")
        self.command(f"limit -p bhard={data_bytes // 1024}k ihard={data_inodes} {number}")
        actual = self.usage(number)
        checkpoint = self.usage(checkpoint_number)
        if (
            actual["byte_limit"] != data_bytes
            or actual["inode_limit"] != data_inodes
            or checkpoint["byte_limit"] != ck_kib * 1024
            or checkpoint["inode_limit"] != ck_inodes
        ):
            raise ServiceError("sandbox_backend_unavailable", 503)
        return {
            "checkpoint_number": checkpoint_number,
            "checkpoint_bytes": checkpoint["byte_limit"],
            "data_bytes": data_bytes,
        }

    def account_usage(self, record: dict[str, Any]) -> dict[str, int]:
        used = self.usage(record["project_number"])
        if record.get("checkpoint_number") is not None:
            checkpoint = self.usage(record["checkpoint_number"])
            used = {key: used[key] + checkpoint[key] for key in used}
        return used
