#!/usr/bin/env python3
"""只在项目专用 WSL 内启动沙盒资格与非 root 应用。"""

from __future__ import annotations

import argparse
import ctypes
import os
import pwd
import re
import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYTHON = "/home/krim/.venvs/krim-agent/bin/python"
PREPARER = str(ROOT / "scripts/prepare_dev_sandbox.py")
PREFIX = "krim-agent-p5-sandbox-"


def checked_unit(value: str) -> str:
    if not value.startswith(PREFIX) or not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
        raise ValueError("只能操作本项目创建的资格 unit")
    return value if value.endswith(".service") else value + ".service"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    commands.add_parser("build-rootfs")
    commands.add_parser("start")
    status = commands.add_parser("status")
    status.add_argument("unit")
    execute = commands.add_parser("exec")
    execute.add_argument("unit")
    execute.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if os.environ.get("WSL_DISTRO_NAME") != "Krim-Ubuntu-24.04":
        parser.error("此开发辅助命令仅供 Krim-Ubuntu-24.04 使用")
    if args.action == "build-rootfs":
        return subprocess.run([PYTHON, "-I", "-B", PREPARER, "build-rootfs"], cwd=ROOT).returncode
    if args.action == "status":
        return subprocess.run(
            [
                "/usr/bin/systemctl",
                "show",
                checked_unit(args.unit),
                "--property=Id,ActiveState,SubState,ControlGroup,Delegate,DelegateSubgroup,User,NoNewPrivileges",
            ],
            check=False,
        ).returncode
    if os.geteuid() != 0:
        parser.error("start/exec 需要在该 WSL 内由 root 建立专用委派或迁移本进程")
    if args.action == "start":
        unit = PREFIX + datetime.now(UTC).strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:8]
        command = [
            "/usr/bin/systemd-run",
            "--quiet",
            "--collect",
            "--no-block",
            "--unit=" + unit,
            "--property=Type=exec",
            "--property=User=krim",
            "--property=Group=krim",
            "--property=Delegate=yes",
            "--property=DelegateSubgroup=app",
            "--property=NoNewPrivileges=yes",
            "--property=UMask=0077",
            "--property=WorkingDirectory=" + str(ROOT),
            "--setenv=PYTHONDONTWRITEBYTECODE=1",
            PYTHON,
            "-I",
            "-B",
            PREPARER,
            "qualify",
        ]
        result = subprocess.run(command, check=False)
        if result.returncode == 0:
            print(unit + ".service", flush=True)
        return result.returncode
    unit = checked_unit(args.unit)
    if not args.command:
        parser.error("exec 需要提供非 root 应用命令")
    state = subprocess.check_output(
        ["/usr/bin/systemctl", "show", unit, "--value", "--property=ActiveState"], text=True
    ).strip()
    if state != "active":
        parser.error("资格服务尚未激活")
    relative = subprocess.check_output(
        ["/usr/bin/systemctl", "show", unit, "--value", "--property=ControlGroup"], text=True
    ).strip()
    parent = Path("/sys/fs/cgroup") / relative.lstrip("/")
    if not relative.startswith("/system.slice/") or parent.name != unit:
        parser.error("专用 cgroup 位置不匹配")
    account = pwd.getpwnam("krim")
    # 只迁移本启动进程，然后在执行应用前永久丢弃 root 权限。
    (parent / "app/cgroup.procs").write_text(str(os.getpid()))
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(38, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "无法设置 NoNewPrivileges")
    os.setgroups([])
    os.setgid(account.pw_gid)
    os.setuid(account.pw_uid)
    os.chdir(ROOT)
    env = os.environ | {
        "HOME": account.pw_dir,
        "USER": "krim",
        "LOGNAME": "krim",
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    os.execvpe(args.command[0], args.command, env)
    return 127


if __name__ == "__main__":
    raise SystemExit(main())
