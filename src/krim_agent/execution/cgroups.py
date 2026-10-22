"""只管理本Runner创建的委派叶及私有重启票据，不根据公有PID发信号。"""

import asyncio
import json
import os
import stat
import time
import uuid
from pathlib import Path

from krim_agent.contracts import ServiceError


class Leaf:
    def __init__(self, root: Path, tickets: Path, record):
        self.root, self.tickets, self.record = root, tickets, record
        if (root / "cgroup.procs").read_text().strip() or not {"cpu", "memory", "pids"} <= set(
            (root / "cgroup.subtree_control").read_text().split()
        ):
            raise ServiceError("sandbox_unavailable")
        tickets.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path = root / ("krim-d1-" + uuid.uuid4().hex)
        self.path.mkdir()
        self.ticket = tickets / (self.path.name + ".json")
        self.identity = {
            "name": self.path.name,
            "dev": self.path.stat().st_dev,
            "inode": self.path.stat().st_ino,
            "root_dev": root.stat().st_dev,
            "root_inode": root.stat().st_ino,
            "boot": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            "uid": os.getuid(),
        }
        with self.ticket.open("x") as f:
            json.dump(self.identity, f)
        self.ticket.chmod(0o600)
        record("leaf_created", **self.identity)

    def check(self):
        st = self.path.stat()
        root = self.root.stat()
        if (st.st_dev, st.st_ino) != (self.identity["dev"], self.identity["inode"]) or (
            root.st_dev,
            root.st_ino,
        ) != (self.identity["root_dev"], self.identity["root_inode"]):
            raise ServiceError("sandbox_cleanup_required")

    def limits(self, profile):
        for k, v in {
            "cpu.max": "50000 100000",
            "memory.max": str(profile.memory),
            "memory.swap.max": "0",
            "memory.oom.group": "1",
            "pids.max": str(profile.pids),
        }.items():
            (self.path / k).write_text(v)
            if (self.path / k).read_text().strip() != v:
                raise ServiceError("sandbox_unavailable")
        self.record("limits", cpu="50000 100000", memory=profile.memory, pids=profile.pids)

    def join(self, pid):
        self.check()
        (self.path / "cgroup.procs").write_text(str(pid))
        if str(pid) not in (self.path / "cgroup.procs").read_text().split():
            raise ServiceError("sandbox_setup")
        self.record("joined", pid=pid)

    def members(self):
        self.check()
        return set((self.path / "cgroup.procs").read_text().split())

    def counters(self):
        self.check()

        def values(name):
            return dict(line.split() for line in (self.path / name).read_text().splitlines())

        return {
            "cpu": values("cpu.stat"),
            "memory": values("memory.events"),
            "pids": values("pids.events"),
        }

    async def finish(self):
        if self.path.exists():
            self.check()
            (self.path / "cgroup.kill").write_text("1")
            self.record("cgroup_kill")
            deadline = time.monotonic() + 10
            while "populated 1" in (self.path / "cgroup.events").read_text():
                if time.monotonic() > deadline:
                    raise ServiceError("sandbox_cleanup_failed")
                await asyncio.sleep(0.02)
            self.record("populated_zero", events=(self.path / "cgroup.events").read_text())
            self.path.rmdir()
            self.record("leaf_removed")
        if self.ticket.exists():
            self.ticket.unlink()


def recover(root: Path, tickets: Path):
    """重启只核实自有inode/boot/UID；退出码不可恢复，不补造成功。"""
    if not tickets.exists():
        return []
    results = []
    for file in tickets.iterdir():
        st = file.lstat()
        if (
            not stat.S_ISREG(st.st_mode)
            or st.st_uid != os.getuid()
            or st.st_mode & 0o077
            or st.st_nlink != 1
        ):
            raise ServiceError("sandbox_cleanup_required")
        data = json.loads(file.read_text())
        name = data.get("name", "")
        if (
            not name.startswith("krim-d1-")
            or len(name) != 40
            or any(c not in "0123456789abcdef" for c in name[8:])
        ):
            raise ServiceError("sandbox_cleanup_required")
        leaf = root / name
        if (
            data["uid"] != os.getuid()
            or data["boot"] != Path("/proc/sys/kernel/random/boot_id").read_text().strip()
            or (root.stat().st_dev, root.stat().st_ino) != (data["root_dev"], data["root_inode"])
        ):
            raise ServiceError("sandbox_cleanup_required")
        before = after = None
        if leaf.exists():
            before = (leaf / "cgroup.events").read_text()
            if (leaf.stat().st_dev, leaf.stat().st_ino) != (data["dev"], data["inode"]):
                raise ServiceError("sandbox_cleanup_required")
            (leaf / "cgroup.kill").write_text("1")
            deadline = time.monotonic() + 10
            while "populated 1" in (leaf / "cgroup.events").read_text():
                if time.monotonic() > deadline:
                    raise ServiceError("sandbox_cleanup_failed")
                time.sleep(0.02)
            after = (leaf / "cgroup.events").read_text()
            leaf.rmdir()
        file.unlink()
        results.append({"leaf": name, "exit_code": None, "cleanup": "clean", "status": "unknown",
             "identity": data, "events_before": before, "events_after": after,
             "leaf_removed": not leaf.exists()})
    return results
