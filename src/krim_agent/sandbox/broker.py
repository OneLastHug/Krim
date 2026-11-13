"""本机存储Broker：Unix对端认证、XFS配额、内部ID目录与持久写租约。"""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import os
import signal
import socket
import stat
import struct
import uuid
from pathlib import Path
from typing import Any

from krim_agent.contracts import ServiceError
from krim_agent.runtime.contracts import settled
from krim_agent.sandbox.broker_client import MAX_MESSAGE
from krim_agent.sandbox.filesystem import ProjectFiles
from krim_agent.sandbox.jobs import CommandJobs
from krim_agent.sandbox.leases import ProjectLeases
from krim_agent.sandbox.quota import QuotaLimits
from krim_agent.sandbox.quota_management import QuotaChanges
from krim_agent.sandbox.quota_retirement import QuotaRetirement
from krim_agent.sandbox.quota_retirement_adapter import BrokerRetirementPort
from krim_agent.sandbox.recovery import Recovery
from krim_agent.sandbox.runtime_profiles import ResolvedRuntime, RuntimeCatalog
from krim_agent.sandbox.xfs import XfsAccounts, internal_id, private_directory


class StorageBroker:
    """不接受Docker API、任意宿主路径或命令；只有可信控制面可访问。"""

    def __init__(
        self,
        storage: Path,
        state: Path,
        *,
        image: str | None = None,
        runtime: ResolvedRuntime | None = None,
        catalog: RuntimeCatalog | None = None,
        allow_dev: bool = False,
    ) -> None:
        if (runtime is not None or catalog is not None) and image is None:
            raise ServiceError("sandbox_config_invalid", 422)
        self.storage = private_directory(storage)
        self.state = private_directory(state)
        if state.stat().st_uid != os.geteuid() or state.stat().st_mode & 0o077:
            raise ServiceError("sandbox_config_invalid", 422)
        self.fd = os.open(state, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        self.lock = os.open(
            "owner.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600, dir_fd=self.fd
        )
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(self.lock)
            os.close(self.fd)
            raise ServiceError("sandbox_cleanup_required") from None
        self.storage_lock = os.open(
            storage / ".krim-broker.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600
        )
        try:
            info = os.fstat(self.storage_lock)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ServiceError("facts_corrupt")
            fcntl.flock(self.storage_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            os.close(self.storage_lock)
            os.close(self.lock)
            os.close(self.fd)
            raise ServiceError("sandbox_cleanup_required") from None
        self.environments = storage / "environments"
        self.lease_root = state / "leases"
        self.environments.mkdir(mode=0o700, exist_ok=True)
        self.lease_root.mkdir(mode=0o700, exist_ok=True)
        try:
            self.quota = XfsAccounts(storage, self.environments)
            self.files = ProjectFiles(self.environments)
            self.leases = ProjectLeases(self.lease_root)
        except BaseException:
            if hasattr(self, "files"):
                self.files.close()
            os.close(self.storage_lock)
            os.close(self.lock)
            os.close(self.fd)
            raise
        self.active: dict[str, Any] = {}
        self.jobs = (
            CommandJobs(self, image, runtime, catalog=catalog, allow_dev=allow_dev)
            if image is not None
            else None
        )
        self.recovery = Recovery(self)
        self.quota_changes = QuotaChanges(self)
        self.quota_retirements = QuotaRetirement(BrokerRetirementPort(self))
        self.closed = False

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            if self.jobs is not None:
                self.jobs.close()
            self.leases.close()
        finally:
            self.files.close()
            os.close(self.storage_lock)
            os.close(self.lock)
            os.close(self.fd)

    def _read(self, name: str) -> dict[str, Any] | None:
        try:
            fd = os.open(name, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW, dir_fd=self.fd)
        except FileNotFoundError:
            return None
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 65536:
                raise ServiceError("facts_corrupt")
            raw = os.read(fd, 65537)
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise ValueError("记录无效")
            return value
        except (ValueError, UnicodeError):
            raise ServiceError("facts_corrupt") from None
        finally:
            os.close(fd)

    def _save(self, name: str, value: dict[str, Any]) -> None:
        raw = json.dumps(value, ensure_ascii=False, allow_nan=False).encode()
        if len(raw) > 65536:
            raise ServiceError("sandbox_limit")
        total = 0
        count = 0
        exists = False
        with os.scandir(self.fd) as entries:
            for entry in entries:
                info = entry.stat(follow_symlinks=False)
                if stat.S_ISREG(info.st_mode):
                    total += info.st_size
                    count += 1
                if entry.name == name:
                    exists = True
                    total -= info.st_size
        # 保留4MiB给已有动作的停止/结算更新，不能任意创建无限预览日志。
        if (
            not exists
            and (count >= 4096 or total + len(raw) > 60 * 1024**2)
            or total + len(raw) > 64 * 1024**2
        ):
            raise ServiceError("sandbox_limit")
        temporary = ".tmp-" + uuid.uuid4().hex
        fd = os.open(
            temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=self.fd
        )
        try:
            with os.fdopen(os.dup(fd), "wb") as stream:
                stream.write(raw)
                stream.flush()
            os.fsync(fd)
            os.replace(temporary, name, src_dir_fd=self.fd, dst_dir_fd=self.fd)
            os.fsync(self.fd)
        finally:
            os.close(fd)
            try:
                os.unlink(temporary, dir_fd=self.fd)
            except FileNotFoundError:
                pass

    def _environment(self, environment_id: str) -> dict[str, Any]:
        internal_id(environment_id)
        value = self._read("env-" + environment_id + ".json")
        if value is None or value.get("status") != "ready":
            raise ServiceError("sandbox_backend_unavailable", 503)
        return value

    def _project(self, environment_id: str, project_id: str) -> dict[str, Any]:
        self._environment(environment_id)
        internal_id(project_id)
        value = self._read("project-" + project_id + ".json")
        if value is None or value.get("environment_id") != environment_id:
            raise ServiceError("not_found", 404)
        return value

    def provision_environment(self, environment_id: str, limits: dict[str, Any]) -> dict[str, Any]:
        internal_id(environment_id)
        quota = QuotaLimits.model_validate(limits)
        registry = self._read("accounts.json") or {"allocations": {}}
        allocations = registry["allocations"]
        number = self.quota.project_number(environment_id)
        if any(
            v["project_number"] == number and key != environment_id
            for key, v in allocations.items()
        ):
            raise ServiceError("sandbox_limit")
        if environment_id in allocations and allocations[environment_id]["limits"] != limits:
            # 动态调额需要独立的prepare/apply事务；不能在provision重试中偷改。
            raise ServiceError("revision_conflict")
        usage = os.statvfs(self.storage)
        capacity = usage.f_blocks * usage.f_frsize
        reserved = sum(
            v["limits"]["environment_bytes"]
            for key, v in allocations.items()
            if key != environment_id
        )
        if (environment_id not in allocations and len(allocations) >= 128) or (
            reserved + quota.environment_bytes > capacity * 0.85
        ):
            raise ServiceError("sandbox_limit")
        allocations[environment_id] = {"project_number": number, "limits": limits}
        self._save("accounts.json", registry)
        previous = self._read("env-" + environment_id + ".json")
        if previous is not None and previous.get("status") == "ready":
            actual = self.quota.account_usage(previous)
            if (
                actual["byte_limit"] != quota.environment_bytes // 1024 * 1024
                or actual["inode_limit"] != quota.environment_inodes
            ):
                raise ServiceError("sandbox_backend_unavailable", 503)
            return {"environment_id": environment_id, **previous, "usage": actual}
        result = self.quota.provision(environment_id, quota)
        self._save("env-" + environment_id + ".json", {**result, "status": "ready"})
        return {"environment_id": environment_id, **result, "status": "ready"}

    def provision_project(self, environment_id: str, project_id: str) -> dict[str, Any]:
        self._environment(environment_id)
        internal_id(project_id)
        if self._environment(environment_id).get("quota_operation"):
            raise ServiceError("workspace_busy")
        prior = self._read("project-" + project_id + ".json")
        if prior is not None and prior.get("environment_id") != environment_id:
            raise ServiceError("not_found", 404)
        self.files.create(environment_id, project_id)
        content = self.environments / environment_id / "projects" / project_id / "content"
        # 此入口及其父目录不交给容器；只有content在容器内可写。
        os.chown(content, 65532, 65532, follow_symlinks=False)
        record = (
            dict(prior)
            if prior is not None
            else {
                "environment_id": environment_id,
                "project_id": project_id,
                "status": "ready",
                "generation": 0,
            }
        )
        self._save("project-" + project_id + ".json", record)
        return record

    def dispatch(self, action: str, values: dict[str, Any]) -> dict[str, Any]:
        if self.closed:
            raise ServiceError("closed")
        if action.startswith(("restore_", "checkpoint_", "quota_")):
            from krim_agent.sandbox.maintenance import dispatch_maintenance

            return dispatch_maintenance(self, action, values)
        if action == "health" and not values:
            self.quota.current()
            return {
                "storage_available": True,
                "execution_available": bool(self.jobs and self.jobs.available_programs()),
                "image": self.jobs.image if self.jobs else None,
                "image_id": self.jobs.runner.identity if self.jobs and self.jobs.runner else None,
                "configured_programs": self.jobs.configured_programs() if self.jobs else [],
                "executable_programs": self.jobs.available_programs() if self.jobs else [],
                "available_programs": self.jobs.available_programs() if self.jobs else [],
                "runtimes": self.jobs.runtime_info() if self.jobs else {},
                "enforcement": "xfs_project_quota",
            }
        if action in {"list", "find"} and "offset" not in values:
            values = {**values, "offset": 0}
        if action == "find" and "limit" not in values:
            values = {**values, "limit": 100}
        if action == "read":
            values = {
                "expected_sha256": None,
                "expected_stat": None,
                "offset": 0,
                "limit": 65536,
                **values,
            }
        if action == "search":
            values = {
                "offset": 0,
                "include_glob": None,
                "mode": "literal",
                "case_sensitive": True,
                "before": 0,
                "after": 0,
                **values,
            }
        fields = {
            "provision_environment": {"environment_id", "limits"},
            "provision_project": {"environment_id", "project_id"},
            "usage": {"environment_id"},
            "list": {"environment_id", "project_id", "path", "limit", "offset"},
            "find": {"environment_id", "project_id", "path", "include_glob", "limit", "offset"},
            "search": {
                "environment_id", "project_id", "path", "query", "limit", "offset",
                "include_glob", "mode", "case_sensitive", "before", "after",
            },
            "read": {
                "environment_id", "project_id", "path", "offset", "limit",
                "expected_sha256", "expected_stat",
            },
            "claim": {"environment_id", "project_id", "lease_id", "deadline"},
            "release": {"environment_id", "project_id", "lease_id", "clean"},
            "file_start": {"environment_id", "project_id", "operation_id", "mutation", "kind"},
            "command_start": {
                "environment_id",
                "project_id",
                "operation_id",
                "command",
                "runtime_image",
                "runtime_fingerprint",
            },
            "command_reconcile": {"environment_id", "project_id", "operation_id"},
            "command_status": {"environment_id", "project_id", "operation_id", "renew", "cancel"},
        }
        if action not in fields or set(values) != fields[action]:
            raise ServiceError("invalid_input", 422)
        environment_id = internal_id(values["environment_id"])
        if action == "provision_environment":
            return self.provision_environment(environment_id, values["limits"])
        if action == "usage":
            record = self._environment(environment_id)
            return {
                "usage": self.quota.account_usage(record),
                "enforcement": "xfs_project_quota",
            }
        project_id = internal_id(values["project_id"])
        if action == "provision_project":
            return self.provision_project(environment_id, project_id)
        self._project(environment_id, project_id)
        if action == "list":
            return self.files.list(
                environment_id, project_id, values["path"], values["limit"], values["offset"]
            )
        if action == "find":
            return self.files.find(
                environment_id,
                project_id,
                path=values["path"],
                include_glob=values["include_glob"],
                limit=values["limit"],
                offset=values["offset"],
            )
        if action == "search":
            return self.files.search(
                environment_id,
                project_id,
                values["query"],
                values["path"],
                values["limit"],
                values["offset"],
                values["include_glob"],
                values["mode"],
                values["case_sensitive"],
                values["before"],
                values["after"],
            )
        if action == "read":
            return self.files.read(
                environment_id,
                project_id,
                values["path"],
                values["offset"],
                values["limit"],
                expected_sha256=values["expected_sha256"],
                expected_stat=values["expected_stat"],
            )
        if action.startswith("command_") or action == "file_start":
            if self.jobs is None:
                raise ServiceError("sandbox_backend_unavailable", 503)
            if action == "file_start":
                return self.jobs.submit(
                    environment_id,
                    project_id,
                    values["operation_id"],
                    values["mutation"],
                    kind=values["kind"],
                )
            if action == "command_start":
                operation_id = internal_id(values["operation_id"])
                command_record = self._read(self.jobs.filename(operation_id))
                command = values["command"]
                if set(values) != {
                    "environment_id", "project_id", "operation_id", "command",
                    "runtime_image", "runtime_fingerprint",
                }:
                    raise ServiceError("invalid_input", 422)
                if (
                    command_record is None
                    or command_record.get("environment_id") != environment_id
                    or command_record.get("project_id") != project_id
                    or command_record.get("status") not in {"accepted", "intent"}
                    or command_record.get("command") != command
                    or command_record.get("image") != values["runtime_image"]
                    or command_record.get("runtime_fingerprint") != values["runtime_fingerprint"]
                ):
                    raise ServiceError("target_binding_changed")
                return self.jobs.submit(
                    environment_id, project_id, operation_id, command
                )
            if action == "command_reconcile":
                return self.jobs.reconcile(environment_id, project_id, values["operation_id"])
            if type(values["renew"]) is not bool or type(values["cancel"]) is not bool:
                raise ServiceError("invalid_input", 422)
            return self.jobs.status(
                environment_id,
                project_id,
                values["operation_id"],
                renew=values["renew"],
                cancel=values["cancel"],
            )
        lease_id = internal_id(values["lease_id"])
        if action == "claim":
            lease = self.leases.claim(project_id, lease_id, deadline=values["deadline"])
            self.active[project_id] = lease
            return {"project_id": project_id, "lease_id": lease_id, "status": "active"}
        owned_lease = self.active.get(project_id)
        if owned_lease is None or owned_lease.lease_id != lease_id or values["clean"] is not True:
            raise ServiceError("sandbox_cleanup_required")
        owned_lease.release(clean=True)
        del self.active[project_id]
        return {"project_id": project_id, "lease_id": lease_id, "status": "idle"}


async def serve(
    socket_path: Path,
    storage: Path,
    state: Path,
    uid: int,
    gid: int,
    image: str | None = None,
    runtime_manifest: Path | None = None,
    allow_dev: bool = False,
) -> None:
    private_directory(socket_path.parent)
    try:
        os.lstat(socket_path)
    except FileNotFoundError:
        occupied = False
    else:
        occupied = True
    if occupied:
        # 不删未知socket；遗留路径由运维显式核实再清理。
        raise ServiceError("sandbox_cleanup_required")
    if allow_dev and runtime_manifest is None:
        raise ServiceError("sandbox_config_invalid", 422)
    if runtime_manifest is not None and image is None:
        raise ServiceError("sandbox_config_invalid", 422)
    catalog = RuntimeCatalog.load(runtime_manifest) if runtime_manifest is not None else None
    if catalog is not None:
        python_entry = catalog.entry("python", "normal_job")
        if python_entry.availability == "dev_verified" and python_entry.image != image:
            raise ServiceError("sandbox_config_invalid", 422)
    broker = StorageBroker(storage, state, image=image, catalog=catalog, allow_dev=allow_dev)
    gate = asyncio.Semaphore(8)
    dispatch_lock = asyncio.Lock()
    checkpoint_gate = asyncio.Semaphore(2)
    handlers: set[asyncio.Task[Any]] = set()
    sockets = 0

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal sockets
        sockets += 1
        task = asyncio.current_task()
        if task is not None:
            handlers.add(task)
        try:
            if sockets > 16:
                return
            peer = writer.get_extra_info("socket")
            _, peer_uid, _ = struct.unpack(
                "3i", peer.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
            )
            if peer_uid != uid:
                return
            async with asyncio.timeout(15), gate:
                raw = await reader.readline()
                if not raw.endswith(b"\n") or len(raw) > MAX_MESSAGE:
                    raise ServiceError("invalid_input", 422)
                value = json.loads(raw)
                if (
                    not isinstance(value, dict)
                    or set(value) != {"action", "arguments"}
                    or type(value["action"]) is not str
                    or not isinstance(value["arguments"], dict)
                ):
                    raise ServiceError("invalid_input", 422)
                try:
                    if value["action"].startswith("checkpoint_"):
                        # Git维护只锁所属账号对象库，不能让一次repack堵住所有账号RPC。
                        async with checkpoint_gate:
                            result = await settled(
                                asyncio.to_thread(
                                    broker.dispatch, value["action"], value["arguments"]
                                )
                            )
                    else:
                        async with dispatch_lock:
                            result = await settled(
                                asyncio.to_thread(
                                    broker.dispatch, value["action"], value["arguments"]
                                )
                            )
                    response: dict[str, Any] = {"result": result}
                except ServiceError as error:
                    response = {"error": error.code, "status": error.status}
                except (OSError, ValueError, TypeError, KeyError):
                    response = {"error": "sandbox_backend_unavailable", "status": 503}
                payload = json.dumps(response, ensure_ascii=False, allow_nan=False).encode() + b"\n"
                if len(payload) > MAX_MESSAGE:
                    raise ServiceError("sandbox_limit")
                writer.write(payload)
                await writer.drain()
        except (OSError, ValueError, TimeoutError, ServiceError):
            pass
        finally:
            sockets -= 1
            if task is not None:
                handlers.discard(task)
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass

    previous = os.umask(0o117)
    try:
        server = await asyncio.start_unix_server(handle, path=socket_path, limit=MAX_MESSAGE + 1)
    finally:
        os.umask(previous)
    os.chown(socket_path, 0, gid, follow_symlinks=False)
    os.chmod(socket_path, 0o660, follow_symlinks=False)
    info = os.lstat(socket_path)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    async def maintain() -> None:
        from krim_agent.sandbox.maintenance import scheduled_cleanup

        while not stop.is_set():
            try:
                async with checkpoint_gate:
                    await settled(asyncio.to_thread(scheduled_cleanup, broker))
            except (OSError, ServiceError, ValueError):
                pass  # 回收失败不放宽配额，下一维护周期可重试原状态。
            try:
                await asyncio.wait_for(stop.wait(), timeout=15)
            except TimeoutError:
                pass

    maintenance_task = asyncio.create_task(maintain())
    try:
        async with server:
            await stop.wait()
    finally:
        stop.set()
        await maintenance_task
        if handlers:
            await settled(asyncio.gather(*tuple(handlers), return_exceptions=True))
        async with dispatch_lock:
            await settled(asyncio.to_thread(broker.close))
        try:
            current = os.lstat(socket_path)
            if (current.st_dev, current.st_ino) == (info.st_dev, info.st_ino):
                os.unlink(socket_path)
        except FileNotFoundError:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", type=Path, required=True)
    parser.add_argument("--storage", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--allow-uid", type=int, required=True)
    parser.add_argument("--socket-gid", type=int, required=True)
    parser.add_argument("--image", default=None)
    parser.add_argument("--runtime-manifest", type=Path, default=None)
    parser.add_argument("--allow-dev-runtimes", action="store_true")
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error("存储Broker需要宿主配额管理权限")
    asyncio.run(
        serve(
            args.socket,
            args.storage,
            args.state,
            args.allow_uid,
            args.socket_gid,
            args.image,
            args.runtime_manifest,
            args.allow_dev_runtimes,
        )
    )


if __name__ == "__main__":
    main()
