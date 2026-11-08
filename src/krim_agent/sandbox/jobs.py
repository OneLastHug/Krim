"""Broker的持久命令作业；原operation只核实，过期许可停止而不自动重放。"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.sandbox.checkpoints import ShadowCheckpoints
from krim_agent.sandbox.command import SandboxCommand
from krim_agent.sandbox.file_editing import ProjectFileEditor
from krim_agent.sandbox.file_models import (
    DirectoryCreate,
    FilePatch,
    FileRemove,
    FileRename,
    FileWrite,
)
from krim_agent.sandbox.port_adapters import AcceptedProjectMutation
from krim_agent.sandbox.ports import ProjectRef
from krim_agent.sandbox.quota import QuotaLimits
from krim_agent.sandbox.runner import RunscRunner
from krim_agent.sandbox.xfs import internal_id

if TYPE_CHECKING:
    from krim_agent.sandbox.broker import StorageBroker
    from krim_agent.sandbox.runtime_profiles import ResolvedRuntime


class _FileEditJournal:
    """复用Broker作业账本；不建立第二份业务数据库或重新领取操作。"""

    def __init__(self, jobs: CommandJobs, record: dict[str, Any]) -> None:
        self.jobs = jobs
        self.row = record

    def record(self, operation_id: str, status: str, detail: str = "") -> None:
        with self.jobs.lock:
            self.row["edit_status"] = {
                "plan_id": operation_id, "status": status, "detail": detail
            }
            self.jobs.broker._save(self.jobs.filename(self.row["operation_id"]), self.row)

    def status(self, operation_id: str) -> str | None:
        value = self.row.get("edit_status", {})
        if not isinstance(value, dict):
            raise ServiceError("facts_corrupt")
        if value.get("plan_id") != operation_id:
            return None
        status = value.get("status")
        if not isinstance(status, str) or status not in {
            "applied", "rejected", "unknown", "unapplied"
        }:
            raise ServiceError("facts_corrupt")
        return status


class CommandJobs:
    def __init__(
        self, broker: StorageBroker, image: str, runtime: ResolvedRuntime | None = None
    ) -> None:
        self.broker = broker
        installation = broker._read("execution-installation.json")
        if installation is None:
            import uuid

            installation = {"id": str(uuid.uuid4())}
            broker._save("execution-installation.json", installation)
        self.runner = (
            RunscRunner(image, installation["id"])
            if runtime is None
            else RunscRunner(image, installation["id"], runtime)
        )
        self.pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="krim-command")
        self.lock = threading.RLock()
        self.active: dict[str, dict[str, Any]] = {}
        self.prepare_locks: dict[str, threading.Lock] = {}
        self.closed = False

    @staticmethod
    def filename(operation_id: str) -> str:
        return "command-" + internal_id(operation_id) + ".json"

    def submit(
        self,
        environment_id: str,
        project_id: str,
        operation_id: str,
        command: dict[str, Any],
        *,
        kind: str = "command",
    ) -> dict[str, Any]:
        with self.lock:
            if self.closed:
                raise ServiceError("closed")
            models: dict[str, type[BaseModel]] = {
                "command": SandboxCommand,
                "write": FileWrite,
                "mkdir": DirectoryCreate,
                "remove": FileRemove,
                "patch": FilePatch,
                "rename": FileRename,
            }
            if kind not in models:
                raise ServiceError("invalid_input", 422)
            values: Any = models[kind].model_validate(command)
            if values.project_id != project_id:
                raise ServiceError("invalid_input", 422)
            env_state = self.broker._environment(environment_id)
            if env_state.get("quota_operation"):
                raise ServiceError("workspace_busy")
            project_state = self.broker._project(environment_id, project_id)
            if project_state["status"] != "ready":
                raise ServiceError("workspace_busy")
            digest = fingerprint(
                {
                    "environment": environment_id,
                    "command": command,
                    "image": self.runner.identity,
                    "runtime_fingerprint": getattr(self.runner, "runtime_fingerprint", None),
                    "kind": kind,
                }
            )
            previous = self.broker._read(self.filename(operation_id))
            if previous is not None:
                if previous["request_hash"] != digest:
                    raise ServiceError("key_conflict")
                return self.status(environment_id, project_id, operation_id)
            if len(self.active) >= 2:
                raise ServiceError("capacity", 429)
            project = self.broker._project(environment_id, project_id)
            if project.get("generation", 0) != values.expected_generation:
                raise ServiceError("input_revision_conflict")
            lease = self.broker.leases.claim(project_id, operation_id, deadline=time.time() + 120)
            cancel = threading.Event()
            record = {
                "operation_id": operation_id,
                "environment_id": environment_id,
                "project_id": project_id,
                "request_hash": digest,
                "kind": kind,
                "status": "accepted",
                "generation": values.expected_generation,
                "receipt": None,
            }
            try:
                self.broker._save(self.filename(operation_id), record)
            except BaseException:
                lease.release(clean=True)
                raise
            state = {
                "environment_id": environment_id,
                "cancel": cancel,
                "permit": time.time() + 5,
                "lease": lease,
                "record": record,
                "prepare_lock": self.prepare_locks.setdefault(environment_id, threading.Lock()),
            }
            self.active[operation_id] = state
            self.pool.submit(self._run, state, values)
            return record.copy()

    def _edit(
        self, state: dict[str, Any], values: FilePatch | FileRename, phase: Any
    ) -> dict[str, Any]:
        row = state["record"]
        # Broker只接收内部环境句柄；身份归属已在控制面审批并绑定此environment。
        reference = ProjectRef(
            row["environment_id"], row["environment_id"], row["project_id"], row["generation"]
        )
        editor = ProjectFileEditor(
            AcceptedProjectMutation(self.broker, reference, state["lease"]),
            _FileEditJournal(self, row),
        )
        if isinstance(values, FilePatch):
            plan = editor.plan_patch(
                reference, expected_generation=values.expected_generation,
                path=values.path, expected_sha256=values.expected_sha256,
                offset=values.offset, delete_length=values.delete_length,
                replacement=values.replacement,
            )
            phase("edit_planned", {"edit_plan": plan.model_dump(mode="json")})
            return editor.apply_patch(plan, port_operation_id=row["operation_id"])
        rename = editor.plan_rename(
            reference, expected_generation=values.expected_generation,
            source=values.source, destination=values.destination,
            expected_source_sha256=values.expected_source_sha256,
        )
        phase("edit_planned", {"edit_plan": rename.model_dump(mode="json")})
        return editor.apply_rename(rename, port_operation_id=row["operation_id"])

    def _run(self, state: dict[str, Any], values: Any) -> None:
        record = state["record"]
        operation_id, project_id = record["operation_id"], record["project_id"]
        environment_id = record["environment_id"]
        finished = threading.Event()

        def watchdog() -> None:
            while not finished.wait(0.2):
                if time.time() > state["permit"]:
                    state["cancel"].set()
                    return

        watcher = threading.Thread(target=watchdog, daemon=True)
        watcher.start()

        def phase(name: str, result: dict[str, Any]) -> None:
            with self.lock:
                state["lease"].check()
                record["status"] = name
                record.update(result)
                self.broker._save(self.filename(operation_id), record)
                if state["cancel"].is_set() and not attempted:
                    raise ServiceError("cancelled")

        attempted = False
        checkpoint = None
        checkpoints = None
        try:
            with state["prepare_lock"]:
                environment = self.broker._environment(environment_id)
                limits = QuotaLimits.model_validate(environment["limits"])
                if environment.get("checkpoint_number") is None:
                    partition = self.broker.quota.checkpoint_partition(environment_id, limits)
                    environment.update(partition)
                    self.broker._save("env-" + environment_id + ".json", environment)
                phase("checkpoint", {})
                captured = self.broker.files.snapshot(environment_id, project_id)
                checkpoints = ShadowCheckpoints(
                    self.broker.environments / environment_id / "checkpoints"
                )
                checkpoint = checkpoints.capture(
                    environment_id, project_id, captured["files"], captured["excluded"], limits
                )
                record["checkpoint_id"] = checkpoint["checkpoint_id"]
                checkpoints.reference(
                    environment_id, project_id, checkpoint["checkpoint_id"], "job", operation_id
                )
                phase("prepared", {"checkpoint_id": checkpoint["checkpoint_id"]})
            # 项目入口父目录由Broker拥有，租户只能写content内；不跟随替换的入口。
            with self.broker.files._content(environment_id, project_id):
                content = (
                    self.broker.environments / environment_id / "projects" / project_id / "content"
                )
                if state["cancel"].is_set():
                    raise ServiceError("cancelled")
                attempted = True
                if isinstance(values, SandboxCommand):
                    receipt = self.runner.run(
                        operation_id,
                        content,
                        values.argv,
                        values.wall_seconds,
                        state["cancel"],
                        phase,
                    )
                else:
                    phase("file_entering", {})
                    try:
                        if isinstance(values, (FilePatch, FileRename)):
                            changed = self._edit(state, values, phase)
                        elif isinstance(values, FileWrite):
                            changed = self.broker.files.write(
                                environment_id,
                                project_id,
                                values.path,
                                values.content.encode(),
                                values.expected_sha256,
                            )
                        elif isinstance(values, FileRemove):
                            changed = self.broker.files.remove(
                                environment_id, project_id, values.path, values.expected_sha256
                            )
                        else:
                            changed = self.broker.files.mkdir(
                                environment_id, project_id, values.path
                            )
                        receipt = {
                            "executor_started": True,
                            "exit_code": None,
                            "stdout": "",
                            "stderr": "",
                            "cleanup": "clean",
                            "output_complete": True,
                            "code": None,
                            "file": changed,
                        }
                        phase("started", {"executor_started": True})
                    except ServiceError as error:
                        if error.code not in {
                            "artifact_conflict",
                            "invalid_input",
                            "artifact_invalid",
                            "artifact_limit",
                            "permission_denied",
                            "not_found",
                            "source_changed",
                            "key_conflict",
                            "revision_conflict",
                            "sandbox_limit",
                        }:
                            raise
                        outcome = record.get("edit_status", {}).get("status")
                        if isinstance(values, (FilePatch, FileRename)) and outcome == "unknown":
                            raise ServiceError("sandbox_operation_unknown") from error
                        receipt = {
                            "executor_started": outcome == "applied",
                            "exit_code": None,
                            "stdout": "",
                            "stderr": "",
                            "cleanup": "clean",
                            "output_complete": True,
                            "code": error.code,
                        }

            with self.lock:
                receipt["checkpoint_id"] = record.get("checkpoint_id")
                record["receipt"] = receipt
                record["status"] = "finished" if receipt["cleanup"] == "clean" else "unknown"
                # 只要进入创建阶段就保守失效旧generation；失败/取消不回滚目录。
                generation = record["generation"] + 1
                project = self.broker._project(environment_id, project_id)
                project["generation"] = generation
                project["status"] = "ready" if receipt["cleanup"] == "clean" else "blocked"
                self.broker._save("project-" + project_id + ".json", project)
                record["generation"] = generation
                self.broker._save(self.filename(operation_id), record)
                state["lease"].release(clean=receipt["cleanup"] == "clean")
                if receipt["cleanup"] == "clean" and record.get("kind", "command") == "command":
                    try:
                        self.runner.remove(operation_id)
                    except ServiceError:
                        pass  # 回执已保存，清理失败保留已停止容器供核实。
        except BaseException as error:
            with self.lock:
                if not attempted:
                    record["receipt"] = {
                        "executor_started": False,
                        "exit_code": None,
                        "stdout": "",
                        "stderr": "",
                        "cleanup": "clean",
                        "output_complete": True,
                        "code": error.code if isinstance(error, ServiceError) else "sandbox_setup",
                    }
                record["status"] = "unknown" if attempted else "finished"
                try:
                    self.broker._save(self.filename(operation_id), record)
                finally:
                    state["lease"].release(clean=not attempted)
        finally:
            if (
                checkpoint is not None
                and checkpoints is not None
                and record.get("status") == "finished"
            ):
                try:
                    checkpoints.release_reference(
                        environment_id, project_id, checkpoint["checkpoint_id"], "job", operation_id
                    )
                except ServiceError:
                    pass
            finished.set()
            watcher.join(timeout=1)
            with self.lock:
                self.active.pop(operation_id, None)

    def status(
        self,
        environment_id: str,
        project_id: str,
        operation_id: str,
        *,
        renew: bool = False,
        cancel: bool = False,
    ) -> dict[str, Any]:
        with self.lock:
            self.broker._project(environment_id, project_id)
            row = self.broker._read(self.filename(operation_id))
            if (
                row is None
                or row["environment_id"] != environment_id
                or row["project_id"] != project_id
            ):
                raise ServiceError("not_found", 404)
            active = self.active.get(operation_id)
            if active is not None:
                if renew:
                    active["permit"] = time.time() + 5
                if cancel:
                    active["cancel"].set()
            elif row["status"] not in {"finished", "unknown"}:
                # Broker重启不能把旧命令当成没执行：只核实并停止原容器。
                try:
                    if row.get("kind", "command") == "command":
                        stopped = self.runner.stop(operation_id)
                    else:
                        stopped = {"State": {"ExitCode": None}}
                    row["receipt"] = {
                        "executor_started": None,
                        "exit_code": stopped["State"]["ExitCode"],
                        "cleanup": "clean",
                        "output_complete": False,
                        "stdout": "",
                        "stderr": "",
                        "code": "execution_unknown",
                    }
                except ServiceError:
                    pass
                row["status"] = "unknown"
                self.broker._save(self.filename(operation_id), row)
            return row

    def close(self) -> None:
        with self.lock:
            self.closed = True
            for state in self.active.values():
                state["cancel"].set()
        self.pool.shutdown(wait=True, cancel_futures=False)

    def reconcile(self, environment_id: str, project_id: str, operation_id: str) -> dict[str, Any]:
        """显式恢复只核实并停止原实例，绝不重放用户命令或伪造原输出。"""
        with self.lock:
            if operation_id in self.active:
                self.active[operation_id]["cancel"].set()
                raise ServiceError("workspace_busy")
            self.broker._project(environment_id, project_id)
            row = self.broker._read(self.filename(operation_id))
            if (
                row is None
                or row["environment_id"] != environment_id
                or row["project_id"] != project_id
            ):
                raise ServiceError("not_found", 404)
            if row["status"] == "finished" and row.get("reconciled"):
                return row
            if row["status"] == "finished" and row.get("receipt", {}).get("cleanup") == "clean":
                stopped = {"State": {"ExitCode": row["receipt"]["exit_code"]}}
            elif row.get("kind", "command") == "command":
                stopped = self.runner.stop(operation_id)
            else:
                stopped = {"State": {"ExitCode": None}}
            lease = self.broker.leases.status(project_id)
            if lease["status"] != "idle":
                self.broker.leases.reconcile(project_id, operation_id, stopped=True)
            project = self.broker._project(environment_id, project_id)
            project["generation"] = max(project.get("generation", 0), row["generation"] + 1)
            project["status"] = "ready"
            self.broker._save("project-" + project_id + ".json", project)
            receipt = row.get("receipt") or {}
            row["receipt"] = {
                **receipt,
                "executor_started": receipt.get("executor_started"),
                "exit_code": stopped["State"]["ExitCode"],
                "cleanup": "clean",
                "output_complete": False,
                "code": "execution_unknown",
                "stdout": receipt.get("stdout", ""),
                "stderr": receipt.get("stderr", ""),
            }
            if row.get("checkpoint_id"):
                store = ShadowCheckpoints(self.broker.environments / environment_id / "checkpoints")
                store.release_reference(
                    environment_id, project_id, row["checkpoint_id"], "job", operation_id
                )
            row["status"] = "finished"
            row["reconciled"] = True
            row["generation"] = project["generation"]
            self.broker._save(self.filename(operation_id), row)
            if row.get("kind", "command") == "command":
                try:
                    self.runner.remove(operation_id)
                except ServiceError:
                    pass
            return row
