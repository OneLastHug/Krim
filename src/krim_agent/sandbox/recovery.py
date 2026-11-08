"""多文件恢复账本；只执行已确认摘要计划，中断后按原/目标状态核实。"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.sandbox.checkpoints import ShadowCheckpoints
from krim_agent.sandbox.quota import QuotaLimits
from krim_agent.sandbox.xfs import internal_id

if TYPE_CHECKING:
    from krim_agent.sandbox.broker import StorageBroker


def sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


class Recovery:
    """每次Broker调用只应用一文件；控制面批次逐步复核授权后继续。"""

    def __init__(self, broker: StorageBroker) -> None:
        self.broker = broker
        self.active: set[str] = set()
        self.point: Callable[[str, dict[str, Any]], None] = lambda phase, row: None

    @staticmethod
    def name(operation: str) -> str:
        return "restore-" + internal_id(operation) + ".json"

    def store(self, environment: str) -> ShadowCheckpoints:
        self.broker._environment(environment)
        path = self.broker.environments / environment / "checkpoints"
        return ShadowCheckpoints(path)

    def _record(self, environment: str, project: str, operation: str) -> dict[str, Any]:
        self.broker._project(environment, project)
        row = self.broker._read(self.name(operation))
        if row is None or row["environment_id"] != environment or row["project_id"] != project:
            raise ServiceError("not_found", 404)
        return row

    def _idle(self, environment: str, project: str) -> None:
        record = self.broker._environment(environment)
        if record.get("quota_operation"):
            raise ServiceError("workspace_busy")
        if self.broker.leases.status(project)["status"] != "idle":
            raise ServiceError("workspace_busy")

    def _actual(self, environment: str, project: str, path: str) -> str | None:
        try:
            return cast(str, self.broker.files.read(environment, project, path, limit=1)["sha256"])
        except ServiceError as error:
            if error.code == "not_found":
                return None
            raise

    def preview(self, environment: str, project: str, checkpoint: str) -> dict[str, Any]:
        self._idle(environment, project)
        self.broker._project(environment, project)
        lease = self.broker.leases.claim(project, str(uuid.uuid4()), deadline=time.time() + 60)
        try:
            captured = self.broker.files.snapshot(environment, project)
            store = self.store(environment)
            target = next(
                (r for r in store.list(environment, project) if r["checkpoint_id"] == checkpoint),
                None,
            )
            if target is None:
                raise ServiceError("not_found", 404)
            current = captured["files"]
            changes = []
            target_paths = {r["path"] for r in target["files"]}
            for entry in target["files"]:
                path = entry["path"]
                actual = sha(current[path]) if path in current else None
                if actual != entry["sha256"]:
                    # 现存但未覆盖的大文件/链接不能被当成缺失然后覆盖。
                    if self._actual(environment, project, path) != actual:
                        raise ServiceError("artifact_conflict")
                    changes.append(
                        {
                            "step_id": str(uuid.uuid4()),
                            "path": path,
                            "action": "write",
                            "expected_sha256": actual,
                            "target_sha256": entry["sha256"],
                            "size": entry["size"],
                            "selected_by_default": True,
                        }
                    )
            preserved = []
            excluded_targets = {r["path"] for r in target["excluded"]}
            for path, body in current.items():
                if path not in target_paths:
                    preserved.append(path)
                    if path not in excluded_targets and not any(
                        path.startswith(p + "/") for p in excluded_targets
                    ):
                        changes.append(
                            {
                                "step_id": str(uuid.uuid4()),
                                "path": path,
                                "action": "delete",
                                "expected_sha256": sha(body),
                                "target_sha256": None,
                                "size": 0,
                                "selected_by_default": False,
                            }
                        )
            if len(changes) > 256:
                raise ServiceError("artifact_limit")
            plan = {
                "plan_id": str(uuid.uuid4()),
                "environment_id": environment,
                "project_id": project,
                "checkpoint_id": checkpoint,
                "commit": target["commit"],
                "generation": self.broker._project(environment, project).get("generation", 0),
                "created_at": time.time(),
                "expires_at": time.time() + 900,
                "changes": changes,
                "preserved": preserved[:256],
                "excluded": captured["excluded"][:128],
                "target_excluded": target["excluded"][:128],
            }
            plan["plan_digest"] = fingerprint(plan)
            self.broker._save("restore-plan-" + plan["plan_id"] + ".json", plan)
            return plan
        finally:
            lease.release(clean=True)

    def confirm(
        self,
        environment: str,
        project: str,
        plan_id: str,
        digest: str,
        operation: str,
        generation: int,
        selected: list[str],
    ) -> dict[str, Any]:
        internal_id(plan_id)
        internal_id(operation)
        if (
            type(selected) is not list
            or not selected
            or any(type(p) is not str for p in selected)
            or len(set(selected)) != len(selected)
        ):
            raise ServiceError("invalid_input", 422)
        previous = self.broker._read(self.name(operation))
        key = fingerprint(
            {"plan": plan_id, "digest": digest, "selected": selected, "generation": generation}
        )
        if previous is not None:
            if previous.get("request_hash") != key:
                raise ServiceError("key_conflict")
            return self.status(environment, project, operation)
        self._idle(environment, project)
        plan = self.broker._read("restore-plan-" + plan_id + ".json")
        if (
            plan is None
            or plan["project_id"] != project
            or plan["environment_id"] != environment
            or plan["plan_digest"] != digest
        ):
            raise ServiceError("not_found", 404)
        if plan["expires_at"] < time.time():
            raise ServiceError("revision_conflict")
        if type(generation) is not int or plan["generation"] != generation:
            raise ServiceError("input_revision_conflict")
        entries = {r["step_id"]: r for r in plan["changes"]}
        if not set(selected) <= entries.keys():
            raise ServiceError("invalid_input", 422)
        row = {
            "operation_id": operation,
            "environment_id": environment,
            "project_id": project,
            "request_hash": key,
            "plan_id": plan_id,
            "checkpoint_id": plan["checkpoint_id"],
            "commit": plan["commit"],
            "generation": generation,
            "status": "preparing",
            "steps": [{**entries[i], "status": "prepared"} for i in selected],
            "before_checkpoint": None,
            "reason": None,
            "next_step": 0,
        }
        # 为每步临时inode、状态及结算字段预留日志空间；不能恢复中途才因64KiB上限失去账本。
        if (
            len(json.dumps(row, ensure_ascii=False).encode()) + len(row["steps"]) * 256 + 2048
            > 65536
        ):
            raise ServiceError("artifact_limit")
        self.broker._save(self.name(operation), row)
        return self._apply(row, preparing=True)

    def _save(self, row: dict[str, Any]) -> None:
        self.broker._save(self.name(row["operation_id"]), row)

    def _project_state(self, row: dict[str, Any], status: str) -> None:
        project = self.broker._project(row["environment_id"], row["project_id"])
        project["generation"] = max(project.get("generation", 0), row["generation"])
        project["status"] = status
        project["restore_operation"] = row["operation_id"] if status == "blocked" else None
        self.broker._save("project-" + row["project_id"] + ".json", project)

    def _release_roots(self, row: dict[str, Any]) -> None:
        store = self.store(row["environment_id"])
        store.release_reference(
            row["environment_id"],
            row["project_id"],
            row["checkpoint_id"],
            "restore-target",
            row["operation_id"],
        )
        if row["before_checkpoint"] and row["status"] in {"completed", "abandoned_partial"}:
            row["before_protected_until"] = time.time() + 86400
        elif row["before_checkpoint"]:
            store.release_reference(
                row["environment_id"],
                row["project_id"],
                row["before_checkpoint"],
                "restore-before",
                row["operation_id"],
            )
        self._save(row)

    def _apply(self, row: dict[str, Any], *, preparing: bool) -> dict[str, Any]:
        environment, project, operation = (
            row["environment_id"],
            row["project_id"],
            row["operation_id"],
        )
        if self.broker._environment(environment).get("quota_operation"):
            raise ServiceError("workspace_busy")
        lease = self.broker.leases.claim(project, operation, deadline=time.time() + 120)
        self.active.add(operation)
        mutated = not preparing
        try:
            target = self.store(environment)
            if preparing:
                current = self.broker._project(environment, project)
                if current.get("generation", 0) != row["generation"]:
                    raise ServiceError("input_revision_conflict")
                selected = next(
                    (
                        r
                        for r in target.list(environment, project)
                        if r["checkpoint_id"] == row["checkpoint_id"]
                    ),
                    None,
                )
                if selected is None or selected["commit"] != row["commit"]:
                    raise ServiceError("revision_conflict")
                for step in row["steps"]:
                    if self._actual(environment, project, step["path"]) != step["expected_sha256"]:
                        raise ServiceError("artifact_conflict")
                    if step["action"] == "write":
                        body = target.content(
                            environment, project, row["checkpoint_id"], step["path"]
                        )
                        if sha(body) != step["target_sha256"]:
                            raise ServiceError("facts_corrupt")
                captured = self.broker.files.snapshot(environment, project)
                for step in row["steps"]:
                    if (
                        step["expected_sha256"] is not None
                        and step["path"] not in captured["files"]
                    ):
                        raise ServiceError("artifact_limit")
                env = self.broker._environment(environment)
                usage = self.broker.quota.usage(env["project_number"])
                growth = sum(
                    max(0, s["size"] - len(captured["files"].get(s["path"], b"")))
                    for s in row["steps"]
                    if s["action"] == "write"
                )
                scratch = max((s["size"] for s in row["steps"]), default=0) + 65536
                if usage["byte_limit"] - usage["environment_bytes"] < growth + scratch:
                    raise ServiceError("sandbox_limit")
                new_files = sum(s["expected_sha256"] is None for s in row["steps"])
                if (
                    usage["inode_limit"] - usage["environment_inodes"]
                    < new_files + len(row["steps"]) * 16 + 2
                ):
                    raise ServiceError("sandbox_limit")
                target.reference(
                    environment, project, row["checkpoint_id"], "restore-target", operation
                )
                limits = QuotaLimits.model_validate(env["limits"])
                before = target.capture(
                    environment, project, captured["files"], captured["excluded"], limits
                )
                row["before_checkpoint"] = before["checkpoint_id"]
                target.reference(
                    environment, project, before["checkpoint_id"], "restore-before", operation
                )
                self._save(row)
                self.point("before_saved", row)
                row["generation"] += 1
                row["generation_advanced"] = True
                row["status"] = "applying"
                self._save(row)
                self._project_state(row, "blocked")
                mutated = True
            else:
                if not row.get("generation_advanced"):
                    row["generation"] += 1
                    row["generation_advanced"] = True
                    self._save(row)
                self._project_state(row, "blocked")
            until = time.monotonic() + 5
            completed = 0
            for step in row["steps"]:
                if step["status"] == "verified":
                    continue
                if completed >= 1 or time.monotonic() > until:
                    row["status"] = "paused"
                    row["reason"] = "batch_limit"
                    break
                lease.check()
                actual = self._actual(environment, project, step["path"])
                if actual != step["expected_sha256"]:
                    raise ServiceError("artifact_conflict")
                step["status"] = "applying"
                self._save(row)
                self.point("before_file", row)
                if step["action"] == "write":
                    data = target.content(environment, project, row["checkpoint_id"], step["path"])
                    if "/" in step["path"]:
                        self.broker.files.mkdir(
                            environment, project, step["path"].rsplit("/", 1)[0]
                        )

                    def journal(temporary: dict[str, Any], entry: dict[str, Any] = step) -> None:
                        entry["temporary"] = temporary
                        self._save(row)
                        self.point("temporary_recorded", row)

                    self.broker.files.restore_write(
                        environment, project, step["path"], data, actual, journal
                    )
                else:
                    self.broker.files.remove(environment, project, step["path"], cast(str, actual))
                self.point("file_durable", row)
                if self._actual(environment, project, step["path"]) != step["target_sha256"]:
                    raise ServiceError("artifact_conflict")
                step["status"] = "verified"
                completed += 1
                row["next_step"] = sum(s["status"] == "verified" for s in row["steps"])
                self._save(row)
            else:
                row["status"] = "completed"
                row["reason"] = None
            self._save(row)
            if row["status"] == "completed":
                self._project_state(row, "ready")
                self._release_roots(row)
        except Exception as error:
            row["reason"] = error.code if isinstance(error, ServiceError) else "execution_unknown"
            row["status"] = (
                "paused"
                if mutated
                else "conflict"
                if row["reason"]
                in {"artifact_conflict", "input_revision_conflict", "revision_conflict"}
                else "failed_preparation"
            )
            self._save(row)
            if mutated:
                self._project_state(row, "blocked")
            else:
                try:
                    self._release_roots(row)
                except ServiceError:
                    pass
        finally:
            self.active.discard(operation)
            lease.release(clean=row["status"] in {"completed", "conflict", "failed_preparation"})
        return self._progress(row)

    @staticmethod
    def _progress(row: dict[str, Any]) -> dict[str, Any]:
        verified = sum(s["status"] == "verified" for s in row["steps"])
        return {
            **row,
            "progress": {
                "total": len(row["steps"]),
                "verified": verified,
                "pending": len(row["steps"]) - verified,
            },
        }

    def status(self, environment: str, project: str, operation: str) -> dict[str, Any]:
        row = self._record(environment, project, operation)
        if row["status"] in {"preparing", "applying", "verifying"} and operation not in self.active:
            row["status"] = "paused"
            row["reason"] = "interrupted"
            self._save(row)
            self._project_state(row, "blocked")
        return self._progress(row)

    def reconcile(self, environment: str, project: str, operation: str) -> dict[str, Any]:
        if operation in self.active:
            raise ServiceError("workspace_busy")
        row = self._record(environment, project, operation)
        if row["status"] in {"completed", "abandoned_partial", "conflict", "failed_preparation"}:
            return self._progress(row)
        lease_status = self.broker.leases.status(project)
        if lease_status["status"] != "idle":
            # 只处理本恢复持有的租约；不同operation的旧写者绝不清理。
            self.broker.leases.reconcile(project, operation, stopped=True)
        lease = self.broker.leases.claim(project, operation, deadline=time.time() + 60)
        try:
            conflicts = []
            for step in row["steps"]:
                if step.get("temporary"):
                    self.broker.files.cleanup_restore_temporary(
                        environment, project, step["path"], step["temporary"]
                    )
                actual = self._actual(environment, project, step["path"])
                if step["status"] in {"applying", "verified"} and actual == step["target_sha256"]:
                    step["status"] = "verified"
                elif step["status"] != "verified" and actual == step["expected_sha256"]:
                    step["status"] = "prepared"
                else:
                    conflicts.append(step["path"])
            row["status"] = "paused"
            row["reason"] = "artifact_conflict" if conflicts else "reconciled"
            row["conflicts"] = conflicts
            self._save(row)
            self._project_state(row, "blocked")
        finally:
            lease.release(clean=False)
        return self._progress(row)

    def resume(self, environment: str, project: str, operation: str) -> dict[str, Any]:
        row = self.reconcile(environment, project, operation)
        if row["status"] in {"completed", "abandoned_partial", "conflict", "failed_preparation"}:
            return row
        if row.get("conflicts"):
            return row
        if not row["before_checkpoint"]:
            # 备份之前崩溃不延用已失效的预览，用户重新预览确认。
            raise ServiceError("revision_conflict")
        self.broker.leases.reconcile(project, operation, stopped=True)
        return self._apply(row, preparing=False)

    def abandon(self, environment: str, project: str, operation: str) -> dict[str, Any]:
        row = self.reconcile(environment, project, operation)
        if row["status"] in {"completed", "abandoned_partial", "conflict", "failed_preparation"}:
            return row
        self.broker.leases.reconcile(project, operation, stopped=True)
        lease = self.broker.leases.claim(project, operation, deadline=time.time() + 60)
        try:
            row["status"] = "abandoned_partial"
            row["reason"] = "user_confirmed"
            row["generation"] = (
                max(
                    row["generation"],
                    self.broker._project(environment, project).get("generation", 0),
                )
                + 1
            )
            self._save(row)
            self._project_state(row, "ready")
            self._release_roots(row)
        finally:
            lease.release(clean=True)
        return self._progress(row)
