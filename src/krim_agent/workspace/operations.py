"""已接受Store/Artifact/Runner的实际数据消费者；不拥有第二队列或执行器。"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import time
from collections.abc import Callable
from typing import Any, Literal, cast

from pydantic import BaseModel, JsonValue
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError, encoded, fingerprint
from krim_agent.execution.linux import SandboxRunner
from krim_agent.runtime.contracts import ExecutionBudget, settled
from krim_agent.security import LOCAL, AccessPolicy, PrincipalContext
from krim_agent.storage import work
from krim_agent.storage.models import work_turns
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.tools.core import Invocation
from krim_agent.tools.pipeline import CURRENT, PHASE, Scope
from krim_agent.tools.workspace import Command, PlanUpdate, Read, Search, Write
from krim_agent.workspace.artifacts import ArtifactStore
from krim_agent.workspace.types import (
    ArtifactFile,
    ArtifactRevision,
    ExecutionAccess,
    Plan,
    WorkFrame,
    WorkspaceProfile,
)


class WorkspaceOperations:
    def __init__(self, store: SQLiteStore, artifacts: ArtifactStore, profile: WorkspaceProfile,
                 *, access_policy: AccessPolicy | None = None,
        runner: SandboxRunner | None = None) -> None:
        self.store, self.artifacts, self.profile = store, artifacts, profile
        self.access_policy, self.runner = access_policy, runner
        self.closed = False
        self._profile_hash = fingerprint(profile.model_dump(mode="json"))

    def principal(self, frame: WorkFrame) -> PrincipalContext:
        if self.access_policy is None:
            principal = LOCAL
        else:
            credential = next(
                (c for c in self.access_policy.credentials if c.principal_id == frame.actor_id),
                None)
            if credential is None:
                raise ServiceError("permission_denied")
            principal = PrincipalContext(credential.principal_id, credential.generation,
                                         credential.memberships, credential.legacy)
        if principal.principal_id != frame.actor_id:
            raise ServiceError("permission_denied")
        return principal

    async def prepare(self, run_id: str) -> WorkFrame:
        frame = await self.store.work_read(lambda conn: work.load(conn, run_id))
        if frame is None or self.closed:
            raise ServiceError("workspace_contract_required")
        access = ExecutionAccess(self.principal(frame), frame.grant.workspace_id, frame.session_id)
        if access.grant() != frame.grant:
            raise ServiceError("target_binding_changed")
        expected = self.profile.runtime.model_dump(
            mode="json") | {"max_tools_per_step": self.profile.max_tools_per_step}
        runtime = cast(dict[str, JsonValue], frame.conditions["runtime"])
        if any(runtime.get(k) != v for k, v in expected.items()):
            raise ServiceError("target_binding_changed")
        if frame.conditions["environment"] != self.profile.environment:
            raise ServiceError("target_binding_changed")
        return frame

    def scope(self, frame: WorkFrame, check: Callable[[], None], deadline: float,
              execution_budget: ExecutionBudget | None = None) -> Scope:
        access = ExecutionAccess(self.principal(frame), frame.grant.workspace_id, frame.session_id)

        def fence() -> None:
            check()
            if self.closed or fingerprint(
                self.profile.model_dump(
                mode="json")) != self._profile_hash or access.grant() != frame.grant:
                raise ServiceError("target_binding_changed")
            current_deadline = (execution_budget.deadline if execution_budget is not None
                                else deadline)
            if time.monotonic() >= current_deadline:
                raise ServiceError("run_timeout")

        return Scope(frame, access, self, fence, deadline, execution_budget)

    async def policy(self, invocation: Invocation) -> Literal["allow", "deny", "ask"]:
        scope = CURRENT.get()
        if scope is None or scope.operations is not self:
            return "deny"
        scope.check()
        risk = "exec" if invocation.name == "command_run" else "write" if invocation.name in {
            "workspace_write", "plan_update"} else "read"
        if risk not in scope.access.grant().operations:
            return "deny"
        if invocation.name in {"workspace_write", "command_run"}:
            parameters = (
                Write if invocation.name == "workspace_write" else Command).model_validate_json(
                invocation.arguments_json)
            head = await self.artifacts.head(scope.access)
            scope.check()
            if head.revision != parameters.expected_revision:
                raise ServiceError("input_revision_conflict")
            if isinstance(parameters, Write):
                old = next((file for file in head.manifest if file.path == parameters.path), None)
                if (old.sha256 if old else None) != parameters.expected_file_hash:
                    raise ServiceError("input_revision_conflict")
                size = old.size if old else 0
                if parameters.offset > size or parameters.offset+parameters.delete_length > size:
                    raise ServiceError("invalid_input")
                if size-parameters.delete_length+len(parameters.replacement.encode()) > 1048576:
                    raise ServiceError("artifact_limit")
        return "ask" if risk in {"write", "exec"} and invocation.name != "plan_update" else "allow"

    async def runner_hook(self, phase: str) -> None:
        scope = CURRENT.get()
        if scope is None:
            raise ServiceError("workspace_contract_required")
        token = PHASE.set(phase)
        try:
            if phase in {"payload_output", "quiescent"}:
                if scope.invocation is None:
                    raise ServiceError("workspace_contract_required")
                call = scope.invocation
                # 可信控制通道的实际payload输出/退出证明已经进入；不以dispatch意图推导。
                await self.store.append(call.run_id, "workspace_started", {"call_id": call.call_id,
                    "span_id": call.span_id, "executor_started": True},
                    event_id="workspace-started:"+call.call_id)
            await self.store.work_read(lambda conn: work.effect_admission(conn,
                scope.frame.session_id))
            scope.check()
        finally:
            PHASE.reset(token)

    def runner_fence(self) -> bool:
        scope = CURRENT.get()
        if scope is None:
            return False
        scope.check()
        return True

    async def _read(self, file: ArtifactFile, check: Callable[[], None]) -> bytes:
        async def owned() -> bytes:
            def reading() -> bytes:
                check()
                self.artifacts._current()
                return self.artifacts._blob(file.sha256, file.size)
            value = await settled(asyncio.to_thread(reading))
            check()
            return value
        return await self.artifacts._owned(owned())

    async def _files(self, access: ExecutionAccess, revision: ArtifactRevision,
     check: Callable[[], None]) -> dict[str, bytes]:
        # revision已经逐次授权；线程只读同一私有dirfd的固定hash/长度对象。
        result = {}
        for file in revision.manifest:
            result[file.path] = await self._read(file, check)
            check()
        return result

    async def observe(self, invocation: Invocation) -> dict[str, JsonValue] | None:
        scope = CURRENT.get()
        if scope is None:
            return None
        found = await self.store.work_read(lambda conn: work.effect(conn, scope.access,
            invocation.call_id))
        if found is None or found["run_id"] != invocation.run_id or found["receipt"] is None:
            return None
        return cast(dict[str, JsonValue], found["receipt"])

    async def execute(self, invocation: Invocation, parameters: BaseModel,
     context: object) -> JsonValue:
        from krim_agent.workspace import verification

        observation = None
        observation_failure = None
        observation_helpers = verification._OBSERVATION_HELPERS
        from krim_agent.execution.types import Request

        scope = CURRENT.get()
        if scope is None or scope.operations is not self:
            raise ServiceError("workspace_contract_required")
        check, access = scope.check, scope.access
        check()
        if not isinstance(parameters, (Read, Search, Write, Command, PlanUpdate)):
            raise ServiceError("invalid_input")
        reading = isinstance(parameters, (Read, Search))
        head = await self.artifacts.get(access,
            parameters.revision_id) if isinstance(parameters, (Read,
            Search)) else await self.artifacts.head(access)
        check()
        if isinstance(parameters, (Write,
            Command)) and head.revision != parameters.expected_revision:
            raise ServiceError("input_revision_conflict")
        kind = "read" if reading else "command" if isinstance(parameters, Command) else "write"
        async def intent(conn: AsyncConnection) -> dict[str, Any]:
            result = await work.intent(conn, access, invocation.run_id, call_id=invocation.call_id,
                operation_key=invocation.call_id,
                args_hash=fingerprint(parameters.model_dump(mode="json")),
                tool_hash=invocation.tool_hash, kind=kind, input_revision=head.revision_id)
            check()
            return result
        async def confirmed() -> dict[str, Any] | None:
            found = await self.store.work_read(lambda conn: work.effect(conn, access,
                invocation.call_id))
            return found if found and found[
                "run_id"] == invocation.run_id and found[
                "args_hash"] == fingerprint(
                parameters.model_dump(
                mode="json")) and found["tool_hash"] == invocation.tool_hash else None
        prior = await self.store.work_write(intent, confirmed)
        check()
        if prior["phase"] != "intent":
            # 未知或已派发原键只能核实，不重执行。
            if isinstance(parameters,
                PlanUpdate) and prior["receipt"] and prior["receipt"]["output_complete"] is True:
                return {"plan_revision": parameters.expected_revision+1,
                    "steps": len(parameters.steps)}
            if prior["receipt"] and prior["receipt"].get("artifact_revision_id"):
                revision = await self.artifacts.get(access,
                    prior["receipt"]["artifact_revision_id"])
                check()
                return revision.model_dump(mode="json")
            raise ServiceError("execution_unknown")
        async def dispatch(conn: AsyncConnection) -> bool:
            await work.dispatch(conn, invocation.call_id, access)
            check()
            return True
        async def dispatched() -> bool | None:
            found = await confirmed()
            return True if found and found["phase"] == "dispatch_intent" else None
        await self.store.work_write(dispatch, dispatched)
        check()
        async def remember(actual: dict[str, JsonValue]) -> dict[str, Any]:
            async def write(conn: AsyncConnection) -> dict[str, Any]:
                result = await work.record_result(conn, access, invocation.call_id, actual,
                    run_id=invocation.run_id)
                await self.store._event(conn, invocation.run_id, "workspace_receipt", {
                    "call_id": invocation.call_id, "provider_call_id": invocation.provider_call_id,
                    "span_id": invocation.span_id, "step_index": invocation.step_index,
                    "name": invocation.name, "receipt": result,
                }, "workspace-receipt:"+invocation.call_id)
                nonlocal observation_failure
                if observation is not None:
                    await conn.exec_driver_sql("SAVEPOINT krim_d4_observation")
                    try:
                        observation_helpers[0]()
                        check()
                        await self.store._event(conn, invocation.run_id,
                            "workspace_command_observation", observation,
                            "workspace-command-observation:"+invocation.call_id)
                        observation_helpers[0]()
                        check()
                    except ServiceError as error:
                        if error.code not in {"event_limit", "output_limit", "key_conflict",
                                              "target_binding_changed", "run_timeout",
                                              "completion_unverified"}:
                            raise
                        await conn.exec_driver_sql("ROLLBACK TO krim_d4_observation")
                        observation_failure = error.code
                    await conn.exec_driver_sql("RELEASE krim_d4_observation")
                return result
            async def receipt_confirmed() -> dict[str, Any] | None:
                found = await confirmed()
                return found["receipt"] if found and found["receipt"] is not None else None
            return await self.store.work_write(write, receipt_confirmed)

        if isinstance(parameters, (Read, Search)):
            if isinstance(parameters, Read):
                file = next((file for file in head.manifest if file.path == parameters.path), None)
                if file is None:
                    raise ServiceError("not_found", 404)
                body = await self._read(file, check)
                check()
                from krim_agent.workspace.types import ArtifactChunk
                chunk = body[parameters.offset:parameters.offset+parameters.length]
                result = ArtifactChunk(revision_id=head.revision_id, path=file.path,
                    offset=parameters.offset, total_size=file.size, length=len(chunk),
                    sha256=file.sha256,
                    data=base64.b64encode(chunk).decode()).model_dump(mode="json")
            else:
                files = await self._files(access, head, check)
                check()
                matches: list[JsonValue] = []
                for name, body in sorted(files.items()):
                    try:
                        text = body.decode("utf-8")
                    except UnicodeDecodeError:
                        continue
                    for line, content in enumerate(text.splitlines(), 1):
                        if parameters.query in content and len(matches) < 50:
                            item: dict[str, JsonValue] = {"path": name, "line": line,
                                "preview": content[:96]}
                            if len(encoded({"matches": matches+[item]}).encode()) <= 8192:
                                matches.append(item)
                result = {"revision_id": head.revision_id, "matches": matches}
            await remember({"executor_started": True, "exit_code": None,
                "output_complete": True, "cleanup": "clean", "code": None})
            check()
            return result
        if isinstance(parameters, Command):
            if self.runner is None:
                raise ServiceError("sandbox_unavailable")
            files = await self._files(access, head, check)
            check()
            try:
                receipt = await self.runner.run(Request(tuple(parameters.argv), files,
                    parameters.cwd))
            except (ServiceError, asyncio.CancelledError) as error:
                # Runner在取消/绑定失败时不返回Receipt；只保留我们确已持久观察的进入事实。
                # 不读其私有_resources、不从监督进程wait或证据日志补造payload exit/clean。
                async def interrupted(
                    failure: ServiceError | asyncio.CancelledError = error) -> dict[str, Any]:
                    observed = await self.store.snapshot(invocation.run_id)
                    started = any(event.kind == "workspace_started"
                        and event.data.get(
                        "call_id") == invocation.call_id for event in observed.events)
                    return await remember({"executor_started": True if started else None,
                        "exit_code": None, "output_complete": None, "cleanup": "unknown",
                        "code": failure.code if isinstance(failure, ServiceError) else "cancelled"})
                await settled(interrupted())
                raise
            # 同步深快照先于新增观察/封印/await；files仅允许不可变bytes值。
            known_receipt = (receipt.exit_code, receipt.output_complete, receipt.cleanup,
                                receipt.code, receipt.stdout, receipt.stderr)
            snapshot_files = tuple(receipt.files.items()) if type(receipt.files) is dict else None
            command_files = dict(snapshot_files) if snapshot_files is not None and all(
                type(name) is str and type(value) is bytes
                for name, value in snapshot_files) else None
            try:
                observation_helpers[0]()
                check()
                verification_contract = scope.frame.conditions.get("verification")
                contract = verification_contract.get("contract") if isinstance(
                    verification_contract, dict) else None
                if not isinstance(contract, dict):
                    raise verification.ObservationRejected("completion_unverified")
                if observation_helpers[2]() != contract:
                    raise verification.ObservationRejected("target_binding_changed")
                observation = observation_helpers[1](invocation, head.revision_id,
                    known_receipt, contract)
                if observation["discovered_tests"] is None:
                    observation_failure = "completion_unverified"
            except ServiceError as error:
                if error.code not in {"target_binding_changed", "run_timeout",
                                      "completion_unverified"}:
                    raise
                observation_failure = error.code
            except (ValueError, UnicodeError, RecursionError):
                observation_failure = "completion_unverified"
            observed = await self.store.snapshot(invocation.run_id)
            started = type(known_receipt[0]) is int or any(event.kind == "workspace_started"
                and event.data.get("call_id") == invocation.call_id for event in observed.events)
            await remember({"executor_started": True if started else None,
                "exit_code": known_receipt[0], "output_complete": known_receipt[1],
                "cleanup": known_receipt[2], "code": known_receipt[3]})
            check()
            if observation_failure is not None:
                raise ServiceError(observation_failure)
            from krim_agent.workspace.artifacts import manifest_for

            if (known_receipt[1] is not True or known_receipt[2] != "clean"
                    or type(known_receipt[0]) is not int):
                raise ServiceError("execution_unknown")
            if command_files is None:
                raise ServiceError("artifact_invalid")
            manifest = manifest_for(command_files)
            digest = fingerprint({
                "key": invocation.call_id, "expected": head.revision,
                "run_id": invocation.run_id, "call_id": invocation.call_id,
                "manifest": manifest, "exit_code": known_receipt[0],
            })
            actual: dict[str, JsonValue] = {
                "executor_started": True, "exit_code": known_receipt[0],
                "output_complete": True, "cleanup": known_receipt[2], "code": known_receipt[3],
            }

            async def publish_command() -> ArtifactRevision:
                self.artifacts._current()
                await self.store.work_read(
                    lambda conn: work.row(conn, access.session_id, access, write=True)
                )
                check()
                old = await self.artifacts._known(access, "effect", invocation.call_id, digest)
                check()
                if old is not None:
                    return old
                if self.store.readonly:
                    raise ServiceError("readonly")
                await self.artifacts._point("before_stage")
                check()

                def stage_command() -> list[dict[str, Any]]:
                    check()
                    return self.artifacts._stage(command_files)

                staged = await settled(asyncio.to_thread(stage_command))
                check()
                if staged != manifest:
                    raise ServiceError("artifact_invalid")
                await self.artifacts._point("before_cas")
                check()

                async def commit_command(conn: AsyncConnection) -> ArtifactRevision:
                    check()
                    revision = await work.publish(
                        conn, access.session_id, access, kind="effect",
                        key=invocation.call_id, digest=digest, expected=head.revision,
                        manifest=manifest, run_id=invocation.run_id,
                        call_id=invocation.call_id, receipt=actual,
                    )
                    # SQL等待/INSERT后漂移必须在原事务提交前抛错、回滚引用。
                    check()
                    return revision

                revision = await self.store.work_write(
                    commit_command,
                    lambda: self.artifacts._known(access, "effect", invocation.call_id, digest),
                )
                await self.artifacts._point("after_cas")
                check()
                return revision

            revision = await self.artifacts._owned(publish_command())
            check()
            return {"revision_id": revision.revision_id, "exit_code": known_receipt[0],
                    "output_complete": known_receipt[1], "cleanup": known_receipt[2]}
        if isinstance(parameters, PlanUpdate):
            async def plan(conn: AsyncConnection) -> dict[str, JsonValue]:
                old = (
                    await conn.execute(
                    select(
                    work_turns).where(work_turns.c.run_id == invocation.run_id))).mappings().one()
                check()
                if old["plan_revision"] != parameters.expected_revision:
                    raise ServiceError("revision_conflict")
                steps: list[JsonValue] = [
                    s.model_dump(mode="json") | {"proposed_by": "model"} for s in parameters.steps]
                value: dict[str, JsonValue] = {"revision": parameters.expected_revision+1,
                    "steps": steps}
                Plan.model_validate_json(encoded(value))
                await conn.execute(
                    update(
                    work_turns).where(
                    work_turns.c.run_id == invocation.run_id).values(plan=value,
                    plan_revision=value["revision"]))
                check()
                await work.record_result(conn, access, invocation.call_id,
                    {"executor_started": True,
                    "exit_code": None, "output_complete": True, "cleanup": "clean",
                    "code": None}, run_id=invocation.run_id)
                await self.store._event(conn, invocation.run_id, "workspace_receipt", {
                    "call_id": invocation.call_id, "provider_call_id": invocation.provider_call_id,
                    "span_id": invocation.span_id, "step_index": invocation.step_index,
                    "name": invocation.name, "receipt": {"executor_started": True,
                    "exit_code": None,
                        "output_complete": True, "cleanup": "clean", "code": None},
                }, "workspace-receipt:"+invocation.call_id)
                check()
                return {"plan_revision": parameters.expected_revision+1, "steps": len(steps)}
            async def plan_confirmed() -> dict[str, JsonValue] | None:
                found = await confirmed()
                if not found or found["receipt"] is None:
                    return None
                async def read(conn: AsyncConnection) -> dict[str, JsonValue] | None:
                    row = (
                        await conn.execute(
                        select(
                        work_turns).where(
                        work_turns.c.run_id == invocation.run_id))).mappings().one()
                    expected = {"revision": parameters.expected_revision+1,
                        "steps": [
                        s.model_dump(
                        mode="json") | {"proposed_by": "model"} for s in parameters.steps]}
                    return {"plan_revision": parameters.expected_revision+1,
                        "steps": len(parameters.steps)} if row["plan"] == expected else None
                return await self.store.work_read(read)
            result = await self.store.work_write(plan, plan_confirmed)
            check()
            return result
        if not isinstance(parameters, Write):
            raise ServiceError("invalid_input")
        files = await self._files(access, head, check)
        check()
        old = files.get(parameters.path)
        if (
            hashlib.sha256(
            old).hexdigest() if old is not None else None) != parameters.expected_file_hash:
            raise ServiceError("input_revision_conflict")
        data = old or b""
        if parameters.offset > len(data) or parameters.offset+parameters.delete_length > len(data):
            raise ServiceError("invalid_input")
        files[
            parameters.path] = data[
            :parameters.offset]+parameters.replacement.encode(
            )+data[parameters.offset+parameters.delete_length:]
        async def publish() -> JsonValue:
            def stage() -> list[dict[str, JsonValue]]:
                check()
                return self.artifacts._stage(files)
            manifest = await settled(asyncio.to_thread(stage))
            check()
            actual: dict[str, JsonValue] = {"executor_started": True, "exit_code": None,
                "output_complete": True, "cleanup": "clean", "code": None}
            async def commit(conn: AsyncConnection) -> ArtifactRevision:
                check()
                revision = await work.publish(conn, access.session_id, access, kind="effect",
                    key=invocation.call_id,
                    digest=fingerprint({"arguments": parameters.model_dump(mode="json"),
                    "manifest": manifest}),
                    expected=head.revision, manifest=manifest, run_id=invocation.run_id,
                    call_id=invocation.call_id, receipt=actual)
                check()
                await self.store._event(conn, invocation.run_id, "workspace_receipt", {
                    "call_id": invocation.call_id, "provider_call_id": invocation.provider_call_id,
                    "span_id": invocation.span_id, "step_index": invocation.step_index,
                    "name": invocation.name,
                    "receipt": actual | {"artifact_revision_id": revision.revision_id},
                }, "workspace-receipt:"+invocation.call_id)
                # 同事务返回前的最后fence失败会使原_write回滚；不改受保护事务机制。
                check()
                return revision
            result = await self.store.work_write(commit, lambda: self.artifacts._known(access,
                "effect", invocation.call_id))
            check()
            return result.model_dump(mode="json")
        return await self.artifacts._owned(publish())

    async def close(self) -> None:
        # Application先join原Run producer；借用能力不关闭底层owner。
        self.closed = True
