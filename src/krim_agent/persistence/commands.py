"""命令账本：先保存派发意图，过期不重放，原producer只可补充迟到事实。"""

from __future__ import annotations

import hashlib
import json
import secrets
import time
from collections.abc import Mapping
from datetime import UTC
from typing import Literal, cast
from uuid import uuid4

from pydantic import JsonValue, ValidationError
from sqlalchemy import func, select, update
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.execution.directories import (
    CommandDirectoryCatalog,
    directory_metadata_limit,
    freeze_directory_manifest,
    read_directory_manifest,
)
from krim_agent.execution.models import ExecutionResult, ExecutionStatus, SandboxFile, SandboxLimits
from krim_agent.execution.records import (
    CommandFile,
    CommandPhase,
    CommandRequest,
    CommandReservation,
    CommandResource,
    CommandSnapshot,
    CommandView,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import check_json, utc_now
from krim_agent.workspace import Workspace

from .agent_sessions import assert_session_open, assert_session_owner
from .command_models import CommandEventRow, CommandFileRow, CommandGuardRow, CommandRow
from .database import AgentSessionRow, Database, RunRow, TopicRow

ACTIVE = {"preparing", "dispatched", "running"}


def digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()


def freeze_files(
    files: Mapping[str, SandboxFile | bytes], limits: SandboxLimits
) -> dict[str, SandboxFile]:
    if len(files) > limits.artifact_files:
        raise ServiceError("execution_input_limit", "命令文件数量超过上限", 422)
    frozen, size = {}, 0
    for path, raw in sorted(files.items()):
        if not Workspace._parts(path):
            raise ServiceError("execution_input_path", "命令文件须为相对路径", 422)
        value = raw if isinstance(raw, SandboxFile) else SandboxFile(raw)
        if (
            not isinstance(value.data, bytes)
            or type(value.mode) is not int
            or not 0 <= value.mode <= 0o777
        ):
            raise ServiceError("execution_input_file", "命令文件内容或权限无效", 422)
        size += len(value.data)
        if len(value.data) > limits.file_bytes or size > limits.artifact_bytes:
            raise ServiceError("execution_input_limit", "命令文件字节数超过上限", 422)
        frozen[path] = value
    return frozen


def metadata(path: str, value: SandboxFile) -> CommandFile:
    return CommandFile(
        path=path,
        sha256=hashlib.sha256(value.data).hexdigest(),
        byte_size=len(value.data),
        mode=value.mode,
    )


class CommandStore:
    def __init__(
        self,
        database: Database,
        *,
        owner_id: str | None = None,
        lease_seconds: float = 10,
        max_commands: int = 500,
        max_reserved_bytes: int = 536870912,
    ) -> None:
        if not 0.2 <= lease_seconds <= 600 or not 1 <= max_commands <= 5000:
            raise ValueError("命令租约或数量上限无效")
        if not 1024 <= max_reserved_bytes <= 2147483648:
            raise ValueError("命令持久容量无效")
        self.database, self.owner_id = database, owner_id or str(uuid4())
        self.lease_seconds, self.max_commands = lease_seconds, max_commands
        self.max_reserved_bytes = max_reserved_bytes

    @staticmethod
    async def _writer(session: AsyncSession) -> None:
        # 首条SQL取得SQLite写锁；覆盖两Database实例和真正独立进程的准入竞争。
        await session.execute(
            insert(CommandGuardRow).values(id=1, revision=0).on_conflict_do_nothing()
        )
        await session.execute(
            update(CommandGuardRow)
            .where(CommandGuardRow.id == 1)
            .values(revision=CommandGuardRow.revision + 1)
        )

    async def _scope(
        self, session: AsyncSession, scope: str, *, writing: bool, run_id: str | None = None
    ) -> str:
        if scope.startswith("session:"):
            row = await session.get(AgentSessionRow, scope.removeprefix("session:"))
            if row is None:
                raise ServiceError("agent_session_not_found", "命令会话不存在", 404)
            if writing:
                await assert_session_owner(session, row, self.owner_id)
                await assert_session_open(session, row)
            topic = row.topic_id
        elif scope.startswith("topic:"):
            topic = scope.removeprefix("topic:")
            if await session.get(TopicRow, topic) is None:
                raise ServiceError("topic_not_found", "命令话题不存在", 404)
            if await session.scalar(
                select(AgentSessionRow.session_id).where(AgentSessionRow.topic_id == topic)
            ):
                raise ServiceError("execution_scope_managed", "Agent命令须使用自身会话", 403)
        else:
            raise ServiceError("execution_scope_invalid", "命令作用域无效", 422)
        if run_id is not None:
            run = await session.get(RunRow, run_id)
            if run is None or run.topic_id != topic:
                raise ServiceError("execution_run_mismatch", "命令Run不属于该作用域", 403)
            if writing and (run.status not in {"queued", "running"} or run.stop_requested):
                raise ServiceError("execution_run_closed", "命令所属Run已停止", 409)
        return topic

    @staticmethod
    def _snapshot(row: CommandRow) -> CommandSnapshot:
        if digest(row.snapshot) != row.request_hash:
            raise ServiceError("execution_record_corrupt", "命令快照校验失败", 409)
        return CommandSnapshot.model_validate(row.snapshot)

    @staticmethod
    def _event(session: AsyncSession, row: CommandRow, kind: str) -> None:
        row.revision += 1
        row.updated_at = utc_now()
        session.add(
            CommandEventRow(
                command_id=row.command_id,
                sequence=row.revision,
                kind=kind,
                payload={
                    "phase": row.phase,
                    "dispatched": row.dispatched,
                    "started": row.started,
                    "stdout_bytes": len(row.stdout),
                    "stderr_bytes": len(row.stderr),
                    "error_code": row.error_code,
                },
                created_at=row.updated_at,
            )
        )

    def _expire(self, session: AsyncSession, row: CommandRow) -> None:
        if row.phase in ACTIVE and (row.lease_expires_at or 0) <= time.time():
            row.phase, row.error_code = "interrupted", "execution_interrupted"
            row.owner_id, row.lease_expires_at = None, None
            row.interrupted_at = utc_now()
            self._event(session, row, "interrupted")

    @staticmethod
    async def _row(session: AsyncSession, scope: str, identifier: str) -> CommandRow:
        row = await session.get(CommandRow, identifier)
        if row is None or row.scope_id != scope:
            raise ServiceError("execution_not_found", "命令不在该作用域中", 404)
        return row

    def _view(self, row: CommandRow) -> CommandView:
        saved = self._snapshot(row)
        result = row.result or {}
        outcome = result.get("status")
        if row.phase == "interrupted" and outcome is None:
            outcome = "uncertain" if row.dispatched else "setup_failed"
        return CommandView(
            command_id=row.command_id,
            scope_id=row.scope_id,
            topic_id=row.topic_id,
            request=saved.request,
            resource=saved.resource,
            run_id=row.run_id,
            call_id=row.call_id,
            phase=cast(CommandPhase, row.phase),
            revision=row.revision,
            cancel_requested=row.cancel_requested,
            dispatched=row.dispatched,
            started=row.started,
            outcome=cast(ExecutionStatus | None, outcome),
            exit_code=cast(int | None, result.get("exit_code")),
            output_truncated=bool(result.get("output_truncated", False)),
            stdout_bytes=len(row.stdout),
            stderr_bytes=len(row.stderr),
            input_files=len(saved.inputs),
            artifact_files=cast(int, result.get("artifact_files", 0)),
            artifacts_complete=bool(result.get("artifacts_complete", False)),
            processes_reaped=row.result is not None,
            error_code=row.error_code,
            created_at=row.created_at.replace(tzinfo=UTC),
            updated_at=row.updated_at.replace(tzinfo=UTC),
            interrupted_at=row.interrupted_at.replace(tzinfo=UTC) if row.interrupted_at else None,
        )

    async def reserve(
        self,
        scope: str,
        request: CommandRequest,
        resource: CommandResource,
        limits: SandboxLimits,
        files: Mapping[str, SandboxFile | bytes] | None = None,
        *,
        directories: tuple[str, ...] = (),
        run_id: str | None = None,
        call_id: str | None = None,
        origin: dict[str, JsonValue] | None = None,
    ) -> CommandReservation:
        values = freeze_files(files or {}, limits)
        if origin is not None:
            check_json(origin, max_bytes=131072)
        if (run_id is None) != (call_id is None):
            raise ServiceError("execution_call_binding", "Run与call须同时绑定", 422)
        if len(directories) > limits.artifact_files or any(
            not Workspace._parts(path) for path in directories
        ):
            raise ServiceError("execution_input_path", "命令目录无效或超限", 422)
        snapshot = CommandSnapshot.model_validate(
            CommandSnapshot(
                request=request,
                resource=resource,
                limits=limits,
                directories=sorted(set(directories)),
                inputs=[metadata(path, value) for path, value in values.items()],
                run_id=run_id,
                call_id=call_id,
                origin=origin,
            ).model_dump()
        )
        payload = snapshot.model_dump(mode="json")
        if origin is None:
            payload.pop("origin", None)
        checksum = digest(payload)
        reserved = (
            sum(len(value.data) for value in values.values())
            + limits.artifact_bytes
            + limits.output_bytes
            + directory_metadata_limit(limits)
            + 256
        )
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self._writer(session)
            topic = await self._scope(session, scope, writing=False, run_id=run_id)
            previous = await session.scalar(
                select(CommandRow).where(
                    CommandRow.scope_id == scope,
                    CommandRow.request_key == snapshot.request.request_key,
                )
            )
            if previous is not None:
                if previous.request_hash != checksum:
                    raise ServiceError("execution_key_conflict", "原键已绑定其他命令或输入", 409)
                self._expire(session, previous)
                return CommandReservation(self._view(previous), None)
            await self._scope(session, scope, writing=True, run_id=run_id)
            if scope.startswith("session:"):
                agent = await session.get(AgentSessionRow, scope.removeprefix("session:"))
                assert agent is not None
                if agent.permissions.get("execution") != resource.model_dump(mode="json"):
                    raise ServiceError("execution_binding_changed", "会话未绑定此执行资源", 403)
            if run_id is not None and await session.scalar(
                select(CommandRow.command_id).where(
                    CommandRow.run_id == run_id, CommandRow.call_id == call_id
                )
            ):
                raise ServiceError("execution_call_conflict", "原工具调用已绑定其他命令键", 409)
            count = await session.scalar(
                select(func.count()).select_from(CommandRow).where(CommandRow.scope_id == scope)
            )
            used = await session.scalar(
                select(func.coalesce(func.sum(CommandRow.reserved_bytes), 0))
            )
            if (count or 0) >= self.max_commands or (
                used or 0
            ) + reserved > self.max_reserved_bytes:
                raise ServiceError("execution_storage_limit", "命令记录或私有成果容量已达上限", 429)
            row = CommandRow(
                command_id=str(uuid4()),
                scope_id=scope,
                topic_id=topic,
                request_key=snapshot.request.request_key,
                request_hash=checksum,
                snapshot=payload,
                run_id=run_id,
                call_id=call_id,
                phase="preparing",
                revision=0,
                token=secrets.token_hex(32),
                owner_id=self.owner_id,
                lease_expires_at=time.time() + self.lease_seconds,
                sandbox_id=None,
                cancel_requested=False,
                dispatched=False,
                started=False,
                stdout=b"",
                stderr=b"",
                result=None,
                error_code=None,
                reserved_bytes=reserved,
                created_at=utc_now(),
                updated_at=utc_now(),
                interrupted_at=None,
            )
            session.add(row)
            await session.flush()
            for path, value in values.items():
                session.add(
                    CommandFileRow(
                        command_id=row.command_id,
                        kind="input",
                        path=path,
                        mode=value.mode,
                        sha256=hashlib.sha256(value.data).hexdigest(),
                        content=value.data,
                    )
                )
            self._event(session, row, "accepted")
            return CommandReservation(self._view(row), row.token)

    async def _live(
        self, session: AsyncSession, scope: str, identifier: str, token: str
    ) -> CommandRow:
        row = await self._row(session, scope, identifier)
        if (
            row.token != token
            or row.owner_id != self.owner_id
            or row.phase not in ACTIVE
            or (row.lease_expires_at or 0) <= time.time()
        ):
            raise ServiceError("execution_lease_lost", "命令原生产者资格已失效", 409)
        if row.cancel_requested:
            raise ServiceError("execution_cancelled", "命令已请求取消", 409)
        await self._scope(session, scope, writing=True, run_id=row.run_id)
        saved = self._snapshot(row)
        if scope.startswith("session:"):
            agent = await session.get(AgentSessionRow, scope.removeprefix("session:"))
            assert agent is not None
            if agent.permissions.get("execution") != saved.resource.model_dump(mode="json"):
                raise ServiceError("execution_binding_changed", "命令资源授权已变化", 403)
        return row

    async def authorize(
        self,
        scope: str,
        identifier: str,
        token: str,
        *,
        sandbox_id: str | None = None,
        dispatch: bool = False,
    ) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self._writer(session)
            row = await self._live(session, scope, identifier, token)
            if sandbox_id is not None:
                if row.sandbox_id not in {None, sandbox_id}:
                    raise ServiceError("execution_already_bound", "命令已绑定原隔离执行", 409)
                row.sandbox_id = sandbox_id
            if dispatch:
                if row.phase != "preparing" or row.sandbox_id is None:
                    raise ServiceError("execution_already_dispatched", "命令不能再次派发", 409)
                row.phase, row.dispatched = "dispatched", True
                self._event(session, row, "dispatched")

    async def progress(
        self,
        scope: str,
        identifier: str,
        token: str,
        stdout: bytes,
        stderr: bytes,
        *,
        started: bool,
    ) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self._writer(session)
            row = await self._live(session, scope, identifier, token)
            self._output(row, stdout, stderr)
            changed = (row.stdout, row.stderr, row.started) != (
                stdout,
                stderr,
                started or row.started,
            )
            if started and not row.dispatched:
                raise ServiceError("execution_record_order", "命令未记录派发意图", 409)
            row.stdout, row.stderr, row.started = stdout, stderr, started or row.started
            if row.started:
                row.phase = "running"
            row.lease_expires_at = time.time() + self.lease_seconds
            if changed:
                self._event(session, row, "progress")

    def _output(self, row: CommandRow, stdout: bytes, stderr: bytes) -> None:
        limit = self._snapshot(row).limits.output_bytes
        if (
            not isinstance(stdout, bytes)
            or not isinstance(stderr, bytes)
            or not stdout.startswith(row.stdout)
            or not stderr.startswith(row.stderr)
            or len(stdout) + len(stderr) > limit
        ):
            raise ServiceError("execution_output_conflict", "命令输出回退、改写或超限", 409)

    async def finish(
        self, scope: str, identifier: str, token: str, result: ExecutionResult
    ) -> CommandView:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self._writer(session)
            row = await self._row(session, scope, identifier)
            if row.token != token or row.sandbox_id not in {None, result.execution_id}:
                raise ServiceError("execution_receipt_owner", "回执不属于原命令生产者", 409)
            if row.started and not result.started or result.started and not row.dispatched:
                raise ServiceError("execution_receipt_order", "命令开始事实冲突", 409)
            values = {item.path: SandboxFile(item.data, item.mode) for item in result.artifacts}
            if len(values) != len(result.artifacts):
                raise ServiceError("execution_artifact_invalid", "命令产物路径重复", 409)
            saved = self._snapshot(row)
            values = freeze_files(values, saved.limits)
            manifest = freeze_directory_manifest(
                result.directories, set(values), saved.limits, complete=result.artifacts_complete
            )
            payload: dict[str, JsonValue] = {
                "status": result.status,
                "exit_code": result.exit_code,
                "output_truncated": result.output_truncated,
                "artifacts_complete": result.artifacts_complete,
                "artifact_files": len(values),
                "artifacts": [
                    metadata(path, value).model_dump(mode="json") for path, value in values.items()
                ],
                "stdout_sha256": hashlib.sha256(result.stdout).hexdigest(),
                "stderr_sha256": hashlib.sha256(result.stderr).hexdigest(),
                "error_code": result.error_code,
                "elapsed_seconds": result.elapsed_seconds,
                "descendants_stopped": result.descendants_stopped,
            }
            if manifest is not None:
                payload["directory_manifest"] = manifest
            if row.result is not None:
                if row.result != payload:
                    raise ServiceError("execution_receipt_conflict", "原命令已有不同终态", 409)
                return self._view(row)
            self._output(row, result.stdout, result.stderr)
            self._expire(session, row)
            row.result, row.phase, row.error_code = payload, "finished", result.error_code
            row.stdout, row.stderr = result.stdout, result.stderr
            row.dispatched, row.started = row.dispatched or result.dispatched, result.started
            row.sandbox_id = result.execution_id
            row.owner_id, row.lease_expires_at = None, None
            row.reserved_bytes = (
                sum(item.byte_size for item in saved.inputs)
                + len(result.stdout)
                + len(result.stderr)
                + sum(len(value.data) for value in values.values())
                + (len(json.dumps(manifest).encode("utf-8")) if manifest is not None else 0)
            )
            for path, value in values.items():
                session.add(
                    CommandFileRow(
                        command_id=identifier,
                        kind="artifact",
                        path=path,
                        mode=value.mode,
                        sha256=hashlib.sha256(value.data).hexdigest(),
                        content=value.data,
                    )
                )
            self._event(session, row, "finished")
            return self._view(row)

    async def get(self, scope: str, identifier: str) -> CommandView:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self._writer(session)
            await self._scope(session, scope, writing=False)
            row = await self._row(session, scope, identifier)
            self._expire(session, row)
            return self._view(row)

    async def interrupt(self, scope: str, identifier: str, token: str) -> None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self._writer(session)
            row = await self._row(session, scope, identifier)
            if row.token != token:
                raise ServiceError("execution_receipt_owner", "中断不属于原生产者", 409)
            if row.phase in ACTIVE:
                row.lease_expires_at = 0
                self._expire(session, row)

    async def by_key(self, scope: str, key: str) -> CommandView | None:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self._writer(session)
            await self._scope(session, scope, writing=False)
            row = await session.scalar(
                select(CommandRow).where(
                    CommandRow.scope_id == scope, CommandRow.request_key == key
                )
            )
            if row is None:
                return None
            self._expire(session, row)
            return self._view(row)

    async def list_commands(
        self, scope: str, *, after: str | None = None, limit: int = 50
    ) -> list[CommandView]:
        if not 1 <= limit <= 100:
            raise ServiceError("execution_page", "命令分页大小须为1–100", 422)
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self._writer(session)
            await self._scope(session, scope, writing=False)
            query = select(CommandRow).where(CommandRow.scope_id == scope)
            if after is not None:
                previous = await self._row(session, scope, after)
                query = query.where(
                    (CommandRow.created_at > previous.created_at)
                    | (
                        (CommandRow.created_at == previous.created_at)
                        & (CommandRow.command_id > previous.command_id)
                    )
                )
            rows = list(
                await session.scalars(
                    query.order_by(CommandRow.created_at, CommandRow.command_id).limit(limit)
                )
            )
            for row in rows:
                self._expire(session, row)
            return [self._view(row) for row in rows]

    async def cancel(self, scope: str, identifier: str) -> CommandView:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self._writer(session)
            await self._scope(session, scope, writing=False)
            row = await self._row(session, scope, identifier)
            self._expire(session, row)
            if row.phase in ACTIVE and not row.cancel_requested:
                row.cancel_requested = True
                self._event(session, row, "cancel_requested")
            return self._view(row)

    async def output(self, scope: str, identifier: str) -> tuple[bytes, bytes]:
        async with self.database.write_lock, self.database.sessions() as session:
            await self._scope(session, scope, writing=False)
            row = await self._row(session, scope, identifier)
            if row.result and (
                hashlib.sha256(row.stdout).hexdigest() != row.result["stdout_sha256"]
                or hashlib.sha256(row.stderr).hexdigest() != row.result["stderr_sha256"]
            ):
                raise ServiceError("execution_record_corrupt", "命令输出校验失败", 409)
            return row.stdout, row.stderr

    async def origin(self, scope: str, identifier: str) -> dict[str, JsonValue] | None:
        async with self.database.write_lock, self.database.sessions() as session:
            await self._scope(session, scope, writing=False)
            return self._snapshot(await self._row(session, scope, identifier)).origin

    async def artifact_metadata(self, scope: str, identifier: str) -> list[CommandFile]:
        async with self.database.write_lock, self.database.sessions() as session:
            await self._scope(session, scope, writing=False)
            row = await self._row(session, scope, identifier)
            return [
                CommandFile.model_validate(value)
                for value in cast(list[JsonValue], (row.result or {}).get("artifacts", []))
            ]

    async def result_snapshot(
        self, scope: str, identifier: str
    ) -> tuple[CommandView, bytes, bytes, list[CommandFile]]:
        async with self.database.write_lock, self.database.sessions.begin() as session:
            await self._writer(session)
            await self._scope(session, scope, writing=False)
            row = await self._row(session, scope, identifier)
            self._expire(session, row)
            if row.result and (
                hashlib.sha256(row.stdout).hexdigest() != row.result["stdout_sha256"]
                or hashlib.sha256(row.stderr).hexdigest() != row.result["stderr_sha256"]
            ):
                raise ServiceError("execution_record_corrupt", "命令输出校验失败", 409)
            files = [
                CommandFile.model_validate(value)
                for value in cast(list[JsonValue], (row.result or {}).get("artifacts", []))
            ]
            return self._view(row), row.stdout, row.stderr, files

    async def artifact(self, scope: str, identifier: str, path: str) -> SandboxFile:
        async with self.database.write_lock, self.database.sessions() as session:
            await self._scope(session, scope, writing=False)
            row = await self._row(session, scope, identifier)
            wanted = next(
                (
                    CommandFile.model_validate(value)
                    for value in cast(list[JsonValue], (row.result or {}).get("artifacts", []))
                    if isinstance(value, dict) and value.get("path") == path
                ),
                None,
            )
            item = await session.get(CommandFileRow, (identifier, "artifact", path))
            if wanted is None or item is None:
                raise ServiceError("execution_file_not_found", "原命令没有该产物", 404)
            value = SandboxFile(item.content, item.mode)
            if metadata(path, value) != wanted or item.sha256 != wanted.sha256:
                raise ServiceError("execution_record_corrupt", "命令产物校验失败", 409)
            return value

    async def files(
        self, scope: str, identifier: str, kind: Literal["input", "artifact"]
    ) -> dict[str, SandboxFile]:
        if kind not in {"input", "artifact"}:
            raise ServiceError("execution_file_kind", "命令文件来源无效", 422)
        async with self.database.write_lock, self.database.sessions() as session:
            await self._scope(session, scope, writing=False)
            row = await self._row(session, scope, identifier)
            saved = self._snapshot(row)
            expected = (
                saved.inputs
                if kind == "input"
                else [
                    CommandFile.model_validate(item)
                    for item in cast(list[JsonValue], (row.result or {}).get("artifacts", []))
                ]
            )
            values = list(
                await session.scalars(
                    select(CommandFileRow).where(
                        CommandFileRow.command_id == identifier, CommandFileRow.kind == kind
                    )
                )
            )
            actual = {item.path: SandboxFile(item.content, item.mode) for item in values}
            if any(
                hashlib.sha256(item.content).hexdigest() != item.sha256 for item in values
            ) or sorted(item.model_dump_json() for item in expected) != sorted(
                metadata(path, value).model_dump_json() for path, value in actual.items()
            ):
                raise ServiceError("execution_record_corrupt", "命令私有文件校验失败", 409)
            return actual

    async def directory_catalog(self, scope: str, identifier: str) -> CommandDirectoryCatalog:
        """可信宿主冷读取原目录事实；旧记录能力缺失不冒充空目录集合。"""
        async with self.database.write_lock, self.database.sessions() as session:
            await self._scope(session, scope, writing=False)
            row = await self._row(session, scope, identifier)
            snapshot = self._snapshot(row)
            try:
                raw_files = (row.result or {}).get("artifacts", [])
                if not isinstance(raw_files, list):
                    raise ValueError("artifacts")
                values = [CommandFile.model_validate(item) for item in raw_files]
                files = {item.path for item in values}
                if len(files) != len(values) or len(values) != (row.result or {}).get(
                    "artifact_files", 0
                ):
                    raise ValueError("artifacts")
            except (ValidationError, ValueError, TypeError):
                raise ServiceError(
                    "execution_record_corrupt", "命令文件清单校验失败", 409
                ) from None
            manifest = read_directory_manifest(row.result, files, snapshot.limits)
            return CommandDirectoryCatalog(
                command_id=identifier,
                recorded=manifest is not None,
                complete=manifest is not None
                and manifest.complete
                and row.phase == "finished"
                and row.started
                and (row.result or {}).get("status") in {"completed", "failed", "output_limit"},
                entries=manifest.entries if manifest is not None else [],
                source_hash=digest({"request": row.request_hash, "result": row.result})
                if row.result is not None
                else None,
            )

    async def events(
        self, scope: str, identifier: str, *, after: int = 0, limit: int = 100
    ) -> list[dict[str, JsonValue]]:
        if after < 0 or not 1 <= limit <= 100:
            raise ServiceError("execution_page", "命令事件分页无效", 422)
        async with self.database.write_lock, self.database.sessions() as session:
            await self._scope(session, scope, writing=False)
            await self._row(session, scope, identifier)
            rows = await session.scalars(
                select(CommandEventRow)
                .where(CommandEventRow.command_id == identifier, CommandEventRow.sequence > after)
                .order_by(CommandEventRow.sequence)
                .limit(limit)
            )
            return [
                {"sequence": row.sequence, "kind": row.kind, "payload": row.payload} for row in rows
            ]
