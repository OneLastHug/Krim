"""C0 ProcessBackend over I's repository, B's runtime and a trusted host worker."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Coroutine, Iterable, Iterator
from dataclasses import replace
from typing import Any, Protocol, TypeVar

from krim_agent.contracts import ServiceError
from krim_agent.development.contracts import (
    CommandRequest,
    EnvironmentHandle,
    OutputPage,
    ProcessHandle,
    RunOwnership,
)
from krim_agent.development.ports import (
    ExecutionBackend,
    ProcessRecord,
    ProcessRepository,
    RuntimeExecution,
    RuntimeStatus,
    command_digest,
)

from .client import WorkerClient, WorkerError

Authorize = Callable[[CommandRequest, str], Awaitable[None]]
T = TypeVar("T")
TERMINAL = {"exited", "cancelled", "failed"}
STDIN_CHUNK_BYTES = 8192


def stdin_chunks(data: str, limit: int = STDIN_CHUNK_BYTES) -> Iterator[str]:
    """Split input into worker RPCs of at most ``limit`` encoded bytes.

    Cuts only step back over UTF-8 continuation bytes, so every chunk decodes
    cleanly and the concatenation reproduces the input exactly. The worker keeps
    its own 65536-byte per-RPC cap; this small default keeps each control
    message far below the supervisor MAX_MESSAGE envelope.
    """
    if type(limit) is not int or not 4 <= limit <= STDIN_CHUNK_BYTES:
        raise ValueError("invalid stdin chunk window")
    encoded = data.encode()
    if len(encoded) <= limit:
        yield data
        return
    begin = 0
    while begin < len(encoded):
        end = min(begin + limit, len(encoded))
        if end < len(encoded):
            while (encoded[end] & 0b11000000) == 0b10000000:
                end -= 1
        yield encoded[begin:end].decode()
        begin = end


class SecretLifecycle(Protocol):
    """Host-only hook; no secret values in shared/persistent contracts."""

    async def launched(self, record: ProcessRecord, execution: RuntimeExecution) -> None: ...
    async def finished(self, record: ProcessRecord) -> None: ...
    def tracks(self, record: ProcessRecord) -> bool: ...


class TerminalBackend:
    """Authorization is injected by I; matching DTOs are never authorization.

    B enforces process lifetime inside its execution unit, including when this
    API process is gone. I/B must persist stop fences before calling stop_run or
    stop_environment; this instance's lock only serializes its own admissions.
    """

    def __init__(
        self, repository: ProcessRepository, executions: ExecutionBackend,
        worker: WorkerClient, *, authorize: Authorize,
        secrets: SecretLifecycle | None = None,
    ) -> None:
        self.repository = repository
        self.executions = executions
        self.worker = worker
        self.authorize = authorize
        self.secrets = secrets
        self._launch_lock = asyncio.Lock()
        self._locks: dict[str, asyncio.Lock] = {}
        self._input_locks: dict[str, asyncio.Lock] = {}
        self._tasks: set[asyncio.Task[Any]] = set()

    def _lock(self, process_id: str) -> asyncio.Lock:
        return self._locks.setdefault(process_id, asyncio.Lock())

    async def _settled(self, operation: Coroutine[Any, Any, T]) -> T:
        # A cancelled HTTP/tool wait must not leave an accepted launch or
        # requested cancellation halfway through its durable handoff.
        task = asyncio.create_task(operation)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        cancelled = False
        while True:
            try:
                result = await asyncio.shield(task)
                break
            except asyncio.CancelledError:
                if task.cancelled():
                    raise
                cancelled = True
                if task.done():
                    result = task.result()
                    break
        if cancelled:
            raise asyncio.CancelledError
        return result

    async def _settled_result(
        self, operation: Coroutine[Any, Any, T],
    ) -> tuple[T, bool]:
        """Finish a durable launch and report caller cancellation to its owner."""
        task = asyncio.create_task(operation)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        cancelled = False
        while True:
            try:
                return await asyncio.shield(task), cancelled
            except asyncio.CancelledError:
                if task.cancelled():
                    raise
                cancelled = True
                if task.done():
                    return task.result(), cancelled

    @staticmethod
    def _execution(record: ProcessRecord) -> RuntimeExecution:
        if record.execution_id is None:
            raise ValueError("execution identity is missing")
        return RuntimeExecution(
            execution_id=record.execution_id, environment=record.request.environment,
            operation_id=record.request.operation_id, process_id=record.handle.process_id,
            attach_argv=(), pty=record.request.pty,
        )

    async def _bound(self, process: ProcessHandle, action: str) -> ProcessRecord:
        record = await self.repository.get(process.process_id)
        record.handle.require_binding(process)
        record.handle.require_binding(record.request)
        if record.request_digest != command_digest(record.request):
            raise ValueError("stored command binding changed")
        await self.authorize(record.request, action)
        return record

    @staticmethod
    def _check_runtime(status: RuntimeStatus, execution: RuntimeExecution) -> None:
        if status.execution_id != execution.execution_id:
            raise ValueError("runtime execution binding changed")
        if status.state not in {"created", "running", "exited", "unknown"}:
            raise ValueError("invalid runtime state")

    async def _save_state(
        self, record: ProcessRecord, state: str, exit_code: int | None,
    ) -> ProcessRecord:
        for _ in range(8):
            if record.handle.state in TERMINAL:
                return record
            if record.handle.state == "unknown" and state in {"accepted", "running"}:
                return record
            values = record.handle.model_dump()
            values.update(state=state, exit_code=exit_code)
            handle = ProcessHandle.model_validate(values)
            if handle == record.handle:
                return record
            updated = replace(record, handle=handle, revision=record.revision + 1)
            try:
                return await self.repository.save(record, updated)
            except ServiceError as error:
                if error.code not in {"revision_conflict", "input_revision_conflict"}:
                    raise
                current = await self.repository.get(record.handle.process_id)
                current.handle.require_binding(record.handle)
                if (current.request_digest != record.request_digest
                        or current.execution_id != record.execution_id):
                    raise ValueError("stored process binding changed") from None
                if current.revision == record.revision:
                    raise
                if state not in TERMINAL or current.handle.state in TERMINAL:
                    # A stale observation never replaces newer state.
                    return current
                # A confirmed stop must remain durable even if B has already
                # removed the unit. Retry over a concurrent nonterminal update
                # rather than losing the only recoverable terminal receipt.
                record = current
        raise ServiceError("revision_conflict")

    async def _refresh(self, record: ProcessRecord) -> ProcessRecord:
        if record.handle.state in TERMINAL:
            return record
        if record.execution_id is None:
            return await self._save_state(record, "unknown", None)
        execution = self._execution(record)
        if record.request.authorization.secret_refs and (
            self.secrets is None or not self.secrets.tracks(record)
        ):
            # An old reservation cannot reconstruct the delivered revisions.
            # Recovery therefore stops the exact unit instead of retaining an
            # unmonitored Secret process or calling launch/delivery again.
            status = await self.executions.cancel(execution)
            self._check_runtime(status, execution)
            if status.state != "exited":
                return await self._save_state(record, "unknown", None)
            await self._close_attachment(record)
            record = await self._save_state(record, "cancelled", status.exit_code)
            if self.secrets is not None:
                await self.secrets.finished(record)
            return record
        status = await self.executions.inspect(execution)
        self._check_runtime(status, execution)
        if status.state == "exited":
            if status.exit_code is None:
                return await self._save_state(record, "unknown", None)
            return await self._save_state(record, "exited", status.exit_code)
        if status.state == "running":
            return await self._save_state(record, "running", None)
        # Includes created: admission happened, but launch outcome is not
        # recoverable. Do not run attach_argv again or call prepare again.
        return await self._save_state(record, "unknown", None)

    async def submit(
        self, request: CommandRequest, *,
        stdin_chunks: Iterable[str] | None = None,
    ) -> ProcessHandle:
        if request.authorization.secret_refs and (
            self.secrets is None or request.timeout_seconds is None
        ):
            raise ServiceError("permission_denied", 403)
        await self.authorize(request, "submit")
        if stdin_chunks is None:
            record, _ = await self._settled(self._admit_and_attach(request))
        else:
            (record, execute), cancelled = await self._settled_result(
                self._admit_and_attach(request),
            )
            if cancelled:
                if execute:
                    await self._settled(self._abort_initial_input(record, cancelled=True))
                raise asyncio.CancelledError
            if execute and record.handle.state == "running":
                try:
                    await self._send_initial_input(record, stdin_chunks)
                    async with self._lock(record.handle.process_id):
                        current = await self._bound(record.handle, "inspect")
                        record = await self._refresh(current)
                except asyncio.CancelledError:
                    await self._settled(self._abort_initial_input(record, cancelled=True))
                    raise
                except Exception:
                    await self._settled(self._abort_initial_input(record, cancelled=False))
                    raise
        if request.wait_seconds and record.handle.state not in TERMINAL:
            try:
                async with asyncio.timeout(request.wait_seconds):
                    while record.handle.state not in TERMINAL:
                        await asyncio.sleep(0.025)
                        handle = await self.inspect(record.handle)
                        record = replace(record, handle=handle)
            except TimeoutError:
                pass
        return record.handle

    async def _admit_and_attach(
        self, request: CommandRequest,
    ) -> tuple[ProcessRecord, bool]:
        async with self._launch_lock:
            reservation = await self.repository.reserve(request, command_digest(request))
            record = reservation.record
            record.handle.require_binding(request)
            if record.request_digest != command_digest(request):
                raise ValueError("reservation request binding changed")
            async with self._lock(record.handle.process_id):
                if not reservation.execute:
                    return await self._refresh(record), False
                if record.handle.state != "accepted" or record.execution_id is not None:
                    raise ValueError("only fresh accepted reservations may launch")
                execution: RuntimeExecution | None = None
                try:
                    async with self.executions.prepare(request, record.handle.process_id) as target:
                        # Validate all immutable fields before saving or spawning.
                        if (target.environment.model_dump(exclude={"state"})
                                != request.environment.model_dump(exclude={"state"})
                                or target.operation_id != request.operation_id
                                or target.process_id != record.handle.process_id
                                or target.pty != request.pty or not target.attach_argv):
                            raise ValueError("runtime launch binding changed")
                        execution = target
                        record = await self.repository.save(
                            record, replace(
                                record, execution_id=target.execution_id,
                                revision=record.revision + 1,
                            ),
                        )
                        local = await self.worker.attach(request, record.handle, target)
                        if local.get("state") not in {"running", "exited"}:
                            raise WorkerError("attachment_launch_unknown")
                        if request.authorization.secret_refs:
                            if self.secrets is None:
                                raise ServiceError("permission_denied", 403)
                            await self.secrets.launched(record, target)
                        # The worker durably owns the spawned CLI before the
                        # project fence is released. B.inspect takes that same
                        # fence, so runtime confirmation must happen outside.
                    async with asyncio.timeout(5):
                        while True:
                            status = await self.executions.inspect(target)
                            self._check_runtime(status, target)
                            if status.state in {"running", "exited"}:
                                break
                            if status.state == "unknown":
                                raise WorkerError("runtime_launch_unknown")
                            await asyncio.sleep(0.02)
                    state = "running"
                    if status.state == "exited":
                        state = "exited" if status.exit_code is not None else "unknown"
                    return await self._save_state(
                        record, state, status.exit_code if state == "exited" else None,
                    ), True
                except Exception as error:
                    # The immutable admission is never undone. Best-effort
                    # cleanup can prove cancellation; otherwise retain unknown.
                    record = await self.repository.get(record.handle.process_id)
                    stopped = False
                    exit_code = None
                    if execution is not None:
                        try:
                            # Even a save conflict must retire the unit B just
                            # created, using the validated descriptor in hand.
                            # Never cancel a different persisted execution.
                            cleanup = replace(execution, attach_argv=())
                            status = await self.executions.cancel(cleanup)
                            self._check_runtime(status, cleanup)
                            if status.state == "exited" and record.execution_id in {
                                None, execution.execution_id,
                            }:
                                stopped = True
                                exit_code = status.exit_code
                                if record.execution_id == execution.execution_id:
                                    await self._close_attachment(record)
                        except Exception:
                            pass
                    record = await self._save_state(
                        record, "failed" if stopped else "unknown", exit_code,
                    )
                    if stopped and self.secrets is not None:
                        await self.secrets.finished(record)
                    if isinstance(error, ValueError) or (
                        isinstance(error, ServiceError) and error.code not in {
                            "revision_conflict", "input_revision_conflict",
                        }
                    ):
                        raise
                    return record, True

    async def _send_initial_input(
        self, record: ProcessRecord, source: Iterable[str],
    ) -> None:
        """Send one fresh command's input only after its launch is confirmed."""
        async with self._input_locks.setdefault(record.handle.process_id, asyncio.Lock()):
            def bounded_chunks() -> Iterator[str]:
                for text in source:
                    yield from stdin_chunks(text)

            chunks = iter(bounded_chunks())
            current = next(chunks, None)
            if current is None:
                await self._send_initial_input_chunk(record, "", close=True)
                return

            following = next(chunks, None)
            while True:
                await self._send_initial_input_chunk(
                    record, current, close=following is None,
                )
                if following is None:
                    break
                current = following
                following = next(chunks, None)

    async def _send_initial_input_chunk(
        self, record: ProcessRecord, data: str, *, close: bool,
    ) -> None:
        async with self._lock(record.handle.process_id):
            current_record = await self._bound(record.handle, "write_stdin")
            current_record = await self._refresh(current_record)
            if current_record.handle.state != "running":
                raise ValueError("process is not running")
            reply = await self.worker.invoke(self.worker.control(
                current_record.request, current_record.handle, "stdin",
                data=data, close=close,
            ))
            if reply.get("state") == "unknown":
                raise WorkerError("terminal_worker_failed")

    async def _abort_initial_input(
        self, record: ProcessRecord, *, cancelled: bool,
    ) -> ProcessRecord:
        """Stop the exact newly admitted execution after an incomplete handoff."""
        async with self._launch_lock:
            async with self._lock(record.handle.process_id):
                current = await self.repository.get(record.handle.process_id)
                current.handle.require_binding(record.handle)
                if current.execution_id != record.execution_id:
                    return current
                if current.handle.state in TERMINAL:
                    return current
                if current.execution_id is None:
                    return await self._save_state(current, "unknown", None)

                execution = self._execution(current)
                try:
                    status = await self.executions.cancel(execution)
                    self._check_runtime(status, execution)
                except Exception:
                    return await self._save_state(current, "unknown", None)
                if status.state != "exited":
                    return await self._save_state(current, "unknown", None)

                await self._close_attachment(current)
                state = "cancelled" if cancelled else "failed"
                current = await self._save_state(current, state, status.exit_code)
                if self.secrets is not None:
                    await self.secrets.finished(current)
                return current

    async def inspect(self, process: ProcessHandle) -> ProcessHandle:
        async with self._lock(process.process_id):
            record = await self._bound(process, "inspect")
            record = await self._refresh(record)
            await self._release_finished(record)
            return record.handle

    async def _release_finished(self, record: ProcessRecord) -> None:
        if record.handle.state not in TERMINAL or record.execution_id is None:
            return
        try:
            local = await self.worker.inspect(record.request, record.handle)
            if local.get("state") not in TERMINAL | {"unknown"}:
                return  # Preserve the live attachment until output is drained.
        except (WorkerError, OSError, TimeoutError):
            pass
        # Terminal status is durable before removing the runtime resource.
        # B.release must be idempotent; the host log is retained independently.
        await self.executions.release(self._execution(record))
        if self.secrets is not None:
            await self.secrets.finished(record)

    async def read_output(self, process: ProcessHandle, cursor: int) -> OutputPage:
        if cursor < 0:
            raise ValueError("negative output cursor")
        record = await self._bound(process, "read_output")
        reply = await self.worker.invoke(
            self.worker.control(record.request, record.handle, "output", cursor=cursor),
        )
        if reply.get("state") == "unknown" and reply.get("reason") == "receipt_missing":
            raise WorkerError("terminal_log_unavailable")
        page = OutputPage.model_validate(reply)
        if page.process_id != process.process_id or page.cursor != cursor:
            raise ValueError("output binding changed")
        return page

    async def write_stdin(
        self, process: ProcessHandle, data: str, *, close: bool = False,
    ) -> None:
        # Total input length is unbounded; it streams as independent bound and
        # authorized RPCs. A revoked grant, a stopped unit, a lost worker
        # receipt, or one failed chunk ends the stream without resending that
        # chunk, and only the final RPC carries close. The lock is re-acquired
        # per chunk so cancellation and stop stay responsive while a large
        # stream is in flight.
        # Serialize separate input calls without holding the cancellation lock
        # across the full stream; chunks from concurrent callers must not mix.
        async with self._input_locks.setdefault(process.process_id, asyncio.Lock()):
            chunks = iter(stdin_chunks(data))
            current = next(chunks)
            while True:
                following = next(chunks, None)
                final = close and following is None
                async with self._lock(process.process_id):
                    record = await self._bound(process, "write_stdin")
                    record = await self._refresh(record)
                    if record.handle.state != "running":
                        raise ValueError("process is not running")
                    reply = await self.worker.invoke(self.worker.control(
                        record.request, record.handle, "stdin", data=current, close=final,
                    ))
                    if reply.get("state") == "unknown":
                        raise WorkerError("terminal_worker_failed")
                if following is None:
                    break
                current = following

    async def _close_attachment(self, record: ProcessRecord) -> None:
        try:
            await self.worker.invoke(self.worker.control(record.request, record.handle, "cancel"))
        except (WorkerError, OSError, TimeoutError):
            # B already proved the complete execution stopped. Lost host
            # attachment/logs do not invalidate that runtime result.
            pass

    async def _cancel(self, process: ProcessHandle, *, action: str = "cancel") -> ProcessHandle:
        async with self._lock(process.process_id):
            record = await self._bound(process, action)
            if record.handle.state in TERMINAL:
                return record.handle
            if record.execution_id is None:
                return (await self._save_state(record, "unknown", None)).handle
            execution = self._execution(record)
            status = await self.executions.cancel(execution)
            self._check_runtime(status, execution)
            if status.state != "exited":
                return (await self._save_state(record, "unknown", None)).handle
            await self._close_attachment(record)
            record = await self._save_state(record, "cancelled", status.exit_code)
            if self.secrets is not None:
                await self.secrets.finished(record)
            return record.handle

    async def cancel(self, process: ProcessHandle) -> ProcessHandle:
        result: ProcessHandle = await self._settled(self._cancel(process))
        return result

    async def stop_run(self, ownership: RunOwnership) -> None:
        await self._settled(self._stop(ownership=ownership))

    async def stop_environment(self, environment: EnvironmentHandle) -> None:
        await self._settled(self._stop(environment=environment))

    async def _stop(
        self, *, ownership: RunOwnership | None = None,
        environment: EnvironmentHandle | None = None,
    ) -> None:
        async with self._launch_lock:
            if ownership is not None:
                records = await self.repository.list_run(ownership)
                if any(item.handle.ownership != ownership for item in records):
                    raise ValueError("repository returned a different run")
                selected = [item for item in records if item.handle.lifecycle == "run"]
                action = "stop_run"
            else:
                assert environment is not None
                selected = list(await self.repository.list_environment(environment))
                if any(item.handle.environment.model_dump(exclude={"state"})
                       != environment.model_dump(exclude={"state"}) for item in selected):
                    raise ValueError("repository returned a different environment")
                action = "stop_environment"
            # Attempt every cleanup even if one fails; report failures rather
            # than treating a partial environment stop as complete.
            results = await asyncio.gather(*(
                self._cancel(item.handle, action=action) for item in selected
            ), return_exceptions=True)
            errors = [item for item in results if isinstance(item, Exception)]
            if errors:
                raise ExceptionGroup("terminal cleanup failed", errors)
            if any(isinstance(item, ProcessHandle) and item.state == "unknown" for item in results):
                raise WorkerError("terminal_cleanup_unknown")
