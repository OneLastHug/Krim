"""Host-only project secret lifecycle; existing operation/process rows remain owner.

Packages are sealed in the existing one-shot queue. Only a constructor-fixed
transport sees an anonymous stdin FD at the current authorization fence. No
plaintext is retained in request/argv/env/logs or regular project files.
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import subprocess
import tempfile
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import BinaryIO

from krim_agent.account_mcp.contracts import SecretVault
from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.development.contracts import CommandRequest
from krim_agent.development.ports import ProcessRecord, RuntimeExecution
from krim_agent.project_access.delivery import SealedDeliveryQueue
from krim_agent.project_access.process_secrets import CommandSecretBinding
from krim_agent.project_access.secrets import DeliveryBinding, SecretMaterial
from krim_agent.project_access.service import ProjectAccessService, command_intent

StartWriter = Callable[[CommandSecretBinding, BinaryIO], subprocess.Popen[bytes]]
StopExecution = Callable[[CommandSecretBinding], Awaitable[None]]
Workspace = Callable[[CommandRequest], Awaitable[str]]


def docker_secret_writer(
    binding: CommandSecretBinding, source: BinaryIO,
) -> subprocess.Popen[bytes]:
    """Fixed Docker stdin writer. No shell, credentials in argv, or inherited env."""
    from krim_agent.project_access.process_bootstrap import SECRET_WRITE_CODE

    return subprocess.Popen(
        ("/usr/bin/docker", "--host=unix:///var/run/docker.sock", "exec", "--interactive",
         "--user=65532:65532", binding.execution_id, "/usr/bin/python3", "-I", "-c",
         SECRET_WRITE_CODE),
        stdin=source, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        env={"PATH": "/usr/bin:/bin", "HOME": "/nonexistent", "LC_ALL": "C"},
        start_new_session=True, close_fds=True,
    )


@dataclass
class _Slot:
    binding: CommandSecretBinding
    delivery: DeliveryBinding
    writer: subprocess.Popen[bytes] | None = None
    queued: bool = False
    released: bool = False


class CommandSecretRouter:
    """Bounded volatile handoff, not an admission/recovery ledger.

    register happens only after the existing process reserve returned execute.
    A fresh router cannot resurrect old queue items, and slot reuse is refused.
    """

    def __init__(self, vault: SecretVault, *, start: StartWriter = docker_secret_writer) -> None:
        self._queue = SealedDeliveryQueue(vault, capacity=32, max_bytes=262144, ttl=10)
        self._start = start
        self._slots: dict[str, _Slot] = {}
        self._closed = False

    def register(self, binding: CommandSecretBinding) -> None:
        if self._closed or binding.process_id in self._slots or len(self._slots) >= 32:
            raise ServiceError("permission_denied", 403)
        # The queue's secret_id is a private bundle key, not a project secret
        # lookup. Put every revision/process/execution into its authenticated
        # context rather than binding just the first member's revision.
        key = "bundle." + fingerprint({
            "revisions": [[key, revision] for key, revision in binding.revisions],
            "process": binding.process_id,
            "execution": binding.execution_id,
        })
        delivery = DeliveryBinding(
            command_intent(binding.request, workspace_id=binding.workspace_id),
            binding.request.authorization, key, 1,
        )
        self._slots[binding.process_id] = _Slot(binding, delivery)

    def _slot(self, binding: CommandSecretBinding) -> _Slot:
        slot = self._slots.get(binding.process_id)
        if self._closed or slot is None or slot.binding != binding:
            raise ServiceError("permission_denied", 403)
        return slot

    def deliver_command(
        self, binding: CommandSecretBinding, materials: tuple[SecretMaterial, ...], *,
        expires_at: float,
    ) -> None:
        from krim_agent.project_access.process_bootstrap import secret_token

        slot = self._slot(binding)
        if slot.queued or slot.released or len(materials) != len(binding.revisions):
            raise ServiceError("permission_denied", 403)
        # This bounded serialization goes only into the encrypted queue.
        package = json.dumps({
            "token": secret_token(binding.request, binding.process_id),
            "refs": list(binding.request.authorization.secret_refs),
            "expires_at": expires_at,
            "values": [base64.b64encode(m.borrow()).decode("ascii") for m in materials],
        }, separators=(",", ":")).encode()
        try:
            self._queue.deliver(slot.delivery, SecretMaterial(package))
            slot.queued = True
        finally:
            del package

    def release_command(self, binding: CommandSecretBinding, check: Callable[[], None]) -> None:
        slot = self._slot(binding)
        if not slot.queued or slot.released:
            raise ServiceError("permission_denied", 403)
        # Mark consumed BEFORE decrypt/spawn; no failure permits another try.
        slot.released = True
        router = self

        class Sink:
            def deliver(self, actual: DeliveryBinding, material: SecretMaterial) -> None:
                if actual != slot.delivery:
                    raise ServiceError("permission_denied", 403)
                # Anonymous/unlinked OS temp file, never a project pathname.
                # memfd is preferred; fallback TemporaryFile unlinks on Linux.
                if hasattr(os, "memfd_create"):
                    file = os.fdopen(os.memfd_create("krim-secret", os.MFD_CLOEXEC), "w+b")
                else:
                    file = tempfile.TemporaryFile(mode="w+b")
                with file:
                    file.write(material.borrow())
                    file.seek(0)
                    check()
                    slot.writer = router._start(binding, file)

        self._queue.release(slot.delivery, Sink())

    async def wait_writer(self, binding: CommandSecretBinding) -> None:
        slot = self._slot(binding)
        if slot.writer is None:
            raise ServiceError("execution_error", 500)
        await asyncio.wait_for(asyncio.to_thread(slot.writer.wait, 10), 11)
        if slot.writer.returncode != 0:
            raise ServiceError("execution_error", 500)

    def discard(self, binding: CommandSecretBinding) -> None:
        slot = self._slots.get(binding.process_id)
        if slot is None:
            return
        if slot.binding != binding:
            raise ServiceError("permission_denied", 403)
        # Kill/reap only our exact host writer. The caller stops the complete
        # execution namespace separately before declaring cleanup successful.
        if slot.writer is not None:
            if slot.writer.poll() is None:
                slot.writer.kill()
            slot.writer.wait(timeout=5)
        # A queue item is discarded by exact operation (not broad project revoke).
        self._queue.discard(slot.delivery)
        self._slots.pop(binding.process_id, None)

    def close(self) -> None:
        self._closed = True
        failed = False
        try:
            for slot in tuple(self._slots.values()):
                try:
                    self.discard(slot.binding)
                except Exception:
                    failed = True
        finally:
            self._queue.close()
        if failed:
            raise ServiceError("execution_error", 500)


class CommandSecretLifecycle:
    """Terminal hook: new execution only, watcher cleanup uses host authority.

    I's stop callback must stop/reap the exact execution_id with all immutable
    bindings, never use a revoked actor as cleanup authority. Polling is a
    fallback; committed management changes should call invalidate immediately.
    """

    def __init__(
        self, access: ProjectAccessService, router: CommandSecretRouter, *,
        workspace: Workspace, stop: StopExecution, interval: float = .1,
    ) -> None:
        if not .02 <= interval <= 5:
            raise ValueError("invalid secret watcher interval")
        self.access, self.router = access, router
        self.workspace, self.stop, self.interval = workspace, stop, interval
        self._active: dict[str, CommandSecretBinding] = {}
        self._watchers: dict[str, asyncio.Task[None]] = {}
        self._retiring: set[str] = set()
        self._closed = False

    async def launched(self, record: ProcessRecord, execution: RuntimeExecution) -> None:
        request = record.request
        if not request.authorization.secret_refs:
            return
        if self._closed or record.handle.process_id in self._active:
            raise ServiceError("permission_denied", 403)
        record.handle.require_binding(request)
        if (record.handle.state != "accepted" or record.execution_id != execution.execution_id
                or execution.process_id != record.handle.process_id
                or execution.operation_id != request.operation_id
                or execution.environment.model_dump(exclude={"state"})
                != request.environment.model_dump(exclude={"state"})):
            raise ServiceError("permission_denied", 403)
        workspace = await self.workspace(request)
        intent = command_intent(request, workspace_id=workspace)
        receipt = await self.access.resolve(request.authorization, intent)
        if self._closed or record.handle.process_id in self._active:
            raise ServiceError("permission_denied", 403)
        revisions = dict(receipt.secret_revisions)
        binding = CommandSecretBinding(
            request, workspace, record.handle.process_id, execution.execution_id,
            tuple((key, revisions[key]) for key in request.authorization.secret_refs),
        )
        self._active[binding.process_id] = binding
        try:
            self.router.register(binding)
            await self.access.deliver_command_secrets(binding)
            await self.access.release_command_secrets(binding)
            await self.router.wait_writer(binding)
            # Writer's completion never substitutes for current authorization.
            await self.access.validate_command_secrets(binding)
        except BaseException:
            # Terminal's prepare fence is still held here. Do not await a
            # cleanup callback that reacquires it. Retain the exact binding and
            # retry host cleanup in the background after this launch unwinds.
            self._watchers[binding.process_id] = asyncio.create_task(
                self._watch(binding, failed=True)
            )
            raise
        self._watchers[binding.process_id] = asyncio.create_task(self._watch(binding))

    async def _watch(self, binding: CommandSecretBinding, *, failed: bool = False) -> None:
        try:
            while binding.process_id in self._active:
                await asyncio.sleep(self.interval)
                try:
                    if failed or self._closed or binding.process_id in self._retiring:
                        raise ServiceError("permission_denied", 403)
                    await asyncio.wait_for(self.access.validate_command_secrets(binding), 5)
                except Exception:
                    failed = True
                    try:
                        await self._retire(binding, stop=True)
                        return
                    except Exception:
                        # Stop is not confirmed. Preserve/retry the exact unit;
                        # never advertise successful cleanup or re-deliver.
                        continue
        except asyncio.CancelledError:
            raise

    async def _retire(self, binding: CommandSecretBinding, *, stop: bool) -> None:
        # Revoke the pending transport even when execution stop fails. Keep
        # lifecycle registration until BOTH cleanup steps are confirmed.
        self._retiring.add(binding.process_id)
        failed = False
        try:
            self.router.discard(binding)
        except Exception:
            failed = True
        if stop:
            try:
                await self.stop(binding)
            except Exception:
                failed = True
        if failed:
            raise ServiceError("execution_error", 500)
        self._active.pop(binding.process_id, None)
        self._retiring.discard(binding.process_id)
        watcher = self._watchers.pop(binding.process_id, None)
        if watcher is not None and watcher is not asyncio.current_task():
            # A settled TerminalBackend.cancel may be a child task awaited by
            # this watcher. Joining it here would create a parent/child cycle.
            watcher.cancel()

    def tracks(self, record: ProcessRecord) -> bool:
        """Only an exact live in-memory watcher permits continued Secret use."""
        binding = self._active.get(record.handle.process_id)
        watcher = self._watchers.get(record.handle.process_id)
        return (
            not self._closed and record.handle.process_id not in self._retiring
            and binding is not None and binding.request == record.request
            and binding.execution_id == record.execution_id
            and watcher is not None and not watcher.done()
        )

    async def finished(self, record: ProcessRecord) -> None:
        binding = self._active.get(record.handle.process_id)
        if binding is not None:
            if (record.execution_id != binding.execution_id or record.request != binding.request
                    or record.handle.state not in {"exited", "cancelled", "failed"}):
                raise ServiceError("permission_denied", 403)
            await self._retire(binding, stop=False)

    async def invalidate(self, owner_id: str, project_id: str) -> None:
        failed = False
        for binding in tuple(self._active.values()):
            if (binding.request.environment.owner_id, binding.request.environment.project_id) == (
                owner_id, project_id,
            ):
                try:
                    await self._retire(binding, stop=True)
                except Exception:
                    failed = True
        if failed:
            raise ServiceError("execution_error", 500)

    async def close(self) -> None:
        self._closed = True
        failed = False
        for binding in tuple(self._active.values()):
            try:
                await self._retire(binding, stop=True)
            except Exception:
                failed = True
        try:
            self.router.close()
        except Exception:
            failed = True
        if failed:
            raise ServiceError("execution_error", 500)
