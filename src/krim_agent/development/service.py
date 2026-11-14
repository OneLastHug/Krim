"""I composition for persistent projects, B runtime, C terminal and D authorization."""

from __future__ import annotations

import asyncio
import hashlib
import os
import stat
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

from pydantic import BaseModel, Field, JsonValue
from sqlalchemy import select

from krim_agent.accounts.service import IdentityService
from krim_agent.agent_sdk.host import CURRENT_OPERATION
from krim_agent.contracts import ServiceError, StrictModel, encoded, parse_json
from krim_agent.dev_environment.backend import DevelopmentBackend
from krim_agent.dev_environment.execution import ContainerExecutions
from krim_agent.dev_environment.locking import ProjectLocks
from krim_agent.dev_terminal.backend import TerminalBackend
from krim_agent.dev_terminal.client import WorkerClient
from krim_agent.dev_terminal.host import HostWorkerConnection
from krim_agent.development.configuration import DevelopmentConfiguration
from krim_agent.development.contracts import (
    AuthorizationRef,
    CommandRequest,
    EnvironmentHandle,
    OperationIdentity,
    RunOwnership,
)
from krim_agent.development.ports import EnvironmentRecord
from krim_agent.project_access.service import ProjectAccessService, command_intent
from krim_agent.project_access.types import ProjectGrant
from krim_agent.runtime.contracts import ExecutionBudget
from krim_agent.runtime.sdk_loop import DevelopmentRunScope
from krim_agent.sandbox.bindings import read_binding
from krim_agent.sandbox.service import SandboxService
from krim_agent.security import PrincipalContext
from krim_agent.storage.development import DevelopmentStore
from krim_agent.storage.development_models import development_environments, project_access_records
from krim_agent.storage.project_access import SQLiteAccessRepository
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.tools.core import Decision, Invocation, ToolSpec
from krim_agent.tools.pipeline import CURRENT, Scope
from krim_agent.tools.registry import ToolRegistry

_HOST_CLEANUP: ContextVar[bool] = ContextVar("development_host_cleanup", default=False)


@dataclass(frozen=True)
class DevelopmentScope:
    actor: PrincipalContext
    workspace_id: str
    environment: EnvironmentHandle
    ownership: RunOwnership
    operations: DevelopmentService
    check: Any
    execution_budget: ExecutionBudget
    invocation: Invocation | None = None

    @property
    def deadline(self) -> float:
        return self.execution_budget.deadline


class ShellInput(StrictModel):
    command: str = Field(min_length=1, max_length=32768)
    cwd: str = "."
    pty: bool = False
    lifecycle: Literal["run", "project_service"] = "run"
    timeout_seconds: float | None = Field(default=60.0, gt=0, le=86400)
    wait_seconds: float = Field(default=1.0, ge=0, le=20)


class ProcessInput(StrictModel):
    process_id: str = Field(min_length=1, max_length=128)


class OutputInput(ProcessInput):
    cursor: int = Field(default=0, ge=0)


class StdinInput(ProcessInput):
    data: str = Field(max_length=65536)
    close: bool = False


class ReadInput(StrictModel):
    path: str = Field(min_length=1, max_length=512)
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=32768, ge=1, le=65536)


class ListInput(StrictModel):
    path: str = "."
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=100, ge=1, le=100)


class NoSecretDelivery:
    def deliver(self, binding: Any, material: Any) -> None:
        raise ServiceError("permission_denied")


class NoSecretVault:
    def decrypt(self, context: Any, ciphertext: str) -> bytes:
        raise ServiceError("permission_denied")

    def encrypt(self, context: Any, plaintext: bytes) -> str:
        raise ServiceError("permission_denied")


class DevelopmentService:
    def __init__(
        self,
        store: SQLiteStore,
        identities: IdentityService,
        sandbox: SandboxService,
        configuration: DevelopmentConfiguration,
    ) -> None:
        self.store, self.identities, self.sandbox, self.configuration = (
            store,
            identities,
            sandbox,
            configuration,
        )
        self.repositories = DevelopmentStore(store)
        self.access_repository = SQLiteAccessRepository(store)
        self.access = ProjectAccessService(
            self.access_repository,
            NoSecretVault(),
            secret_sink=NoSecretDelivery(),
            git_sink=NoSecretDelivery(),
        )
        private = configuration.private_state_root
        self.locks = ProjectLocks(private / "locks")
        self.environments = DevelopmentBackend(
            self.repositories.environments,
            self.locks,
            self.authorize_environment,
            processes=self.repositories.processes,
        )
        self.executions = ContainerExecutions(self.environments, self.repositories.processes)
        self.worker = WorkerClient(HostWorkerConnection(private / "supervisor"))
        self.terminal = TerminalBackend(
            self.repositories.processes,
            self.executions,
            self.worker,
            authorize=self.authorize_terminal,
        )
        self._worker_process: asyncio.subprocess.Process | None = None

    @staticmethod
    def prepare_private(configuration: DevelopmentConfiguration) -> None:
        root = configuration.private_state_root
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if root.resolve() != root:
            raise ServiceError("sandbox_config_invalid")
        for path in (root, root / "locks", root / "supervisor", root / "worker-placeholder"):
            path.mkdir(mode=0o700, exist_ok=True)
            info = path.lstat()
            if (
                not stat.S_ISDIR(info.st_mode)
                or info.st_uid != os.geteuid()
                or info.st_mode & 0o077
            ):
                raise ServiceError("sandbox_config_invalid")

    async def start_supervisor(self) -> None:
        directory = self.configuration.private_state_root / "supervisor"
        # Connect to an already-running trusted worker without launching anything.
        try:
            reader, writer = await asyncio.open_unix_connection(str(directory / "control.sock"))
            writer.close()
            await writer.wait_closed()
            return
        except OSError:
            pass
        self._worker_process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "krim_agent.dev_terminal.worker",
            "serve",
            "--state",
            str(directory),
            "--project",
            str(self.configuration.private_state_root / "worker-placeholder"),
            "--trusted-attachments",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            start_new_session=True,
        )
        async with asyncio.timeout(20):
            while True:
                if self._worker_process.returncode is not None:
                    raise ServiceError("sandbox_backend_unavailable")
                try:
                    _, writer = await asyncio.open_unix_connection(str(directory / "control.sock"))
                    writer.close()
                    await writer.wait_closed()
                    return
                except OSError:
                    await asyncio.sleep(0.05)

    async def close(self) -> None:
        # A persistent supervisor and project services are owned by deployment,
        # not by a single API process. Closing this adapter never restarts jobs.
        return

    @contextmanager
    def host_cleanup(self) -> Iterator[None]:
        token = _HOST_CLEANUP.set(True)
        try:
            yield
        finally:
            _HOST_CLEANUP.reset(token)

    async def authorize_environment(self, environment: EnvironmentHandle, action: str) -> None:
        handle = environment
        record = await self.repositories.environments.resolve(handle)
        if _HOST_CLEANUP.get() and action == "stop":
            return
        actor = self.identities.resolve(handle.owner_id)
        project = await self.sandbox.project(actor, handle.project_id, write=action != "inspect")
        if project["principal_id"] != record.environment.owner_id:
            raise ServiceError("permission_denied")
        if action == "execute" and record.environment.state != "ready":
            raise ServiceError("workspace_busy")

    async def authorize_terminal(self, request: CommandRequest, action: str) -> None:
        if _HOST_CLEANUP.get() and action in {"stop_run", "stop_environment", "cancel"}:
            await self.repositories.environments.resolve(request.environment)
            return
        actor = self.identities.resolve(request.authorization.actor_id)
        if actor.generation != request.authorization.actor_generation:
            raise ServiceError("permission_denied")
        project = await self.sandbox.project(
            actor,
            request.environment.project_id,
            write=action in {"submit", "write_stdin", "cancel"},
        )
        if project["principal_id"] != request.ownership.owner_id:
            raise ServiceError("permission_denied")
        if action == "submit":
            snapshot = await self.store.snapshot(request.ownership.run_id)
            if snapshot.run.stop_requested or snapshot.run.status != "running":
                raise ServiceError("cancelled")
            await self.access.resolve(
                request.authorization, command_intent(request, workspace_id=project["workspace_id"])
            )

    async def prepare_project(
        self, actor: PrincipalContext, project_id: str, *, request_key: str
    ) -> EnvironmentHandle:
        project = await self.sandbox.project(actor, project_id, write=True)
        if project["status"] != "ready":
            await self.sandbox.provision(actor, project_id=project_id, request_key=request_key)
            project = await self.sandbox.project(actor, project_id, write=True)
        handle = EnvironmentHandle(
            owner_id=actor.principal_id,
            project_id=project_id,
            environment_id="dev-" + project_id,
            generation=1,
            state="creating",
        )
        async with self.store.lock, self.store.engine.connect() as conn:
            current = (await conn.execute(select(development_environments.c.record).where(
                development_environments.c.project_id == project_id,
                development_environments.c.owner_id == actor.principal_id,
            ))).scalar_one_or_none()
        if current is not None:
            handle = EnvironmentHandle.model_validate(current["environment"])
        try:
            registered = await self.repositories.environments.resolve(handle)
        except ServiceError as error:
            if error.code != "not_found":
                raise
            content = (
                self.configuration.broker_environments_root
                / project["environment_id"]
                / "projects"
                / project_id
                / "content"
            )
            info = await asyncio.to_thread(content.lstat)
            record = EnvironmentRecord(
                handle,
                self.configuration.image_id,
                content,
                info.st_dev,
                info.st_ino,
                cpu_cores=self.configuration.cpu_cores,
                memory_bytes=self.configuration.memory_bytes,
                pids_limit=self.configuration.pids_limit,
                max_processes=self.configuration.max_processes,
            )
            registered = await self.repositories.environments.register(record)
        return await self.environments.start(registered.environment)

    async def grant_project(
        self,
        actor: PrincipalContext,
        project_id: str,
        *,
        expected_revision: int,
        expires_at: float,
        requires_approval: bool,
    ) -> ProjectGrant:
        project = await self.sandbox.project(actor, project_id, write=True)
        actor.owns(project["workspace_id"], project["principal_id"], write=True, approve=True)
        if not time.time() < expires_at <= time.time() + 86400:
            raise ServiceError("invalid_input", 422)
        grant = ProjectGrant(
            owner_id=project["principal_id"],
            workspace_id=project["workspace_id"],
            project_id=project_id,
            actor_id=actor.principal_id,
            actor_generation=actor.generation,
            revision=expected_revision + 1,
            actions=("shell.execute",),
            requires_approval=requires_approval,
            expires_at=expires_at,
        )
        await self.access_repository.put(grant, expected_revision=expected_revision)
        return grant

    async def scope(self, run_id: str, check: Any, budget: ExecutionBudget) -> DevelopmentRunScope:
        snapshot = await self.store.snapshot(run_id)
        context = snapshot.session_context
        if context is None:
            raise ServiceError("workspace_contract_required")
        async with self.store.lock, self.store.engine.connect() as conn:
            from krim_agent.storage.models import sessions

            owner = await conn.scalar(
                select(sessions.c.owner_id).where(sessions.c.id == context.session_id)
            )
        actor = self.identities.resolve(str(owner))
        binding = await read_binding(self.sandbox, actor, context.session_id)
        if binding["project_id"] is None:
            raise ServiceError("workspace_contract_required")
        async with self.store.lock, self.store.engine.connect() as conn:
            raw = (
                await conn.execute(
                    select(development_environments.c.record).where(
                        development_environments.c.project_id == binding["project_id"],
                        development_environments.c.owner_id == actor.principal_id,
                    )
                )
            ).scalar_one_or_none()
        if raw is None:
            raise ServiceError("sandbox_backend_unavailable")
        environment = EnvironmentHandle.model_validate(raw["environment"])
        record = await self.repositories.environments.resolve(environment)
        if record.environment.state != "ready":
            raise ServiceError("sandbox_backend_unavailable")
        ownership = RunOwnership(
            owner_id=actor.principal_id, session_id=context.session_id, run_id=run_id
        )

        def fence() -> None:
            check()
            self.identities.validate(actor)

        scope = DevelopmentScope(
            actor, context.workspace_id, environment, ownership, self, fence, budget
        )
        return DevelopmentRunScope(environment, ownership, cast(Scope, scope))

    def current(self, invocation: Invocation) -> DevelopmentScope:
        scope = CURRENT.get()
        if (
            not isinstance(scope, DevelopmentScope)
            or scope.operations is not self
            or scope.invocation != invocation
        ):
            raise ServiceError("permission_denied")
        scope.check()
        return scope

    async def _grant(self, scope: DevelopmentScope) -> ProjectGrant:
        async with self.store.lock, self.store.engine.connect() as conn:
            row = (
                await conn.execute(
                    select(project_access_records.c.document).where(
                        project_access_records.c.project_id == scope.environment.project_id,
                        project_access_records.c.owner_id == scope.actor.principal_id,
                        project_access_records.c.kind == "grant",
                        project_access_records.c.resource_id == scope.actor.principal_id,
                    )
                )
            ).scalar_one_or_none()
        if row is None:
            raise ServiceError("permission_denied")
        grant = ProjectGrant.model_validate_json(encoded(row))
        if (
            grant.revoked
            or grant.expires_at <= time.time()
            or grant.actor_generation != scope.actor.generation
            or "shell.execute" not in grant.actions
        ):
            raise ServiceError("permission_denied")
        return grant

    async def policy(self, invocation: Invocation) -> Decision:
        scope = self.current(invocation)
        if invocation.name in {"dev_read", "dev_list", "dev_process", "dev_output", "dev_cancel"}:
            return "allow"
        grant = await self._grant(scope)
        return "ask" if grant.requires_approval else "allow"

    async def _command(
        self,
        scope: DevelopmentScope,
        values: ShellInput,
        identity: OperationIdentity,
        approval_id: str | None,
    ) -> CommandRequest:
        ref = AuthorizationRef(
            actor_id=scope.actor.principal_id,
            actor_generation=scope.actor.generation,
            project_id=scope.environment.project_id,
            approval_id=approval_id,
            arguments_digest="0" * 64,
        )
        request = CommandRequest(
            operation_id=identity.operation_id,
            environment=scope.environment,
            ownership=scope.ownership,
            authorization=ref,
            **values.model_dump(),
        )
        intent = command_intent(request, workspace_id=scope.workspace_id)
        return request.model_copy(
            update={
                "authorization": ref.model_copy(update={"arguments_digest": intent.digest(ref)})
            }
        )

    async def approval_projection(
        self, invocation: Invocation, request: dict[str, JsonValue]
    ) -> dict[str, JsonValue]:
        if invocation.name != "dev_shell":
            return request
        saved = await self.store.read_checkpoint(invocation.run_id)
        if saved is None:
            raise ServiceError("facts_corrupt")
        checkpoint, _ = saved
        entries = checkpoint.get("entries")
        if not isinstance(entries, dict):
            raise ServiceError("facts_corrupt")
        entry = next(
            (
                value
                for value in entries.values()
                if value["invocation"]["call_id"] == invocation.call_id
            ),
            None,
        )
        if entry is None or entry["identity"] is None:
            raise ServiceError("facts_corrupt")
        identity = OperationIdentity.model_validate(entry["identity"])
        scope = (
            await self.scope(
                invocation.run_id,
                lambda: None,
                ExecutionBudget(asyncio.get_running_loop().time() + 30),
            )
        ).tools
        assert isinstance(scope, DevelopmentScope)
        values = ShellInput.model_validate(parse_json(invocation.arguments_json))
        command = await self._command(scope, values, identity, cast(str, request["request_id"]))
        intent = command_intent(command, workspace_id=scope.workspace_id)
        return {
            **request,
            "project_access": {
                "actor_id": scope.actor.principal_id,
                "actor_generation": scope.actor.generation,
                "owner_id": scope.ownership.owner_id,
                "workspace_id": scope.workspace_id,
                "project_id": scope.environment.project_id,
                "operation_id": identity.operation_id,
                "action": intent.action,
                "arguments_digest": command.authorization.arguments_digest,
            },
        }

    async def execute(
        self, invocation: Invocation, parameters: BaseModel, context: Any
    ) -> JsonValue:
        scope = self.current(invocation)
        if isinstance(parameters, ShellInput):
            identity = CURRENT_OPERATION.get()
            if identity is None or identity.call_id != invocation.call_id:
                raise ServiceError("facts_corrupt")
            snapshot = await self.store.snapshot(invocation.run_id)
            approval = next(
                (
                    event.data["request_id"]
                    for event in snapshot.events
                    if event.kind == "approval_request"
                    and event.data.get("call_id") == invocation.call_id
                ),
                None,
            )
            request = await self._command(scope, parameters, identity, cast(str | None, approval))
            await self.repositories.operations.bind_command(identity, request)
            return cast(JsonValue, (await self.terminal.submit(request)).model_dump(mode="json"))
        if isinstance(parameters, ProcessInput):
            record = await self.repositories.processes.get(parameters.process_id)
            if (
                record.handle.environment.owner_id != scope.actor.principal_id
                or record.handle.environment.project_id != scope.environment.project_id
            ):
                raise ServiceError("permission_denied")
            result: BaseModel
            if isinstance(parameters, OutputInput):
                result = await self.terminal.read_output(record.handle, parameters.cursor)
            elif isinstance(parameters, StdinInput):
                await self._grant(scope)
                await self.terminal.write_stdin(
                    record.handle, parameters.data, close=parameters.close
                )
                return {"accepted": True}
            elif invocation.name == "dev_cancel":
                result = await self.terminal.cancel(record.handle)
            else:
                result = await self.terminal.inspect(record.handle)
            return cast(JsonValue, result.model_dump(mode="json"))
        if isinstance(parameters, ReadInput):
            return cast(
                JsonValue,
                await self.sandbox.read_file(
                    scope.actor, scope.environment.project_id, **parameters.model_dump()
                ),
            )
        if isinstance(parameters, ListInput):
            return cast(
                JsonValue,
                await self.sandbox.directory(
                    scope.actor, scope.environment.project_id, **parameters.model_dump()
                ),
            )
        raise ServiceError("invalid_input")

    async def observe(self, invocation: Invocation) -> dict[str, JsonValue] | None:
        return None

    async def cleanup_run(self, run_id: str) -> None:
        snapshot = await self.store.snapshot(run_id)
        if snapshot.session_context is None:
            return
        async with self.store.lock, self.store.engine.connect() as conn:
            from krim_agent.storage.models import sessions

            owner = await conn.scalar(
                select(sessions.c.owner_id).where(
                    sessions.c.id == snapshot.session_context.session_id
                )
            )
        ownership = RunOwnership(
            owner_id=str(owner), session_id=snapshot.session_context.session_id, run_id=run_id
        )
        with self.host_cleanup():
            await self.terminal.stop_run(ownership)

    async def revoke_owner(self, owner_id: str) -> None:
        async with self.store.lock, self.store.engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        select(development_environments.c.record).where(
                            development_environments.c.owner_id == owner_id,
                        )
                    )
                )
                .scalars()
                .all()
            )
        with self.host_cleanup():
            for raw in rows:
                await self.environments.stop(EnvironmentHandle.model_validate(raw["environment"]))

    def registry(self) -> ToolRegistry:
        async def unavailable(_parameters: BaseModel) -> JsonValue:
            raise ServiceError("workspace_contract_required")

        digest = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        specs: list[tuple[str, type[BaseModel], str, str]] = [
            (
                "dev_shell",
                ShellInput,
                "command",
                "Run project Shell commands; project_service retains explicit services.",
            ),
            ("dev_process", ProcessInput, "read", "Read process status without restarting it."),
            ("dev_output", OutputInput, "read", "Read bounded process output using a byte cursor."),
            ("dev_stdin", StdinInput, "write", "Send terminal input or close stdin."),
            ("dev_cancel", ProcessInput, "write", "Stop the complete execution unit."),
            ("dev_read", ReadInput, "read", "Read a bounded project file."),
            ("dev_list", ListInput, "read", "List a project directory."),
        ]
        return ToolRegistry(
            tuple(
                ToolSpec(
                    name,
                    model,
                    unavailable,
                    {"type": "object"},
                    "development/1",
                    digest,
                    risk="exec"
                    if capability == "command"
                    else "write"
                    if capability == "write"
                    else "read",
                    scoped_executor=self.execute,
                    capability=capability,
                    description=description,
                )
                for name, model, capability, description in specs
            ),
            max_tools=32,
        )
