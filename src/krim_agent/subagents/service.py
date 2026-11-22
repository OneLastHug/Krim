"""Host-owned background agents, durable mailboxes and root coordination.

Model requests identify goals and members. Authority always comes from the live
RunService producer, persisted session ownership and the current project scope.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, cast
from weakref import WeakValueDictionary

from ai import events
from ai import messages as sdk
from pydantic import JsonValue
from sqlalchemy import func, select

from krim_agent.agent_sdk.model import host_messages
from krim_agent.config import minimum_limit
from krim_agent.contracts import Message, RunRequest, ServiceError, encoded, fingerprint
from krim_agent.storage.subagent_models import (
    subagent_activations,
    subagent_child_runs,
    subagent_members,
    subagent_tasks,
    subagent_trees,
)
from krim_agent.tools.core import Invocation

from .budgets import RequestBudgets
from .configuration import (
    SubagentProfileConfiguration,
    SubagentsConfiguration,
    resolve_worker_profile,
)
from .context import fork_context, fresh_context
from .errors import SubagentError
from .mailbox import MailboxWakeups
from .providers.base import (
    ProviderRegistration,
    ProviderRegistry,
    ProviderUnavailable,
    UnsupportedCapability,
)
from .scheduling import RequestLimiter
from .storage import SubagentStore
from .tool_models import ControlAgent, ListAgents, SendAgentMessage, SpawnAgent, WaitAgent
from .types import (
    ACTIVE_TASK_STATUS,
    AcceptSpawn,
    CommitInput,
    ReserveAgent,
    SendMessage,
    SettleResult,
    StartTask,
    TreeScope,
)

_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class Caller:
    member: dict[str, Any]
    tree: dict[str, Any]
    scope: Any


class SubagentService:
    def __init__(
        self,
        store: Any,
        configuration: SubagentsConfiguration,
        *,
        session_tools: Any = None,
    ) -> None:
        self.store = store
        self.repository = SubagentStore(store)
        self.budgets = RequestBudgets(store)
        self.configuration = configuration
        self.session_tools = session_tools
        self.runtime: Any = None
        self.providers = ProviderRegistry()
        self.requests = RequestLimiter(configuration.max_model_requests)
        self.processes = RequestLimiter(configuration.max_tool_processes)
        self.wakeups = MailboxWakeups()
        self._locks: WeakValueDictionary[str, asyncio.Lock] = WeakValueDictionary()
        self._residency_gate = asyncio.Lock()
        self._delivery_gate = asyncio.Lock()
        self._registrations: dict[tuple[str, str], ProviderRegistration] = {}
        self._profiles: dict[str, SubagentProfileConfiguration] = {}
        self._activations: dict[str, ProviderRegistration] = {}
        self._launches: dict[str, asyncio.Task[Any]] = {}
        self._disposals: dict[str, asyncio.Task[dict[str, Any]]] = {}
        self._process_debts: dict[tuple[str, str], Any] = {}
        self._orphan_process_debts: set[tuple[str, str]] = set()
        self._closed = False
        self._faulted = False
        self.event_sink: Any = None
        self.workspace: Any = None
        self.sandbox_service: Any = None
        self.authorize_session: Any = None

    def attach_runtime(self, runtime: Any, session_tools: Any) -> None:
        if self.runtime is not None and self.runtime is not runtime:
            raise ValueError("subagent runtime is already attached")
        self.runtime, self.session_tools = runtime, session_tools

    def register_profile(
        self,
        name: str,
        provider: Any,
        profile: SubagentProfileConfiguration,
        *,
        family: str = "target-http",
    ) -> None:
        route = "route-" + fingerprint({"profile": name, "family": family})[:32]
        registration = self.providers.register(
            route,
            provider,
            version=provider.version,
            capabilities=provider.capabilities,
        )
        self._registrations[name, family] = registration
        self._profiles[name] = profile.model_copy(deep=True)

    def _lock(self, tree_id: str) -> asyncio.Lock:
        lock = self._locks.get(tree_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[tree_id] = lock
        return lock

    def _health(self) -> None:
        if not self.store.healthy or self._faulted:
            raise ServiceError("storage_unavailable", 503)
        if self._closed:
            raise ServiceError("closed", 503)

    def _enabled(self) -> None:
        self._health()
        if not self.configuration.enabled:
            raise SubagentError(
                "subagent_capability",
                accepted=False,
                action="Delegation is disabled in the host configuration.",
            )

    async def member_for_run(self, run_id: str) -> dict[str, Any] | None:
        async def read(conn: Any) -> str | None:
            child = await conn.scalar(
                select(subagent_child_runs.c.agent_id).where(
                    subagent_child_runs.c.run_id == run_id,
                )
            )
            if child is not None:
                return str(child)
            root = await conn.scalar(
                select(subagent_members.c.id)
                .join(
                    subagent_trees,
                    subagent_members.c.tree_id == subagent_trees.c.id,
                )
                .where(subagent_trees.c.root_run_id == run_id, subagent_members.c.role == "root")
            )
            return str(root) if root is not None else None

        identity = await self.store.work_read(read)
        return await self.repository.get_agent(identity) if identity is not None else None

    async def _root_live(self, tree: dict[str, Any]) -> None:
        if tree["status"] != "open":
            raise SubagentError("subagent_tree_closed", accepted=False)
        root_id = tree.get("root_run_id")
        if root_id is None:
            raise ServiceError("facts_corrupt")
        root = await self.store.snapshot(root_id)
        if root.run.stop_requested or root.run.status not in {"queued", "running", "awaiting"}:
            raise SubagentError(
                "subagent_tree_closed", accepted=False, action="The root task is no longer active."
            )

    async def _caller(self, invocation: Invocation) -> Caller:
        self._enabled()
        if self.runtime is None or self.session_tools is None:
            raise ServiceError("execution_error")
        scope = await self.session_tools.resolve(invocation)
        if self.runtime.caller_run() != invocation.run_id:
            raise ServiceError("permission_denied")
        member = await self.member_for_run(invocation.run_id)
        if member is None:
            host_scope = TreeScope(
                session_id=scope.session_id,
                turn_id=scope.turn_id,
                run_id=invocation.run_id,
                workspace_id=scope.workspace_id,
                owner_id=scope.actor.principal_id,
                project_id=scope.project_id,
                request_key="root:" + invocation.run_id,
            )
            async with self._lock("session:" + scope.session_id):

                async def latest(conn: Any) -> str | None:
                    project = (
                        subagent_trees.c.project_id == scope.project_id
                        if scope.project_id is not None
                        else subagent_trees.c.project_id.is_(None)
                    )
                    return await conn.scalar(
                        select(subagent_trees.c.id)
                        .where(
                            subagent_trees.c.session_id == scope.session_id,
                            project,
                        )
                        .order_by(subagent_trees.c.created_at.desc())
                        .limit(1)
                    )

                prior_id = await self.store.work_read(latest)
                if prior_id is not None:
                    tree = await self.repository.tree(prior_id)
                    if tree["root_run_id"] != invocation.run_id:
                        tree = await self.repository.bind_root(
                            host_scope,
                            prior_id,
                            expected_input_version=tree["input_version"],
                        )
                else:
                    tree = await self.repository.ensure_tree(host_scope)
            snapshot = await self.store.snapshot(invocation.run_id)
            limits = snapshot.target.get("limits", {})
            configured_steps = limits.get("max_steps") if isinstance(limits, dict) else None
            initial_spent = sum(event.kind == "model_start" for event in snapshot.events)
            await self.budgets.ensure(
                tree["tree_id"],
                minimum_limit(
                    configured_steps if type(configured_steps) is int else None,
                    self.configuration.max_requests_per_tree,
                ),
                initial_spent=initial_spent,
            )

            async def root_member(conn: Any) -> str | None:
                return await conn.scalar(
                    select(subagent_members.c.id).where(
                        subagent_members.c.tree_id == tree["tree_id"],
                        subagent_members.c.role == "root",
                    )
                )

            root_member_id = await self.store.work_read(root_member)
            if root_member_id is None:
                reservation = await self.repository.reserve_agent(
                    ReserveAgent(
                        tree_id=tree["tree_id"],
                        path="lead",
                        role="root",
                        provider="sdk_session",
                        profile="root",
                        provider_version=fingerprint(snapshot.target),
                        profile_version="root/1",
                        request_key="root:" + invocation.run_id,
                    )
                )
                root_member_id = reservation["agent"]["agent_id"]
            member = await self.repository.get_agent(root_member_id)
            activation = member.get("activation")
            if activation is not None and activation["status"] == "starting":
                try:
                    await self.repository.transition_activation(
                        activation["activation_id"],
                        expected_status="starting",
                        status="resident",
                    )
                except ServiceError as error:
                    if error.code != "revision_conflict":
                        raise
        tree = await self.repository.tree(member["tree_id"])
        if (
            tree["owner_id"] != scope.actor.principal_id
            or tree["session_id"] != scope.session_id
            or tree["workspace_id"] != scope.workspace_id
            or tree["project_id"] != scope.project_id
        ):
            raise ServiceError("permission_denied")
        scope.check()
        await self._root_live(tree)
        return Caller(member, tree, scope)

    async def _target(self, caller: Caller, target: str) -> dict[str, Any]:
        async def read(conn: Any) -> str | None:
            found = await conn.scalar(
                select(subagent_members.c.id).where(
                    subagent_members.c.tree_id == caller.tree["tree_id"],
                    (subagent_members.c.id == target) | (subagent_members.c.path == target),
                )
            )
            return str(found) if found is not None else None

        identity = await self.store.work_read(read)
        if identity is None:
            raise ServiceError("not_found", 404)
        return await self.repository.get_agent(identity)

    def _profile(
        self,
        name: str | None,
        *,
        family: str = "target-http",
    ) -> tuple[str, SubagentProfileConfiguration, ProviderRegistration]:
        selected = name or self.configuration.default_profile or "default"
        try:
            profile = (
                resolve_worker_profile(self.configuration, None)
                if name is None
                else self._profiles[selected]
            )
            registered = self._registrations[selected, family]
            entry = self.providers.resolve(registered.name)
        except (KeyError, ProviderUnavailable):
            raise SubagentError(
                "provider_not_configured",
                accepted=False,
                action="Select a configured worker profile.",
            ) from None
        return selected, profile, entry

    async def _depth_limit(self, member: dict[str, Any]) -> int:
        if member["role"] == "root":
            return self.configuration.max_depth
        task_id = member.get("current_task_id")
        if task_id is None:
            return member["depth"]
        task = await self.repository.task(task_id)
        value = task["input"].get("depth_limit", member["depth"])
        return min(self.configuration.max_depth, value) if type(value) is int else member["depth"]

    async def _fork_source(self, run_id: str) -> list[Message]:
        snapshot = await self.store.snapshot(run_id)
        context = snapshot.session_context
        prepared = getattr(context, "prepared", None)
        if prepared is not None:
            return list(prepared.messages)
        legacy = getattr(context, "context", None)
        if legacy is not None:
            return list(legacy.messages)
        saved = await self.store.read_checkpoint(run_id)
        if saved is not None:
            checkpoint, digest = saved
            if fingerprint(checkpoint) != digest:
                raise ServiceError("facts_corrupt")
            return host_messages(
                [sdk.Message.model_validate(value) for value in checkpoint.get("messages", [])]
            )
        return list(getattr(context, "messages", ()))

    async def spawn(self, invocation: Invocation, request: SpawnAgent) -> dict[str, Any]:
        caller = await self._caller(invocation)
        spawn_key = "spawn:" + fingerprint({"run": invocation.run_id, "call": invocation.call_id})
        intent = fingerprint(request.model_dump(mode="json"))
        prior = await self.repository._find_member_by_key(caller.tree["tree_id"], spawn_key)
        if prior is not None:
            receipt = await self.repository._spawn_task(prior["agent"]["agent_id"])
            if receipt is not None:
                if receipt["task"]["input"].get("request_fingerprint") != intent:
                    raise ServiceError("key_conflict")
                return {
                    "accepted": True,
                    "agent_id": receipt["agent_id"],
                    "path": prior["agent"]["path"],
                    "activation_id": receipt["activation_id"],
                    "task_id": receipt["task_id"],
                    "message_id": receipt["message_id"],
                    "seq": receipt["seq"],
                    "scheduled": receipt["task"]["status"] in ACTIVE_TASK_STATUS,
                    "delivery": "background",
                    "created": False,
                }
        root = await self.store.snapshot(caller.tree["root_run_id"])
        key, profile, registration = self._profile(request.profile, family=root.request.target)
        try:
            registration.capabilities.require(
                fork=request.context == "fork",
                structured_output=profile.structured_output,
                workspace_isolation=profile.workspace == "isolated_write",
            )
        except UnsupportedCapability as error:
            raise SubagentError(
                "subagent_capability",
                accepted=False,
                action="Unsupported capability: " + error.capability,
            ) from None
        depth_limit = await self._depth_limit(caller.member)
        depth = caller.member["depth"] + 1
        if depth > depth_limit:
            raise SubagentError("subagent_depth_exceeded", accepted=False)
        size = len((request.task + request.background).encode("utf-8"))
        if size > self.configuration.max_message_bytes:
            raise SubagentError("input_limit", accepted=False)
        name = request.name or "worker-" + fingerprint(invocation.call_id)[:8]
        path = caller.member["path"] + "/" + name
        binding = registration.provider.descriptor
        system = str(binding.get("system_prompt", ""))
        if request.context == "fork":
            snapshot = fork_context(
                await self._fork_source(invocation.run_id),
                request.task,
                source_run_id=invocation.run_id,
                system_prompt=system,
                prefix_end=request.prefix_end,
                max_bytes=profile.context_max_bytes,
                max_messages=profile.context_max_messages,
            )
        else:
            snapshot = fresh_context(
                request.task,
                system_prompt=system,
                background=request.background,
                max_bytes=profile.context_max_bytes,
            )
        history = [message.model_dump(mode="json") for message in snapshot.messages[1:-1]]
        payload: dict[str, Any] = {
            "text": snapshot.messages[-1].content,
            "history": history,
            "snapshot": snapshot.record(),
            "profile_snapshot": profile.model_dump(mode="json"),
            "depth_limit": min(depth_limit, depth + profile.max_depth),
            "request_fingerprint": intent,
        }
        async with self._lock(caller.tree["tree_id"]), self._residency_gate:
            caller.scope.check()
            await self._root_live(await self.repository.tree(caller.tree["tree_id"]))
            if prior is None:
                await self._check_residency()
            reservation = await self.repository.reserve_agent(
                ReserveAgent(
                    tree_id=caller.tree["tree_id"],
                    parent_id=caller.member["agent_id"],
                    path=path,
                    provider=registration.name,
                    profile=key,
                    provider_version=registration.version,
                    profile_version=fingerprint(profile.model_dump(mode="json")),
                    max_depth=depth_limit,
                    resident_limit=self.configuration.max_resident_agents,
                    request_key="spawn:"
                    + fingerprint({"run": invocation.run_id, "call": invocation.call_id}),
                )
            )
        member, activation = reservation["agent"], reservation["activation"]
        if activation is None:
            raise ServiceError("facts_corrupt")
        accepted = False
        budget_key = "spawn:" + fingerprint({"run": invocation.run_id, "call": invocation.call_id})
        budget_reserved = False
        try:
            # Provider and workspace preparation never hold a tree/database lock.
            self.providers.assert_current(registration)
            if profile.provider == "process" or profile.workspace == "isolated_write":
                from .workspace import SandboxWorkspaceReader, WorkspaceIsolation

                if self.sandbox_service is None or caller.tree["project_id"] is None:
                    raise SubagentError(
                        "subagent_capability",
                        accepted=False,
                        action="A process worker requires the current project sandbox.",
                    )
                assignment, baseline = await WorkspaceIsolation(
                    SandboxWorkspaceReader(
                        self.sandbox_service,
                        caller.scope.actor,
                    )
                ).assign(
                    profile.workspace,
                    source_project_id=caller.tree["project_id"],
                    agent_id=member["agent_id"],
                    input_version=caller.tree["input_version"],
                )
                if baseline.truncated:
                    raise SubagentError(
                        "input_limit",
                        accepted=False,
                        action="The isolated workspace baseline exceeds configured bounds.",
                    )
                payload["workspace"] = assignment.model_dump(mode="json")
                payload["baseline"] = baseline.model_dump(mode="json")
                if profile.provider == "process":
                    from .providers.process import build_task_document

                    payload["text"] = build_task_document(str(payload["text"]), assignment)
            parent_key = None
            if caller.member.get("current_task_id") is not None:
                parent_task = await self.repository.task(caller.member["current_task_id"])
                parent_key = parent_task["input"].get("budget_key")
            await self.budgets.reserve(
                caller.tree["tree_id"], budget_key, None, parent_key=parent_key
            )
            budget_reserved = True
            payload["budget_key"] = budget_key
            async with self._lock(caller.tree["tree_id"]):
                caller.scope.check()
                receipt = await self.repository.accept_spawn(
                    AcceptSpawn(
                        agent_id=member["agent_id"],
                        activation_id=activation["activation_id"],
                        input=cast(dict[str, JsonValue], payload),
                        parent_run_id=invocation.run_id,
                    )
                )
                accepted = True
                self._activations[activation["activation_id"]] = registration
                task = receipt["task"]
                launch = (
                    self._schedule_start(
                        member["agent_id"], task, activation, registration, caller.tree
                    )
                    if task["status"] == "accepted"
                    else None
                )
            if launch is not None:
                await asyncio.shield(launch)
            await self.wakeups.publish(caller.member["agent_id"])
            return {
                "accepted": True,
                "agent_id": member["agent_id"],
                "path": path,
                "activation_id": activation["activation_id"],
                "task_id": receipt["task_id"],
                "message_id": receipt["message_id"],
                "seq": receipt["seq"],
                "scheduled": task["status"] in ACTIVE_TASK_STATUS,
                "delivery": "background",
            }
        except BaseException:
            if not accepted and budget_reserved and self.store.healthy:
                await asyncio.shield(self.budgets.release(caller.tree["tree_id"], budget_key))
            if not accepted and reservation["created"]:
                await asyncio.shield(
                    self.repository.rollback_reservation(
                        member["agent_id"],
                        activation["activation_id"],
                    )
                )
            raise

    def _schedule_start(
        self,
        agent_id: str,
        task: dict[str, Any],
        activation: dict[str, Any],
        registration: ProviderRegistration,
        tree: dict[str, Any],
    ) -> asyncio.Task[Any]:
        existing = self._launches.get(task["task_id"])
        if existing is not None:
            return existing

        async def launch() -> Any:
            try:
                await self._root_live(await self.repository.tree(tree["tree_id"]))
                current = await self.repository.get_agent(agent_id)
                active = current.get("activation")
                if active is None or active["activation_id"] != activation["activation_id"]:
                    raise SubagentError("subagent_activation_closed", accepted=True)
                if active["status"] == "starting":
                    await self.repository.transition_activation(
                        active["activation_id"],
                        expected_status="starting",
                        status="resident",
                    )
                return await self._start(agent_id, task, active, registration, tree)
            except BaseException as error:
                if self.store.healthy and await self.repository.result(task["task_id"]) is None:
                    code = error.code if isinstance(error, ServiceError) else "execution_error"
                    await self.repository.settle_result(
                        SettleResult(
                            task_id=task["task_id"],
                            stop_reason=(
                                "aborted" if isinstance(error, asyncio.CancelledError) else "error"
                            ),
                            code=code,
                            output={"text": "", "reason": "Accepted startup failed."},
                        )
                    )
                    budget_key = task["input"].get("budget_key")
                    if isinstance(budget_key, str):
                        await self.budgets.release(tree["tree_id"], budget_key)
                if isinstance(error, asyncio.CancelledError):
                    raise
                return None
            finally:
                parent = (await self.repository.get_agent(agent_id))["parent_id"]
                if parent is not None:
                    await self.wakeups.publish(parent)

        job = asyncio.create_task(launch(), name="krim-subagent-start-" + task["task_id"])
        self._launches[task["task_id"]] = job

        def finished(done: asyncio.Task[Any]) -> None:
            if self._launches.get(task["task_id"]) is done:
                self._launches.pop(task["task_id"], None)
            if not done.cancelled():
                error = done.exception()
                if error is not None:
                    _LOG.error("Subagent startup failed: type=%s", type(error).__name__)

        job.add_done_callback(finished)
        return job

    async def _start(
        self,
        agent_id: str,
        task: dict[str, Any],
        activation: dict[str, Any],
        registration: ProviderRegistration,
        tree: dict[str, Any],
    ) -> Any:
        text = task["input"].get("text")
        if not isinstance(text, str) or not text.strip():
            raise ServiceError("facts_corrupt")
        return await registration.provider.start(
            StartTask(
                agent_id=agent_id,
                task_id=task["task_id"],
                activation_id=activation["activation_id"],
                request=RunRequest(
                    text=text, request_key="ac-child." + task["task_id"], target="target-http"
                ),
                target=registration.provider.descriptor,
                owner_turn_id=tree["root_turn_id"],
            )
        )

    async def recover_tool(self, invocation: Invocation) -> JsonValue | None:
        """Read an accepted control receipt; never dispatch unknown work again."""
        from krim_agent.contracts import parse_json

        if invocation.name not in {"spawn_agent", "send_message"}:
            return None
        if self.session_tools is None:
            return None
        scope = await self.session_tools.by_run(invocation.run_id)
        member = await self.member_for_run(invocation.run_id)
        if member is None:
            return None
        tree = await self.repository.tree(member["tree_id"])
        if tree["session_id"] != scope.session_id or tree["owner_id"] != scope.actor.principal_id:
            raise ServiceError("permission_denied")
        arguments = parse_json(invocation.arguments_json)
        if invocation.name == "spawn_agent":
            parsed = SpawnAgent.model_validate(arguments)
            key = "spawn:" + fingerprint({"run": invocation.run_id, "call": invocation.call_id})
            reserved = await self.repository._find_member_by_key(tree["tree_id"], key)
            if reserved is None:
                return None
            receipt = await self.repository._spawn_task(reserved["agent"]["agent_id"])
            if receipt is None:
                return None
            if receipt["task"]["input"].get("request_fingerprint") != fingerprint(
                parsed.model_dump(mode="json"),
            ):
                raise ServiceError("facts_corrupt")
            return cast(
                JsonValue,
                {
                    "ok": True,
                    "accepted": True,
                    "agent_id": receipt["agent_id"],
                    "activation_id": receipt["activation_id"],
                    "path": reserved["agent"]["path"],
                    "task_id": receipt["task_id"],
                    "message_id": receipt["message_id"],
                    "seq": receipt["seq"],
                    "delivery": "background",
                    "created": False,
                },
            )
        parsed_message = SendAgentMessage.model_validate(arguments)
        target = await self._target(Caller(member, tree, scope), parsed_message.target)
        key = "message:" + fingerprint(
            {
                "sender": member["agent_id"],
                "key": parsed_message.send_key or (invocation.run_id + ":" + invocation.call_id),
            }
        )
        receipt = await self.repository.message_by_key(target["agent_id"], key)
        if receipt is None:
            return None
        if receipt["body"].get("request_fingerprint") != fingerprint(
            parsed_message.model_dump(mode="json"),
        ):
            raise ServiceError("facts_corrupt")
        return cast(
            JsonValue,
            {
                "ok": True,
                "accepted": True,
                "message_id": receipt["message_id"],
                "task_id": receipt["task_id"],
                "seq": receipt["seq"],
                "wake": receipt["wake"],
                "created": False,
            },
        )

    async def list_agents(self, invocation: Invocation, request: ListAgents) -> dict[str, Any]:
        caller = await self._caller(invocation)
        return {
            "tree_id": caller.tree["tree_id"],
            "caller_id": caller.member["agent_id"],
            "agents": await self.repository.list_agents(
                caller.tree["tree_id"],
                after_path=request.after_path,
                limit=request.limit,
            ),
        }

    def _registration(self, member: dict[str, Any], *, cold: bool = False) -> ProviderRegistration:
        activation = member.get("activation")
        if not cold and activation is not None:
            retained = self._activations.get(activation["activation_id"])
            if retained is not None:
                return retained
        try:
            entry = self.providers.resolve(member["provider"], version=member["provider_version"])
            profile = self._profiles[member["profile"]]
        except (KeyError, ProviderUnavailable):
            raise SubagentError(
                "subagent_not_recoverable",
                accepted=False,
                action="The saved provider/profile route is not available.",
            ) from None
        if fingerprint(profile.model_dump(mode="json")) != member["profile_version"]:
            raise SubagentError(
                "subagent_not_recoverable",
                accepted=False,
                action="The worker profile changed; select a new worker.",
            )
        return entry

    async def _check_residency(self) -> None:
        async def read(conn: Any) -> int:
            return int(
                await conn.scalar(
                    select(func.count())
                    .select_from(
                        subagent_activations.join(
                            subagent_members,
                            subagent_activations.c.agent_id == subagent_members.c.id,
                        ),
                    )
                    .where(
                        subagent_members.c.role == "agent",
                        subagent_activations.c.status.in_({"starting", "resident", "closing"}),
                    )
                )
            )

        if await self.store.work_read(read) >= self.configuration.max_resident_agents:
            raise SubagentError(
                "capacity",
                accepted=False,
                status=429,
                action="Host residency is full; release an idle worker first.",
            )

    async def _restore(self, member: dict[str, Any]) -> dict[str, Any]:
        registration = self._registration(member, cold=True)
        if not registration.capabilities.continuable:
            raise SubagentError("subagent_not_continuable", accepted=False)
        latest = member.get("activation")
        # Callers hold the per-tree gate. The short residency gate protects
        # admission across trees and never spans provider/network preparation.
        async with self._residency_gate:
            await self._check_residency()
            restored = await self.repository.begin_activation(
                member["agent_id"],
                provider_version=registration.version,
                expected_instance=latest["instance"] if latest else 0,
                resident_limit=self.configuration.max_resident_agents,
            )
        activation = restored.get("activation", restored)
        self._activations[activation["activation_id"]] = registration
        await self.repository.transition_activation(
            activation["activation_id"],
            expected_status="starting",
            status="resident",
        )
        return await self.repository.get_agent(member["agent_id"])

    async def _message_count(self, agent_id: str) -> int:
        from krim_agent.storage.subagent_models import subagent_messages

        async def read(conn: Any) -> int:
            return int(
                await conn.scalar(
                    select(func.count())
                    .select_from(subagent_messages)
                    .where(
                        subagent_messages.c.target_id == agent_id,
                        subagent_messages.c.checkpoint_id.is_(None),
                    )
                )
            )

        return await self.store.work_read(read)

    async def send_message(
        self, invocation: Invocation, request: SendAgentMessage
    ) -> dict[str, Any]:
        caller = await self._caller(invocation)
        target = await self._target(caller, request.target)
        if target["role"] == "root" and request.new_task:
            raise SubagentError(
                "invalid_input",
                accepted=False,
                action="Send information to the active root, not a new user task.",
            )
        if len(request.text.encode("utf-8")) > self.configuration.max_message_bytes:
            raise SubagentError("input_limit", accepted=False)
        if target["role"] != "root":
            registration = self._registration(target, cold=not target["resident"])
            if not registration.capabilities.continuable:
                raise SubagentError("subagent_not_continuable", accepted=False)
        send_key = "message:" + fingerprint(
            {
                "sender": caller.member["agent_id"],
                "key": request.send_key or (invocation.run_id + ":" + invocation.call_id),
            }
        )
        intent = fingerprint(request.model_dump(mode="json"))
        replay = await self.repository.message_by_key(target["agent_id"], send_key)
        if replay is not None:
            if (
                replay["body"].get("request_fingerprint") != intent
                or replay["sender_id"] != caller.member["agent_id"]
            ):
                raise ServiceError("key_conflict")
            return {
                "accepted": True,
                "message_id": replay["message_id"],
                "seq": replay["seq"],
                "task_id": replay["task_id"],
                "created": False,
                "wake": replay["wake"],
                "scheduled": (target.get("task") or {}).get("status") in ACTIVE_TASK_STATUS,
            }
        body: dict[str, Any] = {
            "text": request.text,
            "request_fingerprint": intent,
            "parent_run_id": invocation.run_id,
        }
        if request.new_task:
            saved = await self.repository.history(target["agent_id"])
            profile = self._profiles[target["profile"]]
            previous = saved["payload"].get("messages", []) if saved is not None else []
            snapshot = fork_context(
                [Message.model_validate(item) for item in previous],
                request.text,
                source_run_id=(target.get("task") or {}).get("run_id") or invocation.run_id,
                system_prompt=str(registration.provider.descriptor.get("system_prompt", "")),
                max_bytes=profile.context_max_bytes,
                max_messages=profile.context_max_messages,
            )
            body.update(
                history=[item.model_dump(mode="json") for item in snapshot.messages[1:-1]],
                profile_snapshot=profile.model_dump(mode="json"),
                depth_limit=await self._depth_limit(target),
                snapshot=snapshot.record(),
            )
        budget_key = "send:" + fingerprint({"target": target["agent_id"], "send_key": send_key})
        budget_reserved = False
        if request.new_task:
            parent_key = None
            if caller.member.get("current_task_id") is not None:
                parent_task = await self.repository.task(caller.member["current_task_id"])
                parent_key = parent_task["input"].get("budget_key")
            await self.budgets.reserve(
                caller.tree["tree_id"], budget_key, None, parent_key=parent_key
            )
            body["budget_key"] = budget_key
            budget_reserved = True
        accepted = False
        launch = None
        scheduled = False
        try:
            async with self._lock(caller.tree["tree_id"]):
                caller.scope.check()
                await self._root_live(await self.repository.tree(caller.tree["tree_id"]))
                target = await self.repository.get_agent(target["agent_id"])
                if (
                    await self._message_count(target["agent_id"])
                    >= self.configuration.max_queued_messages
                ):
                    raise SubagentError(
                        "capacity", accepted=False, status=429, action="The target mailbox is full."
                    )
                current = target.get("task")
                task_id = request.task_id
                if not request.new_task and target["role"] != "root":
                    if current is None or current["status"] not in ACTIVE_TASK_STATUS:
                        raise SubagentError(
                            "subagent_task_closed",
                            accepted=False,
                            action="Use new_task=true for follow-up work.",
                        )
                    if task_id is not None and task_id != current["task_id"]:
                        raise SubagentError("subagent_task_closed", accepted=False)
                    task_id = current["task_id"]
                if request.wake == "auto" and not target["resident"] and target["role"] != "root":
                    target = await self._restore(target)
                receipt = await self.repository.accept_message(
                    SendMessage(
                        tree_id=caller.tree["tree_id"],
                        target_id=target["agent_id"],
                        sender_id=caller.member["agent_id"],
                        kind="task" if request.new_task else "message",
                        wake=request.wake,
                        body=cast(dict[str, JsonValue], body),
                        send_key=send_key,
                        task_id=task_id,
                    )
                )
                accepted = True
                if request.wake == "auto" and target["role"] != "root":
                    task = await self.repository.task(receipt["task_id"])
                    if task["status"] == "accepted":
                        launch = self._schedule_start(
                            target["agent_id"],
                            task,
                            target["activation"],
                            self._registration(target),
                            caller.tree,
                        )
                        scheduled = True
                    else:
                        scheduled = task["status"] in ACTIVE_TASK_STATUS
            if launch is not None:
                await asyncio.shield(launch)
        except BaseException:
            if not accepted and budget_reserved and self.store.healthy:
                # A lost COMMIT response is verified using the original key.
                found = await self.repository.message_by_key(target["agent_id"], send_key)
                if found is None:
                    await asyncio.shield(self.budgets.release(caller.tree["tree_id"], budget_key))
            raise
        await self.wakeups.publish(target["agent_id"])
        return {
            "accepted": True,
            **receipt,
            "scheduled": scheduled,
            "wake": request.wake,
            "action": (
                "Queued only; no idle worker was started."
                if request.wake == "queue_only"
                else "Accepted for the next safe boundary."
            ),
        }

    async def wait_agent(self, invocation: Invocation, request: WaitAgent) -> dict[str, Any]:
        caller = await self._caller(invocation)

        async def read(cursor: int) -> list[dict[str, Any]]:
            current = await self.repository.get_agent(caller.member["agent_id"])
            inbox = await self.repository.inbox(
                current["agent_id"], after_seq=max(cursor, current["inbox_cursor"])
            )
            notices = await self.repository.notifications(
                caller.tree["tree_id"],
                consumer_id=current["agent_id"],
                after_seq=max(request.after_notification_seq, current["notify_cursor"]),
            )
            return [{"kind": "message", **item} for item in inbox] + [
                {"kind": "result", **item} for item in notices
            ]

        # Waiting is observational. Only the next input checkpoint consumes rows.
        rows = await self.wakeups.wait(
            caller.member["agent_id"],
            request.after_seq,
            read,
            seconds=request.seconds,
            check=caller.scope.check,
        )
        return {
            "items": [
                {
                    key: value
                    for key, value in item.items()
                    if key in {"kind", "message_id", "notification_id", "task_id", "seq"}
                }
                for item in rows
            ],
            "delivery": "next_input_boundary",
            "timed_out": not rows,
            "after_seq": max(
                [request.after_seq] + [item["seq"] for item in rows if item["kind"] == "message"]
            ),
            "after_notification_seq": max(
                [request.after_notification_seq]
                + [item["seq"] for item in rows if item["kind"] == "result"]
            ),
        }

    async def _validate_model_scope(self, run_id: str, check: Callable[[], None]) -> None:
        self._health()
        check()
        member = await self.member_for_run(run_id)
        if member is not None:
            tree = await self.repository.tree(member["tree_id"])
            await self._root_live(tree)
            if self.session_tools is not None:
                await self.session_tools.by_run(run_id, check)
            task = member.get("task")
            if task is not None and task["source_version"] != tree["input_version"]:
                raise ServiceError("cancelled")
        check()

    async def remember_process_debt(self, run_id: str, operation_id: str, slot: Any) -> None:
        await self.store.append(
            run_id,
            "process_cleanup_debt",
            {
                "operation_id": operation_id,
                "cleanup": "unknown",
            },
            event_id="process-debt:" + fingerprint(operation_id),
        )
        self._process_debts[run_id, operation_id] = slot

    async def reconcile_process_debts(self, run_id: str) -> bool:
        from krim_agent.storage.sandbox_models import sandbox_operations

        snapshot = await self.store.snapshot(run_id)
        pending = [
            event.data["operation_id"]
            for event in snapshot.events
            if event.kind == "process_cleanup_debt"
        ]
        for operation_id in pending:

            async def read(conn: Any, operation_id: str = operation_id) -> Any:
                return (
                    (
                        await conn.execute(
                            select(sandbox_operations).where(
                                sandbox_operations.c.operation_id == operation_id,
                            )
                        )
                    )
                    .mappings()
                    .first()
                )

            record = await self.store.work_read(read)
            receipt = record["result"].get("receipt") if record is not None else None
            if (
                record is None
                or record["status"] != "completed"
                or not isinstance(receipt, dict)
                or receipt.get("cleanup") != "clean"
            ):
                return False
            self._orphan_process_debts.discard((run_id, operation_id))
            slot = self._process_debts.pop((run_id, operation_id), None)
            if slot is not None:
                await slot.__aexit__(None, None, None)
            # The sandbox operation's completed/clean receipt is the durable
            # reconciliation fact. Terminal runs cannot accept new run events.
            if getattr(getattr(snapshot, "run", None), "status", "running") not in {
                "completed", "failed", "aborted", "interrupted",
            }:
                await self.store.append(run_id, "process_cleanup_reconciled", {
                    "operation_id": operation_id, "cleanup": "clean",
                }, event_id="process-reconciled:" + fingerprint(operation_id))
        return True

    @asynccontextmanager
    async def process_slot(self, run_id: str) -> AsyncIterator[None]:
        self._health()
        if self._orphan_process_debts:
            raise ServiceError("subagent_cleanup_failed")
        async with self.processes.slot(run_id):
            self._health()
            yield

    @asynccontextmanager
    async def request_slot(
        self, run_id: str, index: int, check: Callable[[], None]
    ) -> AsyncIterator[None]:
        await self._validate_model_scope(run_id, check)
        member = await self.member_for_run(run_id)
        kind = "child" if member is not None and member["role"] == "agent" else "root"
        async with self.requests.slot(run_id, kind=kind, guard=check):
            await self._validate_model_scope(run_id, check)
            lease_key = None
            if member is not None and member.get("task") is not None:
                task = await self.repository.task(member["task"]["task_id"])
                lease_key = task["input"].get("budget_key")
                if task["status"] == "awaiting_approval":
                    await self.repository.set_task_status(task["task_id"], status="running")
            if member is not None:
                await self.budgets.charge(member["tree_id"], run_id, index, lease_key=lease_key)
            yield

    async def prepare_input(
        self,
        run_id: str,
        history: list[sdk.Message],
        checkpoint: Callable[[Sequence[sdk.Message]], dict[str, Any]],
        *,
        initial: bool,
    ) -> list[sdk.Message]:
        self._health()
        member = await self.member_for_run(run_id)
        if member is None:
            return []
        async with self._lock(member["tree_id"]), self._delivery_gate:
            member = await self.repository.get_agent(member["agent_id"])
            tree = await self.repository.tree(member["tree_id"])
            await self._root_live(tree)
            inbox = await self.repository.inbox(
                member["agent_id"], after_seq=member["inbox_cursor"]
            )
            notices = await self.repository.notifications(
                tree["tree_id"],
                consumer_id=member["agent_id"],
                after_seq=member["notify_cursor"],
            )
            added: list[sdk.Message] = []
            task = member.get("task")
            for item in inbox:
                # The initial task is already the engine's explicit user input.
                if (
                    initial
                    and item["kind"] == "task"
                    and task is not None
                    and item["task_id"] == task["task_id"]
                ):
                    continue
                envelope = {
                    "source": "subagent_message",
                    "message_id": item["message_id"],
                    "sender_id": item["sender_id"],
                    "task_id": item["task_id"],
                    "body": item["body"],
                }
                added.append(sdk.Message(role="user", parts=[sdk.TextPart(text=encoded(envelope))]))
            for notice in notices:
                source_task = await self.repository.task(notice["task_id"])
                envelope = {
                    "source": "subagent_result",
                    "source_input_version": source_task["source_version"],
                    "current_input_version": tree["input_version"],
                    "previous_task": source_task["source_version"] != tree["input_version"],
                    "notification_id": notice["notification_id"],
                    "agent_id": notice["producer_id"],
                    "task_id": notice["task_id"],
                    "result": notice["result"],
                }
                added.append(sdk.Message(role="user", parts=[sdk.TextPart(text=encoded(envelope))]))
            if inbox or notices:
                await self.repository.commit_input(
                    CommitInput(
                        agent_id=member["agent_id"],
                        run_id=run_id,
                        payload=cast(dict[str, JsonValue], checkpoint([*history, *added])),
                        message_ids=[item["message_id"] for item in inbox],
                        notification_ids=[item["notification_id"] for item in notices],
                        expected_inbox_cursor=member["inbox_cursor"],
                        expected_notify_cursor=member["notify_cursor"],
                    )
                )
            return added

    async def commit_step(
        self,
        run_id: str,
        history: list[sdk.Message],
        checkpoint: Callable[[Sequence[sdk.Message]], dict[str, Any]],
    ) -> None:
        self._health()
        member = await self.member_for_run(run_id)
        if member is not None:
            await self.repository.commit_step(member["agent_id"], run_id, checkpoint(history))

    async def workspace_diff(self, caller: Caller, task_id: str) -> dict[str, Any]:
        task = await self.repository.task(task_id)
        if task["tree_id"] != caller.tree["tree_id"] or task["run_id"] is None:
            raise ServiceError("not_found", 404)
        snapshot = await self.store.snapshot(task["run_id"])
        event = next(
            (item for item in reversed(snapshot.events) if item.kind == "workspace_patch"), None
        )
        if event is None:
            raise ServiceError("not_found", 404)
        caller.scope.check()
        return {
            "task_id": task_id,
            "run_id": task["run_id"],
            "event_seq": event.seq,
            "patch": event.data,
            "baseline": task["input"].get("baseline"),
        }

    async def workspace_baseline(self, run_id: str) -> Any:
        from .workspace import WorkspaceBaseline

        member = await self.member_for_run(run_id)
        if member is None or member.get("task") is None:
            raise ServiceError("workspace_contract_required")
        task = await self.repository.task(member["task"]["task_id"])
        if task["run_id"] != run_id:
            raise ServiceError("permission_denied")
        raw = task["input"].get("baseline")
        if not isinstance(raw, dict):
            raise ServiceError("facts_corrupt")
        return WorkspaceBaseline.model_validate_json(encoded(raw))

    async def recover(self) -> dict[str, Any]:
        result = await self.repository.recover()
        from .types import TERMINAL_TASK_STATUS

        async def finished_leases(conn: Any) -> list[tuple[str, str]]:
            rows = (
                await conn.execute(
                    select(subagent_tasks.c.tree_id, subagent_tasks.c.input).where(
                        subagent_tasks.c.status.in_(TERMINAL_TASK_STATUS),
                    )
                )
            ).all()
            return [
                (str(tree_id), payload["budget_key"])
                for tree_id, payload in rows
                if isinstance(payload, dict) and isinstance(payload.get("budget_key"), str)
            ]

        for tree_id, key in await self.store.work_read(finished_leases):
            await self.budgets.release(tree_id, key)
        from krim_agent.storage.models import run_events

        async def debts(conn: Any) -> list[tuple[str, str]]:
            rows = (
                await conn.execute(
                    select(run_events.c.run_id, run_events.c.data).where(
                        run_events.c.kind == "process_cleanup_debt",
                    )
                )
            ).all()
            return [(str(run_id), str(data["operation_id"])) for run_id, data in rows]

        self._orphan_process_debts.update(await self.store.work_read(debts))
        for run_id in {item[0] for item in self._orphan_process_debts}:
            await self.reconcile_process_debts(run_id)
        return result

    async def _has_work(self, member: dict[str, Any]) -> bool:
        from krim_agent.storage.subagent_models import subagent_messages, subagent_notifications

        async def read(conn: Any) -> bool:
            descendants = (
                select(subagent_members.c.id)
                .where(
                    subagent_members.c.parent_id == member["agent_id"],
                )
                .cte("pending_descendants", recursive=True)
            )
            descendants = descendants.union_all(
                select(subagent_members.c.id).join(
                    descendants,
                    subagent_members.c.parent_id == descendants.c.id,
                )
            )
            auto_task = (
                select(subagent_messages.c.id)
                .where(
                    subagent_messages.c.task_id == subagent_tasks.c.id,
                    subagent_messages.c.wake == "auto",
                )
                .exists()
            )
            tasks = await conn.scalar(
                select(func.count())
                .select_from(subagent_tasks)
                .where(
                    subagent_tasks.c.agent_id.in_(select(descendants.c.id)),
                    (subagent_tasks.c.status.in_({"running", "waiting_input", "awaiting_approval"}))
                    | ((subagent_tasks.c.status == "accepted") & auto_task),
                )
            )
            starting = await conn.scalar(
                select(func.count())
                .select_from(subagent_activations)
                .where(
                    subagent_activations.c.agent_id.in_(select(descendants.c.id)),
                    subagent_activations.c.status == "starting",
                )
            )
            notices = await conn.scalar(
                select(func.count())
                .select_from(subagent_notifications)
                .where(
                    subagent_notifications.c.consumer_id == member["agent_id"],
                    subagent_notifications.c.status == "pending",
                )
            )
            messages = await conn.scalar(
                select(func.count())
                .select_from(subagent_messages)
                .where(
                    subagent_messages.c.target_id == member["agent_id"],
                    subagent_messages.c.checkpoint_id.is_(None),
                    subagent_messages.c.wake == "auto",
                )
            )
            return bool(tasks or starting or notices or messages)

        return await self.store.work_read(read)

    async def finish_or_continue(
        self,
        run_id: str,
        history: list[sdk.Message],
        checkpoint: Callable[[Sequence[sdk.Message]], dict[str, Any]],
        check: Callable[[], None],
    ) -> list[sdk.Message]:
        member = await self.member_for_run(run_id)
        if member is None:
            return []
        complete = host_messages(history)
        await self.repository.save_history(
            member["agent_id"],
            {
                "messages": [message.model_dump(mode="json") for message in complete],
            },
        )
        while True:
            self._health()
            check()
            added = await self.prepare_input(run_id, history, checkpoint, initial=False)
            if added:
                return added
            async with self._lock(member["tree_id"]):
                member = await self.repository.get_agent(member["agent_id"])
                if not await self._has_work(member):
                    if member["role"] == "root":
                        closed = await self.repository.close_root_input(
                            member["tree_id"],
                            consumer_id=member["agent_id"],
                        )
                        if closed["closed"]:
                            return []
                    elif member.get("task") is not None:
                        closed = await self.repository.close_task_input(member["task"]["task_id"])
                        if closed["closed"]:
                            return []
                    else:
                        return []

            async def ready(cursor: int, member: dict[str, Any] = member) -> list[dict[str, Any]]:
                self._health()
                check()
                inbox = await self.repository.inbox(member["agent_id"], after_seq=cursor)
                notices = await self.repository.notifications(
                    member["tree_id"],
                    consumer_id=member["agent_id"],
                    after_seq=member["notify_cursor"],
                )
                if inbox or notices:
                    return [*inbox, *notices]
                return [] if await self._has_work(member) else [{"settled": True}]

            await self.wakeups.wait(
                member["agent_id"], member["inbox_cursor"], ready, seconds=30.0, check=check
            )

    async def on_event(self, run_id: str, event: events.AgentEvent) -> None:
        self._health()
        if isinstance(event, events.HookEvent) and event.hook.status == "pending":
            member = await self.member_for_run(run_id)
            if member is not None and member.get("task") is not None:
                await self.repository.set_task_status(
                    member["task"]["task_id"], status="awaiting_approval"
                )
        if self.event_sink is not None:
            await self.event_sink(run_id, event)

    async def on_settled(self, run_id: str, status: str, code: str | None) -> None:
        member = await self.member_for_run(run_id)
        if member is None:
            return
        if member["role"] == "root":
            if status != "completed":
                await self.on_stopping(run_id)
            await self.drain_descendants(member["agent_id"], close_members=False)
            return

        # A late observer must refer to its own run, never the member's newer task.
        async def read(conn: Any) -> str | None:
            return await conn.scalar(
                select(subagent_child_runs.c.task_id).where(
                    subagent_child_runs.c.run_id == run_id,
                )
            )

        task_id = await self.store.work_read(read)
        if task_id is None or await self.repository.result(task_id) is not None:
            return
        saved = await self.store.read_checkpoint(run_id)
        text = ""
        if saved is not None:
            payload, digest = saved
            if fingerprint(payload) != digest:
                raise ServiceError("facts_corrupt")
            for value in reversed(payload.get("messages", [])):
                message = sdk.Message.model_validate(value)
                if message.role == "assistant":
                    text = "".join(
                        part.text for part in message.parts if isinstance(part, sdk.TextPart)
                    )
                    break
        reason = {"completed": "completed", "aborted": "aborted", "interrupted": "interrupted"}.get(
            status,
            "max_tokens"
            if code in {"provider_output_limit", "output_limit"}
            else "refusal"
            if code == "provider_refusal"
            else "error",
        )
        output: dict[str, JsonValue] = {"text": text, "run_id": run_id, "usage_ref": run_id}
        snapshot = await self.store.snapshot(run_id)
        patch_event = next(
            (event for event in reversed(snapshot.events) if event.kind == "workspace_patch"), None
        )
        if patch_event is not None:
            output["artifact"] = {
                "kind": "workspace_patch",
                "run_id": run_id,
                "event_seq": patch_event.seq,
                "fingerprint": patch_event.data.get("fingerprint"),
            }
        process_result = next(
            (event.data for event in reversed(snapshot.events) if event.kind == "process_finished"),
            None,
        )
        if process_result is not None:
            output["process"] = process_result
            output["text"] = str(process_result.get("stdout") or "")
            if status == "completed" and (
                process_result.get("status") != "finished" or process_result.get("exit_code") != 0
            ):
                reason = "error"
                code = str(process_result.get("code") or "execution_error")
        try:
            settled = await self.repository.settle_result(
                SettleResult(
                    task_id=task_id,
                    run_id=run_id,
                    stop_reason=reason,
                    code=code,
                    output=output,
                )
            )
        except ServiceError:
            if not self.store.healthy:
                self._faulted = True
                await self.wakeups.close()
                await self.requests.close()
            raise
        settled_task = await self.repository.task(task_id)
        lease_key = settled_task["input"].get("budget_key")
        if isinstance(lease_key, str):
            await self.budgets.release(member["tree_id"], lease_key)
        parent_id = settled["notification"]["consumer_id"]
        if parent_id is not None:
            tree = await self.repository.tree(member["tree_id"])
            if tree["status"] != "open":
                await self.repository.suppress_notifications(
                    [settled["notification"]["notification_id"]],
                    reason="root_closed",
                )
            await self.wakeups.publish(parent_id)
        await self.wakeups.publish(member["agent_id"])

    async def on_stopping(self, run_id: str) -> None:
        member = await self.member_for_run(run_id)
        if member is None or member["role"] != "root":
            return
        tree = await self.repository.tree(member["tree_id"])
        async with self._lock(member["tree_id"]):
            if tree["status"] == "open":
                await self.repository.advance_input(
                    tree["tree_id"], expected_input_version=tree["input_version"]
                )
                await self.repository.close_tree(tree["tree_id"], reason="stopped")
        await self.drain_descendants(member["agent_id"], close_members=False)
        await self.wakeups.publish(member["agent_id"])

    async def _reparent(self, agent_id: str) -> dict[str, Any]:
        from .delivery import reparent_pending

        async with self._delivery_gate:
            shifted = await reparent_pending(self.store, agent_id)
        await self.wakeups.publish(shifted["consumer_id"])
        return shifted

    async def on_cancelled(self, run_id: str) -> None:
        await self.on_stopping(run_id)
        member = await self.member_for_run(run_id)
        if member is not None and member["role"] != "root":
            await self._reparent(member["agent_id"])

    async def _controllable(self, caller: Caller, request: ControlAgent) -> dict[str, Any]:
        target = await self._target(caller, request.target)
        if target["agent_id"] == caller.member["agent_id"] or target["role"] == "root":
            raise ServiceError("permission_denied")
        parent_id = target["parent_id"]
        while parent_id is not None and parent_id != caller.member["agent_id"]:
            parent_id = (await self.repository.get_agent(parent_id))["parent_id"]
        if parent_id is None:
            raise ServiceError("permission_denied")
        activation = target.get("activation")
        if request.activation_id is not None and (
            activation is None or activation["activation_id"] != request.activation_id
        ):
            raise SubagentError(
                "subagent_activation_closed",
                accepted=False,
                action="This activation is stale; inspect the current member.",
            )
        if request.task_id is not None and request.task_id != target.get("current_task_id"):
            raise SubagentError("subagent_task_closed", accepted=False)
        return target

    async def interrupt_agent(
        self, invocation: Invocation, request: ControlAgent
    ) -> dict[str, Any]:
        caller = await self._caller(invocation)
        target = await self._controllable(caller, request)
        task = target.get("task")
        if task is not None and task["run_id"] is not None and task["status"] in ACTIVE_TASK_STATUS:
            await self._registration(target).provider.interrupt(task["run_id"])
        return {
            "agent_id": target["agent_id"],
            "interrupted": True,
            "continuable": self._registration(target).capabilities.continuable,
        }

    async def control(
        self,
        actor: Any,
        workspace_id: str,
        session_id: str,
        agent_id: str,
        *,
        action: str,
        activation_id: str | None = None,
        task_id: str | None = None,
    ) -> dict[str, Any]:
        if self.authorize_session is None or action not in {"interrupt", "close"}:
            raise ServiceError("permission_denied")
        await self.authorize_session(actor, workspace_id, session_id, write=True)
        target = await self.repository.get_agent(agent_id)
        tree = await self.repository.tree(target["tree_id"])
        if (
            tree["session_id"] != session_id
            or tree["workspace_id"] != workspace_id
            or tree["owner_id"] != actor.principal_id
        ):
            raise ServiceError("not_found", 404)
        if target["role"] == "root":
            raise ServiceError("permission_denied")
        current = target.get("activation")
        if activation_id is not None and (
            current is None or current["activation_id"] != activation_id
        ):
            raise SubagentError("subagent_activation_closed", accepted=False)
        if task_id is not None and task_id != target.get("current_task_id"):
            raise SubagentError("subagent_task_closed", accepted=False)
        # Recheck the user and current project immediately before the control
        # mutation; stopped/offline roots are deliberately still inspectable.
        await self.authorize_session(actor, workspace_id, session_id, write=True)
        if action == "close":
            return await self._close_target(target, lambda: None)
        task = target.get("task")
        if task is not None and task["status"] in ACTIVE_TASK_STATUS and task["run_id"] is not None:
            await self.runtime.cancel_delegate(task["run_id"])
        return {"agent_id": agent_id, "interrupted": True}

    async def _descendant_ids(self, agent_id: str) -> list[str]:
        async def read(conn: Any) -> list[str]:
            descendants = (
                select(subagent_members.c.id)
                .where(
                    subagent_members.c.parent_id == agent_id,
                )
                .cte("closing_descendants", recursive=True)
            )
            descendants = descendants.union_all(
                select(subagent_members.c.id).join(
                    descendants,
                    subagent_members.c.parent_id == descendants.c.id,
                )
            )
            return list((await conn.execute(select(descendants.c.id))).scalars())

        return await self.store.work_read(read)

    async def close_agent(self, invocation: Invocation, request: ControlAgent) -> dict[str, Any]:
        caller = await self._caller(invocation)
        target = await self._controllable(caller, request)
        return await self._close_target(target, caller.scope.check)

    async def _close_target(
        self, target: dict[str, Any], check: Callable[[], None]
    ) -> dict[str, Any]:
        from sqlalchemy import update

        async with self._lock(target["tree_id"]):
            check()
            ids = [target["agent_id"], *await self._descendant_ids(target["agent_id"])]

            async def close_members(conn: Any) -> dict[str, Any]:
                import time

                await conn.execute(
                    update(subagent_members)
                    .where(
                        subagent_members.c.id.in_(ids),
                        subagent_members.c.status == "active",
                    )
                    .values(
                        status="closed",
                        closed_at=time.time(),
                        revision=subagent_members.c.revision + 1,
                    )
                )

                return {"closed": list(ids)}

            async def confirm_closed() -> dict[str, Any] | None:
                async def read_closed(conn: Any) -> int:
                    return int(
                        await conn.scalar(
                            select(func.count())
                            .select_from(
                                subagent_members,
                            )
                            .where(
                                subagent_members.c.id.in_(ids),
                                subagent_members.c.status == "closed",
                            )
                        )
                    )

                count = await self.store.work_read(read_closed)
                return {"closed": list(ids)} if count == len(ids) else None

            await self.store.work_write(close_members, confirm_closed)
        from .delivery import settle_unstarted

        for identity in reversed(ids):
            await settle_unstarted(
                self.store, self.repository, self.budgets, identity, "member_closed"
            )
            await self._reparent(identity)
        cleanup = await self.drain_descendants(target["agent_id"], close_members=False)
        if target.get("activation") is not None:
            cleanup.append(await self.dispose(target["activation"]["activation_id"]))
        return {"agent_id": target["agent_id"], "closed": True, "cleanup": cleanup}

    async def dispose(self, activation_id: str) -> dict[str, Any]:
        cleanup = self._disposals.get(activation_id)
        if cleanup is None:
            cleanup = asyncio.create_task(
                self._dispose(activation_id), name="krim-subagent-dispose-" + activation_id
            )
            self._disposals[activation_id] = cleanup
        try:
            return await asyncio.shield(cleanup)
        finally:
            if cleanup.done() and self._disposals.get(activation_id) is cleanup:
                self._disposals.pop(activation_id, None)

    async def _dispose(self, activation_id: str) -> dict[str, Any]:
        async def read(conn: Any) -> tuple[dict[str, Any], list[str]]:
            row = (
                (
                    await conn.execute(
                        select(subagent_activations).where(
                            subagent_activations.c.id == activation_id,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                raise ServiceError("not_found", 404)
            active = list(
                (
                    await conn.execute(
                        select(subagent_child_runs.c.run_id).where(
                            subagent_child_runs.c.activation_id == activation_id,
                        )
                    )
                ).scalars()
            )
            return dict(row), active

        activation, active_runs = await self.store.work_read(read)
        if activation["status"] == "released":
            return {"activation_id": activation_id, "released": True}
        if activation["status"] != "closing":
            await self.repository.transition_activation(
                activation_id,
                expected_status=activation["status"],
                status="closing",
            )
        member = await self.repository.get_agent(activation["agent_id"])
        from .delivery import settle_unstarted

        await settle_unstarted(
            self.store, self.repository, self.budgets, member["agent_id"], "member_closed"
        )
        try:
            for run_id in active_runs:
                snapshot = await self.store.snapshot(run_id)
                if snapshot.run.status in {"queued", "running", "awaiting"}:
                    await self.runtime.cancel_delegate(run_id)
                if not await self.reconcile_process_debts(run_id):
                    raise ServiceError("subagent_cleanup_failed")
            if member["role"] != "root":
                registration = self._activations.get(activation_id)
                if registration is None:
                    registration = self._registration(member, cold=True)
                await registration.provider.dispose(activation_id)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            code = error.code if isinstance(error, ServiceError) else "subagent_cleanup_failed"
            await self.repository.transition_activation(
                activation_id,
                expected_status="closing",
                status="closing",
                cleanup_code=code,
            )
            _LOG.warning("Subagent cleanup remains pending: type=%s", type(error).__name__)
            return {"activation_id": activation_id, "released": False, "cleanup_code": code}
        await self.repository.transition_activation(
            activation_id,
            expected_status="closing",
            status="released",
        )
        self._activations.pop(activation_id, None)
        if member["role"] != "root":
            await self._reparent(member["agent_id"])
        if (member.get("activation") or {}).get("activation_id") == activation_id:
            await self.wakeups.close(member["agent_id"])
        return {"activation_id": activation_id, "released": True}

    async def drain_descendants(
        self, agent_id: str, *, close_members: bool = False
    ) -> list[dict[str, Any]]:
        identities = await self._descendant_ids(agent_id)
        if not identities:
            return []

        async def read(conn: Any) -> list[str]:
            return list(
                (
                    await conn.execute(
                        select(subagent_activations.c.id).where(
                            subagent_activations.c.agent_id.in_(identities),
                            subagent_activations.c.status.in_({"starting", "resident", "closing"}),
                        )
                    )
                ).scalars()
            )

        activation_ids = await self.store.work_read(read)
        cleanup: list[dict[str, Any]] = []
        for identity in reversed(activation_ids):
            cleanup.append(await self.dispose(identity))
        return cleanup

    async def drain(self) -> list[dict[str, Any]]:
        self._closed = True
        await self.requests.close()
        await self.processes.close()
        await self.wakeups.close()

        async def read(conn: Any) -> tuple[list[str], list[str]]:
            trees = list(
                (
                    await conn.execute(
                        select(subagent_trees.c.id).where(
                            subagent_trees.c.status == "open",
                        )
                    )
                ).scalars()
            )
            activations = list(
                (
                    await conn.execute(
                        select(subagent_activations.c.id).where(
                            subagent_activations.c.status.in_({"starting", "resident", "closing"}),
                        )
                    )
                ).scalars()
            )
            return trees, activations

        trees, activations = await self.store.work_read(read)
        for tree_id in trees:
            await self.repository.close_tree(tree_id, reason="host_shutdown")
        launches = list(self._launches.values())
        if launches:
            await asyncio.gather(*launches, return_exceptions=True)
        result = []
        for activation_id in activations:
            result.append(await self.dispose(activation_id))
        return result
