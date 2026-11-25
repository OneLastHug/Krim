"""一个dispatcher；实际producer/容量仍归RunService。"""

from __future__ import annotations

import asyncio
import base64
import binascii
import builtins
import json
import math
import re
from typing import TYPE_CHECKING, Any, Literal, cast
from uuid import UUID

from pydantic import JsonValue
from sqlalchemy import select

from krim_agent.accounts.service import IdentityService
from krim_agent.config import Settings, minimum_limit
from krim_agent.context.types import ContextPolicy, upgrade_to_token_window
from krim_agent.contracts import (
    RETIRED_TARGETS,
    ApprovalReply,
    RunStatus,
    ServiceError,
    encoded,
    fingerprint,
    parse_json,
)
from krim_agent.providers.openai_compatible import OpenAICompatibleProvider
from krim_agent.runtime.contracts import settled
from krim_agent.runtime.service import RunBinding, RunService
from krim_agent.security import PrincipalContext
from krim_agent.storage import sessions as sql
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.workspace.types import ExecutionAccess, WorkspaceTurnRequest

if TYPE_CHECKING:
    from krim_agent.goals.continuation import ContinuationRequest
    from krim_agent.goals.service import GoalService

from .types import (
    ControlRequest,
    ControlView,
    SessionCreate,
    SessionView,
    TraceIndexItem,
    TraceIndexPage,
    TurnRequest,
    TurnView,
)

_CONTROL_KINDS = frozenset({"rename", "close", "resume", "cancel_turn", "approval"})
_TRACE_STATUSES = frozenset({"queued", "running", "completed", "failed", "aborted", "interrupted"})
_TRACE_CURSOR_RE = re.compile(r"[A-Za-z0-9_-]{1,1024}\Z")


def _trace_cursor_scope(
    principal: PrincipalContext,
    workspace: str,
    role: str,
    limit: int,
    status: RunStatus | None,
    session_id: str | None,
    turn_id: str | None,
) -> str:
    return fingerprint(
        {
            "version": 1,
            "principal_id": principal.principal_id,
            "generation": principal.generation,
            "workspace_id": workspace,
            "role": role,
            "limit": limit,
            "status": status,
            "session_id": session_id,
            "turn_id": turn_id,
        }
    )


def _decode_trace_cursor(cursor: str | None, scope: str) -> tuple[float, str] | None:
    if cursor is None:
        return None
    try:
        if not _TRACE_CURSOR_RE.fullmatch(cursor):
            raise ValueError
        raw = base64.b64decode(
            cursor + "=" * ((4 - len(cursor) % 4) % 4), altchars=b"-_", validate=True
        )
        if base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=") != cursor:
            raise ValueError
        payload = json.loads(raw)
        if (
            not isinstance(payload, dict)
            or set(payload) != {"version", "scope", "created_at", "run_id"}
            or type(payload["version"]) is not int
            or payload["version"] != 1
            or payload["scope"] != scope
            or type(payload["created_at"]) is not float
            or not math.isfinite(payload["created_at"])
            or not isinstance(payload["run_id"], str)
            or str(UUID(payload["run_id"])) != payload["run_id"]
            or encoded(payload).encode("utf-8") != raw
        ):
            raise ValueError
        return payload["created_at"], payload["run_id"]
    except (ValueError, TypeError, UnicodeDecodeError, binascii.Error, OverflowError):
        raise ServiceError("invalid_cursor", 416) from None


def _encode_trace_cursor(scope: str, item: TraceIndexItem) -> str:
    payload = {
        "version": 1,
        "scope": scope,
        "created_at": item.created_at,
        "run_id": item.run_id,
    }
    return base64.urlsafe_b64encode(encoded(payload).encode("utf-8")).decode("ascii").rstrip("=")


async def read_trace_index(
    store: SQLiteStore,
    principal: PrincipalContext,
    workspace: str,
    *,
    limit: int = 20,
    cursor: str | None = None,
    status: RunStatus | None = None,
    session_id: str | None = None,
    turn_id: str | None = None,
) -> TraceIndexPage:
    """读取当前身份可见的Session Turn索引，不读取Run快照。"""
    membership = principal.member(workspace)
    if type(principal.generation) is not int or principal.generation < 1:
        raise ServiceError("unauthorized", 401)
    if type(limit) is not int or not 1 <= limit <= 50:
        raise ServiceError("invalid_input", 422)
    if status is not None and (not isinstance(status, str) or status not in _TRACE_STATUSES):
        raise ServiceError("invalid_input", 422)
    if session_id is not None and (
        not isinstance(session_id, str) or not session_id or len(session_id) > 128
    ):
        raise ServiceError("invalid_input", 422)
    if turn_id is not None and (not isinstance(turn_id, str) or not turn_id or len(turn_id) > 128):
        raise ServiceError("invalid_input", 422)

    scope = _trace_cursor_scope(
        principal, workspace, membership.role, limit, status, session_id, turn_id
    )
    before = _decode_trace_cursor(cursor, scope)
    rows = await sql.list_trace_index(
        store,
        workspace,
        principal.principal_id,
        membership.role == "admin",
        limit,
        status=status,
        session_id=session_id,
        turn_id=turn_id,
        before=before,
    )
    try:
        items = [
            TraceIndexItem.model_validate({"source_kind": "session_turn", **row})
            for row in rows[:limit]
        ]
    except ValueError:
        raise ServiceError("facts_corrupt") from None
    next_cursor = _encode_trace_cursor(scope, items[-1]) if len(rows) > limit else None
    return TraceIndexPage(items=items, next_cursor=next_cursor)


def _validate_control_arguments(
    kind: str,
    turn_id: str | None = None,
    approval: ApprovalReply | None = None,
    request_id: str | None = None,
) -> None:
    if kind not in _CONTROL_KINDS:
        raise ServiceError("invalid_input", 422)
    if kind == "approval":
        if (
            not isinstance(turn_id, str)
            or not turn_id
            or approval is None
            or not isinstance(request_id, str)
            or not request_id
        ):
            raise ServiceError("invalid_input", 422)
        return
    if approval is not None or request_id is not None:
        raise ServiceError("invalid_input", 422)
    if kind == "cancel_turn":
        if not isinstance(turn_id, str) or not turn_id:
            raise ServiceError("invalid_input", 422)
    elif turn_id is not None:
        raise ServiceError("invalid_input", 422)


class SessionService:
    def __init__(
        self,
        store: SQLiteStore,
        runs: RunService,
        settings: Settings,
        *,
        identities: IdentityService | None = None,
        account_mcp: Any | None = None,
        goals: GoalService | None = None,
    ) -> None:
        self.store, self.runs, self.settings = store, runs, settings
        # 稳定身份服务引用；托管模式由composition注入，legacy保持None。
        self.identities = identities
        self.account_mcp = account_mcp
        self.goals = goals
        # Runtime answer reconciliation shares this admission fence so a newly
        # accepted user turn supersedes old input before any resume/dispatch.
        self._input_lock: asyncio.Lock = (
            getattr(runs, "input_gate", None) or getattr(runs, "_input_lock", None)
            or asyncio.Lock()
        )
        self.closed = False
        self.failure: str | None = None
        self.controls: dict[str, asyncio.Task[ControlView]] = {}
        self._control_inputs: dict[str, str] = {}
        self._started: set[str] = set()
        self._revoked: set[str] = set()
        self._close_task: asyncio.Task[None] | None = None
        self._dispatcher = asyncio.create_task(self._dispatch(), name="krim-session-dispatch")
        self._dispatcher.add_done_callback(self._consume_dispatch)

    def _consume_dispatch(self, task: asyncio.Task[None]) -> None:
        if not task.cancelled() and task.exception() is not None:
            self.failure = "storage_unavailable"

    def _admitting(self) -> None:
        if self.closed:
            raise ServiceError("closed", 503)
        if not self.store.healthy or self.failure:
            raise ServiceError("storage_unavailable", 503)

    def _live_principal(self, principal: PrincipalContext) -> PrincipalContext:
        """托管模式下对请求主体做当前授权复核；拒绝旧generation/memberships。"""
        identities = self.identities
        if identities is None or not getattr(identities, "managed", False):
            return principal
        try:
            return identities.validate(principal)
        except ServiceError:
            raise ServiceError("permission_denied", 403) from None

    async def get(
        self,
        principal: PrincipalContext,
        workspace: str,
        session_id: str,
        *,
        write: bool = False,
        approve: bool = False,
    ) -> SessionView:
        # 统一入口当前验证：旧generation/已停用主体的SDK直调在此拒绝。
        principal = self._live_principal(principal)
        principal.member(workspace, write=write, approve=approve)
        session = await sql.get_session(self.store, session_id)
        if session.workspace_id != workspace:
            raise ServiceError("not_found", 404)
        principal.owns(workspace, session.owner_id, write=write, approve=approve)
        if self.store.schema_version >= 19:
            from krim_agent.project_access.policy import resolve_project_access
            from krim_agent.storage.sandbox_models import sandbox_session_bindings

            if session.owner_id != principal.principal_id:
                raise ServiceError("not_found", 404)

            async def project_access(conn: Any) -> None:
                project_id = await conn.scalar(select(sandbox_session_bindings.c.project_id).where(
                    sandbox_session_bindings.c.session_id == session_id,
                ))
                if project_id is not None:
                    access = await resolve_project_access(conn, principal, project_id)
                    access.require()  # saving chat is not writing project files

            await self.store.work_read(project_access)
        return session

    async def by_key(self, principal: PrincipalContext, workspace: str, key: str) -> SessionView:
        principal = self._live_principal(principal)
        principal.member(workspace)
        session = await sql.by_key(self.store, workspace, principal.principal_id, key)
        return await self.get(principal, workspace, session.id)

    async def list(
        self,
        principal: PrincipalContext,
        workspace: str,
        offset: int = 0,
        limit: int = 20,
        *,
        view: Literal["active", "archived", "trash"] = "active",
    ) -> list[SessionView]:
        principal = self._live_principal(principal)
        membership = principal.member(workspace)
        if (
            type(offset) is not int or offset < 0
            or type(limit) is not int or not 1 <= limit <= 50
            or view not in {"active", "archived", "trash"}
        ):
            raise ServiceError("invalid_input", 422)
        return await sql.list_sessions(
            self.store,
            workspace,
            principal.principal_id,
            membership.role == "admin",
            offset,
            limit,
            view=view,
        )

    async def traces(
        self,
        principal: PrincipalContext,
        workspace: str,
        *,
        limit: int = 20,
        cursor: str | None = None,
        status: RunStatus | None = None,
        session_id: str | None = None,
        turn_id: str | None = None,
    ) -> TraceIndexPage:
        self._admitting()
        principal = self._live_principal(principal)
        return await read_trace_index(
            self.store,
            principal,
            workspace,
            limit=limit,
            cursor=cursor,
            status=status,
            session_id=session_id,
            turn_id=turn_id,
        )

    async def create(
        self, principal: PrincipalContext, workspace: str, request: SessionCreate
    ) -> SessionView:
        self._admitting()
        principal = self._live_principal(principal)
        principal.member(workspace, write=True)
        body = SessionCreate.model_validate_json(request.model_dump_json())
        try:
            await sql.by_key(self.store, workspace, principal.principal_id, body.request_key)
        except ServiceError as error:
            if error.code != "not_found":
                raise
        else:
            return await sql.create_session(
                self.store,
                workspace,
                principal.principal_id,
                body,
                {},
                ContextPolicy(),
                self.settings,
                principal=principal,
            )
        if body.target_key in RETIRED_TARGETS:
            raise ServiceError("target_retired", 410)
        binding = self.runs.bindings.get(body.target_key)
        if binding is None:
            if not self.runs.bindings:
                raise ServiceError("provider_not_configured", 503)
            raise ServiceError("invalid_input", 422)
        binding.assert_current()
        output = (
            binding.loop.provider.config.max_output_tokens
            if isinstance(binding.loop.provider, OpenAICompatibleProvider)
            else 2048
        )
        capacity = binding.settings.request_capacity(binding.loop.provider)
        configured_bytes = self.settings.context_max_bytes
        default_policy = ContextPolicy()
        output_headroom = default_policy.max_tokens - default_policy.compaction_trigger_tokens
        inherited = ContextPolicy(
            max_bytes=minimum_limit(capacity, configured_bytes),
            max_messages=self.settings.context_max_messages,
            reserved_output_tokens=max(
                self.settings.context_reserved_output_tokens or 0,
                min(output or default_policy.reserved_output_tokens, output_headroom),
            ),
        )
        policy = upgrade_to_token_window(body.context_policy) if body.context_policy else inherited
        if policy.max_bytes is None and inherited.max_bytes is not None:
            policy = policy.model_copy(update={"max_bytes": inherited.max_bytes})
        if (
            inherited.max_bytes is not None and policy.max_bytes is not None
            and policy.max_bytes > inherited.max_bytes
            or policy.max_tokens != inherited.max_tokens
            or policy.compaction_trigger_tokens != inherited.compaction_trigger_tokens
            or (inherited.max_messages is not None and (
                policy.max_messages is None or policy.max_messages > inherited.max_messages
            ))
            or policy.reserved_output_tokens < inherited.reserved_output_tokens
        ):
            raise ServiceError("invalid_input", 422)
        return await sql.create_session(
            self.store,
            workspace,
            principal.principal_id,
            body,
            cast(dict[str, JsonValue], parse_json(binding.descriptor_json)),
            policy,
            self.settings,
            principal=principal,
        )

    async def submit(
        self,
        principal: PrincipalContext,
        workspace: str,
        session_id: str,
        request: TurnRequest | WorkspaceTurnRequest,
    ) -> TurnView:
        self._admitting()
        from krim_agent.goals.continuation import INTERNAL_TURN_PREFIXES

        if request.request_key.startswith(INTERNAL_TURN_PREFIXES):
            raise ServiceError("invalid_input", 422)
        if isinstance(request, WorkspaceTurnRequest):
            # 工作请求的嵌套expected必须在首await前脱离调用者；legacy顺序不变。
            request = WorkspaceTurnRequest.model_validate_json(request.model_dump_json())
        accepted_session = await self.get(principal, workspace, session_id, write=True)
        selected_binding = self.runs.bindings.get(accepted_session.target_key)
        retired = accepted_session.target_key in RETIRED_TARGETS
        if retired or selected_binding is None:
            # Existing idempotency keys still reach the storage equality check;
            # a new turn must never silently acquire another target's privileges.
            try:
                await sql.turn_by_key(self.store, session_id, request.request_key)
            except ServiceError as error:
                if error.code != "not_found":
                    raise
                raise ServiceError(
                    "target_retired" if retired else "provider_not_configured",
                    410 if retired else 503,
                ) from None
        if (
            selected_binding is not None
            and selected_binding.loop.sandbox is not None
            and accepted_session.owner_id != principal.principal_id
        ):
            # 普通会话只持有owner快照，不能将管理员提交误当owner授权。
            raise ServiceError("permission_denied", 403)
        # 入队前实时复核：边界认证与本次写入之间被停用/降权的主体不再排队。
        principal = self._live_principal(principal)
        body = (
            WorkspaceTurnRequest.model_validate_json(request.model_dump_json())
            if isinstance(request, WorkspaceTurnRequest)
            else TurnRequest.model_validate_json(request.model_dump_json())
        )
        if (self.settings.max_input_bytes is not None
                and len(body.text.encode()) > self.settings.max_input_bytes):
            raise ServiceError("input_limit", 422)
        from krim_agent.extensions import ExtensionService

        async def accept() -> TurnView:
            async with self._input_lock:
                result = await sql.submit(
                    self.store,
                    session_id,
                    body,
                    self.settings,
                    access=ExecutionAccess(principal, workspace, session_id)
                    if isinstance(body, WorkspaceTurnRequest)
                    else None,
                    actor=principal,
                    extension_service=ExtensionService(
                        self.store,
                        self.identities
                        if self.identities is not None and self.identities.managed
                        else None,
                    ),
                    account_mcp_service=getattr(self, "account_mcp", None),
                    upgrade_target=self._boundary_target_upgrade(selected_binding),
                )
                await self._supersede_inputs(principal, workspace, result)
                await self._supersede_goal_turns(principal, workspace, result)
                self.runs.slot_changed.set()
                return result

        # A cancelled HTTP waiter must not leave an admitted turn behind its
        # input fence; finish admission/cleanup first and then propagate cancel.
        return await settled(accept())

    def _boundary_target_upgrade(self, binding: RunBinding | None) -> dict[str, JsonValue] | None:
        """binding当前一致时返回现行目标描述符；否则None保持冻结target。

        显式新用户消息才到达此处；内部续接走enqueue，不经过本方法。
        assert_current失败（含工具/绑定身份漂移）即返回None。冻结行与现行
        描述符的同异、白名单（仅实现摘要漂移）与busy栅栏全部由storage在
        升级事务内按实时冻结行核对，service不读快照对比，避免读取后漂移。
        """
        if binding is None:
            return None
        try:
            binding.assert_current()
        except ServiceError:
            return None
        return cast(dict[str, JsonValue], parse_json(binding.descriptor_json))

    async def _supersede_inputs(
        self, principal: PrincipalContext, workspace: str, accepted: TurnView
    ) -> None:
        interactions = getattr(self.runs, "interactions", None)
        if interactions is None or accepted.owner_id != principal.principal_id:
            return
        from krim_agent.storage.interaction_models import interaction_requests
        from krim_agent.storage.models import runs, turns

        async def earlier(conn: Any) -> builtins.list[str]:
            return list(await conn.scalars(
                select(interaction_requests.c.run_id)
                .join(turns, interaction_requests.c.turn_id == turns.c.id)
                .join(runs, interaction_requests.c.run_id == runs.c.id)
                .where(
                    turns.c.session_id == accepted.session_id,
                    turns.c.ordinal < accepted.ordinal,
                    (interaction_requests.c.status == "pending") | (runs.c.status == "awaiting"),
                    runs.c.status.in_(("queued", "running", "awaiting")),
                ).distinct()
            ))

        for run_id in await self.store.work_read(earlier):
            await self.runs.cancel(run_id)
        await interactions.supersede(
            principal, workspace, accepted.session_id,
            "message:" + fingerprint({"turn_id": accepted.id}),
            before_turn_id=accepted.id,
        )

    def _owner_principal(self, owner_id: str) -> PrincipalContext:
        if owner_id in self._revoked:
            raise ServiceError("permission_denied", 403)
        identities = self.identities
        if identities is not None and identities.managed:
            return identities.resolve(owner_id)
        host = getattr(self.runs, "session_tool_host", None)
        policy = getattr(host, "access_policy", None)
        credential = (
            next((item for item in policy.credentials if item.principal_id == owner_id), None)
            if policy is not None else None
        )
        if credential is not None:
            return PrincipalContext(
                credential.principal_id, credential.generation,
                credential.memberships, credential.legacy,
            )
        if owner_id == "local":
            from krim_agent.security import LOCAL

            return LOCAL
        raise ServiceError("permission_denied", 403)

    async def _supersede_goal_turns(
        self, principal: PrincipalContext, workspace: str, accepted: TurnView
    ) -> None:
        if self.goals is None or accepted.owner_id != principal.principal_id:
            return
        from krim_agent.goals.continuation import INTERNAL_TURN_PREFIXES
        from krim_agent.storage.models import turns

        if accepted.request_key.startswith(INTERNAL_TURN_PREFIXES):
            return

        from krim_agent.storage.goal_models import goal_continuations

        async def current(conn: Any) -> tuple[bool, builtins.list[str]]:
            newer_intent = await conn.scalar(select(goal_continuations.c.request_key).where(
                goal_continuations.c.session_id == accepted.session_id,
                goal_continuations.c.status == "queued",
                goal_continuations.c.created_at > accepted.created_at,
            ).limit(1))
            if newer_intent is not None:
                return False, []
            later = await conn.scalar(select(turns.c.id).where(
                turns.c.session_id == accepted.session_id, turns.c.ordinal > accepted.ordinal,
            ).limit(1))
            ids = list(await conn.scalars(select(turns.c.id).where(
                turns.c.session_id == accepted.session_id,
                turns.c.status.in_(("queued", "running")),
                turns.c.created_at <= accepted.created_at,
                turns.c.request_key.startswith(INTERNAL_TURN_PREFIXES[0])
                | turns.c.request_key.startswith(INTERNAL_TURN_PREFIXES[1]),
            )))
            return later is None, ids

        newest, ids = await self.store.work_read(current)
        if not newest:
            return
        await self.goals.supersede_continuations(principal, workspace, accepted.session_id)
        for turn_id in ids:
            turn = await sql.get_turn(self.store, turn_id)
            if turn.run_id is not None:
                await self.runs.cancel(turn.run_id)
            else:
                await sql.fail_queued(self.store, turn_id, "cancelled", status="cancelled")

    async def enqueue(self, request: ContinuationRequest) -> None:
        """Host-only Goal queue adapter; persisted intent, never text, grants enqueue."""
        self._admitting()
        goals = self.goals
        if goals is None:
            raise ServiceError("permission_denied", 403)
        from krim_agent.extensions import ExtensionService
        from krim_agent.goals.continuation import ContinuationRequest
        from krim_agent.storage.goal_models import goal_continuations, session_goals
        from krim_agent.storage.interaction_models import interaction_requests
        from krim_agent.storage.models import sessions, turns

        frozen = ContinuationRequest.model_validate_json(request.model_dump_json())
        async with self._input_lock:
            actor = self._owner_principal(frozen.owner_id)
            actor.member(frozen.workspace_id, write=True)

            async def known(conn: Any) -> bool:
                row = (await conn.execute(select(goal_continuations).where(
                    goal_continuations.c.session_id == frozen.session_id,
                    goal_continuations.c.request_key == frozen.continue_key,
                    goal_continuations.c.owner_id == actor.principal_id,
                    goal_continuations.c.workspace_id == frozen.workspace_id,
                ))).mappings().first()
                if row is None or row["payload"] != frozen.model_dump(mode="json"):
                    raise ServiceError("input_revision_conflict", 409)
                existing = (await conn.execute(select(turns).where(
                    turns.c.session_id == frozen.session_id,
                    turns.c.request_key == frozen.turn_request_key,
                ))).mappings().first()
                if existing is None:
                    return False
                if existing["text"] != frozen.text or existing["owner_id"] != actor.principal_id:
                    raise ServiceError("key_conflict", 409)
                return True

            if await self.store.work_read(known):
                return

            async def check(conn: Any) -> int:
                row = (await conn.execute(select(goal_continuations).where(
                    goal_continuations.c.session_id == frozen.session_id,
                    goal_continuations.c.request_key == frozen.continue_key,
                ))).mappings().first()
                head = (await conn.execute(select(session_goals).where(
                    session_goals.c.session_id == frozen.session_id,
                ))).mappings().first()
                session = (await conn.execute(select(sessions).where(
                    sessions.c.id == frozen.session_id,
                    sessions.c.workspace_id == frozen.workspace_id,
                    sessions.c.owner_id == actor.principal_id,
                ))).mappings().first()
                if row is None or row["status"] != "queued" or row["payload"] != (
                    frozen.model_dump(mode="json")
                ) or head is None or head["goal_id"] != frozen.goal_id or (
                    head["status"] != "active" or head["revision"] != frozen.goal_revision
                ):
                    raise ServiceError("input_revision_conflict", 409)
                if session is None or session["status"] != "open":
                    raise ServiceError("session_closed", 409)
                if session["dispatch_paused"]:
                    raise ServiceError("session_busy", 409)
                if session["target_key"] == "target-workspace":
                    raise ServiceError("workspace_contract_required", 409)
                pending = await conn.scalar(select(interaction_requests.c.id).where(
                    interaction_requests.c.session_id == frozen.session_id,
                    interaction_requests.c.status == "pending",
                ).limit(1))
                if pending is not None:
                    raise ServiceError("session_busy", 409)
                origin = await conn.scalar(select(turns.c.id).where(
                    turns.c.id == frozen.origin_turn_id,
                    turns.c.run_id == frozen.origin_run_id,
                    turns.c.session_id == frozen.session_id,
                    turns.c.owner_id == actor.principal_id,
                ))
                if origin is None:
                    raise ServiceError("facts_corrupt")
                if frozen.source_turn_id is not None:
                    source = (await conn.execute(select(turns).where(
                        turns.c.id == frozen.source_turn_id,
                        turns.c.run_id == frozen.source_run_id,
                        turns.c.session_id == frozen.session_id,
                    ))).mappings().first()
                    if source is None or source["status"] not in {"completed", "failed"}:
                        raise ServiceError("input_revision_conflict", 409)
                    newer_user = await conn.scalar(select(turns.c.id).where(
                        turns.c.session_id == frozen.session_id,
                        turns.c.ordinal > source["ordinal"],
                        ~turns.c.request_key.startswith("goal-continue:"),
                        ~turns.c.request_key.startswith("goal-resume:"),
                    ).limit(1))
                    if newer_user is not None:
                        raise ServiceError("input_revision_conflict", 409)
                else:
                    newer_user = await conn.scalar(select(turns.c.id).where(
                        turns.c.session_id == frozen.session_id,
                        turns.c.created_at > row["created_at"],
                        ~turns.c.request_key.startswith("goal-continue:"),
                        ~turns.c.request_key.startswith("goal-resume:"),
                    ).limit(1))
                    if newer_user is not None:
                        raise ServiceError("input_revision_conflict", 409)
                active = await conn.scalar(select(turns.c.id).where(
                    turns.c.session_id == frozen.session_id,
                    turns.c.status.in_(("queued", "running")),
                    turns.c.request_key != frozen.turn_request_key,
                ).limit(1))
                if active is not None:
                    raise ServiceError("session_busy", 409)
                return int(session["revision"])

            revision = await self.store.work_read(check)
            await sql.submit(
                self.store, frozen.session_id, TurnRequest(
                    request_key=frozen.turn_request_key, text=frozen.text,
                    expected_revision=revision,
                ), self.settings, actor=actor,
                extension_service=ExtensionService(self.store, self.identities),
                account_mcp_service=self.account_mcp,
            )
            # Do not call public submit: internal text is not a new user message
            # and must not supersede itself or create fresh Goal authority.
            self.runs.slot_changed.set()

    async def turn(
        self, principal: PrincipalContext, workspace: str, session_id: str, turn_id: str
    ) -> TurnView:
        await self.get(principal, workspace, session_id)
        result = await sql.get_turn(self.store, turn_id)
        if result.session_id != session_id or result.workspace_id != workspace:
            raise ServiceError("not_found", 404)
        return result

    async def turn_by_key(
        self, principal: PrincipalContext, workspace: str, session_id: str, key: str
    ) -> TurnView:
        await self.get(principal, workspace, session_id)
        return await sql.turn_by_key(self.store, session_id, key)

    async def turns(
        self,
        principal: PrincipalContext,
        workspace: str,
        session_id: str,
        offset: int = 0,
        limit: int = 20,
    ) -> builtins.list[TurnView]:
        await self.get(principal, workspace, session_id)
        if (type(offset) is not int or offset < 0
                or type(limit) is not int or not 1 <= limit <= 50):
            raise ServiceError("invalid_input", 422)
        return await sql.list_turns(self.store, session_id, offset, limit)

    async def wait(
        self, principal: PrincipalContext, workspace: str, session_id: str, turn_id: str
    ) -> TurnView:
        queue_timeout, run_timeout = (
            self.settings.session_queue_timeout, self.settings.run_timeout
        )
        reader_budget = (
            None if queue_timeout is None or run_timeout is None
            else queue_timeout + run_timeout + 30
        )
        async with asyncio.timeout(reader_budget) as reader_timeout:
            while True:
                value = await self.turn(principal, workspace, session_id, turn_id)
                if value.status not in {"queued", "running"}:
                    return value
                producer = self.runs.tasks.get(value.run_id) if value.run_id is not None else None
                if value.run_id is not None and reader_timeout.when() is not None:
                    # A parked run has no live producer; human input/approval
                    # waits are durable and must not consume a reader deadline.
                    reader_timeout.reschedule(None)
                if producer is None or producer.done():
                    if (reader_budget is not None and reader_timeout.when() is None
                            and value.run_id is None):
                        reader_timeout.reschedule(asyncio.get_running_loop().time() + 30)
                    if producer is not None and value.run_id is not None:
                        await self.runs.wait(value.run_id)
                await asyncio.sleep(0.02)

    def _dispatch_block(self, turn: TurnView) -> str | None:
        """派发前对队列owner做实时复核；返回失败码，None表示放行。"""
        if turn.owner_id in self._revoked:
            return "permission_denied"
        identities = self.identities
        if identities is None or not getattr(identities, "managed", False):
            return None
        try:
            principal = identities.resolve(turn.owner_id)
            # viewer不能派发demo/http等需要写授权的会话任务；workspace任务
            # 另由prepare/fence按提交时grant比对generation漂移。
            principal.member(turn.workspace_id, write=True)
        except ServiceError:
            return "permission_denied"
        return None

    async def revoke_principal(self, principal_id: str) -> dict[str, JsonValue]:
        """停用/吊销/重签发后的运行时收敛（由on_change发布成功后调用）。

        立即阻止该主体的queued turn派发并请求取消其名下全部活跃Run
        （含demo/target-http等非workspace session任务）；等待全部收束、
        结果已知后才返回。撤销门仅在收敛期间生效：收敛完成后重新认证
        （重新启用/新签发key）的主体正常排队派发，不永久拉黑。幂等；
        不触碰其他主体。v1直连Run在存储中无主体归属，不在此收敛范围
        （该缺口由HTTP门禁与legacy主体限制覆盖）。
        """
        self._revoked.add(principal_id)
        try:
            failed_queued: list[str] = []
            for turn_id in await sql.queued(self.store):
                turn = await sql.get_turn(self.store, turn_id)
                if turn.owner_id != principal_id:
                    continue
                await sql.fail_queued(self.store, turn_id, "permission_denied")
                failed_queued.append(turn_id)
            cancelled_runs: list[str] = []
            failures: list[ServiceError] = []
            # Parked runs have no live producer, including after an application
            # reopen. Persisted session ownership, not Task.done(), defines scope.
            for run_id in await sql.owned_active_runs(self.store, principal_id):
                try:
                    await self.runs.cancel(run_id)
                    cancelled_runs.append(run_id)
                except ServiceError as error:
                    failures.append(error)
            if failures:
                raise failures[0]
            return {
                "principal_id": principal_id,
                "failed_queued": cast(list[JsonValue], failed_queued),
                "cancelled_runs": cast(list[JsonValue], cancelled_runs),
            }
        finally:
            self._revoked.discard(principal_id)

    async def _recover_input_supersessions(self) -> None:
        """Repair admission committed before its input cleanup/notification."""
        if getattr(self.runs, "interactions", None) is None:
            return
        from krim_agent.storage.interaction_models import interaction_requests
        from krim_agent.storage.models import runs, sessions, turns

        previous = turns.alias("input_previous")
        successor = turns.alias("input_successor")

        async def candidates(conn: Any) -> builtins.list[dict[str, Any]]:
            rows = (await conn.execute(
                select(
                    successor.c.id, sessions.c.owner_id, sessions.c.workspace_id,
                ).select_from(
                    interaction_requests.join(
                        previous, interaction_requests.c.turn_id == previous.c.id
                    )
                    .join(runs, interaction_requests.c.run_id == runs.c.id)
                    .join(sessions, previous.c.session_id == sessions.c.id)
                    .join(successor, (successor.c.session_id == sessions.c.id)
                          & (successor.c.ordinal > previous.c.ordinal))
                ).where(
                    sessions.c.status == "open", successor.c.status == "queued",
                    (interaction_requests.c.status == "pending") | (runs.c.status == "awaiting"),
                ).distinct().order_by(successor.c.ordinal)
            )).mappings().all()
            return [dict(row) for row in rows]

        async with self._input_lock:
            for record in await self.store.work_read(candidates):
                if record["owner_id"] in self._revoked:
                    continue
                identities = self.identities
                if identities is not None and identities.managed:
                    try:
                        principal = identities.resolve(record["owner_id"])
                    except ServiceError:
                        continue
                else:
                    host = getattr(self.runs, "session_tool_host", None)
                    policy = getattr(host, "access_policy", None)
                    credential = (
                        next((item for item in policy.credentials
                              if item.principal_id == record["owner_id"]), None)
                        if policy is not None else None
                    )
                    if credential is not None:
                        principal = PrincipalContext(
                            credential.principal_id, credential.generation,
                            credential.memberships, credential.legacy,
                        )
                    elif record["owner_id"] == "local":
                        from krim_agent.security import LOCAL

                        principal = LOCAL
                    else:
                        continue
                try:
                    principal.member(record["workspace_id"], write=True)
                    accepted = await sql.get_turn(self.store, record["id"])
                    await self._supersede_inputs(principal, record["workspace_id"], accepted)
                except ServiceError as error:
                    if error.code not in {"unauthorized", "permission_denied", "not_found"}:
                        raise

    async def _goal_turn_permitted(self, turn: TurnView) -> bool:
        from krim_agent.goals.continuation import INTERNAL_TURN_PREFIXES

        prefix = next((value for value in INTERNAL_TURN_PREFIXES
                       if turn.request_key.startswith(value)), None)
        if prefix is None:
            return True
        if self.goals is None:
            return False
        from krim_agent.storage.goal_models import goal_continuations, session_goals
        from krim_agent.storage.models import turns

        async def read(conn: Any) -> bool:
            row = (await conn.execute(select(goal_continuations).where(
                goal_continuations.c.session_id == turn.session_id,
                goal_continuations.c.request_key == turn.request_key[len(prefix):],
            ))).mappings().first()
            head = (await conn.execute(select(session_goals).where(
                session_goals.c.session_id == turn.session_id,
            ))).mappings().first()
            if (row is None or head is None or row["status"] != "queued"
                    or head["status"] != "active" or row["goal_id"] != head["goal_id"]
                    or row["goal_revision_base"] != head["revision"]
                    or row["owner_id"] != turn.owner_id
                    or row["payload"].get("text") != turn.text):
                return False
            source_id = row["source_turn_id"] or row["origin_turn_id"]
            ordinal = await conn.scalar(select(turns.c.ordinal).where(turns.c.id == source_id))
            if ordinal is None:
                return False
            after = (
                turns.c.ordinal > ordinal if row["source_turn_id"] is not None else
                turns.c.created_at > row["created_at"]
            )
            newer = await conn.scalar(select(turns.c.id).where(
                turns.c.session_id == turn.session_id, after,
                ~turns.c.request_key.startswith(INTERNAL_TURN_PREFIXES[0]),
                ~turns.c.request_key.startswith(INTERNAL_TURN_PREFIXES[1]),
            ).limit(1))
            return newer is None

        return await self.store.work_read(read)

    async def _fail_goal_enqueue(self, request: ContinuationRequest, code: str) -> None:
        """Persist a permanent queue failure once; it is not new Goal authority."""
        import time

        from sqlalchemy import update

        from krim_agent.storage.goal_models import goal_continuations

        async def write(conn: Any) -> bool:
            row = (await conn.execute(select(goal_continuations).where(
                goal_continuations.c.session_id == request.session_id,
                goal_continuations.c.request_key == request.continue_key,
            ))).mappings().first()
            if row is None or row["status"] != "queued" or row["payload"] != (
                request.model_dump(mode="json")
            ):
                return False
            await conn.execute(update(goal_continuations).where(
                goal_continuations.c.session_id == request.session_id,
                goal_continuations.c.request_key == request.continue_key,
                goal_continuations.c.status == "queued",
            ).values(status="failed", code=code, updated_at=time.time()))
            return True

        async def confirm() -> bool | None:
            async def read(conn: Any) -> bool | None:
                row = (await conn.execute(select(goal_continuations.c.status,
                                                goal_continuations.c.code).where(
                    goal_continuations.c.session_id == request.session_id,
                    goal_continuations.c.request_key == request.continue_key,
                ))).mappings().first()
                return True if row is not None and row["status"] == "failed" and (
                    row["code"] == code
                ) else None
            return await self.store.work_read(read)

        await self.store.work_write(write, confirm)

    async def _reconcile_goals(self) -> None:
        """One decision at a safe boundary of the existing dispatcher, never a new loop."""
        goals = self.goals
        if goals is None:
            return
        from sqlalchemy import func

        from krim_agent.goals.continuation import ContinuationRequest, TurnFinishedContext
        from krim_agent.storage.goal_models import goal_continuations, session_goals
        from krim_agent.storage.interaction_models import interaction_requests
        from krim_agent.storage.models import runs, sessions, turns

        async def read(conn: Any) -> tuple[
            builtins.list[dict[str, Any]], builtins.list[dict[str, Any]], builtins.list[str]
        ]:
            latest = select(
                turns.c.session_id, func.max(turns.c.ordinal).label("ordinal")
            ).group_by(turns.c.session_id).subquery()
            unsettled_internal = select(goal_continuations.c.request_key).where(
                goal_continuations.c.session_id == turns.c.session_id,
                goal_continuations.c.goal_id == session_goals.c.goal_id,
                goal_continuations.c.status == "queued",
                (turns.c.request_key == "goal-continue:" + goal_continuations.c.request_key)
                | (turns.c.request_key == "goal-resume:" + goal_continuations.c.request_key),
            ).exists()
            terminal = (await conn.execute(select(
                turns.c.id.label("turn_id"), turns.c.run_id,
                sessions.c.id.label("session_id"), sessions.c.owner_id, sessions.c.workspace_id,
                runs.c.status, runs.c.code, runs.c.stop_requested,
            ).select_from(
                turns.outerjoin(latest, (turns.c.session_id == latest.c.session_id)
                                & (turns.c.ordinal == latest.c.ordinal))
                .join(sessions, turns.c.session_id == sessions.c.id)
                .join(session_goals, session_goals.c.session_id == sessions.c.id)
                .join(runs, turns.c.run_id == runs.c.id)
            ).where(
                sessions.c.status == "open", sessions.c.dispatch_paused.is_(False),
                ((session_goals.c.status == "active") & latest.c.ordinal.is_not(None))
                | unsettled_internal,
                runs.c.status.in_(("completed", "failed", "aborted", "interrupted")),
            ))).mappings().all()
            queued = (await conn.execute(select(goal_continuations.c.payload).where(
                goal_continuations.c.status == "queued",
            ))).scalars().all()
            stopped = list(await conn.scalars(select(turns.c.run_id).join(
                session_goals, session_goals.c.session_id == turns.c.session_id,
            ).where(
                session_goals.c.status.in_(("paused", "cancelled")),
                turns.c.status == "running", turns.c.run_id.is_not(None),
                turns.c.request_key.startswith("goal-continue:")
                | turns.c.request_key.startswith("goal-resume:"),
            )))
            return [dict(row) for row in terminal], [dict(row) for row in queued], stopped

        terminal, queued, stopped = await self.store.work_read(read)
        for run_id in stopped:
            await self.runs.cancel(run_id)
        # Recovery of an intent committed before host enqueue (including resume).
        for payload in queued:
            request = ContinuationRequest.model_validate(payload)
            try:
                await self.enqueue(request)
            except ServiceError as error:
                if error.code in {"storage_unavailable", "closed"}:
                    raise
                if error.code not in {"session_busy", "capacity"}:
                    await self._fail_goal_enqueue(request, error.code)
        for item in terminal:
            task = self.runs.tasks.get(item["run_id"])
            if task is not None and not task.done():
                continue
            try:
                actor = self._owner_principal(item["owner_id"])
                actor.member(item["workspace_id"], write=True)
                async def waiting(conn: Any, session_id: str = item["session_id"]) -> bool:
                    return (await conn.scalar(select(interaction_requests.c.id).where(
                        interaction_requests.c.session_id == session_id,
                        interaction_requests.c.status == "pending",
                    ).limit(1))) is not None
                pending = await self.store.work_read(waiting)
                def check_actor(actor: PrincipalContext = actor) -> None:
                    self._live_principal(actor)
                    self._owner_principal(actor.principal_id)

                await goals.on_turn_finished(TurnFinishedContext(
                    actor=actor, workspace_id=item["workspace_id"],
                    session_id=item["session_id"], turn_id=item["turn_id"], run_id=item["run_id"],
                    status=item["status"], code=item["code"],
                    stop_requested=item["stop_requested"], pending_interaction=pending,
                    check=check_actor,
                ))
            except ServiceError as error:
                if error.code in {"storage_unavailable", "closed"}:
                    raise

    async def _dispatch(self) -> None:
        last_session: str | None = None
        while not self.closed:
            self.runs.slot_changed.clear()
            if self.store.healthy:
                try:
                    # Repair accepted newer messages before old answers can
                    # resume after a crash; reconcile owns the same gate itself.
                    await self._recover_input_supersessions()
                    await self.runs.reconcile_inputs()
                    # Goal callbacks may enqueue through this service and must
                    # acquire the gate themselves, not recursively under it.
                    await self._reconcile_goals()
                except ServiceError as error:
                    if error.code == "storage_unavailable":
                        self.failure = error.code
                        break
                candidates = [
                    (await sql.get_turn(self.store, tid)) for tid in await sql.queued(self.store)
                ]
                index = next(
                    (i for i, v in enumerate(candidates) if v.session_id == last_session), -1
                )
                candidates = candidates[index + 1 :] + candidates[: index + 1]
                for turn in candidates:
                    if self.closed:
                        break
                    try:
                        code = self._dispatch_block(turn)
                        if code is not None:
                            await sql.fail_queued(self.store, turn.id, code)
                            continue
                        async with self._input_lock:
                            if not await self._goal_turn_permitted(turn):
                                await sql.fail_queued(
                                    self.store, turn.id, "cancelled", status="cancelled"
                                )
                                continue
                            if self.goals is not None:
                                actor = self._owner_principal(turn.owner_id)
                                await self._supersede_goal_turns(actor, turn.workspace_id, turn)
                            run = await self.runs.start_turn(turn.id)
                        self._started = {
                            rid
                            for rid in self._started
                            if rid in self.runs.tasks and not self.runs.tasks[rid].done()
                        }
                        self._started.add(run.id)
                        last_session = turn.session_id
                    except ServiceError as error:
                        if error.code == "capacity":
                            continue
                        if error.code in {"closed", "storage_unavailable"}:
                            if not self.closed:
                                self.failure = error.code
                            break
                        await sql.fail_queued(self.store, turn.id, error.code)
            try:
                await asyncio.wait_for(self.runs.slot_changed.wait(), timeout=1)
            except TimeoutError:
                pass

    async def control(
        self,
        principal: PrincipalContext,
        workspace: str,
        session_id: str,
        kind: Literal["rename", "close", "resume", "cancel_turn", "approval"],
        body: ControlRequest,
        *,
        turn_id: str | None = None,
        approval: ApprovalReply | None = None,
        request_id: str | None = None,
    ) -> ControlView:
        _validate_control_arguments(kind, turn_id, approval, request_id)
        await self.get(
            principal, workspace, session_id, write=kind != "approval", approve=kind == "approval"
        )
        if turn_id is not None:
            await self.turn(principal, workspace, session_id, turn_id)
        payload: dict[str, JsonValue] = cast(dict[str, JsonValue], body.model_dump(mode="json"))
        if approval is not None:
            payload = cast(dict[str, JsonValue], approval.model_dump(mode="json")) | {
                "request_id": request_id
            }
        frozen = cast(dict[str, JsonValue], parse_json(encoded(payload)))
        key = session_id + ":" + body.request_key
        digest = fingerprint(
            {"kind": kind, "actor": principal.principal_id, "payload": frozen, "turn_id": turn_id}
        )
        if key in self.controls:
            if self._control_inputs[key] != digest:
                raise ServiceError("key_conflict")
            return await settled(self.controls[key])
        if self.closed:
            raise ServiceError("closed", 503)
        task = asyncio.create_task(
            self._control(
                session_id,
                kind,
                principal.principal_id,
                body,
                frozen,
                turn_id,
                approval,
                request_id,
            ),
            name="krim-session-control",
        )
        if len(self.controls) >= 128:
            for old_key, old_task in list(self.controls.items()):
                if old_task.done():
                    self.controls.pop(old_key)
                    self._control_inputs.pop(old_key)
        self.controls[key] = task
        self._control_inputs[key] = digest
        task.add_done_callback(lambda t: None if t.cancelled() else t.exception())
        return await settled(task)

    async def _join_session(self, session_id: str, turn_id: str | None = None) -> None:
        async def join(value: TurnView) -> None:
            if value.run_id is None:
                return
            if value.status == "running":
                await self.runs.cancel(value.run_id)
            task = self.runs.tasks.get(value.run_id)
            if task is not None and not task.done():
                await settled(task)

        offset, page_size = 0, 50
        first_error: BaseException | None = None
        while True:
            values = (
                [await sql.get_turn(self.store, turn_id)]
                if turn_id else await sql.list_turns(self.store, session_id, offset, page_size)
            )
            outcomes = await asyncio.gather(*(join(v) for v in values), return_exceptions=True)
            for outcome in outcomes:
                if isinstance(outcome, BaseException) and first_error is None:
                    first_error = outcome
            if turn_id or len(values) < page_size:
                if first_error is not None:
                    raise first_error
                return
            offset += len(values)

    async def _control(
        self,
        session_id: str,
        kind: str,
        actor: str,
        body: ControlRequest,
        payload: dict[str, JsonValue],
        turn_id: str | None,
        approval: ApprovalReply | None,
        request_id: str | None,
    ) -> ControlView:
        if kind == "rename" and not body.title:
            raise ServiceError("invalid_input", 422)
        try:
            control = await sql.begin_control(
                self.store,
                session_id,
                body.request_key,
                kind,
                actor,
                body.expected_revision,
                payload,
                turn_id,
            )
        except ServiceError as error:
            if error.code == "storage_unavailable" and kind in {"close", "cancel_turn"}:
                await self._join_session(session_id, turn_id)
            raise
        if control.status == "completed":
            return control
        if kind in {"close", "cancel_turn"}:
            await self._join_session(session_id, turn_id)
        if kind == "approval":
            assert turn_id is not None and approval is not None and request_id is not None
            view = await sql.get_turn(self.store, turn_id)
            if view.run_id is None:
                raise ServiceError("not_terminal")
            result = await self.runs.reply_approval(view.run_id, request_id, approval)
        elif turn_id:
            result = cast(
                dict[str, JsonValue],
                (await sql.get_turn(self.store, turn_id)).model_dump(mode="json"),
            )
        else:
            result = cast(
                dict[str, JsonValue],
                (await sql.get_session(self.store, session_id)).model_dump(mode="json"),
            )
            if kind == "close":
                result["status"] = "closed"
        complete = await sql.complete_control(self.store, control, result)
        self.runs.slot_changed.set()
        return complete

    async def close(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await settled(self._close_task)

    async def _close(self) -> None:
        self.closed = True
        self.runs.slot_changed.set()
        try:
            await self._dispatcher
        finally:
            try:
                outcomes = await asyncio.gather(
                    *(
                        self.runs.cancel(rid)
                        for rid in self._started
                        if rid in self.runs.tasks and not self.runs.tasks[rid].done()
                    ),
                    return_exceptions=True,
                )
                for result in outcomes:
                    if isinstance(result, BaseException):
                        raise result
            finally:
                await asyncio.gather(*self.controls.values(), return_exceptions=True)
