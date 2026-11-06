"""一个dispatcher；实际producer/容量仍归RunService。"""

from __future__ import annotations

import asyncio
import base64
import binascii
import builtins
import json
import math
import re
from typing import Literal, cast
from uuid import UUID

from pydantic import JsonValue

from krim_agent.config import Settings
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import (
    ApprovalReply,
    RunStatus,
    ServiceError,
    encoded,
    fingerprint,
    parse_json,
)
from krim_agent.providers.openai_compatible import OpenAICompatibleProvider
from krim_agent.runtime.contracts import settled
from krim_agent.runtime.service import RunService
from krim_agent.security import PrincipalContext
from krim_agent.storage import sessions as sql
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.workspace.types import ExecutionAccess, WorkspaceTurnRequest

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
_TRACE_STATUSES = frozenset(
    {"queued", "running", "completed", "failed", "aborted", "interrupted"}
)
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
    if turn_id is not None and (
        not isinstance(turn_id, str) or not turn_id or len(turn_id) > 128
    ):
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
        identities: object | None = None,
    ) -> None:
        self.store, self.runs, self.settings = store, runs, settings
        # 稳定身份服务引用；托管模式由composition注入，legacy保持None。
        self.identities = identities
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
        return session

    async def by_key(self, principal: PrincipalContext, workspace: str, key: str) -> SessionView:
        principal = self._live_principal(principal)
        principal.member(workspace)
        return await sql.by_key(self.store, workspace, principal.principal_id, key)

    async def list(
        self, principal: PrincipalContext, workspace: str, offset: int = 0, limit: int = 20
    ) -> list[SessionView]:
        principal = self._live_principal(principal)
        membership = principal.member(workspace)
        if not 0 <= offset <= 100000 or not 1 <= limit <= 50:
            raise ServiceError("invalid_input", 422)
        return await sql.list_sessions(
            self.store, workspace, principal.principal_id, membership.role == "admin", offset, limit
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
            )
        binding = self.runs.bindings.get(body.target_key)
        if binding is None:
            raise ServiceError("invalid_input", 422)
        binding.assert_current()
        output = (
            binding.loop.provider.config.max_output_tokens
            if isinstance(binding.loop.provider, OpenAICompatibleProvider)
            else 2048
        )
        inherited = ContextPolicy(
            max_bytes=min(self.settings.context_max_bytes, binding.settings.max_request_bytes),
            max_messages=self.settings.context_max_messages,
            reserved_output_tokens=max(self.settings.context_reserved_output_tokens or 0, output),
        )
        policy = body.context_policy or inherited
        if (
            policy.max_bytes > inherited.max_bytes
            or policy.max_messages > inherited.max_messages
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
        )

    async def submit(
        self,
        principal: PrincipalContext,
        workspace: str,
        session_id: str,
        request: TurnRequest | WorkspaceTurnRequest,
    ) -> TurnView:
        self._admitting()
        if isinstance(request, WorkspaceTurnRequest):
            # 工作请求的嵌套expected必须在首await前脱离调用者；legacy顺序不变。
            request = WorkspaceTurnRequest.model_validate_json(request.model_dump_json())
        await self.get(principal, workspace, session_id, write=True)
        # 入队前实时复核：边界认证与本次写入之间被停用/降权的主体不再排队。
        principal = self._live_principal(principal)
        body = (
            WorkspaceTurnRequest.model_validate_json(request.model_dump_json())
            if isinstance(request, WorkspaceTurnRequest)
            else TurnRequest.model_validate_json(request.model_dump_json())
        )
        if len(body.text.encode()) > self.settings.max_input_bytes:
            raise ServiceError("input_limit", 422)
        result = await sql.submit(
            self.store,
            session_id,
            body,
            self.settings,
            access=ExecutionAccess(principal, workspace, session_id)
            if isinstance(body, WorkspaceTurnRequest)
            else None,
        )
        self.runs.slot_changed.set()
        return result

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
        if not 0 <= offset <= 100000 or not 1 <= limit <= 50:
            raise ServiceError("invalid_input", 422)
        return await sql.list_turns(self.store, session_id, offset, limit)

    async def wait(
        self, principal: PrincipalContext, workspace: str, session_id: str, turn_id: str
    ) -> TurnView:
        async with asyncio.timeout(
            self.settings.session_queue_timeout + self.settings.run_timeout + 30
        ) as reader_timeout:
            while True:
                value = await self.turn(principal, workspace, session_id, turn_id)
                if value.status not in {"queued", "running"}:
                    return value
                producer = self.runs.tasks.get(value.run_id) if value.run_id is not None else None
                if producer is not None and not producer.done():
                    # 运行与审批有自己的预算，读者不再用旧墙钟期限提前退出。
                    if reader_timeout.when() is not None:
                        reader_timeout.reschedule(None)
                else:
                    if reader_timeout.when() is None:
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
            for run_id, task in list(self.runs.tasks.items()):
                if task.done():
                    continue
                try:
                    source = (await self.runs.store.snapshot(run_id)).session_context
                except ServiceError:
                    continue  # 快照不可得时无法归属，不猜着取消别人的Run。
                if source is None:
                    continue
                session = await sql.get_session(self.store, source.session_id)
                if session.owner_id != principal_id:
                    continue
                try:
                    await self.runs.cancel(run_id)
                    cancelled_runs.append(run_id)
                except ServiceError as error:
                    failures.append(error)
            if failures:
                raise failures[0]
            return {
                "principal_id": principal_id,
                "failed_queued": failed_queued,
                "cancelled_runs": cancelled_runs,
            }
        finally:
            self._revoked.discard(principal_id)

    async def _dispatch(self) -> None:
        last_session: str | None = None
        while not self.closed:
            self.runs.slot_changed.clear()
            if self.store.healthy:
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
        values = (
            [await sql.get_turn(self.store, turn_id)]
            if turn_id
            else await sql.list_turns(self.store, session_id, 0, self.settings.session_max_turns)
        )

        async def join(value: TurnView) -> None:
            if value.run_id is None:
                return
            if value.status == "running":
                await self.runs.cancel(value.run_id)
            task = self.runs.tasks.get(value.run_id)
            if task is not None and not task.done():
                await settled(task)

        outcomes = await asyncio.gather(*(join(v) for v in values), return_exceptions=True)
        for value in outcomes:
            if isinstance(value, BaseException):
                raise value

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
