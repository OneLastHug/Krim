"""Durable questions and owner-confirmed plan mode, independent of a run loop."""

from __future__ import annotations

import time
import uuid
from collections.abc import Awaitable, Callable, Collection
from copy import deepcopy
from typing import TYPE_CHECKING, Any, TypeVar

from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.security import PrincipalContext
from krim_agent.storage import interaction as sql
from krim_agent.storage.interaction_models import interaction_mutations, interaction_requests
from krim_agent.storage.models import sessions
from krim_agent.tools.session import SessionScopeResolver, SessionToolScope, ToolBundle

from .contracts import (
    AnswerRequest,
    AskUserArgs,
    CancelRequest,
    EnterPlanModeArgs,
    ExitPlanModeArgs,
    RequestPending,
)

if TYPE_CHECKING:
    from krim_agent.accounts.service import IdentityService
    from krim_agent.storage.sqlite import SQLiteStore
    from krim_agent.tools.core import ToolSpec

T = TypeVar("T")
AnswerNotifier = Callable[[dict[str, Any]], Awaitable[None]]
TRUSTED_RESOURCE_NAMES = frozenset({
    "list_mcp_resources", "list_mcp_resource_templates", "read_mcp_resource"
})


def plan_allows(spec: ToolSpec, trusted_readonly_names: Collection[str] = ()) -> bool:
    """Allow host reads, internal planning, and explicitly trusted MCP reads.

    The host must bind the allowlist to its current authorized catalog. A
    remote server's read-only annotation alone must never populate this list.
    Existing source and binding policies still apply to any allowed tool.
    """
    if spec.source is not None and spec.source.kind == "mcp":
        return spec.risk == "read" and spec.name in trusted_readonly_names
    if spec.name in TRUSTED_RESOURCE_NAMES:
        return spec.risk == "read" and spec.name in trusted_readonly_names
    if spec.name == "plan_update" and spec.source is None and spec.capability == "plan":
        return True
    if spec.capability == "session_state":
        return spec.name in {
            "ask_user", "enter_plan_mode", "exit_plan_mode", "plan_update", "compact_context",
            "goal_update",
        }
    return spec.risk == "read"


class InteractionService:
    def __init__(
        self,
        store: SQLiteStore,
        identities: IdentityService | None = None,
        *,
        trusted_readonly_names: Collection[str] = (),
        on_answer: AnswerNotifier | None = None,
    ) -> None:
        self.store, self.identities = store, identities
        self.trusted_readonly_names = frozenset(trusted_readonly_names)
        self.on_answer = on_answer

    def actor(self, actor: PrincipalContext) -> PrincipalContext:
        return self.identities.validate(actor) if self.identities is not None else actor

    async def _session(
        self,
        conn: AsyncConnection,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        *,
        write: bool = False,
        allow_closed: bool = False,
    ) -> Any:
        self.actor(actor).member(workspace_id, write=write)
        row = (
            await conn.execute(select(sessions).where(
                sessions.c.id == session_id, sessions.c.workspace_id == workspace_id
            ))
        ).mappings().first()
        # Questions belong to the user, including against workspace admins.
        if row is None or row["owner_id"] != actor.principal_id:
            raise ServiceError("not_found", 404)
        if write and not allow_closed and row["status"] != "open":
            raise ServiceError("session_closed", 409)
        return row

    async def _read(
        self,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        operation: Callable[[AsyncConnection], Awaitable[T]],
        *,
        write: bool = False,
    ) -> T:
        self.actor(actor)

        async def read(conn: AsyncConnection) -> T:
            await self._session(conn, actor, workspace_id, session_id, write=write)
            result = await operation(conn)
            self.actor(actor)
            return result

        result = await self.store.work_read(read)
        self.actor(actor)
        return result

    async def _mutate(
        self,
        actor: PrincipalContext,
        workspace_id: str,
        session_id: str,
        request_key: str,
        action: str,
        body: dict[str, Any],
        operation: Callable[[AsyncConnection], Awaitable[dict[str, Any]]],
        check: Callable[[], None] = lambda: None,
    ) -> dict[str, Any]:
        if not request_key or len(request_key) > 128 or any(
            ord(char) < 33 or ord(char) > 126 for char in request_key
        ):
            raise ServiceError("invalid_input", 422)
        self.actor(actor)
        check()
        digest = fingerprint({"action": action, "body": body})

        async def known(conn: AsyncConnection) -> dict[str, Any] | None:
            # Authorization is checked even for idempotent retries.
            await self._session(
                conn, actor, workspace_id, session_id, write=True, allow_closed=True
            )
            check()
            row = (
                await conn.execute(select(interaction_mutations).where(
                    interaction_mutations.c.session_id == session_id,
                    interaction_mutations.c.request_key == request_key,
                ))
            ).mappings().first()
            if row is None:
                return None
            if row["request_hash"] != digest:
                raise ServiceError("key_conflict", 409)
            return dict(row["result"])

        async def write(conn: AsyncConnection) -> dict[str, Any]:
            previous = await known(conn)
            if previous is not None:
                return previous
            await self._session(conn, actor, workspace_id, session_id, write=True)
            result = await operation(conn)
            self.actor(actor)
            check()
            await conn.execute(insert(interaction_mutations).values(
                session_id=session_id, request_key=request_key, request_hash=digest, result=result
            ))
            return result

        async def confirm() -> dict[str, Any] | None:
            return await self._read(actor, workspace_id, session_id, known)

        result = await self.store.work_write(write, confirm)
        self.actor(actor)
        check()
        return result

    @staticmethod
    async def _state(conn: AsyncConnection, session_id: str) -> dict[str, Any]:
        current = await sql.mode(conn, session_id)
        return {
            "mode": current["mode"], "revision": current["revision"],
            "pending": await sql.pending(conn, session_id),
        }

    async def state(
        self, actor: PrincipalContext, workspace_id: str, session_id: str
    ) -> dict[str, Any]:
        return await self._read(
            actor, workspace_id, session_id, lambda conn: self._state(conn, session_id)
        )

    async def read_request(
        self, actor: PrincipalContext, workspace_id: str, session_id: str, request_id: str
    ) -> dict[str, Any]:
        async def read(conn: AsyncConnection) -> dict[str, Any]:
            item = await sql.request(conn, request_id, session_id)
            if item is None:
                raise ServiceError("not_found", 404)
            return item
        return await self._read(actor, workspace_id, session_id, read)

    @staticmethod
    async def _resume_fence(conn: AsyncConnection, item: dict[str, Any]) -> None:
        if item["kind"] != "plan_confirmation" or item["status"] != "answered":
            return
        confirmed = item["response"]["confirmed"]
        current = await sql.mode(conn, item["session_id"])
        expected_mode = "execute" if confirmed else "plan"
        expected_revision = item["mode_revision"] + (1 if confirmed else 0)
        if current["mode"] != expected_mode or current["revision"] != expected_revision:
            raise ServiceError("input_revision_conflict", 409)

    async def resume(
        self, actor: PrincipalContext, workspace_id: str, session_id: str, request_id: str
    ) -> dict[str, Any]:
        async def read(conn: AsyncConnection) -> dict[str, Any]:
            item = await sql.request(conn, request_id, session_id)
            if item is None:
                raise ServiceError("not_found", 404)
            return item
        async def checked(conn: AsyncConnection) -> dict[str, Any]:
            item = await read(conn)
            await self._resume_fence(conn, item)
            return item
        result = await self._read(actor, workspace_id, session_id, checked, write=True)
        if result["status"] == "pending":
            raise RequestPending(result)
        return result

    async def _request(
        self,
        scope: SessionToolScope,
        payload: dict[str, Any],
        request_key: str,
        kind: str,
    ) -> dict[str, Any]:
        async def create(conn: AsyncConnection) -> dict[str, Any]:
            current = await sql.mode(conn, scope.session_id)
            if kind == "plan_confirmation" and current["mode"] != "plan":
                raise ServiceError("input_revision_conflict", 409)
            # A newer question replaces an old waiting request, never its answer.
            for old in await sql.pending(conn, scope.session_id):
                await sql.settle(conn, old, "superseded", {"reason": "new_request"})
            now = time.time()
            result = {
                "id": str(uuid.uuid4()), "workspace_id": scope.workspace_id,
                "session_id": scope.session_id, "owner_id": scope.actor.principal_id,
                "turn_id": scope.turn_id, "run_id": scope.run_id, "request_key": request_key,
                "kind": kind, "status": "pending", "revision": 1,
                "mode_revision": current["revision"], "payload": payload, "response": None,
                "created_at": now, "updated_at": now,
            }
            await conn.execute(insert(interaction_requests).values(**result))
            return result

        result = await self._mutate(
            scope.actor, scope.workspace_id, scope.session_id, request_key, kind,
            {"payload": payload, "turn_id": scope.turn_id, "run_id": scope.run_id}, create,
            scope.check,
        )
        # Replay the current answer instead of the originally pending snapshot.
        async def read(conn: AsyncConnection) -> dict[str, Any]:
            item = await sql.request(conn, result["id"], scope.session_id)
            if item is None:
                raise ServiceError("facts_corrupt")
            await self._resume_fence(conn, item)
            return item
        current = await self._read(
            scope.actor, scope.workspace_id, scope.session_id, read, write=True
        )
        scope.check()
        return current

    async def ask(
        self, scope: SessionToolScope, body: AskUserArgs, request_key: str
    ) -> dict[str, Any]:
        return await self._request(scope, body.model_dump(mode="json"), request_key, "user_input")

    async def enter_plan_mode(
        self, scope: SessionToolScope, body: EnterPlanModeArgs, request_key: str
    ) -> dict[str, Any]:
        async def enter(conn: AsyncConnection) -> dict[str, Any]:
            # Re-entering invalidates an outstanding execution proposal.
            for old in await sql.pending(conn, scope.session_id):
                if old["kind"] == "plan_confirmation":
                    await sql.settle(conn, old, "superseded", {"reason": "plan_revised"})
            await sql.set_mode(conn, scope.session_id, "plan")
            return await self._state(conn, scope.session_id)
        return await self._mutate(
            scope.actor, scope.workspace_id, scope.session_id, request_key, "enter_plan_mode",
            {"payload": body.model_dump(mode="json"), "turn_id": scope.turn_id,
             "run_id": scope.run_id}, enter, scope.check,
        )

    async def exit_plan_mode(
        self, scope: SessionToolScope, body: ExitPlanModeArgs, request_key: str
    ) -> dict[str, Any]:
        return await self._request(
            scope, body.model_dump(mode="json"), request_key, "plan_confirmation"
        )

    @staticmethod
    def _answers(item: dict[str, Any], body: AnswerRequest) -> dict[str, Any]:
        if item["kind"] == "plan_confirmation":
            if body.confirmed is None or body.answers:
                raise ServiceError("invalid_input", 422)
            return {"confirmed": body.confirmed}
        if body.confirmed is not None:
            raise ServiceError("invalid_input", 422)
        questions = {q["id"]: q for q in item["payload"]["questions"]}
        if len(body.answers) != len(questions) or {
            answer.question_id for answer in body.answers
        } != set(questions):
            raise ServiceError("invalid_input", 422)
        for answer in body.answers:
            question = questions[answer.question_id]
            options = set(answer.option_ids)
            available = {option["id"] for option in question["options"]}
            text = bool(answer.text and answer.text.strip())
            if (
                not options <= available
                or (question["mode"] == "single" and len(options) > 1)
                or (question["mode"] == "text" and (options or not text))
                or (question["mode"] != "text" and text and not question["allow_text"])
                or (not options and not text)
            ):
                raise ServiceError("invalid_input", 422)
        return {"answers": [answer.model_dump(mode="json") for answer in body.answers]}

    async def _notify_answer(self, result: dict[str, Any]) -> None:
        if self.on_answer is None:
            return
        try:
            await self.on_answer(deepcopy(result))
        except Exception:
            # Persistence is authoritative; a transient wake-up failure is
            # recovered by the host's durable pending scan/checkpoint restore.
            return

    @staticmethod
    async def _answer_fence(conn: AsyncConnection, item: dict[str, Any]) -> None:
        from krim_agent.storage.models import turns

        original = await conn.scalar(select(turns.c.ordinal).where(
            turns.c.id == item["turn_id"], turns.c.session_id == item["session_id"],
        ))
        if original is None:
            return
        later = await conn.scalar(select(turns.c.id).where(
            turns.c.session_id == item["session_id"], turns.c.ordinal > original,
        ).limit(1))
        if later is not None:
            raise ServiceError("input_revision_conflict", 409)

    async def answer(
        self, actor: PrincipalContext, workspace_id: str, session_id: str,
        request_id: str, body: AnswerRequest,
    ) -> dict[str, Any]:
        async def answer(conn: AsyncConnection) -> dict[str, Any]:
            item = await sql.request(conn, request_id, session_id)
            if item is None:
                raise ServiceError("not_found", 404)
            if item["revision"] != body.expected_revision or item["status"] != "pending":
                raise ServiceError("input_revision_conflict", 409)
            await self._answer_fence(conn, item)
            response = self._answers(item, body)
            if item["kind"] == "plan_confirmation":
                current = await sql.mode(conn, session_id)
                if current["mode"] != "plan" or current["revision"] != item["mode_revision"]:
                    raise ServiceError("input_revision_conflict", 409)
                if body.confirmed:
                    await sql.set_mode(conn, session_id, "execute")
            return await sql.settle(conn, item, "answered", response)
        result = await self._mutate(
            actor, workspace_id, session_id, body.request_key, "answer",
            {"request_id": request_id, **body.model_dump(mode="json")}, answer,
        )
        await self._notify_answer(result)
        return result

    async def cancel(
        self, actor: PrincipalContext, workspace_id: str, session_id: str,
        request_id: str, body: CancelRequest,
    ) -> dict[str, Any]:
        async def cancel(conn: AsyncConnection) -> dict[str, Any]:
            item = await sql.request(conn, request_id, session_id)
            if item is None:
                raise ServiceError("not_found", 404)
            if item["revision"] != body.expected_revision or item["status"] != "pending":
                raise ServiceError("input_revision_conflict", 409)
            return await sql.settle(conn, item, "cancelled", {"reason": body.reason})
        result = await self._mutate(
            actor, workspace_id, session_id, body.request_key, "cancel",
            {"request_id": request_id, **body.model_dump(mode="json")}, cancel,
        )
        await self._notify_answer(result)
        return result

    async def supersede(
        self, actor: PrincipalContext, workspace_id: str, session_id: str, request_key: str,
        *, before_turn_id: str | None = None,
    ) -> dict[str, Any]:
        """Supersede only questions preceding a successfully admitted user turn."""
        async def supersede(conn: AsyncConnection) -> dict[str, Any]:
            cutoff: int | None = None
            if before_turn_id is not None:
                from krim_agent.storage.models import turns

                cutoff = await conn.scalar(select(turns.c.ordinal).where(
                    turns.c.id == before_turn_id, turns.c.session_id == session_id,
                ))
                if cutoff is None:
                    raise ServiceError("not_found", 404)
                # Replaying an older admitted message cannot invalidate a
                # newer turn's question or plan confirmation mode fence.
                later = await conn.scalar(select(turns.c.id).where(
                    turns.c.session_id == session_id, turns.c.ordinal > cutoff,
                ).limit(1))
                if later is not None:
                    return await self._state(conn, session_id)
            for old in await sql.pending(conn, session_id):
                if cutoff is not None:
                    ordinal = await conn.scalar(select(turns.c.ordinal).where(
                        turns.c.id == old["turn_id"], turns.c.session_id == session_id,
                    ))
                    if ordinal is None or ordinal >= cutoff:
                        continue
                await sql.settle(conn, old, "superseded", {"reason": "user_message"})
            # Also fence a confirmed proposal racing the new user message.
            current = await sql.mode(conn, session_id)
            if current["revision"] > 0:
                await sql.set_mode(conn, session_id, current["mode"])
            return await self._state(conn, session_id)
        return await self._mutate(
            actor, workspace_id, session_id, request_key, "supersede",
            {} if before_turn_id is None else {"before_turn_id": before_turn_id}, supersede,
        )

    async def permits(
        self,
        scope: SessionToolScope,
        spec: ToolSpec,
        *,
        trusted_readonly_names: Collection[str] | None = None,
    ) -> bool:
        scope.check()
        current = await self.state(scope.actor, scope.workspace_id, scope.session_id)
        scope.check()
        trusted = (
            self.trusted_readonly_names
            if trusted_readonly_names is None
            else frozenset(trusted_readonly_names)
        )
        return current["mode"] != "plan" or plan_allows(spec, trusted)

    def bundle(self, resolve: SessionScopeResolver) -> ToolBundle:
        from .tools import interaction_bundle
        return interaction_bundle(self, resolve)
