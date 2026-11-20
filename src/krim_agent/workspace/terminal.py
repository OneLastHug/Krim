"""Fail-closed terminal handoff for historical workspace runs."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Literal

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import RunStatus, ServiceError, fingerprint
from krim_agent.storage.models import run_events, runs, work_turns
from krim_agent.tools.pipeline import _PRODUCER
from krim_agent.workspace.types import ExecutionAccess, Verification

SCHEMA = "completion-verification/1"


@dataclass(frozen=True)
class VerificationIdentity:
    schema: str
    rules: str
    implementation_digest: str
    command_profile_digest: str
    limits_digest: str


@dataclass(frozen=True)
class ConditionResult:
    id: str
    kind: str
    result: Literal["passed", "failed", "unknown"]
    reason: str | None = None


@dataclass(frozen=True)
class VerificationResult:
    run_id: str
    turn_id: str
    session_id: str
    workspace_id: str
    actor_id: str
    grant_hash: str
    request_hash: str
    completion_hash: str
    conditions_hash: str
    effective_fingerprint: str
    admission_input_revision_id: str
    command_call_id: str | None
    command_args_hash: str | None
    command_input_revision_id: str | None
    output_revision_id: str | None
    output_manifest_hash: str | None
    receipt_hash: str | None
    observation_hash: str | None
    verifier_identity: VerificationIdentity
    state: Literal["passed", "failed", "unknown"]
    results: tuple[ConditionResult, ...]
    reason: str | None


@dataclass(eq=False)
class _Handoff:
    issuer: Any | None
    store: object
    producer: asyncio.Task[Any] | None
    owner: object
    access: ExecutionAccess | None
    result: VerificationResult
    check: Callable[[], None]
    run_deadline: float
    active: bool = True


@dataclass(eq=False)
class _TerminalAttempt:
    store: object
    conn: AsyncConnection | None
    run_id: str
    handoff: _Handoff | None
    stop_requested: bool = False
    negative_pair: tuple[RunStatus, str] | None = None
    active: bool = True


_HANDOFF: ContextVar[_Handoff | None] = ContextVar("krim-verification", default=None)
_ATTEMPT: ContextVar[_TerminalAttempt | None] = ContextVar("krim-terminal-attempt", default=None)


class VerificationCommitRejected(ServiceError):
    def __init__(self, attempt: _TerminalAttempt, pair: tuple[RunStatus, str]) -> None:
        if pair not in {
            ("aborted", "cancelled"),
            ("failed", "run_timeout"),
            ("failed", "target_binding_changed"),
            ("failed", "completion_unverified"),
        }:
            raise ValueError("无效终态否决")
        super().__init__(pair[1])
        self.attempt, self.pair = attempt, pair

    def issued_for(self, attempt: _TerminalAttempt) -> bool:
        return self.attempt is attempt and attempt.active

    def negative_pair(self) -> tuple[RunStatus, str]:
        return self.pair


def stage_result(
    result: VerificationResult,
    *,
    issuer: Any,
    store: object,
    producer: asyncio.Task[Any],
    access: ExecutionAccess,
    check: Callable[[], None],
    run_deadline: float,
) -> None:
    _TERMINAL_ASSERT()
    owner = _PRODUCER.get()
    if (
        owner is None
        or not owner.active
        or owner.task is not producer
        or producer is not asyncio.current_task()
        or owner.run_id != result.run_id
        or issuer.store is not store
        or issuer._issued.get(result.run_id) is not result
    ):
        raise ServiceError("completion_unverified")
    check()
    issuer.assert_current()
    _HANDOFF.set(_Handoff(issuer, store, producer, owner, access, result, check, run_deadline))


def private_handoff_for_same_owner_and_run(run_id: str) -> _Handoff | None:
    _TERMINAL_ASSERT()
    value = _HANDOFF.get()
    if value is None or value.result.run_id != run_id or not value.active:
        return None
    return value


async def terminal_attempt_same_conn(
    conn: AsyncConnection, store: object, run_id: str, handoff: _Handoff | None
) -> _TerminalAttempt | None:
    _TERMINAL_ASSERT()
    if getattr(store, "schema_version", None) not in range(4, 19):
        return None
    row = (
        await conn.execute(select(work_turns.c.conditions).where(work_turns.c.run_id == run_id))
    ).scalar_one_or_none()
    if (
        row is None
        or not isinstance(row.get("verification"), dict)
        or row["verification"].get("schema") != SCHEMA
    ):
        return None
    stopped = await conn.scalar(select(runs.c.stop_requested).where(runs.c.id == run_id))
    result = _TerminalAttempt(store, conn, run_id, handoff, stop_requested=bool(stopped))
    _ATTEMPT.set(result)
    return result


def terminal_commit_fence(
    attempt: _TerminalAttempt, *, selected_status: RunStatus, selected_code: str | None
) -> None:
    _TERMINAL_ASSERT()
    handoff = attempt.handoff

    def reject(status: RunStatus, code: str) -> None:
        raise VerificationCommitRejected(attempt, (status, code))

    if attempt.stop_requested:
        reject("aborted", "cancelled")
    if (
        handoff is None
        or not handoff.active
        or handoff.store is not attempt.store
        or handoff.producer is None
        or handoff.producer.done()
        or handoff.issuer is None
        or not getattr(handoff.owner, "active", False)
        or handoff.result.run_id != attempt.run_id
    ):
        reject("failed", "completion_unverified")
    assert handoff is not None
    assert handoff.producer is not None and handoff.issuer is not None
    if handoff.producer.cancelling():
        reject("aborted", "cancelled")
    if time.monotonic() >= handoff.run_deadline:
        reject("failed", "run_timeout")
    try:
        handoff.check()
        handoff.issuer.assert_current()
    except ServiceError as error:
        if error.code == "run_timeout":
            reject("failed", "run_timeout")
        if error.code in {
            "target_binding_changed",
            "tool_binding_changed",
            "context_binding_changed",
        }:
            reject("failed", "target_binding_changed")
        raise
    if (
        selected_status != "completed"
        or selected_code is not None
        or handoff.result.state != "passed"
    ):
        reject("failed", "completion_unverified")


def stage_negative_handoff_for_same_owner(
    attempt: _TerminalAttempt, rejection: VerificationCommitRejected
) -> None:
    _TERMINAL_ASSERT()
    if not rejection.issued_for(attempt):
        raise rejection
    attempt.negative_pair = rejection.negative_pair()


def terminal_commit_fence_for_negative_result(
    attempt: _TerminalAttempt, *, selected_status: RunStatus, selected_code: str | None
) -> None:
    _TERMINAL_ASSERT()
    if not attempt.active or selected_status == "completed":
        raise ServiceError("completion_unverified")
    if attempt.negative_pair is not None and attempt.negative_pair != (
        selected_status,
        selected_code,
    ):
        raise ServiceError("completion_unverified")


async def consume_terminal(conn: AsyncConnection, run_id: str, *, interrupted: bool) -> None:
    _TERMINAL_ASSERT()
    from krim_agent.storage import work

    attempt = _ATTEMPT.get()
    frame = await work.load(conn, run_id)
    if frame is None:
        return
    status = (
        await conn.execute(select(runs.c.status, runs.c.code).where(runs.c.id == run_id))
    ).one()
    if status.status == "completed":
        if attempt is None or attempt.conn is not conn or attempt.run_id != run_id:
            raise ServiceError("completion_unverified")
        _TERMINAL_HELPERS[1](attempt, selected_status=status.status, selected_code=status.code)
        handoff = attempt.handoff
        assert handoff is not None and handoff.access is not None
        proof = handoff.result
        if proof.command_call_id is None or proof.output_revision_id is None:
            raise VerificationCommitRejected(attempt, ("failed", "completion_unverified"))
        if (
            proof.turn_id != frame.turn_id
            or proof.session_id != frame.session_id
            or proof.request_hash != fingerprint(frame.original_request.model_dump(mode="json"))
            or proof.conditions_hash != fingerprint(frame.conditions)
            or proof.effective_fingerprint != frame.effective_fingerprint
        ):
            raise VerificationCommitRejected(attempt, ("failed", "completion_unverified"))
        effect = await work.effect(conn, handoff.access, proof.command_call_id)
        head = await work.row(conn, frame.session_id, handoff.access)
        revision = await work.get_revision(
            conn, frame.session_id, proof.output_revision_id, handoff.access
        )
        observation = await conn.scalar(
            select(run_events.c.data).where(
                run_events.c.run_id == run_id,
                run_events.c.event_id
                == "workspace-command-observation:" + str(proof.command_call_id),
            )
        )
        if (
            effect is None
            or fingerprint(effect["receipt"]) != proof.receipt_hash
            or effect["args_hash"] != proof.command_args_hash
            or effect["input_revision"] != proof.command_input_revision_id
            or head["head_revision"] != revision.revision_id
            or revision.manifest_hash != proof.output_manifest_hash
            or revision.parent_id != proof.command_input_revision_id
            or revision.producer_run_id != run_id
            or revision.producer_call_id != proof.command_call_id
            or observation is None
            or fingerprint(observation) != proof.observation_hash
            or fingerprint(handoff.access.grant().model_dump(mode="json")) != proof.grant_hash
        ):
            raise VerificationCommitRejected(attempt, ("failed", "completion_unverified"))
        value = Verification(
            state="passed",
            revision_id=proof.output_revision_id,
            command_call_id=proof.command_call_id,
            results=tuple(
                {"id": r.id, "kind": r.kind, "result": r.result, "reason": r.reason}
                for r in proof.results
            ),
        )
    else:
        failed_proof = (
            attempt.handoff.result
            if (
                attempt is not None
                and attempt.handoff is not None
                and attempt.handoff.active
                and attempt.handoff.result.state != "passed"
                and attempt.handoff.result.run_id == run_id
                and attempt.handoff.result.turn_id == frame.turn_id
                and status.code == "completion_unverified"
            )
            else None
        )
        value = Verification(
            state=failed_proof.state
            if failed_proof
            else (
                "unknown"
                if interrupted
                or status.code in {"cancelled", "run_timeout", "target_binding_changed"}
                else "failed"
            ),
            revision_id=failed_proof.output_revision_id if failed_proof else None,
            command_call_id=failed_proof.command_call_id if failed_proof else None,
            results=tuple(
                {"id": item.id, "kind": item.kind, "result": item.result, "reason": item.reason}
                for item in failed_proof.results
            )
            if failed_proof
            else tuple(
                {
                    "id": item.id,
                    "kind": item.kind,
                    "result": "unknown",
                    "reason": "completion_unverified",
                }
                for item in frame.original_request.completion.conditions
            ),
        )
    await conn.execute(
        update(work_turns)
        .where(work_turns.c.run_id == run_id)
        .values(verification=value.model_dump(mode="json"))
    )
    if status.status == "completed":
        assert attempt is not None
        _TERMINAL_HELPERS[1](attempt, selected_status=status.status, selected_code=status.code)


def clear_handoff_at_owner_end(attempt: _TerminalAttempt) -> None:
    attempt.active = False
    if attempt.handoff is not None:
        handoff = attempt.handoff
        handoff.active = False
        if handoff.issuer is not None:
            handoff.issuer._issued.pop(attempt.run_id, None)
        handoff.issuer, handoff.store, handoff.producer = None, None, None
        handoff.owner, handoff.access = None, None
        handoff.check = lambda: None
    if _ATTEMPT.get() is attempt:
        _ATTEMPT.set(None)
    if _HANDOFF.get() is attempt.handoff:
        _HANDOFF.set(None)
    attempt.handoff, attempt.conn, attempt.store = None, None, None


_TERMINAL_HELPERS = (
    terminal_attempt_same_conn,
    terminal_commit_fence,
    stage_negative_handoff_for_same_owner,
    terminal_commit_fence_for_negative_result,
    clear_handoff_at_owner_end,
    consume_terminal,
)


def assert_current() -> None:
    actual = tuple(
        (globals().get(name), getattr(globals().get(name), "__code__", None))
        for name in _TERMINAL_NAMES
    )
    if actual != _TERMINAL_TOKEN:
        raise ServiceError("target_binding_changed")


_TERMINAL_ASSERT = assert_current
_TERMINAL_NAMES = (
    "stage_result",
    "private_handoff_for_same_owner_and_run",
    "terminal_attempt_same_conn",
    "terminal_commit_fence",
    "stage_negative_handoff_for_same_owner",
    "terminal_commit_fence_for_negative_result",
    "consume_terminal",
    "clear_handoff_at_owner_end",
    "assert_current",
    "_TERMINAL_ASSERT",
)
_TERMINAL_TOKEN = tuple(
    (globals().get(name), getattr(globals().get(name), "__code__", None))
    for name in _TERMINAL_NAMES
)
