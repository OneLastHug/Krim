"""有界完成核验；借原Store/Artifact，不拥有执行、连接、队列或后台任务。"""

from __future__ import annotations

import ast
import asyncio
import base64
import hashlib
import re
import sys
import time
import weakref
from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Literal, cast

from pydantic import JsonValue
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import RunStatus, ServiceError, encoded, fingerprint, parse_json
from krim_agent.runtime.contracts import settled
from krim_agent.source_fingerprints import source_digest
from krim_agent.storage.models import run_events, runs, tool_effects, work_turns
from krim_agent.tools.pipeline import _PRODUCER
from krim_agent.workspace.types import ExecutionAccess, Verification, WorkFrame

SCHEMA = "completion-verification/1"
COMMAND = ("-m", "unittest", "discover", "-s", ".", "-p", "test_*.py")
LIMITS = {
    "seconds": 5,
    "file_bytes": 65536,
    "total_bytes": 1048576,
    "conditions": 16,
    "ast_nodes": 50000,
    "depth": 128,
    "observation_bytes": 4096,
}


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
    issuer: Verifier | None
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


class ObservationRejected(ServiceError):
    """只限可选观察的语义拒绝；不吞数据库/事务错误。"""


def descriptor() -> dict[str, JsonValue]:
    return {
        "schema": SCHEMA,
        "rules": "python-unittest/1+strict-json/1+bounded-ast/1",
        "implementation_digest": source_digest(__file__),
        "command_profile_digest": fingerprint({"argv": list(COMMAND), "cwd": "."}),
        "limits_digest": fingerprint(LIMITS),
    }


def _module_current() -> None:
    # 直接看当前符号，不调用可能已被替换的binding_token或解析helper。
    actual = tuple(
        (globals().get(name), getattr(globals().get(name), "__code__", None)) for name in _NAMES
    )
    if actual != _MODULE_TOKEN:
        raise ServiceError("target_binding_changed")


def binding_token() -> tuple[object, ...]:
    return tuple(
        (globals().get(name), getattr(globals().get(name), "__code__", None)) for name in _NAMES
    ) + (source_digest(__file__),)


def _condition(
    kind: str, content: bytes, *, pointer: str, expected: JsonValue, symbols: tuple[str, ...]
) -> bool:
    if kind == "json_equal":
        value = parse_json(content.decode("utf-8"))
        if len(pointer.encode()) > 256 or (pointer and not pointer.startswith("/")):
            raise ValueError("pointer无效")
        parts = pointer.split("/")[1:] if pointer else []
        if len(parts) > 16:
            raise ValueError("pointer过深")
        for part in parts:
            if re.search(r"~(?![01])", part):
                raise ValueError("pointer转义无效")
            key = part.replace("~1", "/").replace("~0", "~")
            if type(value) is dict:
                value = value[key]
            elif type(value) is list and re.fullmatch(r"0|[1-9][0-9]*", key):
                value = value[int(key)]
            else:
                raise ValueError("pointer类型无效")
        # 规范JSON保留bool/number及整数/浮点表示差别，不用True == 1。
        return encoded(value) == encoded(expected)
    tree = ast.parse(content.decode("utf-8"))
    pending: list[tuple[ast.AST, int]] = [(tree, 0)]
    count = 0
    while pending:
        node, depth = pending.pop()
        count += 1
        if count > LIMITS["ast_nodes"] or depth > LIMITS["depth"]:
            raise ValueError("AST超限")
        pending.extend((child, depth + 1) for child in ast.iter_child_nodes(node))
    names = {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    }
    return set(symbols).issubset(names)


def _command_events(events: list[Any], effect: Any, observation: Any, receipt: Any) -> bool:
    from krim_agent.tools.workspace import Command

    def records(kind: str) -> list[Any]:
        return [
            item["data"]
            for item in events
            if item["kind"] == kind and item["data"].get("call_id") == effect["call_id"]
        ]

    starts, ends = records("tool_start"), records("tool_end")
    approvals = records("approval_request")
    if len(starts) != 1 or len(ends) != 1 or len(approvals) != 1:
        return False
    start, end, approval = starts[0], ends[0], approvals[0]
    arguments = start.get("arguments")
    try:
        normalized = Command.model_validate_json(encoded(arguments)).model_dump(mode="json")
    except ValueError:
        return False
    raw_hash = fingerprint(arguments)
    replies = [
        item["data"].get("reply")
        for item in events
        if item["kind"] == "approval_reply"
        and item["data"].get("request_id") == approval.get("request_id")
    ]
    return (
        start.get("name") == end.get("name") == approval.get("name") == "command_run"
        and end.get("arguments") == arguments
        and approval.get("arguments") == arguments
        and fingerprint(normalized) == effect["args_hash"]
        and observation.get("invocation_args_hash") == raw_hash
        and approval.get("arguments_hash") == raw_hash
        and approval.get("tool_hash") == effect["tool_hash"]
        and len(replies) == 1
        and isinstance(replies[0], dict)
        and replies[0].get("allow") is True
        and replies[0].get("call_id") == effect["call_id"]
        and replies[0].get("arguments_hash") == raw_hash
        and replies[0].get("tool_hash") == effect["tool_hash"]
        and end.get("status") == "ok"
        and end.get("decision") == "ask"
        and end.get("executor_started") is True
        and end.get("result_available") is True
        and isinstance(end.get("value"), dict)
        and end["value"].get("revision_id") == receipt.get("artifact_revision_id")
        and any(
            item["kind"] == "workspace_started" and item["data"].get("call_id") == effect["call_id"]
            for item in events
        )
    )


class Verifier:
    def __init__(self, store: Any, artifacts: Any) -> None:
        if artifacts.store is not store:
            raise ServiceError("workspace_unavailable")
        self.store, self.artifacts = store, artifacts
        self._module = sys.modules[__name__]
        self._token = binding_token()
        self._verify_function = type(self).verify
        # 同一函数对象也能被替换__code__；保留实际对象引用，避免只比较id。
        self._method_tokens = tuple(
            (name, method, getattr(method, "__code__", None))
            for name in ("verify", "descriptor", "binding_token", "assert_current")
            for method in (getattr(getattr(self, name), "__func__", getattr(self, name)),)
        )
        self._artifact_methods = tuple(
            (name, getattr(getattr(artifacts, name), "__func__", getattr(artifacts, name)))
            for name in ("get", "head", "chunk")
        )
        self._issued: weakref.WeakValueDictionary[str, VerificationResult] = (
            weakref.WeakValueDictionary()
        )

    def descriptor(self) -> dict[str, JsonValue]:
        return descriptor()

    def binding_token(self) -> tuple[object, ...]:
        actual = tuple(
            (name, method, getattr(method, "__code__", None))
            for name in ("verify", "descriptor", "binding_token", "assert_current")
            for method in (getattr(getattr(self, name), "__func__", getattr(self, name)),)
        )
        if any(
            name != old_name or method is not old_method or code is not old_code
            for (name, method, code), (old_name, old_method, old_code) in zip(
                actual, self._method_tokens, strict=True
            )
        ):
            raise ServiceError("target_binding_changed")
        return binding_token() + actual

    def assert_current(self) -> None:
        # 复核属性与代码对象，不调用四个待核验方法。
        actual = tuple(
            (name, method, getattr(method, "__code__", None))
            for name in ("verify", "descriptor", "binding_token", "assert_current")
            for method in (getattr(getattr(self, name), "__func__", getattr(self, name)),)
        )
        if any(
            name != old_name or method is not old_method or code is not old_code
            for (name, method, code), (old_name, old_method, old_code) in zip(
                actual, self._method_tokens, strict=True
            )
        ):
            raise ServiceError("target_binding_changed")
        _module_current()
        if (
            self._token != binding_token()
            or getattr(self.verify, "__func__", self.verify) is not self._verify_function
            or self._artifact_methods
            != tuple(
                (
                    name,
                    getattr(
                        getattr(self.artifacts, name), "__func__", getattr(self.artifacts, name)
                    ),
                )
                for name in ("get", "head", "chunk")
            )
        ):
            raise ServiceError("target_binding_changed")

    async def verify(
        self,
        run_id: str,
        frame: WorkFrame,
        access: ExecutionAccess,
        *,
        check: Callable[[], None],
        deadline: float,
    ) -> VerificationResult:
        from krim_agent.storage import work

        verification_deadline = min(deadline, time.monotonic() + LIMITS["seconds"])

        def guard() -> None:
            check()
            self.assert_current()
            if time.monotonic() >= deadline:
                raise ServiceError("run_timeout")

        guard()
        frozen = WorkFrame.model_validate_json(frame.model_dump_json())
        identity = self.descriptor()
        contract = cast(dict[str, Any], frozen.conditions["verification"])
        if (
            contract.get("contract") != identity
            or contract.get("completion")
            != frozen.original_request.completion.model_dump(mode="json")
            or contract.get("completion_fingerprint") != fingerprint(contract["completion"])
        ):
            raise ServiceError("target_binding_changed")

        async def evidence(conn: AsyncConnection) -> tuple[Any, list[Any], list[Any]]:
            current = await work.load(conn, run_id)
            await work.row(conn, frozen.session_id, access)
            effects = (
                (
                    await conn.execute(
                        select(tool_effects)
                        .where(tool_effects.c.run_id == run_id, tool_effects.c.kind == "command")
                        .order_by(tool_effects.c.created_at)
                    )
                )
                .mappings()
                .all()
            )
            events = (
                (await conn.execute(select(run_events).where(run_events.c.run_id == run_id)))
                .mappings()
                .all()
            )
            return current, list(effects), list(events)

        current, effects, events = await self.store.work_read(evidence)
        guard()
        if current is None or current.model_dump() != frozen.model_dump():
            raise ServiceError("target_binding_changed")
        effect = effects[-1] if effects else None
        receipt = effect["receipt"] if effect is not None else None
        observation = next(
            (
                event["data"]
                for event in events
                if effect is not None
                and event["event_id"] == "workspace-command-observation:" + effect["call_id"]
            ),
            None,
        )
        state: Literal["passed", "failed", "unknown"] = "unknown"
        reason: str | None = "missing_command"
        revision = None
        results: list[ConditionResult] = []
        completion = frozen.original_request.completion
        if receipt is not None:
            assert effect is not None
            if (
                receipt.get("executor_started") is True
                and type(receipt.get("exit_code")) is int
                and receipt.get("output_complete") is True
                and receipt.get("cleanup") == "clean"
                and receipt.get("code") is None
            ):
                state = "failed"
                reason = "command_failed"
                if receipt["exit_code"] == 0 and receipt.get("artifact_revision_id"):
                    revision = await self.artifacts.get(access, receipt["artifact_revision_id"])
                    guard()
                    head = await self.artifacts.head(access)
                    guard()
                    linked = (
                        head.revision_id == revision.revision_id
                        and revision.parent_id == effect["input_revision"]
                        and revision.producer_run_id == run_id
                        and revision.producer_call_id == effect["call_id"]
                        and effect["grant_hash"]
                        == fingerprint(frozen.grant.model_dump(mode="json"))
                    )
                    observed = (
                        observation is not None
                        and observation.get("profile_match") is True
                        and type(observation.get("discovered_tests")) is int
                        and observation["discovered_tests"] > 0
                        and observation.get("receipt_hash")
                        == fingerprint(
                            {k: v for k, v in receipt.items() if k != "artifact_revision_id"}
                        )
                        and observation.get("args_hash") == effect["args_hash"]
                        and observation.get("input_revision") == effect["input_revision"]
                        and observation.get("contract") == identity
                        and _command_events(events, effect, observation, receipt)
                    )
                    state = "passed" if linked and observed else "unknown"
                    reason = None if state == "passed" else "missing_observation"
        try:
            if state == "passed" and revision is not None:
                files = {file.path: file for file in revision.manifest}
                if any(path not in files for path in completion.required_paths):
                    state, reason = "failed", "missing_artifact"
                total = 0
                for condition in completion.conditions:
                    guard()
                    if time.monotonic() >= verification_deadline:
                        raise TimeoutError
                    file = files.get(condition.path)
                    valid = False
                    if file is not None and file.size <= LIMITS["file_bytes"]:
                        total += file.size
                        if total > LIMITS["total_bytes"]:
                            raise ValueError("解析总量超限")
                        body = bytearray()
                        while len(body) < file.size:
                            chunk = await self.artifacts.chunk(
                                access,
                                revision.revision_id,
                                file.path,
                                offset=len(body),
                                length=min(16384, file.size - len(body)),
                            )
                            guard()
                            data = base64.b64decode(chunk.data, validate=True)
                            if not data:
                                raise ValueError("文件缺片")
                            body.extend(data)
                        if hashlib.sha256(body).hexdigest() != file.sha256:
                            raise ValueError("文件hash不符")
                        valid = await settled(
                            asyncio.to_thread(
                                _condition,
                                condition.kind,
                                bytes(body),
                                pointer=condition.pointer,
                                expected=condition.expected,
                                symbols=condition.symbols,
                            )
                        )
                        guard()
                    results.append(
                        ConditionResult(
                            condition.id, condition.kind, "passed" if valid else "failed"
                        )
                    )
                    if not valid:
                        state, reason = "failed", "condition_failed"
                if time.monotonic() >= verification_deadline:
                    raise TimeoutError
        except TimeoutError:
            guard()
            state, reason = "unknown", "verification_timeout"
        except (ValueError, UnicodeError, SyntaxError, RecursionError, KeyError, IndexError):
            state, reason = "failed", "invalid_artifact"
        guard()
        checked = {item.id for item in results}
        results.extend(
            ConditionResult(item.id, item.kind, "unknown", reason)
            for item in completion.conditions
            if item.id not in checked
        )
        result = VerificationResult(
            run_id,
            frozen.turn_id,
            frozen.session_id,
            frozen.grant.workspace_id,
            frozen.actor_id,
            fingerprint(frozen.grant.model_dump(mode="json")),
            fingerprint(frozen.original_request.model_dump(mode="json")),
            fingerprint(completion.model_dump(mode="json")),
            fingerprint(frozen.conditions),
            frozen.effective_fingerprint,
            frozen.input_revision.revision_id,
            effect["call_id"] if effect else None,
            effect["args_hash"] if effect else None,
            effect["input_revision"] if effect else None,
            revision.revision_id if revision else None,
            revision.manifest_hash if revision else None,
            fingerprint(receipt) if receipt is not None else None,
            fingerprint(observation) if observation is not None else None,
            VerificationIdentity(**cast(dict[str, str], identity)),
            state,
            tuple(results),
            reason,
        )
        self._issued[run_id] = result
        return result


def stage_result(
    result: VerificationResult,
    *,
    issuer: Verifier,
    store: object,
    producer: asyncio.Task[Any],
    access: ExecutionAccess,
    check: Callable[[], None],
    run_deadline: float,
) -> None:
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
    value = _HANDOFF.get()
    if value is None or value.result.run_id != run_id or not value.active:
        return None
    return value


async def terminal_attempt_same_conn(
    conn: AsyncConnection, store: object, run_id: str, handoff: _Handoff | None
) -> _TerminalAttempt | None:
    if getattr(store, "schema_version", None) not in {4, 5, 6}:
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
    if not rejection.issued_for(attempt):
        raise rejection
    attempt.negative_pair = rejection.negative_pair()


def terminal_commit_fence_for_negative_result(
    attempt: _TerminalAttempt, *, selected_status: RunStatus, selected_code: str | None
) -> None:
    if not attempt.active or selected_status == "completed":
        raise ServiceError("completion_unverified")
    if attempt.negative_pair is not None and attempt.negative_pair != (
        selected_status,
        selected_code,
    ):
        raise ServiceError("completion_unverified")


async def consume_terminal(conn: AsyncConnection, run_id: str, *, interrupted: bool) -> None:
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


def command_observation(
    invocation: Any, input_revision: str, receipt: Any, contract: dict[str, JsonValue]
) -> dict[str, JsonValue]:
    from krim_agent.tools.workspace import Command

    output = receipt[4] + b"\n" + receipt[5]
    match = re.findall(rb"(?:^|\n)Ran ([0-9]+) tests? in [0-9.]+s\r?\n", output)
    count = int(match[0]) if len(match) == 1 and len(match[0]) <= 8 else None
    raw_args = parse_json(invocation.arguments_json)
    args = Command.model_validate_json(invocation.arguments_json).model_dump(mode="json")
    profile = (
        isinstance(args, dict)
        and args.get("program") == "python"
        and args.get("argv") == list(COMMAND)
        and args.get("cwd", ".") == "."
    )
    actual = {
        "executor_started": True if type(receipt[0]) is int else None,
        "exit_code": receipt[0],
        "output_complete": receipt[1],
        "cleanup": receipt[2],
        "code": receipt[3],
    }
    value: dict[str, JsonValue] = {
        "schema": SCHEMA,
        "call_id": invocation.call_id,
        "run_id": invocation.run_id,
        "args_hash": fingerprint(args),
        "invocation_args_hash": fingerprint(raw_args),
        "tool_hash": invocation.tool_hash,
        "input_revision": input_revision,
        "contract": contract,
        "receipt_hash": fingerprint(actual),
        "stdout_bytes": len(receipt[4]),
        "stdout_hash": hashlib.sha256(receipt[4]).hexdigest(),
        "stderr_bytes": len(receipt[5]),
        "stderr_hash": hashlib.sha256(receipt[5]).hexdigest(),
        "profile_match": profile,
        "discovered_tests": count,
        "reason": None if count is not None else "unparsed",
    }
    if len(encoded(value).encode()) > LIMITS["observation_bytes"]:
        raise ObservationRejected("completion_unverified")
    return value


# 私有捕获引用使helper符号被替换时仍能安全拒绝，不进入替代入口。
_TERMINAL_HELPERS = (
    terminal_attempt_same_conn,
    terminal_commit_fence,
    stage_negative_handoff_for_same_owner,
    terminal_commit_fence_for_negative_result,
    clear_handoff_at_owner_end,
    consume_terminal,
)
_OBSERVATION_HELPERS = (_module_current, command_observation, descriptor)
_NAMES = (
    "descriptor",
    "binding_token",
    "_module_current",
    "_condition",
    "_command_events",
    "stage_result",
    "private_handoff_for_same_owner_and_run",
    "terminal_attempt_same_conn",
    "terminal_commit_fence",
    "stage_negative_handoff_for_same_owner",
    "terminal_commit_fence_for_negative_result",
    "clear_handoff_at_owner_end",
    "consume_terminal",
    "command_observation",
)
_MODULE_TOKEN = tuple(
    (globals().get(name), getattr(globals().get(name), "__code__", None)) for name in _NAMES
)
