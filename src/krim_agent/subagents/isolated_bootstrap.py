"""Approved in-run workspace bootstrap for isolated SDK children (plan §9.2).

Before the child's first model sample, the isolated copy is materialized by
one fixed host ``shell`` call through the run's own admission chain — the
exact bootstrap source the process provider uses (git worktree first, bounded
copy fallback, uncommitted changes included). The command is built entirely
from the host-validated assignment directory; no model input picks paths, and
the model never sees the bootstrap call.

Production admission and durability: ``OperationStore.admit`` creates the
deterministic operation entry before any execution, ``bind_command`` and the
terminal inside ``DevelopmentService.execute`` stay authoritative, and the
receipt is saved ``completed``/``unknown`` with ``result_ref`` exactly like
the SDK adapter does. ``ask`` decisions write the ``isolated-bootstrap/1``
checkpoint (deterministic identity + ownership + budget + pending approval
data) and park; ``RunService`` recovery hands back a ``BootstrapResume`` whose
replies replay only the not-yet-started call. A started but unconfirmed
operation is surfaced as ``execution_unknown`` instead of being re-executed.
Completion is verified against the real exit state and output manifest and
then marked with the durable ``workspace_prepared`` run event (task input and
its frozen hash stay untouched).
"""

from __future__ import annotations

import asyncio
import math
import shlex
from dataclasses import replace
from typing import Any, cast

from krim_agent.agent_sdk.host import CURRENT_OPERATION
from krim_agent.contracts import ServiceError, ToolObservation, encoded, fingerprint, parse_json
from krim_agent.development.contracts import OperationIdentity, OperationReceipt
from krim_agent.runtime.sdk_execution import Parked
from krim_agent.subagents.providers.process import PREPARE_WORKSPACE_SCRIPT
from krim_agent.subagents.workspace import WorkspaceAssignment, WorkspaceLimits
from krim_agent.tools.core import Invocation

POLL_CAP_SECONDS = 120.0
POLL_INTERVAL_SECONDS = 0.1


def bootstrap_command(directory: str, limits: dict[str, int]) -> str:
    """Render the fixed bootstrap as one development shell command string.

    The script and its two arguments are plain ``shlex``-quoted words of
    ``/usr/bin/python3 -I -c`` — never shell grammar. The command runs with
    the project root as cwd and writes into the assignment directory.
    """
    if not isinstance(directory, str) or not directory:
        raise ServiceError("invalid_input", 422)
    return " ".join((
        "exec /usr/bin/python3 -I -c",
        shlex.quote(PREPARE_WORKSPACE_SCRIPT),
        shlex.quote(directory),
        shlex.quote(encoded(limits)),
    ))


def _manifest_from(output: Any) -> dict[str, Any] | None:
    """The bootstrap's last JSON manifest line, mirroring the process reader."""
    if not isinstance(output, dict) or not isinstance(output.get("text"), str):
        return None
    for line in reversed(output["text"].splitlines()):
        candidate = line.strip()
        if not candidate.startswith("{"):
            continue
        try:
            parsed = parse_json(candidate)
        except ValueError:
            continue
        if isinstance(parsed, dict) and parsed.get("mode") in {"worktree", "copy"}:
            return cast(dict[str, Any], parsed)
    return None


def _verified_result(value: Any) -> dict[str, Any]:
    """Exit-0, untruncated, manifest-bearing result of one bootstrap shell."""
    if not isinstance(value, dict):
        raise ServiceError("subagent_capability")
    manifest = _manifest_from(value.get("output"))
    if (
        value.get("state") != "exited"
        or value.get("exit_code") != 0
        or manifest is None
        or manifest.get("truncated") is True
    ):
        # 副本不完整就不能宣称隔离成功（与 process provider 同一门槛）。
        raise ServiceError("subagent_capability")
    return manifest


async def _invoke_read(
    executor: Any,
    context: Any,
    name: str,
    arguments: dict[str, Any],
    *,
    run_id: str,
    tag: str,
) -> Any:
    """One read-tool invocation through the run's own admission chain."""
    spec = executor.registry.get(name)
    invocation = Invocation(
        run_id=run_id,
        call_id=f"bootstrap-{tag}-{next_index()}",
        span_id=f"span-bootstrap-{tag}-{next_index()}",
        step_index=0,
        name=name,
        arguments_json=encoded(arguments),
        tool_hash=spec.fingerprint,
    )
    admission = await executor.admit(invocation, context, timeout_seconds=None)
    observation = await executor.execute(invocation, context, admission)
    if observation.status != "ok":
        raise ServiceError(observation.code or "execution_error")
    return observation.value if isinstance(observation.value, dict) else {}


_counter = 0


def next_index() -> int:
    global _counter
    _counter += 1
    return _counter


async def _await_exit_and_collect(
    executor: Any,
    context: Any,
    *,
    run_id: str,
    process_id: str,
    budget: Any,
) -> dict[str, Any]:
    """Wait for the real handle to exit and read its persisted output once.

    A running handle is never re-executed: ``process_status`` is polled until
    a terminal state within the execution deadline, then ``read_output``
    fetches the bounded page for verification.
    """
    loop = asyncio.get_running_loop()
    deadline = getattr(budget, "deadline", None)
    deadline = float(deadline) if isinstance(deadline, (int, float)) else None
    cap = loop.time() + POLL_CAP_SECONDS
    if deadline is None or cap < deadline:
        deadline = cap
    state: Any = None
    exit_code: Any = None
    while True:
        status = await _invoke_read(
            executor, context, "process_status", {"process_id": process_id},
            run_id=run_id, tag="status",
        )
        state = status.get("state")
        exit_code = status.get("exit_code")
        if state in {"exited", "cancelled", "failed", "unknown"}:
            break
        if loop.time() >= deadline:
            raise ServiceError("execution_unknown")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    if state != "exited" or exit_code != 0:
        raise ServiceError("subagent_capability")
    chunks: list[str] = []
    cursor = 0
    while True:
        page = await _invoke_read(
            executor, context, "read_output",
            {"process_id": process_id, "cursor": cursor},
            run_id=run_id, tag="output",
        )
        if (
            page.get("process_id") != process_id
            or page.get("cursor") != cursor
            or not isinstance(page.get("text"), str)
            or page.get("truncated") is True
        ):
            raise ServiceError("subagent_capability")
        chunks.append(page["text"])
        next_cursor = page.get("next_cursor")
        if type(next_cursor) is not int or next_cursor < cursor:
            raise ServiceError("subagent_capability")
        if page.get("eof") is True:
            break
        if next_cursor <= cursor:
            raise ServiceError("subagent_capability")
        cursor = next_cursor
        if sum(len(chunk.encode("utf-8")) for chunk in chunks) > 8192:
            raise ServiceError("subagent_capability")
    return {
        "state": "exited", "exit_code": exit_code,
        "output": {"text": "".join(chunks), "eof": True, "truncated": False},
    }


async def prepare_isolated_workspace(
    *,
    run_id: str,
    store: Any,
    operations: Any,
    executor: Any,
    context: Any,
    environment: Any,
    ownership: Any,
    emit: Any,
    approval_request: Any,
    resume: Any,
    budget: Any,
    assignment: WorkspaceAssignment,
    task_id: str,
    check: Any = lambda: None,
    limits: WorkspaceLimits | None = None,
) -> None:
    """Materialize ``assignment.directory`` once, before any model sampling.

    Prepared state is the ``workspace_prepared`` run event; it is never
    written into the hash-frozen task input. Requires a development ``shell``
    execution surface; other tool families cannot prepare a copy and are
    rejected up front.
    """
    snapshot = await store.snapshot(run_id)
    if any(event.kind == "workspace_prepared" for event in snapshot.events):
        return
    directory = assignment.directory
    if not isinstance(directory, str) or not directory:
        raise ServiceError("invalid_input", 422)
    shell_spec = executor.registry.get("shell")
    if shell_spec.scoped_executor is None or shell_spec.capability != "command":
        raise ServiceError("workspace_unavailable", 503)
    for name in ("process_status", "read_output"):
        try:
            probe = executor.registry.get(name)
        except ServiceError:
            raise ServiceError("workspace_unavailable", 503) from None
        if probe.scoped_executor is None or probe.capability != "read":
            raise ServiceError("workspace_unavailable", 503)
    invocation = Invocation(
        run_id=run_id,
        call_id=f"bootstrap-{run_id}",
        span_id=f"span-bootstrap-{run_id}",
        step_index=0,
        name="shell",
        arguments_json=encoded({
            "command": bootstrap_command(
                directory, (limits or WorkspaceLimits()).copy_bounds(),
            ),
            "wait_seconds": 20,
        }),
        tool_hash=shell_spec.fingerprint,
    )
    identity = OperationIdentity(
        operation_id=f"op-bootstrap-{run_id}",
        call_id=invocation.call_id,
        environment=environment,
        ownership=ownership,
        request_digest=fingerprint({
            "name": "shell",
            "args": parse_json(invocation.arguments_json),
            "tool_hash": shell_spec.fingerprint,
        }),
    )
    if resume is not None:
        replay_identity = getattr(resume, "identity", None)
        if replay_identity != identity.model_dump(mode="json"):
            raise ServiceError("facts_corrupt")

    async def prepared(
        manifest: dict[str, Any] | None, receipt: OperationReceipt, *, replayed: bool
    ) -> None:
        await emit("workspace_prepared", {
            "agent_id": assignment.agent_id,
            "task_id": task_id,
            "directory": directory,
            "baseline_fingerprint": assignment.baseline_fingerprint,
            "mode": manifest.get("mode") if manifest else None,
            "head": manifest.get("head") if manifest else None,
            "copied": manifest.get("copied") if manifest else None,
            "skipped": manifest.get("skipped") if manifest else None,
            "truncated": manifest.get("truncated") if manifest else None,
            "operation_id": receipt.identity.operation_id,
            "call_id": receipt.identity.call_id,
            "exit_code": 0,
            "replayed": replayed,
        })

    existing = await operations.lookup(identity)
    if existing is not None and existing.state == "completed":
        # 持久条目已完成：持久 tool_end 证实 exit 0/未截断才标记，不重跑。
        manifest = await _verify_completed_receipt(store, run_id, existing)
        await prepared(manifest, existing, replayed=True)
        return

    admission = await executor.admit(invocation, context, timeout_seconds=None)
    replies = getattr(resume, "replies", None) or {}
    replayed_approval = resume is not None and replies.get(invocation.call_id) is True
    if admission.decision == "ask" and not admission.approved:
        if not replayed_approval:
            data = await approval_request(invocation, budget.remaining)
            await store.append(run_id, "approval_request", data)
            remaining = budget.remaining
            checkpoint = {
                "format": "isolated-bootstrap/1",
                "run_id": run_id,
                "ownership": ownership.model_dump(mode="json"),
                "identity": identity.model_dump(mode="json"),
                "budget": {
                    "remaining": (
                        remaining
                        if isinstance(remaining, (int, float))
                        and not isinstance(remaining, bool)
                        and math.isfinite(remaining)
                        else None
                    ),
                },
                "pending_invocations": [data],
            }
            await store.write_checkpoint(run_id, checkpoint, fingerprint(checkpoint))
            # awaiting 事实固定在 approval_request 事件与该 checkpoint 里；
            # 批复唤醒由 RunService prepare_resume 分发 BootstrapResume。
            raise Parked()
        admission = replace(admission, approved=True)
    check()
    if existing is not None:
        # 审批批准后允许继续的唯一既有条目：BootstrapResume 确认且仍是
        # accepted（bind 同 digest 幂等）；其余状态一律不盲目重放。
        if not replayed_approval or existing.state != "accepted":
            raise ServiceError("execution_unknown")
        receipt = existing
    else:
        # 持久操作条目在审批通过后、真实执行前创建（bind_command 要求 accepted）。
        operation_admission = await operations.admit(identity)
        receipt = operation_admission.receipt
        if not operation_admission.execute:
            raise ServiceError("execution_unknown")
    token = CURRENT_OPERATION.set(identity)
    try:
        observation = await executor.execute(invocation, context, admission)
    finally:
        CURRENT_OPERATION.reset(token)
    if observation.status != "ok":
        await operations.save_receipt(receipt.model_copy(
            update={"state": "unknown", "result_ref": None},
        ))
        raise ServiceError(observation.code or "execution_error")
    receipt_value = observation.value if isinstance(observation.value, dict) else {}
    if receipt_value.get("state") != "exited":
        process_id = receipt_value.get("process_id")
        if not isinstance(process_id, str) or not process_id:
            await operations.save_receipt(receipt.model_copy(
                update={"state": "unknown", "result_ref": None},
            ))
            raise ServiceError("execution_unknown")
        try:
            receipt_value = await _await_exit_and_collect(
                executor, context, run_id=run_id, process_id=process_id, budget=budget,
            )
        except ServiceError as error:
            if error.code == "execution_unknown":
                await operations.save_receipt(receipt.model_copy(
                    update={"state": "unknown", "result_ref": None},
                ))
            raise
    try:
        manifest = _verified_result(receipt_value)
    except ServiceError:
        await operations.save_receipt(receipt.model_copy(
            update={"state": "failed", "result_ref": None},
        ))
        raise
    # completed 只在真实退出且结果核实后落库；等待中的句柄绝不提前记账。
    receipt = receipt.model_copy(update={
        "state": "completed", "result_ref": invocation.call_id,
    })
    await operations.save_receipt(receipt)
    await prepared(manifest, receipt, replayed=False)


async def _verify_completed_receipt(
    store: Any, run_id: str, receipt: OperationReceipt
) -> dict[str, Any]:
    """Verify the persisted tool_end of a completed bootstrap operation."""
    if receipt.result_ref is None:
        raise ServiceError("facts_corrupt")
    snapshot = await store.snapshot(run_id)
    event = next(
        (
            item for item in reversed(snapshot.events)
            if item.kind == "tool_end" and item.data.get("call_id") == receipt.result_ref
        ),
        None,
    )
    if event is None:
        raise ServiceError("facts_corrupt")
    observed = ToolObservation.model_validate(event.data)
    if observed.status != "ok" or observed.result_available is not True:
        raise ServiceError("facts_corrupt")
    return _verified_result(observed.value)
