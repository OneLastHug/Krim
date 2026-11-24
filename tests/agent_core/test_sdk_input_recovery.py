"""Same-run recovery for strict tool argument errors.

The invalid batch is never dispatched. The model receives a bounded, safe
failure observation, corrects the call in the same run, and only the corrected
call executes. Checkpoint replay must not re-execute or duplicate the failure.
"""

from __future__ import annotations

import json
from typing import Any

import ai
import pytest
from pydantic import Field, ValidationError, field_validator

from krim_agent.agent_sdk.adapter import StepCommit
from krim_agent.contracts import StrictModel
from krim_agent.tools.input_errors import ToolInputError
from tests.coding_agent.sdk_fixtures import Scripted, done, model, setup, turn


def _tool_results(history: Any, call_ids: set[str]) -> list[Any]:
    return [
        part
        for message in history
        for part in message.tool_results
        if part.tool_call_id in call_ids
    ]


def _lifecycle(events: list[Any], provider_call_id: str) -> list[str]:
    call_id = next(
        event.data["call_id"]
        for event in events
        if event.kind == "tool_start" and event.data.get("provider_call_id") == provider_call_id
    )
    return [
        event.kind
        for event in events
        if event.kind in {"tool_start", "tool_intent", "tool_end"}
        and event.data.get("call_id") == call_id
    ]


@pytest.mark.asyncio
async def test_invalid_then_correction_completes_with_fail_on_tool_error() -> None:
    harness = setup(approval=False)
    adapter = harness.adapter()
    adapter.fail_on_tool_error = True
    provider = Scripted(
        [
            turn(("read", "bad", "not-an-int")),
            turn(("read", "good", 7)),
            done(),
        ]
    )

    result = await adapter.run(model(provider), [ai.user_message("go")])

    assert result.status == "completed"
    assert harness.counts == {"read": 1, "write": 0}
    assert len(provider._seen) == 3
    assert harness.trace.count("read") == 1

    first = _tool_results(provider._seen[1], {"bad"})
    assert len(first) == 1
    part = first[0]
    assert part.is_error is True
    assert part.result["status"] == "failed"
    assert part.result["code"] == "invalid_input"
    assert part.result["decision"] == "deny"
    assert part.result["executor_started"] is False
    assert part.result["result_available"] is True
    assert part.result["value"]["retryable"] is True
    assert "arguments" not in part.result
    assert "not-an-int" not in json.dumps(part.result)

    events = list(harness.store.events)
    assert _lifecycle(events, "bad") == ["tool_start", "tool_end"]
    assert _lifecycle(events, "good") == ["tool_start", "tool_intent", "tool_end"]
    assert harness.operations.trace == []


@pytest.mark.asyncio
async def test_invalid_batch_blocks_write_sibling_and_corrected_batch_executes_once() -> None:
    harness = setup(approval=False)
    provider = Scripted(
        [
            turn(("write", "w1", 1), ("read", "bad", "not-an-int")),
            turn(("write", "w2", 2), ("read", "good", 2)),
            done(),
        ]
    )

    result = await harness.adapter().run(model(provider), [ai.user_message("go")])

    assert result.status == "completed"
    assert harness.counts == {"read": 1, "write": 1}
    assert harness.operations.trace == ["lookup", "admit", "save_receipt"]

    blocked = _tool_results(provider._seen[1], {"w1", "bad"})
    assert {part.result["code"] for part in blocked} == {
        "invalid_input",
        "batch_not_executed",
    }
    assert all(part.is_error for part in blocked)
    assert all(part.result["status"] == "failed" for part in blocked)
    assert all(part.result["executor_started"] is False for part in blocked)
    assert all(part.result["result_available"] is True for part in blocked)

    corrected = _tool_results(provider._seen[2], {"w2", "good"})
    assert [part.result["status"] for part in corrected] == ["ok", "ok"]

    events = list(harness.store.events)
    blocked_call_ids = {
        event.data["call_id"]
        for event in events
        if event.kind == "tool_start" and event.data.get("provider_call_id") in {"w1", "bad"}
    }
    intent_call_ids = {event.data["call_id"] for event in events if event.kind == "tool_intent"}
    assert intent_call_ids.isdisjoint(blocked_call_ids)
    assert len(intent_call_ids) == 2  # corrected write and read

    write_ok = [
        event
        for event in events
        if event.kind == "tool_end"
        and event.data.get("name") == "write"
        and event.data.get("status") == "ok"
    ]
    assert len(write_ok) == 1


def test_tool_input_error_exposes_only_safe_diagnostics() -> None:
    class Input(StrictModel):
        count: int = Field(gt=10)
        name: str

        @field_validator("name")
        @classmethod
        def reject(cls, value: str) -> str:
            raise ValueError("SYNTHETIC_SECRET custom validator")

    with pytest.raises(ValidationError) as raised:
        Input.model_validate(
            {
                "count": 1,
                "name": "ok",
                "SECRET_EXTRA": "SYNTHETIC_VALUE",
            }
        )
    details = ToolInputError(raised.value, Input.model_json_schema()).details

    assert details["code"] == "invalid_input"
    assert details["executor_started"] is False
    assert details["retryable"] is True
    assert details["truncated"] is False
    assert set(details) == {
        "code",
        "executor_started",
        "retryable",
        "errors",
        "truncated",
        "message",
    }
    errors = details["errors"]
    assert len(errors) == 3
    assert all(set(issue) <= {"field", "type", "message", "constraint"} for issue in errors)
    assert {issue["type"] for issue in errors} == {
        "greater_than",
        "invalid_value",
        "extra_forbidden",
    }
    rendered = json.dumps(details, sort_keys=True)
    assert "SYNTHETIC_SECRET" not in rendered
    assert "SYNTHETIC_VALUE" not in rendered
    assert "SECRET_EXTRA" not in rendered
    assert "<field>" in rendered
    greater = next(issue for issue in errors if issue["type"] == "greater_than")
    assert greater["field"] == ["count"]
    assert greater["constraint"] == {"gt": 10}
    invalid = next(issue for issue in errors if issue["type"] == "invalid_value")
    assert invalid["field"] == ["name"]
    assert "SYNTHETIC" not in invalid["message"]

    class Wide(StrictModel):
        a: int
        b: int
        c: int
        d: int
        e: int
        f: int
        g: int
        h: int
        i: int
        j: int

    with pytest.raises(ValidationError) as wide:
        Wide.model_validate({name: "wrong" for name in "abcdefghij"})
    wide_details = ToolInputError(wide.value, Wide.model_json_schema()).details
    assert len(wide_details["errors"]) == 8
    assert wide_details["truncated"] is True


@pytest.mark.asyncio
async def test_invalid_result_then_approval_park_resume_commits_steps_once() -> None:
    harness = setup(approval=True)
    commits: list[StepCommit] = []

    async def on_step_commit(commit: StepCommit) -> None:
        commits.append(commit)

    adapter = harness.adapter()
    adapter.on_step_commit = on_step_commit
    provider = Scripted(
        [
            turn(("read", "bad", "not-an-int")),
            turn(("write", "w1", 1)),
            done(),
        ]
    )

    first = await adapter.run(model(provider), [ai.user_message("go")])

    assert first.status == "awaiting"
    assert harness.counts == {"read": 0, "write": 0}
    assert len(provider._seen) == 2
    same_turn = _tool_results(provider._seen[1], {"bad"})
    assert len(same_turn) == 1 and same_turn[0].is_error is True
    assert [commit.step for commit in commits] == [1, 2]
    events = list(harness.store.events)
    assert (
        len(
            [
                event
                for event in events
                if event.kind == "tool_end" and event.data.get("code") == "invalid_input"
            ]
        )
        == 1
    )
    assert not [
        event for event in events if event.kind == "tool_end" and event.data.get("name") == "write"
    ]
    approval = next(event for event in events if event.kind == "approval_request")

    resumed_provider = Scripted([done()])
    second = await adapter.run(
        model(resumed_provider),
        resume=True,
        replies={approval.data["call_id"]: True},
    )

    assert second.status == "completed"
    assert harness.counts == {"read": 0, "write": 1}
    assert len(_tool_results(resumed_provider._seen[0], {"bad"})) == 1
    assert len(_tool_results(resumed_provider._seen[0], {"w1"})) == 1
    assert [commit.step for commit in commits] == [1, 2, 3]
    events = list(harness.store.events)
    assert (
        len(
            [
                event
                for event in events
                if event.kind == "tool_end" and event.data.get("code") == "invalid_input"
            ]
        )
        == 1
    )
    write_ok = [
        event
        for event in events
        if event.kind == "tool_end"
        and event.data.get("name") == "write"
        and event.data.get("status") == "ok"
    ]
    assert len(write_ok) == 1
    assert harness.operations.trace == ["lookup", "admit", "save_receipt"]


@pytest.mark.asyncio
@pytest.mark.parametrize("tamper", ["errors", "decision"])
async def test_checkpoint_input_rejection_tamper_is_rejected(tamper: str) -> None:
    harness = setup(approval=False)
    adapter = harness.adapter()
    provider = Scripted([turn(("read", "bad", "not-an-int")), done()])
    result = await adapter.run(model(provider), [ai.user_message("go")])
    assert result.status == "completed"

    checkpoint, stored = harness.store.saved
    entry = checkpoint["entries"]["bad"]
    if tamper == "errors":
        entry["input_error"]["errors"][0]["message"] = "tampered"
    else:
        entry["decision"] = "allow"
    harness.store.saved = (checkpoint, stored)
    harness.store.status = "awaiting"

    with pytest.raises(ValueError, match="checkpoint input rejection"):
        await adapter.run(model(Scripted([done()])), resume=True)


@pytest.mark.asyncio
async def test_session_run_corrects_input_then_resumes_approval_with_durable_history(
    tmp_path,
) -> None:
    from krim_agent.context import builder
    from krim_agent.providers.types import ModelToolCall
    from tests.coding_agent.test_execution_policy_runtime import (
        DONE,
        application,
        await_completed,
        close_case,
        reply,
        write_call,
    )

    case = await application(
        tmp_path,
        [
            [ModelToolCall(id="invalid", name="write", arguments={"value": "bad"})],
            [write_call(7)],
            [DONE],
        ],
    )
    try:
        assert (await case.service.wait(case.run.id)).status == "awaiting"
        assert case.counts["write"] == 0
        snapshot = await case.store.snapshot(case.run.id)
        invalid = [
            event
            for event in snapshot.events
            if event.kind == "tool_end" and event.data.get("code") == "invalid_input"
        ]
        assert len(invalid) == 1
        assert invalid[0].data["executor_started"] is False
        assert invalid[0].data["result_available"] is True
        builder.validate_groups(builder.transcript(snapshot))
        pending = await case.service.approvals(case.run.id)
        assert len(pending) == 1
        await case.service.reply_approval(
            case.run.id,
            pending[0]["request_id"],
            reply(pending[0], allow=True),
        )
        await await_completed(case)
        snapshot = await case.store.snapshot(case.run.id)
        assert case.counts["write"] == 1
        assert (
            len(
                [
                    event
                    for event in snapshot.events
                    if event.kind == "tool_end" and event.data.get("code") == "invalid_input"
                ]
            )
            == 1
        )
        transcript = builder.transcript(snapshot)
        builder.validate_groups(transcript)
        assert not builder.has_unknown_execution(snapshot.events)
        assert len([message for message in transcript if message.role == "tool"]) == 2
    finally:
        await close_case(case)
