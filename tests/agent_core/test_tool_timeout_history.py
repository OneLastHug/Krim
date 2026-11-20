"""Unknown tool outcomes are valid persisted facts, not missing bookkeeping."""

import pytest

from krim_agent.context import builder
from krim_agent.contracts import (
    RunEvent,
    RunRequest,
    RunView,
    ServiceError,
    ToolObservation,
    parse_json,
)
from krim_agent.runtime.contracts import RunSnapshot
from krim_agent.storage.sessions import task_transcript


def snapshot_with_outcome(status="failed", started=None):
    run_id = "timeout-history"
    end = ToolObservation(
        call_id="host-write", provider_call_id="provider-write", span_id="a" * 16,
        step_index=0, name="write", arguments={}, status=status,
        executor_started=started, result_available=None, decision="allow",
        code="tool_timeout" if status == "failed" else "cancelled",
    ).model_dump(mode="json")
    raw = [
        ("model_end", {"step_index": 0, "tool_calls": [{
            "id": "provider-write", "name": "write", "arguments": {},
        }]}),
        ("tool_start", {"step_index": 0, "provider_call_id": "provider-write",
                        "call_id": "host-write", "name": "write", "status": "unknown",
                        "executor_started": False, "result_available": False}),
        ("tool_intent", {"step_index": 0, "call_id": "host-write"}),
        ("tool_end", end),
    ]
    return RunSnapshot(
        run=RunView(id=run_id, request_key="timeout",
                    status="aborted" if status == "cancelled" else "failed",
                    code="cancelled" if status == "cancelled" else "tool_timeout",
                    trace_id="b" * 32, root_span_id="c" * 16, created_at=1.0, finished_at=2.0),
        request=RunRequest(text="write once", request_key="timeout"), target={},
        events=tuple(RunEvent(run_id=run_id, seq=index, event_id=f"e{index}", kind=kind, data=data)
                     for index, (kind, data) in enumerate(raw)),
    )


@pytest.mark.parametrize("status", ["failed", "cancelled"])
@pytest.mark.parametrize("started", [None, True])
def test_persisted_unknown_tool_outcome_keeps_pairing_and_forbids_replay(status, started):
    snapshot = snapshot_with_outcome(status, started)
    before = [event.model_dump(mode="json") for event in snapshot.events]
    first = task_transcript(snapshot)
    second = task_transcript(snapshot)
    assert first == second
    tool = next(message for message in first if message.role == "tool")
    assert tool.call_id == "provider-write"
    payload = parse_json(tool.content)
    assert payload["status"] == status
    assert payload["result_available"] is None
    assert payload["execution_outcome"] == "unknown"
    assert payload["replay_forbidden"] is True
    assert "重放" in payload["note"]
    assert builder.deterministic_signature(snapshot.events) is None
    assert [event.model_dump(mode="json") for event in snapshot.events] == before
    builder.validate_groups(first)


def test_missing_availability_is_still_a_bookkeeping_error():
    snapshot = snapshot_with_outcome()
    del snapshot.events[-1].data["result_available"]
    with pytest.raises(ServiceError, match="facts_corrupt"):
        task_transcript(snapshot)


def test_success_cannot_claim_an_unknown_result():
    snapshot = snapshot_with_outcome()
    snapshot.events[-1].data["status"] = "ok"
    with pytest.raises(ServiceError, match="facts_corrupt"):
        task_transcript(snapshot)
