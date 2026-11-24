"""Admission failure must remain a failed turn, not an unrecoverable ledger."""

from dataclasses import replace

import pytest

from krim_agent.context import builder
from krim_agent.contracts import ServiceError, parse_json
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.sqlite import SQLiteStore
from tests.agent_core.test_tool_timeout_history import snapshot_with_outcome
from tests.coding_agent.test_development_storage import prepare


def unadmitted_snapshot(status="failed"):
    snapshot = snapshot_with_outcome()
    return replace(
        snapshot,
        run=snapshot.run.model_copy(update={"status": status, "code": "invalid_input"}),
        events=snapshot.events[:1],
    )


@pytest.mark.parametrize("status", ["failed", "aborted", "interrupted"])
def test_failed_unadmitted_call_has_unknown_pairing_without_fabricated_execution(status):
    snapshot = unadmitted_snapshot(status)
    before = [event.model_dump(mode="json") for event in snapshot.events]
    first = session_sql.task_transcript(snapshot)
    assert first == session_sql.task_transcript(snapshot)
    tool = next(message for message in first if message.role == "tool")
    payload = parse_json(tool.content)
    assert tool.call_id == "provider-write"
    assert payload["status"] == "unknown"
    assert payload["source"] == "persisted_model_call_without_tool_start"
    assert payload["executor_started"] is None
    assert payload["result_available"] is None
    assert payload["replay_forbidden"] is True
    assert builder.deterministic_signature(snapshot.events) is None
    assert builder.has_unknown_execution(snapshot.events) is True
    assert builder.has_waiting_task(snapshot.events) is False
    assert [event.model_dump(mode="json") for event in snapshot.events] == before
    builder.validate_groups(first)


def test_completed_call_without_admission_remains_corrupt():
    with pytest.raises(ServiceError, match="facts_corrupt"):
        session_sql.task_transcript(unadmitted_snapshot("completed"))


def test_tool_end_without_admission_remains_corrupt():
    snapshot = snapshot_with_outcome()
    snapshot = replace(snapshot, events=(snapshot.events[0], snapshot.events[-1]))
    with pytest.raises(ServiceError, match="facts_corrupt"):
        session_sql.task_transcript(snapshot)


def test_tool_intent_without_admission_remains_corrupt():
    snapshot = snapshot_with_outcome()
    snapshot = replace(snapshot, events=snapshot.events[:1] + snapshot.events[2:3])
    with pytest.raises(ServiceError, match="facts_corrupt"):
        session_sql.task_transcript(snapshot)


async def record_model_call_before_admission(store):
    async with store.lock, store.engine.connect() as connection:
        row = (await connection.exec_driver_sql("SELECT id FROM runs")).first()
    run_id = row[0]
    await store.append(run_id, "model_start", {"span_id": "e" * 16, "step_index": 0})
    await store.append(run_id, "model_end", {
        "span_id": "e" * 16,
        "step_index": 0,
        "tool_calls": [{"id": "invalid-find", "name": "find",
                        "arguments": {"include_glob": "**/*", "limit": 150, "path": "."}}],
    })
    return run_id


async def test_session_admission_failure_can_commit_terminal_state(tmp_path):
    store, _repositories, _environment, _request = await prepare(tmp_path / "admission.sqlite")
    try:
        run_id = await record_model_call_before_admission(store)
        result = await store.finish(run_id, "failed", "invalid_input")
        assert result.status == "failed" and result.code == "invalid_input"
        snapshot = await store.snapshot(run_id)
        assert len([event for event in snapshot.events if event.kind == "terminal"]) == 1
        assert not any(event.kind.startswith("tool_") for event in snapshot.events)
        assert builder.has_unknown_execution(snapshot.events)
        assert store.healthy
    finally:
        await store.close()


async def test_restart_recovers_unadmitted_model_call_without_rewriting_facts(tmp_path):
    path = tmp_path / "recovery.sqlite"
    store, _repositories, _environment, _request = await prepare(path)
    run_id = await record_model_call_before_admission(store)
    original = [event.model_dump(mode="json") for event in (await store.snapshot(run_id)).events]
    await store.close()
    reopened = await SQLiteStore.open(path)
    try:
        await reopened.recover()
        snapshot = await reopened.snapshot(run_id)
        assert snapshot.run.status == "interrupted"
        assert snapshot.run.code == "interrupted"
        after = [event.model_dump(mode="json") for event in snapshot.events]
        assert after[:len(original)] == original
        assert not any(event.kind.startswith("tool_") for event in snapshot.events)
        assert builder.has_unknown_execution(snapshot.events)
        assert reopened.healthy
    finally:
        await reopened.close()
