from __future__ import annotations

import pytest

from krim_agent.contracts import Message, encoded
from krim_agent.subagents.context import completed_turns, fork_context, fresh_context


def history() -> list[Message]:
    return [
        Message(role="system", content="parent instructions"),
        Message(role="user", content="first request"),
        Message(
            role="assistant",
            content="",
            tool_calls=[
                {"id": "call-1", "name": "read", "arguments": {}},
            ],
        ),
        Message(role="tool", call_id="call-1", content=encoded({"status": "unknown"})),
        Message(role="assistant", content="could not confirm the operation"),
        Message(role="user", content="second request"),
        Message(role="assistant", content="second answer"),
    ]


def test_fresh_input_does_not_include_parent_history() -> None:
    snapshot = fresh_context("child task", system_prompt="worker", background="entrypoint")
    assert [message.content for message in snapshot.messages] == [
        "worker",
        "entrypoint\n\nchild task",
    ]
    assert snapshot.source_run_id is None


def test_fork_uses_completed_prefix_and_preserves_unknown_observation() -> None:
    parent = history()
    parent.extend(
        [
            Message(role="user", content="current request"),
            Message(
                role="assistant",
                content="",
                tool_calls=[
                    {"id": "call-active", "name": "spawn_agent", "arguments": {}},
                ],
            ),
        ]
    )
    snapshot = fork_context(
        parent, "delegated goal", source_run_id="root-run", system_prompt="worker"
    )
    assert snapshot.prefix_end == 7
    assert snapshot.messages[0].content == "worker"
    assert snapshot.messages[-1].content == "delegated goal"
    assert all("call-active" not in str(message.tool_calls) for message in snapshot.messages)
    assert snapshot.messages[3].content == encoded({"status": "unknown"})
    parent[1] = Message(role="user", content="changed later")
    assert snapshot.messages[1].content == "first request"


def test_explicit_fork_position_must_be_a_complete_turn() -> None:
    with pytest.raises(ValueError, match="completed turn"):
        fork_context(history(), "task", source_run_id="run", system_prompt="worker", prefix_end=4)


def test_smaller_worker_window_removes_whole_turns() -> None:
    snapshot = fork_context(
        history(), "task", source_run_id="run", system_prompt="worker", max_messages=4
    )
    assert snapshot.omitted_turns == 1
    assert [message.content for message in snapshot.messages] == [
        "worker",
        "second request",
        "second answer",
        "task",
    ]
    assert snapshot.prefix_end == 7


def test_task_itself_cannot_be_silently_truncated() -> None:
    with pytest.raises(ValueError, match="context limit"):
        fork_context(
            history(), "long task", source_run_id="run", system_prompt="worker", max_bytes=1
        )
    with pytest.raises(ValueError, match="context limit"):
        fresh_context("long task", system_prompt="worker", max_bytes=1)


def test_fork_rejects_orphan_results() -> None:
    broken = [
        Message(role="user", content="task"),
        Message(role="tool", call_id="missing", content="result"),
    ]
    with pytest.raises(ValueError, match="matching pending call"):
        completed_turns(broken)
