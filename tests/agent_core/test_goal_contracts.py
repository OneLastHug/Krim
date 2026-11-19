"""Strict goal wire contracts and digest stability."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from krim_agent.goals.contracts import (
    EvidenceRef,
    GoalControlRequest,
    GoalCreateRequest,
    GoalRead,
    GoalSnapshot,
    GoalUpdate,
    goal_digest,
)


def test_goal_update_accepts_json_lists_and_rejects_bad_shapes():
    body = GoalUpdate.model_validate(
        {
            "request_key": "create-1",
            "expected_revision": 0,
            "action": "set",
            "objective": "  完成研究报告  ",
            "completion_criteria": [" 来源引用齐全 ", "结论可追溯"],
            "evidence_refs": [{"kind": "test", "ref": "pytest:test_x", "note": "通过"}],
        }
    )
    assert body.objective == "完成研究报告"
    assert body.completion_criteria == ("来源引用齐全", "结论可追溯")
    assert body.evidence_refs[0].kind == "test"

    for bad in (
        {"action": "explode", "objective": "x"},
        {"action": "set", "objective": "   "},
        {"action": "set", "objective": "x", "completion_criteria": ["a", "a"]},
        {"action": "set", "objective": "x", "completion_criteria": ["  "]},
        {"action": "set", "objective": "x", "request_key": "bad key"},
        {"action": "set", "objective": "x", "evidence_refs": [{"kind": "other", "ref": " "}]},
        {"action": "progress", "objective": "x", "unexpected": True},
    ):
        with pytest.raises(ValidationError):
            GoalUpdate.model_validate({"request_key": "k", "expected_revision": 0, **bad})


def test_goal_control_and_create_requests_are_strict():
    control = GoalControlRequest(request_key="pause-1", expected_revision=3, reason="用户暂停")
    assert control.expected_revision == 3
    create = GoalCreateRequest(
        request_key="new",
        expected_revision=2,
        objective="新目标",
        completion_criteria=["证据"],
    )
    assert create.objective == "新目标"
    with pytest.raises(ValidationError):
        GoalCreateRequest(
            request_key="new", expected_revision=2, objective="x", completion_criteria=[""]
        )
    with pytest.raises(ValidationError):
        GoalControlRequest(request_key="bad key", expected_revision=1)
    with pytest.raises(ValidationError):
        GoalRead(revision=-1)


def test_snapshot_and_digest_are_content_addressed():
    evidence = (EvidenceRef(kind="tool_call", ref="call-1"),)
    first = goal_digest({"goal_id": "g", "revision": 1})
    second = goal_digest({"revision": 1, "goal_id": "g"})
    assert first == second and len(first) == 64
    assert goal_digest({"goal_id": "g", "revision": 2}) != first

    snapshot = GoalSnapshot(
        goal_id=None,
        session_id="s",
        workspace_id="w",
        owner_id=None,
        revision=0,
        status=None,
        objective=None,
        content_hash=goal_digest({"version": "goals/1", "goal": None}),
    )
    assert snapshot.continuation is None and snapshot.plan is None
    assert snapshot.model_dump(mode="json")["evidence_refs"] == []
    assert evidence[0].identity == ("tool_call", "call-1")
