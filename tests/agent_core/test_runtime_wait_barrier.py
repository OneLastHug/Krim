"""An SDK checkpoint waits for both tool approval and structured user input."""

from types import SimpleNamespace

import pytest

from krim_agent.runtime.service import RunService


def event(kind, request_id):
    return SimpleNamespace(kind=kind, data={"request_id": request_id})


def service():
    # These helpers use only durable snapshot facts; no background dispatcher.
    return object.__new__(RunService)


@pytest.mark.parametrize("approval,input_reply,expected", [
    (False, False, False), (True, False, False), (False, True, False), (True, True, True),
])
def test_mixed_waits_require_every_reply(approval, input_reply, expected):
    events = [event("approval_request", "approval"), event("user_input_request", "question")]
    if approval:
        events.append(event("approval_reply", "approval"))
    if input_reply:
        events.append(event("user_input_reply", "question"))
    assert service()._all_waits_answered(SimpleNamespace(events=events)) is expected


def test_reply_ids_cannot_satisfy_another_kind_of_wait():
    events = [event("approval_request", "same-id"), event("user_input_request", "same-id"),
              event("approval_reply", "same-id")]
    assert service()._all_waits_answered(SimpleNamespace(events=events)) is False


def test_resume_guard_rejects_early_trigger_even_with_no_registered_task():
    snapshot = SimpleNamespace(
        run=SimpleNamespace(status="awaiting", stop_requested=False),
        events=[event("user_input_request", "question")],
    )
    assert service()._trigger_resume("run", snapshot) is False
