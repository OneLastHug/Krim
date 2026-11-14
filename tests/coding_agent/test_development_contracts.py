"""Consumer examples for C0. These checks do not claim backend isolation."""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from krim_agent.development.contracts import (
    CommandRequest,
    OperationAdmission,
    OperationIdentity,
    OperationReceipt,
    OutputPage,
    ProcessHandle,
)

FIXTURE = Path(__file__).parent / "fixtures" / "c0-v1.json"


def command(**changes: object) -> CommandRequest:
    values = json.loads(FIXTURE.read_text())
    values.update(changes)
    return CommandRequest.model_validate_json(json.dumps(values))


def test_first_command_contains_complete_ownership_without_host_paths() -> None:
    request = command()
    assert request.environment.owner_id == request.ownership.owner_id
    assert request.authorization.project_id == request.environment.project_id
    assert request.operation_id == "operation-first-command"
    assert "host_path" not in request.model_dump_json()
    assert CommandRequest.model_validate_json(request.model_dump_json()) == request


@pytest.mark.parametrize("cwd", ["/etc", "../other", "a/../../other", "C:/data", "a\\b", "x\x00"])
def test_rejects_non_project_cwd(cwd: str) -> None:
    with pytest.raises(ValidationError):
        command(cwd=cwd)


def test_project_service_retains_creating_run_but_has_independent_lifetime() -> None:
    request = command(lifecycle="project_service", timeout_seconds=None)
    assert request.ownership.run_id == "run-a"
    assert request.timeout_seconds is None
    with pytest.raises(ValidationError):
        command(timeout_seconds=None)


def test_scope_drift_is_not_accepted() -> None:
    values = json.loads(FIXTURE.read_text())
    values["authorization"]["project_id"] = "project-b"
    with pytest.raises(ValidationError):
        command(**values)
    values = json.loads(FIXTURE.read_text())
    values["ownership"]["owner_id"] = "owner-b"
    with pytest.raises(ValidationError):
        command(**values)


def test_unknown_operation_is_not_permission_to_execute_again() -> None:
    request = command()
    identity = OperationIdentity(
        operation_id=request.operation_id,
        call_id="call-1",
        environment=request.environment,
        ownership=request.ownership,
        request_digest="a" * 64,
    )
    receipt = OperationReceipt(identity=identity, state="unknown")
    assert not OperationAdmission(receipt=receipt, execute=False).execute
    with pytest.raises(ValidationError):
        OperationAdmission(receipt=receipt, execute=True)


def test_unknown_process_cannot_claim_a_successful_exit() -> None:
    request = command()
    with pytest.raises(ValidationError):
        ProcessHandle(
            process_id="process-a", operation_id=request.operation_id,
            environment=request.environment, ownership=request.ownership,
            lifecycle="run", state="unknown", exit_code=0, log_ref="log-a",
        )


@pytest.mark.parametrize("section,field,value", [
    ("environment", "project_id", "other-project"),
    ("environment", "environment_id", "other-environment"),
    ("environment", "generation", 2),
    ("ownership", "session_id", "other-session"),
    ("ownership", "run_id", "other-run"),
    (None, "operation_id", "other-operation"),
    (None, "lifecycle", "project_service"),
])
def test_process_reply_cannot_drift_from_admitted_request(
    section: str | None, field: str, value: str | int,
) -> None:
    request = command()
    process = ProcessHandle(
        process_id="process-a", operation_id=request.operation_id,
        environment=request.environment, ownership=request.ownership,
        lifecycle=request.lifecycle, state="running", log_ref="log-a",
    )
    process.require_binding(request)
    data = process.model_dump(mode="json")
    if section:
        data[section][field] = value
    else:
        data[field] = value
    changed = ProcessHandle.model_validate_json(json.dumps(data))
    with pytest.raises(ValueError, match="binding changed"):
        changed.require_binding(request)


def test_output_cursor_is_monotonic_and_independent_of_decoded_length() -> None:
    page = OutputPage(process_id="process-a", cursor=10, next_cursor=13, text="好")
    assert page.next_cursor - page.cursor == 3
    with pytest.raises(ValidationError):
        OutputPage(process_id="process-a", cursor=10, next_cursor=9, text="")
