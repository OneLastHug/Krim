"""Reject project-relative shell mistakes at the recoverable schema boundary."""

import pytest
from pydantic import ValidationError

from krim_agent.development.service import ShellInput
from krim_agent.tools.input_errors import ToolInputError


@pytest.mark.parametrize(
    "cwd", ["/workspace", "../escape", "a/../b", "C:\\tmp", "a\\b", "bad\x00name"]
)
def test_shell_rejects_invalid_cwd_before_dispatch(cwd):
    with pytest.raises(ValidationError) as caught:
        ShellInput(command="pwd", cwd=cwd)
    details = ToolInputError(caught.value, ShellInput.model_json_schema()).details
    assert details["executor_started"] is False
    assert details["errors"][0]["field"] == ["cwd"]
    assert cwd not in str(details)


@pytest.mark.parametrize("cwd", [".", "backend", "frontend/src"])
def test_shell_accepts_project_relative_cwd(cwd):
    assert ShellInput(command="pwd", cwd=cwd).cwd == cwd


@pytest.mark.parametrize("command", ["   ", "echo\x00bad"])
def test_shell_rejects_empty_or_nul_command_before_dispatch(command):
    with pytest.raises(ValidationError):
        ShellInput(command=command)
