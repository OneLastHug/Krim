import asyncio
from pathlib import Path

import pytest

from krim_agent.development.contracts import CommandRequest, OutputPage, ProcessHandle
from krim_agent.development.output import shell_receipt


def handle(*, state="exited", exit_code=127):
    request = CommandRequest.model_validate_json(
        (Path(__file__).parent / "fixtures" / "c0-v1.json").read_text(encoding="utf-8")
    )
    return ProcessHandle(
        process_id="process-a", operation_id=request.operation_id,
        environment=request.environment, ownership=request.ownership,
        lifecycle=request.lifecycle, state=state, exit_code=exit_code, log_ref="log-a",
    )


@pytest.mark.asyncio
async def test_exit_127_and_stderr_are_visible_in_first_receipt():
    process = handle()

    async def read(saved, cursor):
        assert saved == process and cursor == 0
        return OutputPage(process_id=saved.process_id, cursor=0, next_cursor=33,
                          text="sh: missing-command: not found\n", eof=True)

    result = await shell_receipt(process, read)
    assert result["state"] == "exited" and result["exit_code"] == 127
    assert "not found" in result["output"]["text"]
    assert result["log_ref"] == "log-a"


@pytest.mark.asyncio
async def test_cursor_and_utf8_page_are_not_recomputed_from_character_count():
    process = handle(state="running", exit_code=None)

    async def read(saved, cursor):
        return OutputPage(process_id=saved.process_id, cursor=0, next_cursor=3,
                          text="好", truncated=True, eof=False)

    result = await shell_receipt(process, read)
    assert result["exit_code"] is None and result["state"] == "running"
    assert result["output"]["next_cursor"] == 3
    assert result["output"]["truncated"] is True


@pytest.mark.asyncio
async def test_log_failure_does_not_change_confirmed_command_result_or_leak_error():
    process = handle()

    async def read(saved, cursor):
        raise OSError("private endpoint or token")

    result = await shell_receipt(process, read)
    assert result["exit_code"] == 127 and result["state"] == "exited"
    assert result["output"] is None
    assert result["output_error"]["cursor"] == 0
    assert "private" not in str(result)


@pytest.mark.asyncio
async def test_cancellation_is_not_reclassified_as_output_failure():
    async def read(saved, cursor):
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await shell_receipt(handle(), read)


@pytest.mark.asyncio
async def test_other_process_output_is_not_exposed():
    async def read(saved, cursor):
        return OutputPage(process_id="other-process", cursor=0, next_cursor=5, text="other")

    result = await shell_receipt(handle(), read)
    assert result["output"] is None
    assert "other-process" not in str(result)
