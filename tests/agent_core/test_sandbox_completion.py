"""Declared completion checks use persisted contracts, not model final text."""

import tomllib
from pathlib import Path

import pytest
from sqlalchemy import select
from test_sandbox_command import approval, command_app, finished

from krim_agent.sandbox.tasks import SandboxCompletion
from krim_agent.storage.sandbox_models import sandbox_operations


def test_background_task_module_is_in_wheel_manifest():
    root = Path(__file__).resolve().parents[2]
    manifest = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    assert "src/krim_agent/sandbox/tasks.py" in (
        manifest["tool"]["hatch"]["build"]["targets"]["wheel"]["only-include"]
    )


@pytest.mark.parametrize(
    "exit_code,expected_status,expected_verification",
    [(0, "completed", "passed"), (1, "failed", "failed")],
)
async def test_declared_completion_evaluates_finished_background_receipt(
    tmp_path, wire, monkeypatch, exit_code, expected_status, expected_verification
):
    async with command_app(
        tmp_path,
        wire,
        monkeypatch,
        tool_name="sandbox_task_start",
        bind_session=True,
        completion=SandboxCompletion(),
    ) as (app, broker, _actor, _project, run_id, request):
        broker.exit_code = exit_code
        await app.service.reply_approval(run_id, request["request_id"], approval(request))
        view = await finished(app, run_id)
        assert view.status == expected_status
        assert view.code == (None if expected_status == "completed" else "completion_unverified")
        assert len(broker.starts) == 1
        assert all(poll["renew"] is False for poll in broker.statuses)
        events = await app.service.events(run_id)
        verdict = next(
            event.data for event in events if event.kind == "sandbox_completion_verified"
        )
        assert verdict["schema"] == "sandbox-completion/1"
        assert verdict["state"] == expected_verification
        async with app.store.lock, app.store.engine.connect() as conn:
            saved = await conn.scalar(
                select(sandbox_operations.c.result).where(
                    sandbox_operations.c.operation_id == broker.starts[0]["operation_id"]
                )
            )
        assert saved["receipt"]["verification"] == expected_verification
        assert saved["receipt"]["completion_verification"] == verdict


async def test_foreground_exit_zero_does_not_satisfy_background_completion_contract(
    tmp_path, wire, monkeypatch
):
    async with command_app(
        tmp_path, wire, monkeypatch, completion=SandboxCompletion()
    ) as (app, broker, _actor, _project, run_id, request):
        await app.service.reply_approval(run_id, request["request_id"], approval(request))
        view = await finished(app, run_id)
        assert view.status == "failed"
        assert view.code == "completion_unverified"
        events = await app.service.events(run_id)
        receipt = next(event.data["value"] for event in events if event.kind == "tool_end")
        assert receipt["exit_code"] == 0
        assert receipt["verification"] == "not_evaluated"
        verdict = next(
            event.data for event in events if event.kind == "sandbox_completion_verified"
        )
        assert verdict["state"] == "failed"
        assert verdict["tasks"] == []
        assert len(broker.starts) == 1
