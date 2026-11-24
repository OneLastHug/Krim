"""Legacy copies resume only with matching durable preparation and exit-zero facts."""

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.subagents.isolated_bootstrap import _prior_prepared
from krim_agent.subagents.workspace import WorkspaceAssignment, workspace_directory
from tests.coding_agent.test_development_storage import prepare_child


@pytest.mark.parametrize("broken", [None, "directory", "baseline", "receipt"])
async def test_legacy_preparation_requires_matching_success_ledger(tmp_path, broken):
    store, _, request, _ = await prepare_child(tmp_path / "legacy.sqlite")
    agent_id = ""
    try:
        from sqlalchemy import select

        from krim_agent.storage.subagent_models import subagent_child_runs

        async def owner(conn):
            return (await conn.execute(select(subagent_child_runs).where(
                subagent_child_runs.c.run_id == request.ownership.run_id,
            ))).mappings().one()

        mapping = await store.work_read(owner)
        agent_id = mapping["agent_id"]
        assignment = WorkspaceAssignment(
            mode="isolated_write", agent_id=agent_id,
            source_project_id=request.environment.project_id,
            workspace_project_id=request.environment.project_id,
            directory=workspace_directory(agent_id), baseline_fingerprint="a" * 64,
            input_version=1,
        )
        value = {
            "state": "exited", "exit_code": 1 if broken == "receipt" else 0,
            "environment": request.environment.model_dump(mode="json"),
            "ownership": request.ownership.model_dump(mode="json"),
            "output": {"text": '{"mode":"copy","copied":2,"truncated":false}\n'},
        }
        await store.append(request.ownership.run_id, "tool_end", {
            "name": "shell", "call_id": "legacy-copy", "status": "ok",
            "result_available": True, "value": value,
        })
        await store.append(request.ownership.run_id, "workspace_prepared", {
            "agent_id": agent_id, "call_id": "legacy-copy", "exit_code": 0,
            "directory": "other" if broken == "directory" else assignment.directory,
            "baseline_fingerprint": "b" * 64 if broken == "baseline" else "a" * 64,
        })
        if broken == "receipt":
            with pytest.raises(ServiceError, match="subagent_capability"):
                await _prior_prepared(
                    store, "next-child", assignment, request.ownership, request.environment,
                )
        else:
            prior = await _prior_prepared(
                store, "next-child", assignment, request.ownership, request.environment,
            )
            assert (prior is not None) == (broken is None)
    finally:
        await store.close()


def test_verified_legacy_copy_is_adopted_without_overwriting(tmp_path):
    import json
    import subprocess
    import sys

    from krim_agent.subagents.providers.process import PREPARE_WORKSPACE_SCRIPT

    directory = workspace_directory("legacy-worker")
    target = tmp_path / directory
    target.mkdir(parents=True)
    (target / "app.py").write_text("legacy worker edits")
    (tmp_path / "app.py").write_text("source changed")
    prepared = {"mode": "copy", "copied": 1, "skipped": 0, "truncated": False}
    run = subprocess.run(
        [sys.executable, "-I", "-c", PREPARE_WORKSPACE_SCRIPT, directory,
         json.dumps({"prior_prepared": prepared})], cwd=tmp_path,
        text=True, capture_output=True, check=True,
    )
    result = json.loads(run.stdout)
    assert result["reused"] is True and result["copied"] == 0
    assert (target / "app.py").read_text() == "legacy worker edits"
