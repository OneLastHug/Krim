"""真实隔离命令的完整目录依赖计划：原来源绑定、冷读和零写入。"""

import json
import shutil
import sys

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select
from test_command_publication import execute, setup

from krim_agent.execution.directory_publication import (
    CommandDirectoryDependencyRequest,
    CommandDirectoryDependencyView,
)
from krim_agent.persistence.command_directory_dependencies import CommandDirectoryDependencyStore
from krim_agent.persistence.command_models import CommandRow
from krim_agent.persistence.database import (
    AgentSessionRow,
    WorkspaceDirectoryAttemptRow,
    WorkspaceDirectoryProposalRow,
    WorkspaceEditAttemptRow,
    WorkspaceEditProposalRow,
)
from krim_agent.protocol.domain import ServiceError

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要真实Linux隔离命令",
)


async def counts(database):
    async with database.sessions() as sql:
        return [
            await sql.scalar(select(func.count()).select_from(table))
            for table in (
                WorkspaceDirectoryProposalRow,
                WorkspaceDirectoryAttemptRow,
                WorkspaceEditProposalRow,
                WorkspaceEditAttemptRow,
            )
        ]


async def prepare(runtime, tmp_path, *, code=None):
    service, agent = await setup(runtime, tmp_path)
    result = await execute(
        agent,
        code
        or "import os;os.makedirs('tree/sub');os.chmod('tree',0o750);"
        "os.chmod('tree/sub',0o700);open('tree/sub/data.bin','wb').write(bytes([0,255]))",
    )
    store = CommandDirectoryDependencyStore(service.workspace_edits)
    return service, agent, result, store


async def test_real_tree_plan_binds_original_source_and_cold_reads_zero_new_effects(
    runtime, tmp_path, monkeypatch
):
    service, agent, result, store = await prepare(runtime, tmp_path)
    try:
        assert result.outcome == "completed" and not (tmp_path / "tree").exists()

        async def forbidden(*_args, **_kwargs):
            raise AssertionError("依赖计划不得重跑原命令")

        binding = service._handle(agent.agent_id).definition.execution
        assert binding is not None
        monkeypatch.setattr(binding.runner, "start", forbidden)
        scope = "session:" + agent.session_id
        request = CommandDirectoryDependencyRequest(command_id=result.command_id)
        before = await counts(runtime.repository.database)
        plan = await store.plan(scope, request)
        assert plan.command_id == result.command_id and plan.scope_id == scope
        assert plan.workspace_key == "files" and plan.resource_fingerprint
        assert plan.outcome == "completed" and plan.complete
        assert [step.path for step in plan.plan.steps] == ["tree", "tree/sub", "tree/sub/data.bin"]
        assert [step.mode for step in plan.plan.steps] == [0o750, 0o700, None]
        assert plan.files[0].path == "tree/sub/data.bin" and plan.files[0].byte_size == 2
        assert CommandDirectoryDependencyView.model_validate_json(plan.model_dump_json()) == plan
        repeated = await store.plan(
            scope,
            CommandDirectoryDependencyRequest(
                command_id=result.command_id, source_hash=plan.source_hash
            ),
        )
        assert repeated == plan and await counts(runtime.repository.database) == before == [0] * 4
        assert list(tmp_path.iterdir()) == []
    finally:
        await service.close()


@pytest.mark.parametrize("field", ["root", "owner_id", "scope", "manifest", "mode", "call_id"])
def test_request_cannot_supply_private_source_or_authority_fields(field):
    with pytest.raises(ValidationError):
        CommandDirectoryDependencyRequest.model_validate({"command_id": "id", field: "forged"})


@pytest.mark.parametrize("case", ["files", "operation", "accept_failed"])
async def test_json_view_refuses_inconsistent_plan_or_failed_source(runtime, tmp_path, case):
    service, agent, result, store = await prepare(runtime, tmp_path)
    try:
        plan = await store.plan(
            "session:" + agent.session_id,
            CommandDirectoryDependencyRequest(command_id=result.command_id),
        )
        payload = plan.model_dump(mode="json")
        if case == "files":
            payload["files"] = []
        elif case == "operation":
            payload["plan"] = {
                "operation": "delete",
                "steps": [
                    {
                        "path": "tree",
                        "kind": "directory",
                        "mode": 0o750,
                        "depth": 1,
                        "depends_on": [],
                    }
                ],
            }
        else:
            payload["outcome"] = "failed"
        with pytest.raises(ValidationError):
            CommandDirectoryDependencyView.model_validate_json(json.dumps(payload))
        assert await counts(runtime.repository.database) == [0] * 4
    finally:
        await service.close()


@pytest.mark.parametrize("case", ["scope", "hash", "capacity", "resource"])
async def test_plan_refuses_foreign_stale_and_over_capacity_without_new_facts(
    runtime, tmp_path, case
):
    service, agent, result, store = await prepare(runtime, tmp_path)
    try:
        scope = "session:" + agent.session_id
        kwargs = {}
        expected = {
            "scope": "execution_not_found",
            "hash": "execution_directory_dependency_source_conflict",
            "capacity": "execution_directory_dependency_limit",
            "resource": "workspace_binding_changed",
        }[case]
        if case == "scope":
            other = (await runtime.repository.create_topic("foreign")).id
            scope = "topic:" + other
        elif case == "hash":
            kwargs["source_hash"] = "0" * 64
        elif case == "capacity":
            kwargs["max_entries"] = 2
        else:
            handle = service._handle(agent.agent_id)
            row = await service.sessions.for_agent(agent.agent_id)
            async with runtime.repository.database.sessions.begin() as sql:
                current = await sql.get(AgentSessionRow, row.session_id)
                manifest = {**current.permissions["workspace"], "fingerprint": "0" * 64}
                current.permissions = {**current.permissions, "workspace": manifest}
            assert handle.definition.workspace is not None
        with pytest.raises(ServiceError) as refused:
            await store.plan(
                scope, CommandDirectoryDependencyRequest(command_id=result.command_id, **kwargs)
            )
        assert refused.value.code == expected
        assert (
            await counts(runtime.repository.database) == [0] * 4
            and not (tmp_path / "tree").exists()
        )
    finally:
        await service.close()


async def test_failed_complete_tree_requires_explicit_accept(runtime, tmp_path):
    service, agent, result, store = await prepare(
        runtime, tmp_path, code="import os,sys;os.mkdir('tree');sys.exit(3)"
    )
    try:
        assert result.outcome == "failed" and result.artifacts_complete
        scope = "session:" + agent.session_id
        with pytest.raises(ServiceError) as refused:
            await store.plan(scope, CommandDirectoryDependencyRequest(command_id=result.command_id))
        assert refused.value.code == "execution_failed_artifacts"
        value = await store.plan(
            scope,
            CommandDirectoryDependencyRequest(command_id=result.command_id, accept_failed=True),
        )
        assert value.outcome == "failed" and value.accept_failed
        assert [step.path for step in value.plan.steps] == ["tree"]
        assert await counts(runtime.repository.database) == [0] * 4
    finally:
        await service.close()


@pytest.mark.parametrize("case", ["unrecorded", "incomplete", "hash", "mode", "empty"])
async def test_old_incomplete_corrupt_or_unpublishable_directory_source_is_refused(
    runtime, tmp_path, case
):
    service, agent, result, store = await prepare(
        runtime,
        tmp_path,
        code="pass"
        if case == "empty"
        else "import os;os.mkdir('tree');os.chmod('tree',0o500)"
        if case == "mode"
        else None,
    )
    try:
        if case in {"unrecorded", "incomplete", "hash"}:
            async with runtime.repository.database.sessions.begin() as sql:
                row = await sql.get(CommandRow, result.command_id)
                payload = json.loads(json.dumps(row.result))
                if case == "unrecorded":
                    payload.pop("directory_manifest")
                elif case == "incomplete":
                    payload["artifacts_complete"] = False
                else:
                    payload["directory_manifest"]["sha256"] = "0" * 64
                row.result = payload
        expected = {
            "unrecorded": "execution_directories_unrecorded",
            "incomplete": "execution_artifacts_incomplete",
            "hash": "execution_record_corrupt",
            "mode": "execution_artifact_mode",
            "empty": "execution_directory_dependency_empty",
        }[case]
        with pytest.raises(ServiceError) as refused:
            await store.plan(
                "session:" + agent.session_id,
                CommandDirectoryDependencyRequest(command_id=result.command_id),
            )
        assert (
            refused.value.code == expected and await counts(runtime.repository.database) == [0] * 4
        )
        assert list(tmp_path.iterdir()) == []
    finally:
        await service.close()
