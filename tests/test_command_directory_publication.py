"""真实隔离目录来源到0036提案，按原scope/资源/原操作发布与撤销。"""

import asyncio
import json
import os
import shutil
import sys

import pytest
from alembic import command
from sqlalchemy import delete, select, text, update
from test_command_management_tcp import command_server as command_server  # noqa: F401
from test_command_publication import execute, setup

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.client import KrimClient
from krim_agent.execution import CommandSpec
from krim_agent.execution.directory_publication import (
    CommandDirectoryChangesRequest,
    CommandDirectoryProposalRequest,
    CommandDirectoryProposalView,
)
from krim_agent.persistence.database import CommandDirectoryProposalRow
from krim_agent.persistence.migration import migration_config
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import AgentCommandRequest, DirectoryChangeAction, DirectoryQueryRequest
from krim_agent.workspace_directory import ArtifactDirectorySpec, DirectoryPreviewRequest

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要真实Linux隔离命令与目录条件事务",
)

NAMES = [
    "command_execute",
    "command_get",
    "workspace_read",
    "directory_source_preview",
    "directory_source_query",
    "directory_change",
    "directory_query",
    "directory_reconcile",
    "directory_finalize",
]


async def proposal(service, agent, command_id, path="empty", key="preview", **kwargs):
    request = CommandDirectoryProposalRequest(
        command_id=command_id, path=path, request_key=key, **kwargs
    )
    value = await service._workspace_call(
        agent.agent_id, "directory_source_preview", request.model_dump()
    )
    return CommandDirectoryProposalView.model_validate_json(json.dumps(value))


async def test_real_command_directory_source_create_keep_undo_and_same_key(runtime, tmp_path):
    service, agent = await setup(runtime, tmp_path, names=NAMES)
    try:
        result = await execute(agent, "import os;os.mkdir('empty');os.chmod('empty',0o750)")
        assert result.outcome == "completed" and not (tmp_path / "empty").exists()
        view = await proposal(service, agent, result.command_id)
        assert view.directory.empty and view.directory.mode == 0o750
        assert view.proposal.path == "empty" and view.proposal.operation == "create"
        repeated = await proposal(service, agent, result.command_id)
        assert repeated.proposal.proposal_id == view.proposal.proposal_id
        catalog = await service.command_directory_changes(
            agent.agent_id,
            CommandDirectoryChangesRequest(command_id=result.command_id, limit=1),
        )
        assert (
            catalog.entries[0].path == "empty"
            and catalog.entries[0].proposal_id == view.proposal.proposal_id
        )
        applied = await agent.change_directory(
            DirectoryChangeAction(
                proposal_id=view.proposal.proposal_id,
                action="apply",
                expected_revision=0,
                request_key="apply",
            )
        )
        assert applied.status == "done" and (tmp_path / "empty").is_dir()
        queried = await agent.query_directory(
            DirectoryQueryRequest(kind="proposal", identifier=view.proposal.proposal_id)
        )
        kept = await agent.change_directory(
            DirectoryChangeAction(
                proposal_id=view.proposal.proposal_id,
                action="keep",
                expected_revision=queried.proposal.revision,
                request_key="keep",
            )
        )
        assert kept.status == "done"
        queried = await agent.query_directory(
            DirectoryQueryRequest(kind="proposal", identifier=view.proposal.proposal_id)
        )
        undone = await agent.change_directory(
            DirectoryChangeAction(
                proposal_id=view.proposal.proposal_id,
                action="undo",
                expected_revision=queried.proposal.revision,
                request_key="undo",
            )
        )
        assert undone.status == "done" and not (tmp_path / "empty").exists()
        await agent.finalize_directory_change(undone.operation_id)
        with pytest.raises(ValueError):
            DirectoryPreviewRequest(
                spec=ArtifactDirectorySpec(
                    path="empty",
                    operation="create",
                    mode=0o750,
                    command_id=result.command_id,
                    source_hash=view.source_hash,
                ),
                request_key="ordinary-forged",
            )
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["missing", "hash", "mode", "path"])
async def test_source_link_missing_or_changed_cannot_publish(runtime, tmp_path, fault):
    service, agent = await setup(runtime, tmp_path, names=NAMES)
    try:
        result = await execute(agent, "import os;os.mkdir('empty')")
        view = await proposal(service, agent, result.command_id)
        database = runtime.repository.database
        async with database.write_lock, database.sessions.begin() as sql:
            if fault == "missing":
                await sql.execute(
                    delete(CommandDirectoryProposalRow).where(
                        CommandDirectoryProposalRow.proposal_id == view.proposal.proposal_id
                    )
                )
            else:
                changes = {
                    "hash": {"source_hash": "0" * 64},
                    "mode": {"mode": 0o700},
                    "path": {"path": "other"},
                }[fault]
                await sql.execute(
                    update(CommandDirectoryProposalRow)
                    .where(CommandDirectoryProposalRow.proposal_id == view.proposal.proposal_id)
                    .values(**changes)
                )
        with pytest.raises(ServiceError) as rejected:
            await agent.change_directory(
                DirectoryChangeAction(
                    proposal_id=view.proposal.proposal_id,
                    action="apply",
                    expected_revision=0,
                    request_key="apply",
                )
            )
        assert rejected.value.code == "execution_proposal_source"
        assert not (tmp_path / "empty").exists()
    finally:
        await service.close()


async def test_failed_directory_requires_accept_and_nested_directory_is_container_only(
    runtime, tmp_path
):
    service, agent = await setup(runtime, tmp_path, names=NAMES)
    try:
        result = await execute(
            agent,
            "import os,sys;os.makedirs('parent/child');"
            "open('parent/file.txt','w').write('x');sys.exit(3)",
        )
        assert result.outcome == "failed" and result.artifacts_complete
        with pytest.raises(ServiceError) as denied:
            await proposal(service, agent, result.command_id, path="parent")
        assert denied.value.code == "execution_failed_artifacts"
        parent = await proposal(
            service, agent, result.command_id, path="parent", accept_failed=True
        )
        assert not parent.directory.empty and parent.accept_failed
        changed = await agent.change_directory(
            DirectoryChangeAction(
                proposal_id=parent.proposal.proposal_id,
                action="apply",
                expected_revision=0,
                request_key="parent-apply",
            )
        )
        assert changed.status == "done" and list((tmp_path / "parent").iterdir()) == []
        child = await proposal(
            service, agent, result.command_id, path="parent/child", key="child", accept_failed=True
        )
        assert child.directory.empty and not (tmp_path / "parent/child").exists()
        async with runtime.repository.database.sessions() as sql:
            links = list(await sql.scalars(select(CommandDirectoryProposalRow)))
            assert len(links) == 2
    finally:
        await service.close()


async def test_sync_client_real_tcp_source_proposal_and_original_directory_action(
    command_server, tmp_path
):
    url, root, _app = command_server

    def scenario():
        with KrimClient(url, "test-key") as client:
            client.agent_profile_put(
                AgentProfileUpdate(
                    expected_revision=0,
                    config=AgentProfile(
                        name="source-tcp",
                        description="目录来源TCP",
                        workspace_key="local",
                        execution_key="local",
                        tool_names=NAMES,
                    ),
                )
            )
            agent = client.agent_spawn("source-tcp")
            accepted = client.agent_command_start(
                agent.agent_id,
                AgentCommandRequest(
                    request_key="source-tcp-command",
                    command=CommandSpec(
                        argv=[
                            "/usr/bin/python3",
                            "-I",
                            "-c",
                            "import os;os.mkdir('empty');os.chmod('empty',0o750)",
                        ]
                    ),
                ),
                confirmed=True,
            )
            result = client.agent_command_wait(agent.agent_id, accepted.command_id)
            assert result.outcome == "completed" and not (root / "empty").exists()
            request = CommandDirectoryProposalRequest(
                command_id=result.command_id, path="empty", request_key="source-tcp-proposal"
            )
            proposal = client.agent_command_directory_propose(agent.agent_id, request)
            assert proposal.directory.empty and proposal.proposal.mode == 0o750
            page = client.agent_command_directory_changes(
                agent.agent_id, result.command_id, limit=1
            )
            assert (
                page.entries[0].path == "empty"
                and page.entries[0].proposal_id == proposal.proposal.proposal_id
            )
            applied = client.agent_directory_change(
                agent.agent_id,
                DirectoryChangeAction(
                    proposal_id=proposal.proposal.proposal_id,
                    action="apply",
                    expected_revision=0,
                    request_key="apply",
                ),
                confirmed=True,
            )
            assert applied.status == "done" and (root / "empty").is_dir()
            repeated = client.agent_command_directory_propose(agent.agent_id, request)
            assert repeated.proposal.proposal_id == proposal.proposal.proposal_id
            current = client.agent_directory_query(
                agent.agent_id, kind="proposal", identifier=proposal.proposal.proposal_id
            ).proposal
            undone = client.agent_directory_change(
                agent.agent_id,
                DirectoryChangeAction(
                    proposal_id=proposal.proposal.proposal_id,
                    action="undo",
                    expected_revision=current.revision,
                    request_key="undo",
                ),
                confirmed=True,
            )
            assert undone.status == "done" and not (root / "empty").exists()
            return agent.agent_id, request

    agent_id, request = await asyncio.to_thread(scenario)
    assert agent_id and request.command_id
    path = tmp_path / "directory-request.json"
    path.write_text(request.model_dump_json())
    env = {key: value for key, value in os.environ.items() if not key.startswith("KRIM_AGENT_")}
    env["KRIM_AGENT_API_KEY"] = "test-key"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "krim_agent",
        "commands",
        "directory-propose",
        agent_id,
        request.command_id,
        "--request",
        str(path),
        "--url",
        url,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), 20)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    assert process.returncode == 0, stderr.decode()
    value = json.loads(stdout)
    assert value["command_id"] == request.command_id and value["proposal"]["status"] == "undone"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "krim_agent",
        "commands",
        "directory-changes",
        agent_id,
        request.command_id,
        "--url",
        url,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), 20)
    assert process.returncode == 0, stderr.decode()
    assert json.loads(stdout)["entries"][0]["path"] == "empty"


async def test_same_key_concurrent_previews_have_one_source_link(runtime, tmp_path):
    service, agent = await setup(runtime, tmp_path, names=NAMES)
    try:
        result = await execute(agent, "import os;os.mkdir('empty')")
        first, second = await asyncio.gather(
            proposal(service, agent, result.command_id),
            proposal(service, agent, result.command_id),
        )
        assert first.proposal.proposal_id == second.proposal.proposal_id
        async with runtime.repository.database.sessions() as sql:
            links = list(await sql.scalars(select(CommandDirectoryProposalRow)))
            assert len(links) == 1
    finally:
        await service.close()


async def test_source_changed_after_stage_intent_is_rejected_before_publish(
    runtime, tmp_path, monkeypatch
):
    service, agent = await setup(runtime, tmp_path, names=NAMES)
    try:
        result = await execute(agent, "import os;os.mkdir('empty')")
        view = await proposal(service, agent, result.command_id)
        store = service.manager.directory_changes.store
        original_record = store.record_intent

        async def change_after_intent(scope, identifier, reference):
            await original_record(scope, identifier, reference)
            async with store.database.write_lock, store.database.sessions.begin() as sql:
                await sql.execute(
                    update(CommandDirectoryProposalRow)
                    .where(CommandDirectoryProposalRow.proposal_id == view.proposal.proposal_id)
                    .values(source_hash="0" * 64)
                )

        monkeypatch.setattr(store, "record_intent", change_after_intent)
        result = await agent.change_directory(
            DirectoryChangeAction(
                proposal_id=view.proposal.proposal_id,
                action="apply",
                expected_revision=0,
                request_key="apply",
            )
        )
        assert result.status == "rejected" and not (tmp_path / "empty").exists()
        assert result.receipt is not None and not result.receipt.changed
    finally:
        await service.close()


async def test_nonempty_source_migration_refuses_loss_and_retains_directory_ledger(
    runtime, tmp_path
):
    service, agent = await setup(runtime, tmp_path, names=NAMES)
    try:
        result = await execute(agent, "import os;os.mkdir('empty')")
        view = await proposal(service, agent, result.command_id)
    finally:
        await service.close()
    database = runtime.repository.database
    with pytest.raises(RuntimeError, match="命令目录来源非空"):
        async with database.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(
                    migration_config(sync), "0036_workspace_directory_ledger"
                )
            )
    async with database.engine.connect() as connection:
        assert (
            await connection.execute(text("SELECT version_num FROM alembic_version"))
        ).scalar_one() == "0046_tree_exchange_origins"
        assert (
            await connection.execute(text("SELECT proposal_id FROM command_directory_proposals"))
        ).scalar_one() == view.proposal.proposal_id
        assert not (await connection.execute(text("PRAGMA foreign_key_check"))).all()
