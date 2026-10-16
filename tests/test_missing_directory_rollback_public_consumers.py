"""公开五操作沿原完整保留Move；SDK/HTTP/TCP Client/CLI同来源、版本与冷历史。"""

import asyncio
import json
import sys

import pytest
from test_missing_baseline_rollback_control import action, setup
from test_tree_move_public_consumers import api_for, tcp_for

from krim_agent.client import KrimClient
from krim_agent.sdk import MissingDirectoryRollbackQuery
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter


async def cli(url, agent_id, operation, value=None, *options):
    arguments = [
        sys.executable,
        "-I",
        "-m",
        "krim_agent",
        "missing-directory-rollbacks",
        operation,
        agent_id,
    ]
    if value is not None:
        arguments.append(value)
    process = await asyncio.create_subprocess_exec(
        *arguments,
        "--url",
        url,
        *options,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        output, error = await asyncio.wait_for(process.communicate(), 15)
        assert process.returncode == 0, error.decode()
        return json.loads(output)
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()


async def test_public_missing_rollback_preserves_user_changes_keep_reverse_and_cold_keys(
    runtime, tmp_path, monkeypatch
):
    service, agent, binding, source, wanted = await setup(runtime, tmp_path)
    try:
        planned = await agent.preview_missing_directory_rollback(wanted)
        assert (
            planned.source_plan_id == source.plan_id
            and planned.source_operation_id == source.operation_id
        )
        assert (
            planned.baseline == source.baseline
            and planned.approved_fingerprint == wanted.expected_fingerprint
        )
        async with api_for(runtime, service, monkeypatch) as (app, api), tcp_for(app) as url:
            base = f"/v1/agents/{agent.agent_id}/workspace/missing-directory-rollbacks"
            applied = await api.post(base + "/change", json=action(planned).model_dump(mode="json"))
            assert applied.status_code == 202 and applied.json()["operation"]["status"] == "done", (
                applied.text
            )
            assert not (tmp_path / "tree").exists()
            assert (tmp_path / "preserved/nested/nul").read_bytes() == b"user\0modified\xff"
            assert (tmp_path / "preserved/user-extra").read_bytes() == b"preserve me"
            current = (
                await agent.query_missing_directory_rollback(
                    MissingDirectoryRollbackQuery(kind="by_key", request_key=wanted.request_key)
                )
            ).proposal
            assert current.rollback_state == "preserved"
            request_file = tmp_path / "action.json"
            request_file.write_text(action(current, "keep", "public-keep").model_dump_json())
            kept = await cli(url, agent.agent_id, "change", None, "--request", str(request_file))
            assert (
                kept["operation"]["status"] == "done"
                and not kept["operation"]["receipt"]["changed"]
            )
            current = (
                await agent.query_missing_directory_rollback(
                    MissingDirectoryRollbackQuery(
                        kind="proposal", identifier=planned.move.proposal_id
                    )
                )
            ).proposal

            def reverse():
                with KrimClient(url) as client:
                    return client.agent_missing_directory_rollback_change(
                        agent.agent_id, action(current, "restore_tree", "public-reverse")
                    )

            restored = await asyncio.to_thread(reverse)
            assert (
                restored.operation.status == "done"
                and (tmp_path / "tree/user-extra").read_bytes() == b"preserve me"
            )
            assert (tmp_path / "tree/nested/nul").read_bytes() == b"user\0modified\xff" and not (
                tmp_path / "preserved"
            ).exists()
            assert (
                await service.workspace_missing_directory_restore(agent.agent_id, source.plan_id)
                == source
            )

            def forbidden(*_args, **_kwargs):
                pytest.fail("原键/公共历史不能再次准备或发布Move")

            monkeypatch.setattr(WorkspaceTreeWriter, "prepare", forbidden)
            monkeypatch.setattr(WorkspaceTreeWriter, "apply", forbidden)
            original = await agent.preview_missing_directory_rollback(wanted)
            assert original.rollback_state == "restored"
            assert (
                await api.get(
                    base + "/query", params={"kind": "by_key", "request_key": wanted.request_key}
                )
            ).json()["proposal"] == original.model_dump(mode="json")

            def reads():
                with KrimClient(url) as client:
                    assert (
                        client.agent_missing_directory_rollback_preview(agent.agent_id, wanted)
                        == original
                    )
                    assert (
                        client.agent_missing_directory_rollback_query(
                            agent.agent_id,
                            MissingDirectoryRollbackQuery(
                                kind="proposal", identifier=original.move.proposal_id
                            ),
                        ).proposal
                        == original
                    )
                    assert client.agent_missing_directory_rollback_query(
                        agent.agent_id,
                        MissingDirectoryRollbackQuery(
                            kind="history", identifier=original.move.proposal_id
                        ),
                    ).history.items
                    assert client.agent_missing_directory_rollback_query(
                        agent.agent_id, MissingDirectoryRollbackQuery(kind="proposals")
                    ).proposals.items == [original]
                    assert (
                        client.agent_missing_directory_rollback_query(
                            agent.agent_id,
                            MissingDirectoryRollbackQuery(kind="by_key", request_key="absent"),
                        ).proposal
                        is None
                    )
                    assert (
                        client.agent_missing_directory_rollback_reconcile(
                            agent.agent_id, restored.operation.operation_id
                        )
                        == restored
                    )
                    assert (
                        client.agent_missing_directory_rollback_cancel(
                            agent.agent_id, original.move.proposal_id
                        )
                        == original
                    )

            await asyncio.to_thread(reads)
            request_file.write_text(wanted.model_dump_json())
            assert await cli(
                url, agent.agent_id, "preview", None, "--request", str(request_file)
            ) == original.model_dump(mode="json")
            assert (
                await cli(
                    url,
                    agent.agent_id,
                    "query",
                    None,
                    "--kind",
                    "by_key",
                    "--request-key",
                    wanted.request_key,
                )
            )["proposal"] == original.model_dump(mode="json")
            assert await cli(
                url, agent.agent_id, "reconcile", restored.operation.operation_id
            ) == restored.model_dump(mode="json")
            assert await cli(
                url, agent.agent_id, "cancel", original.move.proposal_id
            ) == original.model_dump(mode="json")
            assert not service.manager.tree_moves._operations
            assert binding.workspace.root == tmp_path
    finally:
        await service.close()
