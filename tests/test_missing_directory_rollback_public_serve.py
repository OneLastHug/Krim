"""实际serve显式授权、当前树inspect、确认回退与持久重启冷读取。"""

import asyncio
import os
import socket
import stat
import sys

import httpx
import pytest
from test_directory_checkpoint_capture import make_tree
from test_missing_baseline_rollback_control import NAMES, action
from test_missing_directory_rollback_public_consumers import cli

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.client import ClientError, KrimClient
from krim_agent.sdk import (
    DirectoryCheckpointCreate,
    DirectoryRestoreTarget,
    MissingDirectoryRestoreExecute,
    MissingDirectoryRestorePreview,
    MissingDirectoryRollbackPreview,
    MissingDirectoryRollbackQuery,
)
from krim_agent.workspace_missing_directory_restore_control import (
    MISSING_DIRECTORY_RESTORE_STEP_TOOLS,
)


async def test_actual_serve_inspect_current_version_confirm_preserve_and_restart_same_facts(
    tmp_path,
):
    files = tmp_path / "files"
    files.mkdir()
    make_tree(files)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    listener.close()
    url = f"http://127.0.0.1:{port}"
    environment = dict(os.environ)
    environment.update(
        {
            "KRIM_AGENT_DATABASE_URL": f"sqlite+aiosqlite:///{tmp_path / 'state.sqlite'}",
            "KRIM_AGENT_PROVIDER": "fake",
            "KRIM_AGENT_API_KEY": "",
            "KRIM_AGENT_INTERACTIVE_TOOLS": "true",
        }
    )

    async def start():
        child = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            "-m",
            "krim_agent",
            "serve",
            "--workspace",
            str(files),
            "--workspace-write",
            "--tree-moves",
            "--directory-checkpoints",
            "--missing-directory-restores",
            "--missing-directory-rollbacks",
            "--port",
            str(port),
            env=environment,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            async with httpx.AsyncClient(timeout=1) as api, asyncio.timeout(25):
                while True:
                    if child.returncode is not None:
                        _, errors = await child.communicate()
                        raise AssertionError(errors.decode())
                    try:
                        if (await api.get(url + "/healthz")).status_code == 200:
                            return child
                    except httpx.TransportError:
                        pass
                    await asyncio.sleep(0.05)
        except BaseException:
            await stop(child)
            raise

    async def stop(child):
        if child is not None and child.returncode is None:
            child.terminate()
            try:
                _, errors = await asyncio.wait_for(child.communicate(), 10)
                assert child.returncode in {0, -15}, errors.decode()
            except TimeoutError:
                child.kill()
                await child.communicate()
                raise

    process = None
    try:
        process = await start()
        with KrimClient(url) as client:
            await asyncio.to_thread(
                client.agent_profile_put,
                AgentProfileUpdate(
                    expected_revision=0,
                    config=AgentProfile(
                        name="public-missing-rollback",
                        description="真实serve缺失基线回退与持久重启",
                        workspace_key="local",
                        persistent=True,
                        tool_timeout_seconds=10,
                        tool_names=[
                            "workspace_directory_checkpoint_capture",
                            *MISSING_DIRECTORY_RESTORE_STEP_TOOLS.values(),
                            "tree_move_inspect",
                            *NAMES,
                        ],
                    ),
                ),
            )
            agent = await asyncio.to_thread(client.agent_spawn, "public-missing-rollback")
            captured = await asyncio.to_thread(
                client.agent_workspace_create_directory_checkpoint,
                agent.agent_id,
                DirectoryCheckpointCreate(root_path="tree", request_key="original-target"),
                confirmed=True,
            )
            target = await asyncio.to_thread(
                client.agent_workspace_wait_directory_checkpoint,
                agent.agent_id,
                captured.checkpoint_id,
                timeout_seconds=10,
            )
            assert target.status == "available", target
            (files / "tree").rename(files / "retained-original")
            planned = await asyncio.to_thread(
                client.agent_workspace_preview_missing_directory_restore,
                agent.agent_id,
                MissingDirectoryRestorePreview(
                    target=DirectoryRestoreTarget(identifier=target.checkpoint_id),
                    request_key="original-missing",
                ),
                confirmed=True,
            )
            ready = await asyncio.to_thread(
                client.agent_workspace_wait_missing_directory_restore,
                agent.agent_id,
                planned.plan_id,
                timeout_seconds=10,
            )
            assert ready.phase == "ready", ready
            await asyncio.to_thread(
                client.agent_workspace_execute_missing_directory_restore,
                agent.agent_id,
                MissingDirectoryRestoreExecute(
                    plan_id=ready.plan_id,
                    expected_revision=ready.revision,
                    request_key="original-execute",
                ),
                confirmed=True,
            )
            source = await asyncio.to_thread(
                client.agent_workspace_wait_missing_directory_restore,
                agent.agent_id,
                ready.plan_id,
                timeout_seconds=10,
            )
            assert source.phase == "finished", source
            (files / "tree/nested/nul").write_bytes(b"user\0modified\xff")
            (files / "tree/user-extra").write_bytes(b"preserve me")
            inspected = await asyncio.to_thread(
                client.agent_tree_move_inspect, agent.agent_id, "tree"
            )
            assert inspected.found and inspected.fingerprint != source.receipt.after_fingerprint
            wanted = MissingDirectoryRollbackPreview(
                plan_id=source.plan_id,
                preserve_path="preserved",
                expected_fingerprint=inspected.fingerprint,
                request_key="original-preview",
            )
            proposal = await asyncio.to_thread(
                client.agent_missing_directory_rollback_preview,
                agent.agent_id,
                wanted,
                confirmed=True,
            )
            assert proposal.approved_fingerprint == inspected.fingerprint
            assert proposal.published_fingerprint == source.receipt.after_fingerprint
            with pytest.raises(ClientError) as denied:
                await asyncio.to_thread(
                    client.agent_missing_directory_rollback_change, agent.agent_id, action(proposal)
                )
            assert denied.value.status == 409 and denied.value.code == "approval_required"
            assert (files / "tree").is_dir() and not (files / "preserved").exists()
            request_file = tmp_path / "change.json"
            request_file.write_text(action(proposal).model_dump_json())
            applied = await cli(
                url, agent.agent_id, "change", None, "--request", str(request_file), "--confirm"
            )
            assert applied["operation"]["status"] == "done" and not (files / "tree").exists()
            assert (files / "preserved/nested/nul").read_bytes() == b"user\0modified\xff"
            assert stat.S_IMODE((files / "preserved/nested/nul").stat().st_mode) == 0o640
            inode = (files / "preserved").stat().st_ino
        await stop(process)
        process = await start()
        with KrimClient(url) as client:
            original = await asyncio.to_thread(
                client.agent_missing_directory_rollback_query,
                agent.agent_id,
                MissingDirectoryRollbackQuery(
                    kind="operation_by_key", request_key="rollback-apply"
                ),
            )
            assert original.operation.model_dump(mode="json") == applied
            assert (
                await asyncio.to_thread(
                    client.agent_workspace_missing_directory_restore, agent.agent_id, source.plan_id
                )
                == source
            )
        assert (
            await cli(
                url,
                agent.agent_id,
                "query",
                None,
                "--kind",
                "by_key",
                "--request-key",
                "original-preview",
            )
        )["proposal"]["rollback_state"] == "preserved"
        assert (files / "preserved").stat().st_ino == inode
        assert (files / "preserved/user-extra").read_bytes() == b"preserve me"
    finally:
        await stop(process)
