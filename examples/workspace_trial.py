"""工作区整体演练：本地SDK、真实CLI/TCP、检查点与跨进程原结果查询。

全部文件和数据库位于临时目录；使用离线FakeProvider，不调用付费服务。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import httpx

import krim_agent
from krim_agent.client import ClientError, KrimClient
from krim_agent.config import Settings
from krim_agent.sdk import (
    CheckpointRestoreExecute,
    CheckpointRestorePreview,
    FileChangeSpec,
    NativeBatchRequest,
    NativeBatchTarget,
    PythonAI,
    WorkspaceChangeAction,
    WorkspaceCheckpointCreate,
)
from krim_agent.task_state import TaskMutation, TaskWriteRequest

ORIGINAL = "工作区版本A\n"
EDITED = "工作区版本B\n"
EXTERNAL = "外部编辑版本C\n"


@dataclass(frozen=True)
class Saved:
    agent_id: str
    checkpoint_id: str
    plan_id: str
    plan_revision: int
    batch_id: str
    apply_id: str
    last_task_id: str


async def sdk_prepare(directory: Path) -> Saved:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{directory / 'workspace.sqlite'}",
        interactive_tools=True,
    )
    async with await PythonAI.create(
        settings=settings,
        workspace_root=directory / "files",
        workspace_version="trial-v1",
        workspace_writable=True,
    ) as ai:
        agent = await ai.agent(persistent=True)
        assert any(item.path == "note.txt" for item in (await agent.list_files()).entries)
        assert (await agent.search_files("版本A")).matches[0].path == "note.txt"
        before = await agent.read_file("note.txt")
        assert before.sha256 == hashlib.sha256(ORIGINAL.encode()).hexdigest()
        tasks = await agent.update_tasks(
            "plan",
            TaskMutation(
                operation="replace",
                items=["保存和修改文件", "重启后恢复并验证"],
                expected_revision=0,
                request_key="trial-plan",
            ),
        )
        checkpoint = await agent.create_file_checkpoint(
            WorkspaceCheckpointCreate(
                title="演练原版本",
                paths=["note.txt", "missing.txt"],
                request_key="trial-checkpoint",
            )
        )
        checkpoint = await agent.wait_file_checkpoint(checkpoint.checkpoint_id)
        assert checkpoint.status == "available", checkpoint
        proposal = await agent.preview_file_change(
            FileChangeSpec(
                path="note.txt",
                operation="write",
                expected_sha256=before.sha256,
                content=EDITED,
            ),
            request_key="trial-change-preview",
        )
        assert "-工作区版本A" in proposal.preview.diff and "+工作区版本B" in proposal.preview.diff
        assert (directory / "files/note.txt").read_text() == ORIGINAL
        applied = await agent.change_file(
            WorkspaceChangeAction(
                proposal_id=proposal.proposal_id,
                action="apply",
                expected_revision=proposal.revision,
                request_key="trial-change-apply",
            ),
            confirmed=True,
        )
        if applied.status == "uncertain":
            applied = await agent.reconcile_file_change(applied.operation_id, confirmed=True)
        assert applied.status == "done", applied
        await agent.finalize_file_change(applied.operation_id, confirmed=True)
        page = await agent.file_change_list()
        item = next(item for item in page.items if item.proposal_id == proposal.proposal_id)
        batch = await agent.start_file_change_batch(
            NativeBatchRequest(
                action="keep",
                snapshot_revision=page.snapshot_revision,
                targets=[
                    NativeBatchTarget(proposal_id=item.proposal_id, expected_revision=item.revision)
                ],
                request_key="trial-keep",
            ),
            confirmed=True,
        )
        batch = await agent.wait_file_change_batch(batch.batch_id)
        if batch.status == "uncertain":
            operation = batch.items[0].operation
            assert operation is not None
            await agent.reconcile_file_change(operation.operation_id, confirmed=True)
            batch = await agent.file_change_batch(batch.batch_id)
        assert batch.status == "done", batch
        plan = await agent.preview_file_checkpoint_restore(
            CheckpointRestorePreview(
                checkpoint_id=checkpoint.checkpoint_id,
                request_key="trial-restore-preview",
            )
        )
        plan = await agent.wait_file_checkpoint_restore(plan.plan_id)
        assert plan.phase == "ready", plan
        await agent.update_tasks(
            "plan",
            TaskMutation(
                operation="complete",
                item_id=tasks.items[0].id,
                expected_revision=1,
                request_key="trial-first-done",
            ),
        )
        return Saved(
            agent.agent_id,
            checkpoint.checkpoint_id,
            plan.plan_id,
            plan.revision,
            batch.batch_id,
            applied.operation_id,
            tasks.items[1].id,
        )


class Server:
    def __init__(self, directory: Path) -> None:
        self.directory = directory
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            self.port = listener.getsockname()[1]
        self.url = f"http://127.0.0.1:{self.port}"
        self.environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("KRIM_AGENT_") and key != "PYTHONPATH"
        }
        self.environment.update(
            {
                "KRIM_AGENT_PROVIDER": "fake",
                "KRIM_AGENT_DATABASE_URL": f"sqlite+aiosqlite:///{directory / 'workspace.sqlite'}",
                "KRIM_AGENT_INTERACTIVE_TOOLS": "true",
                "KRIM_AGENT_API_KEY": "workspace-trial-key",
            }
        )
        self.process: subprocess.Popen[bytes] | None = None
        self.starts = 0

    def start(self) -> None:
        assert self.process is None
        with (self.directory / "server.log").open("ab") as output:
            self.process = subprocess.Popen(
                [
                    sys.executable,
                    "-I",
                    "-m",
                    "krim_agent",
                    "serve",
                    "--port",
                    str(self.port),
                    "--workspace",
                    str(self.directory / "files"),
                    "--workspace-version",
                    "trial-v1",
                    "--workspace-write",
                ],
                env=self.environment,
                stdout=output,
                stderr=output,
            )
        deadline = time.monotonic() + 30
        with httpx.Client(trust_env=False, timeout=1) as http:
            while time.monotonic() < deadline:
                if self.process.poll() is not None:
                    raise RuntimeError("工作区演练服务启动失败")
                try:
                    if http.get(self.url + "/healthz").is_success:
                        self.starts += 1
                        return
                except httpx.HTTPError:
                    pass
                time.sleep(0.05)
        raise RuntimeError("工作区演练服务尚未就绪")

    def stop(self) -> None:
        process = self.process
        if process is None:
            return
        process.send_signal(signal.SIGINT if os.name != "nt" else signal.SIGTERM)
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
            raise RuntimeError("工作区演练服务未正常收束") from None
        finally:
            self.process = None

    def cli(self, *arguments: str) -> Any:
        result = subprocess.run(
            [
                sys.executable,
                "-I",
                "-m",
                "krim_agent",
                "workspace",
                *arguments,
                "--url",
                self.url,
            ],
            env=self.environment,
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, result.stderr or result.stdout
        return json.loads(result.stdout)

    def client(self) -> KrimClient:
        return KrimClient(self.url, "workspace-trial-key")


def confirm_restore(client: KrimClient, saved: Saved, plan_id: str) -> str:
    result = client.agent_workspace_wait_restore(saved.agent_id, plan_id)
    if result.status == "uncertain":
        for item in result.items:
            if item.operation is not None and item.operation.status == "uncertain":
                checked = client.agent_workspace_reconcile(
                    saved.agent_id, item.operation.operation_id, confirmed=True
                )
                assert checked.operation_id == item.operation.operation_id
        result = client.agent_workspace_restore(saved.agent_id, plan_id)
    assert result.status == "done", result
    for item in result.items:
        assert item.operation is not None and item.operation.status == "done"
        client.agent_workspace_finalize(saved.agent_id, item.operation.operation_id, confirmed=True)
    assert result.items[0].operation is not None
    return result.items[0].operation.operation_id


def main() -> None:
    if not sys.platform.startswith("linux"):
        raise SystemExit("此工作区写入演练需要已经验证的Linux文件事务能力")
    with TemporaryDirectory(prefix="krim-workspace-trial-") as directory:
        root = Path(directory)
        (root / "files").mkdir()
        file = root / "files/note.txt"
        file.write_text(ORIGINAL, encoding="utf-8")
        (root / "files/.env").write_text("synthetic-private-value", encoding="utf-8")
        saved = asyncio.run(sdk_prepare(root))
        server = Server(root)
        try:
            server.start()
            with server.client() as client:
                assert client.agent(saved.agent_id).session_id == saved.agent_id
                assert (
                    client.agent_workspace_restore(saved.agent_id, saved.plan_id).phase == "ready"
                )
                assert (
                    client.agent_workspace_native_batch(saved.agent_id, saved.batch_id).status
                    == "done"
                )
                current = client.agent_read_file(saved.agent_id, "note.txt")
                assert current.content == EDITED
                assert server.cli("read", saved.agent_id, "note.txt")["sha256"] == current.sha256
                try:
                    client.agent_read_file(saved.agent_id, ".env")
                except ClientError as error:
                    assert error.code == "workspace_sensitive"
                else:
                    raise AssertionError("敏感文件不应开放")
                server.cli(
                    "execute-restore",
                    saved.agent_id,
                    saved.plan_id,
                    "--revision",
                    str(saved.plan_revision),
                    "--request-key",
                    "trial-restore-execute",
                    "--confirm",
                )
                operation_id = confirm_restore(client, saved, saved.plan_id)
                assert file.read_text() == ORIGINAL and not (root / "files/missing.txt").exists()
                assert (
                    server.cli(
                        "restore-by-key", saved.agent_id, "--request-key", "trial-restore-preview"
                    )["plan_id"]
                    == saved.plan_id
                )
                client.agent_start(saved.agent_id, "/echo workspace-resumed", "trial-model-turn")
                run = client.agent_wait(saved.agent_id)
                assert run.status == "completed" and run.result is not None
                assert "workspace-resumed" in run.result.text
                tasks = client.agent_tasks_write(
                    saved.agent_id,
                    "plan",
                    TaskWriteRequest(
                        operation="complete",
                        item_id=saved.last_task_id,
                        expected_revision=2,
                        request_key="trial-last-done",
                    ),
                )
                assert tasks.revision == 3 and all(item.completed for item in tasks.items)
            server.stop()
            server.start()
            with server.client() as client:
                assert client.agent(saved.agent_id).turn == 1
                assert client.agent_tasks(saved.agent_id, "plan").revision == 3
                file.write_text(EXTERNAL, encoding="utf-8")
                repeated = client.agent_workspace_execute_restore(
                    saved.agent_id,
                    CheckpointRestoreExecute(
                        plan_id=saved.plan_id,
                        expected_revision=saved.plan_revision,
                        request_key="trial-restore-execute",
                    ),
                    confirmed=True,
                )
                assert repeated.items[0].operation is not None
                assert repeated.items[0].operation.operation_id == operation_id
                assert file.read_text() == EXTERNAL
                stale = client.agent_workspace_preview_restore(
                    saved.agent_id,
                    CheckpointRestorePreview(
                        checkpoint_id=saved.checkpoint_id,
                        request_key="trial-conflict-preview",
                    ),
                )
                stale = client.agent_workspace_wait_restore(saved.agent_id, stale.plan_id)
                assert stale.phase == "ready", stale
                file.write_text("预览后的外部版本D\n", encoding="utf-8")
                client.agent_workspace_execute_restore(
                    saved.agent_id,
                    CheckpointRestoreExecute(
                        plan_id=stale.plan_id,
                        expected_revision=stale.revision,
                        request_key="trial-conflict-execute",
                    ),
                    confirmed=True,
                )
                conflict = client.agent_workspace_wait_restore(saved.agent_id, stale.plan_id)
                assert (
                    conflict.status != "done"
                    and conflict.items[0].error_code == "workspace_conflict"
                )
                assert file.read_text() == "预览后的外部版本D\n"
                fresh = client.agent_workspace_preview_restore(
                    saved.agent_id,
                    CheckpointRestorePreview(
                        checkpoint_id=saved.checkpoint_id,
                        request_key="trial-new-preview",
                    ),
                )
                fresh = client.agent_workspace_wait_restore(saved.agent_id, fresh.plan_id)
                assert fresh.phase == "ready" and fresh.plan_id != saved.plan_id
                client.agent_workspace_execute_restore(
                    saved.agent_id,
                    CheckpointRestoreExecute(
                        plan_id=fresh.plan_id,
                        expected_revision=fresh.revision,
                        request_key="trial-new-execute",
                    ),
                    confirmed=True,
                )
                confirm_restore(client, saved, fresh.plan_id)
                assert file.read_text() == ORIGINAL
                closed = client.agent_session_close(
                    saved.agent_id, client.agent_session(saved.agent_id).revision
                )
                assert closed.status == "closed"
                assert (
                    client.agent_workspace_checkpoint(saved.agent_id, saved.checkpoint_id).status
                    == "available"
                )
                assert (
                    client.agent_workspace_restore(saved.agent_id, saved.plan_id).status == "done"
                )
                assert (
                    client.agent_workspace_operation(saved.agent_id, saved.apply_id).status
                    == "done"
                )
                assert client.agent_destroy(saved.agent_id).status == "destroyed"
            print(
                json.dumps(
                    {
                        "package": krim_agent.__file__,
                        "platform": sys.platform,
                        "sdk_read_write": True,
                        "native_keep_batch": True,
                        "sdk_to_cli_restore": True,
                        "cli_service_starts": server.starts,
                        "owner_handoffs": server.starts,
                        "service_restarts": server.starts - 1,
                        "original_operation_preserved": True,
                        "external_conflict_rejected": True,
                        "checkpoint_restored_again": True,
                        "task_revision": 3,
                        "model": "fake-offline echo; file actions use host APIs",
                    },
                    ensure_ascii=False,
                )
            )
        finally:
            server.stop()


if __name__ == "__main__":
    main()
