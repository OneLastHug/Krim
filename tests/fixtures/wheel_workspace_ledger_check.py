"""项目外wheel：提案/完整私有快照、CAS事实与文件回执重启后读取。"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy import text

import krim_agent
from krim_agent.persistence import Database, KrimRepository, WorkspaceEditStore
from krim_agent.workspace import Workspace
from krim_agent.workspace_change import FileChangeSpec, WorkspaceChangeAction
from krim_agent.workspace_writer import WorkspaceWriter


async def main():
    assert "/site-packages/" in krim_agent.__file__
    with TemporaryDirectory(prefix="krim-ledger-wheel-") as directory:
        base = Path(directory)
        root = base / "files"
        root.mkdir()
        (root / "file.txt").write_text("old\n", encoding="utf-8")
        database = Database(f"sqlite+aiosqlite:///{base / 'ledger.sqlite'}")
        await database.initialize()
        with Workspace(root, writable=True) as workspace:
            topic = await KrimRepository(database).create_topic("安装包文件任务")
            scope = "topic:" + topic.id
            store = WorkspaceEditStore(database)
            writer = WorkspaceWriter(workspace)
            prepared = writer.prepare(
                FileChangeSpec(
                    path="file.txt",
                    operation="write",
                    expected_sha256=workspace.read("file.txt").sha256,
                    content="new\n",
                )
            )
            proposal, _ = await store.preview(
                scope, "files", workspace.fingerprint, "preview", prepared
            )
            request = WorkspaceChangeAction(
                proposal_id=proposal.view.proposal_id,
                action="apply",
                expected_revision=0,
                request_key="apply",
            )
            attempt, _ = await store.reserve(scope, request, workspace.fingerprint, prepared)
            effect = writer.apply(prepared)
            await store.settle(scope, attempt.view.operation_id, effect.receipt, effect.recovery)
            await database.close()
            cold_db = Database(f"sqlite+aiosqlite:///{base / 'ledger.sqlite'}")
            await cold_db.initialize()
            cold = WorkspaceEditStore(cold_db)
            try:
                recovered = await cold.get(scope, proposal.view.proposal_id)
                assert (
                    recovered.prepared.before.data == b"old\n"
                    and recovered.prepared.after == b"new\n"
                )
                assert recovered.view.status == "applied"
                replay = await cold.find_action(scope, request, workspace.fingerprint)
                assert (
                    replay.view.status == "done"
                    and replay.view.operation_id == attempt.view.operation_id
                )
                assert (root / "file.txt").read_text() == "new\n"
                writer.finalize(prepared, replay.recovery)
                assert not (
                    await cold.mark_cleaned(scope, replay.view.operation_id)
                ).view.cleanup_pending
                async with cold_db.engine.connect() as connection:
                    head = (
                        await connection.execute(text("SELECT version_num FROM alembic_version"))
                    ).scalar()
                    assert head == "0039_mixed_workspace_batches"
                print(
                    json.dumps(
                        {
                            "package": krim_agent.__file__,
                            "migration": head,
                            "private_snapshot_restored": True,
                            "same_operation": True,
                            "file_sha256": workspace.read("file.txt").sha256,
                        }
                    )
                )
            finally:
                await cold_db.close()


asyncio.run(main())
