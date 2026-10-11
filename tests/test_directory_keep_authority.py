"""保留授权等待期间父路径变化不能依据旧fd确认成功。"""

import pytest

from krim_agent.workspace import Workspace
from krim_agent.workspace_directory import DirectoryChangeSpec, WorkspaceDirectoryWriter


@pytest.mark.parametrize("operation", ["move", "delete"])
def test_keep_rechecks_parent_after_authority_wait(tmp_path, operation):
    (tmp_path / "source").mkdir()
    (tmp_path / "target").mkdir()
    (tmp_path / "source" / "empty").mkdir()
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        snapshot = writer.capture("source/empty")
        prepared = writer.prepare(
            DirectoryChangeSpec(
                path="source/empty",
                operation=operation,
                expected_fingerprint=snapshot.fingerprint,
                target_path="target/moved" if operation == "move" else None,
            )
        )
        applied = writer.apply(prepared)
        assert applied.receipt.status == "done"

        def authorize():
            (tmp_path / "source").rename(tmp_path / "source-old")
            (tmp_path / "source").mkdir()

        kept = writer.verify_keep(prepared, applied, authorize=authorize)
        assert kept.receipt.status == "rejected" and not kept.receipt.changed
        assert kept.receipt.error_code == "workspace_parent_changed"
