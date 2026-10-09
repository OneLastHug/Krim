import json
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要真实Linux命令与文件事务",
)
WORKER = Path(__file__).with_name("fixtures") / "command_publication_worker.py"


def run(root, mode):
    result = subprocess.run(
        [sys.executable, str(WORKER), str(root), mode], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == (23 if mode.startswith("crash") else 0), (
        result.stdout + result.stderr
    )
    return json.loads(result.stdout) if result.returncode == 0 else None


def test_proposal_is_restored_and_published_then_original_keys_never_rewrite(tmp_path):
    first = run(tmp_path, "preview")
    assert first["content"] == "before" and first["calls"] == 1
    published = run(tmp_path, "apply")
    assert published["calls"] == 0 and published["content"] == "artifact"
    (tmp_path / "files/file.txt").write_text("external")
    replay = run(tmp_path, "replay")
    assert replay["calls"] == 0 and replay["content"] == "external"
    assert (
        replay["proposal_id"] == first["proposal_id"]
        and replay["operation_id"] == published["operation_id"]
    )


@pytest.mark.parametrize("mode", ["crash-before", "crash-after"])
def test_dead_publication_process_is_reconciled_without_repeating_command_or_file_action(
    tmp_path, mode
):
    run(tmp_path, mode)
    with sqlite3.connect(tmp_path / "agent.sqlite") as database:
        database.execute("UPDATE agent_handles SET lease_expires_at=0")
        database.execute("UPDATE workspace_edit_attempts SET lease_expires_at=0")
    result = run(tmp_path, "recover")
    assert result["calls"] == 0 and result["status"] == (
        "done" if mode == "crash-after" else "rejected"
    )
    assert result["content"] == ("artifact" if mode == "crash-after" else "before")
