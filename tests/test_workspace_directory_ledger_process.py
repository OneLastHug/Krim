"""目录账本真实生产者退出后按原意图核实，不能重新执行目录动作。"""

import json
import sqlite3
import subprocess
import sys
from pathlib import Path


def test_directory_ledger_process_crash_restores_original_operation_without_replay(tmp_path):
    worker = Path(__file__).with_name("fixtures") / "directory_ledger_worker.py"
    written = subprocess.run(
        [sys.executable, str(worker), str(tmp_path), "write"],
        capture_output=True,
        text=True,
        timeout=25,
    )
    assert written.returncode == 23, written.stdout + written.stderr
    with sqlite3.connect(tmp_path / "state.sqlite") as connection:
        connection.execute("UPDATE workspace_directory_attempts SET lease_expires_at=0")
    reader = subprocess.run(
        [sys.executable, str(worker), str(tmp_path), "read"],
        capture_output=True,
        text=True,
        timeout=25,
    )
    assert reader.returncode == 0, reader.stdout + reader.stderr
    result = json.loads(reader.stdout)
    assert result["status"] == "done" and result["mode"] == 0o750
    with sqlite3.connect(tmp_path / "state.sqlite") as connection:
        assert (
            connection.execute("SELECT count(*) FROM workspace_directory_attempts").fetchone()[0]
            == 1
        )
