"""持久保存原意图后强退，核实只读取原目录事实，不重放系统调用。"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux真实目录事务")


@pytest.mark.parametrize("action", ["create", "delete"])
@pytest.mark.parametrize("phase", ["before", "after"])
def test_real_directory_process_crash_verifies_original_effect_without_replay(
    tmp_path, action, phase
):
    worker = Path(__file__).with_name("fixtures") / "directory_writer_worker.py"
    crashed = subprocess.run(
        [sys.executable, str(worker), str(tmp_path), phase, action],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert crashed.returncode == 23, crashed.stdout + crashed.stderr
    resumed = subprocess.run(
        [sys.executable, str(worker), str(tmp_path), "verify", action],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert resumed.returncode == 0, resumed.stdout + resumed.stderr
    value = json.loads(resumed.stdout)
    assert value["status"] == ("done" if phase == "after" else "rejected")
    assert value["target_exists"] == (phase == "after" if action == "create" else phase != "after")
    assert not value["stage_exists"]
