import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要真实Linux命令隔离",
)


def test_original_agent_command_survives_restart_and_resource_change_is_rejected(tmp_path):
    worker = Path(__file__).with_name("fixtures") / "agent_command_worker.py"
    results = []
    for mode in ("create", "wrong", "read"):
        result = subprocess.run(
            [sys.executable, str(worker), str(tmp_path), mode],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        results.append(json.loads(result.stdout))
    assert [item["calls"] for item in results] == [1, 0, 0]
    assert len({item["agent_id"] for item in results}) == 1
    assert len({item["command_id"] for item in results}) == 1
