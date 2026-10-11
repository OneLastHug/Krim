"""目录来源重开仅查询原事实，进程模式由实际临时数据库承接。"""

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要真实Linux目录来源进程",
)


def test_directory_source_survives_independent_process_without_runner_or_replay(tmp_path):
    worker = Path(__file__).with_name("fixtures") / "command_directory_worker.py"
    results = []
    for mode in ("write", "read", "read"):
        result = subprocess.run(
            [sys.executable, str(worker), str(tmp_path), mode],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        results.append(json.loads(result.stdout)["catalog"])
    assert results[0] == results[1] == results[2]
    catalog = results[0]
    assert catalog["recorded"] and catalog["complete"]
    entries = {entry["path"]: entry["mode"] for entry in catalog["entries"]}
    assert set(entries) == {
        "empty",
        "nested",
        "nested/empty",
        "input",
        "explicit",
        "explicit/empty",
    }
    assert entries["empty"] == 0o750
