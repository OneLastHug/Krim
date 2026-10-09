import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要真实CLI服务与隔离执行",
)


def test_cli_service_restart_preserves_original_commands_and_file_operations():
    script = Path(__file__).resolve().parent.parent / "examples/command_management_trial.py"
    result = subprocess.run(
        [sys.executable, "-I", str(script)], capture_output=True, text=True, timeout=90
    )
    assert result.returncode == 0, result.stdout + result.stderr
    value = json.loads(result.stdout)
    assert value["real_cli_service"] and value["cli_and_client"]
    assert value["restored_original"] and value["external_files_preserved"]
