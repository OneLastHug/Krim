"""发行声明与冷导入；最终wheel/site身份由installed_sessions另行核对。"""

import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path

from krim_agent import __version__
from krim_agent.api.server import create_app

ROOT = Path(__file__).resolve().parents[2]


def test_release_versions_and_root_lock_metadata():
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())
    lock = tomllib.loads((ROOT / "uv.lock").read_text())
    assert project["project"]["version"] == __version__ == "0.4.0a1"
    assert create_app().version == "0.4.0a1"
    roots = [p for p in lock["package"] if p["name"] == "krim-agent"]
    assert len(roots) == 1 and roots[0]["version"] == "0.4.0a1"
    assert roots[0]["source"] == {"editable": "."}
    before_raw = subprocess.check_output(
        ["git", "--no-optional-locks", "show", "43952e1129fbf9357a550507936827900b6d836c:uv.lock"],
        text=True,
    )
    before = tomllib.loads(before_raw)
    expected = before_raw.replace(
        'name = "krim-agent"\nversion = "0.2.0a1"', 'name = "krim-agent"\nversion = "0.4.0a1"', 1
    )
    assert (ROOT / "uv.lock").read_text() == expected
    third_party = [p for p in lock["package"] if p["name"] != "krim-agent"]
    assert len(third_party) == 38
    assert third_party == [p for p in before["package"] if p["name"] != "krim-agent"]
    expected_root = next(p for p in before["package"] if p["name"] == "krim-agent") | {
        "version": "0.4.0a1"
    }
    assert roots[0] == expected_root


def test_exact_package_and_import_purity(tmp_path):
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())
    scope = json.loads(
        re.search(r"```json\n(.*?)\n```", (ROOT / "docs/agent-core/AC-M2-SCOPE.md").read_text(), re.S)[1]
    )
    declared = project["tool"]["hatch"]["build"]["targets"]["wheel"]["only-include"]
    assert len(declared) == 75 and set(declared) == set(scope["package_files"])
    assert all((ROOT / p).is_file() for p in declared)
    code = (
        "import sys;sys.path.insert(0," + repr(str(ROOT / "src")) + ");"
        "import socket,httpx;"
        "socket.socket.connect=lambda *a,**k:(_ for _ in ()).throw("
        "AssertionError('cold network'));"
        "httpx.AsyncClient.__init__=lambda *a,**k:(_ for _ in ()).throw("
        "AssertionError('cold client'));"
        "import krim_agent.context,krim_agent.sessions,krim_agent.security;"
        "from krim_agent.sdk import SessionSDK;"
        "assert not any(k.startswith(('krim_agent.protocol','krim_agent.persistence',"
        "'krim_agent.agents')) for k in sys.modules)"
    )
    result = subprocess.run(
        [sys.executable, "-I", "-B", "-c", code], cwd=tmp_path, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    assert list(tmp_path.iterdir()) == []
    files = {Path(p).name for p in scope["add"] if p.startswith("tests/agent_core/test_")}
    assert files <= set(project["tool"]["pytest"]["ini_options"]["python_files"])
    assert project["tool"]["pytest"]["ini_options"]["testpaths"] == [
        "tests/core",
        "tests/agent_core",
    ]
