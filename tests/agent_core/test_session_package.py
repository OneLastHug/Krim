"""发行声明与冷导入；最终wheel/site身份由installed_sessions另行核对。"""

import fnmatch
import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path

from krim_agent import __version__
from krim_agent.api.server import create_app
from scripts.verify_distribution import distribution_contract

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
    scope_text = (ROOT / "docs/agent-core/AC-M2-SCOPE.md").read_text()
    scope = json.loads(re.search(r"```json\n(.*?)\n```", scope_text, re.S)[1])
    declared = project["tool"]["hatch"]["build"]["targets"]["wheel"]["only-include"]
    font_additions = {
        "src/krim_agent/font_assets.py",
        "src/krim_agent/web/font-manifest.json",
        "src/krim_agent/web/assets/fonts",
    }
    assert len(declared) == len(set(declared))
    assert set(scope["package_files"]) <= set(declared)
    assert font_additions <= set(declared)
    assert set(distribution_contract(ROOT)["wheel_include_paths"]) == set(declared)
    for name in declared:
        path = ROOT / name
        if name == "src/krim_agent/web/font-manifest.json":
            assert not path.exists() or path.is_file()
        elif name == "src/krim_agent/web/assets/fonts":
            assert not path.exists() or path.is_dir()
        else:
            assert path.is_file()
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
    python_files = project["tool"]["pytest"]["ini_options"].get("python_files", ["test_*.py"])
    assert all(
        any(fnmatch.fnmatchcase(name, pattern) for pattern in python_files) for name in files
    )
    assert {
        "tests/core",
        "tests/agent_core",
    } <= set(project["tool"]["pytest"]["ini_options"]["testpaths"])
