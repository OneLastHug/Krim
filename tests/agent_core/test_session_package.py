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
        ["git", "--no-optional-locks", "show", "90c1ed69167f8b1b35ebd84e80e010ce05c75304:uv.lock"],
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


