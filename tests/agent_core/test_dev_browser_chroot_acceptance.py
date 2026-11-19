"""Approved-profile and PID evidence checks without a Docker/browser launch."""

import hashlib
import importlib.util
import json
import os
import sys
from pathlib import Path

from krim_agent.dev_browser.launcher import SECCOMP_SHA256

ROOT = Path(__file__).parents[2]
DEPLOY = ROOT / "deployment/development-browser"
sys.path.insert(0, str(DEPLOY))
from seccomp_proposal import proposal  # noqa: E402


def load_probe():
    spec = importlib.util.spec_from_file_location(
        "fixed_acceptance_probe", DEPLOY / "sandbox_probe.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_approved_profile_reproduces_only_the_reviewed_change():
    current = json.loads(
        (DEPLOY / "seccomp-chroot.json").read_text()
    )
    original = json.loads(json.dumps(current))
    rules = [r for r in original["syscalls"] if r.get("names") == ["chroot"]]
    assert len(rules) == 1 and rules[0]["includes"] == {}
    rules[0]["includes"] = {"caps": ["CAP_SYS_CHROOT"]}
    # Canonical serialization is the pinned original's own tab/newline format.
    raw = (json.dumps(original, indent="\t") + "\n").encode()
    approved = proposal(raw)
    assert json.loads(approved) == current
    assert hashlib.sha256(approved).hexdigest() == SECCOMP_SHA256
    assert (DEPLOY / "verify_container.py").read_text().count(
        "f922c7c9bfc1ece0b72b244e56dc85756ded002c837fb46054cfd8b275c74b6b"
    ) == 1


def test_proc_evidence_uses_reported_pid_not_executable_path():
    probe = load_probe()
    pid = os.getpid()
    result = probe.inspect_reported_processes([{"id": pid, "type": "renderer"}])
    assert result[0]["pid"] == pid and result[0]["role"] == "renderer"
    assert result[0]["status"]["Uid"].split()[0] == str(os.getuid())
    assert result[0]["no_sandbox_flag"] is False
    assert result[0]["namespaces"]["pid"] == os.readlink("/proc/self/ns/pid")
    assert probe.remaining_processes([pid]) == [pid]


def test_unreadable_pid_is_not_a_successful_renderer():
    probe = load_probe()
    missing_pid = 2**30
    result = probe.inspect_reported_processes([{"id": missing_pid, "type": "renderer"}])
    assert result[0]["status"] == {"unavailable": "proc_status_unreadable"}
    assert result[0]["no_sandbox_flag"] == "unreadable"
    assert probe.remaining_processes([missing_pid]) == []
