"""Review-only proposal changes exactly one rule; this never selects a profile."""

import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

PATH = Path(__file__).parents[2] / "deployment/development-browser/seccomp_proposal.py"
spec = importlib.util.spec_from_file_location("seccomp_proposal", PATH)
proposal_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(proposal_module)


def test_proposal_changes_only_chroot_capability_condition(monkeypatch):
    original = {
        "defaultAction": "SCMP_ACT_ERRNO",
        "syscalls": [
            {"names": ["read", "write"], "action": "SCMP_ACT_ALLOW", "includes": {}},
            {
                "names": ["chroot"],
                "action": "SCMP_ACT_ALLOW",
                "args": [],
                "includes": {"caps": ["CAP_SYS_CHROOT"]},
                "excludes": {},
            },
        ],
    }
    raw = json.dumps(original).encode()
    monkeypatch.setattr(proposal_module, "EXPECTED", hashlib.sha256(raw).hexdigest())
    proposed = json.loads(proposal_module.proposal(raw))
    assert proposed["syscalls"][1]["includes"] == {}
    proposed["syscalls"][1]["includes"] = {"caps": ["CAP_SYS_CHROOT"]}
    assert proposed == original


def test_mismatched_approved_profile_is_rejected():
    with pytest.raises(ValueError, match="original_profile_mismatch"):
        proposal_module.proposal(b'{"syscalls":[]}')
