"""Quota commands use the actual mount while project data stays scoped."""

import json
import subprocess

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.sandbox.xfs import XfsAccounts


def test_subdirectory_storage_uses_resolved_filesystem_mount(xfs_mount_root, monkeypatch):
    storage = xfs_mount_root / "isolated"
    storage.mkdir()
    root = storage / "environments"
    root.mkdir()
    commands = []

    def run(argv, **kwargs):
        commands.append(argv)
        if argv[0] == "findmnt":
            return subprocess.CompletedProcess(argv, 0, json.dumps({
                "filesystems": [{"target": str(xfs_mount_root), "fstype": "xfs",
                                 "options": "rw,nodev,prjquota"}],
            }), "")
        assert argv[-1] == str(xfs_mount_root)
        return subprocess.CompletedProcess(argv, 0, "Accounting: ON\nEnforcement: ON\n", "")

    monkeypatch.setattr(subprocess, "run", run)
    quota = XfsAccounts(storage, root)
    assert quota.root == root and quota.mount == storage and quota.quota_mount == xfs_mount_root
    quota.current()
    assert len(commands) == 3


@pytest.mark.parametrize("case", ["not-xfs", "no-quota", "outside", "multiple", "malformed"])
def test_resolved_mount_must_cover_the_storage(xfs_mount_root, monkeypatch, case):
    storage = xfs_mount_root / "isolated"
    storage.mkdir()
    root = storage / "environments"
    root.mkdir()
    outside = xfs_mount_root / "other"
    outside.mkdir()
    entry = {"target": str(outside if case == "outside" else xfs_mount_root),
             "fstype": "ext4" if case == "not-xfs" else "xfs",
             "options": "rw" if case == "no-quota" else "rw,prjquota"}

    def run(argv, **kwargs):
        assert argv[0] == "findmnt"
        output = ("{" if case == "malformed" else json.dumps({
            "filesystems": [entry, entry] if case == "multiple" else [entry],
        }))
        return subprocess.CompletedProcess(argv, 0, output, "")

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(ServiceError, match="sandbox_backend_unavailable"):
        XfsAccounts(storage, root)
