"""真实flock与持久租约：过期不能证明旧写者已停止。"""

import os
import time
import uuid

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.sandbox.leases import ProjectLeases


def test_same_project_exclusive_and_different_project_parallel(tmp_path):
    leases = ProjectLeases(tmp_path)
    a, b = str(uuid.uuid4()), str(uuid.uuid4())
    first = leases.claim(a, str(uuid.uuid4()), deadline=time.time() + 60)
    second = leases.claim(b, str(uuid.uuid4()), deadline=time.time() + 60)
    try:
        with pytest.raises(ServiceError, match="workspace_busy"):
            leases.claim(a, str(uuid.uuid4()), deadline=time.time() + 60)
        first.check()
        first.release(clean=True)
        third = leases.claim(a, str(uuid.uuid4()), deadline=time.time() + 60)
        third.release(clean=True)
        second.release(clean=True)
    finally:
        leases.close()


def test_expiry_never_releases_old_writer(tmp_path, monkeypatch):
    leases = ProjectLeases(tmp_path)
    project, lease_id = str(uuid.uuid4()), str(uuid.uuid4())
    now = time.time()
    first = leases.claim(project, lease_id, deadline=now + 10)
    monkeypatch.setattr(time, "time", lambda: now + 11)
    try:
        with pytest.raises(ServiceError, match="sandbox_limit"):
            first.check()
        with pytest.raises(ServiceError, match="workspace_busy"):
            leases.claim(project, str(uuid.uuid4()), deadline=now + 60)
        with pytest.raises(ServiceError, match="workspace_busy"):
            leases.reconcile(project, lease_id, stopped=True)
        first.release(clean=False)
        assert leases.status(project)["status"] == "blocked"
        with pytest.raises(ServiceError, match="sandbox_cleanup_required"):
            leases.claim(project, str(uuid.uuid4()), deadline=now + 60)
        with pytest.raises(ServiceError, match="sandbox_cleanup_required"):
            leases.reconcile(project, lease_id, stopped=False)
        leases.reconcile(project, lease_id, stopped=True)
        assert leases.status(project)["status"] == "idle"
    finally:
        leases.close()


def test_close_preserves_blocked_record_across_restart(tmp_path):
    project, lease_id = str(uuid.uuid4()), str(uuid.uuid4())
    first = ProjectLeases(tmp_path)
    first.claim(project, lease_id, deadline=time.time() + 60)
    first.close()
    second = ProjectLeases(tmp_path)
    try:
        assert second.status(project)["status"] == "blocked"
        with pytest.raises(ServiceError, match="sandbox_cleanup_required"):
            second.claim(project, str(uuid.uuid4()), deadline=time.time() + 60)
        second.reconcile(project, lease_id, stopped=True)
        clean = second.claim(project, str(uuid.uuid4()), deadline=time.time() + 60)
        clean.release(clean=True)
    finally:
        second.close()


@pytest.mark.parametrize("deadline", [True, float("nan"), float("inf"), -1, 0, "later"])
def test_invalid_deadline_rejected(tmp_path, deadline):
    leases = ProjectLeases(tmp_path)
    try:
        with pytest.raises(ServiceError):
            leases.claim(str(uuid.uuid4()), str(uuid.uuid4()), deadline=deadline)
    finally:
        leases.close()


def test_linked_state_and_lock_files_are_rejected(tmp_path):
    project = str(uuid.uuid4())
    outside = tmp_path / "outside"
    outside.write_text("unchanged")
    (tmp_path / (project + ".lock")).symlink_to(outside)
    leases = ProjectLeases(tmp_path)
    try:
        with pytest.raises(ServiceError, match="facts_corrupt"):
            leases.claim(project, str(uuid.uuid4()), deadline=time.time() + 60)
        (tmp_path / (project + ".lock")).unlink()
        (tmp_path / (project + ".json")).symlink_to(outside)
        with pytest.raises(ServiceError, match="facts_corrupt"):
            leases.claim(project, str(uuid.uuid4()), deadline=time.time() + 60)
        assert outside.read_text() == "unchanged"
    finally:
        leases.close()


def test_lease_root_replacement_and_close(tmp_path):
    root = tmp_path / "leases"
    root.mkdir()
    leases = ProjectLeases(root)
    root.rename(tmp_path / "old")
    root.mkdir()
    with pytest.raises(ServiceError, match="artifact_invalid"):
        leases.status(str(uuid.uuid4()))
    leases.close()
    with pytest.raises(ServiceError, match="closed"):
        leases.status(str(uuid.uuid4()))


def test_independent_manager_observes_flock(tmp_path):
    project = str(uuid.uuid4())
    one, two = ProjectLeases(tmp_path), ProjectLeases(tmp_path)
    lease = one.claim(project, str(uuid.uuid4()), deadline=time.time() + 60)
    try:
        with pytest.raises(ServiceError, match="workspace_busy"):
            two.claim(project, str(uuid.uuid4()), deadline=time.time() + 60)
        assert os.stat(tmp_path / (project + ".lock")).st_nlink == 1
    finally:
        lease.release(clean=True)
        one.close()
        two.close()


def test_crashed_process_active_record_blocks_new_writer(tmp_path):
    import subprocess
    import sys

    project, lease_id = str(uuid.uuid4()), str(uuid.uuid4())
    code = (
        "import os,time;from pathlib import Path;"
        "from krim_agent.sandbox.leases import ProjectLeases;"
        f"m=ProjectLeases(Path({str(tmp_path)!r}));"
        f"m.claim({project!r},{lease_id!r},deadline=time.time()+60);os._exit(0)"
    )
    subprocess.run([sys.executable, "-c", code], check=True, timeout=10)
    manager = ProjectLeases(tmp_path)
    try:
        assert manager.status(project)["status"] == "active"
        with pytest.raises(ServiceError, match="sandbox_cleanup_required"):
            manager.claim(project, str(uuid.uuid4()), deadline=time.time() + 60)
        manager.reconcile(project, lease_id, stopped=True)
    finally:
        manager.close()


def test_failed_close_keeps_stale_intent_but_releases_flock(tmp_path, monkeypatch):
    project, lease_id = str(uuid.uuid4()), str(uuid.uuid4())
    first = ProjectLeases(tmp_path)
    first.claim(project, lease_id, deadline=time.time() + 60)

    def unavailable(*args, **kwargs):
        raise OSError("control metadata full")

    monkeypatch.setattr(first, "_save", unavailable)
    with pytest.raises(OSError):
        first.close()
    second = ProjectLeases(tmp_path)
    try:
        with pytest.raises(ServiceError, match="sandbox_cleanup_required"):
            second.claim(project, str(uuid.uuid4()), deadline=time.time() + 60)
        second.reconcile(project, lease_id, stopped=True)
    finally:
        second.close()
