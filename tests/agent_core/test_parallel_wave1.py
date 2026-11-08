"""首轮主会话共享接线验证；不与各实现包写同一测试文件。"""

import hashlib
import uuid

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.sandbox.filesystem import ProjectFiles


@pytest.fixture
def project(tmp_path):
    root = tmp_path / "accounts"
    root.mkdir()
    files = ProjectFiles(root)
    env, selected = str(uuid.uuid4()), str(uuid.uuid4())
    files.create(env, selected)
    content = root / env / "projects" / selected / "content"
    try:
        yield files, env, selected, content
    finally:
        files.close()


def test_atomic_rename_moves_one_inode_and_checks_destination(project):
    files, env, selected, content = project
    digest = files.write(env, selected, "source", b"kept", None)["sha256"]
    (content / "folder").mkdir()
    inode = (content / "source").stat().st_ino
    result = files.rename(env, selected, "source", "folder/target", digest)
    assert result["atomic"] and (content / "folder/target").stat().st_ino == inode
    assert not (content / "source").exists()
    files.write(env, selected, "existing", b"other", None)
    with pytest.raises(ServiceError, match="artifact_conflict"):
        files.rename(env, selected, "folder/target", "existing", digest)
    assert (content / "existing").read_bytes() == b"other"
    expected = hashlib.sha256(b"other").hexdigest()
    files.rename(env, selected, "folder/target", "existing", digest, expected)
    assert (content / "existing").read_bytes() == b"kept"


def test_rename_rejects_linked_target_and_paths_without_changing_outside(project, tmp_path):
    files, env, selected, content = project
    digest = files.write(env, selected, "source", b"kept", None)["sha256"]
    outside = tmp_path / "outside"
    outside.write_bytes(b"outside")
    (content / "linked").symlink_to(outside)
    for destination in ["../outside", ".git/HEAD", "linked"]:
        with pytest.raises(ServiceError):
            files.rename(env, selected, "source", destination, digest)
    assert (content / "source").read_bytes() == b"kept"
    assert outside.read_bytes() == b"outside"
    with pytest.raises(ServiceError, match="artifact_conflict"):
        files.rename(env, selected, "source", "new", "0" * 64)


def test_shared_mutation_adapter_requires_the_accepted_lease_and_generation(project, tmp_path):
    import time
    import uuid

    from krim_agent.sandbox.leases import ProjectLeases
    from krim_agent.sandbox.port_adapters import AcceptedProjectMutation
    from krim_agent.sandbox.ports import ProjectRef

    files, env, selected, content = project
    locks = tmp_path / "leases"
    locks.mkdir()
    leases = ProjectLeases(locks)
    operation = str(uuid.uuid4())
    lease = leases.claim(selected, operation, deadline=time.time() + 60)
    ref = ProjectRef("account-a", env, selected, 4)
    current = {"generation": 4, "status": "ready"}

    class Broker:
        def _project(self, environment, project_id):
            assert environment == env and project_id == selected
            return current

    broker = Broker()
    broker.files = files
    try:
        port = AcceptedProjectMutation(broker, ref, lease)
        written = port.write(ref, operation, "a.txt", b"approved", None)
        assert port.read(ref, "a.txt") == b"approved"
        with pytest.raises(ServiceError, match="permission_denied"):
            port.write(ref, str(uuid.uuid4()), "a.txt", b"wrong", written["sha256"])
        with pytest.raises(ServiceError, match="permission_denied"):
            port.write(ref, None, "a.txt", b"unbound", written["sha256"])
        with pytest.raises(ServiceError, match="permission_denied"):
            port.rename(ref, None, "a.txt", "unbound.txt", written["sha256"], None)
        renamed = port.rename(ref, operation, "a.txt", "b.txt", written["sha256"], None)
        assert renamed["atomic"] and port.read(ref, "b.txt") == b"approved"
        current["generation"] = 5
        with pytest.raises(ServiceError, match="input_revision_conflict"):
            port.read(ref, "b.txt")
        assert (content / "b.txt").read_bytes() == b"approved"
    finally:
        lease.release(clean=True)
        leases.close()
