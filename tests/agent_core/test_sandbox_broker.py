"""Broker协议与持久状态的合成测试；XFS执行由独立真实实验覆盖。"""

import time
import uuid

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.sandbox import broker as module
from krim_agent.sandbox.broker import StorageBroker
from krim_agent.sandbox.quota import QuotaLimits


class FakeQuota:
    def __init__(self, mount, root):
        self.rows = {}

    def current(self):
        pass

    def project_number(self, environment_id):
        return int(environment_id.replace("-", "")[:7], 16)

    def provision(self, environment_id, limits):
        number = self.project_number(environment_id)
        self.rows[number] = {
            "environment_bytes": 0,
            "environment_inodes": 4,
            "byte_limit": limits.environment_bytes,
            "inode_limit": limits.environment_inodes,
        }
        return {
            "project_number": number,
            "limits": limits.model_dump(mode="json"),
            "usage": self.rows[number],
            "enforcement": "xfs_project_quota",
        }

    def account_usage(self, record):
        return self.usage(record["project_number"])

    def usage(self, number):
        return self.rows[number]


@pytest.fixture
def storage(tmp_path, monkeypatch):
    mount, state = tmp_path / "disk", tmp_path / "state"
    mount.mkdir(mode=0o700)
    state.mkdir(mode=0o700)
    monkeypatch.setattr(module, "XfsAccounts", FakeQuota)
    monkeypatch.setattr(module.os, "chown", lambda *a, **k: None)
    value = StorageBroker(mount, state)
    try:
        yield value
    finally:
        value.close()


def test_provision_does_not_accept_host_paths_and_projects_are_bound(storage):
    env, project, other = (str(uuid.uuid4()) for _ in range(3))
    limits = QuotaLimits().model_dump(mode="json")
    result = storage.dispatch("provision_environment", {"environment_id": env, "limits": limits})
    assert result["status"] == "ready"
    storage.dispatch("provision_project", {"environment_id": env, "project_id": project})
    storage.dispatch("provision_environment", {"environment_id": other, "limits": limits})
    with pytest.raises(ServiceError, match="not_found"):
        storage.dispatch(
            "read",
            {
                "environment_id": other,
                "project_id": project,
                "path": "x",
                "offset": 0,
                "limit": 1024,
            },
        )
    with pytest.raises(ServiceError, match="invalid_input"):
        storage.dispatch(
            "provision_project", {"environment_id": env, "project_id": project, "host_path": "/etc"}
        )
    with pytest.raises(ServiceError, match="invalid_input"):
        storage.dispatch("exec", {"command": "anything"})
    assert (
        storage.dispatch(
            "list", {"environment_id": env, "project_id": project, "path": ".", "limit": 10}
        )["entries"]
        == []
    )


def test_same_project_cannot_be_double_claimed(storage):
    env, project, lease = (str(uuid.uuid4()) for _ in range(3))
    storage.provision_environment(env, QuotaLimits().model_dump(mode="json"))
    storage.provision_project(env, project)
    args = {
        "environment_id": env,
        "project_id": project,
        "lease_id": lease,
        "deadline": time.time() + 60,
    }
    storage.dispatch("claim", args)
    with pytest.raises(ServiceError, match="workspace_busy"):
        storage.dispatch("claim", {**args, "lease_id": str(uuid.uuid4())})
    with pytest.raises(ServiceError, match="sandbox_cleanup_required"):
        storage.dispatch(
            "release",
            {
                "environment_id": env,
                "project_id": project,
                "lease_id": str(uuid.uuid4()),
                "clean": True,
            },
        )
    storage.dispatch(
        "release", {"environment_id": env, "project_id": project, "lease_id": lease, "clean": True}
    )
    assert storage.leases.status(project)["status"] == "idle"


def test_broker_owner_lock_and_idempotent_quota_provision(storage):
    with pytest.raises(ServiceError, match="sandbox_cleanup_required"):
        StorageBroker(storage.storage, storage.state)
    env = str(uuid.uuid4())
    limits = QuotaLimits().model_dump(mode="json")
    storage.provision_environment(env, limits)
    assert storage.provision_environment(env, limits)["status"] == "ready"
    with pytest.raises(ServiceError, match="revision_conflict"):
        storage.provision_environment(env, {**limits, "environment_bytes": 6 * 1024**3})
    storage.close()
    with pytest.raises(ServiceError, match="closed"):
        storage.dispatch("health", {})


def test_storage_cannot_have_two_brokers_using_different_state_roots(storage, tmp_path):
    other = tmp_path / "other-state"
    other.mkdir(mode=0o700)
    with pytest.raises(ServiceError, match="sandbox_cleanup_required"):
        StorageBroker(storage.storage, other)


def test_reprovision_does_not_reset_project_generation_or_blocked_state(storage):
    env, project = str(uuid.uuid4()), str(uuid.uuid4())
    storage.provision_environment(env, QuotaLimits().model_dump(mode="json"))
    storage.provision_project(env, project)
    original = storage._project(env, project)
    original.update(generation=7, status="blocked")
    storage._save("project-" + project + ".json", original)
    prepared = storage.provision_project(env, project)
    assert prepared["generation"] == 7 and prepared["status"] == "blocked"
