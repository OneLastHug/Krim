"""Host-resolved Git network policies, independent of idle environment config."""

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.development.git_operations import derive_intent
from tests.coding_agent.test_development_git_operations import (
    REF,
    LocalTransport,
    native,
    request,
)
from tests.coding_agent.test_development_git_operations import fixture as fixture


async def clone_fixture(f, monkeypatch):
    remote = f.root / "network-remote.git"
    native(f.root, "init", "--bare", str(remote))
    tree = native(remote, "mktree", data=b"").decode()
    oid = native(remote, "commit-tree", tree, data=b"network fixture\n").decode()
    native(remote, "update-ref", REF, oid)
    LocalTransport.remote = remote
    LocalTransport.instances = []
    monkeypatch.setattr("krim_agent.development.git_operations.DockerGitTransport", LocalTransport)
    req = request("git.clone", object_id=oid)
    identity = await f.operation(req)
    assert f.record.network_id is None and f.record.network_policy_ref is None
    assert (await f.facade.clone(f.actor, identity, req)).object_id == oid
    persisted = await f.repositories.environments.resolve(f.record.environment)
    assert persisted == f.record
    assert f.network_resolutions
    assert all(item[1] == identity and item[2] == req for item in f.network_resolutions)
    return oid


async def test_remote_uses_host_policy_with_empty_environment_network(fixture, monkeypatch):
    await clone_fixture(fixture, monkeypatch)


@pytest.mark.parametrize("configured", [False, True])
async def test_missing_resolver_or_policy_denies_remote_but_local_works(
    fixture, monkeypatch, configured
):
    f = fixture
    oid = await clone_fixture(f, monkeypatch)
    f.network_policy_ref = None
    facade = f.make_facade(network=configured)
    before = len(f.network_resolutions)
    try:
        local = request("git.diff", object_id=oid, expected_old_id=oid)
        assert not (await facade.diff(f.actor, await f.operation(local), local)).changed
        assert len(f.network_resolutions) == before
        remote = request("git.push", object_id=oid, expected_old_id=oid)
        identity = await f.operation(remote)
        with pytest.raises(ServiceError, match="permission_denied"):
            await facade.push(f.actor, identity, remote)
        assert (await f.repositories.operations.lookup(identity)).state == "accepted"
        assert len(LocalTransport.instances) == 1
    finally:
        await facade.close()


async def test_policy_change_interrupts_remote_before_spawn(fixture, monkeypatch):
    f = fixture

    class ChangedPolicy(LocalTransport):
        async def prepare(self, repository):
            await super().prepare(repository)
            f.network_policy_ref = "changed-policy"
            await self.resolve()

        def spawn(self, *args, **kwargs):
            raise AssertionError("changed policy must not reach spawn")

    ChangedPolicy.instances = []
    monkeypatch.setattr("krim_agent.development.git_operations.DockerGitTransport", ChangedPolicy)
    req = request("git.clone", object_id="a" * 40)
    identity = await f.operation(req)
    with pytest.raises(ServiceError, match="permission_denied"):
        await f.facade.clone(f.actor, identity, req)
    assert ChangedPolicy.instances[-1].closed
    assert not f.facade._active
    assert (await f.repositories.operations.lookup(identity)).state == "running"
    assert len(f.network_resolutions) >= 2


async def test_policy_id_is_not_authorization_by_itself(fixture, monkeypatch):
    f = fixture
    f.network_policy_ref = "not-a-current-grant"

    def forbidden(*args, **kwargs):
        raise AssertionError("missing NetworkGrant must deny before transport")

    monkeypatch.setattr("krim_agent.development.git_operations.DockerGitTransport", forbidden)
    req = request("git.clone", object_id="a" * 40)
    identity = await f.operation(req)
    with pytest.raises(ServiceError, match="permission_denied"):
        await f.facade.clone(f.actor, identity, req)


async def test_remote_approval_projection_precedes_operation_admission(fixture):
    f = fixture
    req = request("git.clone", object_id="a" * 40)
    identity = await f.operation(req, admit=False)
    scope = await f.facade.scope(f.actor, identity, req)
    derived = derive_intent(req, scope, identity, "approval-before-admit")
    assert derived.authorization.network_policy_ref == "git-network"
    assert derived.authorization.arguments_digest == derived.intent.digest(derived.authorization)
    assert derived.approval_projection["operation_id"] == identity.operation_id
    assert await f.repositories.operations.lookup(identity) is None
    assert f.network_resolutions[-1] == (f.actor, identity, req)
    with pytest.raises(ServiceError, match="sandbox_operation_unknown"):
        await f.facade.clone(f.actor, identity, req, approval_id="approval-before-admit")
    assert await f.repositories.operations.lookup(identity) is None
