"""Repository grant and independent network mapping commit together."""

import time

import pytest
from sqlalchemy import select, update

from krim_agent.accounts.service import IdentityService
from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.project_access.git import GitGrant, GitIdentity
from krim_agent.project_access.types import NetworkGrant
from krim_agent.storage.development_git import git_network_policy, put_git_grant
from krim_agent.storage.development_models import project_access_records
from krim_agent.storage.project_access import SQLiteAccessRepository
from tests.coding_agent.test_development_storage import prepare


async def test_git_grant_and_network_mapping_share_cas_without_environment_network(tmp_path):
    store, _, environment, request = await prepare(tmp_path / "grants.sqlite")
    identities = await IdentityService.open(store)
    access = SQLiteAccessRepository(store)
    actor = identities.resolve(request.authorization.actor_id)
    project_id = environment.environment.project_id
    grant = GitGrant(owner_id=actor.principal_id, project_id=project_id,
                     repository="https://git.example/team/project.git", refs=("refs/heads/qa",),
                     actions=("git.clone", "git.push"),
                     identity=GitIdentity(name="Fixture", email="fixture@example.invalid"),
                     expires_at=time.time() + 300)
    try:
        assert environment.network_policy_ref is None
        await access.put(NetworkGrant(owner_id=actor.principal_id, project_id=project_id,
                                      policy_id="git-policy", revision=1, hosts=("git.example",),
                                      expires_at=time.time() + 300), expected_revision=0)
        with pytest.raises(ServiceError, match="permission_denied"):
            await put_git_grant(store, actor, grant, None, expected_revision=0)
        assert await put_git_grant(store, actor, grant, "git-policy", expected_revision=0) == 1
        assert await git_network_policy(access, actor, project_id, grant.repository) == "git-policy"
        with pytest.raises(ServiceError, match="revision_conflict"):
            await put_git_grant(store, actor, grant, "git-policy", expected_revision=0)
        await put_git_grant(store, actor, grant.model_copy(update={"revoked": True}),
                            "git-policy", expected_revision=1)
        with pytest.raises(ServiceError, match="permission_denied"):
            await git_network_policy(access, actor, project_id, grant.repository)
        async with store.engine.connect() as conn:
            rows = (await conn.execute(select(project_access_records).where(
                project_access_records.c.resource_id == fingerprint(grant.repository),
            ))).mappings().all()
        assert len(rows) == 2 and all(row["revision"] == 2 for row in rows)
    finally:
        await identities.close()
        await store.close()


async def test_git_mapping_drift_and_wrong_network_are_rejected(tmp_path):
    store, _, environment, request = await prepare(tmp_path / "grants.sqlite")
    identities = await IdentityService.open(store)
    access = SQLiteAccessRepository(store)
    actor = identities.resolve(request.authorization.actor_id)
    project_id = environment.environment.project_id
    grant = GitGrant(owner_id=actor.principal_id, project_id=project_id,
                     repository="https://git.example/team/project.git", refs=("refs/heads/qa",),
                     actions=("git.clone",),
                     identity=GitIdentity(name="Fixture", email="fixture@example.invalid"),
                     expires_at=time.time() + 300)
    try:
        await access.put(NetworkGrant(owner_id=actor.principal_id, project_id=project_id,
                                      policy_id="wrong", revision=1, hosts=("other.example",),
                                      expires_at=time.time() + 300), expected_revision=0)
        with pytest.raises(ServiceError, match="permission_denied"):
            await put_git_grant(store, actor, grant, "wrong", expected_revision=0)
        local = grant.model_copy(update={"actions": ("git.diff", "git.commit")})
        await put_git_grant(store, actor, local, None, expected_revision=0)
        assert await git_network_policy(access, actor, project_id, grant.repository) is None
        async with store.engine.begin() as conn:
            await conn.execute(update(project_access_records).where(
                project_access_records.c.project_id == project_id,
                project_access_records.c.kind == "git_network",
            ).values(revision=2))
        with pytest.raises(ServiceError, match="permission_denied"):
            await git_network_policy(access, actor, project_id, grant.repository)
    finally:
        await identities.close()
        await store.close()
