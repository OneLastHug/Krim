"""Real SQLite download authorization; no Docker or external network in this suite."""

import asyncio
import time
from dataclasses import replace

import pytest
from pydantic import ValidationError
from sqlalchemy import update

from krim_agent.accounts.service import IdentityService
from krim_agent.contracts import ServiceError
from krim_agent.dev_environment.model import RuntimeSpec
from krim_agent.development.configuration import DevelopmentConfiguration
from krim_agent.development.ports import command_digest
from krim_agent.development.service import DevelopmentService, ShellInput
from krim_agent.project_access.service import command_intent
from krim_agent.project_access.types import ProjectGrant
from krim_agent.sandbox.configuration import SandboxConfiguration
from krim_agent.sandbox.service import SandboxService
from krim_agent.storage.models import runs
from krim_agent.storage.sandbox_models import sandbox_projects
from tests.coding_agent.test_development_storage import prepare


@pytest.fixture
async def prepared(tmp_path):
    store, repositories, environment, request = await prepare(tmp_path / "network.sqlite")
    identities = await IdentityService.open(store)
    actor = identities.resolve(request.authorization.actor_id)
    sandbox = SandboxService(store, identities, SandboxConfiguration())
    async with store.engine.begin() as conn:
        await conn.execute(update(sandbox_projects).where(
            sandbox_projects.c.project_id == environment.environment.project_id,
        ).values(status="ready"))
    configuration = DevelopmentConfiguration(
        image_id=environment.image_id, broker_environments_root=tmp_path / "environments",
        private_state_root=tmp_path / "private",
    )
    DevelopmentService.prepare_private(configuration)
    service = DevelopmentService(store, identities, sandbox, configuration)
    project = await sandbox.project(actor, environment.environment.project_id)
    grant = ProjectGrant(
        owner_id=actor.principal_id, workspace_id=project["workspace_id"],
        project_id=project["project_id"], actor_id=actor.principal_id,
        actor_generation=actor.generation, revision=1, actions=("shell.execute",),
        requires_approval=False, expires_at=time.time() + 600,
    )
    await service.access_repository.put(grant, expected_revision=0)
    policy = await service.configure_download_policy(
        actor, project["project_id"], "packages", expected_revision=0,
        hosts=("registry.npmjs.org",), expires_at=time.time() + 600,
    )
    request = request.model_copy(update={
        "authorization": request.authorization.model_copy(
            update={"network_policy_ref": "packages"},
        ),
    })
    intent = command_intent(request, workspace_id=project["workspace_id"])
    request = request.model_copy(update={
        "authorization": request.authorization.model_copy(
            update={"arguments_digest": intent.digest(request.authorization)},
        ),
    })
    reservation = await repositories.processes.reserve(request, command_digest(request))
    spec = RuntimeSpec(environment, request, reservation.record.handle.process_id)
    try:
        yield service, actor, spec, policy, grant
    finally:
        await service.close()
        await sandbox.close()
        await identities.close()
        await store.close()


async def test_download_resolves_persisted_command_and_current_policy(prepared):
    service, actor, spec, policy, _ = prepared
    assert spec.record.network_policy_ref is None
    assert spec.network.policy_ref == "packages"
    resolved = await service.resolve_download_policy(spec)
    assert resolved.grant == policy
    forged = replace(spec, request=spec.request.model_copy(update={"command": "another command"}))
    with pytest.raises(ServiceError, match="tool_binding_changed"):
        await service.resolve_download_policy(forged)
    with pytest.raises(ServiceError, match="permission_denied"):
        await service.resolve_download_policy(RuntimeSpec(spec.record))
    revoked = await service.configure_download_policy(
        actor, policy.project_id, policy.policy_id, expected_revision=1,
        hosts=policy.hosts, expires_at=policy.expires_at, revoked=True,
    )
    assert revoked.revision == 2 and revoked.revoked
    with pytest.raises(ServiceError, match="permission_denied"):
        await service.resolve_download_policy(spec)


async def test_download_stops_on_run_cancellation_and_unknown_process(prepared):
    service, _, spec, _, _ = prepared
    row = await service.repositories.processes.get(spec.process_id)
    await service.repositories.processes.save(row, replace(
        row, revision=row.revision + 1, handle=row.handle.model_copy(update={"state": "unknown"}),
    ))
    with pytest.raises(ServiceError, match="tool_binding_changed"):
        await service.resolve_download_policy(spec)


async def test_run_stop_flag_prevents_further_downloads(prepared):
    service, _, spec, _, _ = prepared
    async with service.store.engine.begin() as conn:
        await conn.execute(update(runs).where(
            runs.c.id == spec.request.ownership.run_id,
        ).values(stop_requested=True))
    with pytest.raises(ServiceError, match="cancelled"):
        await service.resolve_download_policy(spec)


async def test_policy_update_waits_for_invalidation_despite_cancellation(prepared, monkeypatch):
    service, actor, spec, policy, _ = prepared
    entered, finish = asyncio.Event(), asyncio.Event()

    async def invalidate(owner, project, *, policy_id=None):
        assert (owner, project, policy_id) == (policy.owner_id, policy.project_id, policy.policy_id)
        entered.set()
        await finish.wait()

    monkeypatch.setattr(service.downloads, "invalidate", invalidate)
    task = asyncio.create_task(service.configure_download_policy(
        actor, policy.project_id, policy.policy_id, expected_revision=1,
        hosts=policy.hosts, expires_at=policy.expires_at, revoked=True,
    ))
    await asyncio.wait_for(entered.wait(), 3)
    task.cancel()
    with pytest.raises(ServiceError, match="permission_denied"):
        await service.resolve_download_policy(spec)
    assert (policy.owner_id, policy.project_id) in service._network_updates
    finish.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not service._network_updates
    with pytest.raises(ServiceError, match="permission_denied"):
        await service.resolve_download_policy(spec)


async def test_owner_revocation_cleans_other_projects_when_one_stop_fails(prepared, monkeypatch):
    service, actor, spec, _, _ = prepared
    second = await service.sandbox.create_project(actor, {
        "workspace_id": actor.memberships[0].workspace_id, "title": "second project",
    }, request_key="second-project")
    environment = spec.record.environment.model_copy(update={
        "environment_id": "second-environment", "project_id": second["project_id"],
    })
    await service.repositories.environments.register(replace(
        spec.record, environment=environment, revision=1,
    ))
    stopped, invalidated = [], []

    async def invalidate(owner, project, *, policy_id=None):
        assert owner == actor.principal_id
        invalidated.append(project)

    async def stop(handle):
        stopped.append(handle.project_id)
        if handle.project_id == spec.handle.project_id:
            raise ServiceError("sandbox_backend_unavailable")
        return handle.model_copy(update={"state": "stopped"})

    monkeypatch.setattr(service.downloads, "invalidate", invalidate)
    monkeypatch.setattr(service.environments, "stop", stop)
    with pytest.raises(ServiceError, match="sandbox_backend_unavailable"):
        await service.revoke_owner(actor.principal_id)
    expected = {spec.handle.project_id, second["project_id"]}
    assert set(stopped) == expected and set(invalidated) == expected


def test_download_cannot_silently_extend_to_persistent_service():
    with pytest.raises(ValidationError):
        ShellInput(command="npm install", lifecycle="project_service", download_policy="packages")
    assert ShellInput(command="npm install", download_policy="packages").lifecycle == "run"
