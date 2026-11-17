"""Project Secret management and recovery through the shared development service."""

import asyncio
import json
import time
from types import SimpleNamespace

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy import select

from krim_agent.api.development import ProjectSecretBody
from krim_agent.api.server import create_app
from krim_agent.config import Settings
from krim_agent.development.configuration import DevelopmentConfiguration
from krim_agent.development.service import DevelopmentService, ShellInput
from krim_agent.storage.development_models import project_access_records
from tests.coding_agent.test_development_git_operations import Fixture


@pytest.fixture
async def wired(tmp_path, credential_cache_root):
    fixture = await Fixture().open(tmp_path, cache_root=credential_cache_root)
    configuration = DevelopmentConfiguration(
        image_id=fixture.record.image_id,
        broker_environments_root=tmp_path / "environments",
        private_state_root=tmp_path / "service-private",
    )
    DevelopmentService.prepare_private(configuration)
    development = DevelopmentService(
        fixture.store, fixture.identities, fixture.sandbox, configuration, vault=fixture.vault,
    )
    try:
        yield fixture, development
    finally:
        await development.close()
        await fixture.close()


async def test_project_secret_api_encrypts_and_rotates_without_git_or_lock_cycle(
    wired, monkeypatch,
):
    fixture, development = wired
    assert development.git is None
    calls = []

    async def cleanup(owner_id, project_id):
        # This task must be able to reacquire the project lock after the CAS.
        assert (owner_id, project_id) in development._network_updates
        async with development.locks.hold(owner_id, project_id):
            calls.append((owner_id, project_id))

    monkeypatch.setattr(development.command_secrets, "invalidate", cleanup)
    issued = await fixture.identities.rotate_key(fixture.actor, {}, request_key="secret-api-key")
    application = SimpleNamespace(
        store=fixture.store, identities=fixture.identities, development=development,
        settings=Settings(), access_policy=None,
    )
    api = create_app(application=application)
    route = f"/v2/me/development/projects/{fixture.record.environment.project_id}/secrets/app-token"
    value = "synthetic value with spaces\nand unicode: 测试"
    body = {"expected_revision": 0, "expires_at": time.time() + 300, "value": value}
    async with (
        api.router.lifespan_context(api),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://127.0.0.1",
            headers={"Authorization": "Bearer " + str(issued.token)},
        ) as client,
    ):
        async with asyncio.timeout(5):
            response = await client.put(route, json=body)
        assert response.status_code == 200, response.text
        assert response.json() == {"secret_id": "app-token", "revision": 1, "revoked": False}
        assert value not in response.text
        forbidden = await client.put(route, json=body, headers={"Authorization": ""})
        assert forbidden.status_code == 401
        conflict = await client.put(route, json=body)
        assert conflict.status_code == 409
        rotated = await client.put(route, json={**body, "expected_revision": 1, "value": "new"})
        assert rotated.status_code == 200 and rotated.json()["revision"] == 2
        revoked = await client.put(route, json={
            **body, "expected_revision": 2, "value": "new", "revoked": True,
        })
        assert revoked.status_code == 200 and revoked.json()["revoked"] is True
        invalid = await client.put(route, json={**body, "value": "测" * 3000})
        assert invalid.status_code == 422
    assert len(calls) == 4  # Successful CAS and failed CAS both retire stale resources.
    async with fixture.store.engine.connect() as conn:
        persisted = await conn.scalar(select(project_access_records.c.document).where(
            project_access_records.c.resource_id == "app-token",
        ))
    assert persisted["purpose"] == "project.delivery"
    assert value not in json.dumps(persisted, ensure_ascii=False)
    model = ProjectSecretBody.model_validate(body)
    assert value not in repr(model) and "value" not in model.model_dump()


async def test_secret_command_refs_remain_only_in_authorization(wired):
    fixture, development = wired
    from krim_agent.development.service import DevelopmentScope
    from krim_agent.project_access.service import command_intent
    from krim_agent.runtime.contracts import ExecutionBudget

    scope = DevelopmentScope(
        fixture.actor, fixture.workspace, fixture.record.environment, fixture.ownership,
        development, lambda: None, ExecutionBudget(asyncio.get_running_loop().time() + 30),
    )
    request = ShellInput.model_validate({
        "command": "read provided descriptor", "secret_refs": ["app-token"],
        "timeout_seconds": 30,
    }, strict=True)
    from krim_agent.development.contracts import OperationIdentity

    identity = OperationIdentity(
        operation_id="secret-operation", call_id="secret-call", request_digest="a" * 64,
        environment=fixture.record.environment, ownership=fixture.ownership,
    )
    command = await development._command(scope, request, identity, None)
    assert command.authorization.secret_refs == ("app-token",)
    assert "secret_refs" not in command.model_dump(exclude={"authorization"})
    intent = command_intent(command, workspace_id=fixture.workspace)
    assert command.authorization.arguments_digest == intent.digest(command.authorization)


async def test_project_secret_startup_recovery_cancels_without_redelivery(wired, monkeypatch):
    fixture, development = wired
    from tests.coding_agent.test_project_secret_authorization import prepare_command

    prepared = await prepare_command(fixture.root / "recovery.sqlite")
    try:
        recovered = DevelopmentService(
            prepared.store, fixture.identities, fixture.sandbox, development.configuration,
            vault=fixture.vault,
        )
        cancelled = []

        async def cancel(handle):
            cancelled.append(handle.process_id)
            return handle.model_copy(update={"state": "cancelled", "exit_code": 137})

        async def forbidden(*args):
            raise AssertionError("startup must not re-deliver")

        monkeypatch.setattr(recovered.terminal, "cancel", cancel)
        monkeypatch.setattr(recovered.command_secrets, "launched", forbidden)
        await recovered.recover_secret_executions()
        assert cancelled == [prepared.binding.process_id]
        await recovered.close()
    finally:
        await prepared.store.close()


@pytest.mark.parametrize("values", [
    {"secret_refs": ["a", "a"]}, {"secret_refs": ["path/name"]},
    {"secret_refs": ["a"], "timeout_seconds": None},
    {"secret_refs": ["s" + str(i) for i in range(9)]},
])
def test_secret_shell_input_rejects_invalid_references(values):
    with pytest.raises(ValidationError):
        ShellInput(command="fixture", **values)
