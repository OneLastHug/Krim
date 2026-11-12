"""沙箱管理地基：归属、三级配额、幂等和未就绪状态的真实表达。"""

from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest

from krim_agent.accounts.service import IdentityService
from krim_agent.api.server import create_app
from krim_agent.composition import open_application
from krim_agent.config import SESSION_FIELDS, Settings
from krim_agent.contracts import ServiceError
from krim_agent.sandbox.configuration import SandboxConfiguration
from krim_agent.sandbox.service import SandboxService
from krim_agent.storage.sqlite import SQLiteStore

GIB = 1024**3


@asynccontextmanager
async def managed(path, configuration=None):
    store = await SQLiteStore.open(path)
    identities = await IdentityService.open(store)
    boot = await identities.bootstrap("管理员", request_key="boot")
    admin = identities.authenticate("Bearer " + boot.token)
    created = await identities.create_account(admin, {"display_name": "甲"}, request_key="create-a")
    issued = await identities.issue_key(
        admin, created.account.principal_id, {}, request_key="key-a"
    )
    member = identities.authenticate("Bearer " + issued.token)
    sandbox = SandboxService(store, identities, configuration or SandboxConfiguration())
    try:
        yield store, identities, sandbox, admin, member, str(boot.token), str(issued.token)
    finally:
        await sandbox.close()
        await identities.close()
        await store.close()


async def test_configuration_hierarchy_and_admin_only(tmp_path):
    config = SandboxConfiguration.model_validate(
        {"quotas": {"profiles": {"large": {"environment_bytes": 20 * GIB}}}}
    )
    async with managed(tmp_path / "quota.sqlite", config) as (_, _, service, admin, member, *_):
        initial = await service.environment(member)
        assert initial["limits"]["environment_bytes"] == 5 * GIB
        assert initial["revision"] == 0
        assert initial["usage"] is None
        assert initial["runtime_available"] is False
        configured = await service.configure_quota(
            admin,
            member.principal_id,
            {
                "expected_revision": 0,
                "profile_name": "large",
                "overrides": {"environment_bytes": 12 * GIB, "frozen_bytes": 2 * GIB},
            },
            request_key="quota-a",
        )
        assert configured["limits"]["environment_bytes"] == 12 * GIB
        assert configured["limits"]["frozen_bytes"] == 2 * GIB
        assert configured["enforcement"] == "not_provisioned"
        assert configured["revision"] == 1
        assert (await service.environment(member))["environment_id"] == configured["environment_id"]
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.configure_quota(
                member,
                member.principal_id,
                {
                    "expected_revision": 1,
                    "overrides": {"environment_bytes": 30 * GIB},
                },
                request_key="escalate",
            )
        with pytest.raises(ServiceError, match="permission_denied"):
            await service.environment(member, admin.principal_id)


async def test_projects_are_private_and_request_keys_are_actor_scoped(tmp_path):
    async with managed(tmp_path / "projects.sqlite") as (_, _, service, admin, member, *_):
        a_body = {"workspace_id": member.memberships[0].workspace_id, "title": "项目甲"}
        first = await service.create_project(member, a_body, request_key="same-key")
        replay = await service.create_project(member, a_body, request_key="same-key")
        assert first == replay
        assert first["status"] == "unprovisioned"
        assert "storage_ref" not in first and "lease_id" not in first
        second = await service.create_project(
            admin,
            {
                "workspace_id": admin.memberships[0].workspace_id,
                "title": "管理员项目",
            },
            request_key="same-key",
        )
        assert first["project_id"] != second["project_id"]
        assert (await service.list_projects(member))["total"] == 1
        assert (await service.project(member, first["project_id"])) == first
        with pytest.raises(ServiceError, match="not_found"):
            await service.project(admin, first["project_id"])
        with pytest.raises(ServiceError, match="not_found"):
            await service.create_project(
                member,
                {
                    "workspace_id": admin.memberships[0].workspace_id,
                    "title": "越权",
                },
                request_key="wrong-workspace",
            )
        with pytest.raises(ServiceError, match="key_conflict"):
            await service.create_project(
                member, {**a_body, "title": "不同内容"}, request_key="same-key"
            )
        operation = await service.operation(member, "same-key")
        assert operation["result"]["project_id"] == first["project_id"]


async def test_quota_revision_and_replay(tmp_path):
    async with managed(tmp_path / "revision.sqlite") as (_, _, service, admin, member, *_):
        body = {"expected_revision": 0, "overrides": {"environment_bytes": 8 * GIB}}
        first = await service.configure_quota(admin, member.principal_id, body, request_key="q")
        assert first == await service.configure_quota(
            admin, member.principal_id, body, request_key="q"
        )
        with pytest.raises(ServiceError, match="revision_conflict"):
            await service.configure_quota(admin, member.principal_id, body, request_key="old")
        with pytest.raises(ServiceError, match="key_conflict"):
            await service.configure_quota(
                admin,
                member.principal_id,
                {**body, "overrides": {"environment_bytes": 9 * GIB}},
                request_key="q",
            )
        reduced = await service.configure_quota(
            admin,
            member.principal_id,
            {
                "expected_revision": 1,
                "overrides": {"environment_bytes": 3 * GIB},
            },
            request_key="smaller",
        )
        assert reduced["revision"] == 2 and reduced["limits"]["environment_bytes"] == 3 * GIB
        assert reduced["usage"] is None


async def test_commit_response_loss_looks_up_original_operation(tmp_path, monkeypatch):
    async with managed(tmp_path / "unknown.sqlite") as (store, _, service, _, member, *_):
        original = store._write
        calls = 0

        async def lost_reply(operation):
            nonlocal calls
            calls += 1
            await original(operation)
            raise ServiceError("storage_unavailable", 503)

        monkeypatch.setattr(store, "_write", lost_reply)
        result = await service.create_project(
            member,
            {
                "workspace_id": member.memberships[0].workspace_id,
                "title": "回包丢失",
            },
            request_key="lost",
        )
        assert calls == 1
        assert result["project_id"]
        assert (await service.list_projects(member))["total"] == 1


async def test_old_authorization_rejected_and_rotation_preserves_environment(tmp_path):
    async with managed(tmp_path / "identity.sqlite") as (_, identities, service, admin, member, *_):
        project = await service.create_project(
            member,
            {
                "workspace_id": member.memberships[0].workspace_id,
                "title": "持久归属",
            },
            request_key="project",
        )
        rotated = await identities.rotate_key(member, {}, request_key="rotate")
        new_actor = identities.authenticate("Bearer " + rotated.token)
        assert (await service.project(new_actor, project["project_id"])) == project
        view = await identities.account(admin, member.principal_id)
        await identities.update_account(
            admin,
            member.principal_id,
            {
                "expected_revision": view.revision,
                "status": "disabled",
            },
            request_key="disable",
        )
        with pytest.raises(ServiceError, match="unauthorized"):
            await service.list_projects(member)


async def test_management_api_feature_gate_and_unavailable_runtime(tmp_path):
    path = tmp_path / "api.sqlite"
    store = await SQLiteStore.open(path)
    identities = await IdentityService.open(store)
    token = str((await identities.bootstrap("管理员", request_key="boot")).token)
    await identities.close()
    await store.close()
    configuration = tmp_path / "sandbox.json"
    configuration.write_text("{}")
    configuration.chmod(0o600)
    for enabled in [False, True]:
        settings = Settings(database=path, sandbox_config_file=configuration if enabled else None)
        async with await open_application(path, settings=settings) as application:
            api = create_app(application=application)
            async with (
                api.router.lifespan_context(api),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=api),
                    base_url="http://127.0.0.1",
                    headers={"Authorization": "Bearer " + token},
                ) as client,
            ):
                response = await client.get("/v2/me/sandbox")
                if not enabled:
                    assert response.status_code == 409
                    assert response.json()["error"]["code"] == "sandbox_management_unavailable"
                    continue
                assert response.status_code == 200, response.text
                assert response.json()["runtime_available"] is False
                assert response.json()["usage"] is None
                unavailable = await client.post(
                    "/v2/me/sandbox/provision", json={"request_key": "prepare"}
                )
                assert unavailable.status_code == 503
                assert unavailable.json()["error"]["code"] == "sandbox_backend_unavailable"
                assert (await client.get("/v2/me")).status_code == 200
                assert "environment_id" not in (await client.get("/v2/me")).json()


async def test_api_creates_metadata_and_rejects_invalid_mutations(tmp_path):
    path = tmp_path / "api-write.sqlite"
    store = await SQLiteStore.open(path)
    identities = await IdentityService.open(store)
    token = str((await identities.bootstrap("管理员", request_key="boot")).token)
    await identities.close()
    await store.close()
    configuration = tmp_path / "sandbox.json"
    configuration.write_text("{}")
    configuration.chmod(0o600)
    async with await open_application(
        path, settings=Settings(database=path, sandbox_config_file=configuration)
    ) as application:
        api = create_app(application=application)
        async with (
            api.router.lifespan_context(api),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api),
                base_url="http://127.0.0.1",
                headers={"Authorization": "Bearer " + token},
            ) as client,
        ):
            actor = (await client.get("/v2/me")).json()
            principal_id = actor["principal_id"]
            body = {"workspace_id": actor["workspaces"][0]["workspace_id"], "title": "项目"}
            assert (await client.post("/v2/me/sandbox/projects", json=body)).status_code == 422
            created = await client.post(
                "/v2/me/sandbox/projects", json={**body, "request_key": "p"}
            )
            assert created.status_code == 201, created.text
            assert (await client.get("/v2/me/sandbox/projects")).json()["total"] == 1
            assert (
                await client.get("/v2/me/sandbox/operations", params={"request_key": "p"})
            ).status_code == 200
            bad = await client.put(
                f"/v2/admin/accounts/{principal_id}/sandbox/quota",
                json={
                    "request_key": "bad",
                    "expected_revision": True,
                    "overrides": {},
                },
            )
            assert bad.status_code == 422, bad.text
            unknown = await client.put(
                f"/v2/admin/accounts/{principal_id}/sandbox/quota",
                json={
                    "request_key": "unknown",
                    "expected_revision": 1,
                    "profile_name": "missing",
                },
            )
            assert unknown.status_code == 422, unknown.text


def test_configuration_manifest_requires_matching_image_and_dev_gate(tmp_path):
    from pydantic import ValidationError

    manifest = Path(__file__).resolve().parents[2] / "runtime_images" / "runtime-manifest.json"
    image = "sha256:d163eadaea1f779baca092829e2c6969e7514bf284880f8637d8204320ff505b"
    common = {
        "broker_socket": tmp_path / "broker.sock",
        "command_image": image,
        "runtime_manifest": manifest,
    }
    with pytest.raises(ValidationError):
        SandboxConfiguration.model_validate(common)
    selected = SandboxConfiguration.model_validate({**common, "allow_dev_runtimes": True})
    assert selected.resolve_runtime("python").image == image
    with pytest.raises(ServiceError, match="sandbox_backend_unavailable"):
        selected.resolve_runtime("node")
    with pytest.raises(ValidationError):
        SandboxConfiguration.model_validate(
            {**common, "allow_dev_runtimes": True, "command_image": "sha256:" + "a" * 64}
        )


def test_configuration_accepts_only_fixed_image_references(tmp_path):
    from pydantic import ValidationError

    for image in [
        "sha256:" + "a" * 64,
        "docker.io/library/python@sha256:" + "b" * 64,
    ]:
        config = SandboxConfiguration.model_validate(
            {"broker_socket": tmp_path / "broker.sock", "command_image": image}
        )
        assert config.command_image == image
    for image in [
        "python:latest",
        "docker.io/library/python:3.12@sha256:" + "b" * 64,
        "docker.io/library/python@sha256:" + "b" * 63,
        "docker.io/library/node@sha256:" + "b" * 64,
    ]:
        with pytest.raises(ValidationError):
            SandboxConfiguration.model_validate(
                {"broker_socket": tmp_path / "broker.sock", "command_image": image}
            )


def test_trusted_configuration_limits_permissions_and_symlinks(tmp_path):
    path = tmp_path / "sandbox.json"
    path.write_text("{}")
    path.chmod(0o600)
    assert SandboxConfiguration.load(path).quotas.resolve().environment_bytes == 5 * GIB
    assert "sandbox_config_file" in SESSION_FIELDS
    path.chmod(0o644)
    with pytest.raises(ServiceError, match="sandbox_config_invalid"):
        SandboxConfiguration.load(path)
    path.chmod(0o600)
    link = tmp_path / "link.json"
    link.symlink_to(path)
    with pytest.raises(ServiceError, match="sandbox_config_invalid"):
        SandboxConfiguration.load(link)
    path.write_text('{"unknown": 1}')
    with pytest.raises(ServiceError, match="sandbox_config_invalid"):
        SandboxConfiguration.load(path)


async def test_project_cap_does_not_block_replay(tmp_path):
    configuration = SandboxConfiguration(max_projects_per_account=1)
    async with managed(tmp_path / "cap.sqlite", configuration) as (_, _, service, _, member, *_):
        body = {"workspace_id": member.memberships[0].workspace_id, "title": "唯一项目"}
        first = await service.create_project(member, body, request_key="first")
        assert await service.create_project(member, body, request_key="first") == first
        with pytest.raises(ServiceError, match="sandbox_limit"):
            await service.create_project(member, body, request_key="second")
        assert (await service.list_projects(member))["total"] == 1


async def test_auth_is_rechecked_after_waiting_for_database_lock(tmp_path):
    import asyncio

    from sqlalchemy import update

    from krim_agent.storage.account_models import accounts

    async with managed(tmp_path / "waiting.sqlite") as (store, _, service, _, member, *_):
        await store.lock.acquire()
        task = asyncio.create_task(
            service.create_project(
                member,
                {
                    "workspace_id": member.memberships[0].workspace_id,
                    "title": "等待中的操作",
                },
                request_key="waiting",
            )
        )
        try:
            await asyncio.sleep(0)
            async with store.engine.begin() as conn:
                await conn.execute(
                    update(accounts)
                    .where(accounts.c.principal_id == member.principal_id)
                    .values(status="disabled")
                )
        finally:
            store.lock.release()
        with pytest.raises(ServiceError, match="unauthorized"):
            await task


def test_invalid_profile_combination_is_rejected_at_startup():
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        SandboxConfiguration.model_validate(
            {"quotas": {"profiles": {"invalid": {"environment_bytes": 1024**2}}}}
        )


def test_platform_without_secure_open_flags_returns_configuration_error(tmp_path, monkeypatch):
    import os

    path = tmp_path / "config.json"
    path.write_text("{}")
    monkeypatch.delattr(os, "O_NOFOLLOW")
    with pytest.raises(ServiceError, match="sandbox_config_invalid"):
        SandboxConfiguration.load(path)


async def test_put_quota_replaces_profile_and_overrides_explicitly(tmp_path):
    config = SandboxConfiguration.model_validate(
        {"quotas": {"profiles": {"small": {"environment_bytes": 2 * GIB}}}}
    )
    async with managed(tmp_path / "put.sqlite", config) as (_, _, service, admin, member, *_):
        first = await service.configure_quota(
            admin,
            member.principal_id,
            {
                "expected_revision": 0,
                "profile_name": "small",
                "overrides": {},
            },
            request_key="small",
        )
        assert first["limits"]["environment_bytes"] == 2 * GIB
        reset = await service.configure_quota(
            admin,
            member.principal_id,
            {
                "expected_revision": 1,
                "profile_name": None,
                "overrides": {},
            },
            request_key="reset",
        )
        assert reset["profile_name"] is None
        assert reset["limits"]["environment_bytes"] == 5 * GIB
