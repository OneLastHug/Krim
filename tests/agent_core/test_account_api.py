"""托管账号 API 与原身份契约、数据库身份模式的集成验证。"""

from contextlib import asynccontextmanager

import httpx
import pytest
from pydantic import SecretStr

from krim_agent.accounts.service import IdentityService
from krim_agent.api.server import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import ServiceError
from krim_agent.storage.sqlite import SQLiteStore


async def initialize(path):
    store = await SQLiteStore.open(path)
    try:
        identities = await IdentityService.open(store)
        result = await identities.bootstrap("管理员", request_key="bootstrap")
        return result.token
    finally:
        await store.close()


@asynccontextmanager
async def client_for(path, token, **settings):
    async with await open_application(path, settings=Settings(database=path, **settings)) as app:
        api = create_app(application=app)
        async with (
            api.router.lifespan_context(api),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api),
                base_url="http://127.0.0.1",
                headers={"Authorization": "Bearer " + token},
            ) as client,
        ):
            yield app, client


async def test_account_api_lifecycle_keeps_old_me_shape(tmp_path):
    path = tmp_path / "managed.sqlite"
    admin_token = await initialize(path)
    async with client_for(path, admin_token) as (app, client):
        response = await client.get("/v2/me")
        assert response.status_code == 200, response.text
        assert set(response.json()) == {
            "principal_id",
            "generation",
            "legacy",
            "target_keys",
            "workspaces",
        }
        original = response.json()
        profile = await client.get("/v2/me/profile")
        assert profile.status_code == 200, profile.text
        assert profile.json()["display_name"] == "管理员"
        assert (await client.get("/v2/admin/accounts")).json()["total"] == 1
        created = await client.post(
            "/v2/admin/accounts", json={"request_key": "create-member", "display_name": "用户甲"}
        )
        assert created.status_code == 201, created.text
        body = created.json()
        account = body.get("account", body)
        member_id = account["principal_id"]
        issued = await client.post(
            f"/v2/admin/accounts/{member_id}/key/issue", json={"request_key": "issue-member"}
        )
        assert issued.status_code == 200, issued.text
        assert issued.headers["cache-control"] == "no-store"
        member_token = issued.json()["token"]
        assert member_token
        replay = await client.post(
            f"/v2/admin/accounts/{member_id}/key/issue", json={"request_key": "issue-member"}
        )
        assert replay.status_code == 200 and replay.json()["token"] is None
        member_headers = {"Authorization": "Bearer " + member_token}
        own = await client.get("/v2/me/profile", headers=member_headers)
        assert own.status_code == 200 and own.json()["display_name"] == "用户甲"
        denied = await client.get("/v2/admin/accounts", headers=member_headers)
        assert denied.status_code == 403, denied.text
        memberships = await client.get(f"/v2/admin/accounts/{member_id}/memberships")
        assert memberships.status_code == 200, memberships.text
        membership_body = memberships.json()
        assert membership_body["principal_id"] == member_id
        assert membership_body["generation"] == own.json()["generation"]
        assert membership_body["memberships"][0]["personal"] is True
        assert membership_body["memberships"][0]["display_name"] == "个人工作区"
        assert (
            await client.get(f"/v2/admin/accounts/{member_id}/memberships", headers=member_headers)
        ).status_code == 403
        assert member_token not in memberships.text
        escalated = await client.patch(
            "/v2/me/profile",
            headers=member_headers,
            json={
                "request_key": "escalate",
                "expected_revision": own.json()["revision"],
                "platform_role": "platform_admin",
            },
        )
        assert escalated.status_code == 422
        rotated = await client.post(
            "/v2/me/key/rotate", headers=member_headers, json={"request_key": "rotate-member"}
        )
        assert rotated.status_code == 200, rotated.text
        new_token = rotated.json()["token"]
        assert new_token and new_token != member_token
        assert (await client.get("/v2/me", headers=member_headers)).status_code == 401
        new_headers = {"Authorization": "Bearer " + new_token}
        me = await client.get("/v2/me", headers=new_headers)
        assert me.status_code == 200 and me.json()["principal_id"] == member_id
        current = await client.get(f"/v2/admin/accounts/{member_id}")
        disabled = await client.patch(
            f"/v2/admin/accounts/{member_id}",
            json={
                "request_key": "disable-member",
                "expected_revision": current.json()["revision"],
                "status": "disabled",
            },
        )
        assert disabled.status_code == 200, disabled.text
        assert (await client.get("/v2/me", headers=new_headers)).status_code == 401
        assert (await client.get("/v2/me")).json() == original
        events = await client.get(
            "/v2/admin/account-events",
            params={"principal_id": member_id, "request_key": "issue-member"},
        )
        assert events.status_code == 200, events.text
        assert events.json()["total"] == 1
        assert {event["request_key"] for event in events.json()["items"]} == {"issue-member"}
        assert member_token not in events.text and new_token not in events.text
        assert app.identities.managed


async def test_account_mutations_require_request_key_and_revision(tmp_path):
    path = tmp_path / "keys.sqlite"
    token = await initialize(path)
    async with client_for(path, token) as (_, client):
        assert (
            await client.post("/v2/admin/accounts", json={"display_name": "甲"})
        ).status_code == 422
        assert (await client.post("/v2/me/key/rotate", json={})).status_code == 422
        assert (
            await client.patch(
                "/v2/me/profile", json={"request_key": "missing-revision", "display_name": "乙"}
            )
        ).status_code == 422
        profile = (await client.get("/v2/me/profile")).json()
        binding_before = (await client.get("/v2/targets/demo/connection-profile")).json()
        renamed = await client.patch(
            "/v2/me/profile",
            json={
                "request_key": "rename",
                "expected_revision": profile["revision"],
                "display_name": "新名称",
            },
        )
        assert renamed.status_code == 200, renamed.text
        assert (await client.get("/v2/targets/demo/connection-profile")).json() == binding_before
        assert (await client.get("/v2/me/profile")).json()["generation"] == profile["generation"]
        conflict = await client.patch(
            "/v2/me/profile",
            json={
                "request_key": "stale-rename",
                "expected_revision": profile["revision"],
                "display_name": "不覆盖",
            },
        )
        assert conflict.status_code == 409, conflict.text
        last = await client.post("/v2/me/key/revoke", json={"request_key": "revoke-last"})
        assert last.status_code == 403, last.text
        assert (await client.get("/v2/me")).status_code == 200


async def test_managed_database_never_falls_back_to_legacy_or_local(tmp_path):
    path = tmp_path / "mode.sqlite"
    token = await initialize(path)
    for changes in ({"identity_mode": "legacy"}, {"api_key": SecretStr("old-global")}):
        with pytest.raises(ServiceError, match="identity_mode_conflict"):
            await open_application(path, settings=Settings(database=path, **changes))
    async with client_for(path, token) as (_, client):
        for authorization in ("", "Bearer old-global", "Bearer unknown"):
            denied = await client.get("/v2/me", headers={"Authorization": authorization})
            assert denied.status_code == 401
        assert (await client.get("/healthz", headers={"Authorization": ""})).status_code == 200
    with pytest.raises(ServiceError, match="accounts_not_initialized"):
        await open_application(
            tmp_path / "empty.sqlite", settings=Settings(identity_mode="managed")
        )


async def test_connection_profile_is_public_metadata_not_provider_config(tmp_path):
    path = tmp_path / "connection.sqlite"
    token = await initialize(path)
    async with client_for(path, token, gateway_display_name="测试网关") as (_, client):
        response = await client.get("/v2/targets/demo/connection-profile")
        assert response.status_code == 200, response.text
        assert response.json()["mode"] == "local"
        assert response.json()["display_name"] == "本地模式"
        assert set(response.json()) == {
            "profile_id",
            "display_name",
            "mode",
            "target_key",
            "target_fingerprint",
        }
        assert len(response.json()["target_fingerprint"]) == 64
        assert "base_url" not in response.text and "api_key" not in response.text
        assert (await client.get("/v2/targets/unknown/connection-profile")).status_code == 404


async def test_gateway_profile_uses_trusted_name_without_exposing_address(tmp_path, wire):
    path = tmp_path / "named-gateway.sqlite"
    token = await initialize(path)
    async with wire.application(path, settings=Settings(gateway_display_name="团队接入")) as app:
        api = create_app(application=app)
        async with (
            api.router.lifespan_context(api),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api),
                base_url="http://127.0.0.1",
                headers={"Authorization": "Bearer " + token},
            ) as client,
        ):
            response = await client.get("/v2/targets/demo/connection-profile")
            assert response.status_code == 200, response.text
            assert response.json()["mode"] == "api_gateway"
            assert response.json()["display_name"] == "团队接入"
            assert f":{wire.port}" not in response.text
            assert "127.0.0.1" not in response.text and "fixture" not in response.text
            me = (await client.get("/v2/me")).json()
            created = await client.post(
                "/v2/workspaces/" + me["workspaces"][0]["workspace_id"] + "/sessions",
                json={"request_key": "profile-session", "title": "网关身份", "target_key": "demo"},
            )
            assert created.status_code == 201, created.text
            assert created.json()["target_fingerprint"] == response.json()["target_fingerprint"]


async def test_disabling_imported_legacy_identity_cancels_unassociated_v1_run(tmp_path, wire):
    from krim_agent.security import AccessPolicy, Credential, Membership

    path = tmp_path / "legacy-cancel.sqlite"
    store = await SQLiteStore.open(path)
    try:
        identities = await IdentityService.open(store)
        await identities.import_policy(
            AccessPolicy(
                credentials=(
                    Credential(
                        principal_id="administrator",
                        token=SecretStr("admin-test"),
                        memberships=(Membership(workspace_id="admin-work"),),
                    ),
                    Credential(
                        principal_id="legacy-owner",
                        token=SecretStr("legacy-test"),
                        memberships=(Membership(workspace_id="legacy-work"),),
                        legacy=True,
                    ),
                )
            ),
            "administrator",
            request_key="import-legacy",
        )
    finally:
        await store.close()
    async with wire.application(path) as app:
        api = create_app(application=app)
        async with (
            api.router.lifespan_context(api),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api),
                base_url="http://127.0.0.1",
                headers={"Authorization": "Bearer admin-test"},
            ) as client,
        ):
            response = await client.post(
                "/v1/runs",
                headers={"Authorization": "Bearer legacy-test"},
                json={"text": "/hold", "request_key": "legacy-hold"},
            )
            assert response.status_code == 202, response.text
            run_id = response.json()["id"]
            current = (await client.get("/v2/admin/accounts/legacy-owner")).json()
            disabled = await client.patch(
                "/v2/admin/accounts/legacy-owner",
                json={
                    "request_key": "disable-legacy",
                    "expected_revision": current["revision"],
                    "status": "disabled",
                },
            )
            assert disabled.status_code == 200, disabled.text
            assert (await app.service.get(run_id)).status in {"aborted", "failed"}
            assert app.service.tasks[run_id].done()
            assert (
                await client.get("/v2/me", headers={"Authorization": "Bearer legacy-test"})
            ).status_code == 401


async def test_legacy_mode_keeps_original_me_and_no_management(tmp_path):
    async with client_for(tmp_path / "legacy.sqlite", "") as (_, client):
        assert (await client.get("/v2/me")).json()["principal_id"] == "local"
        assert (await client.get("/v2/me/profile")).status_code == 409
        assert (await client.get("/v2/admin/accounts")).status_code == 409
