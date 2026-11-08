"""恢复与在线配额HTTP接口：明确确认、幂等、权限和实际状态。"""

import uuid

from test_account_api import client_for, initialize
from test_sandbox_maintenance import Rpc
from test_sandbox_quota_control import Broker as QuotaBroker


class ApiBroker(Rpc):
    async def call(self, action, **args):
        if action == "checkpoint_list":
            return {"items": [], "total": 0, "offset": args["offset"], "limit": args["limit"]}
        if action == "checkpoint_retention":
            return {"checkpoint_id": args["checkpoint_id"], "pinned": args["pinned"]}
        if action == "checkpoint_cleanup_preview":
            return {"plan_id": str(uuid.uuid4()), "plan_digest": "a" * 64, "candidates": []}
        if action == "checkpoint_cleanup_confirm":
            return {"status": "completed", "physical_reclaim_bytes": 0}
        return await super().call(action, **args)


async def test_restore_http_requires_confirmation_and_replays_original_result(tmp_path):
    database = tmp_path / "api.sqlite"
    token = await initialize(database)
    config = tmp_path / "sandbox.json"
    config.write_text("{}")
    config.chmod(0o600)
    async with client_for(database, token, sandbox_config_file=config) as (app, client):
        rpc = ApiBroker()
        app.sandbox.broker = rpc
        me = (await client.get("/v2/me")).json()
        created = await client.post(
            "/v2/me/sandbox/projects",
            json={
                "request_key": "project",
                "title": "HTTP恢复",
                "workspace_id": me["workspaces"][0]["workspace_id"],
            },
        )
        project = created.json()["project_id"]
        prefix = f"/v2/me/sandbox/projects/{project}"
        assert (
            await client.post(prefix + "/provision", json={"request_key": "prepare"})
        ).status_code == 200
        assert (await client.get(prefix + "/checkpoints")).json()["items"] == []
        checkpoint = str(uuid.uuid4())
        plan = (await client.post(prefix + f"/checkpoints/{checkpoint}/restore-plans")).json()
        endpoint = prefix + f"/restore-plans/{plan['plan_id']}/confirm"
        body = {
            "request_key": "restore",
            "plan_digest": plan["plan_digest"],
            "expected_generation": 0,
            "selected": ["step"],
            "confirmed": True,
        }
        assert (await client.post(endpoint, json={**body, "confirmed": False})).status_code == 422
        response = await client.post(endpoint, json=body)
        assert response.status_code == 200, response.text
        assert response.json()["status"] == "completed"
        assert (await client.post(endpoint, json=body)).json() == response.json()
        assert rpc.step == 2
        retained = await client.put(
            prefix + f"/checkpoints/{checkpoint}/retention",
            json={"request_key": "pin", "expected_generation": 1, "pinned": True},
        )
        assert retained.status_code == 200 and retained.json()["pinned"]
        cleanup = (await client.post(prefix + "/checkpoint-cleanup-plans")).json()
        collected = await client.post(
            prefix + f"/checkpoint-cleanup-plans/{cleanup['plan_id']}/confirm",
            json={
                "request_key": "gc",
                "expected_generation": 1,
                "plan_digest": cleanup["plan_digest"],
                "confirmed": True,
            },
        )
        assert collected.status_code == 200 and collected.json()["physical_reclaim_bytes"] == 0
        assert (
            await client.post(
                endpoint, json={**body, "request_key": "bad", "expected_generation": True}
            )
        ).status_code == 422


async def test_quota_http_admin_only_and_pending_values_are_not_applied(tmp_path):
    database = tmp_path / "quota.sqlite"
    token = await initialize(database)
    config = tmp_path / "sandbox.json"
    config.write_text("{}")
    config.chmod(0o600)
    async with client_for(database, token, sandbox_config_file=config) as (app, client):
        app.sandbox.broker = QuotaBroker()
        actor = app.identities.authenticate("Bearer " + token)
        created = await app.identities.create_account(
            actor, {"display_name": "普通账号"}, request_key="member"
        )
        issued = await app.identities.issue_key(
            actor, created.account.principal_id, {}, request_key="key"
        )
        member = app.identities.authenticate("Bearer " + issued.token)
        await app.sandbox.provision(member, request_key="prepare")
        prefix = f"/v2/admin/accounts/{member.principal_id}/sandbox"
        body = {"expected_revision": 1, "overrides": {"environment_bytes": 8 * 1024**3}}
        denied = await client.post(
            prefix + "/quota-plans", json=body, headers={"Authorization": "Bearer " + issued.token}
        )
        assert denied.status_code == 403
        preview = await client.post(prefix + "/quota-plans", json=body)
        assert preview.status_code == 200, preview.text
        plan = preview.json()
        accepted = await client.post(
            prefix + f"/quota-plans/{plan['plan_id']}/confirm",
            json={
                "request_key": "quota",
                "expected_revision": 1,
                "plan_digest": plan["plan_digest"],
                "confirmed": True,
            },
        )
        assert accepted.status_code == 200, accepted.text
        result = accepted.json()
        assert result["quota_state"] == "pending_reclaim"
        assert result["desired_revision"] == 2 and result["applied_revision"] == 1
        app.sandbox.broker.phase = "applied"
        updated = await client.get(prefix + f"/quota-operations/{result['operation_id']}")
        assert updated.status_code == 200 and updated.json()["quota_state"] == "applied"
