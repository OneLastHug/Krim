"""检查点退役复用现有管理员配额意图与身份围栏。"""

import time
import uuid

import pytest
from test_sandbox_control import managed
from test_sandbox_quota_control import Broker

from krim_agent.contracts import ServiceError
from krim_agent.sandbox.quota_control import confirm, preview, status


class RetirementBroker(Broker):
    async def call(self, action, **args):
        if action == "quota_preview":
            self.calls.append((action, args))
            return {"plan_id": str(uuid.uuid4()), "plan_digest": "c" * 64,
                    "expires_at": time.time() + 900, "retirement": True,
                    "target": {"limits": args["limits"]}}
        if action.startswith("quota_retirement_"):
            self.calls.append((action, args))
            phase = "cancelled" if action.endswith("cancel") else self.phase
            return {"retirement": True, "retirement_state": phase,
                    "quota_state": "applied" if phase == "retired" else phase,
                    "operation_id": args["operation_id"], "actual": {}}
        return await super().call(action, **args)


async def test_retirement_confirm_and_status_keep_old_applied_until_actual_retired(tmp_path):
    async with managed(tmp_path / "retirement.sqlite") as (_, _, service, admin, actor, *_):
        broker = RetirementBroker()
        service.broker = broker
        await service.provision(actor, request_key="prepare")
        plan = await preview(service, admin, actor.principal_id, {
            "expected_revision": 1, "overrides": {"checkpoint_bytes": 0}
        })
        first = await confirm(service, admin, actor.principal_id,
                              plan["plan_id"], plan["plan_digest"], 1, "retire")
        assert first["quota_state"] == "pending_reclaim"
        assert broker.calls[-1][0] == "quota_retirement_confirm"
        waiting = await service.environment(actor)
        assert waiting["limits"]["checkpoint_bytes"] > 0
        assert waiting["desired_limits"]["checkpoint_bytes"] == 0
        assert waiting["applied_revision"] == 1
        with pytest.raises(ServiceError, match="permission_denied"):
            await status(service, actor, actor.principal_id, first["operation_id"])
        broker.phase = "retired"
        done = await status(service, admin, actor.principal_id, first["operation_id"])
        assert broker.calls[-1][0] == "quota_retirement_status"
        assert done["quota_state"] == "applied" and done["applied_revision"] == 2
        final = await service.environment(actor)
        assert final["limits"]["checkpoint_bytes"] == 0
        replay = await confirm(service, admin, actor.principal_id,
                               plan["plan_id"], plan["plan_digest"], 1, "retire")
        assert replay["quota_state"] == "applied"
        assert sum(action == "quota_retirement_confirm" for action, _ in broker.calls) == 1


async def test_retirement_http_requires_explicit_confirmation_and_reports_actual_state(tmp_path):
    from test_account_api import client_for, initialize

    database = tmp_path / "retirement-api.sqlite"
    token = await initialize(database)
    configuration = tmp_path / "sandbox.json"
    configuration.write_text("{}")
    configuration.chmod(0o600)
    async with client_for(database, token, sandbox_config_file=configuration) as (app, client):
        rpc = RetirementBroker()
        app.sandbox.broker = rpc
        actor = app.identities.authenticate("Bearer " + token)
        await app.sandbox.provision(actor, request_key="prepare")
        prefix = f"/v2/admin/accounts/{actor.principal_id}/sandbox"
        response = await client.post(prefix + "/quota-plans", json={
            "expected_revision": 1, "overrides": {"checkpoint_bytes": 0},
        })
        assert response.status_code == 200, response.text
        plan = response.json()
        endpoint = prefix + f"/quota-plans/{plan['plan_id']}/confirm"
        body = {"request_key": "retire", "expected_revision": 1,
                "plan_digest": plan["plan_digest"], "confirmed": True}
        assert (await client.post(endpoint, json={**body, "confirmed": False})).status_code == 422
        assert not any(action == "quota_retirement_confirm" for action, _ in rpc.calls)
        accepted = await client.post(endpoint, json=body)
        assert accepted.status_code == 200, accepted.text
        assert accepted.json()["quota_state"] == "pending_reclaim"
        rpc.phase = "retired"
        operation_id = accepted.json()["operation_id"]
        done = await client.get(prefix + f"/quota-operations/{operation_id}")
        assert done.status_code == 200 and done.json()["quota_state"] == "applied"
        assert done.json()["applied_revision"] == 2


async def test_retirement_cancel_routes_original_operation_without_claiming_applied(tmp_path):
    async with managed(tmp_path / "cancel-retire.sqlite") as (_, _, service, admin, actor, *_):
        broker = RetirementBroker()
        service.broker = broker
        await service.provision(actor, request_key="prepare")
        plan = await preview(service, admin, actor.principal_id, {
            "expected_revision": 1, "overrides": {"checkpoint_bytes": 0}
        })
        accepted = await confirm(service, admin, actor.principal_id,
                                 plan["plan_id"], plan["plan_digest"], 1, "retire")
        cancelled = await status(service, admin, actor.principal_id,
                                 accepted["operation_id"], cancel=True)
        assert broker.calls[-1][0] == "quota_retirement_cancel"
        assert cancelled["quota_state"] == "cancelled" and cancelled["applied_revision"] == 1
        assert (await service.environment(actor))["limits"]["checkpoint_bytes"] > 0
