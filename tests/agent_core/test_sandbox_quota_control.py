"""在线配额API的管理员授权、desired/applied分离与版本幂等。"""

import time
import uuid

import pytest
from test_sandbox_control import managed

from krim_agent.contracts import ServiceError
from krim_agent.sandbox.quota_control import confirm, preview, status


class Broker:
    def __init__(self):
        self.phase = "pending_reclaim"
        self.calls = []

    async def call(self, action, **args):
        self.calls.append((action, args))
        if action.startswith("provision_"):
            return {"status": "ready"}
        if action == "usage":
            return {
                "usage": {
                    "environment_bytes": 6 * 1024**3,
                    "environment_inodes": 100,
                    "byte_limit": 10 * 1024**3,
                    "inode_limit": 100000,
                }
            }
        if action == "quota_preview":
            return {
                "plan_id": str(uuid.uuid4()),
                "plan_digest": "a" * 64,
                "expires_at": time.time() + 900,
                "target": {"limits": args["limits"]},
            }
        return {
            "quota_state": self.phase,
            "desired": {"limits": {}},
            "applied": {},
            "usage": {},
            "operation_id": args["operation_id"],
        }


async def test_pending_quota_keeps_old_config_and_applied_revision(tmp_path):
    async with managed(tmp_path / "q.sqlite") as (_, _, service, admin, actor, *_):
        service.broker = Broker()
        await service.configure_quota(
            admin,
            actor.principal_id,
            {"expected_revision": 0, "overrides": {"environment_bytes": 10 * 1024**3}},
            request_key="initial",
        )
        await service.provision(actor, request_key="prepare")
        plan = await preview(
            service,
            admin,
            actor.principal_id,
            {"expected_revision": 1, "overrides": {"environment_bytes": 5 * 1024**3}},
        )
        response = await confirm(
            service, admin, actor.principal_id, plan["plan_id"], plan["plan_digest"], 1, "quota"
        )
        assert response["quota_state"] == "pending_reclaim"
        env = await service.environment(actor)
        assert env["desired_limits"]["environment_bytes"] == 5 * 1024**3
        assert env["limits"]["environment_bytes"] == 10 * 1024**3
        assert env["applied_revision"] == 1 and env["desired_revision"] == 2
        service.broker.phase = "applied"
        result = await status(service, admin, actor.principal_id, response["operation_id"])
        assert result["applied_revision"] == 2
        now = await service.environment(actor)
        assert now["limits"]["environment_bytes"] == 5 * 1024**3
        assert now["quota_state"] == "applied"
        with pytest.raises(ServiceError, match="permission_denied"):
            await status(service, actor, actor.principal_id, response["operation_id"])


async def test_confirm_preview_ownership_revision_and_idempotence(tmp_path):
    async with managed(tmp_path / "q2.sqlite") as (_, _, service, admin, actor, *_):
        service.broker = Broker()
        service.broker.phase = "applied"
        await service.provision(actor, request_key="prepare")
        with pytest.raises(ServiceError, match="permission_denied"):
            await preview(
                service, actor, actor.principal_id, {"expected_revision": 1, "overrides": {}}
            )
        plan = await preview(
            service,
            admin,
            actor.principal_id,
            {"expected_revision": 1, "overrides": {"environment_bytes": 8 * 1024**3}},
        )
        first = await confirm(
            service, admin, actor.principal_id, plan["plan_id"], plan["plan_digest"], 1, "q"
        )
        again = await confirm(
            service, admin, actor.principal_id, plan["plan_id"], plan["plan_digest"], 1, "q"
        )
        assert first["quota_state"] == again["quota_state"] == "applied"
        assert len([a for a, _ in service.broker.calls if a == "quota_confirm"]) == 1
        with pytest.raises(ServiceError, match="key_conflict"):
            await confirm(service, admin, actor.principal_id, plan["plan_id"], "b" * 64, 1, "q")
        with pytest.raises(ServiceError, match="revision_conflict"):
            await confirm(
                service, admin, actor.principal_id, plan["plan_id"], plan["plan_digest"], 1, "other"
            )


async def test_capacity_rejection_does_not_fake_application_or_permanently_block_account(tmp_path):
    async with managed(tmp_path / "reject-quota.sqlite") as (_, _, service, admin, actor, *_):
        service.broker = Broker()
        await service.provision(actor, request_key="prepare")
        plan = await preview(
            service,
            admin,
            actor.principal_id,
            {"expected_revision": 1, "overrides": {"environment_bytes": 8 * 1024**3}},
        )
        original = service.broker.call

        async def insufficient(action, **params):
            if action == "quota_confirm":
                raise ServiceError("sandbox_limit")
            return await original(action, **params)

        service.broker.call = insufficient
        for _ in range(2):
            with pytest.raises(ServiceError, match="sandbox_limit"):
                await confirm(
                    service,
                    admin,
                    actor.principal_id,
                    plan["plan_id"],
                    plan["plan_digest"],
                    1,
                    "denied",
                )
        env = await service.environment(actor)
        assert env["limits"]["environment_bytes"] == 5 * 1024**3
        assert env["quota_state"] == "failed_validation"
        assert env["applied_revision"] == 1 and env["desired_revision"] == 2


async def test_completed_quota_history_remains_readable_after_a_later_change(tmp_path):
    async with managed(tmp_path / "history.sqlite") as (_, _, service, admin, actor, *_):
        service.broker = Broker()
        service.broker.phase = "applied"
        await service.provision(actor, request_key="prepare")
        first = await preview(
            service,
            admin,
            actor.principal_id,
            {"expected_revision": 1, "overrides": {"environment_bytes": 8 * 1024**3}},
        )
        before = await confirm(
            service, admin, actor.principal_id, first["plan_id"], first["plan_digest"], 1, "one"
        )
        second = await preview(
            service,
            admin,
            actor.principal_id,
            {"expected_revision": 2, "overrides": {"environment_bytes": 9 * 1024**3}},
        )
        await confirm(
            service, admin, actor.principal_id, second["plan_id"], second["plan_digest"], 2, "two"
        )
        old = await status(service, admin, actor.principal_id, before["operation_id"])
        assert old["operation_id"] == before["operation_id"] and old["applied_revision"] == 2
