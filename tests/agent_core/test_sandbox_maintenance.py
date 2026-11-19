"""恢复/GC控制面授权、确认、原请求幂等和逐文件授权复核。"""

import uuid

import pytest
from sqlalchemy import select
from test_sandbox_control import managed

from krim_agent.contracts import ServiceError
from krim_agent.sandbox.management import project_action
from krim_agent.storage.sandbox_models import sandbox_projects


class Rpc:
    def __init__(self):
        self.calls = []
        self.step = 0
        self.total = 2
        self.operation = None
        self.callback = None

    async def call(self, action, **params):
        self.calls.append((action, params))
        if action in {"provision_environment", "provision_project"}:
            return {"status": "ready"}
        if action == "restore_preview":
            return {
                "plan_id": str(uuid.uuid4()),
                "plan_digest": "a" * 64,
                "generation": 0,
                "changes": [{"step_id": "step"}],
            }
        if action == "restore_confirm":
            self.operation = params["operation_id"]
            self.step = max(1, self.step)
        elif action == "restore_resume":
            self.step += 1
        elif action == "restore_abandon":
            self.step = self.total
        if self.callback is not None:
            await self.callback(action)
        return {
            "operation_id": self.operation,
            "status": "completed" if self.step >= self.total else "paused",
            "reason": None if self.step >= self.total else "batch_limit",
            "generation": 1,
            "progress": {"total": self.total, "verified": self.step},
            "steps": [],
        }


async def prepare(service, actor):
    rpc = Rpc()
    service.broker = rpc
    p = await service.create_project(
        actor,
        {"title": "恢复项目", "workspace_id": actor.memberships[0].workspace_id},
        request_key="project",
    )
    await service.provision(actor, project_id=p["project_id"], request_key="prepare")
    return rpc, p["project_id"]


async def test_confirm_is_idempotent_and_does_not_reexecute_file_batches(tmp_path):
    async with managed(tmp_path / "m.sqlite") as (_, _, service, _, actor, *_):
        rpc, p = await prepare(service, actor)
        args = {"plan_id": str(uuid.uuid4()), "plan_digest": "a" * 64, "selected": ["step"]}
        result = await project_action(
            service, actor, p, "restore_confirm", args, request_key="restore", expected_generation=0
        )
        assert result["status"] == "completed" and rpc.step == 2
        repeated = await project_action(
            service, actor, p, "restore_confirm", args, request_key="restore", expected_generation=0
        )
        assert repeated == result and rpc.step == 2
        assert (await service.project(actor, p))["status"] == "ready"
        with pytest.raises(ServiceError, match="key_conflict"):
            await project_action(
                service,
                actor,
                p,
                "restore_confirm",
                {**args, "selected": ["different"]},
                request_key="restore",
                expected_generation=0,
            )


async def test_authorization_is_checked_between_file_steps_and_result_is_settled(tmp_path):
    async with managed(tmp_path / "auth.sqlite") as (store, identities, service, admin, actor, *_):
        rpc, p = await prepare(service, actor)

        async def revoke(action):
            if action == "restore_confirm":
                current = await identities.account(admin, actor.principal_id)
                await identities.update_account(
                    admin,
                    actor.principal_id,
                    {"expected_revision": current.revision, "status": "disabled"},
                    request_key="disable",
                )

        rpc.callback = revoke
        with pytest.raises(ServiceError, match="unauthorized"):
            await project_action(
                service,
                actor,
                p,
                "restore_confirm",
                {"plan_id": str(uuid.uuid4()), "plan_digest": "a" * 64, "selected": ["step"]},
                request_key="restore",
                expected_generation=0,
            )
        assert rpc.step == 1 and not any(a == "restore_resume" for a, _ in rpc.calls)
        async with store.engine.connect() as conn:
            row = (
                (
                    await conn.execute(
                        select(sandbox_projects).where(sandbox_projects.c.project_id == p)
                    )
                )
                .mappings()
                .one()
            )
            assert row["status"] == "blocked" and row["generation"] == 1


async def test_cross_account_maintenance_never_reaches_broker(tmp_path):
    async with managed(tmp_path / "private.sqlite") as (_, _, service, admin, actor, *_):
        rpc, p = await prepare(service, actor)
        before = len(rpc.calls)
        with pytest.raises(ServiceError, match="not_found"):
            await project_action(
                service, admin, p, "restore_preview", {"checkpoint_id": str(uuid.uuid4())}
            )
        assert len(rpc.calls) == before


async def test_partial_batch_stays_blocked_and_original_request_replays_only_result(tmp_path):
    async with managed(tmp_path / "batch.sqlite") as (_, _, service, _, actor, *_):
        rpc, p = await prepare(service, actor)
        rpc.total = 10
        response = await project_action(
            service,
            actor,
            p,
            "restore_confirm",
            {"plan_id": str(uuid.uuid4()), "plan_digest": "a" * 64, "selected": ["step"]},
            request_key="restore",
            expected_generation=0,
        )
        assert response["status"] == "paused" and rpc.step == 8
        assert (await service.project(actor, p))["status"] == "blocked"
        args = {"operation_id": rpc.operation}
        result = await project_action(
            service, actor, p, "restore_resume", args, request_key="continue", expected_generation=1
        )
        assert result["status"] == "completed" and rpc.step == 10
        count = len(rpc.calls)
        assert (
            await project_action(
                service,
                actor,
                p,
                "restore_resume",
                args,
                request_key="continue",
                expected_generation=1,
            )
            == result
        )
        assert len(rpc.calls) == count


async def test_rejected_restore_plan_does_not_leave_a_fake_active_writer(tmp_path):
    async with managed(tmp_path / "reject.sqlite") as (_, _, service, _, actor, *_):
        rpc, p = await prepare(service, actor)
        original = rpc.call

        async def missing(action, **arguments):
            if action == "restore_confirm":
                raise ServiceError("not_found", 404)
            return await original(action, **arguments)

        rpc.call = missing
        args = {"plan_id": str(uuid.uuid4()), "plan_digest": "a" * 64, "selected": ["step"]}
        for _ in range(2):
            with pytest.raises(ServiceError, match="not_found"):
                await project_action(
                    service,
                    actor,
                    p,
                    "restore_confirm",
                    args,
                    request_key="missing-plan",
                    expected_generation=0,
                )
        current = await service.project(actor, p)
        assert current["status"] == "ready" and current["generation"] == 0
