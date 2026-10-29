"""自动化服务由Application持有，先停止派发再关闭评测与运行资源。"""

import asyncio

import httpx
import pytest
from pydantic import SecretStr

from krim_agent.api.server import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import ServiceError
from krim_agent.security import AccessPolicy, Credential, Membership, PrincipalContext


async def test_automation_is_opt_in_and_requires_product_database(tmp_path):
    async with await open_application(tmp_path / "disabled.sqlite") as application:
        assert application.evaluation_automation is None
    path = tmp_path / "invalid.sqlite"
    with pytest.raises(ServiceError) as error:
        await open_application(path, settings=Settings(evaluation_automation_enabled=True))
    assert error.value.code == "invalid_input"
    assert not path.exists()


async def test_application_owns_automation_order_and_http_only_borrows(tmp_path, monkeypatch):
    application = await open_application(
        tmp_path / "owned.sqlite",
        settings=Settings(product_eval_enabled=True, evaluation_automation_enabled=True),
    )
    automation = application.evaluation_automation
    evaluation = application.product_evaluations
    assert automation is not None and evaluation is not None
    assert automation._task is not None and not automation._task.done()
    order = []
    close_automation, close_evaluation = automation.close, evaluation.close

    async def stop_dispatch():
        await close_automation()
        order.append("automation")

    async def stop_evaluation():
        assert automation._task.done()
        order.append("evaluation")
        await close_evaluation()

    monkeypatch.setattr(automation, "close", stop_dispatch)
    monkeypatch.setattr(evaluation, "close", stop_evaluation)
    try:
        api = create_app(application=application)
        async with (
            api.router.lifespan_context(api),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api), base_url="http://localhost"
            ) as client,
        ):
            health = await client.get("/healthz")
            assert health.json()["evaluation_automation_enabled"] is True
            response = await client.get("/v4/workspaces/local/eval/automation/status")
            assert response.status_code == 200
            assert response.json()["automation"]["running"] is True
            assert response.json()["capture"]["enabled"] is False
        assert not automation._task.done()
        await asyncio.gather(application.close(), application.close())
        assert order == ["automation", "evaluation"]
        assert automation._task.done()
        assert application.store.closed and evaluation.repository.closed
    finally:
        await application.close()


async def test_automation_directory_uses_host_principals_without_credentials(tmp_path):
    policy = AccessPolicy(
        credentials=(
            Credential(
                principal_id="operator",
                token=SecretStr("local-fixture-token"),
                generation=7,
                memberships=(Membership(workspace_id="alpha", role="admin"),),
            ),
        )
    )
    async with await open_application(
        tmp_path / "directory.sqlite",
        settings=Settings(product_eval_enabled=True, evaluation_automation_enabled=True),
        access_policy=policy,
    ) as application:
        automation = application.evaluation_automation
        assert automation is not None
        actor = PrincipalContext("operator", 7, policy.credentials[0].memberships)
        assert (await automation.status(actor, "alpha"))["capture"]["enabled"] is False
        assert tuple(automation._actors.values()) == (actor,)
        assert "local-fixture-token" not in repr(automation._actors)
        with pytest.raises(ServiceError) as changed_generation:
            await automation.status(PrincipalContext("operator", 8, actor.memberships), "alpha")
        assert changed_generation.value.code == "permission_denied"
