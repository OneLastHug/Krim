"""Preview composition/API with real SQLite and identity; Docker IO is injected."""

import asyncio
import hashlib
from dataclasses import replace
from pathlib import Path

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy import func, select, update

from krim_agent.accounts.service import IdentityService
from krim_agent.agent_sdk.host import CURRENT_OPERATION
from krim_agent.api.server import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import ServiceError
from krim_agent.development.configuration import (
    DevelopmentConfiguration,
    DevelopmentPreviewConfiguration,
)
from krim_agent.development.contracts import OperationIdentity
from krim_agent.development.service import DevelopmentScope, DevelopmentService
from krim_agent.runtime.contracts import ExecutionBudget
from krim_agent.sandbox.configuration import SandboxConfiguration
from krim_agent.storage.preview_models import preview_credentials, preview_records
from krim_agent.storage.project_membership_models import project_memberships
from krim_agent.tools.core import Invocation
from krim_agent.tools.pipeline import CURRENT
from tests.coding_agent.test_development_entrypoint import ShellModel
from tests.coding_agent.test_preview_storage import setup


def preview_configuration():
    return DevelopmentPreviewConfiguration(
        control_origin="https://control.example",
        control_cookie_domain="control.example",
        preview_domain="preview.example",
    )


@pytest.mark.parametrize("values", [
    {"control_origin": "http://control.example"},
    {"preview_domain": "control.example"},
    {"preview_domain": "child.control.example"},
    {"control_origin": "https://control.example/path"},
])
def test_reject_invalid_preview_configuration(values):
    with pytest.raises(ValidationError):
        DevelopmentPreviewConfiguration(**{**preview_configuration().model_dump(), **values})


@pytest.mark.parametrize("values", [
    {"allow_same_site_subdomains": True},
    {
        "allow_same_site_subdomains": True,
        "preview_domain": "example.test",
        "control_origin": "https://krim-qa.example.test",
        "control_cookie_domain": "example.test",
    },
    {"hostname_prefix": "pre.view"},
    {"hostname_prefix": "Preview"},
    {"hostname_prefix": "p" * 23},
])
def test_reject_invalid_same_site_preview_configuration(values):
    with pytest.raises(ValidationError):
        DevelopmentPreviewConfiguration(**{**preview_configuration().model_dump(), **values})


def test_same_site_subdomain_configuration_contract():
    config = DevelopmentPreviewConfiguration(
        control_origin="https://krim-qa.example.test",
        control_cookie_domain="krim-qa.example.test",
        preview_domain="example.test",
        allow_same_site_subdomains=True,
        hostname_prefix="preview",
    )
    key = hashlib.sha256(b"preview").hexdigest()[:40]
    assert config.origins().host("preview") == f"preview-{key}.example.test"
    # Legacy configurations keep the independent-site p- family untouched.
    assert preview_configuration().origins().host("preview") == f"p-{key}.preview.example"


def test_existing_development_configuration_keeps_preview_disabled(tmp_path):
    config = DevelopmentConfiguration(
        image_id="sha256:" + "a" * 64,
        broker_environments_root=tmp_path / "environments",
        private_state_root=tmp_path / "private",
    )
    assert config.preview is None


@pytest.fixture
async def mounted(tmp_path: Path, monkeypatch):
    path = tmp_path / "preview.sqlite"
    store, _, _, record, actor, grant = await setup(path)
    identities = await IdentityService.open(store)
    issued = await identities.rotate_key(actor, {}, request_key="test-token")
    token = str(issued.token)
    await identities.close()
    await store.close()
    broker = tmp_path / "broker.json"
    broker.write_text(
        SandboxConfiguration(broker_socket=tmp_path / "fixture.sock").model_dump_json()
    )
    broker.chmod(0o600)
    configuration = DevelopmentConfiguration(
        image_id="sha256:" + "a" * 64,
        broker_environments_root=tmp_path / "environments",
        private_state_root=tmp_path / "private",
        resource_policy="limited",
        preview=preview_configuration(),
    )

    async def supervisor(_self):
        return

    monkeypatch.setattr(DevelopmentService, "start_supervisor", supervisor)
    app = await open_application(
        path, settings=Settings(database=path), provider=ShellModel(),
        sandbox_configuration=SandboxConfiguration.model_validate_json(broker.read_text()),
        development_configuration=configuration,
    )
    development = app.development
    assert development is not None
    assert development.preview_store is not None
    assert development.preview_routes is not None
    assert development.previews is not None
    assert development.preview_app is not None
    assert development.preview_routes.source is development.preview_store.authority
    assert development.previews.backend is development.terminal
    observed, closed = [], []

    async def inspect(process):
        return (await development.repositories.processes.get(process.process_id)).handle

    async def observe(process, port, viewer):
        await development.preview_store.authority.registration(process, port, viewer)
        observed.append((process.process_id, port))
        return record.instance

    async def cancel(process):
        row = await development.repositories.processes.get(process.process_id)
        updated = replace(
            row, revision=row.revision + 1,
            handle=row.handle.model_copy(update={"state": "cancelled", "exit_code": 130}),
        )
        return (await development.repositories.processes.save(row, updated)).handle

    original_close = development.preview_routes.aclose

    async def close_routes():
        assert not app.store.closed
        closed.append(True)
        await original_close()

    monkeypatch.setattr(development.terminal, "inspect", inspect)
    monkeypatch.setattr(development.terminal, "cancel", cancel)
    monkeypatch.setattr(development.preview_routes, "observe", observe)
    monkeypatch.setattr(development.preview_routes, "aclose", close_routes)
    api = create_app(application=app)
    try:
        async with (
            api.router.lifespan_context(api),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api), base_url="https://control.example",
                headers={"Authorization": "Bearer " + token},
            ) as client,
        ):
            yield app, client, record, grant, observed
    finally:
        await app.close()
        assert closed == [True]
        assert app.store.closed


async def test_register_preview_replay_and_binding_conflict(mounted):
    app, client, record, _, observed = mounted
    project = record.handle.environment.project_id
    path = f"/v2/me/development/projects/{project}/previews"
    body = {"request_key": "register-preview",
            "process_id": record.process.process_id, "port": 3001}
    response = await client.post(path, json=body)
    assert response.status_code == 200, response.text
    first = response.json()
    again = await client.post(path, json=body)
    assert again.status_code == 200 and again.json() == first
    assert observed == [(record.process.process_id, 3001)]
    changed = await client.post(path, json={**body, "port": 3002})
    assert changed.status_code == 409, changed.text
    alternate_key = await client.post(path, json={**body, "request_key": "another-key"})
    assert alternate_key.status_code == 409, alternate_key.text
    cross_project = await client.post(
        "/v2/me/development/projects/unrelated/previews", json=body,
    )
    assert cross_project.status_code in {403, 404}, cross_project.text
    invalid = await client.post(path, json={**body, "port": True})
    assert invalid.status_code == 422
    async with app.store.engine.connect() as conn:
        assert await conn.scalar(select(func.count()).select_from(preview_records)) == 2
    inspected = await client.get(f"/v2/me/development/previews/{first['preview_id']}")
    assert inspected.status_code == 200 and inspected.json() == first


async def test_open_requires_same_origin_bearer_and_consumes_once(mounted):
    app, client, record, _, _ = mounted
    path = record.handle.access_path
    for headers in (
        {}, {"Origin": "https://other.example"},
        {"Origin": "https://control.example", "Sec-Fetch-Site": "cross-site"},
    ):
        denied = await client.post(path, headers=headers)
        assert denied.status_code == 403, denied.text
    duplicate = await client.post(path, headers=[
        ("Origin", "https://control.example"), ("Origin", "https://control.example"),
    ])
    assert duplicate.status_code == 403
    anonymous = await client.post(path, headers={
        "Origin": "https://control.example", "Authorization": "",
    })
    assert anonymous.status_code == 401
    async with app.store.engine.connect() as conn:
        assert await conn.scalar(select(func.count()).select_from(preview_credentials)) == 0
    opened = await client.post(path, headers={"Origin": "https://control.example"})
    assert opened.status_code == 200, opened.text
    assert opened.headers["cache-control"] == "no-store"
    assert opened.headers["referrer-policy"] == "no-referrer"
    body = opened.json()
    assert set(body) == {"action", "ticket", "expires_at", "allow_embedding"}
    assert body["allow_embedding"] is False
    assert body["ticket"] not in body["action"]
    assert app.development is not None and app.development.preview_app is not None
    preview = app.development.preview_app
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=preview)) as browser:
        response = await browser.post(body["action"], data={"ticket": body["ticket"]},
                                      headers={"Origin": "https://control.example"})
        assert response.status_code == 303, response.text
        cookie = response.headers["set-cookie"]
        assert "Secure" in cookie and "HttpOnly" in cookie and "Domain=" not in cookie
        repeated = await browser.post(body["action"], data={"ticket": body["ticket"]},
                                      headers={"Origin": "https://control.example"})
        assert repeated.status_code == 401
    on_control = await client.get("/.krim-preview/session")
    assert on_control.status_code == 404


async def test_preview_stop_fences_open_and_revoked_grant_denies_new_ticket(mounted):
    app, client, record, grant, _ = mounted
    selected = app.development
    assert selected is not None
    async with app.store.engine.begin() as conn:
        await conn.execute(update(project_memberships).where(
            project_memberships.c.project_id == record.handle.environment.project_id,
            project_memberships.c.principal_id == record.process.ownership.owner_id,
        ).values(access="none", revision=2))
    denied = await client.post(record.handle.access_path,
                               headers={"Origin": "https://control.example"})
    assert denied.status_code in {403, 404}, denied.text
    async with app.store.engine.begin() as conn:
        await conn.execute(update(project_memberships).where(
            project_memberships.c.project_id == record.handle.environment.project_id,
            project_memberships.c.principal_id == record.process.ownership.owner_id,
        ).values(access="write", revision=3))
    stopped = await client.post(f"/v2/me/development/previews/{record.handle.preview_id}/stop")
    assert stopped.status_code == 200, stopped.text
    assert stopped.json()["state"] == "stopped"
    denied = await client.post(record.handle.access_path,
                               headers={"Origin": "https://control.example"})
    assert denied.status_code == 410, denied.text


async def test_download_policy_api_checks_hosts_revision_and_invalidation(mounted, monkeypatch):
    app, client, record, grant, _ = mounted
    selected = app.development
    assert selected is not None
    invalidated = []

    async def invalidate(owner, project, *, policy_id=None):
        invalidated.append((owner, project, policy_id))

    monkeypatch.setattr(selected.downloads, "invalidate", invalidate)
    project = record.handle.environment.project_id
    path = f"/v2/me/development/projects/{project}/download-policies/packages"
    body = {"expected_revision": 0, "hosts": ["registry.npmjs.org"],
            "expires_at": grant.expires_at}
    created = await client.put(path, json=body)
    assert created.status_code == 200, created.text
    assert created.json()["revision"] == 1
    assert invalidated == [(record.handle.environment.owner_id, project, "packages")]
    replay = await client.put(path, json=body)
    assert replay.status_code == 409
    invalid = await client.put(path, json={
        **body, "expected_revision": 1, "hosts": ["127.0.0.1"],
    })
    assert invalid.status_code == 422
    revoked = await client.put(path, json={**body, "expected_revision": 1, "revoked": True})
    assert revoked.status_code == 200 and revoked.json()["revoked"] is True
    assert revoked.json()["revision"] == 2


async def test_model_media_requires_canonical_tool_result_and_current_grant(mounted, monkeypatch):
    from krim_agent.agent_sdk.host_media import HostImageMessage
    from krim_agent.contracts import Message, encoded
    from tests.coding_agent.browser_combination import png

    app, _, record, grant, _ = mounted
    selected = app.development
    assert selected is not None and app.identities is not None
    monkeypatch.setattr(selected, "configuration", selected.configuration.model_copy(
        update={"model_images": True},
    ))
    from krim_agent.contracts import RunRequest
    from krim_agent.sessions.types import TurnRequest
    from krim_agent.storage import sessions as session_sql

    actor = app.identities.resolve(record.process.ownership.owner_id)
    session_id = record.process.ownership.session_id
    from krim_agent.storage.models import sessions

    async with app.store.engine.connect() as conn:
        target = await conn.scalar(select(sessions.c.target).where(sessions.c.id == session_id))
    session = await session_sql.get_session(app.store, session_id)
    turn = await session_sql.submit(app.store, session_id, TurnRequest(
        text="media test", request_key="media-current", expected_revision=session.revision,
    ), app.settings)
    run, _ = await app.store.begin_turn_run(turn.id, RunRequest(
        text="media test", request_key="ac-turn." + turn.id,
    ), target, app.settings)
    await app.store.set_running(run.id)
    ownership = record.process.ownership.model_copy(update={"run_id": run.id})
    saved = selected.browser_artifacts.save(
        png(), record.handle.environment, ownership,
        actor.generation, record.handle.preview_id,
    )
    run_id = run.id
    attach = selected.media_for_run(run_id)
    history = [Message(role="system", content="test"),
               Message(role="user", content=encoded(saved)),
               Message(role="assistant", content="", tool_calls=[{
                   "id": "screenshot-wire", "name": "browser_screenshot",
                   "arguments": {"browser_id": "browser-fixture"},
               }]),
               Message(role="tool", call_id="screenshot-wire", content=encoded(saved))]
    assert await attach(history) == history
    await app.store.append(run_id, "tool_end", {
        "call_id": "screenshot-call", "provider_call_id": "screenshot-wire",
        "span_id": "screenshot-span", "step_index": 0, "name": "browser_screenshot",
        "status": "ok", "executor_started": True, "result_available": True,
        "value": saved, "decision": "allow",
    })
    prepared = await attach(history)
    assert isinstance(prepared[-1], HostImageMessage)
    assert prepared[-1].images[0].data == png()
    assert len(history) == 4
    async with app.store.engine.begin() as conn:
        await conn.execute(update(project_memberships).where(
            project_memberships.c.project_id == record.handle.environment.project_id,
            project_memberships.c.principal_id == record.process.ownership.owner_id,
        ).values(access="none", revision=2))
    with pytest.raises(ServiceError, match="permission_denied|not_found"):
        await attach(history)


async def test_screenshot_api_current_project_and_revoked_grant(mounted):
    from tests.coding_agent.browser_combination import png

    app, client, record, grant, _ = mounted
    selected = app.development
    assert selected is not None and app.identities is not None
    actor = app.identities.resolve(record.process.ownership.owner_id)
    data = png()
    saved = selected.browser_artifacts.save(
        data, record.handle.environment, record.process.ownership,
        actor.generation, record.handle.preview_id,
    )
    path = (f"/v2/me/development/projects/{saved['project_id']}/browser-artifacts/"
            f"{saved['run_id']}/{saved['reference']}")
    response = await client.get(path)
    assert response.status_code == 200 and response.content == data
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["content-type"] == "image/png"
    unrelated = await client.get(path.replace(saved["project_id"], "another-project"))
    assert unrelated.status_code in {403, 404}
    async with app.store.engine.begin() as conn:
        await conn.execute(update(project_memberships).where(
            project_memberships.c.project_id == record.handle.environment.project_id,
            project_memberships.c.principal_id == record.process.ownership.owner_id,
        ).values(access="none", revision=2))
    revoked = await client.get(path)
    assert revoked.status_code in {403, 404}


async def test_browser_tool_records_private_screenshot_and_cleanup(mounted, monkeypatch):
    import base64
    import hashlib
    from types import SimpleNamespace

    from tests.coding_agent.browser_combination import png

    app, _, record, grant, _ = mounted
    selected = app.development
    assert selected is not None and app.identities is not None
    actor = app.identities.resolve(record.process.ownership.owner_id)
    data = png()
    calls = []

    class Browsers:
        async def open(self, viewer, environment, ownership, preview_id):
            assert viewer == actor and environment == record.handle.environment
            calls.append("open")
            return {"browser_id": "browser-fixture", "preview_id": preview_id, "state": "ready"}

        def _bound(self, *args):
            return SimpleNamespace(preview_id=record.handle.preview_id)

        async def action(self, browser_id, viewer, environment, ownership, body):
            calls.append(body["op"])
            if body["op"] == "screenshot":
                return {"media_type": "image/png", "size": len(data),
                        "sha256": hashlib.sha256(data).hexdigest(),
                        "data_base64": base64.b64encode(data).decode()}
            return {"done": True}

        async def close_session(self, *args):
            calls.append("close-session")

        async def close_run(self, run_id):
            calls.append("close-run")
            raise ServiceError("browser_worker_failed", 503)

        async def close(self):
            calls.append("close")

    monkeypatch.setattr(selected, "browsers", Browsers())
    registry = selected.registry()

    async def invoke(name, values):
        spec = registry.get(name)
        assert spec.risk == "write"
        invocation = Invocation(record.process.ownership.run_id, "call", "span", 1,
                                name, "{}", "a" * 64)
        scope = DevelopmentScope(
            actor, grant.workspace_id, record.handle.environment, record.process.ownership,
            selected, lambda: None, ExecutionBudget(asyncio.get_running_loop().time() + 30),
            invocation,
        )
        token = CURRENT.set(scope)
        try:
            return await selected.execute(invocation, spec.parameters.model_validate(values), None)
        finally:
            CURRENT.reset(token)

    opened = await invoke("browser_open", {"preview_id": record.handle.preview_id})
    await invoke("browser_action", {"browser_id": opened["browser_id"],
                                        "action": "click", "selector": "#button"})
    saved = await invoke("browser_screenshot", {"browser_id": opened["browser_id"]})
    assert "data_base64" not in saved and "ticket" not in saved
    found, _ = await selected.read_browser_artifact(actor, saved["project_id"],
                                                  saved["run_id"], saved["reference"])
    assert found == data
    await invoke("browser_close", {"browser_id": opened["browser_id"]})

    async def stop_run(ownership):
        calls.append("terminal-stop")

    monkeypatch.setattr(selected.terminal, "stop_run", stop_run)
    with pytest.raises(ServiceError, match="browser_worker_failed"):
        await selected.cleanup_run(record.process.ownership.run_id)
    assert calls == ["open", "click", "screenshot", "close-session", "close-run", "terminal-stop"]


async def test_preview_tools_are_project_scoped_and_return_no_ticket(mounted):
    app, _, record, grant, _ = mounted
    selected = app.development
    assert selected is not None and app.identities is not None
    actor = app.identities.resolve(record.process.ownership.owner_id)
    registry = selected.registry()
    assert registry.get("preview_register").risk == "write"
    assert registry.get("preview_status").risk == "read"
    assert registry.get("preview_stop").risk == "write"

    async def invoke(name, values, *, cross_project=False, stale_generation=False):
        spec = registry.get(name)
        invocation = Invocation(
            record.process.ownership.run_id, "preview-call", "preview-span", 1,
            name, "{}", "a" * 64,
        )
        environment = record.handle.environment
        if cross_project:
            environment = environment.model_copy(update={"project_id": "another-project"})
        if stale_generation:
            environment = environment.model_copy(update={"generation": environment.generation + 1})
        scope = DevelopmentScope(
            actor, grant.workspace_id, environment, record.process.ownership, selected,
            lambda: None, ExecutionBudget(asyncio.get_running_loop().time() + 30), invocation,
        )
        identity = OperationIdentity(
            operation_id="preview-tool-operation", call_id=invocation.call_id,
            environment=environment, ownership=record.process.ownership, request_digest="b" * 64,
        )
        scope_token = CURRENT.set(scope)
        operation_token = CURRENT_OPERATION.set(identity)
        try:
            return await selected.execute(invocation, spec.parameters.model_validate(values), None)
        finally:
            CURRENT_OPERATION.reset(operation_token)
            CURRENT.reset(scope_token)

    with pytest.raises(ServiceError, match="permission_denied"):
        await invoke("preview_register", {
            "process_id": record.process.process_id, "port": 3003,
        }, stale_generation=True)
    result = await invoke("preview_register", {
        "process_id": record.process.process_id, "port": 3003,
    })
    assert "ticket" not in result and result["state"] == "pending"
    status = await invoke("preview_status", {"preview_id": result["preview_id"]})
    assert status == result
    with pytest.raises(ServiceError, match="permission_denied"):
        await invoke("preview_status", {"preview_id": result["preview_id"]},
                     cross_project=True)
    stopped = await invoke("preview_stop", {"preview_id": result["preview_id"]})
    assert stopped["state"] == "stopped" and "ticket" not in stopped
