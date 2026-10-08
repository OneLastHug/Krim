from pathlib import Path

import httpx
from pydantic import SecretStr

from krim_agent.api import create_app
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry


async def test_static_browser_agent_app_assets_are_same_origin_and_safe(api):
    root = await api.get("/")
    assert root.status_code == 307 and root.headers["location"] == "/ui/"
    page = await api.get("/ui/")
    assert page.status_code == 200 and "Krim Agent" in page.text
    assert page.headers["content-security-policy"].startswith("default-src 'self'")
    assert page.headers["x-content-type-options"] == "nosniff"
    script = await api.get("/ui/app.js")
    api_script = await api.get("/ui/api.js")
    css = await api.get("/ui/styles.css")
    assert script.status_code == api_script.status_code == css.status_code == 200
    assert "consumeEvents" in api_script.text and "textContent" in script.text
    assert "database" not in script.text.lower()
    assert (await api.get("/ui/../pyproject.toml")).status_code in {404, 400}
    host = await api.get("/ui/host.html")
    host_script = await api.get("/ui/host-shell.js")
    host_sdk = await api.get("/ui/host.js")
    assert host.status_code == host_script.status_code == host_sdk.status_code == 200
    assert "iframe" in host.text and "HostClient" in host_sdk.text
    locale_script = await api.get("/ui/i18n.js")
    skills_script = await api.get("/ui/skills-ui.js")
    assert locale_script.status_code == 200
    assert skills_script.status_code == 200
    assert locale_script.headers["x-content-type-options"] == "nosniff"
    assert "textContent" in locale_script.text and "/v1/locales" in locale_script.text
    assert "skill/inject" in skills_script.text and "textContent" in skills_script.text
    changes_script = await api.get("/ui/change-list-ui.js")
    assert changes_script.status_code == 200
    assert "textContent" in changes_script.text and "changeList/sync" in changes_script.text


async def test_static_assets_are_public_but_business_api_still_requires_bearer_key(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'ui-auth.sqlite'}")
    await database.initialize()
    runtime = ChatRuntime(
        KrimRepository(database),
        FakeProvider(delay=0),
        builtin_registry(),
        Settings(api_key=SecretStr("ui-key")),
    )
    app = create_app(runtime=runtime)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as client,
    ):
        assert (await client.get("/ui/")).status_code == 200
        assert (await client.get("/v1/topics")).status_code == 401
        assert (await client.get("/v1/locales")).status_code == 401
        assert (await client.get("/ui/i18n.js")).status_code == 200
        assert (await client.get("/ui/change-list-ui.js")).status_code == 200
        assert (await client.get("/ui/workspace-ui.js")).status_code == 200
        authorized = await client.get(
            "/v1/agents/catalog", headers={"Authorization": "Bearer ui-key"}
        )
        assert authorized.status_code == 200
    await database.close()


def test_frontend_sources_and_built_assets_exist():
    root = Path("frontend")
    package = root / "package.json"
    assert package.exists() and (root / "package-lock.json").exists()
    assert (root / "src/app.ts").exists() and (root / "src/api.ts").exists()
    assert (Path("src/krim_agent/web") / "index.html").exists()
    assert (Path("src/krim_agent/web") / "app.js").exists()
