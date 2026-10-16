from pathlib import Path

import httpx
from pydantic import SecretStr

import krim_agent
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
    directories_script = await api.get("/ui/directory-changes-ui.js")
    assert directories_script.status_code == 200
    assert (
        "DirectoryChangesUI" in directories_script.text
        and "directory-state" in directories_script.text
    )
    tree_script = await api.get("/ui/workspace-trees-ui.js")
    assert tree_script.status_code == 200 and "WorkspaceTreesUI" in tree_script.text
    assert "tree-plans" in tree_script.text and "textContent" in tree_script.text


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
        checkpoint_script = await client.get("/ui/native-checkpoint-ui.js")
        assert checkpoint_script.status_code == 200
        assert (
            "native-restores" in checkpoint_script.text and "textContent" in checkpoint_script.text
        )
        assert (
            await client.get("/v1/agents/example/workspace/native-checkpoints")
        ).status_code == 401
        directory_script = await client.get("/ui/directory-checkpoint-ui.js")
        assert directory_script.status_code == 200
        assert directory_script.headers["cache-control"] == "no-cache"
        assert "script-src 'self'" in directory_script.headers["content-security-policy"]
        assert (await client.head("/ui/directory-checkpoint-ui.js")).content == b""
        assert (await client.post("/ui/directory-checkpoint-ui.js")).status_code == 401
        assert (
            await client.get("/v1/agents/example/workspace/directory-checkpoints")
        ).status_code == 401
        assert (await client.get("/_test/directory-checkpoint-agent")).status_code == 401
        for name in (
            "directory-restore-contract",
            "directory-restore-recovery",
            "directory-restore-ui",
            "missing-directory-rollback-contract",
            "missing-directory-rollback-recovery",
            "missing-directory-rollback-ui",
        ):
            asset = await client.get(f"/ui/{name}.js")
            assert asset.status_code == 200
            assert asset.headers["cache-control"] == "no-cache"
            assert "script-src 'self'" in asset.headers["content-security-policy"]
            assert (await client.head(f"/ui/{name}.js")).content == b""
            assert (await client.post(f"/ui/{name}.js")).status_code == 401
        assert (
            await client.get("/v1/agents/example/workspace/directory-restores")
        ).status_code == 401
        assert (await client.get("/_test/directory-restore-agent")).status_code == 401
        change_script = await client.get("/ui/workspace-changes-ui.js")
        assert change_script.status_code == 200 and "WorkspaceChangesUI" in change_script.text
        command_script = await client.get("/ui/commands-ui.js")
        assert command_script.status_code == 200 and "CommandsUI" in command_script.text
        tree_script = await client.get("/ui/workspace-trees-ui.js")
        assert tree_script.status_code == 200 and "WorkspaceTreesUI" in tree_script.text
        directories_script = await client.get("/ui/directory-changes-ui.js")
        assert (
            directories_script.status_code == 200
            and "DirectoryChangesUI" in directories_script.text
        )
        assert command_script.headers["x-content-type-options"] == "nosniff"
        moves_script = await client.get("/ui/tree-moves-ui.js")
        assert moves_script.status_code == 200 and "TreeMovesUI" in moves_script.text
        assert moves_script.headers["x-content-type-options"] == "nosniff"
        assert "script-src 'self'" in moves_script.headers["content-security-policy"]
        head = await client.head("/ui/tree-moves-ui.js")
        assert head.status_code == 200 and head.content == b""
        assert (await client.post("/ui/tree-moves-ui.js")).status_code == 401
        exchange_script = await client.get("/ui/tree-exchanges-ui.js")
        assert exchange_script.status_code == 200 and "TreeExchangesUI" in exchange_script.text
        assert exchange_script.headers["x-content-type-options"] == "nosniff"
        assert "script-src 'self'" in exchange_script.headers["content-security-policy"]
        assert (await client.head("/ui/tree-exchanges-ui.js")).content == b""
        assert (await client.post("/ui/tree-exchanges-ui.js")).status_code == 401
        root_script = await client.get("/ui/generated-tree-undos-ui.js")
        assert root_script.status_code == 200 and "GeneratedTreeUndosUI" in root_script.text
        assert root_script.headers["x-content-type-options"] == "nosniff"
        assert "script-src 'self'" in root_script.headers["content-security-policy"]
        assert (await client.head("/ui/generated-tree-undos-ui.js")).content == b""
        assert (await client.post("/ui/generated-tree-undos-ui.js")).status_code == 401
        assert (
            await client.get(
                "/v1/agents/example/workspace/generated-tree-undos/query?kind=proposals"
            )
        ).status_code == 401
        assert (
            await client.get("/v1/agents/example/workspace/tree-moves/query?kind=proposals")
        ).status_code == 401
        assert (await client.get("/v1/agents/example/commands")).status_code == 401
        assert (
            await client.get("/v1/agents/example/workspace/tree-plans/by-key?request_key=original")
        ).status_code == 401
        authorized = await client.get(
            "/v1/agents/catalog", headers={"Authorization": "Bearer ui-key"}
        )
        assert authorized.status_code == 200
    await database.close()


def test_frontend_sources_and_built_assets_exist():
    root = Path(__file__).resolve().parents[1] / "frontend"
    package = root / "package.json"
    assert package.exists() and (root / "package-lock.json").exists()
    assert (root / "src/app.ts").exists() and (root / "src/api.ts").exists()
    built = Path(krim_agent.__file__).resolve().parent / "web"
    for name in (
        "index.html",
        "app.js",
        "directory-changes-ui.js",
        "workspace-trees-ui.js",
        "directory-checkpoint-ui.js",
    ):
        assert (built / name).is_file()
