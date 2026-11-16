"""Management and private-tool wiring for Git plus browser operation shapes."""

from __future__ import annotations

import hashlib
import json
import shutil
import time
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy import select

from krim_agent.api.development import GitGrantBody, SecretBody
from krim_agent.api.server import create_app
from krim_agent.config import Settings
from krim_agent.development.configuration import (
    DevelopmentConfiguration,
    DevelopmentGitConfiguration,
)
from krim_agent.development.service import BrowserActionInput, DevelopmentService
from krim_agent.project_access.git_execution import GitExecutionRequest
from krim_agent.storage.development_models import project_access_records
from tests.coding_agent.test_development_git_operations import REF, Fixture

WIRE_REPO = "https://git.example/team/wiring.git"


@pytest.mark.asyncio
async def test_git_management_tools_and_revocation_are_wired(tmp_path: Path, monkeypatch) -> None:
    fixture = await Fixture().open(tmp_path)
    cache_root = Path("/tmp") / ("kg-" + hashlib.sha256(str(tmp_path).encode()).hexdigest()[:12])
    configuration = DevelopmentConfiguration(
        image_id=fixture.record.image_id,
        broker_environments_root=tmp_path / "environments",
        private_state_root=tmp_path / "development-private",
        git=DevelopmentGitConfiguration(
            repository_root=tmp_path / "git-repositories",
            stage_root=tmp_path / "git-stages",
            cache_root=cache_root,
        ),
    )
    DevelopmentService.prepare_private(configuration)
    development = DevelopmentService(
        fixture.store,
        fixture.identities,
        fixture.sandbox,
        configuration,
        vault=fixture.vault,
    )
    try:
        registry = development.registry()
        names = {
            "dev_git_clone",
            "dev_git_diff",
            "dev_git_tree",
            "dev_git_commit",
            "dev_git_push",
        }
        assert names <= set(registry._items)
        for name in names:
            spec = registry.get(name)
            assert spec.risk == "write" and spec.capability == "write"
            assert spec.scoped_executor == development.execute
        assert registry.get("dev_git_tree").parameters is not GitExecutionRequest

        async def no_supervisor(_self):
            return None

        monkeypatch.setattr(DevelopmentService, "start_supervisor", no_supervisor)
        issued = await fixture.identities.rotate_key(
            fixture.actor,
            {},
            request_key="git-wiring-key",
        )
        app_state = SimpleNamespace(
            store=fixture.store,
            identities=fixture.identities,
            development=development,
            settings=Settings(database=tmp_path / "application.sqlite"),
            access_policy=None,
        )
        api = create_app(application=app_state)
        project_id = fixture.record.environment.project_id
        route = f"/v2/me/development/projects/{project_id}/git-grants"
        secret_route = f"/v2/me/development/projects/{project_id}/git-secrets/git-credential"
        expiry = time.time() + 300
        secret = "token-" + hashlib.sha256(b"credential-fixture").hexdigest()
        calls: list[tuple[str, str]] = []
        assert development.git is not None
        invalidate = development.git.invalidate_project

        async def record_invalidation(owner_id: str, selected_project: str) -> None:
            calls.append((owner_id, selected_project))
            await invalidate(owner_id, selected_project)

        monkeypatch.setattr(development.git, "invalidate_project", record_invalidation)
        async with (
            api.router.lifespan_context(api),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api),
                base_url="http://127.0.0.1",
                headers={"Authorization": "Bearer " + str(issued.token)},
            ) as client,
        ):
            grant_body = {
                "expected_revision": 0,
                "repository": WIRE_REPO,
                "refs": [REF],
                "actions": ["git.diff", "git.commit"],
                "identity": {"name": "Fixture Author", "email": "fixture@example.invalid"},
                "expires_at": expiry,
                "credential_secret_id": "git-credential",
            }
            denied = await client.put(
                route, json=grant_body, headers={"Origin": "https://evil.invalid"}
            )
            assert denied.status_code == 403
            unauthenticated = await client.put(
                route, json=grant_body, headers={"Authorization": ""}
            )
            assert unauthenticated.status_code == 401
            granted = await client.put(route, json=grant_body)
            assert granted.status_code == 200, granted.text
            assert granted.json() == {"repository": WIRE_REPO, "revision": 1, "revoked": False}

            project_grant = await client.put(
                f"/v2/me/development/projects/{project_id}/grant",
                json={
                    "expected_revision": 1,
                    "expires_at": expiry,
                    "requires_approval": False,
                    "actions": ["git.diff", "git.commit"],
                },
            )
            assert project_grant.status_code == 200, project_grant.text
            assert set(project_grant.json()["actions"]) == {"git.diff", "git.commit"}

            invalid_grant = await client.put(route, json={**grant_body, "unexpected": "field"})
            assert invalid_grant.status_code == 422
            invalid_refs = await client.put(route, json={**grant_body, "refs": [REF, REF]})
            assert invalid_refs.status_code == 422

            secret_body = {
                "expected_revision": 0,
                "expires_at": expiry,
                "value": secret,
            }
            stored = await client.put(secret_route, json=secret_body)
            assert stored.status_code == 200, stored.text
            assert secret not in stored.text
            assert "value" not in SecretBody.model_validate(secret_body).model_dump()
            assert secret not in repr(SecretBody.model_validate(secret_body))
            assert calls == [(fixture.actor.principal_id, project_id)] * 3

            invalid_secret = await client.put(
                secret_route,
                json={**secret_body, "value": "token with spaces"},
            )
            assert invalid_secret.status_code == 422
            async with fixture.store.lock, fixture.store.engine.connect() as connection:
                persisted = await connection.scalar(
                    select(project_access_records.c.document).where(
                        project_access_records.c.owner_id == fixture.actor.principal_id,
                        project_access_records.c.project_id == project_id,
                        project_access_records.c.kind == "secret",
                        project_access_records.c.resource_id == "git-credential",
                    )
                )
            assert persisted is not None and secret not in json.dumps(persisted)

        async def no_stop(_environment):
            return fixture.record.environment

        async def no_network_invalidate(_owner: str, _project: str, *, policy_id=None):
            return None

        monkeypatch.setattr(development.environments, "stop", no_stop)
        monkeypatch.setattr(development.downloads, "invalidate", no_network_invalidate)
        await development.revoke_owner(fixture.actor.principal_id)
        assert fixture.actor.principal_id in development.git._revoked_owners
    finally:
        await development.close()
        await fixture.close()
        shutil.rmtree(cache_root, ignore_errors=True)


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ({"action": "resize", "preset": "desktop"}, {"preset": "desktop"}),
        (
            {"action": "download", "path": "/downloads/file.bin", "cookie_mode": "preview_only"},
            {"path": "/downloads/file.bin", "cookie_mode": "preview_only"},
        ),
    ],
)
def test_browser_resize_and_download_have_closed_argument_shapes(values, expected) -> None:
    request = BrowserActionInput(browser_id="browser-fixture", **values)
    assert (
        request.model_dump(
            exclude_none=True,
            exclude={"browser_id", "action"},
        )
        == expected
    )


def test_git_and_secret_management_bodies_validate_and_redact() -> None:
    grant = GitGrantBody.model_validate(
        {
            "expected_revision": 0,
            "repository": WIRE_REPO,
            "refs": [REF],
            "actions": ["git.clone", "git.push"],
            "identity": {"name": "Author", "email": "author@example.invalid"},
            "expires_at": time.time() + 60,
            "network_policy_ref": "policy-1",
        }
    )
    assert grant.repository == WIRE_REPO and grant.refs == [REF]
    with pytest.raises(ValidationError):
        GitGrantBody.model_validate(
            {
                "expected_revision": 0,
                "repository": "http://git.example/team/repo.git",
                "refs": [REF],
                "actions": ["git.diff"],
                "identity": {"name": "Author", "email": "author@example.invalid"},
                "expires_at": time.time() + 60,
            }
        )
    with pytest.raises(ValidationError):
        BrowserActionInput(browser_id="browser-fixture", action="resize")
    with pytest.raises(ValidationError):
        BrowserActionInput(
            browser_id="browser-fixture",
            action="resize",
            preset="mobile",
            selector="body",
        )
    with pytest.raises(ValidationError):
        BrowserActionInput(browser_id="browser-fixture", action="download", path="/file")
    with pytest.raises(ValidationError):
        BrowserActionInput(
            browser_id="browser-fixture",
            action="download",
            path="/file",
            cookie_mode="context",
            selector="body",
        )
