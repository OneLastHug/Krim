"""Extension route contracts for paged account and private project memory."""

from types import SimpleNamespace

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from test_memory_tools import bind_projects, memory_environment

from krim_agent.api.extensions import mount_extensions
from krim_agent.contracts import ServiceError


async def test_paged_account_and_project_management_routes(tmp_path):
    async with memory_environment(tmp_path / "routes.sqlite", enabled=False) as (
        store,
        _,
        _,
        ext,
        _,
        scope,
    ):
        await bind_projects(store, scope)
        app = FastAPI()
        app.state.application = SimpleNamespace(extensions=ext)

        @app.middleware("http")
        async def principal(request: Request, call_next):
            request.state.principal = scope.actor
            return await call_next(request)

        @app.exception_handler(ServiceError)
        async def error(request: Request, exc: ServiceError):
            return JSONResponse({"code": exc.code}, status_code=exc.status)

        mount_extensions(app)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            for index in range(3):
                result = await client.post(
                    "/v2/me/memories",
                    json={"request_key": f"account-{index}", "body": f"coffee {index}"},
                )
                assert result.status_code == 201, result.text
            result = await client.get("/v2/me/memories/page", params={"limit": 2})
            assert result.status_code == 200, result.text
            page = result.json()
            assert len(page["items"]) == 2 and page["next_offset"] == 2
            second = (await client.get("/v2/me/memories/page", params={"offset": 2})).json()
            assert len(second["items"]) == 1 and second["next_offset"] is None
            assert len((await client.get("/v2/me/memories")).json()) == 3
            assert (
                await client.get("/v2/me/memories/page", params={"limit": 0})
            ).status_code == 422
            base = f"/v2/workspaces/{scope.workspace_id}/projects/project-one/memories"
            created = await client.post(base, json={"request_key": "project", "body": "中" * 5000})
            assert created.status_code == 201, created.text
            saved = created.json()
            memory_id = saved["id"]
            item = (await client.get(f"{base}/{memory_id}")).json()
            assert item["body"] == "中" * 5000 and item["project_id"] == "project-one"
            assert (await client.get(f"/v2/me/memories/{memory_id}")).status_code == 404
            assert len((await client.get(base)).json()["items"]) == 1
            assert (await client.get(base.replace("project-one", "project-two"))).json()[
                "items"
            ] == []
            assert (
                await client.get(f"{base.replace('project-one', 'project-two')}/{memory_id}")
            ).status_code == 404
            updated = await client.put(
                f"{base}/{memory_id}",
                json={
                    "request_key": "update",
                    "body": "changed",
                    "expected_revision": 1,
                    "expected_content_hash": saved["content_hash"],
                },
            )
            assert updated.status_code == 200, updated.text
            removal = {
                "request_key": "remove",
                "expected_revision": 2,
                "expected_content_hash": updated.json()["content_hash"],
            }
            deleted = await client.request("DELETE", f"{base}/{memory_id}", json=removal)
            assert deleted.status_code == 200 and deleted.json()["removed"]
            assert (await client.get(base)).json()["items"] == []
            assert (await client.get("/v2/me/memory-preference")).json()["enabled"] is False
