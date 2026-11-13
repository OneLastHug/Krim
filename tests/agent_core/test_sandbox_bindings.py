"""项目绑定版本、会话空闲门、权限及旧schema8升级。"""

import base64
import hashlib
import sqlite3
import uuid

import pytest
from sqlalchemy import update
from test_sandbox_control import managed

from krim_agent.config import Settings
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import ServiceError
from krim_agent.sandbox.bindings import bind_project, read_binding
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.models import turns
from krim_agent.storage.extension_models import EXTENSION_TABLES
from krim_agent.storage.sandbox_models import sandbox_projects
from krim_agent.storage.sqlite import SCHEMA_VERSION, SQLiteStore


async def new_session(store, actor):
    workspace = actor.memberships[0].workspace_id
    return await session_sql.create_session(
        store,
        workspace,
        actor.principal_id,
        SessionCreate(request_key=str(uuid.uuid4())),
        {"key": "demo"},
        ContextPolicy(),
        Settings(),
    )


def sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


async def new_project(service, actor):
    return await service.create_project(
        actor,
        {"workspace_id": actor.memberships[0].workspace_id, "title": "项目"},
        request_key=str(uuid.uuid4()),
    )


async def test_bind_unbind_replay_and_session_revision(tmp_path):
    async with managed(tmp_path / "bind.sqlite") as (store, _, service, _, actor, *_):
        session = await new_session(store, actor)
        project = await new_project(service, actor)
        initial = await read_binding(service, actor, session.id)
        assert initial["project_id"] is None and initial["revision"] == 0
        body = {
            "project_id": project["project_id"],
            "expected_revision": 0,
            "expected_session_revision": 0,
        }
        selected = await bind_project(service, actor, session.id, body, request_key="bind")
        assert selected["revision"] == 1 and selected["session_revision"] == 1
        assert await bind_project(service, actor, session.id, body, request_key="bind") == selected
        assert (await read_binding(service, actor, session.id))["project_id"] == project[
            "project_id"
        ]
        with pytest.raises(ServiceError, match="revision_conflict"):
            await bind_project(service, actor, session.id, body, request_key="stale")
        with pytest.raises(ServiceError, match="key_conflict"):
            await bind_project(
                service, actor, session.id, {**body, "project_id": None}, request_key="bind"
            )
        cleared = await bind_project(
            service,
            actor,
            session.id,
            {
                "project_id": None,
                "expected_revision": 1,
                "expected_session_revision": 1,
            },
            request_key="unbind",
        )
        assert cleared["revision"] == 2 and cleared["session_revision"] == 2
        assert cleared["project_id"] is None


async def test_other_actors_and_workspaces_cannot_bind(tmp_path):
    async with managed(tmp_path / "private.sqlite") as (store, _, service, admin, member, *_):
        session = await new_session(store, member)
        private = await new_project(service, admin)
        with pytest.raises(ServiceError, match="not_found"):
            await read_binding(service, admin, session.id)
        with pytest.raises(ServiceError, match="not_found"):
            await bind_project(
                service,
                member,
                session.id,
                {
                    "project_id": private["project_id"],
                    "expected_revision": 0,
                    "expected_session_revision": 0,
                },
                request_key="foreign",
            )
        own = await new_project(service, member)
        await store._write(
            lambda conn: conn.execute(
                update(sandbox_projects)
                .where(sandbox_projects.c.project_id == own["project_id"])
                .values(workspace_id="other-workspace")
            )
        )
        with pytest.raises(ServiceError, match="not_found"):
            await bind_project(
                service,
                member,
                session.id,
                {
                    "project_id": own["project_id"],
                    "expected_revision": 0,
                    "expected_session_revision": 0,
                },
                request_key="other-workspace",
            )


@pytest.mark.parametrize("status", ["queued", "running"])
async def test_queued_running_and_awaiting_turns_prevent_binding_changes(tmp_path, status):
    async with managed(tmp_path / "busy.sqlite") as (store, _, service, _, actor, *_):
        session = await new_session(store, actor)
        project = await new_project(service, actor)
        turn = await session_sql.submit(
            store,
            session.id,
            TurnRequest(request_key="turn", text="等待或运行中的任务", expected_revision=0),
            Settings(),
        )
        await store._write(
            lambda conn: conn.execute(
                update(turns).where(turns.c.id == turn.id).values(status=status)
            )
        )
        current = await read_binding(service, actor, session.id)
        with pytest.raises(ServiceError, match="workspace_busy"):
            await bind_project(
                service,
                actor,
                session.id,
                {
                    "project_id": project["project_id"],
                    "expected_revision": 0,
                    "expected_session_revision": current["session_revision"],
                },
                request_key="busy",
            )


async def test_changed_binding_invalidates_stale_turn_submission(tmp_path):
    async with managed(tmp_path / "order.sqlite") as (store, _, service, _, actor, *_):
        session = await new_session(store, actor)
        project = await new_project(service, actor)
        selected = await bind_project(
            service,
            actor,
            session.id,
            {
                "project_id": project["project_id"],
                "expected_revision": 0,
                "expected_session_revision": 0,
            },
            request_key="binding",
        )
        with pytest.raises(ServiceError, match="revision_conflict"):
            await session_sql.submit(
                store,
                session.id,
                TurnRequest(request_key="old-turn", text="旧状态", expected_revision=0),
                Settings(),
            )
        submitted = await session_sql.submit(
            store,
            session.id,
            TurnRequest(
                request_key="new-turn",
                text="新状态",
                expected_revision=selected["session_revision"],
            ),
            Settings(),
        )
        assert submitted.status == "queued"


async def test_blocked_project_cannot_be_bound_away(tmp_path):
    async with managed(tmp_path / "blocked.sqlite") as (store, _, service, _, actor, *_):
        session = await new_session(store, actor)
        project = await new_project(service, actor)
        await bind_project(
            service,
            actor,
            session.id,
            {
                "project_id": project["project_id"],
                "expected_revision": 0,
                "expected_session_revision": 0,
            },
            request_key="binding",
        )
        await store._write(
            lambda conn: conn.execute(
                update(sandbox_projects)
                .where(sandbox_projects.c.project_id == project["project_id"])
                .values(status="blocked")
            )
        )
        with pytest.raises(ServiceError, match="workspace_busy"):
            await bind_project(
                service,
                actor,
                session.id,
                {"project_id": None, "expected_revision": 1, "expected_session_revision": 1},
                request_key="escape",
            )


async def test_schema8_readonly_and_upgrade_add_only_bindings(tmp_path):
    database = tmp_path / "eight.sqlite"
    async with managed(database) as (store, _, service, _, actor, *_):
        session = await new_session(store, actor)
        project = await new_project(service, actor)
        owner = actor.principal_id
    with sqlite3.connect(database) as conn:
        conn.execute("DROP TABLE sandbox_quota_state")
        conn.execute("DROP TABLE sandbox_session_bindings")
        for table in reversed(EXTENSION_TABLES):
            conn.execute(f'DROP TABLE "{table.name}"')
        conn.execute("PRAGMA user_version=8")
        before = conn.execute("SELECT * FROM sandbox_projects").fetchall()
    reader = await SQLiteStore.open(database, readonly=True)
    try:
        assert reader.schema_version == 8
        async with reader.engine.connect() as conn:
            names = (
                (await conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE type='table'"))
                .scalars()
                .all()
            )
            assert "sandbox_session_bindings" not in names
    finally:
        await reader.close()
    upgraded = await SQLiteStore.open(database)
    try:
        assert upgraded.schema_version == SCHEMA_VERSION
        with sqlite3.connect(database) as conn:
            assert conn.execute("SELECT * FROM sandbox_projects").fetchall() == before
            assert (
                conn.execute("SELECT owner_id FROM sessions WHERE id=?", (session.id,)).fetchone()[
                    0
                ]
                == owner
            )
            assert (
                conn.execute("SELECT project_id FROM sandbox_projects").fetchone()[0]
                == project["project_id"]
            )
    finally:
        await upgraded.close()


async def test_http_binding_routes_use_separate_versions_and_preserve_session_shape(tmp_path):
    from test_account_api import client_for, initialize

    database = tmp_path / "http-binding.sqlite"
    token = await initialize(database)
    configuration = tmp_path / "sandbox.json"
    configuration.write_text("{}")
    configuration.chmod(0o600)
    async with client_for(database, token, sandbox_config_file=configuration) as (app, client):
        me = (await client.get("/v2/me")).json()
        actor = app.identities.authenticate("Bearer " + token)
        session = await new_session(app.store, actor)
        project = await new_project(app.sandbox, actor)
        route = f"/v2/me/sandbox/sessions/{session.id}/project"
        initial = await client.get(route)
        assert initial.status_code == 200 and initial.json()["revision"] == 0
        body = {
            "request_key": "select",
            "project_id": project["project_id"],
            "expected_revision": 0,
            "expected_session_revision": 0,
        }
        response = await client.put(route, json=body)
        assert response.status_code == 200, response.text
        assert response.json()["project_id"] == project["project_id"]
        assert response.json()["session_revision"] == 1
        assert (await client.put(route, json=body)).json() == response.json()
        assert (await client.put(route, json={**body, "request_key": "stale"})).status_code == 409
        assert (
            await client.put(route, json={**body, "request_key": "bad", "expected_revision": True})
        ).status_code == 422
        assert (await client.get("/v2/me")).json() == me
        raw_session = await session_sql.get_session(app.store, session.id)
        assert not hasattr(raw_session, "project_id")


async def test_project_read_routes_forward_pagination_and_search_limits(tmp_path):
    from test_account_api import client_for, initialize

    database = tmp_path / "read-routes.sqlite"
    token = await initialize(database)
    configuration = tmp_path / "sandbox.json"
    configuration.write_text("{}")
    configuration.chmod(0o600)
    calls = []

    class Broker:
        async def call(self, action, **arguments):
            calls.append((action, arguments))
            if action == "list":
                return {
                    "entries": [],
                    "offset": arguments["offset"],
                    "total": 5,
                    "next_offset": None,
                }
            if action == "search":
                return {
                    "matches": [{"path": "src/code.py", "line": 1, "preview": "answer"}],
                    "truncated": False,
                }
            if action == "find":
                return {"files": [], "offset": arguments["offset"], "total": 0}
            if action == "read":
                return {
                    "path": arguments["path"],
                    "offset": arguments["offset"],
                    "size": 5,
                    "eof": True,
                    "sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                    "stat_token": [1, 2, 5, 3, 4],
                    "data": "aGVsbG8=",
                }
            return {"status": "ready"}

    async with client_for(database, token, sandbox_config_file=configuration) as (app, client):
        actor = app.identities.authenticate("Bearer " + token)
        project = await new_project(app.sandbox, actor)
        app.sandbox.broker = Broker()
        await app.sandbox.provision(actor, project_id=project["project_id"], request_key="prepare")
        route = f"/v2/me/sandbox/projects/{project['project_id']}"
        page = await client.get(route + "/files", params={"offset": 4, "limit": 1})
        assert page.status_code == 200 and page.json()["offset"] == 4
        assert calls[-1][1]["limit"] == 1 and calls[-1][1]["offset"] == 4
        found = await client.get(route + "/search", params={"query": "answer", "limit": 3})
        assert found.status_code == 200 and found.json()["matches"][0]["path"] == "src/code.py"
        assert calls[-1][0] == "search" and calls[-1][1]["limit"] == 3
        listed = await client.get(
            route + "/find", params={"include_glob": "**/*.py", "limit": 10}
        )
        assert listed.status_code == 200 and calls[-1][0] == "find"
        protected_glob = await client.get(
            route + "/find", params={"include_glob": "**/.git*"}
        )
        assert protected_glob.status_code == 403
        found_page = await client.get(
            route + "/search",
            params={"query": "answer", "mode": "regex", "include_glob": "**/*.py",
                    "offset": 4, "before": 1, "after": 2},
        )
        assert found_page.status_code == 200
        assert calls[-1][1]["offset"] == 4 and calls[-1][1]["before"] == 1
        names = await client.get(route + "/search", params={"query": "answer", "mode": "regex"})
        assert names.status_code == 200 and calls[-1][1]["mode"] == "regex"
        assert "regex_limit_bytes" in names.json()
        lookup = await client.get(route + "/search", params={"query": "x", "mode": "glob"})
        assert lookup.status_code == 422
        over_limit = await client.get(route + "/search", params={"offset": 4050, "query": "x"})
        assert over_limit.status_code == 422
            route + "/search", params={"offset": 4095, "limit": 1, "query": "answer"}
        )
        assert search_last_page.status_code == 200
        assert calls[-1][1]["offset"] == 4095 and calls[-1][1]["limit"] == 1
        assert (
            await client.get(route + "/search", params={"offset": 4095, "limit": 2, "query": "x"})
        ).status_code == 422
        find_last_page = await client.get(
            route + "/find", params={"offset": 4095, "limit": 1, "include_glob": "**/*.py"}
        )
        assert find_last_page.status_code == 200
        assert calls[-1][1]["offset"] == 4095 and calls[-1][1]["limit"] == 1
        assert (
            await client.get(
                route + "/find", params={"offset": 4095, "limit": 2, "include_glob": "**/*.py"}
            )
        ).status_code == 422
        assert (await client.get(route + "/files", params={"offset": -1})).status_code == 422
        assert (await client.get(route + "/search", params={"query": ""})).status_code == 422
        assert (await client.get(route + "/export-file", params={"path": "../secret"})).status_code == 422
        downloaded = await client.get(route + "/export-file", params={"path": "note.txt"})
        assert downloaded.headers["etag"] == (
            '"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"'
        )
        file_read = await client.get(route + "/file", params={"path": "note.txt"})
        assert file_read.status_code == 200 and "stat_token" not in file_read.json()
        assert "expected_sha256" not in calls[-1][1]
        assert "expected_stat" not in calls[-1][1]
        assert downloaded.headers["x-content-type-options"] == "nosniff"
        invalid_glob = await client.get(route + "/find", params={"include_glob": "../*"})
        assert invalid_glob.status_code == 422
        plain_glob = await client.get(route + "/find", params={"include_glob": "src"})
        assert plain_glob.status_code == 422
        assert (await client.get(route + "/find", params={"include_glob": "../*"})).status_code == 422
        assert (await client.get(route + "/find", params={"include_glob": "src"})).status_code == 422
        preview = await app.sandbox.preview_replace(
            actor, project["project_id"], path="note.txt", expected_generation=0,
            expected_sha256="2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            find="ell", replacement="世界",
        )
        assert preview["patch"]["offset"] == 1
        assert preview["patch"]["delete_length"] == 3
        assert preview["patch"]["replacement"] == "世界"
        assert preview["preview_only"] is True
        assert "-hello" in preview["diff"] and "+h世界o" in preview["diff"]
        unicode_preview = await app.sandbox.preview_replace(
            actor, project["project_id"], path="note.txt", expected_generation=0,
            expected_sha256="2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            find="ell", replacement="x", occurrence=1,
        )
        assert unicode_preview["patch"]["offset"] == 1

        large_lines = [f"é line-{index:05d}\n" for index in range(20000)]
        large_source = "".join(large_lines).encode()
        large_digest = sha256(large_source)
        stat_token = [1, 2, len(large_source), 3, 4]
        expected_offset = len("".join(large_lines[:10000]).encode()) + len("é ".encode())

        class ChunkBroker(Broker):
            def __init__(self):
                super().__init__()
                self.read_calls = []

            async def call(self, action, **arguments):
                if action != "read":
                    return await super().call(action, **arguments)
                self.read_calls.append(arguments)
                offset = arguments["offset"]
                block = large_source[offset:offset + arguments["limit"]]
                return {
                    "path": arguments["path"],
                    "offset": offset,
                    "size": len(large_source),
                    "eof": offset + len(block) >= len(large_source),
                    "sha256": large_digest,
                    "stat_token": stat_token,
                    "data": base64.b64encode(block).decode(),
                }

        chunk_broker = ChunkBroker()
        app.sandbox.broker = chunk_broker
        downloaded, digest = await app.sandbox.download_file(
            actor, project["project_id"], path="large.txt"
        )
        assert downloaded == large_source and digest == large_digest
        assert len(chunk_broker.read_calls) > 1
        assert "expected_sha256" not in chunk_broker.read_calls[0]
        assert "expected_stat" not in chunk_broker.read_calls[0]
        assert all(
            call["expected_sha256"] == large_digest
            and call["expected_stat"] == stat_token
            for call in chunk_broker.read_calls[1:]
        )

        class LegacyChunkBroker(ChunkBroker):
            async def call(self, action, **arguments):
                result = await super().call(action, **arguments)
                if action == "read":
                    result.pop("stat_token", None)
                return result

        legacy_broker = LegacyChunkBroker()
        app.sandbox.broker = legacy_broker
        legacy_download, legacy_digest = await app.sandbox.download_file(
            actor, project["project_id"], path="legacy.txt"
        )
        assert legacy_download == large_source and legacy_digest == large_digest
        assert len(legacy_broker.read_calls) > 1
        assert all(
            "expected_sha256" not in call and "expected_stat" not in call
            for call in legacy_broker.read_calls
        )

        legacy_broker.read_calls.clear()
        legacy_preview = await app.sandbox.preview_replace(
            actor,
            project["project_id"],
            path="legacy.txt",
            expected_generation=0,
            expected_sha256=large_digest,
            find="line-10000",
            replacement="OLD",
        )
        assert legacy_preview["patch"]["offset"] == expected_offset
        assert "-é line-10000" in legacy_preview["diff"]
        assert "+é OLD" in legacy_preview["diff"]
        assert all(
            "expected_sha256" not in call and "expected_stat" not in call
            for call in legacy_broker.read_calls
        )

        chunk_broker.read_calls.clear()
        app.sandbox.broker = chunk_broker
        large_preview = await app.sandbox.preview_replace(
            actor,
            project["project_id"],
            path="large.txt",
            expected_generation=0,
            expected_sha256=large_digest,
            find="line-10000",
            replacement="LONG",
        )
        assert "+é LONG" in large_preview["diff"]
        assert large_preview["diff_truncated"] is False
        assert len(chunk_broker.read_calls) > 1
        assert all(
            call["expected_sha256"] == large_digest
            and call["expected_stat"] == stat_token
            for call in chunk_broker.read_calls[1:]
        )
