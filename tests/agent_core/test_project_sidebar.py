"""Projects are the sole sidebar grouping authority; aliases remain personal."""

import json
import sqlite3
import uuid

import pytest
from sqlalchemy import insert, select
from test_sandbox_control import managed

from krim_agent.config import Settings
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import ServiceError
from krim_agent.project_access.policy import initialize_project_access
from krim_agent.sandbox.bindings import bind_project, read_binding
from krim_agent.sessions.types import (
    SessionCreate,
    SessionSidebarWrite,
    SidebarGroup,
    SidebarSessionEntry,
)
from krim_agent.storage import session_sidebar as sidebar
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.model_gateway_models import MODEL_GATEWAY_TABLE_NAMES
from krim_agent.storage.models import turns
from krim_agent.storage.sandbox_models import sandbox_projects
from krim_agent.storage.sidebar_models import session_sidebars
from krim_agent.storage.sqlite import SQLiteStore


async def project(service, actor, title):
    return (
        await service.create_project(
            actor,
            {
                "workspace_id": actor.memberships[0].workspace_id,
                "title": title,
            },
            request_key=str(uuid.uuid4()),
        )
    )["project_id"]


async def session(store, service, actor, project_id=None):
    item = await session_sql.create_session(
        store,
        actor.memberships[0].workspace_id,
        actor.principal_id,
        SessionCreate(request_key=str(uuid.uuid4())),
        {},
        ContextPolicy(),
        Settings(),
    )
    if project_id is not None:
        await bind_project(
            service,
            actor,
            item.id,
            {
                "project_id": project_id,
                "expected_revision": 0,
                "expected_session_revision": 0,
            },
            request_key=str(uuid.uuid4()),
        )
    return item


async def save(store, actor, document):
    return await sidebar.put(
        store,
        principal=actor,
        workspace=actor.memberships[0].workspace_id,
        body=SessionSidebarWrite(
            groups=document.groups, sessions=document.sessions, expected_revision=document.revision
        ),
    )


async def test_project_alias_and_authoritative_group_survive_reload(tmp_path):
    database = tmp_path / "projects.sqlite"
    async with managed(database) as (store, _, service, _, actor, *_):
        p1 = await project(service, actor, "工程一")
        p2 = await project(service, actor, "工程二")
        item = await session(store, service, actor, p1)
        workspace = actor.memberships[0].workspace_id
        initial = await sidebar.get(store, principal=actor, workspace=workspace)
        assert {g.id for g in initial.groups} == {p1, p2}
        assert initial.sessions[0].group_id == p1
        renamed = initial.model_copy(
            update={
                "groups": tuple(
                    g.model_copy(update={"title": "我的显示名"}) if g.id == p1 else g
                    for g in initial.groups
                )
            }
        )
        written = await save(store, actor, renamed)
        assert (await service.project(actor, p1))["title"] == "工程一"
        assert next(g.title for g in written.groups if g.id == p1) == "我的显示名"
        for wrong in (p2, None):
            entries = (SidebarSessionEntry(session_id=item.id, group_id=wrong),)
            with pytest.raises(ServiceError, match="sidebar_project_mismatch"):
                await sidebar.put(
                    store,
                    principal=actor,
                    workspace=workspace,
                    body=SessionSidebarWrite(
                        groups=written.groups,
                        sessions=entries,
                        expected_revision=written.revision,
                    ),
                )
        with pytest.raises(ServiceError, match="not_found"):
            await sidebar.put(
                store,
                principal=actor,
                workspace=workspace,
                body=SessionSidebarWrite(
                    groups=(*written.groups, SidebarGroup(id="unrelated", title="自由分类")),
                    sessions=written.sessions,
                    expected_revision=written.revision,
                ),
            )
    reader = await SQLiteStore.open(database)
    try:
        assert await sidebar.get(reader, principal=actor, workspace=workspace) == written
    finally:
        await reader.close()


async def test_rebinding_updates_all_layouts_and_invalidates_stale_write(tmp_path):
    async with managed(tmp_path / "rebind.sqlite") as (store, _, service, _, actor, *_):
        p1 = await project(service, actor, "原项目")
        p2 = await project(service, actor, "目标项目")
        parent = await session(store, service, actor, p1)
        child = await session(store, service, actor, p1)
        workspace = actor.memberships[0].workspace_id
        initial = await sidebar.get(store, principal=actor, workspace=workspace)
        saved = await save(
            store,
            actor,
            initial.model_copy(
                update={
                    "sessions": (
                        SidebarSessionEntry(session_id=parent.id, group_id=p1, pinned=True),
                        SidebarSessionEntry(session_id=child.id, group_id=p1, parent_id=parent.id),
                    )
                }
            ),
        )
        binding = await read_binding(service, actor, parent.id)
        await bind_project(
            service,
            actor,
            parent.id,
            {
                "project_id": p2,
                "expected_revision": binding["revision"],
                "expected_session_revision": binding["session_revision"],
            },
            request_key="move-project",
        )
        current = await sidebar.get(store, principal=actor, workspace=workspace)
        assert current.revision == saved.revision + 1
        entries = {e.session_id: e for e in current.sessions}
        assert entries[parent.id].group_id == p2 and entries[parent.id].pinned
        assert entries[child.id].group_id == p1 and entries[child.id].parent_id is None
        with pytest.raises(ServiceError, match="sidebar_revision_conflict"):
            await save(store, actor, saved)
        assert (await session_sql.get_session(store, parent.id)).id == parent.id


async def test_schema20_mixed_groups_migrate_without_touching_session_facts(tmp_path):
    database = tmp_path / "legacy.sqlite"
    async with managed(database) as (store, _, service, _, actor, *_):
        p1 = await project(service, actor, "项目一")
        p2 = await project(service, actor, "项目二")
        a = await session(store, service, actor, p1)
        b = await session(store, service, actor, p2)
        unbound = await session(store, service, actor)
        workspace = actor.memberships[0].workspace_id
        await store._write(
            lambda conn: conn.execute(
                insert(turns).values(
                    id=str(uuid.uuid4()),
                    session_id=a.id,
                    workspace_id=workspace,
                    owner_id=actor.principal_id,
                    request_key="preserved-message",
                    request_hash="a" * 64,
                    text="升级前的完整消息内容",
                    status="completed",
                    ordinal=1,
                    accepted_revision=1,
                    projection_version="test",
                    transcript=[{"role": "assistant", "content": "原始回复"}],
                    created_at=1,
                    started_at=1,
                    finished_at=2,
                )
            )
        )
        await store._write(
            lambda conn: conn.execute(
                insert(session_sidebars).values(
                    principal_id=actor.principal_id,
                    workspace_id=workspace,
                    revision=7,
                    groups=[{"id": "legacy", "title": "旧混合组"}],
                    sessions=[
                        {
                            "session_id": a.id,
                            "group_id": "legacy",
                            "parent_id": None,
                            "pinned": True,
                        },
                        {
                            "session_id": b.id,
                            "group_id": "legacy",
                            "parent_id": a.id,
                            "pinned": False,
                        },
                        {
                            "session_id": unbound.id,
                            "group_id": "legacy",
                            "parent_id": None,
                            "pinned": False,
                        },
                    ],
                    updated_at=1,
                )
            )
        )
    with sqlite3.connect(database) as conn:
        # schema22 增量先删除，使表集与 user_version=20 的历史形态一致。
        for name in sorted(MODEL_GATEWAY_TABLE_NAMES):
            conn.execute(f'DROP TABLE "{name}"')
        conn.execute("PRAGMA user_version=20")
        before = {
            name: conn.execute(f"SELECT * FROM {name}").fetchall()
            for name in ("sessions", "turns", "sandbox_session_bindings")
        }
    reader = await SQLiteStore.open(database)
    try:
        migrated = await sidebar.get(reader, principal=actor, workspace=workspace)
        assert migrated.revision == 8
        entries = {e.session_id: e for e in migrated.sessions}
        assert entries[a.id].group_id == p1 and entries[a.id].pinned
        assert entries[b.id].group_id == p2 and entries[b.id].parent_id is None
        assert entries[unbound.id].group_id is None
        assert {g.title for g in migrated.groups} == {"旧混合组"}
    finally:
        await reader.close()
    with sqlite3.connect(database) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 22
        for name, facts in before.items():
            assert conn.execute(f"SELECT * FROM {name}").fetchall() == facts
        assert json.loads(conn.execute("SELECT sessions FROM session_sidebars").fetchone()[0])
    reader = await SQLiteStore.open(database)
    try:
        assert await sidebar.get(reader, principal=actor, workspace=workspace) == migrated
    finally:
        await reader.close()


async def test_real_projects_exceeding_legacy_group_limit_remain_saveable(tmp_path):
    database = tmp_path / "many-projects.sqlite"
    async with managed(database) as (store, _, service, _, actor, *_):
        first = await project(service, actor, "项目0")
        workspace = actor.memberships[0].workspace_id

        async def seed(conn):
            template = (
                (
                    await conn.execute(
                        select(sandbox_projects).where(
                            sandbox_projects.c.project_id == first,
                        )
                    )
                )
                .mappings()
                .one()
            )
            for index in range(101):
                pid = str(uuid.uuid4())
                await conn.execute(
                    insert(sandbox_projects).values(
                        {
                            **template,
                            "project_id": pid,
                            "title": f"项目{index + 1}",
                        }
                    )
                )
                await initialize_project_access(conn, pid, actor.principal_id)

        await store._write(seed)
        layout = await sidebar.get(store, principal=actor, workspace=workspace)
        assert len(layout.groups) == 102
        saved = await save(store, actor, layout)
        assert await sidebar.get(store, principal=actor, workspace=workspace) == saved


async def test_corrupt_preferences_do_not_block_upgrade_or_project_binding(tmp_path):
    database = tmp_path / "corrupt-layout.sqlite"
    async with managed(database) as (store, _, service, admin, actor, *_):
        p1 = await project(service, actor, "旧项目")
        p2 = await project(service, actor, "新项目")
        item = await session(store, service, actor, p1)
        workspace = actor.memberships[0].workspace_id
        await store._write(
            lambda conn: conn.execute(
                insert(session_sidebars).values(
                    principal_id=admin.principal_id,
                    workspace_id=workspace,
                    revision=4,
                    groups=[],
                    sessions=[{"session_id": "a", "parent_id": "missing"}],
                    updated_at=1,
                )
            )
        )
        binding = await read_binding(service, actor, item.id)
        await bind_project(
            service,
            actor,
            item.id,
            {
                "project_id": p2,
                "expected_revision": binding["revision"],
                "expected_session_revision": binding["session_revision"],
            },
            request_key="ignore-corrupt-preference",
        )
        assert (await read_binding(service, actor, item.id))["project_id"] == p2
    with sqlite3.connect(database) as conn:
        # schema22 增量先删除，使表集与 user_version=20 的历史形态一致。
        for name in sorted(MODEL_GATEWAY_TABLE_NAMES):
            conn.execute(f'DROP TABLE "{name}"')
        conn.execute("PRAGMA user_version=20")
        before = conn.execute("SELECT groups,sessions FROM session_sidebars").fetchall()
    reader = await SQLiteStore.open(database)
    try:
        assert (await session_sql.get_session(reader, item.id)).id == item.id
    finally:
        await reader.close()
    with sqlite3.connect(database) as conn:
        assert conn.execute("SELECT groups,sessions FROM session_sidebars").fetchall() == before
