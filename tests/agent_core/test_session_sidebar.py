"""侧栏呈现持久化：空默认、CAS冲突、身份/工作区隔离、结构校验与schema迁移。

schema21起组id必须是真实可见project_id，group_id与sandbox_session_bindings一致，
GET自动补齐真实项目与owner范围可见会话；<=20旧行为用store.schema_version=20隔离。
"""

from __future__ import annotations

import sqlite3
import uuid
from contextlib import asynccontextmanager

import httpx
import pytest

from krim_agent.api.server import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import ServiceError
from krim_agent.project_access.policy import set_project_access
from krim_agent.security import LOCAL, Membership, PrincipalContext
from krim_agent.sessions.types import (
    SessionCreate,
    SessionSidebarWrite,
    SidebarGroup,
    SidebarSessionEntry,
)
from krim_agent.storage import session_sidebar as sidebar
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.sidebar_models import SIDEBAR_TABLE_NAMES
from krim_agent.storage.sqlite import SCHEMA_VERSION, SQLiteStore
from tests.agent_core.test_account_api import initialize
from tests.agent_core.test_extension_migration import downgrade_to_version


@asynccontextmanager
async def wired_client(database, token, wire):
    """与client_for同形态；经wire夹具装配provider，会话创建才具备运行绑定。"""
    async with wire.application(database) as app:
        api = create_app(application=app)
        async with (
            api.router.lifespan_context(api),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api),
                base_url="http://127.0.0.1",
                headers={"Authorization": "Bearer " + token},
            ) as client,
        ):
            yield app, client


async def seeded_session(store, owner: str, key: str, workspace: str = "local"):
    return await session_sql.create_session(
        store,
        workspace,
        owner,
        SessionCreate(request_key=key),
        {},
        ContextPolicy(),
        Settings(),
    )


def layout(session_id: str, expected_revision: int = 0) -> SessionSidebarWrite:
    # 本地库无项目ACL、会话无绑定：组id不设（凭空组id在schema21会404），
    # 未绑定会话固定留在默认区（group_id=null）。
    return SessionSidebarWrite(
        sessions=(SidebarSessionEntry(session_id=session_id, pinned=True),),
        expected_revision=expected_revision,
    )


async def test_sidebar_defaults_empty_and_survives_reload(tmp_path):
    database = tmp_path / "sidebar.sqlite"
    async with await open_application(database) as host:
        store = host.store
        assert store.schema_version == SCHEMA_VERSION
        session = await seeded_session(store, LOCAL.principal_id, "sidebar-reload")
        empty = await sidebar.get(store, principal=LOCAL, workspace="local")
        # schema21：GET自动补齐owner范围可见会话；本地库无项目，组仍为空、修订为基线0。
        assert empty.revision == 0 and empty.groups == ()
        assert [(entry.session_id, entry.group_id) for entry in empty.sessions] == [
            (session.id, None)
        ]
        written = await sidebar.put(
            store, principal=LOCAL, workspace="local", body=layout(session.id)
        )
        assert written.revision == 1
        assert written.groups == ()
        assert written.sessions[0].session_id == session.id
        assert written.sessions[0].group_id is None
        assert written.sessions[0].pinned is True
    async with await open_application(database) as host:
        reloaded = await sidebar.get(host.store, principal=LOCAL, workspace="local")
        assert reloaded == written


async def test_sidebar_cas_rejects_stale_writes_and_advances(tmp_path):
    database = tmp_path / "sidebar-cas.sqlite"
    async with await open_application(database) as host:
        store = host.store
        session = await seeded_session(store, LOCAL.principal_id, "sidebar-cas")
        first = await sidebar.put(
            store, principal=LOCAL, workspace="local", body=layout(session.id, 0)
        )
        assert first.revision == 1
        with pytest.raises(ServiceError, match="sidebar_revision_conflict"):
            await sidebar.put(
                store, principal=LOCAL, workspace="local", body=layout(session.id, 0)
            )
        assert store.healthy
        cleared = await sidebar.put(
            store,
            principal=LOCAL,
            workspace="local",
            body=SessionSidebarWrite(expected_revision=1),
        )
        # 写投影始终补齐真实项目与可见会话：空写不产生空文档。
        assert cleared.revision == 2 and cleared.groups == ()
        assert [entry.session_id for entry in cleared.sessions] == [session.id]
        with pytest.raises(ServiceError, match="sidebar_revision_conflict"):
            await sidebar.put(
                store, principal=LOCAL, workspace="local", body=layout(session.id, 1)
            )


def test_sidebar_write_model_rejects_structural_violations():
    with pytest.raises(ValueError):
        SessionSidebarWrite(
            groups=(
                SidebarGroup(id="x", title="一"),
                SidebarGroup(id="x", title="二"),
            ),
            expected_revision=0,
        )
    with pytest.raises(ValueError):
        SessionSidebarWrite(
            sessions=(
                SidebarSessionEntry(session_id="a"),
                SidebarSessionEntry(session_id="a"),
            ),
            expected_revision=0,
        )
    with pytest.raises(ValueError):
        SessionSidebarWrite(
            sessions=(SidebarSessionEntry(session_id="a", group_id="ghost"),),
            expected_revision=0,
        )
    with pytest.raises(ValueError):
        SessionSidebarWrite(
            sessions=(SidebarSessionEntry(session_id="a", parent_id="ghost"),),
            expected_revision=0,
        )
    with pytest.raises(ValueError):
        SessionSidebarWrite(
            sessions=(SidebarSessionEntry(session_id="a", parent_id="a"),),
            expected_revision=0,
        )
    with pytest.raises(ValueError):
        SessionSidebarWrite(
            sessions=(
                SidebarSessionEntry(session_id="a", parent_id="b"),
                SidebarSessionEntry(session_id="b", parent_id="a"),
            ),
            expected_revision=0,
        )
    with pytest.raises(ValueError):
        SessionSidebarWrite(
            groups=(SidebarGroup(id="g1", title="一"), SidebarGroup(id="g2", title="二")),
            sessions=(
                SidebarSessionEntry(session_id="a", group_id="g1"),
                SidebarSessionEntry(session_id="b", group_id="g2", parent_id="a"),
            ),
            expected_revision=0,
        )
    forest = SessionSidebarWrite(
        groups=(SidebarGroup(id="g1", title="组"),),
        sessions=(
            SidebarSessionEntry(session_id="root", group_id="g1"),
            SidebarSessionEntry(session_id="child", group_id="g1", parent_id="root"),
            SidebarSessionEntry(session_id="grand", group_id="g1", parent_id="child"),
        ),
        expected_revision=3,
    )
    assert [entry.session_id for entry in forest.sessions] == ["root", "child", "grand"]
    # parent链深度上限32：33层整链拒绝，32层整链通过。
    with pytest.raises(ValueError):
        SessionSidebarWrite(
            sessions=tuple(
                SidebarSessionEntry(
                    session_id=f"n{index}", parent_id=f"n{index - 1}" if index else None
                )
                for index in range(33)
            ),
            expected_revision=0,
        )
    deep_ok = SessionSidebarWrite(
        sessions=tuple(
            SidebarSessionEntry(
                session_id=f"n{index}", parent_id=f"n{index - 1}" if index else None
            )
            for index in range(32)
        ),
        expected_revision=0,
    )
    assert len(deep_ok.sessions) == 32


async def test_sidebar_read_rejects_corrupt_persisted_document(tmp_path):
    database = tmp_path / "sidebar-corrupt.sqlite"
    async with await open_application(database) as host:
        session = await seeded_session(host.store, LOCAL.principal_id, "sidebar-corrupt")
        await sidebar.put(host.store, principal=LOCAL, workspace="local", body=layout(session.id))
        await host.close()
    corrupt_bodies = (
        '[{"session_id":"a"},{"session_id":"a"}]',  # 重复条目
        '[{"session_id":"a","parent_id":"b"},{"session_id":"b","parent_id":"a"}]',  # 成环
        '[{"session_id":"a","parent_id":"ghost"}]',  # 悬挂父引用
    )
    for body in corrupt_bodies:
        with sqlite3.connect(database) as connection:
            connection.execute("UPDATE session_sidebars SET sessions = ?", (body,))
        reader = await SQLiteStore.open(database)
        try:
            with pytest.raises(ServiceError, match="facts_corrupt"):
                await sidebar.get(reader, principal=LOCAL, workspace="local")
            assert reader.healthy
            with pytest.raises(ServiceError, match="facts_corrupt"):
                await sidebar.put(
                    reader,
                    principal=LOCAL,
                    workspace="local",
                    body=SessionSidebarWrite(expected_revision=1),
                )
        finally:
            await reader.close()


async def test_sidebar_references_require_visible_workspace_sessions(tmp_path):
    database = tmp_path / "sidebar-visibility.sqlite"
    async with await open_application(database) as host:
        store = host.store
        mine = await seeded_session(store, "member-1", "sidebar-mine")
        others = await seeded_session(store, "member-2", "sidebar-others")
        actor = PrincipalContext(
            "member-1", 1, (Membership(workspace_id="local", role="member"),)
        )
        written = await sidebar.put(
            store, principal=actor, workspace="local", body=layout(mine.id)
        )
        assert written.revision == 1
        # 非管理员引用他人会话：与不存在同响应，不泄露存在性。
        for target in (others.id, str(uuid.uuid4())):
            with pytest.raises(ServiceError, match="not_found"):
                await sidebar.put(
                    store,
                    principal=actor,
                    workspace="local",
                    body=SessionSidebarWrite(
                        sessions=(SidebarSessionEntry(session_id=target),),
                        expected_revision=written.revision,
                    ),
                )
        assert store.healthy
        # schema21起owner范围与list_sessions一致：管理员引用他人会话同样404。
        for target in (mine.id, others.id):
            with pytest.raises(ServiceError, match="not_found"):
                await sidebar.put(
                    store,
                    principal=LOCAL,
                    workspace="local",
                    body=SessionSidebarWrite(
                        sessions=(SidebarSessionEntry(session_id=target),),
                        expected_revision=0,
                    ),
                )
        # 管理员自己的会话仍可写入自己的布局。
        local_own = await seeded_session(store, LOCAL.principal_id, "sidebar-local")
        admin_write = await sidebar.put(
            store, principal=LOCAL, workspace="local", body=layout(local_own.id)
        )
        assert admin_write.revision == 1 and len(admin_write.sessions) == 1
        assert store.healthy


async def test_sidebar_schema20_legacy_keeps_free_groups_and_admin_scope(tmp_path):
    """schema<=20旧行为显式隔离（store.schema_version=20）：组id自由别名、
    读路径原样返回存储布局不自动补齐、管理员仍可引用他人未绑定会话、空写即空文档。"""
    database = tmp_path / "sidebar-legacy20.sqlite"
    async with await open_application(database) as host:
        store = host.store
        store.schema_version = 20  # 仅隔离旧侧栏分支；本地库会话本就无绑定
        mine = await seeded_session(store, "member-1", "legacy-mine")
        others = await seeded_session(store, "member-2", "legacy-others")
        actor = PrincipalContext(
            "member-1", 1, (Membership(workspace_id="local", role="member"),)
        )
        written = await sidebar.put(
            store,
            principal=actor,
            workspace="local",
            body=SessionSidebarWrite(
                groups=(SidebarGroup(id="grp-1", title="进行中"),),
                sessions=(
                    SidebarSessionEntry(session_id=mine.id, group_id="grp-1", pinned=True),
                ),
                expected_revision=0,
            ),
        )
        assert written.revision == 1
        assert [group.id for group in written.groups] == ["grp-1"]
        reloaded = await sidebar.get(store, principal=actor, workspace="local")
        assert reloaded == written
        admin_write = await sidebar.put(
            store,
            principal=LOCAL,
            workspace="local",
            body=SessionSidebarWrite(
                groups=(SidebarGroup(id="grp-1", title="全部"),),
                sessions=(
                    SidebarSessionEntry(session_id=mine.id, group_id="grp-1", pinned=True),
                    SidebarSessionEntry(session_id=others.id, group_id="grp-1"),
                ),
                expected_revision=0,
            ),
        )
        assert admin_write.revision == 1 and len(admin_write.sessions) == 2
        cleared = await sidebar.put(
            store,
            principal=LOCAL,
            workspace="local",
            body=SessionSidebarWrite(expected_revision=1),
        )
        assert cleared.revision == 2 and cleared.groups == () and cleared.sessions == ()


async def test_sidebar_workspace_membership_gates_read_and_write(tmp_path):
    database = tmp_path / "sidebar-workspace.sqlite"
    async with await open_application(database) as host:
        store = host.store
        session = await seeded_session(store, LOCAL.principal_id, "sidebar-workspace")
        outsider = PrincipalContext(
            "member-9", 1, (Membership(workspace_id="elsewhere", role="admin"),)
        )
        for operation in (
            sidebar.get(store, principal=outsider, workspace="local"),
            sidebar.put(
                store, principal=outsider, workspace="local", body=layout(session.id)
            ),
        ):
            with pytest.raises(ServiceError, match="not_found"):
                await operation
        assert store.healthy
        await sidebar.put(store, principal=LOCAL, workspace="local", body=layout(session.id))


async def test_sidebar_readonly_schema16_gate(tmp_path):
    database = tmp_path / "sidebar-schema16-gate.sqlite"
    async with await open_application(database) as host:
        session = await seeded_session(host.store, LOCAL.principal_id, "sidebar-schema16-gate")
        await host.close()
    downgrade_to_version(database, 16)
    unchanged = database.read_bytes()
    reader = await SQLiteStore.open(database, readonly=True)
    try:
        assert reader.schema_version == 16
        with pytest.raises(ServiceError, match="schema_upgrade_required"):
            await sidebar.get(reader, principal=LOCAL, workspace="local")
        with pytest.raises(ServiceError, match="schema_upgrade_required"):
            await sidebar.put(
                reader, principal=LOCAL, workspace="local", body=layout(session.id)
            )
    finally:
        await reader.close()
    assert database.read_bytes() == unchanged


async def test_migration_from_schema16_preserves_source_and_enables_sidebar(tmp_path):
    database = tmp_path / "sidebar-migration.sqlite"
    async with await open_application(database) as host:
        session = await seeded_session(host.store, LOCAL.principal_id, "sidebar-migration")
        await host.close()
    downgrade_to_version(database, 16)
    with sqlite3.connect(database) as connection:
        before = connection.execute("SELECT * FROM sessions").fetchall()
    reader = await SQLiteStore.open(database, readonly=True)
    try:
        assert reader.schema_version == 16
        assert (await session_sql.get_session(reader, session.id)).id == session.id
    finally:
        await reader.close()
    writer = await SQLiteStore.open(database)
    try:
        assert writer.schema_version == SCHEMA_VERSION
        assert (await session_sql.get_session(writer, session.id)).id == session.id
        written = await sidebar.put(
            writer, principal=LOCAL, workspace="local", body=layout(session.id)
        )
        assert written.revision == 1
    finally:
        await writer.close()
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT * FROM sessions").fetchall() == before
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    assert SIDEBAR_TABLE_NAMES <= tables


async def test_sidebar_api_roundtrip_conflict_and_key_rotation(tmp_path, wire):
    database = tmp_path / "sidebar-api.sqlite"
    token = await initialize(database)
    async with wired_client(database, token, wire) as (app, client):
        actor = app.identities.authenticate("Bearer " + token)
        workspace = actor.memberships[0].workspace_id
        path = f"/v2/workspaces/{workspace}/session-sidebar"
        created = await client.post(
            f"/v2/workspaces/{workspace}/sessions", json={"request_key": "api-sidebar"}
        )
        assert created.status_code == 201, created.text
        session_id = created.json()["id"]
        # 管理账号默认项目随会话创建自动建立；GET自动补齐真实项目与可见会话。
        empty = await client.get(path)
        assert empty.status_code == 200, empty.text
        assert empty.json()["revision"] == 0
        assert [group["title"] for group in empty.json()["groups"]] == ["默认项目"]
        project_id = empty.json()["groups"][0]["id"]
        assert empty.json()["sessions"] == [
            {
                "session_id": session_id,
                "group_id": project_id,
                "parent_id": None,
                "pinned": False,
            }
        ]
        body = {
            "groups": [{"id": project_id, "title": "进行中"}],
            "sessions": [
                {
                    "session_id": session_id,
                    "group_id": project_id,
                    "parent_id": None,
                    "pinned": True,
                }
            ],
            "expected_revision": 0,
        }
        written = await client.put(path, json=body)
        assert written.status_code == 200, written.text
        assert written.json()["revision"] == 1
        assert written.json()["groups"] == [{"id": project_id, "title": "进行中"}]
        stale = await client.put(path, json=body)
        assert stale.status_code == 409, stale.text
        assert stale.json()["error"]["code"] == "sidebar_revision_conflict"
        cleared = await client.put(path, json={"expected_revision": 1})
        assert cleared.status_code == 200, cleared.text
        # 空写不产生空文档：别名回落项目名，会话补齐真实绑定。
        assert cleared.json() == {
            "revision": 2,
            "groups": [{"id": project_id, "title": "默认项目"}],
            "sessions": [
                {
                    "session_id": session_id,
                    "group_id": project_id,
                    "parent_id": None,
                    "pinned": False,
                }
            ],
        }
        # 密钥轮换不清空偏好：同主体新token读到原布局与修订。
        restored = await client.put(path, json={**body, "expected_revision": 2})
        assert restored.status_code == 200 and restored.json()["revision"] == 3
        rotated = await client.post("/v2/me/key/rotate", json={"request_key": "rotate-sidebar"})
        assert rotated.status_code == 200, rotated.text
        fresh_headers = {"Authorization": "Bearer " + rotated.json()["token"]}
        reauth = await client.get(path, headers=fresh_headers)
        assert reauth.status_code == 200, reauth.text
        assert reauth.json()["revision"] == 3
        assert reauth.json()["sessions"][0]["session_id"] == session_id
        assert reauth.json()["sessions"][0]["group_id"] == project_id
        assert (
            await client.get(path, headers={**fresh_headers, "Authorization": "Bearer x"})
        ).status_code == 401


async def test_sidebar_api_isolation_between_principals_and_workspaces(tmp_path, wire):
    database = tmp_path / "sidebar-isolation.sqlite"
    admin_token = await initialize(database)
    async with wired_client(database, admin_token, wire) as (app, client):
        actor = app.identities.authenticate("Bearer " + admin_token)
        workspace = actor.memberships[0].workspace_id
        path = f"/v2/workspaces/{workspace}/session-sidebar"
        admin_session = await client.post(
            f"/v2/workspaces/{workspace}/sessions", json={"request_key": "admin-session"}
        )
        assert admin_session.status_code == 201, admin_session.text
        admin_session_id = admin_session.json()["id"]
        created = await client.post(
            "/v2/admin/accounts", json={"request_key": "member-create", "display_name": "用户乙"}
        )
        assert created.status_code == 201, created.text
        member = created.json().get("account", created.json())
        member_id = member["principal_id"]
        issued = await client.post(
            f"/v2/admin/accounts/{member_id}/key/issue", json={"request_key": "member-issue"}
        )
        member_token = issued.json()["token"]
        member_headers = {"Authorization": "Bearer " + member_token}
        memberships = await client.get(f"/v2/admin/accounts/{member_id}/memberships")
        joined = await client.put(
            f"/v2/admin/accounts/{member_id}/memberships",
            json={
                "request_key": "member-join",
                "expected_generation": memberships.json()["generation"],
                "memberships": [
                    {"workspace_id": workspace, "role": "member", "approve": True}
                ],
            },
        )
        assert joined.status_code == 200, joined.text
        member_session = await client.post(
            f"/v2/workspaces/{workspace}/sessions",
            json={"request_key": "member-session"},
            headers=member_headers,
        )
        assert member_session.status_code == 201, member_session.text
        member_session_id = member_session.json()["id"]
        member_path = path
        # 非管理员引用他人会话被拒（他人项目不可见），且不泄露存在性。
        foreign = await client.put(
            member_path,
            json={
                "sessions": [{"session_id": admin_session_id}],
                "expected_revision": 0,
            },
            headers=member_headers,
        )
        assert foreign.status_code == 404, foreign.text
        # 会话创建即绑定默认项目：GET自动补齐，写入必须引用真实项目id。
        member_sidebar = await client.get(path, headers=member_headers)
        assert member_sidebar.status_code == 200, member_sidebar.text
        assert member_sidebar.json()["revision"] == 0
        member_project = member_sidebar.json()["groups"][0]["id"]
        assert [
            entry["session_id"] for entry in member_sidebar.json()["sessions"]
        ] == [member_session_id]
        own = await client.put(
            member_path,
            json={
                "groups": [{"id": member_project, "title": "我的"}],
                "sessions": [
                    {
                        "session_id": member_session_id,
                        "group_id": member_project,
                        "pinned": True,
                    }
                ],
                "expected_revision": 0,
            },
            headers=member_headers,
        )
        assert own.status_code == 200, own.text
        assert own.json()["revision"] == 1
        # 两主体各自持久化，互不可见：管理员侧看不到成员项目内的会话。
        admin_view = await client.get(path)
        assert admin_view.status_code == 200 and admin_view.json()["revision"] == 0
        admin_project = admin_view.json()["groups"][0]["id"]
        assert member_session_id not in {
            entry["session_id"] for entry in admin_view.json()["sessions"]
        }
        member_view = await client.get(path, headers=member_headers)
        assert member_view.json()["revision"] == 1
        assert member_view.json()["sessions"][0]["session_id"] == member_session_id
        # owner范围与list_sessions一致：管理员引用他人绑定会话或凭空组id均404，
        # 自己默认项目仍可写。
        denied = await client.put(
            path,
            json={
                "groups": [{"id": "grp-1", "title": "全部"}],
                "sessions": [
                    {"session_id": admin_session_id, "group_id": "grp-1"},
                    {"session_id": member_session_id, "group_id": "grp-1"},
                ],
                "expected_revision": 0,
            },
        )
        assert denied.status_code == 404, denied.text
        admin_write = await client.put(
            path,
            json={
                "groups": [{"id": admin_project, "title": "全部"}],
                "sessions": [{"session_id": admin_session_id, "group_id": admin_project}],
                "expected_revision": 0,
            },
        )
        assert admin_write.status_code == 200, admin_write.text
        # 工作区隔离：非成员访问任意未知工作区统一404。
        stranger = await client.get(
            f"/v2/workspaces/{uuid.uuid4()}/session-sidebar", headers=member_headers
        )
        assert stranger.status_code == 404, stranger.text
        # 事务内授权复核（读/写均实时）：旧generation上下文直调存储被拒。
        member_actor = app.identities.resolve(member_id)
        stale_actor = PrincipalContext(
            member_id,
            member_actor.generation + 1,
            member_actor.memberships,
        )
        with pytest.raises(ServiceError, match="permission_denied"):
            await sidebar.get(app.store, principal=stale_actor, workspace=workspace)
        with pytest.raises(ServiceError, match="permission_denied"):
            await sidebar.put(
                app.store,
                principal=stale_actor,
                workspace=workspace,
                body=SessionSidebarWrite(
                    sessions=(SidebarSessionEntry(session_id=member_session_id),),
                    expected_revision=own.json()["revision"],
                ),
            )
        # 上下文成员关系与数据库不一致时，写事务内复核仍拒绝。
        removed = await client.put(
            f"/v2/admin/accounts/{member_id}/memberships",
            json={
                "request_key": "member-leave",
                "expected_generation": member_actor.generation,
                "memberships": [],
            },
        )
        assert removed.status_code == 200, removed.text
        evicted = app.identities.resolve(member_id)
        mismatched = PrincipalContext(
            member_id,
            evicted.generation,
            (Membership(workspace_id=workspace, role="member"),),
        )
        with pytest.raises(ServiceError, match="not_found"):
            await sidebar.put(
                app.store,
                principal=mismatched,
                workspace=workspace,
                body=SessionSidebarWrite(
                    sessions=(SidebarSessionEntry(session_id=member_session_id),),
                    expected_revision=own.json()["revision"],
                ),
            )
        assert app.store.healthy


async def test_sidebar_get_filters_invisible_references_after_demotion(tmp_path, wire):
    """管理员降member且被授权项目ACL撤权后：GET筛掉不可见引用、revision保持基线，
    仍能保存自己项目内的布局；响应不泄露已不可见的会话引用（含他人的）。"""
    database = tmp_path / "sidebar-demotion.sqlite"
    admin_token = await initialize(database)
    async with wired_client(database, admin_token, wire) as (app, client):
        actor = app.identities.authenticate("Bearer " + admin_token)
        admin_id = actor.principal_id
        shared = str(uuid.uuid4())
        # 建立无主共享工作区：admin自任admin，成员加入member。
        mine = await client.get(f"/v2/admin/accounts/{admin_id}/memberships")
        join_self = await client.put(
            f"/v2/admin/accounts/{admin_id}/memberships",
            json={
                "request_key": "join-shared-self",
                "expected_generation": mine.json()["generation"],
                "memberships": [{"workspace_id": shared, "role": "admin", "approve": True}],
            },
        )
        assert join_self.status_code == 200, join_self.text
        created = await client.post(
            "/v2/admin/accounts", json={"request_key": "demotion-member", "display_name": "用户丙"}
        )
        member = created.json().get("account", created.json())
        member_id = member["principal_id"]
        issued = await client.post(
            f"/v2/admin/accounts/{member_id}/key/issue", json={"request_key": "demotion-issue"}
        )
        member_headers = {"Authorization": "Bearer " + issued.json()["token"]}
        theirs = await client.get(f"/v2/admin/accounts/{member_id}/memberships")
        join_member = await client.put(
            f"/v2/admin/accounts/{member_id}/memberships",
            json={
                "request_key": "join-shared-member",
                "expected_generation": theirs.json()["generation"],
                "memberships": [{"workspace_id": shared, "role": "member", "approve": True}],
            },
        )
        assert join_member.status_code == 200, join_member.text
        # 各自创建会话：默认项目随创建自动建立并绑定。
        admin_session = await client.post(
            f"/v2/workspaces/{shared}/sessions", json={"request_key": "demotion-admin"}
        )
        member_session = await client.post(
            f"/v2/workspaces/{shared}/sessions",
            json={"request_key": "demotion-member"},
            headers=member_headers,
        )
        assert admin_session.status_code == 201 and member_session.status_code == 201
        admin_session_id = admin_session.json()["id"]
        member_session_id = member_session.json()["id"]
        path = f"/v2/workspaces/{shared}/session-sidebar"
        member_sidebar = await client.get(path, headers=member_headers)
        assert member_sidebar.status_code == 200, member_sidebar.text
        member_project = member_sidebar.json()["groups"][0]["id"]
        # 项目ACL是owner能力、管理员不继承：由成员授予write后管理员才能在该项目建会话。
        member_actor = app.identities.resolve(member_id)
        granted = await app.store._write(lambda conn: set_project_access(
            conn, member_actor, member_project, admin_id, "write", expected_revision=0
        ))
        assert granted.access == "write"
        granted_session = await client.post(
            f"/v2/workspaces/{shared}/sessions",
            json={"request_key": "demotion-granted", "project_id": member_project},
        )
        assert granted_session.status_code == 201, granted_session.text
        granted_session_id = granted_session.json()["id"]
        seeded = await client.get(path)
        assert seeded.status_code == 200, seeded.text
        assert seeded.json()["revision"] == 0
        bindings = {
            entry["session_id"]: entry["group_id"] for entry in seeded.json()["sessions"]
        }
        assert member_session_id not in bindings
        own_project = bindings[admin_session_id]
        assert bindings[granted_session_id] == member_project
        saved = await client.put(
            path,
            json={
                "groups": [
                    {"id": own_project, "title": "全部"},
                    {"id": member_project, "title": "协作"},
                ],
                "sessions": [
                    {"session_id": admin_session_id, "group_id": own_project, "pinned": True},
                    {"session_id": granted_session_id, "group_id": member_project},
                ],
                "expected_revision": 0,
            },
        )
        assert saved.status_code == 200, saved.text
        assert saved.json()["revision"] == 1
        assert [group["id"] for group in saved.json()["groups"]] == [
            own_project,
            member_project,
        ]
        # 撤回项目授权，并把管理员降级为 member。
        revoked = await app.store._write(lambda conn: set_project_access(
            conn, member_actor, member_project, admin_id, "none", expected_revision=1
        ))
        assert revoked.access == "none"
        current = await client.get(f"/v2/admin/accounts/{admin_id}/memberships")
        demoted = await client.put(
            f"/v2/admin/accounts/{admin_id}/memberships",
            json={
                "request_key": "demote-self",
                "expected_generation": current.json()["generation"],
                "memberships": [{"workspace_id": shared, "role": "member", "approve": True}],
            },
        )
        assert demoted.status_code == 200, demoted.text
        # GET 按当前授权筛选：不可见项目消失、自己项目与别名保留、revision原样。
        filtered = await client.get(path)
        assert filtered.status_code == 200, filtered.text
        body = filtered.json()
        assert body["revision"] == 1
        assert [group["id"] for group in body["groups"]] == [own_project]
        assert [group["title"] for group in body["groups"]] == ["全部"]
        assert [entry["session_id"] for entry in body["sessions"]] == [admin_session_id]
        assert body["sessions"][0]["group_id"] == own_project
        assert body["sessions"][0]["pinned"] is True
        assert all(entry["session_id"] != granted_session_id for entry in body["sessions"])
        assert all(entry["session_id"] != member_session_id for entry in body["sessions"])
        # 降权后仍能保存自己项目内的布局；他人会话与已不可见会话引用均404。
        resaved = await client.put(
            path,
            json={
                "groups": [{"id": own_project, "title": "全部"}],
                "sessions": [
                    {"session_id": admin_session_id, "group_id": own_project, "pinned": True}
                ],
                "expected_revision": 1,
            },
        )
        assert resaved.status_code == 200, resaved.text
        assert resaved.json()["revision"] == 2
        for target in (member_session_id, granted_session_id):
            foreign = await client.put(
                path,
                json={"sessions": [{"session_id": target}], "expected_revision": 2},
            )
            assert foreign.status_code == 404, foreign.text
        assert app.store.healthy
