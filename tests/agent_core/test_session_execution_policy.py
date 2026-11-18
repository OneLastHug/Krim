"""单会话执行模式：持久化、CAS热切换、请求幂等、本会话规则与API行为。"""

from __future__ import annotations

import sqlite3
import warnings

import pytest
from pydantic import ValidationError
from sqlalchemy import insert, select, text, update
from sqlalchemy.exc import SAWarning
from sqlalchemy.ext.asyncio import create_async_engine

from krim_agent.api.server import create_app  # 仅为确认路由可装配；断言经HTTP进行
from krim_agent.contracts import ApprovalReply, ServiceError
from krim_agent.security import LOCAL, Membership, PrincipalContext
from krim_agent.sessions.execution_policy import (
    execution_policy_notifications,
)
from krim_agent.sessions.types import SessionCreate
from krim_agent.storage.account_models import accounts
from krim_agent.storage.execution_policy import (
    EXECUTION_POLICY_SCHEMA,
    allow_rule_satisfied,
    arguments_digest,
    clear_allow_rules,
    count_allow_rules,
    get_mode,
    get_policy,
    get_session_project,
    initialize,
    migrate,
    read_mode_snapshot,
    record_allow_rule,
    session_allow_rule_context,
    set_mode,
)
from krim_agent.storage.execution_policy_models import (
    EXECUTION_POLICY_TABLE_NAMES,
    session_execution_policies,
)
from krim_agent.storage.models import metadata, sessions
from krim_agent.storage.project_membership_models import project_memberships
from krim_agent.storage.sandbox_models import (
    sandbox_environments,
    sandbox_projects,
    sandbox_session_bindings,
)
from krim_agent.storage.sqlite import SQLiteStore
from tests.agent_core.test_account_api import client_for
from tests.agent_core.test_account_api import initialize as bootstrap_token

OTHER = PrincipalContext("member-2", 1, (Membership(workspace_id="local", role="admin"),), True)
CROSS_WORKSPACE = PrincipalContext(
    "local",
    1,
    (
        Membership(workspace_id="local", role="admin"),
        Membership(workspace_id="elsewhere", role="admin"),
    ),
    True,
)
_H1 = "a" * 64
_H2 = "b" * 64
_ARGS_X = "c" * 64
_ARGS_Y = "d" * 64


async def _database(tmp_path, *, exclude_policy_tables: bool = False):
    engine = create_async_engine("sqlite+aiosqlite:///" + str(tmp_path / "policy.sqlite"))
    with warnings.catch_warnings():
        # metadata中work表间的既有循环FK警告与本模块无关，建库顺序无碍。
        warnings.simplefilter("ignore", category=SAWarning)
        tables = [
            table
            for table in metadata.sorted_tables
            if not (exclude_policy_tables and table.name in EXECUTION_POLICY_TABLE_NAMES)
        ]
        async with engine.begin() as conn:
            await conn.run_sync(lambda sync: metadata.create_all(sync, tables=tables))
    return engine


def _store(engine) -> SQLiteStore:
    # 独立引擎上的轻量store外壳：复用同一套读写锁与提交回程核实逻辑。
    store = SQLiteStore(engine, -1, False, None)
    store.schema_version = EXECUTION_POLICY_SCHEMA
    return store


async def _seed_session(engine, session_id, *, workspace="local", owner="local") -> None:
    async with engine.begin() as conn:
        await conn.execute(
            insert(sessions).values(
                id=session_id,
                workspace_id=workspace,
                owner_id=owner,
                create_key="k-" + session_id,
                request_hash="0" * 64,
                title="t",
                target_key="demo",
                target={},
                context_policy={},
                revision=0,
                status="open",
                dispatch_paused=False,
                created_at=1.0,
                closed_at=None,
            )
        )


async def _seed_account(engine, principal_id="local") -> None:
    async with engine.begin() as conn:
        await conn.execute(
            insert(accounts).values(
                principal_id=principal_id,
                display_name=principal_id,
                status="active",
                platform_role="admin",
                authorization_generation=1,
                revision=1,
                legacy=True,
                created_at=1.0,
                updated_at=1.0,
            )
        )


async def _seed_project(
    engine,
    project_id,
    *,
    workspace="local",
    owner="local",
    access="write",
    bound_session=None,
) -> None:
    async with engine.begin() as conn:
        env_id = "env-" + owner
        exists = (
            await conn.execute(
                select(sandbox_environments.c.environment_id).where(
                    sandbox_environments.c.environment_id == env_id
                )
            )
        ).scalar_one_or_none()
        if exists is None:
            await conn.execute(
                insert(sandbox_environments).values(
                    environment_id=env_id,
                    principal_id=owner,
                    profile_name=None,
                    quota_override={},
                    revision=1,
                    status="ready",
                    created_at=1.0,
                    updated_at=1.0,
                )
            )
        await conn.execute(
            insert(sandbox_projects).values(
                project_id=project_id,
                environment_id=env_id,
                principal_id=owner,
                workspace_id=workspace,
                title="P",
                generation=0,
                status="ready",
                lease_id=None,
                lease_expires_at=None,
                created_at=1.0,
                updated_at=1.0,
            )
        )
        if access is not None:
            await conn.execute(
                insert(project_memberships).values(
                    project_id=project_id,
                    principal_id=owner,
                    access=access,
                    revision=1,
                    updated_at=1.0,
                )
            )
        if bound_session is not None:
            await conn.execute(
                insert(sandbox_session_bindings).values(
                    session_id=bound_session,
                    principal_id=owner,
                    project_id=project_id,
                    revision=1,
                    created_at=1.0,
                    updated_at=1.0,
                )
            )


async def _policy_rows(engine) -> dict[str, tuple[str, int]]:
    async with engine.begin() as conn:
        rows = (await conn.execute(select(session_execution_policies))).mappings().all()
    return {row["session_id"]: (row["mode"], row["revision"]) for row in rows}


async def test_migrate_creates_tables_and_backfills_missing_sessions_manual(tmp_path):
    engine = await _database(tmp_path, exclude_policy_tables=True)
    try:
        await _seed_session(engine, "legacy-1")
        await _seed_session(engine, "legacy-2")
        async with engine.begin() as conn:
            names = {
                row[0]
                for row in (
                    await conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
                ).all()
            }
            assert not names & EXECUTION_POLICY_TABLE_NAMES
            await migrate(conn)
            names = {
                row[0]
                for row in (
                    await conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
                ).all()
            }
            assert EXECUTION_POLICY_TABLE_NAMES <= names
        assert await _policy_rows(engine) == {
            "legacy-1": ("manual", 1),
            "legacy-2": ("manual", 1),
        }
        # 已有模式行保守保留；缺行回填可重复执行。
        async with engine.begin() as conn:
            await conn.execute(
                update(session_execution_policies)
                .where(session_execution_policies.c.session_id == "legacy-1")
                .values(mode="bypass")
            )
            await conn.execute(
                session_execution_policies.delete().where(
                    session_execution_policies.c.session_id == "legacy-2"
                )
            )
            await migrate(conn)
        assert await _policy_rows(engine) == {
            "legacy-1": ("bypass", 1),
            "legacy-2": ("manual", 1),
        }
    finally:
        await engine.dispose()


async def test_initialize_writes_initial_mode_and_is_idempotent(tmp_path):
    engine = await _database(tmp_path)
    try:
        await _seed_session(engine, "s-init")
        await _seed_session(engine, "s-init-bypass")
        async with engine.begin() as conn:
            assert await initialize(conn, "s-init") is True
            assert await initialize(conn, "s-init") is False
            assert await initialize(conn, "s-init-bypass", mode="bypass") is True
            try:
                await initialize(conn, "s-init", mode="nonsense")
            except ServiceError as error:
                assert error.code == "invalid_input"
            else:
                raise AssertionError("expected invalid_input")
        assert await _policy_rows(engine) == {
            "s-init": ("manual", 1),
            "s-init-bypass": ("bypass", 1),
        }
    finally:
        await engine.dispose()


async def test_get_policy_defaults_and_owner_strict(tmp_path):
    engine = await _database(tmp_path)
    store = _store(engine)
    try:
        await _seed_session(engine, "s-own")
        view = await get_policy(store, principal=LOCAL, workspace="local", session_id="s-own")
        assert view.mode == "manual" and view.revision == 0
        assert view.project_id is None and view.project_access is None
        for principal, workspace in ((OTHER, "local"), (LOCAL, "other"), (CROSS_WORKSPACE, "nope")):
            try:
                await get_policy(
                    store, principal=principal, workspace=workspace, session_id="s-own"
                )
            except ServiceError as error:
                assert error.code == "not_found" and error.status == 404
            else:
                raise AssertionError("expected not_found")
        try:
            await get_policy(store, principal=LOCAL, workspace="local", session_id="missing")
        except ServiceError as error:
            assert error.code == "not_found"
        else:
            raise AssertionError("expected not_found")
    finally:
        await engine.dispose()


async def test_set_mode_legacy_session_confirms_manual_and_refuses_escalation(tmp_path):
    engine = await _database(tmp_path)
    store = _store(engine)
    try:
        await _seed_session(engine, "s-legacy")
        first = await set_mode(
            store,
            principal=LOCAL,
            workspace="local",
            session_id="s-legacy",
            request_key="legacy-put-1",
            expected_revision=0,
            mode="manual",
        )
        assert first.mode == "manual" and first.revision == 1 and first.updated_by == "local"
        for mode in ("auto", "bypass"):
            try:
                await set_mode(
                    store,
                    principal=LOCAL,
                    workspace="local",
                    session_id="s-legacy",
                    request_key="legacy-put-2",
                    expected_revision=1,
                    mode=mode,
                )
            except ServiceError as error:
                assert error.code == "permission_denied" and error.status == 403
            else:
                raise AssertionError("expected permission_denied")
        # 确认现状的重复PUT只登记台账，不推进版本。
        again = await set_mode(
            store,
            principal=LOCAL,
            workspace="local",
            session_id="s-legacy",
            request_key="legacy-put-3",
            expected_revision=1,
            mode="manual",
        )
        assert again.revision == 1 and again.updated_by == "local"
        assert (await _policy_rows(engine))["s-legacy"] == ("manual", 1)
    finally:
        await engine.dispose()


async def test_set_mode_with_project_cas_replay_and_key_conflicts(tmp_path):
    engine = await _database(tmp_path)
    store = _store(engine)
    try:
        await _seed_account(engine)
        await _seed_session(engine, "s-proj")
        await _seed_project(engine, "proj-1", access="write", bound_session="s-proj")
        view = await get_policy(store, principal=LOCAL, workspace="local", session_id="s-proj")
        assert view.project_id == "proj-1" and view.project_access == "write"
        assert view.mode == "manual" and view.revision == 0
        first = await set_mode(
            store,
            principal=LOCAL,
            workspace="local",
            session_id="s-proj",
            request_key="proj-put-1",
            expected_revision=0,
            mode="auto",
        )
        assert first.mode == "auto" and first.revision == 1
        second = await set_mode(
            store,
            principal=LOCAL,
            workspace="local",
            session_id="s-proj",
            request_key="proj-put-2",
            expected_revision=1,
            mode="bypass",
        )
        assert second.revision == 2 and second.mode == "bypass"
        # 相同键+相同内容重试返回已提交响应，不重写也不降级当前状态。
        replay = await set_mode(
            store,
            principal=LOCAL,
            workspace="local",
            session_id="s-proj",
            request_key="proj-put-1",
            expected_revision=0,
            mode="auto",
        )
        assert replay == first
        current = await get_policy(store, principal=LOCAL, workspace="local", session_id="s-proj")
        assert current.mode == "bypass" and current.revision == 2
        # 相同键不同内容拒绝；跨会话复用同键拒绝。
        await _seed_session(engine, "s-proj-other")
        with pytest.raises(ServiceError, match="key_conflict"):
            await set_mode(
                store,
                principal=LOCAL,
                workspace="local",
                session_id="s-proj",
                request_key="proj-put-1",
                expected_revision=2,
                mode="manual",
            )
        with pytest.raises(ServiceError, match="key_conflict"):
            await set_mode(
                store,
                principal=LOCAL,
                workspace="local",
                session_id="s-proj-other",
                request_key="proj-put-1",
                expected_revision=0,
                mode="manual",
            )
        assert store.healthy
        with pytest.raises(ServiceError, match="revision_conflict"):
            await set_mode(
                store,
                principal=LOCAL,
                workspace="local",
                session_id="s-proj",
                request_key="proj-put-3",
                expected_revision=1,
                mode="bypass",
            )
    finally:
        await engine.dispose()


async def test_project_access_gates_none_missing_and_read(tmp_path):
    engine = await _database(tmp_path)
    store = _store(engine)
    try:
        await _seed_account(engine)
        await _seed_session(engine, "s-none")
        await _seed_session(engine, "s-absent")
        await _seed_session(engine, "s-read")
        await _seed_session(engine, "s-cross")
        await _seed_project(engine, "proj-none", access="none", bound_session="s-none")
        await _seed_project(engine, "proj-absent", access=None, bound_session="s-absent")
        await _seed_project(engine, "proj-read", access="read", bound_session="s-read")
        for session_id in ("s-none", "s-absent"):
            for call in (get_policy, None):
                try:
                    if call is None:
                        await set_mode(
                            store,
                            principal=LOCAL,
                            workspace="local",
                            session_id=session_id,
                            request_key="none-put",
                            expected_revision=0,
                            mode="manual",
                        )
                    else:
                        await call(store, principal=LOCAL, workspace="local", session_id=session_id)
                except ServiceError as error:
                    assert error.code == "not_found" and error.status == 404
                else:
                    raise AssertionError("expected not_found")
        # read项目可改模式；bypass不改变项目能力上限的呈现。
        view = await get_policy(store, principal=LOCAL, workspace="local", session_id="s-read")
        assert view.project_access == "read" and view.mode == "manual"
        switched = await set_mode(
            store,
            principal=LOCAL,
            workspace="local",
            session_id="s-read",
            request_key="read-put-1",
            expected_revision=0,
            mode="bypass",
        )
        assert switched.mode == "bypass" and switched.revision == 1
        assert switched.project_access == "read"
        # 绑定跨工作区按事实损坏处理。
        await _seed_account(engine, "member-2")
        async with engine.begin() as conn:
            await conn.execute(
                update(sandbox_projects)
                .where(sandbox_projects.c.project_id == "proj-read")
                .values(workspace_id="elsewhere")
            )
        try:
            await get_policy(
                store, principal=CROSS_WORKSPACE, workspace="local", session_id="s-read"
            )
        except ServiceError as error:
            assert error.code == "facts_corrupt"
        else:
            raise AssertionError("expected facts_corrupt")
    finally:
        await engine.dispose()


async def test_allow_rules_match_exact_digests_only_and_clear_by_session(tmp_path):
    engine = await _database(tmp_path)
    try:
        await _seed_account(engine)
        await _seed_session(engine, "s-rule-legacy")
        await _seed_session(engine, "s-rule-1")
        await _seed_session(engine, "s-rule-2")
        await _seed_project(engine, "proj-r1", access="write", bound_session="s-rule-1")
        await _seed_project(engine, "proj-r2", access="write", bound_session="s-rule-2")
        async with engine.begin() as conn:
            # 未绑定项目的会话没有可承接的项目能力，不授予规则。
            try:
                await record_allow_rule(
                    conn,
                    "s-rule-legacy",
                    tool_name="shell",
                    tool_hash=_H1,
                    arguments_hash=_ARGS_X,
                )
            except ServiceError as error:
                assert error.code == "permission_denied" and error.status == 403
            else:
                raise AssertionError("expected permission_denied")
            assert await record_allow_rule(
                conn, "s-rule-1", tool_name="shell", tool_hash=_H1, arguments_hash=_ARGS_X
            )
            assert not await record_allow_rule(
                conn, "s-rule-1", tool_name="shell", tool_hash=_H1, arguments_hash=_ARGS_X
            )
            assert await allow_rule_satisfied(
                conn, "s-rule-1", tool_hash=_H1, arguments_hash=_ARGS_X
            )
            # 严格全等：参数变化、工具变化或其他会话都不满足。
            assert not await allow_rule_satisfied(
                conn, "s-rule-1", tool_hash=_H1, arguments_hash=_ARGS_Y
            )
            assert not await allow_rule_satisfied(
                conn, "s-rule-1", tool_hash=_H2, arguments_hash=_ARGS_X
            )
            assert not await allow_rule_satisfied(
                conn, "s-rule-2", tool_hash=_H1, arguments_hash=_ARGS_X
            )
            # 畸形digest永不满足；登记畸形digest被拒绝。
            assert not await allow_rule_satisfied(
                conn, "s-rule-1", tool_hash="short", arguments_hash=_ARGS_X
            )
            for tool_hash, arguments_hash in (("short", _ARGS_X), (_H1, "*" * 64)):
                try:
                    await record_allow_rule(
                        conn,
                        "s-rule-1",
                        tool_name="shell",
                        tool_hash=tool_hash,
                        arguments_hash=arguments_hash,
                    )
                except ServiceError as error:
                    assert error.code == "invalid_input"
                else:
                    raise AssertionError("expected invalid_input")
            assert await clear_allow_rules(conn, "s-rule-1") == 1
            assert await clear_allow_rules(conn, "s-rule-1") == 0
            assert not await allow_rule_satisfied(
                conn, "s-rule-1", tool_hash=_H1, arguments_hash=_ARGS_X
            )
            # digest口径与approval arguments_hash一致：规范化JSON等价。
            assert arguments_digest('{"b":2,"a":1}') == arguments_digest('{"a":1,"b":2}')
            assert arguments_digest('{"a":1}') != arguments_digest('{"a":2}')
        # 换绑项目后旧规则失效；新上下文重新授予刷新上下文。
        async with engine.begin() as conn:
            await conn.execute(
                update(sandbox_session_bindings)
                .where(sandbox_session_bindings.c.session_id == "s-rule-2")
                .values(project_id="proj-r1")
            )
            assert not await allow_rule_satisfied(
                conn, "s-rule-2", tool_hash=_H1, arguments_hash=_ARGS_X
            )
            assert await record_allow_rule(
                conn, "s-rule-2", tool_name="shell", tool_hash=_H1, arguments_hash=_ARGS_X
            )
            assert await allow_rule_satisfied(
                conn, "s-rule-2", tool_hash=_H1, arguments_hash=_ARGS_X
            )
        # ACL revision变化（降权或恢复）使旧规则失效；access=none不能授予。
        async with engine.begin() as conn:
            await conn.execute(
                update(project_memberships)
                .where(project_memberships.c.project_id == "proj-r1")
                .values(revision=2)
            )
            assert not await allow_rule_satisfied(
                conn, "s-rule-1", tool_hash=_H1, arguments_hash=_ARGS_X
            )
            assert await record_allow_rule(
                conn, "s-rule-1", tool_name="shell", tool_hash=_H1, arguments_hash=_ARGS_X
            )
            assert await allow_rule_satisfied(
                conn, "s-rule-1", tool_hash=_H1, arguments_hash=_ARGS_X
            )
            await conn.execute(
                update(project_memberships)
                .where(project_memberships.c.project_id == "proj-r1")
                .values(access="none", revision=3)
            )
            assert not await allow_rule_satisfied(
                conn, "s-rule-1", tool_hash=_H1, arguments_hash=_ARGS_X
            )
            try:
                await record_allow_rule(
                    conn, "s-rule-1", tool_name="shell", tool_hash=_H2, arguments_hash=_ARGS_X
                )
            except ServiceError as error:
                assert error.code == "permission_denied"
            else:
                raise AssertionError("expected permission_denied")
    finally:
        await engine.dispose()


async def test_mode_reads_corrupt_rows_facts_corrupt(tmp_path):
    engine = await _database(tmp_path)
    store = _store(engine)
    try:
        await _seed_session(engine, "s-corrupt")
        async with engine.begin() as conn:
            assert await get_mode(conn, "s-corrupt") == "manual"
            snapshot = await read_mode_snapshot(conn, "s-corrupt")
            assert (snapshot.mode, snapshot.revision, snapshot.updated_at) == ("manual", 0, 0.0)
            await initialize(conn, "s-corrupt", mode="auto")
            snapshot = await read_mode_snapshot(conn, "s-corrupt")
            assert (snapshot.mode, snapshot.revision) == ("auto", 1)
            await conn.execute(
                update(session_execution_policies)
                .where(session_execution_policies.c.session_id == "s-corrupt")
                .values(mode="yolo")
            )
            for reader in (get_mode, None):
                try:
                    if reader is None:
                        await read_mode_snapshot(conn, "s-corrupt")
                    else:
                        await reader(conn, "s-corrupt")
                except ServiceError as error:
                    assert error.code == "facts_corrupt"
                else:
                    raise AssertionError("expected facts_corrupt")
        try:
            await get_policy(store, principal=LOCAL, workspace="local", session_id="s-corrupt")
        except ServiceError as error:
            assert error.code == "facts_corrupt"
        else:
            raise AssertionError("expected facts_corrupt")
    finally:
        await engine.dispose()


async def test_execution_policy_api_get_put_replay_and_conflicts(tmp_path):
    database = tmp_path / "policy-api.sqlite"
    token = await bootstrap_token(database)
    events: list[object] = []
    unsubscribe = execution_policy_notifications.subscribe(events.append)
    try:
        async with client_for(database, token) as (app, client):
            actor = app.identities.authenticate("Bearer " + token)
            workspace = actor.memberships[0].workspace_id
            session = await app.sessions.create(
                actor, workspace, SessionCreate(request_key="policy-api")
            )
            path = f"/v2/workspaces/{workspace}/sessions/{session.id}/execution-policy"
            read = await client.get(path)
            assert read.status_code == 200, read.text
            body = read.json()
            # 创建者write项目的默认项目新会话初始bypass，且绑定项目已呈现。
            assert body["mode"] == "bypass" and body["revision"] == 1
            assert body["project_id"] is not None and body["project_access"] == "write"
            assert len(events) == 0  # 创建事务初始化不发通知；PUT才发布。
            first = await client.put(
                path, json={"mode": "manual", "expected_revision": 1, "request_key": "api-put-1"}
            )
            assert first.status_code == 200, first.text
            assert first.json()["mode"] == "manual" and first.json()["revision"] == 2
            assert len(events) == 1 and events[0].session_id == session.id
            second = await client.put(
                path, json={"mode": "auto", "expected_revision": 2, "request_key": "api-put-2"}
            )
            assert second.status_code == 200, second.text
            assert second.json()["revision"] == 3
            # 相同键相同内容重试返回已提交响应；不重写也不降级当前状态。
            replayed = await client.put(
                path, json={"mode": "manual", "expected_revision": 1, "request_key": "api-put-1"}
            )
            assert replayed.status_code == 200, replayed.text
            assert replayed.json() == first.json()
            # 幂等重放也发布通知：订阅方以重读持久数据为准，发布内容不是状态。
            assert len(events) == 3
            current = await client.get(path)
            assert current.json()["mode"] == "auto" and current.json()["revision"] == 3
            # 相同键不同内容拒绝；过期版本拒绝。
            conflict = await client.put(
                path, json={"mode": "manual", "expected_revision": 3, "request_key": "api-put-1"}
            )
            assert conflict.status_code == 409, conflict.text
            assert conflict.json()["error"]["code"] == "key_conflict"
            stale = await client.put(
                path, json={"mode": "manual", "expected_revision": 1, "request_key": "api-put-3"}
            )
            assert stale.status_code == 409, stale.text
            assert stale.json()["error"]["code"] == "revision_conflict"
            unknown = await client.get(
                f"/v2/workspaces/{workspace}/sessions/missing/execution-policy"
            )
            assert unknown.status_code == 404, unknown.text
            bad_body = await client.put(
                path, json={"mode": "nonsense", "expected_revision": 3, "request_key": "api-bad"}
            )
            assert bad_body.status_code == 422, bad_body.text
            # 确认现状的重复PUT成功但不再推进版本，通知照常发布。
            again = await client.put(
                path, json={"mode": "auto", "expected_revision": 3, "request_key": "api-put-4"}
            )
            assert again.status_code == 200 and again.json()["revision"] == 3
            assert len(events) == 4
            unsubscribe()
            await client.put(
                path, json={"mode": "manual", "expected_revision": 3, "request_key": "api-put-5"}
            )
            assert len(events) == 4
    finally:
        unsubscribe()


async def test_execution_policy_api_legacy_session_reads_manual_and_refuses_escalation(tmp_path):
    database = tmp_path / "policy-api-legacy.sqlite"
    token = await bootstrap_token(database)
    async with client_for(database, token) as (app, client):
        actor = app.identities.authenticate("Bearer " + token)
        workspace = actor.memberships[0].workspace_id
        # 直接落库一条未绑定项目的旧会话：可读manual，PUT拒绝任何升权。
        with sqlite3.connect(database) as connection:
            connection.execute(
                "INSERT INTO sessions (id, workspace_id, owner_id, create_key, request_hash,"
                " title, target_key, target, context_policy, revision, status, dispatch_paused,"
                " created_at, closed_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'open', 0, 1.0, NULL)",
                (
                    "legacy-1",
                    workspace,
                    actor.principal_id,
                    "k-legacy-1",
                    "0" * 64,
                    "旧会话",
                    "demo",
                    "{}",
                    "{}",
                ),
            )
            connection.commit()
        path = f"/v2/workspaces/{workspace}/sessions/legacy-1/execution-policy"
        read = await client.get(path)
        assert read.status_code == 200, read.text
        body = read.json()
        assert body["mode"] == "manual" and body["revision"] == 0
        assert body["project_id"] is None and body["project_access"] is None
        for mode in ("auto", "bypass"):
            escalated = await client.put(
                path,
                json={"mode": mode, "expected_revision": 0, "request_key": "api-legacy-esc"},
            )
            assert escalated.status_code == 403, escalated.text
            assert escalated.json()["error"]["code"] == "permission_denied"
        # 确认manual现状可以把虚拟状态落库为revision 1。
        persisted = await client.put(
            path,
            json={"mode": "manual", "expected_revision": 0, "request_key": "api-legacy-put"},
        )
        assert persisted.status_code == 200, persisted.text
        assert persisted.json()["mode"] == "manual" and persisted.json()["revision"] == 1
        again = await client.put(
            path,
            json={"mode": "manual", "expected_revision": 1, "request_key": "api-legacy-put-2"},
        )
        assert again.status_code == 200 and again.json()["revision"] == 1


async def test_execution_policy_api_with_project_binding(tmp_path):
    database = tmp_path / "policy-api-project.sqlite"
    token = await bootstrap_token(database)
    async with client_for(database, token) as (app, client):
        actor = app.identities.authenticate("Bearer " + token)
        workspace = actor.memberships[0].workspace_id
        session = await app.sessions.create(
            actor, workspace, SessionCreate(request_key="policy-api-project")
        )
        # 新会话已绑定默认项目（创建者write→初始bypass）。
        path = f"/v2/workspaces/{workspace}/sessions/{session.id}/execution-policy"
        read = await client.get(path)
        assert read.status_code == 200, read.text
        body = read.json()
        project_id = body["project_id"]
        assert project_id is not None and body["project_access"] == "write"
        assert body["mode"] == "bypass"
        revision = body["revision"]
        switched = await client.put(
            path,
            json={"mode": "manual", "expected_revision": revision, "request_key": "api-proj-put"},
        )
        assert switched.status_code == 200, switched.text
        assert switched.json()["mode"] == "manual" and switched.json()["revision"] == revision + 1
        # ACL撤权后按无权限处理：不泄露会话策略存在性。
        with sqlite3.connect(database) as connection:
            connection.execute(
                "UPDATE project_memberships SET access='none', revision=revision+1"
                " WHERE project_id=? AND principal_id=?",
                (project_id, actor.principal_id),
            )
            connection.commit()
        revoked = await client.get(path)
        assert revoked.status_code == 404, revoked.text
        blocked = await client.put(
            path,
            json={
                "mode": "bypass",
                "expected_revision": revision + 1,
                "request_key": "api-proj-put-2",
            },
        )
        assert blocked.status_code == 404, blocked.text


def test_execution_policy_router_mounts_without_conflict():
    # 路由装配冒烟：openapi schema中执行策略GET/PUT就位且路径与约定一致。
    schema = create_app().openapi()["paths"]
    policy = schema[
        "/v2/workspaces/{workspace_id}/sessions/{session_id}/execution-policy"
    ]
    assert set(policy) == {"get", "put"}
    rules = schema[
        "/v2/workspaces/{workspace_id}/sessions/{session_id}/execution-policy/allow-rules"
    ]
    assert set(rules) == {"get", "delete"}
    project = schema[
        "/v2/workspaces/{workspace_id}/sessions/{session_id}/execution-policy/project"
    ]
    assert set(project) == {"get"}



def test_approval_reply_scope_defaults_and_denial_guard():
    # 缺省once向后兼容；只有批准可以携带session范围，拒绝携带被合约层拒绝。
    reply = ApprovalReply(
        request_key="k1", call_id="c1", arguments_hash=_ARGS_X, tool_hash=_H1, allow=True
    )
    assert reply.scope == "once"
    session_reply = ApprovalReply(
        request_key="k2",
        call_id="c2",
        arguments_hash=_ARGS_X,
        tool_hash=_H1,
        allow=True,
        scope="session",
    )
    assert session_reply.scope == "session"
    with pytest.raises(ValidationError):
        ApprovalReply(
            request_key="k3",
            call_id="c3",
            arguments_hash=_ARGS_X,
            tool_hash=_H1,
            allow=False,
            scope="session",
        )


async def test_session_scope_context_owner_strict_and_project_gated(tmp_path):
    engine = await _database(tmp_path)
    try:
        store = _store(engine)
        await _seed_account(engine)
        await _seed_account(engine, "member-2")
        await _seed_session(engine, "s-scope")
        await _seed_session(engine, "s-scope-legacy")
        await _seed_project(engine, "proj-scope", access="write", bound_session="s-scope")
        async with engine.begin() as conn:
            assert await record_allow_rule(
                conn, "s-scope", tool_name="shell", tool_hash=_H1, arguments_hash=_ARGS_X
            )
        assert (
            await count_allow_rules(
                store, principal=LOCAL, workspace="local", session_id="s-scope"
            )
            == 1
        )
        # 非当前会话owner严格404，不使用workspace admin例外。
        with pytest.raises(ServiceError) as intruder:
            await count_allow_rules(
                store, principal=OTHER, workspace="local", session_id="s-scope"
            )
        assert intruder.value.status == 404
        # 预检返回会话当前项目上下文；未绑定旧会话没有可承接的项目能力。
        assert await session_allow_rule_context(store, "s-scope") == ("proj-scope", 1)
        assert await session_allow_rule_context(store, "s-scope-legacy") is None
        # 项目详情最小只读视图：真实项目行与真实创建者，不按会话owner猜。
        detail = await get_session_project(
            store, principal=LOCAL, workspace="local", session_id="s-scope"
        )
        assert detail == {
            "session_id": "s-scope",
            "project_id": "proj-scope",
            "title": "P",
            "owner_id": "local",
            "project_access": "write",
            "is_creator": True,
            "sandbox_status": "unprovisioned",
        }
        # 项目创建者与会话owner不同人时如实呈现。
        async with engine.begin() as conn:
            await conn.execute(
                update(sandbox_projects)
                .where(sandbox_projects.c.project_id == "proj-scope")
                .values(principal_id="member-2")
            )
        changed = await get_session_project(
            store, principal=LOCAL, workspace="local", session_id="s-scope"
        )
        assert changed["owner_id"] == "member-2" and changed["is_creator"] is False
    finally:
        await engine.dispose()


async def test_allow_rules_api_count_clear_replay_and_conflicts(tmp_path):
    database = tmp_path / "policy-rules-api.sqlite"
    token = await bootstrap_token(database)
    async with client_for(database, token) as (app, client):
        actor = app.identities.authenticate("Bearer " + token)
        workspace = actor.memberships[0].workspace_id
        session = await app.sessions.create(
            actor, workspace, SessionCreate(request_key="rules-api")
        )
        policy_path = f"/v2/workspaces/{workspace}/sessions/{session.id}/execution-policy"
        rules_path = policy_path + "/allow-rules"
        empty = await client.get(rules_path)
        assert empty.status_code == 200, empty.text
        assert empty.json() == {"session_id": session.id, "rule_count": 0}
        async with app.store.engine.begin() as conn:
            assert await record_allow_rule(
                conn, session.id, tool_name="shell", tool_hash=_H1, arguments_hash=_ARGS_X
            )
            assert await record_allow_rule(
                conn, session.id, tool_name="write", tool_hash=_H2, arguments_hash=_ARGS_Y
            )
        listed = await client.get(rules_path)
        assert listed.status_code == 200 and listed.json()["rule_count"] == 2
        first = await client.request("DELETE", rules_path, json={"request_key": "rules-clear-1"})
        assert first.status_code == 200, first.text
        assert first.json() == {
            "session_id": session.id,
            "cleared_count": 2,
            "rule_count": 0,
        }
        after = await client.get(rules_path)
        assert after.json()["rule_count"] == 0
        # 同键重放返回首次结果：期间新增的规则不会被重试误清。
        async with app.store.engine.begin() as conn:
            assert await record_allow_rule(
                conn, session.id, tool_name="shell", tool_hash=_H1, arguments_hash=_ARGS_X
            )
        replayed = await client.request(
            "DELETE", rules_path, json={"request_key": "rules-clear-1"}
        )
        assert replayed.status_code == 200 and replayed.json() == first.json()
        kept = await client.get(rules_path)
        assert kept.json()["rule_count"] == 1
        # 与mode PUT同键异内容互为key_conflict。
        mode = await client.put(
            policy_path,
            json={"mode": "manual", "expected_revision": 1, "request_key": "rules-mode-1"},
        )
        assert mode.status_code == 200, mode.text
        forward = await client.request(
            "DELETE", rules_path, json={"request_key": "rules-mode-1"}
        )
        assert forward.status_code == 409, forward.text
        assert forward.json()["error"]["code"] == "key_conflict"
        backward = await client.put(
            policy_path,
            json={"mode": "manual", "expected_revision": 2, "request_key": "rules-clear-1"},
        )
        assert backward.status_code == 409, backward.text
        assert backward.json()["error"]["code"] == "key_conflict"
        # 未知会话404。
        missing = await client.request(
            "DELETE",
            f"/v2/workspaces/{workspace}/sessions/missing/execution-policy/allow-rules",
            json={"request_key": "rules-clear-2"},
        )
        assert missing.status_code == 404, missing.text
        # 已批准项目详情：真实项目行，无开发环境记录时unprovisioned。
        policy_view = await client.get(policy_path)
        project_id = policy_view.json()["project_id"]
        assert project_id is not None
        project_view = await client.get(policy_path + "/project")
        assert project_view.status_code == 200, project_view.text
        detail = project_view.json()
        assert detail["project_id"] == project_id
        assert detail["owner_id"] == actor.principal_id
        assert detail["is_creator"] is True and detail["project_access"] == "write"
        assert detail["sandbox_status"] == "unprovisioned"
        # 未绑定旧会话：规则可读0条，项目详情404。
        with sqlite3.connect(database) as connection:
            connection.execute(
                "INSERT INTO sessions (id, workspace_id, owner_id, create_key, request_hash,"
                " title, target_key, target, context_policy, revision, status, dispatch_paused,"
                " created_at, closed_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'open', 0, 1.0, NULL)",
                (
                    "legacy-rules",
                    workspace,
                    actor.principal_id,
                    "k-legacy-rules",
                    "0" * 64,
                    "旧会话",
                    "demo",
                    "{}",
                    "{}",
                ),
            )
            connection.commit()
        legacy_base = f"/v2/workspaces/{workspace}/sessions/legacy-rules/execution-policy"
        legacy_rules = await client.get(legacy_base + "/allow-rules")
        assert legacy_rules.status_code == 200
        assert legacy_rules.json() == {"session_id": "legacy-rules", "rule_count": 0}
        legacy_project = await client.get(legacy_base + "/project")
        assert legacy_project.status_code == 404, legacy_project.text
