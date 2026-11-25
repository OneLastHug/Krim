"""项目侧栏：项目绑定决定分组，主体布局仅保存别名、顺序与置顶。

schema 21 起 group_id 必须等于真实 project_id；历史无绑定会话保留在默认区。
引用按当前授权核验，布局写入不能改变会话的项目绑定。
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.security import PrincipalContext
from krim_agent.sessions.types import (
    SessionSidebarDocument,
    SessionSidebarWrite,
    SidebarGroup,
    SidebarSessionEntry,
)

from .account_models import IDENTITY_META_ID, account_meta, account_workspaces, accounts
from .models import sessions
from .project_membership_models import project_memberships
from .sandbox_models import sandbox_projects, sandbox_session_bindings
from .sessions import read
from .sidebar_models import session_sidebars

if TYPE_CHECKING:
    from .sqlite import SQLiteStore

SIDEBAR_SCHEMA = 17  # session_sidebars 表自 schema 17 引入；更早版本能力不存在
MAX_GROUPS = 100
MAX_SESSIONS = 10000


def require(store: SQLiteStore) -> None:
    if store.schema_version < SIDEBAR_SCHEMA:
        raise ServiceError("schema_upgrade_required")


def _document(row: Any | None) -> SessionSidebarDocument:
    if row is None:
        return SessionSidebarDocument(revision=0, groups=(), sessions=())
    try:
        result = SessionSidebarDocument(
            revision=row["revision"],
            groups=tuple(SidebarGroup.model_validate(item) for item in row["groups"]),
            sessions=tuple(SidebarSessionEntry.model_validate(item) for item in row["sessions"]),
        )
        # 存量布局按写入同规则整体验证；坏数据（重复/悬挂/成环/超深）不原样回放。
        SessionSidebarWrite(groups=result.groups, sessions=result.sessions, expected_revision=0)
    except ValueError:
        raise ServiceError("facts_corrupt") from None
    if result.revision < 0:
        raise ServiceError("facts_corrupt")
    return result


async def _workspace_role(
    conn: AsyncConnection, principal: PrincipalContext, workspace: str
) -> str:
    """事务内当前授权：非托管库以上下文为准；托管主体即时核对账号与成员关系。"""
    meta = (
        await conn.execute(select(account_meta.c.body).where(account_meta.c.id == IDENTITY_META_ID))
    ).scalar_one_or_none()
    if not (isinstance(meta, dict) and meta.get("mode") == "managed"):
        return principal.member(workspace).role
    row = (
        (
            await conn.execute(
                select(accounts.c.status, accounts.c.authorization_generation).where(
                    accounts.c.principal_id == principal.principal_id
                )
            )
        )
        .mappings()
        .first()
    )
    if (
        row is None
        or row["status"] != "active"
        or row["authorization_generation"] != principal.generation
    ):
        raise ServiceError("permission_denied", 403)
    membership = (
        (
            await conn.execute(
                select(account_workspaces.c.role).where(
                    account_workspaces.c.principal_id == principal.principal_id,
                    account_workspaces.c.workspace_id == workspace,
                )
            )
        )
        .mappings()
        .first()
    )
    if membership is None:
        raise ServiceError("not_found", 404)
    return str(membership["role"])


async def _visible_session_ids(
    conn: AsyncConnection,
    workspace: str,
    principal_id: str,
    admin: bool,
    session_ids: tuple[str, ...],
) -> set[str]:
    """当前主体在本工作区实际可见的会话集合（存在且归属允许）。"""
    if not session_ids:
        return set()
    rows = (
        (
            await conn.execute(
                select(sessions.c.id, sessions.c.owner_id).where(
                    sessions.c.workspace_id == workspace,
                    sessions.c.id.in_(sorted(set(session_ids))),
                )
            )
        )
        .mappings()
        .all()
    )
    return {row["id"] for row in rows if admin or row["owner_id"] == principal_id}


async def _assert_visible_sessions(
    conn: AsyncConnection,
    workspace: str,
    principal_id: str,
    admin: bool,
    session_ids: tuple[str, ...],
) -> None:
    """写入引用的会话必须全部可见；不可见与不存在同响应，不泄露存在性。"""
    visible = await _visible_session_ids(conn, workspace, principal_id, admin, session_ids)
    for session_id in session_ids:
        if session_id not in visible:
            raise ServiceError("not_found", 404)


PROJECT_GROUP_SCHEMA = 21


async def _project_facts(
    conn: AsyncConnection,
    workspace: str,
    principal_id: str,
) -> tuple[dict[str, str], dict[str, str | None]]:
    """Read authoritative bindings and ACLs in the same snapshot as the layout."""
    project_rows = (
        await conn.execute(
            select(sandbox_projects.c.project_id, sandbox_projects.c.title)
            .join(
                project_memberships,
                project_memberships.c.project_id == sandbox_projects.c.project_id,
            )
            .where(
                sandbox_projects.c.workspace_id == workspace,
                project_memberships.c.principal_id == principal_id,
                project_memberships.c.access.in_(["read", "write"]),
            )
            .order_by(sandbox_projects.c.created_at, sandbox_projects.c.project_id)
        )
    ).all()
    projects = dict(project_rows)
    query = (
        select(sessions.c.id, sandbox_session_bindings.c.project_id)
        .outerjoin(
            sandbox_session_bindings,
            sandbox_session_bindings.c.session_id == sessions.c.id,
        )
        .where(sessions.c.workspace_id == workspace)
    )
    # Match the session list's schema19+ owner scope, including administrators.
    query = query.where(sessions.c.owner_id == principal_id)
    rows = (await conn.execute(query.order_by(sessions.c.created_at, sessions.c.id))).all()
    actual = dict(rows)
    # A bound session may never be rendered as unbound because its project was hidden.
    actual = {sid: pid for sid, pid in actual.items() if pid is None or pid in projects}
    return projects, actual


def _project_document(
    stored: SessionSidebarDocument,
    projects: dict[str, str],
    actual: dict[str, str | None],
) -> SessionSidebarDocument:
    """Deterministic legacy projection; never guesses or changes a binding.

    Explicit project aliases win. Otherwise the first old group containing a
    project's session supplies its alias. Mixed groups split, same-project groups
    merge, and cross-project parents detach while order and pins survive.
    """
    titles = {group.id: group.title for group in stored.groups if group.id in projects}
    order: list[str] = []
    by_group: dict[str, list[str]] = {}
    for entry in stored.sessions:
        pid = actual.get(entry.session_id)
        if pid is not None and entry.group_id is not None:
            by_group.setdefault(entry.group_id, []).append(pid)
    for group in stored.groups:
        members = [group.id] if group.id in projects else by_group.get(group.id, [])
        for pid in members:
            if pid not in order:
                order.append(pid)
            titles.setdefault(pid, group.title)
    order.extend(pid for pid in projects if pid not in order)
    entries = {entry.session_id: entry for entry in stored.sessions if entry.session_id in actual}
    for sid, pid in actual.items():
        entries.setdefault(sid, SidebarSessionEntry(session_id=sid, group_id=pid))
    return SessionSidebarDocument(
        revision=stored.revision,
        groups=tuple(SidebarGroup(id=pid, title=titles.get(pid, projects[pid])) for pid in order),
        sessions=tuple(
            entry.model_copy(
                update={
                    "group_id": actual[sid],
                    "parent_id": entry.parent_id
                    if entry.parent_id in actual and actual[entry.parent_id] == actual[sid]
                    else None,
                }
            )
            for sid, entry in entries.items()
        ),
    )


async def migrate_project_groups(conn: AsyncConnection) -> None:
    """Upgrade only layout metadata; session facts, messages and bindings stay intact."""
    rows = (await conn.execute(select(session_sidebars))).mappings().all()
    for row in rows:
        try:
            stored = _document(row)
        except ServiceError:
            # A broken preference must not prevent opening otherwise valid session facts.
            # Keep the original bytes; its own read/write still reports facts_corrupt.
            continue
        # Migrate using facts without dropping temporarily invisible/removed members.
        project_rows = (
            await conn.execute(
                select(
                    sandbox_projects.c.project_id,
                    sandbox_projects.c.title,
                )
                .where(sandbox_projects.c.workspace_id == row["workspace_id"])
                .order_by(sandbox_projects.c.created_at, sandbox_projects.c.project_id)
            )
        ).all()
        actual = dict(
            (
                await conn.execute(
                    select(
                        sessions.c.id,
                        sandbox_session_bindings.c.project_id,
                    )
                    .outerjoin(
                        sandbox_session_bindings,
                        sandbox_session_bindings.c.session_id == sessions.c.id,
                    )
                    .where(sessions.c.workspace_id == row["workspace_id"])
                    .order_by(sessions.c.created_at, sessions.c.id)
                )
            ).all()
        )
        # Do not add other principals' sessions to a persisted personal layout.
        refs = {entry.session_id for entry in stored.sessions}
        projected = _project_document(
            stored, dict(project_rows), {sid: pid for sid, pid in actual.items() if sid in refs}
        )
        # Only groups actually referenced or previously named by project ID are migrated.
        keep = {entry.group_id for entry in projected.sessions} | {
            group.id for group in stored.groups if group.id in dict(project_rows)
        }
        groups = tuple(group for group in projected.groups if group.id in keep)
        if groups != stored.groups or projected.sessions != stored.sessions:
            await conn.execute(
                update(session_sidebars)
                .where(
                    session_sidebars.c.principal_id == row["principal_id"],
                    session_sidebars.c.workspace_id == row["workspace_id"],
                )
                .values(
                    groups=[group.model_dump(mode="json") for group in groups],
                    sessions=[entry.model_dump(mode="json") for entry in projected.sessions],
                    revision=stored.revision + 1,
                    updated_at=time.time(),
                )
            )


async def sync_project_binding(
    conn: AsyncConnection, session_id: str, project_id: str | None
) -> None:
    """Invalidate stale layouts atomically with a binding change, across viewers."""
    workspace = await conn.scalar(
        select(sessions.c.workspace_id).where(sessions.c.id == session_id)
    )
    rows = (
        (
            await conn.execute(
                select(session_sidebars).where(
                    session_sidebars.c.workspace_id == workspace,
                )
            )
        )
        .mappings()
        .all()
    )
    for row in rows:
        try:
            stored = _document(row)
        except ServiceError:
            # Invalid personal preferences cannot block another session's project change.
            continue
        ids = {entry.session_id: entry for entry in stored.sessions}
        if session_id not in ids:
            continue
        changed = []
        old_group = ids[session_id].group_id
        for entry in stored.sessions:
            parent = entry.parent_id
            if entry.session_id == session_id:
                parent = parent if parent in ids and ids[parent].group_id == project_id else None
                entry = entry.model_copy(update={"group_id": project_id, "parent_id": parent})
            elif parent == session_id and old_group != project_id:
                entry = entry.model_copy(update={"parent_id": None})
            changed.append(entry)
        groups = list(stored.groups)
        if project_id is not None and project_id not in {group.id for group in groups}:
            title = await conn.scalar(
                select(sandbox_projects.c.title).where(
                    sandbox_projects.c.project_id == project_id,
                )
            )
            if title is None:
                raise ServiceError("facts_corrupt")
            groups.append(SidebarGroup(id=project_id, title=title))
        await conn.execute(
            update(session_sidebars)
            .where(
                session_sidebars.c.principal_id == row["principal_id"],
                session_sidebars.c.workspace_id == workspace,
            )
            .values(
                groups=[group.model_dump(mode="json") for group in groups],
                sessions=[entry.model_dump(mode="json") for entry in changed],
                revision=stored.revision + 1,
                updated_at=time.time(),
            )
        )


async def get(
    store: SQLiteStore, *, principal: PrincipalContext, workspace: str
) -> SessionSidebarDocument:
    require(store)
    principal.member(workspace)

    async def operation(conn: AsyncConnection) -> SessionSidebarDocument:
        role = await _workspace_role(conn, principal, workspace)
        row = (
            (
                await conn.execute(
                    select(session_sidebars).where(
                        session_sidebars.c.principal_id == principal.principal_id,
                        session_sidebars.c.workspace_id == workspace,
                    )
                )
            )
            .mappings()
            .first()
        )
        stored = _document(row)
        if store.schema_version >= PROJECT_GROUP_SCHEMA:
            projects, actual = await _project_facts(
                conn,
                workspace,
                principal.principal_id,
            )
            return _project_document(stored, projects, actual)
        # 读路径按当前角色筛选失效/不可见引用（降权后旧布局仍可读、可改存）：
        # 丢弃不可见条目，父条目被丢弃的子条目 parent 置 null，组顺序与
        # revision（CAS 基线）原样保留，不回写存储。
        visible = await _visible_session_ids(
            conn,
            workspace,
            principal.principal_id,
            role == "admin",
            tuple(entry.session_id for entry in stored.sessions),
        )
        kept = [entry for entry in stored.sessions if entry.session_id in visible]
        kept_ids = {entry.session_id for entry in kept}
        return SessionSidebarDocument(
            revision=stored.revision,
            groups=stored.groups,
            sessions=tuple(
                entry.model_copy(
                    update={"parent_id": entry.parent_id if entry.parent_id in kept_ids else None}
                )
                for entry in kept
            ),
        )

    return await read(store, operation)


async def put(
    store: SQLiteStore,
    *,
    principal: PrincipalContext,
    workspace: str,
    body: SessionSidebarWrite,
) -> SessionSidebarDocument:
    require(store)
    principal.member(workspace)
    if store.schema_version < PROJECT_GROUP_SCHEMA and (
        len(body.groups) > MAX_GROUPS or len(body.sessions) > MAX_SESSIONS
    ):
        raise ServiceError("invalid_input", 422)
    expected = body.expected_revision
    committed: SessionSidebarDocument | None = None

    async def operation(conn: AsyncConnection) -> SessionSidebarDocument:
        nonlocal committed
        role = await _workspace_role(conn, principal, workspace)
        await _assert_visible_sessions(
            conn,
            workspace,
            principal.principal_id,
            role == "admin",
            tuple(entry.session_id for entry in body.sessions),
        )
        row = (
            (
                await conn.execute(
                    select(
                        session_sidebars.c.revision,
                        session_sidebars.c.groups,
                        session_sidebars.c.sessions,
                    ).where(
                        session_sidebars.c.principal_id == principal.principal_id,
                        session_sidebars.c.workspace_id == workspace,
                    )
                )
            )
            .mappings()
            .first()
        )
        current = _document(row)
        if current.revision != expected:
            raise ServiceError("sidebar_revision_conflict", 409)
        saved = SessionSidebarDocument(
            revision=current.revision + 1, groups=body.groups, sessions=body.sessions
        )
        if store.schema_version >= PROJECT_GROUP_SCHEMA:
            projects, actual = await _project_facts(
                conn,
                workspace,
                principal.principal_id,
            )
            if any(group.id not in projects for group in body.groups):
                raise ServiceError("not_found", 404)
            for entry in body.sessions:
                if entry.session_id not in actual:
                    raise ServiceError("not_found", 404)
                if entry.group_id != actual[entry.session_id]:
                    raise ServiceError("sidebar_project_mismatch", 409)
            saved = _project_document(saved, projects, actual)
        revision = saved.revision
        values = dict(
            revision=revision,
            groups=[group.model_dump(mode="json") for group in saved.groups],
            sessions=[entry.model_dump(mode="json") for entry in saved.sessions],
            updated_at=time.time(),
        )
        if row is None:
            await conn.execute(
                insert(session_sidebars).values(
                    principal_id=principal.principal_id, workspace_id=workspace, **values
                )
            )
        else:
            await conn.execute(
                update(session_sidebars)
                .where(
                    session_sidebars.c.principal_id == principal.principal_id,
                    session_sidebars.c.workspace_id == workspace,
                )
                .values(**values)
            )
        committed = saved
        return saved

    async def confirm() -> SessionSidebarDocument | None:
        # COMMIT 回程丢失只核实本请求CAS结果：修订恰好推进到 expected+1
        # 且内容一致时视为已提交；其他主体的并发写入不会满足该组合。
        try:
            found = await get(store, principal=principal, workspace=workspace)
        except ServiceError:
            return None
        if found.revision != expected + 1:
            return None
        if committed is None or fingerprint(found.model_dump(mode="json")) != fingerprint(
            committed.model_dump(mode="json")
        ):
            return None
        return found

    return await store._eval_write(operation, confirm)
