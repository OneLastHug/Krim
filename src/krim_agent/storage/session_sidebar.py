"""侧栏呈现持久化：principal+workspace 一份布局，CAS 修订，仅展示层。

不触碰会话归属、生命周期、项目绑定或执行父子语义；引用的会话按当前
授权即时核验，跨主体引用与不存在引用同响应，不泄露他人会话存在性。
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
            sessions=tuple(
                SidebarSessionEntry.model_validate(item) for item in row["sessions"]
            ),
        )
        # 存量布局按写入同规则整体验证；坏数据（重复/悬挂/成环/超深）不原样回放。
        SessionSidebarWrite(
            groups=result.groups, sessions=result.sessions, expected_revision=0
        )
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
        await conn.execute(
            select(account_meta.c.body).where(account_meta.c.id == IDENTITY_META_ID)
        )
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
    return {
        row["id"]
        for row in rows
        if admin or row["owner_id"] == principal_id
    }


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
        if row is None:
            return SessionSidebarDocument(revision=0, groups=(), sessions=())
        stored = _document(row)
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
                    update={
                        "parent_id": entry.parent_id
                        if entry.parent_id in kept_ids
                        else None
                    }
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
    if len(body.groups) > MAX_GROUPS or len(body.sessions) > MAX_SESSIONS:
        raise ServiceError("invalid_input", 422)
    expected = body.expected_revision

    async def operation(conn: AsyncConnection) -> SessionSidebarDocument:
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
        revision = current.revision + 1
        values = dict(
            revision=revision,
            groups=[group.model_dump(mode="json") for group in body.groups],
            sessions=[entry.model_dump(mode="json") for entry in body.sessions],
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
        return SessionSidebarDocument(
            revision=revision, groups=body.groups, sessions=body.sessions
        )

    async def confirm() -> SessionSidebarDocument | None:
        # COMMIT 回程丢失只核实本请求CAS结果：修订恰好推进到 expected+1
        # 且内容一致时视为已提交；其他主体的并发写入不会满足该组合。
        try:
            found = await get(store, principal=principal, workspace=workspace)
        except ServiceError:
            return None
        if found.revision != expected + 1:
            return None
        if fingerprint(
            SessionSidebarWrite(
                groups=found.groups, sessions=found.sessions, expected_revision=expected
            ).model_dump(mode="json")
        ) != fingerprint(body.model_dump(mode="json")):
            return None
        return found

    return await store._eval_write(operation, confirm)
