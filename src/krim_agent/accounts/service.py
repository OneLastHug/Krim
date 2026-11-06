"""IdentityService：托管账号认证快照与在线管理；数据库事务是状态事实。

同步 authenticate/resolve/validate/principals 只读内存快照，不取数据库锁、
不在任何事务内等待；全部 mutation 经 SQLiteStore 单写事务提交成功后先原子
刷新快照再触发 on_change(principal_id, invalidate, action)，刷新或 hook 失败
按 storage_unavailable fail closed，可用原 (actor, request_key) 重放核实。
最近使用时间为内存有界合并，随 mutation 发布（≥60s）或 close() 落地。
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import re
import secrets
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from pydantic import JsonValue, ValidationError
from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError, StrictModel, encoded, fingerprint
from krim_agent.runtime.contracts import settled
from krim_agent.security import AccessPolicy, Membership, PrincipalContext
from krim_agent.storage.account_models import (
    IDENTITY_META_ID,
    access_keys,
    account_events,
    account_meta,
    account_workspaces,
    accounts,
    workspace_profiles,
)
from krim_agent.storage.models import artifact_revisions, session_controls, work_turns
from krim_agent.storage.models import sessions as legacy_sessions
from krim_agent.storage.models import work_sessions as legacy_work_sessions

from .models import (
    AccountCreated,
    AccountsPage,
    AccountUpdated,
    AccountView,
    BootstrapResult,
    CreateAccountBody,
    EventsPage,
    EventView,
    ImportResult,
    IssuedKey,
    IssueKeyBody,
    KeyRevoked,
    KeyView,
    MembershipsSet,
    OneTimeToken,
    ProfileView,
    RevokeKeyBody,
    RotateKeyBody,
    SetMembershipsBody,
    UpdateAccountBody,
    WorkspaceRef,
)

if TYPE_CHECKING:
    from krim_agent.storage.sqlite import SQLiteStore

USAGE_FLUSH_SECONDS = 60
MAX_PAGE = 100
MAX_MEMBERSHIPS = 64
PERSONAL_WORKSPACE_NAME = "个人工作区"
ZERO_DIGEST = "0" * 64
_PRINCIPAL = re.compile(r"^[a-zA-Z0-9_.-]{1,64}$")
_REQUEST_KEY = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")


def _display_name(value: object) -> str:
    if not isinstance(value, str):
        raise ServiceError("invalid_input", 422)
    name = value.strip()
    if not name or len(name) > 64 or any(not char.isprintable() for char in name):
        raise ServiceError("invalid_input", 422)
    return name


def _principal_id(value: object) -> str:
    if not isinstance(value, str) or _PRINCIPAL.fullmatch(value) is None:
        raise ServiceError("invalid_input", 422)
    return value


def _request_key(value: object) -> str:
    if not isinstance(value, str) or _REQUEST_KEY.fullmatch(value) is None:
        raise ServiceError("invalid_input", 422)
    return value


def _new_key_id() -> str:
    return "key_" + secrets.token_hex(8)


def _new_token(key_id: str) -> str:
    secret = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().rstrip("=")
    return f"krim1.{key_id}.{secret}"


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _masked(key_id: str, token: str) -> str:
    return f"krim1.{key_id}…{token[-4:]}"


def _key_view(row: Any) -> KeyView:
    return KeyView(
        key_id=row["key_id"],
        masked=row["masked"],
        credential_version=row["credential_version"],
        status=row["status"],
        weak=row["weak"],
        issued_at=row["issued_at"],
        revoked_at=row["revoked_at"],
        last_used_at=row["last_used_at"],
    )


def _sorted_members(members: Any) -> tuple[Membership, ...]:
    return tuple(sorted(members, key=lambda item: item.workspace_id))


def _issued_result(
    principal_id: str, key_row: Any, token: str, revoked_key_id: str | None
) -> IssuedKey:
    return IssuedKey(
        principal_id=principal_id,
        key=_key_view(key_row),
        token=OneTimeToken(token),
        revoked_key_id=revoked_key_id,
    )


def _issued_replay(principal_id: str, key_row: Any, summary: dict[str, JsonValue]) -> IssuedKey:
    return IssuedKey(
        principal_id=principal_id,
        key=_key_view(key_row),
        token=None,
        replayed=True,
        revoked_key_id=(
            None if summary.get("revoked_key_id") is None else str(summary["revoked_key_id"])
        ),
    )


@dataclass(frozen=True)
class _State:
    principal_id: str
    display_name: str
    status: str
    platform_role: str
    generation: int
    revision: int
    legacy: bool
    memberships: tuple[Membership, ...]
    active_digests: frozenset[str]


class IdentityService:
    """单实例稳定引用；快照只在提交成功后原子替换。"""

    def __init__(self, store: SQLiteStore) -> None:
        self.store = store
        self.on_change: Callable[[str, bool, str], Awaitable[None]] | None = None
        self._accounts: dict[str, _State] = {}
        self._digests: dict[str, tuple[str, str, str]] = {}
        self._managed = False
        self._source: str | None = None
        self._stale = False
        self._pending_usage: dict[str, float] = {}
        self._usage_checked_at = 0.0

    @classmethod
    async def open(cls, store: SQLiteStore) -> IdentityService:
        service = cls(store)
        await service._reload()
        return service

    async def close(self) -> None:
        """退出前落地未合并的最近使用时间；由Application.close调用。"""
        try:
            await self.flush_usage()
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

    @property
    def managed(self) -> bool:
        return self._managed

    @property
    def source(self) -> str | None:
        return self._source

    @property
    def principals(self) -> tuple[PrincipalContext, ...]:
        if self._stale:
            return ()
        return tuple(
            PrincipalContext(state.principal_id, state.generation, state.memberships, state.legacy)
            for _, state in sorted(self._accounts.items())
            if state.status == "active" and state.active_digests
        )

    # ---- 同步快照读取：无数据库锁，供认证边界与执行路径高频调用 ----

    def authenticate(self, authorization: str) -> PrincipalContext:
        if self._stale:
            raise ServiceError("storage_unavailable", 503)
        if not isinstance(authorization, str) or not authorization.startswith("Bearer "):
            raise ServiceError("unauthorized", 401)
        token = authorization[7:]
        if not token or len(token) > 256:
            raise ServiceError("unauthorized", 401)
        digest = hashlib.sha256(token.encode()).hexdigest()
        found = self._digests.get(digest)
        if found is None:
            hmac.compare_digest(digest, ZERO_DIGEST)
            raise ServiceError("unauthorized", 401)
        principal_id, key_id, stored = found
        if not hmac.compare_digest(digest, stored):
            raise ServiceError("unauthorized", 401)
        state = self._accounts[principal_id]
        if state.status != "active":
            raise ServiceError("unauthorized", 401)
        self._pending_usage[key_id] = time.time()
        return PrincipalContext(principal_id, state.generation, state.memberships, state.legacy)

    def resolve(self, principal_id: str) -> PrincipalContext:
        if self._stale:
            raise ServiceError("storage_unavailable", 503)
        state = self._accounts.get(principal_id) if isinstance(principal_id, str) else None
        if state is None or state.status != "active" or not state.active_digests:
            raise ServiceError("unauthorized", 401)
        return PrincipalContext(
            state.principal_id, state.generation, state.memberships, state.legacy
        )

    def validate(self, principal: PrincipalContext) -> PrincipalContext:
        """旧generation/memberships直接拒绝，不自动刷新为新权限；显式刷新用resolve。"""
        if self._stale:
            raise ServiceError("storage_unavailable", 503)
        state = (
            self._accounts.get(principal.principal_id)
            if isinstance(principal, PrincipalContext)
            else None
        )
        if (
            state is None
            or state.status != "active"
            or not state.active_digests
            or state.generation != principal.generation
            or state.memberships != principal.memberships
            or state.legacy != principal.legacy
        ):
            raise ServiceError("unauthorized", 401)
        return PrincipalContext(
            state.principal_id, state.generation, state.memberships, state.legacy
        )

    async def flush_usage(self) -> None:
        """最近使用时间有界合并：只前移，不随认证频率同步写。"""
        pending = dict(self._pending_usage)
        if not pending:
            return
        self._pending_usage = {}

        async def operation(conn: AsyncConnection) -> None:
            for key_id, used_at in sorted(pending.items()):
                await conn.execute(
                    update(access_keys)
                    .where(
                        access_keys.c.key_id == key_id,
                        access_keys.c.last_used_at.is_(None)
                        | (access_keys.c.last_used_at < used_at),
                    )
                    .values(last_used_at=used_at)
                )

        try:
            await settled(self.store._write(operation))
        except asyncio.CancelledError:
            raise
        except Exception:
            for key_id, used_at in pending.items():
                current = self._pending_usage.get(key_id)
                if current is None or current < used_at:
                    self._pending_usage[key_id] = used_at

    # ---- 事务与快照发布 ----

    async def _read[T](self, operation: Callable[[AsyncConnection], Awaitable[T]]) -> T:
        if self.store.closed:
            raise ServiceError("closed")

        async def owned() -> T:
            async with self.store.lock, self.store.engine.begin() as conn:
                return await operation(conn)

        try:
            return await settled(owned())
        except ServiceError:
            raise
        except Exception:
            raise ServiceError("storage_unavailable", 503) from None

    async def _load(
        self, conn: AsyncConnection
    ) -> tuple[dict[str, _State], dict[str, tuple[str, str, str]], bool, str | None]:
        if self.store.schema_version < 7:
            return {}, {}, False, None
        meta = (
            await conn.execute(
                select(account_meta.c.body).where(account_meta.c.id == IDENTITY_META_ID)
            )
        ).scalar_one_or_none()
        has_data = (
            await conn.execute(select(accounts.c.principal_id).limit(1))
        ).first() is not None or (
            await conn.execute(select(access_keys.c.key_id).limit(1))
        ).first() is not None
        if meta is None or not isinstance(meta, dict) or meta.get("mode") != "managed":
            if has_data:
                # schema7托管承诺：有账号数据就必须有identity元数据，禁止降级回落。
                raise ServiceError("facts_corrupt")
            return {}, {}, False, None
        source = meta.get("source")
        if source is not None and not isinstance(source, str):
            raise ServiceError("facts_corrupt")
        managed = True
        memberships: dict[str, list[Membership]] = {}
        for row in (
            await conn.execute(
                select(account_workspaces).order_by(
                    account_workspaces.c.principal_id, account_workspaces.c.workspace_id
                )
            )
        ).mappings():
            memberships.setdefault(row["principal_id"], []).append(
                Membership(
                    workspace_id=row["workspace_id"], role=row["role"], approve=row["approve"]
                )
            )
        active: dict[str, set[str]] = {}
        digests: dict[str, tuple[str, str, str]] = {}
        for row in (
            await conn.execute(select(access_keys).where(access_keys.c.status == "active"))
        ).mappings():
            digest = row["token_digest"]
            if digest in digests:
                raise ServiceError("facts_corrupt")
            digests[digest] = (row["principal_id"], row["key_id"], digest)
            active.setdefault(row["principal_id"], set()).add(digest)
        states: dict[str, _State] = {}
        for row in (await conn.execute(select(accounts))).mappings():
            principal_id = row["principal_id"]
            states[principal_id] = _State(
                principal_id=principal_id,
                display_name=row["display_name"],
                status=row["status"],
                platform_role=row["platform_role"],
                generation=row["authorization_generation"],
                revision=row["revision"],
                legacy=row["legacy"],
                memberships=_sorted_members(memberships.get(principal_id, ())),
                active_digests=frozenset(active.get(principal_id, ())),
            )
        return states, digests, managed, source

    async def refresh(self) -> None:
        """成功reload后解除stale gate；失败则认证保持拒绝（fail closed）。"""
        await self._reload()

    async def _reload(self) -> None:
        self._stale = True
        states, digests, managed, source = await self._read(self._load)
        self._accounts = states
        self._digests = digests
        self._managed = managed
        self._source = source
        self._stale = False

    async def _publish(self, changes: tuple[tuple[str, bool, str], ...]) -> None:
        """提交成功、写锁释放后执行：刷新快照，落地使用时间，再触发hook。

        刷新或hook失败都停止该次成功返回（fail closed）；客户端可用原
        request_key 重放核实结果。绝不await任何数据库锁持有方。
        """
        try:
            await self._reload()
        except asyncio.CancelledError:
            raise
        except Exception:
            raise ServiceError("storage_unavailable", 503) from None
        if self._pending_usage and time.monotonic() - self._usage_checked_at >= USAGE_FLUSH_SECONDS:
            self._usage_checked_at = time.monotonic()
            await self.flush_usage()
        hook = self.on_change
        if hook is None:
            return
        for principal_id, invalidate, action in changes:
            try:
                await hook(principal_id, invalidate, action)
            except asyncio.CancelledError:
                self._stale = True
                raise
            except Exception:
                self._stale = True
                raise ServiceError("storage_unavailable", 503) from None

    async def _require_loginable(
        self,
        conn: AsyncConnection,
        actor_id: str,
        *,
        generation: int,
        admin: bool,
    ) -> None:
        """拿写锁后重新核实actor：阻止等锁期间被停用/降权/换代仍执行。"""
        row = (
            (await conn.execute(select(accounts).where(accounts.c.principal_id == actor_id)))
            .mappings()
            .first()
        )
        if row is None or row["status"] != "active":
            raise ServiceError("unauthorized", 401)
        keyed = (
            await conn.execute(
                select(access_keys.c.key_id).where(
                    access_keys.c.principal_id == actor_id,
                    access_keys.c.status == "active",
                )
            )
        ).first()
        if keyed is None:
            raise ServiceError("unauthorized", 401)
        if row["authorization_generation"] != generation:
            raise ServiceError("unauthorized", 401)
        if admin and row["platform_role"] != "platform_admin":
            raise ServiceError("permission_denied", 403)

    async def _submit[T](
        self,
        *,
        action: str,
        actor_id: str,
        target: str | None,
        extras: dict[str, JsonValue],
        request_key: str,
        perform: Callable[[AsyncConnection], Awaitable[tuple[T, dict[str, JsonValue]]]],
        rebuild: Callable[[AsyncConnection, Any], Awaitable[T]],
        target_in_digest: bool = True,
        authorize: Callable[[AsyncConnection], Awaitable[None]] | None = None,
    ) -> T:
        """先经各方法的当前actor授权，再按(actor, request_key)重放核实。"""
        payload: dict[str, JsonValue] = {"action": action, "actor": actor_id}
        if target_in_digest:
            payload["target"] = target
        payload.update(extras)
        digest = fingerprint(payload)
        prior = self.store.healthy

        async def operation(conn: AsyncConnection) -> T:
            if authorize is not None:
                # host离线操作例外；在线操作在锁内重新核实当前权限。
                await authorize(conn)
            mine = (
                (
                    await conn.execute(
                        select(account_events).where(
                            account_events.c.request_key == request_key,
                            account_events.c.actor == actor_id,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if mine is not None:
                if mine["request_hash"] != digest:
                    raise ServiceError("key_conflict")
                return await rebuild(conn, mine)
            result, summary = await perform(conn)
            await conn.execute(
                insert(account_events).values(
                    id=str(uuid.uuid4()),
                    request_key=request_key,
                    request_hash=digest,
                    action=action,
                    actor=actor_id,
                    target=target,
                    result=summary,
                    at=time.time(),
                )
            )
            return result

        try:
            return await settled(self.store._write(operation))
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if isinstance(error, ServiceError) and error.code != "storage_unavailable":
                raise
            # COMMIT回程未知：只核实原(actor, request_key)，不自动重放事务。
            try:
                found = await self._event_row(actor_id, request_key)
                if found is not None and found["request_hash"] == digest:
                    self.store.healthy = prior
                    return await self._read(lambda conn: rebuild(conn, found))
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            self.store.healthy = False
            raise ServiceError("storage_unavailable", 503) from None

    async def _event_row(self, actor_id: str, request_key: str) -> Any:
        async def read(conn: AsyncConnection) -> Any:
            return (
                (
                    await conn.execute(
                        select(account_events).where(
                            account_events.c.request_key == request_key,
                            account_events.c.actor == actor_id,
                        )
                    )
                )
                .mappings()
                .first()
            )

        return await self._read(read)

    def _actor(self, actor: PrincipalContext, *, admin: bool = False) -> _State:
        state = (
            self._accounts.get(actor.principal_id) if isinstance(actor, PrincipalContext) else None
        )
        if state is None or state.status != "active" or not state.active_digests:
            raise ServiceError("unauthorized", 401)
        if admin and state.platform_role != "platform_admin":
            raise ServiceError("permission_denied", 403)
        return state

    def _body[T: StrictModel](self, model: type[T], value: object) -> T:
        if isinstance(value, model):
            return value
        try:
            # strict模型的tuple/list字段按JSON输入规则校验，list可进tuple。
            return model.model_validate_json(encoded(value))
        except (ValidationError, ValueError):
            raise ServiceError("invalid_input", 422) from None

    # ---- 读取接口 ----

    async def _account_view(self, conn: AsyncConnection, principal_id: str) -> AccountView:
        row = (
            (await conn.execute(select(accounts).where(accounts.c.principal_id == principal_id)))
            .mappings()
            .first()
        )
        if row is None:
            raise ServiceError("not_found", 404)
        keys = (
            (
                await conn.execute(
                    select(access_keys)
                    .where(access_keys.c.principal_id == principal_id)
                    .order_by(access_keys.c.credential_version)
                )
            )
            .mappings()
            .all()
        )
        active = [item for item in keys if item["status"] == "active"]
        current = active[-1] if active else (keys[-1] if keys else None)
        return AccountView(
            principal_id=row["principal_id"],
            display_name=row["display_name"],
            status=row["status"],
            platform_role=row["platform_role"],
            generation=row["authorization_generation"],
            revision=row["revision"],
            legacy=row["legacy"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            has_active_key=bool(active),
            key=_key_view(current) if current is not None else None,
        )

    async def profile(self, actor: PrincipalContext) -> ProfileView:
        """仅自己的资料；含个人密钥元数据与workspace可读名称。"""
        state = self._actor(actor)

        async def read(conn: AsyncConnection) -> ProfileView:
            view = await self._account_view(conn, state.principal_id)
            refs: list[WorkspaceRef] = []
            for membership in state.memberships:
                row = (
                    await conn.execute(
                        select(
                            workspace_profiles.c.display_name, workspace_profiles.c.owner_id
                        ).where(workspace_profiles.c.workspace_id == membership.workspace_id)
                    )
                ).first()
                refs.append(
                    WorkspaceRef(
                        workspace_id=membership.workspace_id,
                        display_name=(row[0] if row is not None else None)
                        or membership.workspace_id,
                        role=membership.role,
                        approve=membership.approve,
                        personal=row is not None and row[1] == state.principal_id,
                    )
                )
            return ProfileView(
                principal_id=view.principal_id,
                display_name=view.display_name,
                status=view.status,
                platform_role=view.platform_role,
                generation=view.generation,
                revision=view.revision,
                legacy=view.legacy,
                key=view.key,
                workspaces=tuple(refs),
            )

        return await self._read(read)

    async def account(self, actor: PrincipalContext, principal_id: str) -> AccountView:
        self._actor(actor, admin=True)
        principal_id = _principal_id(principal_id)
        return await self._read(lambda conn: self._account_view(conn, principal_id))

    async def memberships(self, actor: PrincipalContext, principal_id: str) -> dict[str, JsonValue]:
        """管理员读取目标成员资料；不通过主体解析签发任何授权。"""
        self._actor(actor, admin=True)
        principal_id = _principal_id(principal_id)

        async def read(conn: AsyncConnection) -> dict[str, JsonValue]:
            await self._require_loginable(
                conn, actor.principal_id, generation=actor.generation, admin=True
            )
            target = await self._target_row(conn, principal_id)
            rows = (
                (
                    await conn.execute(
                        select(
                            account_workspaces.c.workspace_id,
                            account_workspaces.c.role,
                            account_workspaces.c.approve,
                            workspace_profiles.c.display_name,
                            workspace_profiles.c.owner_id,
                        )
                        .outerjoin(
                            workspace_profiles,
                            workspace_profiles.c.workspace_id == account_workspaces.c.workspace_id,
                        )
                        .where(account_workspaces.c.principal_id == principal_id)
                        .order_by(account_workspaces.c.workspace_id)
                    )
                )
                .mappings()
                .all()
            )
            return {
                "principal_id": principal_id,
                "generation": target["authorization_generation"],
                "memberships": [
                    dict(
                        workspace_id=row["workspace_id"],
                        display_name=row["display_name"] or "工作区",
                        role=row["role"],
                        approve=row["approve"],
                        personal=row["owner_id"] == principal_id,
                    )
                    for row in rows
                ],
            }

        return await self._read(read)

    async def list_accounts(
        self,
        actor: PrincipalContext,
        *,
        offset: int = 0,
        limit: int = 20,
        status: str | None = None,
        query: str | None = None,
    ) -> AccountsPage:
        self._actor(actor, admin=True)
        if (
            type(offset) is not int
            or type(limit) is not int
            or offset < 0
            or not 1 <= limit <= MAX_PAGE
        ):
            raise ServiceError("invalid_input", 422)
        if status not in (None, "active", "disabled") or not isinstance(query, (str, type(None))):
            raise ServiceError("invalid_input", 422)
        if query is not None and (not query or len(query) > 64):
            raise ServiceError("invalid_input", 422)

        async def read(conn: AsyncConnection) -> AccountsPage:
            conditions = []
            if status is not None:
                conditions.append(accounts.c.status == status)
            if query is not None:
                escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                conditions.append(accounts.c.display_name.like(f"%{escaped}%", escape="\\"))
            total = (
                await conn.execute(select(func.count()).select_from(accounts).where(*conditions))
            ).scalar_one()
            rows = (
                (
                    await conn.execute(
                        select(accounts.c.principal_id)
                        .where(*conditions)
                        .order_by(accounts.c.created_at, accounts.c.principal_id)
                        .offset(offset)
                        .limit(limit)
                    )
                )
                .scalars()
                .all()
            )
            items = [await self._account_view(conn, principal_id) for principal_id in rows]
            return AccountsPage(items=tuple(items), offset=offset, total=total)

        return await self._read(read)

    async def events(
        self,
        actor: PrincipalContext,
        *,
        offset: int = 0,
        limit: int = 20,
        principal_id: str | None = None,
        request_key: str | None = None,
    ) -> EventsPage:
        self._actor(actor, admin=True)
        if (
            type(offset) is not int
            or type(limit) is not int
            or offset < 0
            or not 1 <= limit <= MAX_PAGE
        ):
            raise ServiceError("invalid_input", 422)
        if principal_id is not None:
            principal_id = _principal_id(principal_id)
        if request_key is not None:
            request_key = _request_key(request_key)

        async def read(conn: AsyncConnection) -> EventsPage:
            conditions = []
            if principal_id is not None:
                conditions.append(account_events.c.target == principal_id)
            if request_key is not None:
                conditions.append(account_events.c.request_key == request_key)
            total = (
                await conn.execute(
                    select(func.count()).select_from(account_events).where(*conditions)
                )
            ).scalar_one()
            rows = (
                (
                    await conn.execute(
                        select(account_events)
                        .where(*conditions)
                        .order_by(account_events.c.at, account_events.c.id)
                        .offset(offset)
                        .limit(limit)
                    )
                )
                .mappings()
                .all()
            )
            items = tuple(
                EventView(
                    id=row["id"],
                    action=row["action"],
                    actor=row["actor"],
                    target=row["target"],
                    request_key=row["request_key"],
                    result=cast("dict[str, JsonValue]", row["result"]),
                    at=row["at"],
                )
                for row in rows
            )
            return EventsPage(items=items, offset=offset, total=total)

        return await self._read(read)

    # ---- 管理mutation ----

    async def create_account(
        self, actor: PrincipalContext, body: object, *, request_key: str
    ) -> AccountCreated:
        state = self._actor(actor, admin=True)
        body = self._body(CreateAccountBody, body)
        display = _display_name(body.display_name)
        key = _request_key(request_key)
        principal_id = str(uuid.uuid4())
        workspace_id = str(uuid.uuid4())
        members = _sorted_members(body.memberships)
        if len({m.workspace_id for m in members}) != len(members) or len(members) > MAX_MEMBERSHIPS:
            raise ServiceError("invalid_input", 422)
        now = time.time()

        async def perform(conn: AsyncConnection) -> tuple[AccountCreated, dict[str, JsonValue]]:
            await conn.execute(
                insert(accounts).values(
                    principal_id=principal_id,
                    display_name=display,
                    status="active",
                    platform_role=body.platform_role,
                    authorization_generation=1,
                    revision=1,
                    legacy=False,
                    created_at=now,
                    updated_at=now,
                )
            )
            await conn.execute(
                insert(workspace_profiles).values(
                    workspace_id=workspace_id,
                    display_name=PERSONAL_WORKSPACE_NAME,
                    owner_id=principal_id,
                )
            )
            rows = [dict(workspace_id=workspace_id, role="admin", approve=True)]
            for membership in members:
                known = (
                    await conn.execute(
                        select(workspace_profiles.c.workspace_id).where(
                            workspace_profiles.c.workspace_id == membership.workspace_id
                        )
                    )
                ).scalar_one_or_none()
                if known is None:
                    await conn.execute(
                        insert(workspace_profiles).values(
                            workspace_id=membership.workspace_id,
                            display_name=membership.workspace_id,
                            owner_id=None,
                        )
                    )
                rows.append(
                    dict(
                        workspace_id=membership.workspace_id,
                        role=membership.role,
                        approve=membership.approve,
                    )
                )
            if len(rows) > MAX_MEMBERSHIPS:
                raise ServiceError("invalid_input", 422)
            await conn.execute(
                insert(account_workspaces),
                [dict(principal_id=principal_id, **row) for row in rows],
            )
            view = await self._account_view(conn, principal_id)
            return AccountCreated(account=view), {"principal_id": principal_id}

        async def rebuild(conn: AsyncConnection, event: Any) -> AccountCreated:
            target = cast("dict[str, JsonValue]", event["result"])["principal_id"]
            return AccountCreated(
                account=await self._account_view(conn, str(target)), replayed=True
            )

        async def reauth(conn: AsyncConnection) -> None:
            await self._require_loginable(
                conn, state.principal_id, generation=actor.generation, admin=True
            )

        result = await self._submit(
            action="create",
            actor_id=state.principal_id,
            target=principal_id,
            # 服务端生成的ID不入payload：重放须按稳定内容匹配同一digest。
            extras={
                "display_name": display,
                "platform_role": body.platform_role,
                "memberships": cast("JsonValue", [m.model_dump(mode="json") for m in members]),
            },
            request_key=key,
            perform=perform,
            rebuild=rebuild,
            target_in_digest=False,
            authorize=reauth,
        )
        await self._publish(((principal_id, False, "create"),))
        return result

    async def update_account(
        self, actor: PrincipalContext, principal_id: str, body: object, *, request_key: str
    ) -> AccountUpdated:
        state = self._actor(actor)
        principal_id = _principal_id(principal_id)
        if principal_id != actor.principal_id:
            self._actor(actor, admin=True)
        body = self._body(UpdateAccountBody, body)
        if (
            principal_id == actor.principal_id
            and state.platform_role != "platform_admin"
            and (body.status is not None or body.platform_role is not None)
        ):
            raise ServiceError("permission_denied", 403)
        display = None if body.display_name is None else _display_name(body.display_name)
        target_state = self._accounts.get(principal_id)
        if target_state is None:
            invalidate, mutation_action = True, "update"
        else:
            disable = body.status is not None and body.status != target_state.status
            enable = body.status == "active" and target_state.status == "disabled"
            role = (
                body.platform_role is not None and body.platform_role != target_state.platform_role
            )
            invalidate = disable or enable or role
            mutation_action = (
                "disable"
                if disable and not enable
                else "enable"
                if enable
                else "role"
                if role
                else "update"
            )

        async def perform(conn: AsyncConnection) -> tuple[AccountUpdated, dict[str, JsonValue]]:
            row = (
                (
                    await conn.execute(
                        select(accounts).where(accounts.c.principal_id == principal_id)
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                raise ServiceError("not_found", 404)
            if body.expected_revision is not None and row["revision"] != body.expected_revision:
                raise ServiceError("revision_conflict")
            changes: dict[str, Any] = {}
            if display is not None and display != row["display_name"]:
                changes["display_name"] = display
            if body.status is not None and body.status != row["status"]:
                changes["status"] = body.status
            if body.platform_role is not None and body.platform_role != row["platform_role"]:
                changes["platform_role"] = body.platform_role
            if changes:
                admin_loginable = (
                    row["platform_role"] == "platform_admin" and row["status"] == "active"
                )
                loses_login = admin_loginable and (
                    changes.get("status") == "disabled" or changes.get("platform_role") == "member"
                )
                if loses_login and not await self._other_loginable_admin(conn, principal_id):
                    raise ServiceError("permission_denied", 403)
                values: dict[str, Any] = {
                    "updated_at": time.time(),
                    "revision": row["revision"] + 1,
                    **changes,
                }
                if "status" in changes or "platform_role" in changes:
                    values["authorization_generation"] = row["authorization_generation"] + 1
                await conn.execute(
                    update(accounts).where(accounts.c.principal_id == principal_id).values(**values)
                )
            view = await self._account_view(conn, principal_id)
            return AccountUpdated(account=view), {
                "principal_id": principal_id,
                "revision": view.revision,
                "generation": view.generation,
            }

        async def rebuild(conn: AsyncConnection, event: Any) -> AccountUpdated:
            target = cast("dict[str, JsonValue]", event["result"])["principal_id"]
            return AccountUpdated(
                account=await self._account_view(conn, str(target)), replayed=True
            )

        admin_check = principal_id != actor.principal_id

        async def reauth(conn: AsyncConnection) -> None:
            await self._require_loginable(
                conn, state.principal_id, generation=actor.generation, admin=admin_check
            )

        result = await self._submit(
            action="update",
            actor_id=state.principal_id,
            target=principal_id,
            extras={
                "display_name": display,
                "status": body.status,
                "platform_role": body.platform_role,
                "expected_revision": body.expected_revision,
            },
            request_key=_request_key(request_key),
            perform=perform,
            rebuild=rebuild,
            authorize=reauth,
        )
        await self._publish(((principal_id, invalidate, mutation_action),))
        return result

    async def _new_key_row(
        self,
        conn: AsyncConnection,
        principal_id: str,
        *,
        request_key: str,
        credential_version: int,
    ) -> tuple[str, str]:
        key_id = _new_key_id()
        token = _new_token(key_id)
        await conn.execute(
            insert(access_keys).values(
                key_id=key_id,
                principal_id=principal_id,
                token_digest=_digest(token),
                credential_version=credential_version,
                masked=_masked(key_id, token),
                weak=len(token) < 32,
                status="active",
                issued_at=time.time(),
                revoked_at=None,
                last_used_at=None,
                request_key=request_key,
            )
        )
        return key_id, token

    async def _current_version(self, conn: AsyncConnection, principal_id: str) -> int:
        return (
            await conn.execute(
                select(func.max(access_keys.c.credential_version)).where(
                    access_keys.c.principal_id == principal_id
                )
            )
        ).scalar_one_or_none() or 0

    async def _revoke_active(self, conn: AsyncConnection, principal_id: str) -> str | None:
        rows = (
            (
                await conn.execute(
                    select(access_keys.c.key_id).where(
                        access_keys.c.principal_id == principal_id,
                        access_keys.c.status == "active",
                    )
                )
            )
            .scalars()
            .all()
        )
        if not rows:
            return None
        if len(rows) > 1:
            raise ServiceError("facts_corrupt")
        await conn.execute(
            update(access_keys)
            .where(access_keys.c.key_id == rows[0])
            .values(status="revoked", revoked_at=time.time())
        )
        return cast("str", rows[0])

    async def _legacy_local_data(self, conn: AsyncConnection) -> bool:
        """旧库workspace 'local'上是否仍有历史归属数据（会话/工作行）。"""
        for table, column in (
            (legacy_sessions, legacy_sessions.c.id),
            (legacy_work_sessions, legacy_work_sessions.c.session_id),
        ):
            found = (
                await conn.execute(select(column).where(table.c.workspace_id == "local").limit(1))
            ).first()
            if found is not None:
                return True
        return False

    async def _other_loginable_admin(self, conn: AsyncConnection, principal_id: str) -> bool:
        rows = (
            (
                await conn.execute(
                    select(accounts.c.principal_id).where(
                        accounts.c.platform_role == "platform_admin",
                        accounts.c.status == "active",
                        accounts.c.principal_id != principal_id,
                    )
                )
            )
            .scalars()
            .all()
        )
        for candidate in rows:
            active = (
                await conn.execute(
                    select(access_keys.c.key_id).where(
                        access_keys.c.principal_id == candidate,
                        access_keys.c.status == "active",
                    )
                )
            ).first()
            if active is not None:
                return True
        return False

    async def _target_row(self, conn: AsyncConnection, principal_id: str) -> Any:
        row = (
            (await conn.execute(select(accounts).where(accounts.c.principal_id == principal_id)))
            .mappings()
            .first()
        )
        if row is None:
            raise ServiceError("not_found", 404)
        return row

    async def _key_row(self, conn: AsyncConnection, key_id: str) -> Any:
        row = (
            (await conn.execute(select(access_keys).where(access_keys.c.key_id == key_id)))
            .mappings()
            .first()
        )
        if row is None:
            raise ServiceError("facts_corrupt")
        return row

    async def issue_key(
        self, actor: PrincipalContext, principal_id: str, body: object, *, request_key: str
    ) -> IssuedKey:
        actor_state = self._actor(actor, admin=True)
        self._body(IssueKeyBody, body)
        principal_id = _principal_id(principal_id)
        key = _request_key(request_key)
        resign = bool(
            (target := self._accounts.get(principal_id)) is not None and target.active_digests
        )

        async def perform(conn: AsyncConnection) -> tuple[IssuedKey, dict[str, JsonValue]]:
            row = await self._target_row(conn, principal_id)
            revoked = await self._revoke_active(conn, principal_id)
            if (revoked is not None) != resign:
                raise ServiceError("facts_corrupt")
            version = await self._current_version(conn, principal_id) + 1
            key_id, token = await self._new_key_row(
                conn, principal_id, request_key=key, credential_version=version
            )
            if revoked is not None:
                # 管理员重签：撤销旧密钥并递增授权版本（安全恢复语义）。
                await conn.execute(
                    update(accounts)
                    .where(accounts.c.principal_id == principal_id)
                    .values(
                        authorization_generation=row["authorization_generation"] + 1,
                        updated_at=time.time(),
                    )
                )
            key_row = await self._key_row(conn, key_id)
            return _issued_result(principal_id, key_row, token, revoked), {
                "principal_id": principal_id,
                "key_id": key_id,
                "revoked_key_id": revoked,
            }

        async def rebuild(conn: AsyncConnection, event: Any) -> IssuedKey:
            summary = cast("dict[str, JsonValue]", event["result"])
            row = await self._key_row(conn, str(summary["key_id"]))
            return _issued_replay(principal_id, row, summary)

        async def reauth(conn: AsyncConnection) -> None:
            await self._require_loginable(
                conn, actor_state.principal_id, generation=actor.generation, admin=True
            )

        result = await self._submit(
            action="issue",
            actor_id=actor_state.principal_id,
            target=principal_id,
            extras={},
            request_key=key,
            perform=perform,
            rebuild=rebuild,
            authorize=reauth,
        )
        await self._publish(((principal_id, resign, "issue"),))
        return result

    async def rotate_key(
        self, actor: PrincipalContext, body: object, *, request_key: str
    ) -> IssuedKey:
        state = self._actor(actor)
        self._body(RotateKeyBody, body)
        principal_id = actor.principal_id
        key = _request_key(request_key)

        async def perform(conn: AsyncConnection) -> tuple[IssuedKey, dict[str, JsonValue]]:
            # 原子轮换：同事务撤销旧密钥并建立新密钥；权限generation不变。
            await self._target_row(conn, principal_id)
            revoked = await self._revoke_active(conn, principal_id)
            if revoked is None:
                raise ServiceError("not_found", 404)
            version = await self._current_version(conn, principal_id) + 1
            key_id, token = await self._new_key_row(
                conn, principal_id, request_key=key, credential_version=version
            )
            key_row = await self._key_row(conn, key_id)
            return _issued_result(principal_id, key_row, token, revoked), {
                "principal_id": principal_id,
                "key_id": key_id,
                "revoked_key_id": revoked,
            }

        async def rebuild(conn: AsyncConnection, event: Any) -> IssuedKey:
            summary = cast("dict[str, JsonValue]", event["result"])
            row = await self._key_row(conn, str(summary["key_id"]))
            return _issued_replay(principal_id, row, summary)

        async def reauth(conn: AsyncConnection) -> None:
            await self._require_loginable(
                conn, state.principal_id, generation=actor.generation, admin=False
            )

        result = await self._submit(
            action="rotate",
            actor_id=principal_id,
            target=principal_id,
            extras={},
            request_key=key,
            perform=perform,
            rebuild=rebuild,
            authorize=reauth,
        )
        await self._publish(((principal_id, False, "rotate"),))
        return result

    async def revoke_key(
        self, actor: PrincipalContext, principal_id: str, body: object, *, request_key: str
    ) -> KeyRevoked:
        state = self._actor(actor)
        principal_id = _principal_id(principal_id)
        if principal_id != actor.principal_id:
            self._actor(actor, admin=True)
        self._body(RevokeKeyBody, body)
        key = _request_key(request_key)

        async def perform(conn: AsyncConnection) -> tuple[KeyRevoked, dict[str, JsonValue]]:
            row = await self._target_row(conn, principal_id)
            active = (
                await conn.execute(
                    select(access_keys.c.key_id).where(
                        access_keys.c.principal_id == principal_id,
                        access_keys.c.status == "active",
                    )
                )
            ).first()
            loginable_admin = (
                row["platform_role"] == "platform_admin"
                and row["status"] == "active"
                and active is not None
            )
            if loginable_admin and not await self._other_loginable_admin(conn, principal_id):
                raise ServiceError("permission_denied", 403)
            key_id = await self._revoke_active(conn, principal_id)
            if key_id is None:
                raise ServiceError("not_found", 404)
            generation = row["authorization_generation"] + 1
            await conn.execute(
                update(accounts)
                .where(accounts.c.principal_id == principal_id)
                .values(authorization_generation=generation, updated_at=time.time())
            )
            key_row = await self._key_row(conn, key_id)
            return KeyRevoked(
                principal_id=principal_id,
                key=_key_view(key_row),
                generation=generation,
            ), {"principal_id": principal_id, "key_id": key_id, "generation": generation}

        async def rebuild(conn: AsyncConnection, event: Any) -> KeyRevoked:
            summary = cast("dict[str, JsonValue]", event["result"])
            row = await self._key_row(conn, str(summary["key_id"]))
            generation = summary["generation"]
            if type(generation) is not int:
                raise ServiceError("facts_corrupt")
            return KeyRevoked(
                principal_id=str(summary["principal_id"]),
                key=_key_view(row),
                generation=generation,
                replayed=True,
            )

        async def reauth(conn: AsyncConnection) -> None:
            await self._require_loginable(
                conn,
                state.principal_id,
                generation=actor.generation,
                admin=principal_id != actor.principal_id,
            )

        result = await self._submit(
            action="revoke",
            actor_id=state.principal_id,
            target=principal_id,
            extras={},
            request_key=key,
            perform=perform,
            rebuild=rebuild,
            authorize=reauth,
        )
        await self._publish(((principal_id, True, "revoke"),))
        return result

    async def recover(
        self, principal_id: str, *, request_key: str, actor: str = "host"
    ) -> IssuedKey:
        """可信宿主CLI恢复：重签密钥并递增授权版本；操作者记录在事件。"""
        principal_id = _principal_id(principal_id)
        key = _request_key(request_key)
        if not isinstance(actor, str) or not actor or len(actor) > 64:
            raise ServiceError("invalid_input", 422)

        async def perform(conn: AsyncConnection) -> tuple[IssuedKey, dict[str, JsonValue]]:
            row = await self._target_row(conn, principal_id)
            revoked = await self._revoke_active(conn, principal_id)
            version = await self._current_version(conn, principal_id) + 1
            key_id, token = await self._new_key_row(
                conn, principal_id, request_key=key, credential_version=version
            )
            await conn.execute(
                update(accounts)
                .where(accounts.c.principal_id == principal_id)
                .values(
                    authorization_generation=row["authorization_generation"] + 1,
                    updated_at=time.time(),
                )
            )
            key_row = await self._key_row(conn, key_id)
            return _issued_result(principal_id, key_row, token, revoked), {
                "principal_id": principal_id,
                "key_id": key_id,
                "revoked_key_id": revoked,
            }

        async def rebuild(conn: AsyncConnection, event: Any) -> IssuedKey:
            summary = cast("dict[str, JsonValue]", event["result"])
            row = await self._key_row(conn, str(summary["key_id"]))
            return _issued_replay(principal_id, row, summary)

        result = await self._submit(
            action="recover",
            actor_id=actor,
            target=principal_id,
            extras={},
            request_key=key,
            perform=perform,
            rebuild=rebuild,
        )
        await self._publish(((principal_id, True, "recover"),))
        return result

    async def set_memberships(
        self, actor: PrincipalContext, principal_id: str, body: object, *, request_key: str
    ) -> MembershipsSet:
        state = self._actor(actor, admin=True)
        body = self._body(SetMembershipsBody, body)
        principal_id = _principal_id(principal_id)
        members = _sorted_members(body.memberships)
        if len({m.workspace_id for m in members}) != len(members) or len(members) > MAX_MEMBERSHIPS:
            raise ServiceError("invalid_input", 422)
        key = _request_key(request_key)

        async def perform(conn: AsyncConnection) -> tuple[MembershipsSet, dict[str, JsonValue]]:
            row = await self._target_row(conn, principal_id)
            if (
                body.expected_generation is not None
                and row["authorization_generation"] != body.expected_generation
            ):
                raise ServiceError("revision_conflict")
            personal = (
                (
                    await conn.execute(
                        select(workspace_profiles.c.workspace_id).where(
                            workspace_profiles.c.owner_id == principal_id
                        )
                    )
                )
                .scalars()
                .all()
            )
            if len(personal) > 1:
                raise ServiceError("facts_corrupt")
            owner_membership: Membership | None = None
            if personal:
                stored = (
                    (
                        await conn.execute(
                            select(account_workspaces).where(
                                account_workspaces.c.principal_id == principal_id,
                                account_workspaces.c.workspace_id == personal[0],
                            )
                        )
                    )
                    .mappings()
                    .first()
                )
                owner_membership = Membership(
                    workspace_id=personal[0],
                    role=stored["role"] if stored is not None else "admin",
                    approve=stored["approve"] if stored is not None else True,
                )
            final: list[Membership] = []
            for membership in members:
                if (
                    owner_membership is not None
                    and membership.workspace_id == owner_membership.workspace_id
                ):
                    if membership != owner_membership:
                        raise ServiceError("invalid_input", 422)
                    continue
                final.append(membership)
            if owner_membership is not None:
                final.append(owner_membership)
            final_members = _sorted_members(final)
            if len(final_members) > MAX_MEMBERSHIPS:
                raise ServiceError("invalid_input", 422)
            for membership in final_members:
                known = (
                    await conn.execute(
                        select(workspace_profiles.c.workspace_id).where(
                            workspace_profiles.c.workspace_id == membership.workspace_id
                        )
                    )
                ).scalar_one_or_none()
                if known is None:
                    await conn.execute(
                        insert(workspace_profiles).values(
                            workspace_id=membership.workspace_id,
                            display_name=membership.workspace_id,
                            owner_id=None,
                        )
                    )
            await conn.execute(
                delete(account_workspaces).where(account_workspaces.c.principal_id == principal_id)
            )
            if final_members:
                await conn.execute(
                    insert(account_workspaces),
                    [
                        dict(
                            principal_id=principal_id,
                            workspace_id=membership.workspace_id,
                            role=membership.role,
                            approve=membership.approve,
                        )
                        for membership in final_members
                    ],
                )
            generation = row["authorization_generation"] + 1
            await conn.execute(
                update(accounts)
                .where(accounts.c.principal_id == principal_id)
                .values(authorization_generation=generation, updated_at=time.time())
            )
            return MembershipsSet(
                principal_id=principal_id, memberships=final_members, generation=generation
            ), {
                "principal_id": principal_id,
                "generation": generation,
                "count": len(final_members),
            }

        async def rebuild(conn: AsyncConnection, event: Any) -> MembershipsSet:
            summary = cast("dict[str, JsonValue]", event["result"])
            rows = (
                (
                    await conn.execute(
                        select(account_workspaces).where(
                            account_workspaces.c.principal_id == principal_id
                        )
                    )
                )
                .mappings()
                .all()
            )
            members_now = _sorted_members(
                Membership(
                    workspace_id=row["workspace_id"], role=row["role"], approve=row["approve"]
                )
                for row in rows
            )
            generation = summary["generation"]
            if type(generation) is not int:
                raise ServiceError("facts_corrupt")
            return MembershipsSet(
                principal_id=principal_id,
                memberships=members_now,
                generation=generation,
                replayed=True,
            )

        async def reauth(conn: AsyncConnection) -> None:
            await self._require_loginable(
                conn, state.principal_id, generation=actor.generation, admin=True
            )

        result = await self._submit(
            action="memberships",
            actor_id=state.principal_id,
            target=principal_id,
            extras={
                "memberships": cast("JsonValue", [m.model_dump(mode="json") for m in members]),
                "expected_generation": body.expected_generation,
            },
            request_key=key,
            perform=perform,
            rebuild=rebuild,
            authorize=reauth,
        )
        await self._publish(((principal_id, True, "memberships"),))
        return result

    # ---- 初始化与恢复 ----

    @staticmethod
    def _import_source(policy: AccessPolicy) -> str:
        return fingerprint(
            {
                "schema": "account-import/1",
                "principals": [
                    {
                        "principal_id": credential.principal_id,
                        "generation": credential.generation,
                        "legacy": credential.legacy,
                        "memberships": [m.model_dump(mode="json") for m in credential.memberships],
                        "token_digest": _digest(credential.token.get_secret_value()),
                    }
                    for credential in policy.credentials
                ],
            }
        )

    async def _reserve_historical_owners(self, conn: AsyncConnection, now: float) -> None:
        """保留旧资源主体，不签发凭据、不授予成员关系、不转交管理员。"""
        known = set((await conn.scalars(select(accounts.c.principal_id))).all())
        owners: set[str] = set()
        for table in (legacy_sessions, legacy_work_sessions):
            owners.update((await conn.scalars(select(table.c.owner_id).distinct())).all())
        for table in (work_turns, session_controls, artifact_revisions):
            owners.update((await conn.scalars(select(table.c.actor_id).distinct())).all())
        for owner in sorted(owners - known):
            _principal_id(owner)
            await conn.execute(
                insert(accounts).values(
                    principal_id=owner,
                    display_name=owner,
                    status="disabled",
                    platform_role="member",
                    authorization_generation=1,
                    revision=1,
                    legacy=False,
                    created_at=now,
                    updated_at=now,
                )
            )

    async def import_policy(
        self,
        policy: object,
        admin_id: str,
        *,
        request_key: str,
        display_names: dict[str, str] | None = None,
    ) -> ImportResult:
        try:
            frozen = (
                policy
                if isinstance(policy, AccessPolicy)
                else AccessPolicy.model_validate_json(encoded(policy))
            )
        except (ValidationError, ValueError):
            raise ServiceError("invalid_input", 422) from None
        admin_id = _principal_id(admin_id)
        key = _request_key(request_key)
        principals = sorted(credential.principal_id for credential in frozen.credentials)
        if admin_id not in principals:
            raise ServiceError("invalid_input", 422)
        names: dict[str, str] = {}
        if display_names is not None:
            if not isinstance(display_names, dict):
                raise ServiceError("invalid_input", 422)
            for principal, name in display_names.items():
                _principal_id(principal)
                names[principal] = _display_name(name)
            if set(names) - set(principals):
                raise ServiceError("invalid_input", 422)
        source = self._import_source(frozen)
        extras: dict[str, JsonValue] = {
            "admin_id": admin_id,
            "source": source,
            "display_names": {principal: names[principal] for principal in sorted(names)},
        }

        async def perform(conn: AsyncConnection) -> tuple[ImportResult, dict[str, JsonValue]]:
            meta = (
                await conn.execute(
                    select(account_meta.c.body).where(account_meta.c.id == IDENTITY_META_ID)
                )
            ).scalar_one_or_none()
            if isinstance(meta, dict) and meta.get("mode") == "managed":
                # 同一来源重复导入幂等；异来源冲突必须报告。
                if meta.get("source") == source:
                    items = [await self._account_view(conn, principal) for principal in principals]
                    return ImportResult(
                        accounts=tuple(items), admin_id=admin_id, source=source, replayed=True
                    ), {
                        "admin_id": admin_id,
                        "source": source,
                        "accounts": cast("JsonValue", principals),
                        "replayed": True,
                    }
                raise ServiceError("revision_conflict")
            if (await conn.execute(select(func.count()).select_from(accounts))).scalar_one():
                raise ServiceError("revision_conflict")
            now = time.time()
            for credential in frozen.credentials:
                principal = credential.principal_id
                display = names.get(principal, principal)
                role = "platform_admin" if principal == admin_id else "member"
                await conn.execute(
                    insert(accounts).values(
                        principal_id=principal,
                        display_name=display,
                        status="active",
                        platform_role=role,
                        authorization_generation=credential.generation,
                        revision=1,
                        legacy=credential.legacy,
                        created_at=now,
                        updated_at=now,
                    )
                )
                members = _sorted_members(credential.memberships)
                if not members:
                    workspace_id = str(uuid.uuid4())
                    await conn.execute(
                        insert(workspace_profiles).values(
                            workspace_id=workspace_id,
                            display_name=PERSONAL_WORKSPACE_NAME,
                            owner_id=principal,
                        )
                    )
                    await conn.execute(
                        insert(account_workspaces).values(
                            principal_id=principal,
                            workspace_id=workspace_id,
                            role="admin",
                            approve=True,
                        )
                    )
                else:
                    await conn.execute(
                        insert(account_workspaces),
                        [
                            dict(
                                principal_id=principal,
                                workspace_id=m.workspace_id,
                                role=m.role,
                                approve=m.approve,
                            )
                            for m in members
                        ],
                    )
                    for m in members:
                        known = (
                            await conn.execute(
                                select(workspace_profiles.c.workspace_id).where(
                                    workspace_profiles.c.workspace_id == m.workspace_id
                                )
                            )
                        ).scalar_one_or_none()
                        if known is None:
                            await conn.execute(
                                insert(workspace_profiles).values(
                                    workspace_id=m.workspace_id,
                                    display_name=m.workspace_id,
                                    owner_id=None,
                                )
                            )
                token = credential.token.get_secret_value()
                key_id = _new_key_id()
                await conn.execute(
                    insert(access_keys).values(
                        key_id=key_id,
                        principal_id=principal,
                        token_digest=_digest(token),
                        credential_version=1,
                        masked=_masked(key_id, token),
                        weak=len(token) < 32,
                        status="active",
                        issued_at=now,
                        revoked_at=None,
                        last_used_at=None,
                        request_key=key,
                    )
                )
            await self._reserve_historical_owners(conn, now)
            await conn.execute(
                insert(account_meta).values(
                    id=IDENTITY_META_ID,
                    body={
                        "mode": "managed",
                        "initialized_at": now,
                        "source": source,
                        "bootstrap": False,
                    },
                )
            )
            items = [await self._account_view(conn, principal) for principal in principals]
            return ImportResult(accounts=tuple(items), admin_id=admin_id, source=source), {
                "admin_id": admin_id,
                "source": source,
                "accounts": cast("JsonValue", principals),
            }

        async def rebuild(conn: AsyncConnection, event: Any) -> ImportResult:
            summary = cast("dict[str, JsonValue]", event["result"])
            accounts_list = summary.get("accounts")
            targets = (
                [str(item) for item in accounts_list]
                if isinstance(accounts_list, list)
                else principals
            )
            items = [await self._account_view(conn, principal) for principal in sorted(targets)]
            return ImportResult(
                accounts=tuple(items),
                admin_id=str(summary["admin_id"]),
                source=str(summary["source"]),
                replayed=True,
            )

        result = await self._submit(
            action="import",
            actor_id="host",
            target=None,
            extras=extras,
            request_key=key,
            perform=perform,
            rebuild=rebuild,
        )
        await self._publish(tuple((principal, False, "import") for principal in principals))
        return result

    async def bootstrap(
        self,
        display_name: str,
        *,
        request_key: str,
        principal_id: str | None = None,
        claim_local: bool = False,
    ) -> BootstrapResult:
        display = _display_name(display_name)
        key = _request_key(request_key)
        explicit_principal = principal_id is not None
        if principal_id is None:
            principal_id = str(uuid.uuid4())
        else:
            principal_id = _principal_id(principal_id)
        if claim_local and principal_id != "local":
            raise ServiceError("invalid_input", 422)
        if principal_id == "local" and not claim_local:
            raise ServiceError("invalid_input", 422)

        async def perform(conn: AsyncConnection) -> tuple[BootstrapResult, dict[str, JsonValue]]:
            meta = (
                await conn.execute(
                    select(account_meta.c.body).where(account_meta.c.id == IDENTITY_META_ID)
                )
            ).scalar_one_or_none()
            if isinstance(meta, dict) and meta.get("mode") == "managed":
                if (
                    await conn.execute(
                        select(accounts.c.principal_id).where(
                            accounts.c.principal_id == principal_id
                        )
                    )
                ).scalar_one_or_none() is not None:
                    view = await self._account_view(conn, principal_id)
                    return BootstrapResult(account=view, replayed=True), {
                        "principal_id": principal_id,
                        "key_id": view.key.key_id if view.key is not None else None,
                        "replayed": True,
                    }
                raise ServiceError("revision_conflict")
            if (await conn.execute(select(func.count()).select_from(accounts))).scalar_one():
                raise ServiceError("revision_conflict")
            if not claim_local and await self._legacy_local_data(conn):
                # 已有local归属数据：必须显式claim_local认领，不得静默认领或遗弃。
                raise ServiceError("revision_conflict")
            now = time.time()
            workspace_id = "local" if claim_local else str(uuid.uuid4())
            await conn.execute(
                insert(accounts).values(
                    principal_id=principal_id,
                    display_name=display,
                    status="active",
                    platform_role="platform_admin",
                    authorization_generation=1,
                    revision=1,
                    legacy=claim_local,
                    created_at=now,
                    updated_at=now,
                )
            )
            known = (
                await conn.execute(
                    select(workspace_profiles.c.workspace_id).where(
                        workspace_profiles.c.workspace_id == workspace_id
                    )
                )
            ).scalar_one_or_none()
            if known is None:
                await conn.execute(
                    insert(workspace_profiles).values(
                        workspace_id=workspace_id,
                        display_name=PERSONAL_WORKSPACE_NAME,
                        owner_id=principal_id,
                    )
                )
            await conn.execute(
                insert(account_workspaces).values(
                    principal_id=principal_id,
                    workspace_id=workspace_id,
                    role="admin",
                    approve=True,
                )
            )
            key_id, token = await self._new_key_row(
                conn, principal_id, request_key=key, credential_version=1
            )
            await self._reserve_historical_owners(conn, now)
            await conn.execute(
                insert(account_meta).values(
                    id=IDENTITY_META_ID,
                    body={
                        "mode": "managed",
                        "initialized_at": now,
                        "source": None,
                        "bootstrap": True,
                    },
                )
            )
            view = await self._account_view(conn, principal_id)
            return BootstrapResult(account=view, token=OneTimeToken(token)), {
                "principal_id": principal_id,
                "key_id": key_id,
            }

        async def rebuild(conn: AsyncConnection, event: Any) -> BootstrapResult:
            summary = cast("dict[str, JsonValue]", event["result"])
            view = await self._account_view(conn, str(summary["principal_id"]))
            return BootstrapResult(account=view, token=None, replayed=True)

        result = await self._submit(
            action="bootstrap",
            actor_id="host",
            target=principal_id,
            # principal_id仅在宿主显式指定时入payload；自动生成ID保持重放稳定。
            extras={
                "display_name": display,
                **({"principal_id": principal_id} if explicit_principal else {}),
                "claim_local": claim_local,
            },
            request_key=key,
            perform=perform,
            rebuild=rebuild,
            target_in_digest=False,
        )
        await self._publish(((principal_id, False, "bootstrap"),))
        return result
