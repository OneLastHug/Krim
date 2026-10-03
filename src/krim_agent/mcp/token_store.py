"""SQLite OAuth 密文与短租约；密钥和数据库由宿主拥有，见 ADR-0015。"""

from __future__ import annotations

import asyncio
import json
import math
import time
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from contextvars import ContextVar
from uuid import uuid4

from cryptography.fernet import Fernet, InvalidToken, MultiFernet
from sqlalchemy import or_, select, update
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.persistence.database import Database, OAuthTokenRow
from krim_agent.protocol.models import utc_now

from .oauth import OAuthConfig, OAuthError, OAuthToken, TokenSnapshot, encode_token, token_scope


class PersistentEncryptedTokenStore:
    def __init__(
        self,
        database: Database,
        keys: bytes | str | Sequence[bytes | str],
        *,
        lease_seconds: float = 60,
        acquire_timeout_seconds: float = 30,
    ) -> None:
        if not math.isfinite(lease_seconds) or not 2 <= lease_seconds <= 600:
            raise ValueError("OAuth 租约寿命必须在 2–600 秒之间")
        if not math.isfinite(acquire_timeout_seconds) or not 0 < acquire_timeout_seconds <= 120:
            raise ValueError("OAuth 租约等待预算必须在 0–120 秒之间")
        values = [keys] if isinstance(keys, (bytes, str)) else list(keys)
        try:
            self._keys = tuple(
                value.encode() if isinstance(value, str) else value for value in values
            )
            if not self._keys:
                raise ValueError
            self._cipher = MultiFernet([Fernet(value) for value in self._keys])
        except (TypeError, ValueError) as error:
            raise OAuthError("invalid_encryption_key", "Fernet 密钥环无效") from error
        self.database = database
        self.lease_seconds = lease_seconds
        self.acquire_timeout_seconds = acquire_timeout_seconds
        self._active_lease: ContextVar[tuple[str, str, str] | None] = ContextVar(
            f"oauth-lease-{uuid4()}", default=None
        )

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncSession]:
        try:
            async with self.database.write_lock, self.database.sessions.begin() as session:
                yield session
        except SQLAlchemyError as error:
            raise OAuthError("token_store_failed", "OAuth 令牌存储操作失败") from error

    async def _seed(
        self, session: AsyncSession, config: OAuthConfig, subject: str, scope: str
    ) -> None:
        await session.execute(
            insert(OAuthTokenRow)
            .values(
                scope_hash=scope,
                subject=subject,
                server=config.server,
                ciphertext=None,
                revision=0,
                lease_owner=None,
                lease_expires_at=None,
                updated_at=utc_now(),
            )
            .on_conflict_do_nothing(index_elements=["scope_hash", "subject"])
        )

    def _encode(self, token: OAuthToken, scope: str, subject: str) -> bytes:
        envelope = json.dumps(
            {
                "version": 1,
                "scope_hash": scope,
                "subject": subject,
                "token": json.loads(encode_token(token)),
            },
            allow_nan=False,
        ).encode()
        if len(envelope) > 65536:
            raise OAuthError("invalid_token_response", "OAuth 加密载荷超过大小限制")
        return self._cipher.encrypt(envelope)

    def _decode(self, ciphertext: bytes, scope: str, subject: str) -> OAuthToken:
        try:
            envelope = json.loads(self._cipher.decrypt(ciphertext))
            if not isinstance(envelope, dict) or envelope.get("version") != 1:
                raise ValueError
            if envelope.get("scope_hash") != scope or envelope.get("subject") != subject:
                raise OAuthError("token_scope_mismatch", "OAuth 密文与记录作用域不一致")
            return OAuthToken.model_validate_json(json.dumps(envelope["token"]))
        except (InvalidToken, KeyError, TypeError, ValueError) as error:
            raise OAuthError("token_corrupt", "OAuth token 密文无法验证") from error

    async def read(self, config: OAuthConfig, subject: str) -> TokenSnapshot:
        scope = token_scope(config, subject)
        async with self._transaction() as session:
            row = await session.get(OAuthTokenRow, (scope, subject))
            if row is None:
                return TokenSnapshot(None, 0)
            token = (
                None
                if row.ciphertext is None
                else self._decode(row.ciphertext, row.scope_hash, row.subject)
            )
            return TokenSnapshot(token, row.revision)

    async def write(
        self,
        config: OAuthConfig,
        subject: str,
        token: OAuthToken,
        *,
        expected_revision: int | None = None,
    ) -> None:
        scope = token_scope(config, subject)
        ciphertext = self._encode(token, scope, subject)
        query = update(OAuthTokenRow).where(
            OAuthTokenRow.scope_hash == scope, OAuthTokenRow.subject == subject
        )
        if expected_revision is not None:
            query = query.where(OAuthTokenRow.revision == expected_revision)
        lease = self._active_lease.get()
        if lease is not None:
            if lease[:2] != (scope, subject):
                raise OAuthError("token_scope_mismatch", "OAuth 写入与当前租约作用域不一致")
            query = query.where(
                OAuthTokenRow.lease_owner == lease[2],
                OAuthTokenRow.lease_expires_at > time.time(),
            )
        else:
            query = query.where(
                or_(
                    OAuthTokenRow.lease_owner.is_(None),
                    OAuthTokenRow.lease_expires_at <= time.time(),
                )
            )
        async with self._transaction() as session:
            await self._seed(session, config, subject, scope)
            result = await session.execute(
                query.values(
                    ciphertext=ciphertext, revision=OAuthTokenRow.revision + 1, updated_at=utc_now()
                ).returning(OAuthTokenRow.scope_hash)
            )
            if result.scalar_one_or_none() is None:
                raise OAuthError("token_conflict", "OAuth 令牌或租约在请求期间已变化")

    async def delete(self, config: OAuthConfig, subject: str) -> None:
        scope = token_scope(config, subject)
        async with self._transaction() as session:
            await self._seed(session, config, subject, scope)
            await session.execute(
                update(OAuthTokenRow)
                .where(OAuthTokenRow.scope_hash == scope, OAuthTokenRow.subject == subject)
                .values(ciphertext=None, revision=OAuthTokenRow.revision + 1, updated_at=utc_now())
            )

    async def _acquire(self, config: OAuthConfig, subject: str, scope: str, owner: str) -> bool:
        async with self._transaction() as session:
            await self._seed(session, config, subject, scope)
            result = await session.execute(
                update(OAuthTokenRow)
                .where(
                    OAuthTokenRow.scope_hash == scope,
                    OAuthTokenRow.subject == subject,
                    or_(
                        OAuthTokenRow.lease_owner.is_(None),
                        OAuthTokenRow.lease_expires_at <= time.time(),
                    ),
                )
                .values(lease_owner=owner, lease_expires_at=time.time() + self.lease_seconds)
                .returning(OAuthTokenRow.scope_hash)
            )
            return result.scalar_one_or_none() is not None

    async def _release(self, scope: str, subject: str, owner: str) -> None:
        async with self._transaction() as session:
            await session.execute(
                update(OAuthTokenRow)
                .where(
                    OAuthTokenRow.scope_hash == scope,
                    OAuthTokenRow.subject == subject,
                    OAuthTokenRow.lease_owner == owner,
                )
                .values(lease_owner=None, lease_expires_at=None)
            )

    @asynccontextmanager
    async def lock(self, config: OAuthConfig, subject: str) -> AsyncIterator[None]:
        scope, owner = token_scope(config, subject), str(uuid4())
        deadline = asyncio.get_running_loop().time() + self.acquire_timeout_seconds
        while True:
            attempt = asyncio.create_task(self._acquire(config, subject, scope, owner))
            try:
                acquired = await asyncio.shield(attempt)
            except asyncio.CancelledError:
                if await attempt:
                    await asyncio.shield(self._release(scope, subject, owner))
                raise
            if acquired:
                break
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise OAuthError("token_lock_timeout", "OAuth 刷新租约等待超时")
            await asyncio.sleep(min(0.04, remaining))
        context = self._active_lease.set((scope, subject, owner))
        try:
            # 先取消本机合作式请求，再允许其他进程接管过期租约。
            async with asyncio.timeout(self.lease_seconds * 0.75):
                yield
        except TimeoutError as error:
            raise OAuthError("token_lock_timeout", "OAuth 租约内操作超过时间预算") from error
        finally:
            self._active_lease.reset(context)
            release = asyncio.create_task(self._release(scope, subject, owner))
            try:
                await asyncio.shield(release)
            except asyncio.CancelledError:
                await release
                raise

    async def rotate_key(self, new_key: bytes | str) -> int:
        value = new_key.encode() if isinstance(new_key, str) else new_key
        if value not in self._keys:
            raise OAuthError("rotation_key_missing", "轮换前须向读密钥环分发新密钥")
        primary = Fernet(value)
        count = 0
        async with self._transaction() as session:
            rows = list(
                await session.scalars(
                    select(OAuthTokenRow).order_by(OAuthTokenRow.scope_hash, OAuthTokenRow.subject)
                )
            )
            if any(
                row.lease_owner is not None and (row.lease_expires_at or 0) > time.time()
                for row in rows
            ):
                raise OAuthError("token_busy", "活跃 OAuth 租约期间不能轮换密钥")
            for row in rows:
                if row.ciphertext is None:
                    continue
                self._decode(row.ciphertext, row.scope_hash, row.subject)
                row.ciphertext = primary.encrypt(self._cipher.decrypt(row.ciphertext))
                row.revision += 1
                row.updated_at = utc_now()
                count += 1
        self._keys = (value, *(key for key in self._keys if key != value))
        self._cipher = MultiFernet([Fernet(key) for key in self._keys])
        return count
