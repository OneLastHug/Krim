"""SQLite 加密 PKCE 状态；跨进程单次消费边界见 ADR-0078。"""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Literal

from cryptography.fernet import Fernet, InvalidToken, MultiFernet
from pydantic import Field
from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.persistence.database import Database, OAuthStateRow
from krim_agent.protocol.models import StrictModel

from .oauth import (
    OAuthConfig,
    OAuthError,
    PKCERequest,
    _pkce_request,
    token_scope,
    validate_token_subject,
)


class _StatePayload(StrictModel):
    version: Literal[1]
    state_hash: str
    subject: str
    scope_hash: str
    config: OAuthConfig
    state: str = Field(pattern=r"^[A-Za-z0-9_-]{43}$")
    verifier: str = Field(pattern=r"^[A-Za-z0-9_-]{43,128}$", repr=False)
    created_at: float = Field(allow_inf_nan=False)
    expires_at: float = Field(allow_inf_nan=False)


def _state_hash(state: str) -> str:
    if not state or len(state) > 256 or re.fullmatch(r"[A-Za-z0-9_-]+", state) is None:
        raise OAuthError("invalid_state", "OAuth state 无效")
    return hashlib.sha256(state.encode()).hexdigest()


class PersistentEncryptedStateStore:
    def __init__(
        self,
        database: Database,
        keys: bytes | str | Sequence[bytes | str],
        *,
        max_states: int = 4096,
        max_subject_states: int = 64,
    ) -> None:
        if not 1 <= max_subject_states <= max_states <= 65536:
            raise ValueError("OAuth 状态容量必须在 1–65536 范围内")
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
        self.max_states, self.max_subject_states = max_states, max_subject_states

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncSession]:
        try:
            async with self.database.write_lock, self.database.sessions.begin() as session:
                yield session
        except SQLAlchemyError as error:
            raise OAuthError("state_store_failed", "OAuth 状态存储操作失败") from error

    def _encode(self, config: OAuthConfig, subject: str, scope: str, request: PKCERequest) -> bytes:
        configured = config.model_dump(mode="json")
        configured["client_secret"] = (
            config.client_secret.get_secret_value() if config.client_secret is not None else None
        )
        encoded = json.dumps(
            {
                "version": 1,
                "state_hash": _state_hash(request.state),
                "subject": subject,
                "scope_hash": scope,
                "config": configured,
                "state": request.state,
                "verifier": request.code_verifier,
                "created_at": request.created_at,
                "expires_at": request.created_at + config.state_ttl_seconds,
            },
            allow_nan=False,
        ).encode()
        if len(encoded) > 65536:
            raise OAuthError("state_payload_limit", "OAuth 状态加密载荷超过大小限制")
        return self._cipher.encrypt(encoded)

    def _decode(
        self,
        ciphertext: bytes,
        state_hash: str,
        subject: str,
        scope: str,
        created_at: float,
        expires_at: float,
    ) -> tuple[OAuthConfig, PKCERequest]:
        try:
            payload = _StatePayload.model_validate_json(self._cipher.decrypt(ciphertext))
            if (
                payload.state_hash != state_hash
                or _state_hash(payload.state) != state_hash
                or payload.subject != subject
                or payload.scope_hash != scope
                or token_scope(payload.config, subject) != scope
                or payload.created_at != created_at
                or payload.expires_at != expires_at
                or expires_at != created_at + payload.config.state_ttl_seconds
            ):
                raise ValueError
            return payload.config, _pkce_request(
                payload.config,
                state=payload.state,
                verifier=payload.verifier,
                created_at=created_at,
            )
        except (InvalidToken, ValueError, OAuthError):
            raise OAuthError("state_corrupt", "OAuth 状态密文或绑定无法验证") from None

    async def begin(self, config: OAuthConfig, *, subject: str) -> PKCERequest:
        config = config.model_copy(deep=True)
        scope = token_scope(config, subject)
        request = _pkce_request(config)
        ciphertext = self._encode(config, subject, scope, request)
        async with self._transaction() as session:
            # 第一条语句取得 SQLite 写锁；容量检查与插入在独立进程间也串行化。
            await session.execute(
                delete(OAuthStateRow).where(OAuthStateRow.expires_at <= time.time())
            )
            total = await session.scalar(select(func.count()).select_from(OAuthStateRow))
            owned = await session.scalar(
                select(func.count())
                .select_from(OAuthStateRow)
                .where(OAuthStateRow.subject == subject)
            )
            if (total or 0) >= self.max_states or (owned or 0) >= self.max_subject_states:
                raise OAuthError("state_store_full", "OAuth 登录状态超过容量限制，请等待到期或清理")
            session.add(
                OAuthStateRow(
                    state_hash=_state_hash(request.state),
                    subject=subject,
                    scope_hash=scope,
                    ciphertext=ciphertext,
                    status="pending",
                    created_at=request.created_at,
                    expires_at=request.created_at + config.state_ttl_seconds,
                    ended_at=None,
                )
            )
        return request

    async def consume(
        self, state: str, *, subject: str, expected_config: OAuthConfig | None = None
    ) -> tuple[OAuthConfig, PKCERequest]:
        validate_token_subject(subject)
        state_hash = _state_hash(state)
        filters = [
            OAuthStateRow.state_hash == state_hash,
            OAuthStateRow.subject == subject,
            OAuthStateRow.status == "pending",
        ]
        if expected_config is not None:
            filters.append(OAuthStateRow.scope_hash == token_scope(expected_config, subject))
        value: tuple[OAuthConfig, PKCERequest] | None = None
        failure: OAuthError | None = None
        async with self._transaction() as session:
            # 条件 UPDATE 先取得写锁并返回原密文；终态写入前不释放事务，
            # 其他进程无法读后竞争消费，不依赖本进程的 asyncio 锁。
            row = (
                await session.execute(
                    update(OAuthStateRow)
                    .where(*filters)
                    .values(ciphertext=OAuthStateRow.ciphertext)
                    .returning(
                        OAuthStateRow.ciphertext,
                        OAuthStateRow.scope_hash,
                        OAuthStateRow.created_at,
                        OAuthStateRow.expires_at,
                    )
                )
            ).first()
            # 写锁等待也计入 TTL；不能用等待之前的时间接受已经过期的回调。
            now = time.time()
            if row is None:
                failure = OAuthError("invalid_state", "OAuth state 不存在、已结束或绑定不匹配")
            else:
                status = "consumed"
                if row.expires_at <= now:
                    status = "expired"
                    failure = OAuthError("expired_state", "OAuth state 已过期")
                else:
                    try:
                        value = self._decode(
                            row.ciphertext,
                            state_hash,
                            subject,
                            row.scope_hash,
                            row.created_at,
                            row.expires_at,
                        )
                    except OAuthError as error:
                        status, failure = "invalidated", error
                await session.execute(
                    update(OAuthStateRow)
                    .where(OAuthStateRow.state_hash == state_hash)
                    .values(ciphertext=None, status=status, ended_at=now)
                )
        # 先提交终态与秘密清除，再报告失败，防止异常导致事务回滚为 pending。
        if failure is not None:
            raise failure
        if value is None:
            raise OAuthError("invalid_state", "OAuth state 不可消费")
        return value

    async def discard(self, state: str, *, subject: str) -> None:
        validate_token_subject(subject)
        state_hash = _state_hash(state)
        async with self._transaction() as session:
            await session.execute(
                update(OAuthStateRow)
                .where(
                    OAuthStateRow.state_hash == state_hash,
                    OAuthStateRow.subject == subject,
                    OAuthStateRow.status == "pending",
                )
                .values(status="cancelled", ciphertext=None, ended_at=time.time())
            )

    async def prune(self, *, subject: str) -> int:
        validate_token_subject(subject)
        async with self._transaction() as session:
            removed = (
                await session.execute(
                    delete(OAuthStateRow)
                    .where(
                        OAuthStateRow.subject == subject, OAuthStateRow.expires_at <= time.time()
                    )
                    .returning(OAuthStateRow.state_hash)
                )
            ).all()
        return len(removed)

    async def rotate_key(self, new_key: bytes | str) -> int:
        value = new_key.encode() if isinstance(new_key, str) else new_key
        if value not in self._keys:
            raise OAuthError("rotation_key_missing", "轮换前须向读密钥环分发新密钥")
        primary, count = Fernet(value), 0
        async with self._transaction() as session:
            # 先写再读，与其他连接的开始/消费/清理保持同一数据库串行顺序。
            await session.execute(
                update(OAuthStateRow)
                .where(OAuthStateRow.status == "pending")
                .values(ciphertext=OAuthStateRow.ciphertext)
            )
            rows = list(await session.scalars(select(OAuthStateRow)))
            for row in rows:
                if row.ciphertext is None:
                    continue
                self._decode(
                    row.ciphertext,
                    row.state_hash,
                    row.subject,
                    row.scope_hash,
                    row.created_at,
                    row.expires_at,
                )
                row.ciphertext = primary.encrypt(self._cipher.decrypt(row.ciphertext))
                count += 1
        self._keys = (value, *(key for key in self._keys if key != value))
        self._cipher = MultiFernet([Fernet(key) for key in self._keys])
        return count
