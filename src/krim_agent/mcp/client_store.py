"""OAuth DCR配置的密文绑定与跨进程注册租约，见ADR-0083。"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import time
from collections.abc import AsyncIterator, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Literal, Protocol
from uuid import uuid4

import httpx
from cryptography.fernet import Fernet, InvalidToken, MultiFernet
from pydantic import Field
from sqlalchemy import or_, select, update
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.persistence.database import Database, OAuthClientRegistrationRow
from krim_agent.protocol.models import StrictModel, utc_now

from .oauth import OAuthConfig, OAuthError, validate_oauth_endpoint, validate_token_subject


class OAuthClientBinding(StrictModel):
    server: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,31}$")
    subject: str = Field(min_length=1, max_length=256)
    resource: str = Field(min_length=1, max_length=2048)
    issuer: str | None = None
    redirect_uri: str = Field(min_length=1, max_length=2048)
    scopes: list[str] = Field(default_factory=list, max_length=64)
    allowed_hosts: set[str] = Field(default_factory=set, max_length=64)
    client_name: str = Field(default="Krim Agent", min_length=1, max_length=128)

    def digest(self) -> str:
        validate_token_subject(self.subject)
        try:
            resource = httpx.URL(self.resource)
            validate_oauth_endpoint(self.resource, {resource.host}, allow_query=True)
            if self.issuer is not None:
                validate_oauth_endpoint(self.issuer, self.allowed_hosts)
        except (httpx.InvalidURL, OAuthError):
            raise OAuthError(
                "oauth_client_binding_invalid", "OAuth DCR声明URL或主机边界无效"
            ) from None
        payload = self.model_dump(mode="json")
        payload["scopes"] = sorted(set(self.scopes))
        payload["allowed_hosts"] = sorted(host.lower().rstrip(".") for host in self.allowed_hosts)
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, allow_nan=False).encode()
        ).hexdigest()

    def validate_config(self, config: OAuthConfig) -> None:
        config.validate_endpoints()
        validate_token_subject(self.subject)
        for endpoint in (config.authorization_endpoint, config.token_endpoint, config.issuer):
            if endpoint is not None:
                validate_oauth_endpoint(endpoint, self.allowed_hosts)
        resource, actual = httpx.URL(self.resource), httpx.URL(config.resource or "")
        if (
            config.server != self.server
            or config.redirect_uri != self.redirect_uri
            or set(config.scopes) != set(self.scopes)
            or config.issuer is None
            or (self.issuer is not None and self.issuer.rstrip("/") != config.issuer.rstrip("/"))
            or (resource.scheme, resource.host, resource.port)
            != (actual.scheme, actual.host, actual.port)
            or not (resource.path.rstrip("/") + "/").startswith(actual.path.rstrip("/") + "/")
            or not {host.lower().rstrip(".") for host in config.allowed_hosts}.issubset(
                {host.lower().rstrip(".") for host in self.allowed_hosts}
            )
        ):
            raise OAuthError("oauth_client_binding_mismatch", "OAuth DCR配置不属于原声明")


@dataclass(frozen=True)
class OAuthClientSnapshot:
    config: OAuthConfig | None
    revision: int


class OAuthClientRegistrationStore(Protocol):
    async def read(self, binding: OAuthClientBinding) -> OAuthClientSnapshot: ...

    async def write(
        self, binding: OAuthClientBinding, config: OAuthConfig, *, expected_revision: int
    ) -> None: ...

    def lock(self, binding: OAuthClientBinding) -> AbstractAsyncContextManager[None]: ...


class _ClientPayload(StrictModel):
    version: Literal[1]
    binding_hash: str
    subject: str
    binding: OAuthClientBinding
    config: OAuthConfig


class PersistentOAuthClientRegistrationStore:
    def __init__(
        self,
        database: Database,
        keys: bytes | str | Sequence[bytes | str],
        *,
        lease_seconds: float = 60,
        acquire_timeout_seconds: float = 30,
    ) -> None:
        if not math.isfinite(lease_seconds) or not 2 <= lease_seconds <= 600:
            raise ValueError("OAuth DCR租约寿命必须在2–600秒")
        if not math.isfinite(acquire_timeout_seconds) or not 0 < acquire_timeout_seconds <= 120:
            raise ValueError("OAuth DCR等待预算无效")
        values = [keys] if isinstance(keys, (bytes, str)) else list(keys)
        try:
            self._keys = tuple(
                value.encode() if isinstance(value, str) else value for value in values
            )
            if not self._keys:
                raise ValueError
            self._cipher = MultiFernet([Fernet(value) for value in self._keys])
        except (TypeError, ValueError) as error:
            raise OAuthError("invalid_encryption_key", "Fernet密钥环无效") from error
        self.database, self.lease_seconds, self.acquire_timeout_seconds = (
            database,
            lease_seconds,
            acquire_timeout_seconds,
        )
        self._lease: ContextVar[tuple[str, str] | None] = ContextVar(
            f"oauth-client-{uuid4()}", default=None
        )

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncSession]:
        try:
            async with self.database.write_lock, self.database.sessions.begin() as session:
                yield session
        except SQLAlchemyError:
            raise OAuthError("oauth_client_store_failed", "OAuth DCR客户端存储失败") from None

    def _encode(self, binding: OAuthClientBinding, config: OAuthConfig) -> bytes:
        binding.validate_config(config)
        values = config.model_dump(mode="json")
        values["client_secret"] = (
            config.client_secret.get_secret_value() if config.client_secret is not None else None
        )
        encoded = json.dumps(
            {
                "version": 1,
                "binding_hash": binding.digest(),
                "subject": binding.subject,
                "binding": binding.model_dump(mode="json"),
                "config": values,
            },
            allow_nan=False,
        ).encode()
        if len(encoded) > 65536:
            raise OAuthError("oauth_client_payload_limit", "OAuth DCR客户端载荷超过大小限制")
        return self._cipher.encrypt(encoded)

    def _decode(self, binding: OAuthClientBinding, blob: bytes) -> OAuthConfig:
        try:
            payload = _ClientPayload.model_validate_json(self._cipher.decrypt(blob))
            if (
                payload.binding_hash != binding.digest()
                or payload.binding.digest() != binding.digest()
                or payload.subject != binding.subject
            ):
                raise ValueError
            binding.validate_config(payload.config)
            return payload.config
        except (InvalidToken, ValueError, OAuthError):
            raise OAuthError("oauth_client_corrupt", "OAuth DCR客户端密文或绑定无法验证") from None

    async def read(self, binding: OAuthClientBinding) -> OAuthClientSnapshot:
        digest = binding.digest()
        async with self._transaction() as session:
            row = await session.get(OAuthClientRegistrationRow, digest)
            if row is None:
                return OAuthClientSnapshot(None, 0)
            if row.subject != binding.subject or row.server != binding.server:
                raise OAuthError("oauth_client_corrupt", "OAuth DCR客户端外层绑定无法验证")
            return OAuthClientSnapshot(
                self._decode(binding, row.ciphertext) if row.ciphertext else None, row.revision
            )

    async def write(
        self, binding: OAuthClientBinding, config: OAuthConfig, *, expected_revision: int
    ) -> None:
        encoded, digest, lease = self._encode(binding, config), binding.digest(), self._lease.get()
        if lease is None or lease[0] != digest:
            raise OAuthError("oauth_client_lease", "OAuth DCR客户端不持有注册租约")
        async with self._transaction() as session:
            written = await session.scalar(
                update(OAuthClientRegistrationRow)
                .where(
                    OAuthClientRegistrationRow.binding_hash == digest,
                    OAuthClientRegistrationRow.subject == binding.subject,
                    OAuthClientRegistrationRow.server == binding.server,
                    OAuthClientRegistrationRow.revision == expected_revision,
                    OAuthClientRegistrationRow.lease_owner == lease[1],
                    OAuthClientRegistrationRow.lease_expires_at > time.time(),
                )
                .values(ciphertext=encoded, revision=expected_revision + 1, updated_at=utc_now())
                .returning(OAuthClientRegistrationRow.binding_hash)
            )
            if written is None:
                raise OAuthError("oauth_client_conflict", "OAuth DCR客户端版本或租约已变化")

    async def _release(self, digest: str, owner: str) -> None:
        async with self._transaction() as session:
            await session.execute(
                update(OAuthClientRegistrationRow)
                .where(
                    OAuthClientRegistrationRow.binding_hash == digest,
                    OAuthClientRegistrationRow.lease_owner == owner,
                )
                .values(lease_owner=None, lease_expires_at=None, updated_at=utc_now())
            )

    @asynccontextmanager
    async def lock(self, binding: OAuthClientBinding) -> AsyncIterator[None]:
        digest, owner = binding.digest(), str(uuid4())
        if self._lease.get() is not None:
            raise OAuthError("oauth_client_lock_reentrant", "OAuth DCR租约不能重入")
        async with self._transaction() as session:
            await session.execute(
                insert(OAuthClientRegistrationRow)
                .values(
                    binding_hash=digest,
                    subject=binding.subject,
                    server=binding.server,
                    ciphertext=None,
                    revision=0,
                    lease_owner=None,
                    lease_expires_at=None,
                    updated_at=utc_now(),
                )
                .on_conflict_do_nothing(index_elements=["binding_hash"])
            )
        deadline = time.monotonic() + self.acquire_timeout_seconds
        while True:
            now = time.time()
            async with self._transaction() as session:
                acquired = await session.scalar(
                    update(OAuthClientRegistrationRow)
                    .where(
                        OAuthClientRegistrationRow.binding_hash == digest,
                        OAuthClientRegistrationRow.subject == binding.subject,
                        OAuthClientRegistrationRow.server == binding.server,
                        or_(
                            OAuthClientRegistrationRow.lease_owner.is_(None),
                            OAuthClientRegistrationRow.lease_expires_at <= now,
                        ),
                    )
                    .values(lease_owner=owner, lease_expires_at=now + self.lease_seconds)
                    .returning(OAuthClientRegistrationRow.binding_hash)
                )
            if acquired:
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise OAuthError("oauth_client_lock_timeout", "OAuth DCR客户端注册等待超时")
            await asyncio.sleep(min(0.03, remaining))
        token = self._lease.set((digest, owner))
        try:
            async with asyncio.timeout(self.lease_seconds * 0.75):
                yield
        except TimeoutError:
            raise OAuthError("oauth_client_lock_timeout", "OAuth DCR租约内操作超时") from None
        finally:
            self._lease.reset(token)
            cleanup = asyncio.create_task(self._release(digest, owner))
            cancelled = False
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    cancelled = True
            cleanup.result()
            if cancelled:
                raise asyncio.CancelledError

    async def delete(self, binding: OAuthClientBinding) -> None:
        async with self._transaction() as session:
            await session.execute(
                update(OAuthClientRegistrationRow)
                .where(
                    OAuthClientRegistrationRow.binding_hash == binding.digest(),
                    OAuthClientRegistrationRow.subject == binding.subject,
                )
                .values(
                    ciphertext=None,
                    revision=OAuthClientRegistrationRow.revision + 1,
                    lease_owner=None,
                    lease_expires_at=None,
                    updated_at=utc_now(),
                )
            )

    async def rotate_key(self, new_key: bytes | str) -> int:
        value = new_key.encode() if isinstance(new_key, str) else new_key
        if value not in self._keys:
            raise OAuthError("rotation_key_missing", "轮换前须向读密钥环分发新密钥")
        primary, count = Fernet(value), 0
        async with self._transaction() as session:
            await session.execute(
                update(OAuthClientRegistrationRow).values(
                    revision=OAuthClientRegistrationRow.revision
                )
            )
            rows = list(await session.scalars(select(OAuthClientRegistrationRow)))
            if any(
                row.lease_owner is not None and (row.lease_expires_at or 0) > time.time()
                for row in rows
            ):
                raise OAuthError("oauth_client_busy", "活跃DCR租约期间不能轮换")
            for row in rows:
                if row.ciphertext is None:
                    continue
                try:
                    data = self._cipher.decrypt(row.ciphertext)
                    payload = _ClientPayload.model_validate_json(data)
                    if (
                        payload.binding_hash != row.binding_hash
                        or payload.binding.digest() != row.binding_hash
                        or payload.subject != row.subject
                        or payload.config.server != row.server
                    ):
                        raise ValueError
                    payload.binding.validate_config(payload.config)
                except (InvalidToken, ValueError, OAuthError):
                    raise OAuthError(
                        "oauth_client_corrupt", "OAuth DCR客户端密文无法轮换"
                    ) from None
                row.ciphertext = primary.encrypt(data)
                row.revision += 1
                row.updated_at = utc_now()
                count += 1
        self._keys = (value, *(key for key in self._keys if key != value))
        self._cipher = MultiFernet([Fernet(key) for key in self._keys])
        return count
