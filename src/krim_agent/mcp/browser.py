"""绑定发起浏览器的OAuth授权intent；code只在交换请求内存中使用。"""

from __future__ import annotations

import asyncio
import hashlib
import re
import secrets
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Literal
from uuid import uuid4

from cryptography.fernet import Fernet, InvalidToken
from pydantic import Field
from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from krim_agent.persistence.database import Database, OAuthBrowserCallbackRow, OAuthStateRow
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel, utc_now

from .auth import MCPOAuthAuth
from .client import _finish_cleanup
from .management import MCPControlRequest, MCPManager
from .oauth import OAuthError, PKCERequest
from .state_store import PersistentEncryptedStateStore
from .token_store import PersistentEncryptedTokenStore

CALLBACK_PATH = "/v1/mcp/oauth/callback"
Phase = Literal["pending", "exchanging", "complete", "failed", "cancelled", "expired"]


def callback_hash(state: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9_-]{43}", state) is None:
        raise ServiceError("oauth_callback_invalid", "OAuth回程state无效", 400)
    return hashlib.sha256(state.encode()).hexdigest()


def cookie_name(attempt_id: str) -> str:
    return "krim-oauth-" + attempt_id.replace("-", "")


class OAuthBrowserView(StrictModel):
    attempt_id: str
    name: str
    phase: Phase
    expires_at: float
    authorization_url: str | None = None
    error_code: str | None = None


class _Intent(StrictModel):
    version: Literal[1]
    attempt_id: str
    scope: str
    name: str
    subject: str
    resource_hash: str
    profile_revision: int
    state: str = Field(pattern=r"^[A-Za-z0-9_-]{43}$")
    state_hash: str
    browser_hash: str
    authorization_url: str = Field(max_length=8192)
    issuer: str | None = None
    expires_at: float


class PersistentOAuthBrowserCallbackStore:
    def __init__(self, database: Database, key: bytes | str) -> None:
        try:
            self._cipher = Fernet(key.encode() if isinstance(key, str) else key)
        except (TypeError, ValueError) as error:
            raise ValueError("OAuth浏览器intent密钥无效") from error
        self.database = database

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncSession]:
        try:
            async with self.database.write_lock, self.database.sessions.begin() as session:
                yield session
        except SQLAlchemyError:
            raise ServiceError(
                "oauth_browser_store_failed", "OAuth浏览器授权存储失败", 503
            ) from None

    def _decode(self, row: OAuthBrowserCallbackRow) -> _Intent:
        if row.ciphertext is None:
            raise ServiceError("oauth_browser_ended", "OAuth授权记录已结束", 409)
        try:
            value = _Intent.model_validate_json(self._cipher.decrypt(row.ciphertext))
            if (
                value.attempt_id != row.attempt_id
                or value.scope != row.scope
                or value.name != row.name
                or value.subject != row.subject
                or value.resource_hash != row.resource_hash
                or value.browser_hash != row.browser_hash
                or value.expires_at != row.expires_at
                or callback_hash(value.state) != row.state_hash
                or value.state_hash != row.state_hash
            ):
                raise ValueError
            return value
        except (InvalidToken, ValueError):
            raise ServiceError(
                "oauth_browser_corrupt", "OAuth授权intent密文或绑定无效", 409
            ) from None

    def _view(self, row: OAuthBrowserCallbackRow) -> OAuthBrowserView:
        phase: Phase = row.phase  # type: ignore[assignment]
        if phase == "pending" and row.expires_at <= time.time():
            phase = "expired"
        intent = self._decode(row) if phase == "pending" else None
        return OAuthBrowserView(
            attempt_id=row.attempt_id,
            name=row.name,
            phase=phase,
            expires_at=row.expires_at,
            authorization_url=intent.authorization_url if intent else None,
            error_code=row.error_code,
        )

    async def prepare(
        self,
        *,
        scope: str,
        name: str,
        subject: str,
        resource_hash: str,
        profile_revision: int,
        request: PKCERequest,
        browser_secret: str,
        issuer: str | None,
    ) -> OAuthBrowserView:
        digest, now = callback_hash(request.state), time.time()
        async with self._transaction() as session:
            source = (
                await session.execute(
                    update(OAuthStateRow)
                    .where(
                        OAuthStateRow.state_hash == digest,
                        OAuthStateRow.subject == subject,
                        OAuthStateRow.status == "pending",
                        OAuthStateRow.expires_at > now,
                    )
                    .values(ciphertext=OAuthStateRow.ciphertext)
                    .returning(OAuthStateRow.expires_at)
                )
            ).first()
            if source is None:
                raise ServiceError("oauth_browser_state_missing", "原持久OAuth state不可用", 409)
            await session.execute(
                delete(OAuthBrowserCallbackRow).where(
                    OAuthBrowserCallbackRow.expires_at <= now,
                    OAuthBrowserCallbackRow.phase != "exchanging",
                )
            )
            count = await session.scalar(select(func.count()).select_from(OAuthBrowserCallbackRow))
            if (count or 0) >= 256:
                raise ServiceError("oauth_browser_limit", "OAuth授权记录达到上限", 429)
            attempt_id, browser_hash = (
                str(uuid4()),
                hashlib.sha256(browser_secret.encode()).hexdigest(),
            )
            intent = _Intent(
                version=1,
                attempt_id=attempt_id,
                scope=scope,
                name=name,
                subject=subject,
                resource_hash=resource_hash,
                profile_revision=profile_revision,
                state=request.state,
                state_hash=digest,
                browser_hash=browser_hash,
                authorization_url=request.authorization_url,
                issuer=issuer,
                expires_at=source.expires_at,
            )
            row = OAuthBrowserCallbackRow(
                attempt_id=attempt_id,
                state_hash=digest,
                scope=scope,
                name=name,
                subject=subject,
                resource_hash=resource_hash,
                browser_hash=browser_hash,
                ciphertext=self._cipher.encrypt(intent.model_dump_json().encode()),
                phase="pending",
                owner_id=None,
                error_code=None,
                created_at=now,
                expires_at=source.expires_at,
                updated_at=utc_now(),
            )
            session.add(row)
            await session.flush()
            return self._view(row)

    async def view(self, scope: str, name: str, attempt_id: str) -> OAuthBrowserView:
        async with self._transaction() as session:
            row = await session.get(OAuthBrowserCallbackRow, attempt_id)
            if row is None or row.scope != scope or row.name != name:
                raise ServiceError("oauth_browser_missing", "OAuth授权记录不存在", 404)
            return self._view(row)

    async def list(self, scope: str, name: str) -> list[OAuthBrowserView]:
        async with self._transaction() as session:
            rows = await session.scalars(
                select(OAuthBrowserCallbackRow)
                .where(
                    OAuthBrowserCallbackRow.scope == scope,
                    OAuthBrowserCallbackRow.name == name,
                )
                .order_by(OAuthBrowserCallbackRow.created_at.desc())
                .limit(32)
            )
            return [self._view(row) for row in rows]

    async def lookup(self, state: str) -> tuple[str, _Intent]:
        async with self._transaction() as session:
            row = await session.scalar(
                select(OAuthBrowserCallbackRow).where(
                    OAuthBrowserCallbackRow.state_hash == callback_hash(state)
                )
            )
            if row is None:
                raise ServiceError("oauth_browser_missing", "OAuth授权state不存在", 400)
            return row.attempt_id, self._decode(row)

    async def claim(self, state: str, browser_secret: str) -> tuple[str, _Intent]:
        if re.fullmatch(r"[A-Za-z0-9_-]{43}", browser_secret) is None:
            raise ServiceError("oauth_browser_binding", "OAuth回程不属于发起浏览器", 403)
        digest, browser_hash, owner = (
            callback_hash(state),
            hashlib.sha256(browser_secret.encode()).hexdigest(),
            str(uuid4()),
        )
        async with self._transaction() as session:
            row = await session.scalar(
                update(OAuthBrowserCallbackRow)
                .where(
                    OAuthBrowserCallbackRow.state_hash == digest,
                    OAuthBrowserCallbackRow.browser_hash == browser_hash,
                    OAuthBrowserCallbackRow.phase == "pending",
                    OAuthBrowserCallbackRow.expires_at > time.time(),
                )
                .values(phase="exchanging", owner_id=owner, updated_at=utc_now())
                .returning(OAuthBrowserCallbackRow)
            )
            if row is None:
                raise ServiceError(
                    "oauth_browser_binding", "OAuth回程未知、已结束、过期或浏览器不匹配", 403
                )
            return owner, self._decode(row)

    async def finish(
        self, intent: _Intent, owner: str, *, error_code: str | None = None
    ) -> OAuthBrowserView:
        async with self._transaction() as session:
            row = await session.scalar(
                update(OAuthBrowserCallbackRow)
                .where(
                    OAuthBrowserCallbackRow.attempt_id == intent.attempt_id,
                    OAuthBrowserCallbackRow.owner_id == owner,
                    OAuthBrowserCallbackRow.phase == "exchanging",
                )
                .values(
                    phase="failed" if error_code else "complete",
                    ciphertext=None,
                    owner_id=None,
                    error_code=error_code,
                    updated_at=utc_now(),
                )
                .returning(OAuthBrowserCallbackRow)
            )
            if row is None:
                raise ServiceError("oauth_browser_conflict", "OAuth授权记录已变化", 409)
            return self._view(row)

    async def cancel(self, scope: str, name: str, attempt_id: str) -> _Intent:
        async with self._transaction() as session:
            row = await session.scalar(
                update(OAuthBrowserCallbackRow)
                .where(
                    OAuthBrowserCallbackRow.attempt_id == attempt_id,
                    OAuthBrowserCallbackRow.scope == scope,
                    OAuthBrowserCallbackRow.name == name,
                    OAuthBrowserCallbackRow.phase == "pending",
                )
                .values(phase="pending")
                .returning(OAuthBrowserCallbackRow)
            )
            if row is None:
                raise ServiceError(
                    "oauth_browser_not_pending", "OAuth授权已结束或交换中，不能取消", 409
                )
            intent = self._decode(row)
            row.phase, row.ciphertext, row.owner_id, row.updated_at = (
                "cancelled",
                None,
                None,
                utc_now(),
            )
            return intent

    async def discard_state(self, intent: _Intent) -> None:
        async with self._transaction() as session:
            await session.execute(
                update(OAuthStateRow)
                .where(
                    OAuthStateRow.state_hash == intent.state_hash,
                    OAuthStateRow.subject == intent.subject,
                    OAuthStateRow.status == "pending",
                )
                .values(status="cancelled", ciphertext=None, ended_at=time.time())
            )


class OAuthBrowserService:
    def __init__(self, manager: MCPManager, key: bytes | str) -> None:
        self.manager = manager
        self.store = PersistentOAuthBrowserCallbackStore(manager.store.database, key)
        self._closed = False
        self._operations: set[asyncio.Task[object]] = set()
        self._close_task: asyncio.Task[None] | None = None

    def _live(self) -> None:
        if self._closed:
            raise ServiceError("oauth_browser_closed", "OAuth浏览器授权服务已关闭", 503)

    async def close(self) -> None:
        if self._close_task is None:
            self._closed = True
            self._close_task = asyncio.create_task(self._close())
        await _finish_cleanup(self._close_task)

    async def _close(self) -> None:
        for operation in list(self._operations):
            operation.cancel()
        await asyncio.gather(*list(self._operations), return_exceptions=True)

    def _auth(self, name: str) -> MCPOAuthAuth:
        resource = self.manager._resources.get(name)
        if resource is None or not isinstance(resource[0].auth, MCPOAuthAuth):
            raise ServiceError("oauth_browser_unavailable", "该MCP资源未登记OAuth浏览器能力", 503)
        auth = resource[0].auth
        if not isinstance(auth.states, PersistentEncryptedStateStore) or not isinstance(
            auth.tokens, PersistentEncryptedTokenStore
        ):
            raise ServiceError(
                "oauth_browser_unavailable", "OAuth浏览器需要持久state/token存储", 503
            )
        return auth

    async def begin(
        self, name: str, request: MCPControlRequest, origin: str
    ) -> tuple[OAuthBrowserView, str]:
        self._live()
        self.manager._live()
        operation = asyncio.create_task(self._begin(name, request, origin))
        self._operations.add(operation)
        operation.add_done_callback(self._operations.discard)
        return await _finish_cleanup(operation)

    async def _begin(
        self, name: str, request: MCPControlRequest, origin: str
    ) -> tuple[OAuthBrowserView, str]:
        await self.manager._check(name, request)
        auth = self._auth(name)
        if auth.redirect_uri != origin.rstrip("/") + CALLBACK_PATH:
            raise ServiceError(
                "oauth_browser_redirect", "OAuth redirect与本服务固定回调不一致", 409
            )
        config = await auth.authorization_config()
        attempt = await auth.client.begin_authorization(config, auth.subject)
        secret = secrets.token_urlsafe(32)
        try:
            view = await self.store.prepare(
                scope=self.manager.store.scope,
                name=name,
                subject=auth.subject,
                resource_hash=request.resource_hash,
                profile_revision=request.expected_revision,
                request=attempt,
                browser_secret=secret,
                issuer=config.issuer,
            )
            return view, secret
        except BaseException:
            await auth.client.discard_authorization(attempt.state, auth.subject)
            raise

    async def cancel(self, name: str, attempt_id: str) -> OAuthBrowserView:
        intent = await self.store.cancel(self.manager.store.scope, name, attempt_id)
        await _finish_cleanup(asyncio.create_task(self.store.discard_state(intent)))
        return await self.store.view(self.manager.store.scope, name, attempt_id)

    async def callback(
        self,
        state: str,
        code: str | None,
        error: str | None,
        cookies: dict[str, str],
        issuer: str | None = None,
    ) -> OAuthBrowserView:
        self._live()
        self.manager._live()
        operation = asyncio.create_task(self._callback(state, code, error, cookies, issuer))
        self._operations.add(operation)
        operation.add_done_callback(self._operations.discard)
        return await _finish_cleanup(operation)

    async def _callback(
        self,
        state: str,
        code: str | None,
        error: str | None,
        cookies: dict[str, str],
        issuer: str | None,
    ) -> OAuthBrowserView:
        if (code is None) == (error is None) or (
            code is not None and (not code or len(code) > 8192)
        ):
            raise ServiceError("oauth_callback_invalid", "OAuth回程必须有有效code或error", 400)
        attempt_id, hinted = await self.store.lookup(state)
        auth = self._auth(hinted.name)
        owner, intent = await self.store.claim(state, cookies.get(cookie_name(attempt_id), ""))
        failure: str | None = None
        try:
            if intent.scope != self.manager.store.scope or auth.subject != intent.subject:
                raise ServiceError("oauth_browser_binding", "OAuth授权主体或scope不一致", 403)
            await self.manager._check(
                intent.name,
                MCPControlRequest(
                    expected_revision=intent.profile_revision, resource_hash=intent.resource_hash
                ),
            )
            if issuer is not None and issuer.rstrip("/") != (intent.issuer or "").rstrip("/"):
                raise ServiceError("oauth_browser_issuer", "OAuth回程issuer不一致", 403)
            if error is not None:
                failure = "oauth_authorization_denied"
            else:
                assert code is not None
                await auth.client.exchange(intent.state, code, intent.subject, auth.requester)
        except asyncio.CancelledError:
            failure = "oauth_authorization_interrupted"
            raise
        except (ServiceError, OAuthError) as caught:
            failure = caught.code
        except Exception:
            failure = "oauth_authorization_failed"
        except BaseException:
            failure = "oauth_authorization_interrupted"
            raise
        finally:

            async def settle() -> None:
                try:
                    await self.store.discard_state(intent)
                finally:
                    await self.store.finish(intent, owner, error_code=failure)

            await _finish_cleanup(asyncio.create_task(settle()))
        return await self.store.view(self.manager.store.scope, intent.name, intent.attempt_id)
