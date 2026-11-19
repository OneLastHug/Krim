"""Bounded, encrypted handoff storage for project secret delivery.

The queue is an in-process bridge only.  It encrypts a borrowed secret before
retaining it, keeps ciphertext and binding metadata instead of plaintext, and
requires the exact binding again at release time.
"""

from __future__ import annotations

import math
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final

from krim_agent.account_mcp.contracts import SecretContext, SecretVault
from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.development.contracts import AuthorizationRef
from krim_agent.project_access.secrets import (
    DeliveryBinding,
    SecretMaterial,
    SecretSink,
    handoff,
)
from krim_agent.project_access.types import AccessIntent

_DELIVERY_DOMAIN: Final = "krim.project-delivery.v1"
_DELIVERY_PURPOSE: Final = "project.delivery"
_REVISION_LIMIT: Final = 2**31 - 1


@dataclass(frozen=True, slots=True)
class _QueuedSecret:
    ciphertext: str
    binding: DeliveryBinding
    expires_at: float
    charged_bytes: int


def _error(code: str, status: int) -> None:
    raise ServiceError(code, status) from None


def _external_error(exc: Exception) -> tuple[str, int]:
    """Convert an injected adapter exception without retaining its details."""
    if isinstance(exc, ServiceError):
        code = exc.code
        status = exc.status
        allowed = {
            ("invalid_input", 422),
            ("permission_denied", 403),
            ("closed", 409),
            ("capacity", 429),
            ("input_limit", 422),
            ("execution_error", 500),
        }
        if type(code) is str and type(status) is int and (code, status) in allowed:
            return code, status
    return "execution_error", 500


def _binding_context(binding: DeliveryBinding) -> tuple[tuple[str, str, str, str], SecretContext]:
    """Return the queue key and an AAD context for a validated binding."""
    if type(binding) is not DeliveryBinding:
        _error("invalid_input", 422)

    intent = binding.intent
    authorization = binding.authorization
    if not isinstance(intent, AccessIntent) or not isinstance(authorization, AuthorizationRef):
        _error("invalid_input", 422)
    if type(binding.secret_id) is not str or not binding.secret_id:
        _error("invalid_input", 422)
    if type(binding.revision) is not int or not 1 <= binding.revision <= _REVISION_LIMIT:
        _error("invalid_input", 422)

    # SecretContext has a small fixed shape.  Encode the complete delivery
    # binding into its connection field so AESGCM AAD covers every project,
    # operation, authorization, secret, and revision component without using a
    # delimiter-based encoding.
    scope = {
        "domain": _DELIVERY_DOMAIN,
        "owner_id": intent.owner_id,
        "workspace_id": intent.workspace_id,
        "project_id": intent.project_id,
        "operation_id": intent.operation_id,
        "action": intent.action,
        "authorization": {
            "actor_id": authorization.actor_id,
            "actor_generation": authorization.actor_generation,
            "project_id": authorization.project_id,
            "approval_id": authorization.approval_id,
            "arguments_digest": authorization.arguments_digest,
            "secret_refs": list(authorization.secret_refs),
            "network_policy_ref": authorization.network_policy_ref,
        },
        "secret_id": binding.secret_id,
        "revision": binding.revision,
    }
    try:
        connection_id = "project.delivery." + fingerprint(scope)
    except Exception:
        _error("invalid_input", 422)
    return (
        (intent.owner_id, intent.project_id, intent.operation_id, binding.secret_id),
        SecretContext(
            principal_id=intent.owner_id,
            connection_id=connection_id,
            credential_revision=binding.revision,
            purpose=_DELIVERY_PURPOSE,
        ),
    )


def _clock_value(clock: Callable[[], float]) -> float:
    try:
        value = clock()
        numeric = float(value)
    except (OverflowError, TypeError, ValueError):
        _error("execution_error", 500)
    except Exception:
        _error("execution_error", 500)
    if type(value) not in (int, float) or not math.isfinite(numeric):
        _error("execution_error", 500)
    return numeric


class SealedDeliveryQueue(SecretSink):
    """A bounded one-shot encrypted SecretSink.

    ``deliver`` encrypts the borrowed material and retains only its ciphertext,
    binding, expiry, and byte accounting.  One outstanding item is allowed for
    each ``(owner, project, operation, secret_id)`` key.  ``release`` removes a
    matching item before decrypting or invoking the external sink, so every
    release attempt is one-shot and a failed handoff cannot be replayed.

    The caller of ``release`` is the trusted ``ProjectAccessService``.  It must
    call this queue while holding the repository guard and after rechecking the
    current authorization and secret revision.  This queue verifies the exact
    binding that was queued and authenticates its ciphertext, but it is not a
    substitute for that current authorization check or cross-process
    revocation.  No queue lock is held while the vault or sink is called.
    """

    def __init__(
        self,
        vault: SecretVault,
        *,
        capacity: int = 16,
        max_bytes: int = 16384,
        ttl: float = 10,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if type(capacity) is not int or capacity <= 0:
            _error("invalid_input", 422)
        if type(max_bytes) is not int or max_bytes <= 0:
            _error("invalid_input", 422)
        if type(ttl) not in (int, float):
            _error("invalid_input", 422)
        try:
            ttl_value = float(ttl)
        except (OverflowError, ValueError):
            _error("invalid_input", 422)
        if not math.isfinite(ttl_value) or ttl_value <= 0:
            _error("invalid_input", 422)
        if not callable(clock):
            _error("invalid_input", 422)
        try:
            encrypt = vault.encrypt
            decrypt = vault.decrypt
        except Exception:
            _error("invalid_input", 422)
        if not callable(encrypt) or not callable(decrypt):
            _error("invalid_input", 422)

        self._vault = vault
        self._capacity = capacity
        self._max_bytes = max_bytes
        self._ttl = ttl_value
        self._clock = clock
        self._lock = threading.Lock()
        self._entries: dict[tuple[str, str, str, str], _QueuedSecret] = {}
        self._bytes = 0
        self._closed = False

    def __repr__(self) -> str:
        with self._lock:
            return (
                f"{type(self).__name__}(closed={self._closed}, "
                f"entries={len(self._entries)}, bytes={self._bytes})"
            )

    def _evict_expired(self, now: float) -> None:
        expired = [key for key, entry in self._entries.items() if entry.expires_at <= now]
        for key in expired:
            entry = self._entries.pop(key)
            self._bytes -= entry.charged_bytes

    def deliver(self, binding: DeliveryBinding, material: SecretMaterial) -> None:
        """Encrypt and queue one borrowed secret synchronously.

        The input material is erased on every path.  Only ciphertext and the
        immutable binding are retained; plaintext is never serialized, logged,
        represented, or included in an exception.
        """
        error: tuple[str, int] | None = None
        plaintext: bytes | None = None
        ciphertext: str | None = None
        key: tuple[str, str, str, str] | None = None
        context: SecretContext | None = None
        try:
            key, context = _binding_context(binding)
            # Reject closed, duplicate, and already-full queues before calling
            # the injected vault.  The final reservation check below handles
            # races with another producer while encryption is in progress.
            with self._lock:
                if self._closed:
                    error = ("closed", 409)
                else:
                    now = _clock_value(self._clock)
                    self._evict_expired(now)
                    if key in self._entries:
                        error = ("permission_denied", 403)
                    elif len(self._entries) >= self._capacity:
                        error = ("capacity", 429)

            if error is None:
                try:
                    borrowed = material.borrow()
                    borrowed_bytes = (
                        borrowed.nbytes if isinstance(borrowed, memoryview) else len(borrowed)
                    )
                    if borrowed_bytes > self._max_bytes:
                        error = ("input_limit", 422)
                    else:
                        plaintext = bytes(borrowed)
                    del borrowed
                except Exception as exc:
                    error = _external_error(exc)

            if error is None:
                assert plaintext is not None
                if len(plaintext) > self._max_bytes:
                    error = ("input_limit", 422)
                else:
                    try:
                        ciphertext = self._vault.encrypt(context, plaintext)
                    except Exception as exc:
                        error = _external_error(exc)
                    if error is None and type(ciphertext) is not str:
                        error = ("execution_error", 500)

            ciphertext_bytes = 0
            if error is None:
                assert plaintext is not None and ciphertext is not None
                try:
                    ciphertext_bytes = len(ciphertext.encode("utf-8"))
                except Exception as exc:
                    error = _external_error(exc)
                if error is None and ciphertext_bytes > self._max_bytes:
                    error = ("input_limit", 422)
                if error is None and len(plaintext) + ciphertext_bytes > self._max_bytes:
                    error = ("input_limit", 422)

            if error is None:
                assert key is not None and ciphertext is not None and plaintext is not None
                accepted_at = _clock_value(self._clock)
                expires_at = accepted_at + self._ttl
                if not math.isfinite(expires_at):
                    error = ("execution_error", 500)
                else:
                    with self._lock:
                        self._evict_expired(accepted_at)
                        if self._closed:
                            error = ("closed", 409)
                        elif key in self._entries:
                            error = ("permission_denied", 403)
                        else:
                            charged_bytes = len(plaintext) + ciphertext_bytes
                            if len(self._entries) >= self._capacity:
                                error = ("capacity", 429)
                            elif self._bytes + charged_bytes > self._max_bytes:
                                error = ("capacity", 429)
                            else:
                                self._entries[key] = _QueuedSecret(
                                    ciphertext=ciphertext,
                                    binding=binding,
                                    expires_at=expires_at,
                                    charged_bytes=charged_bytes,
                                )
                                self._bytes += charged_bytes
        except Exception as exc:
            if error is None:
                error = _external_error(exc)
        finally:
            try:
                material.erase()
            except Exception as exc:
                if error is None:
                    error = _external_error(exc)
            del plaintext

        if error is not None:
            raise ServiceError(*error) from None

    def release(self, binding: DeliveryBinding, sink: SecretSink) -> None:
        """Consume and hand off exactly one queued binding.

        Matching is performed before decryption, and the entry is removed
        before either decryption or the external sink.  Consequently a vault
        failure, sink failure, or close during decryption cannot leave a
        replayable queue entry.  The trusted caller is still responsible for
        running this under its repository guard and rechecking live
        authorization and secret revision before calling us.
        """
        error: tuple[str, int] | None = None
        plaintext: bytes | None = None
        entry: _QueuedSecret | None = None
        try:
            key, context = _binding_context(binding)
            now = _clock_value(self._clock)
            with self._lock:
                self._evict_expired(now)
                if self._closed:
                    error = ("closed", 409)
                else:
                    candidate = self._entries.get(key)
                    if candidate is None or candidate.binding != binding:
                        error = ("permission_denied", 403)
                    else:
                        entry = self._entries.pop(key)
                        self._bytes -= entry.charged_bytes

            if error is None:
                assert entry is not None
                try:
                    plaintext = self._vault.decrypt(context, entry.ciphertext)
                except Exception as exc:
                    error = _external_error(exc)
                if error is None and type(plaintext) is not bytes:
                    error = ("execution_error", 500)

            if error is None:
                assert plaintext is not None and entry is not None
                # A close or expiry racing with decryption must still fail
                # closed.  The lock is released before handoff, so an external
                # sink cannot block queue operations.
                with self._lock:
                    if self._closed:
                        error = ("closed", 409)
                    else:
                        now = _clock_value(self._clock)
                        if now >= entry.expires_at:
                            error = ("permission_denied", 403)
                if error is None:
                    try:
                        handoff(sink, binding, plaintext)
                    except Exception as exc:
                        error = _external_error(exc)
        except Exception as exc:
            if error is None:
                error = _external_error(exc)
        finally:
            del plaintext

        if error is not None:
            raise ServiceError(*error) from None

    def discard(self, binding: DeliveryBinding) -> None:
        """Discard only this exact binding; never another operation's package."""
        key, _ = _binding_context(binding)
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None and entry.binding == binding:
                self._entries.pop(key)
                self._bytes -= entry.charged_bytes

    def revoke(self, owner_id: str, project_id: str) -> None:
        """Discard queued entries for exactly one owner/project pair."""
        if type(owner_id) is not str or not owner_id:
            _error("invalid_input", 422)
        if type(project_id) is not str or not project_id:
            _error("invalid_input", 422)
        with self._lock:
            matching = [key for key in self._entries if key[0] == owner_id and key[1] == project_id]
            for key in matching:
                entry = self._entries.pop(key)
                self._bytes -= entry.charged_bytes

    def close(self) -> None:
        """Discard every queued entry and permanently reject new operations."""
        with self._lock:
            self._closed = True
            self._entries.clear()
            self._bytes = 0
