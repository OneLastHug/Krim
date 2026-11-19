"""Reuse the account vault with project-specific authenticated context."""

from __future__ import annotations

from dataclasses import dataclass
from typing import NoReturn, Protocol, SupportsIndex

from krim_agent.account_mcp.contracts import SecretContext, SecretVault
from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.development.contracts import AuthorizationRef
from krim_agent.project_access.types import AccessIntent, SecretRecord


def project_secret_context(record: SecretRecord) -> SecretContext:
    # Length-safe domain separation binds owner/project/id even when ids contain
    # delimiters. This is an AAD encoding, not another encryption algorithm.
    connection_id = "project." + fingerprint(
        {
            "domain": "krim.project-secret.v1",
            "owner_id": record.owner_id,
            "project_id": record.project_id,
            "secret_id": record.secret_id,
        }
    )
    return SecretContext(record.owner_id, connection_id, record.revision, record.purpose)


def encrypt_secret(vault: SecretVault, record: SecretRecord, plaintext: bytes) -> SecretRecord:
    """Trusted provisioning only; persistence and revision CAS belong to I."""
    ciphertext = vault.encrypt(project_secret_context(record), plaintext)
    return record.model_copy(update={"ciphertext": ciphertext})


class SecretMaterial:
    """Only the constructor-injected trusted sink may consume the borrowed view.

    No JSON/pickle/repr plaintext. The view is invalidated by erasing its buffer
    after delivery, but Python/vault/consumer copies cannot be promised zeroized.
    Never pass this to a model-facing serializer, logger or generic Shell.
    """

    __slots__ = ("__buffer",)

    def __init__(self, plaintext: bytes) -> None:
        self.__buffer = bytearray(plaintext)

    def __repr__(self) -> str:
        return "SecretMaterial(<redacted>)"

    def __reduce_ex__(self, protocol: SupportsIndex) -> NoReturn:
        raise TypeError("secret material cannot be serialized")

    def borrow(self) -> memoryview:
        return memoryview(self.__buffer).toreadonly()

    def erase(self) -> None:
        self.__buffer[:] = b"\x00" * len(self.__buffer)


@dataclass(frozen=True, slots=True)
class DeliveryBinding:
    intent: AccessIntent
    authorization: AuthorizationRef
    secret_id: str
    revision: int


class SecretSink(Protocol):
    """A trusted synchronous acceptance point, wired by I, not chosen by tools.

    Accept only into a trusted bounded local queue/pipe, failing promptly when
    full. No uncontrolled network or long process waits inside the repository
    guard. Never persist/log plaintext or expose it in model-visible output.
    A delayed trusted consumer rechecks its binding before credential release;
    the synchronous callback alone does not serialize cross-process revocation.
    """

    def deliver(self, binding: DeliveryBinding, material: SecretMaterial) -> None: ...


class SecretDelivery(Protocol):
    """Encrypted pending delivery consumed by the guarded service only."""

    def release(self, binding: DeliveryBinding, sink: SecretSink) -> None: ...


def handoff(sink: SecretSink, binding: DeliveryBinding, plaintext: bytes) -> None:
    material = SecretMaterial(plaintext)
    del plaintext
    failed = False
    try:
        sink.deliver(binding, material)
    except Exception:
        failed = True
    finally:
        material.erase()
    # Raise outside the exception handler: do not retain the sink's exception,
    # which may contain plaintext in args or chained errors.
    if failed:
        raise ServiceError("execution_error", 500)
