"""首轮账号MCP公共契约；仅整合者写入，revision=1。"""

from dataclasses import dataclass
from typing import Protocol

CONTRACT_REVISION = 1


@dataclass(frozen=True)
class SecretContext:
    principal_id: str
    connection_id: str
    credential_revision: int
    purpose: str


class SecretVault(Protocol):
    def encrypt(self, context: SecretContext, plaintext: bytes) -> str: ...
    def decrypt(self, context: SecretContext, ciphertext: str) -> bytes: ...
