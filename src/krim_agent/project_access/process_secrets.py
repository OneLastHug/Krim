"""D-owned exact command delivery binding and borrowed-material port."""
from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from krim_agent.contracts import ServiceError
from krim_agent.development.contracts import CommandRequest
from krim_agent.project_access.secrets import SecretMaterial

MAX_COMMAND_SECRETS = 8
MAX_SECRET_BYTES = 8192
MAX_COMMAND_BYTES = 16384


@dataclass(frozen=True, slots=True)
class CommandSecretBinding:
    """Non-bearer metadata; every use rechecks I's current fenced rows."""

    request: CommandRequest
    workspace_id: str
    process_id: str
    execution_id: str
    revisions: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        refs = self.request.authorization.secret_refs
        if (
            self.request.timeout_seconds is None
            or not 1 <= len(refs) <= MAX_COMMAND_SECRETS
            or len(set(refs)) != len(refs)
            or tuple(key for key, _ in self.revisions) != refs
            or any(type(revision) is not int or not 1 <= revision < 2**31
                   for _, revision in self.revisions)
            or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", self.workspace_id)
            or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", self.process_id)
            or not re.fullmatch(r"[0-9a-f]{64}", self.execution_id)
        ):
            raise ServiceError("permission_denied", 403)


class CommandSecretSink(Protocol):
    """Constructor-fixed trusted acceptance, short and nonblocking under guard.

    Atomically accept a bounded encrypted pending package, never a model-facing
    callback. Async transport must revalidate before release and have a watcher
    plus an independently enforced execution deadline. All views are erased by
    the service immediately after this call.
    """

    def deliver_command(
        self, binding: CommandSecretBinding, materials: tuple[SecretMaterial, ...], *,
        expires_at: float,
    ) -> None: ...

    def release_command(self, binding: CommandSecretBinding, check: Callable[[], None]) -> None:
        """Consume once and spawn/register bounded local transport under guard."""
        ...
