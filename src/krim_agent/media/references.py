"""Host-owned media identities; model inputs never select an owner or host path."""

from __future__ import annotations

import hashlib
import hmac
import re
from dataclasses import dataclass
from typing import Self

from pydantic import Field, field_validator, model_validator

from krim_agent.contracts import ServiceError, StrictModel
from krim_agent.tools.session import SessionToolScope

_REFERENCE = re.compile(r"[A-Za-z0-9_-][A-Za-z0-9_.-]{0,255}\Z")


def relative_image_path(value: str) -> str:
    # Do not normalize away traversal, reinterpret backslashes, or decode URLs.
    if (not value or value.startswith(("/", "~")) or "\\" in value or ":" in value
            or any(ord(c) < 32 or ord(c) == 127 for c in value)
            or any(part in {"", ".", ".."} for part in value.split("/"))):
        raise ValueError("image path must be project-relative")
    return value


def opaque_reference(value: str) -> str:
    if _REFERENCE.fullmatch(value) is None or value in {".", ".."}:
        raise ValueError("image reference must be an opaque artifact identifier")
    return value


class MediaSource(StrictModel):
    path: str | None = None
    reference: str | None = None

    @field_validator("path", "reference")
    @classmethod
    def nonempty(cls, value: str | None) -> str | None:
        if value == "":
            raise ValueError("empty image source")
        return value

    @model_validator(mode="after")
    def valid_source(self) -> Self:
        if (self.path is None) == (self.reference is None):
            raise ValueError("provide exactly one of path or reference")
        if self.path is not None:
            relative_image_path(self.path)
        if self.reference is not None:
            opaque_reference(self.reference)
        return self


class AuthorizedMediaRef(StrictModel):
    """Durable immutable original, issued by the host read/artifact port.

    This record is descriptive, not a bearer capability. Every dereference must
    authorize the current actor, session, workspace and (when set) project again.
    """

    reference: str
    owner_id: str = Field(min_length=1)
    workspace_id: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    project_id: str | None = None
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size: int = Field(ge=1)
    media_type: str
    path: str | None = None

    @model_validator(mode="after")
    def valid_reference(self) -> Self:
        opaque_reference(self.reference)
        if self.path is not None:
            relative_image_path(self.path)
            if self.project_id is None:
                raise ValueError("project path requires project identity")
        return self

    def check_scope(self, scope: SessionToolScope) -> None:
        scope.check()
        scope.actor.member(scope.workspace_id)
        if (self.owner_id != scope.actor.principal_id
                or self.workspace_id != scope.workspace_id
                or self.session_id != scope.session_id
                or self.project_id is not None and self.project_id != scope.project_id):
            raise ServiceError("permission_denied", 403)


@dataclass(frozen=True, slots=True)
class AuthorizedMedia:
    reference: AuthorizedMediaRef
    data: bytes

    def validate(self, scope: SessionToolScope) -> None:
        self.reference.check_scope(scope)
        if (type(self.data) is not bytes or len(self.data) != self.reference.size
                or not hmac.compare_digest(
                    hashlib.sha256(self.data).hexdigest(), self.reference.sha256
                )):
            raise ServiceError("source_changed", 409)
