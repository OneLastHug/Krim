"""可信宿主身份策略；不接受HTTP/dataset注册身份或凭据。"""

from __future__ import annotations

import os
import secrets
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, model_validator

from krim_agent.contracts import ServiceError, StrictModel


class Membership(StrictModel):
    workspace_id: str = Field(pattern=r"^[a-zA-Z0-9_.-]{1,64}$")
    role: Literal["viewer", "member", "admin"] = "member"
    approve: bool = True


class Credential(StrictModel):
    principal_id: str = Field(pattern=r"^[a-zA-Z0-9_.-]{1,64}$")
    token: SecretStr = Field(repr=False)
    generation: int = Field(default=1, ge=1)
    memberships: tuple[Membership, ...]
    legacy: bool = False


class AccessPolicy(StrictModel):
    credentials: tuple[Credential, ...]

    @model_validator(mode="after")
    def unique(self) -> AccessPolicy:
        if not 1 <= len(self.credentials) <= 64:
            raise ValueError("身份数量超限")
        ids = [c.principal_id for c in self.credentials]
        tokens = [c.token.get_secret_value() for c in self.credentials]
        if (
            len(set(ids)) != len(ids)
            or len(set(tokens)) != len(tokens)
            or any(not t or len(t) > 256 for t in tokens)
        ):
            raise ValueError("重复或无效身份")
        if sum(c.legacy for c in self.credentials) > 1:
            raise ValueError("仅允许一个legacy身份")
        if any(
            not c.memberships or len({m.workspace_id for m in c.memberships}) != len(c.memberships)
            for c in self.credentials
        ):
            raise ValueError("工作区无效")
        return self

    @classmethod
    def load(cls, path: Path) -> AccessPolicy:
        try:
            if any(p.is_symlink() for p in (path, *path.parents)):
                raise ValueError("链接策略")
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                info = os.fstat(fd)
                if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_size > 65536:
                    raise ValueError("策略文件权限/容量")
                raw = os.read(fd, 65537)
            finally:
                os.close(fd)
            return cls.model_validate_json(raw)
        except (OSError, ValueError):
            raise ServiceError("access_policy_invalid", 422) from None

    def authenticate(self, authorization: str) -> PrincipalContext:
        for item in self.credentials:
            if secrets.compare_digest(
                authorization.encode(), ("Bearer " + item.token.get_secret_value()).encode()
            ):
                return PrincipalContext(
                    item.principal_id, item.generation, item.memberships, item.legacy
                )
        raise ServiceError("unauthorized", 401)


@dataclass(frozen=True)
class PrincipalContext:
    principal_id: str
    generation: int
    memberships: tuple[Membership, ...]
    legacy: bool = False

    def member(self, workspace: str, *, write: bool = False, approve: bool = False) -> Membership:
        membership = next((m for m in self.memberships if m.workspace_id == workspace), None)
        if membership is None:
            raise ServiceError("not_found", 404)
        if (
            write
            and membership.role == "viewer"
            or approve
            and (not membership.approve or membership.role == "viewer")
        ):
            raise ServiceError("permission_denied", 403)
        return membership

    def owns(
        self, workspace: str, owner: str, *, write: bool = False, approve: bool = False
    ) -> None:
        membership = self.member(workspace, write=write, approve=approve)
        if self.principal_id != owner and membership.role != "admin":
            raise ServiceError("not_found", 404)


LOCAL = PrincipalContext("local", 1, (Membership(workspace_id="local", role="admin"),), True)
