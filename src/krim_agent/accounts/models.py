"""账号管理请求与视图模型；HTTP/CLI复用。body不含request_key，由keyword传入。"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import Field, GetCoreSchemaHandler, JsonValue, StringConstraints
from pydantic_core import core_schema

from krim_agent.contracts import StrictModel
from krim_agent.security import Membership


class OneTimeToken(str):
    """一次性密钥明文：JSON输出为原文，repr/日志为掩码。"""

    def __repr__(self) -> str:
        return "OneTimeToken('**********')"

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> Any:
        return core_schema.no_info_after_validator_function(cls, core_schema.str_schema())


PrincipalId = Annotated[str, StringConstraints(pattern=r"^[a-zA-Z0-9_.-]{1,64}$")]
RequestKey = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9_.:-]{1,128}$")]


class CreateAccountBody(StrictModel):
    display_name: str = Field(min_length=1, max_length=64)
    platform_role: Literal["member", "platform_admin"] = "member"
    memberships: tuple[Membership, ...] = ()


class UpdateAccountBody(StrictModel):
    display_name: str | None = Field(default=None, min_length=1, max_length=64)
    status: Literal["active", "disabled"] | None = None
    platform_role: Literal["member", "platform_admin"] | None = None
    expected_revision: int | None = Field(default=None, ge=1)


class IssueKeyBody(StrictModel):
    pass


class RotateKeyBody(StrictModel):
    pass


class RevokeKeyBody(StrictModel):
    pass


class SetMembershipsBody(StrictModel):
    memberships: tuple[Membership, ...]
    expected_generation: int | None = Field(default=None, ge=1)


class KeyView(StrictModel):
    key_id: str
    masked: str
    credential_version: int
    status: Literal["active", "revoked"]
    weak: bool = False
    issued_at: float
    revoked_at: float | None = None
    last_used_at: float | None = None


class AccountView(StrictModel):
    principal_id: str
    display_name: str
    status: Literal["active", "disabled"]
    platform_role: Literal["member", "platform_admin"]
    generation: int
    revision: int
    legacy: bool
    created_at: float
    updated_at: float
    has_active_key: bool = False
    key: KeyView | None = None


class WorkspaceRef(StrictModel):
    workspace_id: str
    display_name: str
    role: Literal["viewer", "member", "admin"]
    approve: bool
    personal: bool = False


class ProfileView(StrictModel):
    principal_id: str
    display_name: str
    status: Literal["active", "disabled"]
    platform_role: Literal["member", "platform_admin"]
    generation: int
    revision: int
    legacy: bool
    key: KeyView | None = None
    workspaces: tuple[WorkspaceRef, ...] = ()


class IssuedKey(StrictModel):
    """签发/轮换/恢复结果；token仅首发响应一次，重放为None。"""

    principal_id: str
    key: KeyView
    token: OneTimeToken | None = None
    replayed: bool = False
    revoked_key_id: str | None = None


class AccountCreated(StrictModel):
    account: AccountView
    replayed: bool = False


class AccountUpdated(StrictModel):
    account: AccountView
    replayed: bool = False


class KeyRevoked(StrictModel):
    principal_id: str
    key: KeyView
    generation: int
    replayed: bool = False


class MembershipsSet(StrictModel):
    principal_id: str
    memberships: tuple[Membership, ...]
    generation: int
    replayed: bool = False


class EventView(StrictModel):
    id: str
    action: str
    actor: str
    target: str | None = None
    request_key: str
    result: dict[str, JsonValue]
    at: float


class EventsPage(StrictModel):
    items: tuple[EventView, ...]
    offset: int = Field(ge=0)
    total: int = Field(ge=0)


class AccountsPage(StrictModel):
    items: tuple[AccountView, ...]
    offset: int = Field(ge=0)
    total: int = Field(ge=0)


class ImportResult(StrictModel):
    accounts: tuple[AccountView, ...]
    admin_id: str
    source: str
    replayed: bool = False


class BootstrapResult(StrictModel):
    account: AccountView
    token: OneTimeToken | None = None
    replayed: bool = False
