"""沙箱管理 DTO；配置值与已生效值分开。"""

import unicodedata

from pydantic import Field, field_validator

from krim_agent.contracts import StrictModel
from krim_agent.sandbox.quota import QuotaOverrides


class ConfigureQuotaBody(StrictModel):
    """PUT 全量替换：未指定 profile_name 使用全局默认，未指定 overrides 清空覆盖。"""

    expected_revision: int = Field(ge=0)
    profile_name: str | None = Field(default=None, min_length=1, max_length=64)
    overrides: QuotaOverrides = Field(default_factory=QuotaOverrides)


class CreateProjectBody(StrictModel):
    workspace_id: str = Field(min_length=1, max_length=128)
    title: str = Field(min_length=1, max_length=128)

    @field_validator("workspace_id", "title")
    @classmethod
    def plain_text(cls, value: str) -> str:
        result = value.strip()
        if not result or any(unicodedata.category(c).startswith("C") for c in result):
            raise ValueError("项目名称或工作区无效")
        return result


class BindProjectBody(StrictModel):
    expected_revision: int = Field(ge=0)
    expected_session_revision: int = Field(ge=0)
    project_id: str | None

    @field_validator("project_id")
    @classmethod
    def valid_project_id(cls, value: str | None) -> str | None:
        from krim_agent.sandbox.xfs import internal_id

        return internal_id(value) if value is not None else None
