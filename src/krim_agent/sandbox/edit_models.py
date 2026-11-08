"""A3有界文本补丁与显式rename的请求与计划模型；复用StrictModel与既有错误码。"""

from __future__ import annotations

from typing import Annotated, Literal, Self

from pydantic import Field, field_validator, model_validator

from krim_agent.contracts import StrictModel
from krim_agent.sandbox.filesystem import _parts

Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
PlanId = Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
MAX_PATCH_FILE_BYTES = 4 * 1024**2
MAX_REPLACEMENT_BYTES = 16 * 1024


def validate_edit_path(path: str) -> str:
    """相对安全路径：NFC、无../绝对路径/控制字符，拒绝.git与秘密条目。"""
    _parts(path)
    return path


class PatchRequest(StrictModel):
    """单段精确补丁；offset/delete_length为UTF-8字节偏移，必须落在码点边界上。"""

    path: str = Field(min_length=1, max_length=512)
    expected_sha256: Digest
    expected_generation: int = Field(ge=0)
    offset: int = Field(ge=0, le=MAX_PATCH_FILE_BYTES)
    delete_length: int = Field(ge=0, le=MAX_PATCH_FILE_BYTES)
    replacement: str = ""

    @field_validator("path")
    @classmethod
    def valid_path(cls, value: str) -> str:
        return validate_edit_path(value)

    @model_validator(mode="after")
    def bounded_replacement(self) -> Self:
        try:
            encoded = self.replacement.encode("utf-8")
        except UnicodeEncodeError:
            raise ValueError("replacement必须为有效UTF-8文本") from None
        if len(encoded) > MAX_REPLACEMENT_BYTES:
            raise ValueError("replacement超过16KiB限制")
        return self


class RenameRequest(StrictModel):
    """显式rename请求；目标默认必须不存在，源必须是可读取的普通文件。"""

    source: str = Field(min_length=1, max_length=512)
    destination: str = Field(min_length=1, max_length=512)
    expected_source_sha256: Digest
    expected_generation: int = Field(ge=0)

    @field_validator("source", "destination")
    @classmethod
    def valid_path(cls, value: str) -> str:
        return validate_edit_path(value)


class _EditPlan(StrictModel):
    plan_id: PlanId
    principal_id: str = Field(min_length=1)
    environment_id: str = Field(min_length=1)
    project_id: str = Field(min_length=1)
    generation: int = Field(ge=0)


class PatchPlan(_EditPlan):
    """补丁执行计划：携带digest前置条件与目标hash，apply时逐项复核。"""

    kind: Literal["patch"] = "patch"
    path: str = Field(min_length=1, max_length=512)
    source_sha256: Digest
    result_sha256: Digest
    offset: int = Field(ge=0)
    delete_length: int = Field(ge=0)
    replacement_bytes: bytes

    @field_validator("path")
    @classmethod
    def valid_path(cls, value: str) -> str:
        return validate_edit_path(value)

    @model_validator(mode="after")
    def valid_replacement(self) -> Self:
        try:
            self.replacement_bytes.decode("utf-8")
        except UnicodeDecodeError:
            raise ValueError("replacement_bytes必须为有效UTF-8") from None
        if len(self.replacement_bytes) > MAX_REPLACEMENT_BYTES:
            raise ValueError("replacement超过16KiB限制")
        return self


class RenamePlan(_EditPlan):
    """rename执行计划：仅支持原子rename；不提供copy+delete伪装路径。"""

    kind: Literal["rename"] = "rename"
    source: str = Field(min_length=1, max_length=512)
    destination: str = Field(min_length=1, max_length=512)
    source_sha256: Digest
    destination_preexists: Literal[False] = False
    mode: Literal["atomic_rename"] = "atomic_rename"

    @field_validator("source", "destination")
    @classmethod
    def valid_path(cls, value: str) -> str:
        return validate_edit_path(value)
