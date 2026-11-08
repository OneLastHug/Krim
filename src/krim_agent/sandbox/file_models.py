"""sandbox/1文件工具；读无副作用，写单独审批并绑定项目generation。"""

from typing import Annotated, Self

from pydantic import Field, field_validator, model_validator

from krim_agent.contracts import StrictModel
from krim_agent.sandbox.edit_models import PatchRequest, RenameRequest
from krim_agent.sandbox.filesystem import _parts
from krim_agent.sandbox.xfs import internal_id

Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class ProjectInput(StrictModel):
    project_id: str | None = None

    @field_validator("project_id")
    @classmethod
    def valid_project(cls, value: str | None) -> str | None:
        return internal_id(value) if value is not None else None


class FileRead(ProjectInput):
    path: str = Field(min_length=1, max_length=512)
    offset: int = Field(default=0, ge=0, le=4 * 1024**2)
    limit: int = Field(default=16384, ge=1, le=16384)

    @field_validator("path")
    @classmethod
    def valid_path(cls, value: str) -> str:
        _parts(value)
        return value


class FileList(ProjectInput):
    path: str = Field(default=".", max_length=512)
    offset: int = Field(default=0, ge=0, le=4096)
    limit: int = Field(default=50, ge=1, le=100)

    @field_validator("path")
    @classmethod
    def valid_path(cls, value: str) -> str:
        _parts(value, directory=True)
        return value


class FileSearch(FileList):
    query: str = Field(min_length=1, max_length=256)

    @model_validator(mode="after")
    def valid_query(self) -> Self:
        if not self.query.strip() or len(self.query.encode()) > 256 or self.offset:
            raise ValueError("搜索内容无效")
        return self


class DirectoryCreate(ProjectInput):
    expected_generation: int = Field(ge=0)
    path: str = Field(min_length=1, max_length=512)

    @field_validator("path")
    @classmethod
    def valid_path(cls, value: str) -> str:
        _parts(value)
        return value


class FileWrite(DirectoryCreate):
    expected_sha256: Digest | None
    content: str = Field(max_length=16384)

    @model_validator(mode="after")
    def bounded_content(self) -> Self:
        if len(self.content.encode()) > 16384:
            raise ValueError("写入内容过长")
        return self


class FileRemove(DirectoryCreate):
    expected_sha256: Digest


class FilePatch(PatchRequest, ProjectInput):
    """审批参数包含完整精确补丁，Broker仍复核文件hash与项目generation。"""


class FileRename(RenameRequest, ProjectInput):
    """同项目单文件rename，不覆盖目标，不暴露宿主路径。"""


FileChange = FileWrite | DirectoryCreate | FileRemove | FilePatch | FileRename
