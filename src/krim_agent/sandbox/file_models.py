"""sandbox/1文件工具；读无副作用，写单独审批并绑定项目generation。"""

from typing import Annotated, Literal, Self

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


class FileFind(ProjectInput):
    path: str = Field(default=".", max_length=512)
    include_glob: str = Field(min_length=1, max_length=256)
    limit: int = Field(default=100, ge=1, le=100)
    offset: int = Field(default=0, ge=0, le=4095)

    @field_validator("path")
    @classmethod
    def valid_path(cls, value: str) -> str:
        _parts(value, directory=True)
        return value

    @field_validator("include_glob")
    @classmethod
    def valid_glob(cls, value: str) -> str:
        if not any(char in value for char in "*?["):
            raise ValueError("文件模式需要包含glob通配符")
        return value

    @model_validator(mode="after")
    def valid_page(self) -> Self:
        if self.offset + self.limit > 4096:
            raise ValueError("文件分页超出上限")
        return self


class FileSearch(FileList):
    query: str = Field(min_length=1, max_length=256)
    include_glob: str | None = Field(default=None, max_length=256)
    mode: Literal["literal", "regex"] = "literal"
    case_sensitive: bool = True
    before: int = Field(default=0, ge=0, le=3)
    after: int = Field(default=0, ge=0, le=3)

    @model_validator(mode="after")
    def valid_query(self) -> Self:
        if not self.query.strip() or len(self.query.encode()) > 256:
            raise ValueError("搜索内容无效")
        if self.include_glob is not None and len(self.include_glob.encode()) > 256:
            raise ValueError("文件模式无效")
        if self.include_glob is not None and not any(char in self.include_glob for char in "*?["):
            raise ValueError("文件模式需要包含glob通配符")
        if self.offset + self.limit > 4096:
            raise ValueError("搜索分页超出上限")
        if self.mode == "regex" and len(self.query.encode()) > 64:
            raise ValueError("正则表达式过长")
        return self


class DirectoryCreate(ProjectInput):
    expected_generation: int = Field(ge=0)
    path: str = Field(min_length=1, max_length=512)

    @field_validator("path")
    @classmethod
    def valid_path(cls, value: str) -> str:
        _parts(value)
        return value


class FileReplacePreview(ProjectInput):
    path: str = Field(min_length=1, max_length=512)
    expected_generation: int = Field(ge=0)
    expected_sha256: Digest
    find: str = Field(min_length=1, max_length=4096)
    replacement: str = Field(max_length=16384)
    occurrence: int = Field(default=1, ge=1, le=4096)

    @field_validator("path")
    @classmethod
    def valid_path(cls, value: str) -> str:
        _parts(value)
        return value

    @model_validator(mode="after")
    def bounded_text(self) -> Self:
        if len(self.find.encode()) > 4096 or len(self.replacement.encode()) > 16384:
            raise ValueError("替换文本过长")
        if not self.expected_sha256 or len(self.expected_sha256) != 64:
            raise ValueError("文件摘要无效")
        return self


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
