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
    """分页读取；offset无上限（大文件按页读完整内容），limit为单页分页参数。"""

    path: str = Field(min_length=1, max_length=512)
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=16384, ge=1, le=16384)

    @field_validator("path")
    @classmethod
    def valid_path(cls, value: str) -> str:
        _parts(value)
        return value


class FileList(ProjectInput):
    """目录分页；offset无上限，目录列表无条目总数上限。"""

    path: str = Field(default=".", max_length=512)
    offset: int = Field(default=0, ge=0)
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
    offset: int = Field(default=0, ge=0)

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
    find: str = Field(min_length=1)
    replacement: str
    occurrence: int = Field(default=1, ge=1)

    @field_validator("path")
    @classmethod
    def valid_path(cls, value: str) -> str:
        _parts(value)
        return value

    @model_validator(mode="after")
    def valid_digest(self) -> Self:
        if not self.expected_sha256 or len(self.expected_sha256) != 64:
            raise ValueError("文件摘要无效")
        return self


class FileWrite(DirectoryCreate):
    """整体替换写入；应用层不设16KiB内容配额。

    单次调用的实际上限来自传输消息大小，更大内容走账号级暂存分块上传，
    由宿主经ProjectFiles.stream_write流式导入，不在模型参数中携带全文。
    """

    expected_sha256: Digest | None
    content: str


class FileRemove(DirectoryCreate):
    expected_sha256: Digest


class FilePatch(PatchRequest, ProjectInput):
    """审批参数包含完整精确补丁，Broker仍复核文件hash与项目generation。"""


class FileRename(RenameRequest, ProjectInput):
    """同项目单文件rename，不覆盖目标，不暴露宿主路径。"""


FileChange = FileWrite | DirectoryCreate | FileRemove | FilePatch | FileRename
