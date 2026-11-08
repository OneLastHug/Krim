"""首轮沙箱公共契约；可信调用方提供身份与静止项目句柄。"""

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, Protocol

CONTRACT_REVISION = 1


@dataclass(frozen=True)
class ProjectRef:
    principal_id: str
    environment_id: str
    project_id: str
    generation: int


@dataclass(frozen=True)
class ContentInfo:
    content_id: str
    principal_id: str
    size: int
    sha256: str


class ContentSource(Protocol):
    def info(self, principal_id: str, content_id: str) -> ContentInfo: ...
    def chunks(
        self, principal_id: str, content_id: str, chunk_size: int = 65536
    ) -> Iterator[bytes]: ...


class ProjectMutation(Protocol):
    def read(self, project: ProjectRef, path: str) -> bytes | None: ...
    def write(
        self,
        project: ProjectRef,
        operation_id: str,
        path: str,
        content: bytes,
        expected_sha256: str | None,
    ) -> dict[str, Any]: ...
    def rename(
        self,
        project: ProjectRef,
        operation_id: str,
        source: str,
        destination: str,
        expected_sha256: str,
        expected_destination_sha256: str | None,
    ) -> dict[str, Any]: ...
