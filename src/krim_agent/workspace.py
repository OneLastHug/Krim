"""可信宿主授权的文件树；dirfd逐段打开、有限文本读取，不提供进程沙箱。"""

from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import stat
from dataclasses import dataclass, field
from pathlib import Path
from threading import Event, Lock, RLock
from typing import Literal

from pydantic import Field, JsonValue

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel

WORKSPACE_READ_TOOL_NAMES = frozenset({"workspace_read", "workspace_list", "workspace_search"})
WORKSPACE_CHANGE_TOOL_NAMES = frozenset(
    {"workspace_preview", "workspace_change", "workspace_reconcile", "workspace_finalize"}
)
WORKSPACE_DIRECTORY_TOOL_NAMES = frozenset(
    {"directory_preview", "directory_change", "directory_reconcile", "directory_finalize"}
)
WORKSPACE_TOOL_NAMES = (
    WORKSPACE_READ_TOOL_NAMES | WORKSPACE_CHANGE_TOOL_NAMES | WORKSPACE_DIRECTORY_TOOL_NAMES
)
WORKSPACE_POLICY_VERSION = "readonly-v1"
MAX_RESULT_BYTES = 24000
MAX_DIRECTORY_ENTRIES = 5000


class WorkspaceFile(StrictModel):
    path: str
    sha256: str
    byte_size: int
    start_line: int
    lines: int
    content: str
    truncated: bool


class WorkspaceEntry(StrictModel):
    path: str
    kind: Literal["directory", "file"]
    byte_size: int


class WorkspaceListing(StrictModel):
    path: str
    entries: list[WorkspaceEntry]
    truncated: bool
    entries_scanned: int = Field(default=0, ge=0)


class WorkspaceMatch(StrictModel):
    path: str
    line: int
    text: str


class WorkspaceSearch(StrictModel):
    matches: list[WorkspaceMatch]
    files_scanned: int = Field(ge=0)
    files_skipped: int = Field(default=0, ge=0)
    truncated: bool


class WorkspaceView(StrictModel):
    key: str
    version: str
    fingerprint: str
    available: bool
    tool_names: list[str]
    max_file_bytes: int
    writable: bool = False


class Workspace:
    def __init__(
        self, root: str | Path, *, max_file_bytes: int = 1_048_576, writable: bool = False
    ) -> None:
        if (
            os.name != "posix"
            or not all(hasattr(os, flag) for flag in ("O_NOFOLLOW", "O_DIRECTORY", "O_NONBLOCK"))
            or os.open not in os.supports_dir_fd
            or os.scandir not in os.supports_fd
        ):
            raise ServiceError("workspace_platform", "当前平台尚未验证dirfd文件边界", 503)
        if not 1 <= max_file_bytes <= 8_388_608:
            raise ValueError("工作区文件上限无效")
        try:
            self._root = Path(root).resolve(strict=True)
            self._fd = os.open(self._root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        except (OSError, RuntimeError):
            raise ServiceError("workspace_root", "宿主工作区根目录无法打开", 422) from None
        self._max_file_bytes = max_file_bytes
        if type(writable) is not bool:
            os.close(self._fd)
            raise ValueError("工作区写能力须明确为bool")
        self._writable = writable
        self._closed = False
        self._lock = Lock()
        self._write_lock = RLock()
        info = os.fstat(self._fd)
        self.fingerprint = hashlib.sha256(
            json.dumps(
                {
                    "root": str(self._root),
                    "device": info.st_dev,
                    "inode": info.st_ino,
                    "max_file_bytes": max_file_bytes,
                    "policy": WORKSPACE_POLICY_VERSION,
                    **({"write_policy": "renameat2-v1"} if writable else {}),
                },
                sort_keys=True,
            ).encode()
        ).hexdigest()

    @property
    def root(self) -> Path:
        return self._root

    @property
    def max_file_bytes(self) -> int:
        return self._max_file_bytes

    @property
    def writable(self) -> bool:
        return self._writable

    @property
    def closed(self) -> bool:
        return self._closed

    def __enter__(self) -> Workspace:
        return self

    def __exit__(self, *_arguments: object) -> None:
        self.close()

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._closed = True
                os.close(self._fd)

    @staticmethod
    def _parts(path: str) -> list[str]:
        try:
            path_bytes = path.encode("utf-8")
        except UnicodeEncodeError:
            raise ServiceError("workspace_path", "工作区路径必须为UTF-8", 422) from None
        if len(path_bytes) > 512 or any(ord(c) < 32 for c in path) or "\\" in path or ":" in path:
            raise ServiceError("workspace_path", "工作区相对路径无效", 422)
        if path in {"", "."}:
            return []
        parts = path.split("/")
        if any(part in {"", ".", ".."} for part in parts) or len(parts) > 16:
            raise ServiceError("workspace_path", "工作区路径包含越界或无效片段", 422)
        for part in parts:
            lowered = part.casefold()
            if lowered in {
                ".git",
                ".venv",
                "venv",
                "node_modules",
                "__pycache__",
                ".ssh",
                ".aws",
                ".kube",
                ".gnupg",
                ".cache",
                ".data",
                ".tools",
                ".npmrc",
                ".pypirc",
                "id_rsa",
                "id_ed25519",
                "id_ecdsa",
            } or any(
                fnmatch.fnmatchcase(lowered, pattern)
                for pattern in (
                    ".env",
                    ".env.*",
                    "*.key",
                    "*.pem",
                    "*.p12",
                    "*.pfx",
                    "*.keystore",
                    "._krim-tx-*",
                )
            ):
                raise ServiceError("workspace_sensitive", "工作区敏感或内部路径不可读取", 403)
        return parts

    def _open(self, path: str, *, directory: bool = False) -> int:
        parts = self._parts(path)
        with self._lock:
            if self._closed:
                raise ServiceError("workspace_closed", "工作区已关闭", 503)
            current = os.dup(self._fd)
        try:
            for index, part in enumerate(parts):
                flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
                if directory or index < len(parts) - 1:
                    flags |= os.O_DIRECTORY
                next_fd = os.open(part, flags, dir_fd=current)
                os.close(current)
                current = next_fd
            if not directory and (not parts or not stat.S_ISREG(os.fstat(current).st_mode)):
                raise ServiceError("workspace_file_type", "工作区只允许普通文件", 422)
            return current
        except FileNotFoundError:
            os.close(current)
            raise ServiceError("workspace_not_found", "工作区条目不存在", 404) from None
        except OSError:
            os.close(current)
            raise ServiceError("workspace_access", "工作区路径不能访问或包含链接", 403) from None
        except BaseException:
            os.close(current)
            raise

    def _text(self, path: str, *, byte_budget: int | None = None) -> tuple[bytes, str]:
        fd = self._open(path)
        try:
            info = os.fstat(fd)
            if info.st_nlink != 1:
                raise ServiceError("workspace_link", "工作区不读取多硬链接文件", 403)
            size_limit = min(self.max_file_bytes, byte_budget or self.max_file_bytes)
            if info.st_size > size_limit:
                raise ServiceError("workspace_file_limit", "文件超过工作区容量限制", 413)
            chunks: list[bytes] = []
            size = 0
            while size <= size_limit:
                part = os.read(fd, min(65536, size_limit + 1 - size))
                if not part:
                    break
                chunks.append(part)
                size += len(part)
            if size > size_limit:
                raise ServiceError("workspace_file_limit", "文件读取超过工作区容量限制", 413)
            raw = b"".join(chunks)
            after = os.fstat(fd)
            if (info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_nlink) != (
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
                after.st_nlink,
            ) or len(raw) != info.st_size:
                raise ServiceError("workspace_changed", "文件读取期间已变化，请重新读取", 409)
            if b"\0" in raw:
                raise ServiceError("workspace_binary", "工作区文件不是可读取文本", 422)
            try:
                return raw, raw.decode("utf-8")
            except UnicodeDecodeError:
                raise ServiceError("workspace_encoding", "工作区文件必须是UTF-8", 422) from None
        except OSError:
            raise ServiceError("workspace_access", "工作区文件读取失败", 403) from None
        finally:
            os.close(fd)

    def read(self, path: str, *, start_line: int = 1, max_lines: int = 200) -> WorkspaceFile:
        if start_line < 1 or not 1 <= max_lines <= 1000:
            raise ServiceError("workspace_lines", "文件行范围无效", 422)
        raw, decoded = self._text(path)
        return self.file_view(path, raw, decoded, start_line=start_line, max_lines=max_lines)

    @staticmethod
    def file_view(
        path: str, raw: bytes, decoded: str, *, start_line: int = 1, max_lines: int = 200
    ) -> WorkspaceFile:
        """可信读取层将同一已核实内容投影为原有有界输出，不再次打开文件。"""
        lines = decoded.splitlines(keepends=True)
        selected = lines[start_line - 1 : start_line - 1 + max_lines]
        result = WorkspaceFile(
            path=path,
            sha256=hashlib.sha256(raw).hexdigest(),
            byte_size=len(raw),
            start_line=start_line,
            lines=len(selected),
            content="".join(selected),
            truncated=start_line - 1 + len(selected) < len(lines),
        )
        while len(result.model_dump_json().encode()) > MAX_RESULT_BYTES:
            result = result.model_copy(
                update={
                    "content": result.content[: len(result.content) // 2],
                    "truncated": True,
                }
            )
        return result.model_copy(update={"lines": len(result.content.splitlines())})

    def list(self, path: str = ".", *, limit: int = 200) -> WorkspaceListing:
        if not 1 <= limit <= 1000:
            raise ServiceError("workspace_list_limit", "目录结果数量无效", 422)
        fd = self._open(path, directory=True)
        result = WorkspaceListing(path=path, entries=[], truncated=False)
        try:
            with os.scandir(fd) as entries:
                for scanned, entry in enumerate(entries):
                    if scanned >= MAX_DIRECTORY_ENTRIES:
                        result.truncated = True
                        break
                    result.entries_scanned += 1
                    candidate = entry.name if path in {"", "."} else f"{path}/{entry.name}"
                    try:
                        self._parts(candidate)
                        info = entry.stat(follow_symlinks=False)
                    except (ServiceError, OSError):
                        continue
                    kind: Literal["directory", "file"]
                    if stat.S_ISDIR(info.st_mode):
                        kind = "directory"
                    elif stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
                        kind = "file"
                    else:
                        continue
                    if len(result.entries) >= limit:
                        result.truncated = True
                        break
                    result.entries.append(
                        WorkspaceEntry(
                            path=candidate,
                            kind=kind,
                            byte_size=info.st_size,
                        )
                    )
                    if len(result.model_dump_json().encode()) > MAX_RESULT_BYTES:
                        result.entries.pop()
                        result.truncated = True
                        break
            result.entries.sort(key=lambda item: item.path)
            return result
        except OSError:
            raise ServiceError("workspace_access", "工作区目录读取失败", 403) from None
        finally:
            os.close(fd)

    def search(self, query: str, *, path: str = ".", limit: int = 100) -> WorkspaceSearch:
        if not query or len(query) > 200 or not 1 <= limit <= 100:
            raise ServiceError("workspace_query", "文本检索参数无效", 422)
        pending = [path]
        result = WorkspaceSearch(matches=[], files_scanned=0, truncated=False)
        total_bytes, entries_seen = 0, 0
        while pending:
            directory = pending.pop()
            try:
                listing = self.list(directory, limit=1000)
            except ServiceError:
                if directory == path:
                    raise
                result.files_skipped += 1
                result.truncated = True
                continue
            result.truncated = result.truncated or listing.truncated
            entries_seen += listing.entries_scanned
            if entries_seen > MAX_DIRECTORY_ENTRIES:
                return result.model_copy(update={"truncated": True})
            for entry in listing.entries:
                if entry.kind == "directory":
                    if len(entry.path.split("/")) < 16:
                        pending.append(entry.path)
                    else:
                        result.truncated = True
                    continue
                if result.files_scanned >= 1000 or total_bytes >= 10_485_760:
                    return result.model_copy(update={"truncated": True})
                result.files_scanned += 1
                remaining = 10_485_760 - total_bytes
                if entry.byte_size > min(self.max_file_bytes, remaining):
                    result.files_skipped += 1
                    continue
                allowance = max(1, entry.byte_size)
                total_bytes += allowance
                try:
                    _raw, content = self._text(entry.path, byte_budget=allowance)
                except ServiceError:
                    result.files_skipped += 1
                    continue
                for number, line in enumerate(content.splitlines(), start=1):
                    if query in line:
                        result.matches.append(
                            WorkspaceMatch(
                                path=entry.path,
                                line=number,
                                text=line[:500],
                            )
                        )
                        if len(result.model_dump_json().encode()) > MAX_RESULT_BYTES:
                            result.matches.pop()
                            return result.model_copy(update={"truncated": True})
                        if len(result.matches) >= limit:
                            return result.model_copy(update={"truncated": True})
        return result


@dataclass(frozen=True)
class WorkspaceBinding:
    key: str
    version: str
    workspace: Workspace
    fingerprint: str
    # 撤销信号由所有冻结的Agent绑定共享，停用后不能继续发起文件访问。
    revoked: Event = field(default_factory=Event, repr=False, compare=False)

    @property
    def available(self) -> bool:
        return not self.revoked.is_set() and not self.workspace.closed

    def require(self) -> Workspace:
        if not self.available:
            raise ServiceError("workspace_unavailable", "工作区资源已停用或关闭", 403)
        return self.workspace

    def manifest(self) -> dict[str, JsonValue]:
        return {"key": self.key, "version": self.version, "fingerprint": self.fingerprint}

    def view(self, tool_names: list[str]) -> WorkspaceView:
        return WorkspaceView(
            key=self.key,
            version=self.version,
            fingerprint=self.fingerprint,
            available=self.available,
            tool_names=tool_names,
            max_file_bytes=self.workspace.max_file_bytes,
            writable=self.workspace.writable,
        )
