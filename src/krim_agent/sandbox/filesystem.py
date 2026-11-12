"""持久项目的有界文件访问；目录句柄逐层打开，不跟随链接。"""

from __future__ import annotations

import base64
import fnmatch
import hashlib
import json
import os
import re
import stat
import unicodedata
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from krim_agent.contracts import ServiceError

FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK
MAX_FILE = 4 * 1024**2
MAX_SEARCH_PATTERN_BYTES = 256
MAX_SEARCH_ENTRIES = 4096
MAX_SEARCH_BYTES = 32 * 1024**2
MAX_REGEX_SEARCH_BYTES = 512 * 1024
MAX_REGEX_PATTERN_BYTES = 64
MAX_REGEX_LINE_CHARS = 1024
MAX_SEARCH_MATCHES = 4096
MAX_SEARCH_RESULT_BYTES = 64 * 1024


def _uuid(value: str) -> str:
    try:
        if type(value) is not str or str(uuid.UUID(value)) != value:
            raise ValueError("无效ID")
        return value
    except (ValueError, AttributeError):
        raise ServiceError("invalid_input", 422) from None


def _parts(path: str, *, directory: bool = False) -> tuple[str, ...]:
    if directory and path == ".":
        return ()
    if (
        type(path) is not str
        or len(path.encode()) > 512
        or "\\" in path
        or unicodedata.normalize("NFC", path) != path
    ):
        raise ServiceError("invalid_input", 422)
    parts = tuple(path.split("/"))
    if (
        len(parts) > 16
        or any(not p or p in {".", ".."} for p in parts)
        or any(unicodedata.category(c).startswith("C") for c in path)
    ):
        raise ServiceError("invalid_input", 422)
    if any(
        p.lower() in {".git", ".ssh", ".aws", "credentials", "id_rsa", "id_ed25519"}
        or p.lower() == ".env"
        or p.lower().startswith(".env.")
        for p in parts
    ):
        raise ServiceError("permission_denied", 403)
    return parts


def glob_matches(path: str, pattern: str) -> bool:
    path_parts = path.split("/")
    pattern_parts = pattern.split("/")
    reachable = {0}
    for pattern_part in pattern_parts:
        next_reachable: set[int] = set()
        if pattern_part == "**":
            for start in reachable:
                next_reachable.update(range(start, len(path_parts) + 1))
        else:
            next_reachable.update(
                index + 1
                for index in reachable
                if index < len(path_parts)
                and fnmatch.fnmatchcase(path_parts[index], pattern_part)
            )
        reachable = next_reachable
        if not reachable:
            return False
    return len(path_parts) in reachable


def validate_glob(pattern: str) -> str:
    if (
        type(pattern) is not str
        or not pattern
        or len(pattern.encode("utf-8")) > MAX_SEARCH_PATTERN_BYTES
        or not any(char in pattern for char in "*?[")
        or "\\" in pattern
        or any(part in {"", ".", ".."} for part in pattern.split("/"))
        or any(unicodedata.category(char).startswith("C") for char in pattern)
    ):
        raise ServiceError("invalid_input", 422)
    protected = (".git", ".ssh", ".aws", ".env", "credentials", "id_rsa", "id_ed25519")
    for segment in pattern.split("/"):
        if segment.lower() == ".env" or segment.lower().startswith(".env."):
            raise ServiceError("permission_denied", 403)
        try:
            _parts(segment)
        except ServiceError:
            raise ServiceError("permission_denied", 403) from None
        if any(char in segment for char in "*?[") and segment.lower().startswith(protected):
            raise ServiceError("permission_denied", 403)
    return pattern


class ProjectFiles:
    def __init__(self, root: Path) -> None:
        if not root.is_absolute():
            raise ServiceError("sandbox_config_invalid", 422)
        self.root = root
        self.closed = False
        self.parents = [(p, self._directory_identity(p)) for p in reversed([root, *root.parents])]
        self.fd = os.open(root, FLAGS)
        self.identity = os.fstat(self.fd)

    @staticmethod
    def _directory_identity(path: Path) -> tuple[int, int]:
        info = path.lstat()
        if not stat.S_ISDIR(info.st_mode):
            raise ServiceError("artifact_invalid")
        return info.st_dev, info.st_ino

    def _current(self) -> None:
        if self.closed:
            raise ServiceError("closed")
        try:
            if any(self._directory_identity(p) != identity for p, identity in self.parents):
                raise ServiceError("artifact_invalid")
        except OSError:
            raise ServiceError("artifact_invalid") from None

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            os.close(self.fd)

    @contextmanager
    def _content(self, environment_id: str, project_id: str) -> Iterator[int]:
        self._current()
        names = (_uuid(environment_id), "projects", _uuid(project_id), "content")
        opened = []
        current = self.fd
        try:
            for name in names:
                current = os.open(name, FLAGS, dir_fd=current)
                opened.append(current)
            yield current
            self._current()
        except FileNotFoundError:
            raise ServiceError("not_found", 404) from None
        except OSError:
            raise ServiceError("artifact_invalid") from None
        finally:
            for fd in reversed(opened):
                os.close(fd)

    @contextmanager
    def _parent(self, content: int, names: tuple[str, ...]) -> Iterator[int]:
        current = content
        opened = []
        try:
            for name in names:
                current = os.open(name, FLAGS, dir_fd=current)
                opened.append(current)
            yield current
        except FileNotFoundError:
            raise ServiceError("not_found", 404) from None
        except OSError:
            raise ServiceError("artifact_invalid") from None
        finally:
            for fd in reversed(opened):
                os.close(fd)

    def create(self, environment_id: str, project_id: str) -> None:
        self._current()
        names = (_uuid(environment_id), "projects", _uuid(project_id), "content")
        opened = []
        current = self.fd
        try:
            for name in names:
                try:
                    os.mkdir(name, 0o700, dir_fd=current)
                    os.fsync(current)
                except FileExistsError:
                    pass
                current = os.open(name, FLAGS, dir_fd=current)
                opened.append(current)
            self._current()
        except OSError:
            raise ServiceError("artifact_invalid") from None
        finally:
            for fd in reversed(opened):
                os.close(fd)

    @staticmethod
    def _regular(fd: int) -> os.stat_result:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ServiceError("artifact_invalid")
        if info.st_size > MAX_FILE:
            raise ServiceError("artifact_limit")
        return info

    @staticmethod
    def _hash(fd: int, size: int) -> str:
        digest = hashlib.sha256()
        for offset in range(0, size, 65536):
            chunk = os.pread(fd, min(65536, size - offset), offset)
            if not chunk:
                raise ServiceError("artifact_conflict")
            digest.update(chunk)
        return digest.hexdigest()

    def list(
        self,
        environment_id: str,
        project_id: str,
        path: str = ".",
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        """目录项稳定排序分页；敏感名称被过滤且不计入 total，扫描上限仍为 4096 项。"""
        names = _parts(path, directory=True)
        if type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or offset < 0:
            raise ServiceError("invalid_input", 422)
        with (
            self._content(environment_id, project_id) as content,
            self._parent(content, names) as fd,
        ):
            entries: list[dict[str, Any]] = []
            scanned = 0
            with os.scandir(fd) as scan:
                for entry in scan:
                    scanned += 1
                    if scanned > 4096:
                        raise ServiceError("artifact_limit")
                    try:
                        _parts(entry.name)
                    except ServiceError:
                        continue
                    info = entry.stat(follow_symlinks=False)
                    if stat.S_ISDIR(info.st_mode):
                        kind = "directory"
                    elif stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
                        kind = "file"
                    else:
                        kind = "blocked"
                    entries.append({"name": entry.name, "type": kind, "size": info.st_size})
        ordered = sorted(entries, key=lambda e: e["name"])
        total = len(ordered)
        following = offset + limit
        return {
            "path": path,
            "entries": ordered[offset:following],
            "offset": offset,
            "next_offset": following if following < total else None,
            "total": total,
        }

    def find(
        self,
        environment_id: str,
        project_id: str,
        *,
        path: str = ".",
        include_glob: str,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        names = _parts(path, directory=True)
        validate_glob(include_glob)
        if (
            type(limit) is not int
            or not 1 <= limit <= 100
            or type(offset) is not int
            or not 0 <= offset < MAX_SEARCH_ENTRIES
            or offset + limit > MAX_SEARCH_ENTRIES
        ):
            raise ServiceError("invalid_input", 422)
        found: list[dict[str, Any]] = []
        count = 0
        entry_cap = False
        root_prefix = "/".join(names) + "/" if names else ""
        depth_cap = False

        def walk(directory: int, prefix: str, depth: int) -> None:
            nonlocal count, entry_cap, depth_cap
            with os.scandir(directory) as listing:
                items = sorted(
                    ((entry.name, entry.stat(follow_symlinks=False)) for entry in listing),
                    key=lambda item: item[0],
                )
            for name, info in items:
                if entry_cap:
                    return
                count += 1
                if count > MAX_SEARCH_ENTRIES:
                    entry_cap = True
                    return
                full = prefix + name
                try:
                    _parts(name)
                except ServiceError:
                    continue
                if len(names) + depth + 1 > 16:
                    depth_cap = True
                    continue
                try:
                    _parts(full)
                except ServiceError as error:
                    if error.code == "invalid_input" and len(full.encode("utf-8")) > 512:
                        depth_cap = True
                    continue
                if stat.S_ISDIR(info.st_mode):
                    child = os.open(name, FLAGS, dir_fd=directory)
                    try:
                        if (os.fstat(child).st_dev, os.fstat(child).st_ino) != (
                            info.st_dev,
                            info.st_ino,
                        ):
                            raise ServiceError("artifact_conflict")
                        walk(child, full + "/", depth + 1)
                    finally:
                        os.close(child)
                    continue
                relative_path = full[len(root_prefix):] if full.startswith(root_prefix) else full
                matches_glob = glob_matches(relative_path, include_glob)
                if stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and matches_glob:
                    found.append({"path": full, "size": info.st_size})

        with self._content(environment_id, project_id) as content, self._parent(content, names) as fd:
            walk(fd, "/".join(names) + "/" if names else "", 0)
        page = found[offset : offset + limit]
        return {
            "path": path,
            "include_glob": include_glob,
            "files": page,
            "offset": offset,
            "total": None if entry_cap or depth_cap else len(found),
            "next_offset": None
            if entry_cap or depth_cap
            else (offset + len(page) if offset + len(page) < len(found) else None),
            "truncated": entry_cap or depth_cap,
            "entry_cap_reached": entry_cap,
            "depth_cap_reached": depth_cap,
        }

    def read(
        self,
        environment_id: str,
        project_id: str,
        path: str,
        offset: int = 0,
        limit: int = 65536,
        *,
        expected_sha256: str | None = None,
        expected_stat: list[int] | tuple[int, int, int, int, int] | None = None,
    ) -> dict[str, Any]:
        names = _parts(path)
        if (
            type(offset) is not int
            or offset < 0
            or type(limit) is not int
            or not 1 <= limit <= 65536
            or (expected_sha256 is None) != (expected_stat is None)
            or expected_sha256 is not None
            and (
                type(expected_sha256) is not str
                or re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None
            )
            or expected_stat is not None
            and (
                type(expected_stat) not in {list, tuple}
                or len(expected_stat) != 5
                or any(type(value) is not int for value in expected_stat)
                or any(value < 0 for value in expected_stat[:3])
            )
        ):
            raise ServiceError("invalid_input", 422)
        with (
            self._content(environment_id, project_id) as content,
            self._parent(content, names[:-1]) as fd,
        ):
            handle = os.open(names[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
            try:
                info = self._regular(handle)
                initial_stat = (
                    info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns
                )
                if expected_stat is not None and initial_stat != tuple(expected_stat):
                    raise ServiceError("artifact_conflict")
                if offset > info.st_size:
                    if expected_stat is not None:
                        raise ServiceError("artifact_conflict")
                    raise ServiceError("invalid_input", 422)
                # 后续块复用首块摘要；调用方仍须校验完整拼接结果的SHA-256。
                digest = (
                    self._hash(handle, info.st_size)
                    if expected_sha256 is None
                    else expected_sha256
                )
                data = os.pread(handle, limit, offset)
                after = os.fstat(handle)
                if initial_stat != (
                    after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns
                ):
                    raise ServiceError("artifact_conflict")
                return {
                    "path": path,
                    "offset": offset,
                    "size": info.st_size,
                    "eof": offset + len(data) >= info.st_size,
                    "sha256": digest,
                    "stat_token": list(initial_stat),
                    "data": base64.b64encode(data).decode(),
                }
            finally:
                os.close(handle)

    def write(
        self,
        environment_id: str,
        project_id: str,
        path: str,
        data: bytes,
        expected_sha256: str | None,
    ) -> dict[str, Any]:
        return self._replace(environment_id, project_id, path, data, expected_sha256, 1024**2)

    def restore_write(
        self,
        environment_id: str,
        project_id: str,
        path: str,
        data: bytes,
        expected_sha256: str | None,
        journal: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        return self._replace(
            environment_id, project_id, path, data, expected_sha256, MAX_FILE, journal
        )

    def _replace(
        self,
        environment_id: str,
        project_id: str,
        path: str,
        data: bytes,
        expected_sha256: str | None,
        maximum: int,
        journal: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """调用方须持有写租约；父目录必须存在，不隐式扩展目录树。"""
        names = _parts(path)
        if (
            type(data) is not bytes
            or len(data) > maximum
            or expected_sha256 is not None
            and (
                type(expected_sha256) is not str
                or len(expected_sha256) != 64
                or any(c not in "0123456789abcdef" for c in expected_sha256)
            )
        ):
            raise ServiceError("invalid_input", 422)
        with (
            self._content(environment_id, project_id) as content,
            self._parent(content, names[:-1]) as fd,
        ):
            old = None
            try:
                handle = os.open(names[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
            except FileNotFoundError:
                if expected_sha256 is not None:
                    raise ServiceError("artifact_conflict") from None
            else:
                try:
                    old = self._regular(handle)
                    if (
                        expected_sha256 is None
                        or self._hash(handle, old.st_size) != expected_sha256
                    ):
                        raise ServiceError("artifact_conflict")
                finally:
                    os.close(handle)
            temporary = ".krim-write-" + uuid.uuid4().hex
            handle = os.open(
                temporary, os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_WRONLY, 0o600, dir_fd=fd
            )
            try:
                if journal is not None:
                    temporary_info = os.fstat(handle)
                    journal(
                        {
                            "name": temporary,
                            "dev": temporary_info.st_dev,
                            "ino": temporary_info.st_ino,
                        }
                    )
                with os.fdopen(os.dup(handle), "wb") as stream:
                    stream.write(data)
                    stream.flush()
                owner = os.fstat(content)
                if os.geteuid() == 0:
                    os.fchown(handle, owner.st_uid, owner.st_gid)
                os.fsync(handle)
                try:
                    current = os.stat(names[-1], dir_fd=fd, follow_symlinks=False)
                except FileNotFoundError:
                    current = None
                if (old is None) != (current is None) or (
                    old is not None
                    and current is not None
                    and (old.st_dev, old.st_ino, old.st_mtime_ns, old.st_ctime_ns)
                    != (current.st_dev, current.st_ino, current.st_mtime_ns, current.st_ctime_ns)
                ):
                    raise ServiceError("artifact_conflict")
                os.replace(temporary, names[-1], src_dir_fd=fd, dst_dir_fd=fd)
                os.fsync(fd)
                self._current()
                return {"path": path, "size": len(data), "sha256": hashlib.sha256(data).hexdigest()}
            finally:
                os.close(handle)
                try:
                    os.unlink(temporary, dir_fd=fd)
                except FileNotFoundError:
                    pass

    def mkdir(self, environment_id: str, project_id: str, path: str) -> dict[str, Any]:
        """调用方须持有写租约；逐层创建，已存在普通目录幂等通过，文件或链接占位则拒绝。"""
        names = _parts(path)
        with self._content(environment_id, project_id) as content:
            owner = os.fstat(content)
            current = content
            opened = []
            created = False
            try:
                for name in names:
                    try:
                        os.mkdir(name, 0o700, dir_fd=current)
                    except FileExistsError:
                        existing = os.stat(name, dir_fd=current, follow_symlinks=False)
                        if not stat.S_ISDIR(existing.st_mode):
                            raise ServiceError("artifact_invalid") from None
                    else:
                        created = True
                        os.fsync(current)
                    handle = os.open(name, FLAGS, dir_fd=current)
                    opened.append(handle)
                    if os.geteuid() == 0:
                        os.fchown(handle, owner.st_uid, owner.st_gid)
                    current = handle
                os.fsync(current)
                self._current()
            except OSError:
                raise ServiceError("artifact_invalid") from None
            finally:
                for fd in reversed(opened):
                    os.close(fd)
        return {"path": path, "created": created}

    def remove(
        self, environment_id: str, project_id: str, path: str, expected_sha256: str
    ) -> dict[str, Any]:
        """调用方须持有写租约并自行落checkpoint；首版仅删除普通单文件，不递归目录。"""
        names = _parts(path)
        if (
            type(expected_sha256) is not str
            or len(expected_sha256) != 64
            or any(c not in "0123456789abcdef" for c in expected_sha256)
        ):
            raise ServiceError("invalid_input", 422)
        with (
            self._content(environment_id, project_id) as content,
            self._parent(content, names[:-1]) as fd,
        ):
            handle = os.open(names[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
            try:
                info = self._regular(handle)
                if self._hash(handle, info.st_size) != expected_sha256:
                    raise ServiceError("artifact_conflict")
                current = os.stat(names[-1], dir_fd=fd, follow_symlinks=False)
                before = (
                    info.st_dev,
                    info.st_ino,
                    info.st_nlink,
                    info.st_mtime_ns,
                    info.st_ctime_ns,
                )
                now = (
                    current.st_dev,
                    current.st_ino,
                    current.st_nlink,
                    current.st_mtime_ns,
                    current.st_ctime_ns,
                )
                if before != now:
                    raise ServiceError("artifact_conflict")
                os.unlink(names[-1], dir_fd=fd)
                os.fsync(fd)
                return {"path": path, "deleted": True}
            finally:
                os.close(handle)

    def search(
        self,
        environment_id: str,
        project_id: str,
        query: str,
        path: str = ".",
        limit: int = 50,
        offset: int = 0,
        include_glob: str | None = None,
        mode: str = "literal",
        case_sensitive: bool = True,
        before: int = 0,
        after: int = 0,
    ) -> dict[str, Any]:
        """有界文本搜索；regex刻意限制为无量词/分组/回溯的安全子集。"""
        names = _parts(path, directory=True)
        if (
            type(query) is not str
            or mode not in {"literal", "regex"}
            or not query.strip()
            or not 1 <= len(query.encode("utf-8")) <= MAX_SEARCH_PATTERN_BYTES
            or type(limit) is not int
            or not 1 <= limit <= 100
            or type(offset) is not int
            or not 0 <= offset < MAX_SEARCH_MATCHES
            or offset + limit > MAX_SEARCH_MATCHES
            or type(case_sensitive) is not bool
            or type(before) is not int
            or not 0 <= before <= 3
            or type(after) is not int
            or not 0 <= after <= 3
        ):
            raise ServiceError("invalid_input", 422)
        if include_glob is not None:
            validate_glob(include_glob)
        matcher: re.Pattern[str] | None = None
        if mode == "regex":
            # Only single-character escapes and character classes are supported. No
            # groups, alternation, quantifiers, anchors, or backreferences are exposed.
            if (
                len(query.encode("utf-8")) > MAX_REGEX_PATTERN_BYTES
                or any(char in query for char in "*+?{}()|^$")
                or any(
                    query[index] == "\\" and query[index + 1] not in "dDsSwW.[]-\\"
                    for index in range(len(query) - 1)
                )
                or re.search(r"\\[1-9]", query)
            ):
                raise ServiceError("invalid_input", 422)
            try:
                matcher = re.compile(query, 0 if case_sensitive else re.IGNORECASE)
            except re.error:
                raise ServiceError("invalid_input", 422) from None
        elif not case_sensitive:
            query = query.casefold()

        matches: list[dict[str, Any]] = []
        result_bytes = 0
        scanned_bytes = 0
        root_prefix = "/".join(names) + "/" if names else ""
        skipped = 0
        total_matches = 0
        entry_cap = byte_cap = depth_cap = match_cap = False
        count = 0

        def scan(directory: int, prefix: str, depth: int) -> None:
            nonlocal count, scanned_bytes, skipped, total_matches, result_bytes
            nonlocal entry_cap, byte_cap, depth_cap, match_cap
            with os.scandir(directory) as listing:
                items = sorted(
                    ((entry.name, entry.stat(follow_symlinks=False)) for entry in listing),
                    key=lambda item: item[0],
                )
            for name, before_info in items:
                if entry_cap or match_cap:
                    return
                count += 1
                if count > MAX_SEARCH_ENTRIES:
                    entry_cap = True
                    return
                full = prefix + name
                try:
                    _parts(name)
                except ServiceError:
                    continue
                if len(names) + depth + 1 > 16:
                    depth_cap = True
                    continue
                try:
                    _parts(full)
                except ServiceError as error:
                    if error.code == "invalid_input" and len(full.encode("utf-8")) > 512:
                        depth_cap = True
                    continue
                if stat.S_ISDIR(before_info.st_mode):
                    child = os.open(name, FLAGS, dir_fd=directory)
                    try:
                        if (os.fstat(child).st_dev, os.fstat(child).st_ino) != (
                            before_info.st_dev,
                            before_info.st_ino,
                        ):
                            raise ServiceError("artifact_conflict")
                        scan(child, full + "/", depth + 1)
                    finally:
                        os.close(child)
                    continue
                if not (stat.S_ISREG(before_info.st_mode) and before_info.st_nlink == 1):
                    continue
                relative_path = full[len(root_prefix):] if full.startswith(root_prefix) else full
                if include_glob is not None and not glob_matches(relative_path, include_glob):
                    continue
                if before_info.st_size > MAX_FILE:
                    skipped += 1
                    continue
                byte_limit = MAX_REGEX_SEARCH_BYTES if mode == "regex" else MAX_SEARCH_BYTES
                if scanned_bytes + before_info.st_size > byte_limit:
                    skipped += 1
                    byte_cap = True
                    continue
                fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
                try:
                    actual = self._regular(fd)
                    if (actual.st_dev, actual.st_ino) != (before_info.st_dev, before_info.st_ino):
                        raise ServiceError("artifact_conflict")
                    body = os.pread(fd, actual.st_size, 0)
                    after_info = os.fstat(fd)
                    if len(body) != actual.st_size or (
                        actual.st_size,
                        actual.st_mtime_ns,
                        actual.st_ctime_ns,
                    ) != (after_info.st_size, after_info.st_mtime_ns, after_info.st_ctime_ns):
                        raise ServiceError("artifact_conflict")
                finally:
                    os.close(fd)
                scanned_bytes += len(body)
                try:
                    text = body.decode("utf-8")
                except UnicodeDecodeError:
                    skipped += 1
                    continue
                lines = text.splitlines()
                if mode == "regex" and any(len(line) > MAX_REGEX_LINE_CHARS for line in lines):
                    skipped += 1
                    byte_cap = True
                    continue
                for index, line in enumerate(lines):
                    subject = line if mode == "regex" or case_sensitive else line.casefold()
                    hit = matcher.search(subject) is not None if matcher else query in subject
                    if not hit:
                        continue
                    if total_matches >= MAX_SEARCH_MATCHES:
                        match_cap = True
                        return
                    current = total_matches
                    total_matches += 1
                    if offset <= current < offset + limit:
                        item = {
                            "path": full,
                            "line": index + 1,
                            "preview": line[:256],
                            "before": [v[:256] for v in lines[max(0, index-before):index]],
                            "after": [v[:256] for v in lines[index+1:index+1+after]],
                        }
                        item_bytes = len(json.dumps(item, ensure_ascii=False).encode("utf-8"))
                        if result_bytes + item_bytes > MAX_SEARCH_RESULT_BYTES:
                            match_cap = True
                            return
                        result_bytes += item_bytes
                        matches.append(item)

        with (
            self._content(environment_id, project_id) as content,
            self._parent(content, names) as fd,
        ):
            scan(fd, "/".join(names) + "/" if names else "", 0)
        next_offset = offset + len(matches) if offset + len(matches) < total_matches else None
        scan_incomplete = entry_cap or match_cap or byte_cap or depth_cap
        results_truncated = scan_incomplete or next_offset is not None
        return {
            "path": path,
            "offset": offset,
            "matches": matches,
            "total_matches": total_matches if not scan_incomplete else None,
            "next_offset": next_offset if not scan_incomplete else None,
            "truncated": results_truncated,
            "entry_cap_reached": entry_cap,
            "match_cap_reached": match_cap,
            "skipped": skipped,
            "scanned_bytes": scanned_bytes,
            "byte_cap_reached": byte_cap,
            "depth_cap_reached": depth_cap,
        }

    def snapshot(self, environment_id: str, project_id: str) -> dict[str, Any]:
        """写者静止且持有项目租约时使用；覆盖有界，排除项逐一记录。"""
        files: dict[str, bytes] = {}
        excluded: list[dict[str, str]] = []
        count = 0
        size = 0

        def walk(directory: int, prefix: str) -> None:
            nonlocal count, size
            with os.scandir(directory) as scan:
                for entry in scan:
                    count += 1
                    if count > 4096:
                        raise ServiceError("artifact_limit")
                    path = prefix + entry.name
                    try:
                        _parts(path)
                    except ServiceError:
                        excluded.append({"path": path[:512], "reason": "sensitive_or_invalid"})
                        continue
                    # 明确的项目根运行目录，而非忽略任何层级的同名用户文件。
                    if not prefix and entry.name in {"node_modules", ".venv", ".krim-cache"}:
                        excluded.append({"path": path, "reason": "runtime_directory"})
                        continue
                    before = entry.stat(follow_symlinks=False)
                    if stat.S_ISDIR(before.st_mode):
                        child = os.open(entry.name, FLAGS, dir_fd=directory)
                        try:
                            if (os.fstat(child).st_dev, os.fstat(child).st_ino) != (
                                before.st_dev,
                                before.st_ino,
                            ):
                                raise ServiceError("artifact_conflict")
                            walk(child, path + "/")
                        finally:
                            os.close(child)
                    elif stat.S_ISREG(before.st_mode) and before.st_nlink == 1:
                        if before.st_size > MAX_FILE:
                            excluded.append({"path": path, "reason": "file_too_large"})
                            continue
                        if size + before.st_size > 32 * 1024**2:
                            raise ServiceError("artifact_limit")
                        fd = os.open(
                            entry.name,
                            os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                            dir_fd=directory,
                        )
                        try:
                            actual = self._regular(fd)
                            if (actual.st_dev, actual.st_ino) != (before.st_dev, before.st_ino):
                                raise ServiceError("artifact_conflict")
                            body = os.pread(fd, actual.st_size + 1, 0)
                            after = os.fstat(fd)
                            if len(body) != actual.st_size or (
                                actual.st_size,
                                actual.st_mtime_ns,
                                actual.st_ctime_ns,
                            ) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                                raise ServiceError("artifact_conflict")
                            files[path] = body
                            size += len(body)
                        finally:
                            os.close(fd)
                    else:
                        excluded.append({"path": path, "reason": "link_or_special_file"})

        with self._content(environment_id, project_id) as content:
            walk(content, "")
        return {"files": files, "excluded": excluded, "total_bytes": size}

    def cleanup_restore_temporary(
        self, environment_id: str, project_id: str, target: str, identity: dict[str, Any]
    ) -> None:
        """只清原恢复日志记下的临时inode；不扫描或删除用户其它.tmp文件。"""
        names = _parts(target)
        name = identity["name"]
        if (
            type(name) is not str
            or not name.startswith(".krim-write-")
            or len(name) != 44
            or any(c not in "0123456789abcdef" for c in name[12:])
        ):
            raise ServiceError("facts_corrupt")
        with (
            self._content(environment_id, project_id) as content,
            self._parent(content, names[:-1]) as fd,
        ):
            try:
                info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            except FileNotFoundError:
                return
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_nlink != 1
                or (info.st_dev, info.st_ino) != (identity["dev"], identity["ino"])
            ):
                raise ServiceError("artifact_conflict")
            os.unlink(name, dir_fd=fd)
            os.fsync(fd)

    def rename(
        self,
        environment_id: str,
        project_id: str,
        source: str,
        destination: str,
        expected_sha256: str,
        expected_destination_sha256: str | None = None,
    ) -> dict[str, Any]:
        """调用方持写租约/已做检查点；只在同文件系统原子移动普通单文件。"""
        src, dst = _parts(source), _parts(destination)
        if source == destination:
            raise ServiceError("invalid_input", 422)
        for digest in (expected_sha256, expected_destination_sha256):
            if digest is not None and (
                type(digest) is not str
                or len(digest) != 64
                or any(c not in "0123456789abcdef" for c in digest)
            ):
                raise ServiceError("invalid_input", 422)
        if expected_sha256 is None:
            raise ServiceError("invalid_input", 422)
        with self._content(environment_id, project_id) as content:
            with (
                self._parent(content, src[:-1]) as parent_src,
                self._parent(content, dst[:-1]) as parent_dst,
            ):
                handle = os.open(
                    src[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent_src
                )
                other = None
                try:
                    old = self._regular(handle)
                    if self._hash(handle, old.st_size) != expected_sha256:
                        raise ServiceError("artifact_conflict")
                    try:
                        other = os.open(
                            dst[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent_dst
                        )
                    except FileNotFoundError:
                        if expected_destination_sha256 is not None:
                            raise ServiceError("artifact_conflict") from None
                        replaced = None
                    else:
                        replaced = self._regular(other)
                        if (
                            expected_destination_sha256 is None
                            or self._hash(other, replaced.st_size) != expected_destination_sha256
                        ):
                            raise ServiceError("artifact_conflict")
                    if os.fstat(parent_src).st_dev != os.fstat(parent_dst).st_dev:
                        raise ServiceError("artifact_invalid")
                    actual = os.stat(src[-1], dir_fd=parent_src, follow_symlinks=False)

                    def identity(info: os.stat_result) -> tuple[int, ...]:
                        return (
                            info.st_dev,
                            info.st_ino,
                            info.st_nlink,
                            info.st_size,
                            info.st_mtime_ns,
                            info.st_ctime_ns,
                        )

                    if identity(old) != identity(actual):
                        raise ServiceError("artifact_conflict")
                    try:
                        current = os.stat(dst[-1], dir_fd=parent_dst, follow_symlinks=False)
                    except FileNotFoundError:
                        current = None
                    if (
                        (current is None) != (replaced is None)
                        or current is not None
                        and replaced is not None
                        and identity(current) != identity(replaced)
                    ):
                        raise ServiceError("artifact_conflict")
                    os.rename(src[-1], dst[-1], src_dir_fd=parent_src, dst_dir_fd=parent_dst)
                    os.fsync(parent_src)
                    os.fsync(parent_dst)
                    self._current()
                    return {
                        "source": source,
                        "destination": destination,
                        "sha256": expected_sha256,
                        "renamed": True,
                        "atomic": True,
                    }
                finally:
                    os.close(handle)
                    if other is not None:
                        os.close(other)
