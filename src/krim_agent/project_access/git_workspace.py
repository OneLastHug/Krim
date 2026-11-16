"""Host-only import of an authenticated workspace into a private bare Git repo.

The caller supplies the authenticated ``WorkspaceBinding`` and revalidates it
through ``verify``.  This adapter deliberately accepts no model path: both the
workspace root and the destination repository are operator-selected host paths.

Only regular, non-symlink files are imported.  A ``.git`` component (in any
case) is inspected for a safe file type and then excluded from the tree; its
contents are never traversed.  The source is read through directory file
descriptors, and a second descriptor walk rejects directory or file races.
"""

from __future__ import annotations

import asyncio
import os
import re
import stat
import subprocess
import tempfile
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from krim_agent.contracts import ServiceError
from krim_agent.development.contracts import EnvironmentHandle
from krim_agent.project_access.git_execution import TrustedGitRunner

_GIT_ID: Final = re.compile(r"^[0-9a-f]+$")
_MAX_PATH_BYTES: Final = 4096
_MAX_OUTPUT_BYTES: Final = 64 * 1024
_STAGE_FILES: Final = 128
_STAGE_BYTES: Final = 1024 * 1024


@dataclass(frozen=True, slots=True)
class WorkspaceBinding:
    """The immutable source binding authenticated by the integration layer."""

    environment: EnvironmentHandle
    root: Path
    root_device: int
    root_inode: int


@dataclass(frozen=True, slots=True)
class WorkspaceTree:
    """The Git tree and bounded import accounting returned to the host."""

    tree_id: str
    file_count: int
    byte_count: int


@dataclass(frozen=True, slots=True)
class _Fingerprint:
    kind: str
    device: int
    inode: int
    mode: int
    size: int
    mtime_ns: int
    ctime_ns: int
    links: int


@dataclass(frozen=True, slots=True)
class _FileSnapshot:
    path: bytes
    data: bytes
    mode: int
    fingerprint: _Fingerprint


class _StageVerifier:
    """Call the integration revalidator at bounded scan/import checkpoints."""

    def __init__(
        self,
        binding: WorkspaceBinding,
        repository: Path,
        verify: Callable[[WorkspaceBinding, Path], Awaitable[None]],
    ) -> None:
        self._binding = binding
        self._repository = repository
        self._verify = verify
        self._files = 0
        self._bytes = 0

    async def add(self, *, files: int = 0, byte_count: int = 0) -> None:
        self._files += files
        self._bytes += byte_count
        if self._files >= _STAGE_FILES or self._bytes >= _STAGE_BYTES:
            await self.force()

    async def force(self) -> None:
        await self._verify(self._binding, self._repository)
        self._files = 0
        self._bytes = 0


class GitWorkspaceBridge:
    """Import a bounded, race-checked source tree as a Git tree object."""

    def __init__(
        self,
        *,
        git_executable: Path = Path("/usr/bin/git"),
        max_files: int = 4096,
        max_bytes: int = 32 * 1024 * 1024,
        max_file_bytes: int = 8 * 1024 * 1024,
        max_depth: int = 32,
    ) -> None:
        if os.name != "posix":
            raise ValueError("native workspace import is POSIX-only")
        if not isinstance(git_executable, Path) or not git_executable.is_absolute():
            raise ValueError("Git executable must be an absolute Path")
        if (
            any(
                type(value) is not int or value <= 0
                for value in (max_files, max_bytes, max_file_bytes)
            )
            or type(max_depth) is not int
            or max_depth < 0
        ):
            raise ValueError("workspace limits are invalid")
        self._max_file_bytes = min(max_file_bytes, max_bytes)
        try:
            resolved = git_executable.resolve(strict=True)
            info = git_executable.lstat()
        except OSError:
            raise ValueError("Git executable is unavailable") from None
        if resolved != git_executable or not stat.S_ISREG(info.st_mode):
            raise ValueError("Git executable must not be a symlink")
        if not info.st_mode & 0o111 or info.st_mode & 0o022:
            raise ValueError("Git executable is not trusted")
        if info.st_uid not in {os.geteuid(), 0}:
            raise ValueError("Git executable has an unexpected owner")
        self._git = git_executable
        self._max_files = max_files
        self._max_bytes = max_bytes
        self._max_file_bytes = max_file_bytes
        self._max_depth = max_depth

    async def import_tree(
        self,
        binding: WorkspaceBinding,
        repository: Path,
        *,
        verify: Callable[[WorkspaceBinding, Path], Awaitable[None]],
    ) -> WorkspaceTree:
        """Import ``binding.root`` into the approved private bare repository.

        ``verify`` is intentionally required for every call.  The integration
        layer owns the policy for owner, project, environment generation and
        trusted-root authority; a matching dataclass alone is not authority.
        """
        if not isinstance(binding, WorkspaceBinding):
            raise ServiceError("invalid_input", 422)
        if not callable(verify):
            raise ServiceError("invalid_input", 422)
        if not isinstance(repository, Path):
            raise ServiceError("invalid_input", 422)

        await verify(binding, repository)
        root_fd = self._open_bound_root(binding)
        try:
            root = self._canonical_path(binding.root, "workspace root")
            destination = self._validate_repository(repository, root)
            await verify(binding, repository)

            stages = _StageVerifier(binding, repository, verify)
            files, source_state = await self._scan_source(
                root_fd,
                stages,
            )
            await stages.force()
            await verify(binding, repository)

            with self._temporary_index(destination.parent) as temp_dir:
                index_file = Path(temp_dir) / "index"
                await self._import_files(
                    destination,
                    index_file,
                    files,
                    binding,
                    verify,
                    stages,
                )
                await verify(binding, repository)
                tree_id = await self._write_tree(destination, index_file)
                await verify(binding, repository)

                current_state = self._capture_structure(root_fd)
                if current_state != source_state:
                    raise ServiceError("workspace_unavailable", 409)
                await verify(binding, repository)

            # The Git commands may have created object files.  Re-check the
            # destination's trust boundary after the write, before returning.
            self._validate_repository_filesystem(destination)
            return WorkspaceTree(
                tree_id=tree_id,
                file_count=len(files),
                byte_count=sum(len(item.data) for item in files),
            )
        finally:
            try:
                os.close(root_fd)
            except OSError:
                pass

    def _open_bound_root(self, binding: WorkspaceBinding) -> int:
        root = self._canonical_path(binding.root, "workspace root")
        try:
            info = root.lstat()
        except OSError:
            raise ServiceError("workspace_unavailable", 409) from None
        if (
            not stat.S_ISDIR(info.st_mode)
            or type(binding.root_device) is not int
            or type(binding.root_inode) is not int
            or info.st_dev != binding.root_device
            or info.st_ino != binding.root_inode
        ):
            raise ServiceError("permission_denied", 403)
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        fd = os.open("/", flags)
        try:
            for component in root.parts[1:]:
                next_fd = os.open(component, flags, dir_fd=fd)
                os.close(fd)
                fd = next_fd
            opened = os.fstat(fd)
        except OSError:
            os.close(fd)
            raise ServiceError("permission_denied", 403) from None
        if (
            not stat.S_ISDIR(opened.st_mode)
            or opened.st_dev != info.st_dev
            or opened.st_ino != info.st_ino
        ):
            os.close(fd)
            raise ServiceError("workspace_unavailable", 409)
        return fd

    @staticmethod
    def _canonical_path(path: Path, label: str) -> Path:
        if not path.is_absolute():
            raise ServiceError("permission_denied", 403)
        try:
            resolved = path.resolve(strict=True)
        except OSError:
            raise ServiceError("workspace_unavailable", 409) from None
        if resolved != path:
            raise ServiceError("permission_denied", 403)
        return path

    def _validate_repository_filesystem(self, repository: Path) -> None:
        if not repository.is_absolute() or repository.resolve(strict=True) != repository:
            raise ServiceError("permission_denied", 403)
        for current in (repository, *repository.parents):
            try:
                info = current.lstat()
            except OSError:
                raise ServiceError("permission_denied", 403) from None
            if stat.S_ISLNK(info.st_mode):
                raise ServiceError("permission_denied", 403)
        for base, dirs, files in os.walk(repository, topdown=True, followlinks=False):
            base_path = Path(base)
            base_info = base_path.lstat()
            if (
                stat.S_ISLNK(base_info.st_mode)
                or not stat.S_ISDIR(base_info.st_mode)
                or base_info.st_mode & 0o022
                or base_info.st_uid != os.geteuid()
            ):
                raise ServiceError("permission_denied", 403)
            for name in (*dirs, *files):
                candidate = base_path / name
                info = candidate.lstat()
                if (
                    not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode))
                    or info.st_uid != os.geteuid()
                    or info.st_mode & 0o022
                ):
                    raise ServiceError("permission_denied", 403)
        for relative in (
            "objects/info/alternates",
            "objects/info/http-alternates",
            "commondir",
            "config.worktree",
        ):
            candidate = repository / relative
            if candidate.exists() or candidate.is_symlink():
                raise ServiceError("permission_denied", 403)

    def _validate_repository(self, repository: Path, root: Path) -> Path:
        destination = self._canonical_path(repository, "Git repository")
        if destination == root or destination.is_relative_to(root):
            raise ServiceError("permission_denied", 403)
        try:
            self._validate_repository_filesystem(destination)
        except (OSError, ValueError):
            raise ServiceError("permission_denied", 403) from None

        # The runner's filesystem checks reject symlinks and writable foreign
        # entries.  Read the config using the runner's no-includes framing and
        # allowlist before any command can write an object or index.
        config = self._run_git_sync(
            destination,
            ("config", "--local", "--no-includes", "--null", "--list"),
        )
        try:
            entries = TrustedGitRunner._parse_config(config)
        except ServiceError:
            raise ServiceError("permission_denied", 403) from None
        origins = [value for key, value in entries if key == "remote.origin.url"]
        if (
            len(origins) != 1
            or not origins[0]
            or any(ord(char) < 32 or char in "\r\n" for char in origins[0])
        ):
            raise ServiceError("permission_denied", 403)
        try:
            TrustedGitRunner._validate_config(entries, origins[0])
        except ServiceError:
            raise ServiceError("permission_denied", 403) from None

        bare = self._run_git_sync(destination, ("rev-parse", "--is-bare-repository"))
        if bare != b"true\n":
            raise ServiceError("permission_denied", 403)
        object_format = self._run_git_sync(
            destination, ("rev-parse", "--show-object-format=storage")
        )
        try:
            value = object_format.decode("ascii").strip()
        except UnicodeDecodeError:
            raise ServiceError("execution_error", 500) from None
        if value not in {"sha1", "sha256"}:
            raise ServiceError("execution_error", 500)
        return destination

    @staticmethod
    def _fingerprint(info: os.stat_result, *, kind: str | None = None) -> _Fingerprint:
        if kind is None:
            if stat.S_ISDIR(info.st_mode):
                kind = "directory"
            elif stat.S_ISREG(info.st_mode):
                kind = "file"
            elif stat.S_ISLNK(info.st_mode):
                kind = "symlink"
            else:
                kind = "special"
        return _Fingerprint(
            kind=kind,
            device=info.st_dev,
            inode=info.st_ino,
            mode=stat.S_IMODE(info.st_mode),
            size=info.st_size,
            mtime_ns=info.st_mtime_ns,
            ctime_ns=info.st_ctime_ns,
            links=info.st_nlink,
        )

    async def _scan_source(
        self,
        root_fd: int,
        stages: _StageVerifier,
    ) -> tuple[list[_FileSnapshot], dict[bytes, _Fingerprint]]:
        files: list[_FileSnapshot] = []
        state: dict[bytes, _Fingerprint] = {}
        await self._scan_directory(root_fd, b"", 0, files, state, stages)
        return files, state

    async def _scan_directory(
        self,
        directory_fd: int,
        prefix: bytes,
        depth: int,
        files: list[_FileSnapshot],
        state: dict[bytes, _Fingerprint],
        stages: _StageVerifier,
    ) -> None:
        before = self._fstat_or_deny(directory_fd)
        if not stat.S_ISDIR(before.st_mode):
            raise ServiceError("workspace_unavailable", 409)
        before_fp = self._fingerprint(before)
        state[prefix] = before_fp
        names = self._list_directory(directory_fd)
        for name in names:
            component, encoded = self._path_component(name)
            relative = encoded if not prefix else prefix + b"/" + encoded
            if len(relative) > _MAX_PATH_BYTES:
                raise ServiceError("artifact_limit", 413)
            info = self._lstat_at(directory_fd, name)
            self._validate_node(info)
            node_fp = self._fingerprint(info)
            if len(state) >= self._max_files * 4 + 1:
                raise ServiceError("artifact_limit", 413)
            state[relative] = node_fp
            if component.casefold() == ".git":
                # Check the type above, but never open or traverse Git metadata.
                continue
            if stat.S_ISDIR(info.st_mode):
                if depth >= self._max_depth:
                    raise ServiceError("artifact_limit", 413)
                child_fd = self._open_directory_at(directory_fd, name)
                try:
                    child_info = self._fstat_or_deny(child_fd)
                    if self._fingerprint(child_info) != node_fp:
                        raise ServiceError("workspace_unavailable", 409)
                    await self._scan_directory(child_fd, relative, depth + 1, files, state, stages)
                finally:
                    os.close(child_fd)
            else:
                if len(files) >= self._max_files:
                    raise ServiceError("artifact_limit", 413)
                data, final_info = await self._read_file_at(directory_fd, name, info, stages)
                if sum(len(item.data) for item in files) + len(data) > self._max_bytes:
                    raise ServiceError("artifact_limit", 413)
                final_fp = self._fingerprint(final_info)
                if final_fp != node_fp:
                    raise ServiceError("workspace_unavailable", 409)
                state[relative] = final_fp
                files.append(
                    _FileSnapshot(
                        path=relative,
                        data=data,
                        mode=0o755 if final_info.st_mode & 0o111 else 0o644,
                        fingerprint=final_fp,
                    )
                )
                await stages.add(files=1)

        after = self._fstat_or_deny(directory_fd)
        after_fp = self._fingerprint(after)
        if after_fp != before_fp or self._list_directory(directory_fd) != names:
            raise ServiceError("workspace_unavailable", 409)

    def _capture_structure(self, root_fd: int) -> dict[bytes, _Fingerprint]:
        state: dict[bytes, _Fingerprint] = {}
        self._capture_directory(root_fd, b"", 0, state)
        return state

    def _capture_directory(
        self,
        directory_fd: int,
        prefix: bytes,
        depth: int,
        state: dict[bytes, _Fingerprint],
    ) -> None:
        before = self._fstat_or_deny(directory_fd)
        if not stat.S_ISDIR(before.st_mode):
            raise ServiceError("workspace_unavailable", 409)
        before_fp = self._fingerprint(before)
        state[prefix] = before_fp
        names = self._list_directory(directory_fd)
        for name in names:
            component, encoded = self._path_component(name)
            relative = encoded if not prefix else prefix + b"/" + encoded
            if len(relative) > _MAX_PATH_BYTES:
                raise ServiceError("artifact_limit", 413)
            info = self._lstat_at(directory_fd, name)
            self._validate_node(info)
            node_fp = self._fingerprint(info)
            if len(state) >= self._max_files * 4 + 1:
                raise ServiceError("artifact_limit", 413)
            state[relative] = node_fp
            if component.casefold() == ".git":
                continue
            if stat.S_ISDIR(info.st_mode):
                if depth >= self._max_depth:
                    raise ServiceError("artifact_limit", 413)
                child_fd = self._open_directory_at(directory_fd, name)
                try:
                    child_info = self._fstat_or_deny(child_fd)
                    if self._fingerprint(child_info) != node_fp:
                        raise ServiceError("workspace_unavailable", 409)
                    self._capture_directory(child_fd, relative, depth + 1, state)
                finally:
                    os.close(child_fd)
            else:
                # Re-opening a regular file catches a rename/replacement
                # between the directory walk and this verification walk.
                file_fd = self._open_file_at(directory_fd, name)
                try:
                    opened = self._fstat_or_deny(file_fd)
                finally:
                    os.close(file_fd)
                if self._fingerprint(opened) != node_fp:
                    raise ServiceError("workspace_unavailable", 409)

        after = self._fstat_or_deny(directory_fd)
        if self._fingerprint(after) != before_fp or self._list_directory(directory_fd) != names:
            raise ServiceError("workspace_unavailable", 409)

    async def _read_file_at(
        self,
        directory_fd: int,
        name: str,
        expected: os.stat_result,
        stages: _StageVerifier,
    ) -> tuple[bytes, os.stat_result]:
        fd = self._open_file_at(directory_fd, name)
        chunks: list[bytes] = []
        total = 0
        try:
            opened = self._fstat_or_deny(fd)
            if self._fingerprint(opened) != self._fingerprint(expected):
                raise ServiceError("workspace_unavailable", 409)
            if opened.st_size > self._max_file_bytes:
                raise ServiceError("artifact_limit", 413)
            while True:
                try:
                    chunk = os.read(fd, min(1024 * 1024, self._max_file_bytes + 1 - total))
                except OSError:
                    raise ServiceError("workspace_unavailable", 409) from None
                if not chunk:
                    break
                total += len(chunk)
                if total > self._max_file_bytes or total > self._max_bytes:
                    raise ServiceError("artifact_limit", 413)
                chunks.append(chunk)
                await stages.add(byte_count=len(chunk))
            final = self._fstat_or_deny(fd)
            if self._fingerprint(final) != self._fingerprint(expected):
                raise ServiceError("workspace_unavailable", 409)
            return b"".join(chunks), final
        finally:
            os.close(fd)

    @staticmethod
    def _fstat_or_deny(fd: int) -> os.stat_result:
        try:
            return os.fstat(fd)
        except OSError:
            raise ServiceError("workspace_unavailable", 409) from None

    @staticmethod
    def _lstat_at(directory_fd: int, name: str) -> os.stat_result:
        try:
            return os.lstat(name, dir_fd=directory_fd)
        except OSError:
            raise ServiceError("workspace_unavailable", 409) from None

    @staticmethod
    def _list_directory(directory_fd: int) -> list[str]:
        try:
            names = os.listdir(directory_fd)
        except OSError:
            raise ServiceError("workspace_unavailable", 409) from None
        encoded: list[tuple[bytes, str]] = []
        for name in names:
            try:
                raw = os.fsencode(name)
            except UnicodeEncodeError:
                raise ServiceError("permission_denied", 403) from None
            encoded.append((raw, name))
        encoded.sort(key=lambda item: item[0])
        return [name for _raw, name in encoded]

    @staticmethod
    def _path_component(name: str) -> tuple[str, bytes]:
        if name in {"", ".", ".."}:
            raise ServiceError("permission_denied", 403)
        try:
            raw = os.fsencode(name)
            raw.decode("utf-8")
        except (UnicodeDecodeError, UnicodeEncodeError):
            raise ServiceError("permission_denied", 403) from None
        if (
            not raw
            or len(raw) > 255
            or any(byte in raw for byte in (0, 9, 10, 13))
            or any(ord(char) < 32 for char in name)
            or len(raw) > _MAX_PATH_BYTES
        ):
            raise ServiceError("permission_denied", 403)
        return name, raw

    @staticmethod
    def _validate_node(info: os.stat_result) -> None:
        if (
            (stat.S_ISREG(info.st_mode) and info.st_nlink != 1)
            or stat.S_ISLNK(info.st_mode)
            or not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode))
        ):
            raise ServiceError("permission_denied", 403)

    @staticmethod
    def _open_directory_at(directory_fd: int, name: str) -> int:
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        try:
            return os.open(name, flags, dir_fd=directory_fd)
        except OSError:
            raise ServiceError("workspace_unavailable", 409) from None

    @staticmethod
    def _open_file_at(directory_fd: int, name: str) -> int:
        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
        try:
            return os.open(name, flags, dir_fd=directory_fd)
        except OSError:
            raise ServiceError("workspace_unavailable", 409) from None

    async def _import_files(
        self,
        repository: Path,
        index_file: Path,
        files: list[_FileSnapshot],
        binding: WorkspaceBinding,
        verify: Callable[[WorkspaceBinding, Path], Awaitable[None]],
        stages: _StageVerifier,
    ) -> None:
        await verify(binding, repository)
        await self._run_git(
            repository,
            ("read-tree", "--empty"),
            index_file=index_file,
        )
        records: list[bytes] = []
        for item in sorted(files, key=lambda value: value.path):
            raw_oid = await self._run_git(
                repository,
                ("hash-object", "--stdin", "--no-filters", "-w"),
                input_bytes=item.data,
                index_file=index_file,
            )
            oid = self._parse_oid(raw_oid)
            mode = b"100755" if item.mode == 0o755 else b"100644"
            records.append(mode + b" " + oid.encode("ascii") + b" 0\t" + item.path + b"\0")
            await stages.add(files=1, byte_count=len(item.data))
        await stages.force()
        await verify(binding, repository)
        if records:
            await self._run_git(
                repository,
                ("update-index", "-z", "--index-info"),
                input_bytes=b"".join(records),
                index_file=index_file,
            )
        await verify(binding, repository)

    async def _write_tree(self, repository: Path, index_file: Path) -> str:
        raw = await self._run_git(repository, ("write-tree",), index_file=index_file)
        return self._parse_oid(raw)

    @staticmethod
    def _parse_oid(raw: bytes) -> str:
        try:
            text = raw.decode("ascii")
        except UnicodeDecodeError:
            raise ServiceError("execution_error", 500) from None
        if not text.endswith("\n"):
            raise ServiceError("execution_error", 500)
        value = text[:-1]
        if len(value) not in {40, 64} or _GIT_ID.fullmatch(value) is None:
            raise ServiceError("execution_error", 500)
        return value

    @staticmethod
    def _temporary_index(parent: Path) -> tempfile.TemporaryDirectory[str]:
        try:
            temp_root = parent.resolve(strict=True)
            info = parent.lstat()
        except OSError:
            raise ServiceError("execution_error", 500) from None
        if (
            temp_root != parent
            or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_mode & 0o022
        ):
            raise ServiceError("permission_denied", 403)
        try:
            return tempfile.TemporaryDirectory(
                prefix="krim-git-workspace-", dir=str(parent), ignore_cleanup_errors=False
            )
        except OSError:
            raise ServiceError("execution_error", 500) from None

    def _git_environment(self, *, index_file: Path | None) -> dict[str, str]:
        environment = {
            "PATH": f"{self._git.parent}:/usr/bin:/bin",
            "HOME": "/tmp",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_SYSTEM": "/dev/null",
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_ASKPASS": "/bin/false",
            "SSH_ASKPASS": "/bin/false",
            "GIT_SSH_COMMAND": "/bin/false",
            "GIT_EDITOR": "/bin/false",
            "VISUAL": "/bin/false",
            "EDITOR": "/bin/false",
            "GIT_PAGER": "cat",
            "PAGER": "cat",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_PROTOCOL_FROM_USER": "0",
            "LC_ALL": "C",
            "LANG": "C",
            "NO_COLOR": "1",
        }
        if index_file is not None:
            environment["GIT_INDEX_FILE"] = str(index_file)
        return environment

    def _git_argv(self, repository: Path, args: Iterable[str]) -> tuple[str, ...]:
        values = tuple(args)
        if any("\x00" in value for value in values):
            raise ServiceError("invalid_input", 422)
        return (
            str(self._git),
            "--no-pager",
            "--no-optional-locks",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "credential.helper=",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "diff.external=",
            "-c",
            "protocol.allow=never",
            "--git-dir",
            str(repository),
            *values,
        )

    def _run_git_sync(self, repository: Path, args: Iterable[str]) -> bytes:
        argv = self._git_argv(repository, args)
        try:
            result = subprocess.run(
                argv,
                cwd="/",
                env=self._git_environment(index_file=None),
                input=b"",
                capture_output=True,
                check=False,
                shell=False,
                timeout=30,
            )
        except (OSError, subprocess.TimeoutExpired):
            raise ServiceError("execution_error", 500) from None
        if len(result.stdout) + len(result.stderr) > _MAX_OUTPUT_BYTES:
            raise ServiceError("execution_error", 500)
        if result.returncode != 0:
            raise ServiceError("permission_denied", 403)
        return result.stdout

    async def _run_git(
        self,
        repository: Path,
        args: Iterable[str],
        *,
        input_bytes: bytes = b"",
        index_file: Path,
    ) -> bytes:
        if len(input_bytes) > self._max_bytes + _MAX_PATH_BYTES:
            raise ServiceError("artifact_limit", 413)
        argv = self._git_argv(repository, args)
        environment = self._git_environment(index_file=index_file)

        creation = asyncio.create_task(
            asyncio.create_subprocess_exec(
                *argv,
                cwd="/",
                env=environment,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                start_new_session=True,
            )
        )
        cancelled = False
        while not creation.done():
            try:
                await asyncio.shield(creation)
            except asyncio.CancelledError:
                cancelled = True
        process = creation.result()
        try:
            if cancelled:
                raise asyncio.CancelledError
            stdout, _ = await asyncio.wait_for(process.communicate(input_bytes), 30)
            if process.returncode or len(stdout) > _MAX_OUTPUT_BYTES:
                raise ServiceError("execution_error", 500)
            return stdout
        finally:
            if process.returncode is None:
                process.kill()
            await process.wait()
