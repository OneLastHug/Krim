"""Host-only, structured native Git execution.

This module is deliberately not a shell adapter.  The request is bound to the
project-access intent, the repository is selected from the trusted host root,
and every Git process is synchronously accepted by the access service before it
is spawned.  Waiting for a process happens after that acceptance point.
"""

from __future__ import annotations

import asyncio
import hashlib
import math
import os
import re
import selectors
import shlex
import shutil
import signal
import stat
import subprocess
import threading
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, Protocol, cast

from pydantic import Field, field_validator, model_validator

from krim_agent.contracts import ServiceError, StrictModel, parse_json
from krim_agent.development.contracts import AuthorizationRef as Ref
from krim_agent.development.contracts import Identifier
from krim_agent.project_access.git import GitAction, GitIdentity, GitOperation
from krim_agent.project_access.secrets import DeliveryBinding
from krim_agent.project_access.types import AccessIntent

if TYPE_CHECKING:
    from krim_agent.project_access.service import ProjectAccessService


_OID = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_MAX_MESSAGE = 64 * 1024
_MAX_OUTPUT = 64 * 1024
_ZERO = {"0" * 40, "0" * 64}


class GitExecutionRequest(StrictModel):
    """The complete, model-independent shape of one native Git operation."""

    git: GitOperation
    tree_id: str | None = Field(default=None, pattern=r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
    message: str | None = Field(default=None, max_length=_MAX_MESSAGE)

    @field_validator("tree_id")
    @classmethod
    def nonzero_tree(cls, value: str | None) -> str | None:
        if value is not None and value in _ZERO:
            raise ValueError("tree object is required")
        return value

    @field_validator("message")
    @classmethod
    def safe_message(cls, value: str | None) -> str | None:
        if value is not None and ("\x00" in value or len(value.encode("utf-8")) > _MAX_MESSAGE):
            raise ValueError("invalid commit message")
        return value

    @model_validator(mode="after")
    def operation_shape(self) -> GitExecutionRequest:
        operation = self.git
        action = operation.action
        if action == "git.clone":
            if operation.object_id is None or operation.expected_old_id is not None:
                raise ValueError("clone requires only the received object")
            if self.tree_id is not None or self.message is not None:
                raise ValueError("clone has no extra options")
        elif action == "git.push":
            if operation.object_id is None or operation.expected_old_id is None:
                raise ValueError("push requires an object and lease")
            if operation.object_id in _ZERO:
                raise ValueError("push object is required")
            if len(operation.object_id) != len(operation.expected_old_id):
                raise ValueError("Git object identifiers must use one format")
            if self.tree_id is not None or self.message is not None:
                raise ValueError("push has no extra options")
        elif action == "git.diff":
            if operation.object_id is None or operation.expected_old_id is None:
                raise ValueError("diff requires two objects")
            if operation.object_id in _ZERO or operation.expected_old_id in _ZERO:
                raise ValueError("diff objects are required")
            if len(operation.object_id) != len(operation.expected_old_id):
                raise ValueError("Git object identifiers must use one format")
            if self.tree_id is not None or self.message is not None:
                raise ValueError("diff has no extra options")
        elif action == "git.commit":
            if (
                self.tree_id is None
                or operation.expected_old_id is None
                or self.message is None
                or operation.object_id is not None
            ):
                raise ValueError("commit requires tree, lease and message")
            if len(self.tree_id) != len(operation.expected_old_id):
                raise ValueError("Git object identifiers must use one format")
        else:  # GitOperation currently makes this unreachable; keep the boundary closed.
            raise ValueError("unsupported Git operation")
        return self


class GitExecutionResult(StrictModel):
    operation_id: Identifier
    action: GitAction
    object_id: str | None = Field(default=None, pattern=r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
    changed: bool


class GitTransport(Protocol):
    """Trusted network lease adapter; never supplied by model-facing code."""

    def spawn(
        self,
        operation: GitOperation,
        argv: tuple[str, ...],
        *,
        cwd: Path,
        env: dict[str, str],
        stdin: int,
        stdout: int,
        stderr: int,
    ) -> subprocess.Popen[bytes]: ...


class _GitAccess(Protocol):
    async def accept_git(
        self,
        authorization: Ref,
        intent: AccessIntent,
        accept: Callable[[GitIdentity], None],
        *,
        credential_binding: DeliveryBinding | None = None,
    ) -> object: ...


@dataclass(frozen=True, slots=True)
class _Completed:
    returncode: int
    stdout: bytes
    stderr: bytes


class _BoundedOutput(Exception):
    pass


class TrustedGitRunner:
    """Execute exact Git operations in a runner-owned private root.

    Bare repositories are host-staged/imported metadata.  Ordinary Shell never
    writes this root, and D2 exposes no arbitrary path or import operation.
    """

    def __init__(
        self,
        root: Path,
        *,
        git_executable: Path = Path("/usr/bin/git"),
        timeout: float = 30,
    ) -> None:
        if os.name != "posix":
            raise ValueError("native Git execution is POSIX-only")
        if not root.is_absolute() or not git_executable.is_absolute():
            raise ValueError("trusted Git paths must be absolute")
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("Git timeout must be positive")
        if root.resolve() != root:
            raise ServiceError("permission_denied", 403)
        self._root = root
        self._git = git_executable
        self._timeout = timeout
        self._locks: dict[Path, asyncio.Lock] = {}
        self._active_lock = threading.Lock()
        self._active: dict[tuple[str, str], set[subprocess.Popen[bytes]]] = {}
        self._prepare_root()
        self._template = self._private_dir(root / ".git-template")
        self._home = self._private_dir(root / ".home")
        self._xdg = self._private_dir(root / ".config")
        self._tmp = self._private_dir(root / ".tmp")
        self._validate_executable()
        self._base_env = self._make_base_env()

    @property
    def root(self) -> Path:
        """The trusted root, for host diagnostics only; never a model value."""
        return self._root

    def invalidate(self, owner_id: str, project_id: str) -> None:
        """Kill accepted processes for one exact project scope."""
        scope = (self._safe_component(owner_id), self._safe_component(project_id))
        with self._active_lock:
            processes = tuple(self._active.get(scope, ()))
        for process in processes:
            _kill_process(process)

    async def execute(
        self,
        intent: AccessIntent,
        authorization: Ref,
        request: GitExecutionRequest,
        access: ProjectAccessService,
        *,
        credential_helper: tuple[str, ...] = (),
        credential_binding: DeliveryBinding | None = None,
        transport: GitTransport | None = None,
    ) -> GitExecutionResult:
        """Run one exact request after binding it to the supplied intent.

        The access callback is the only place where a Popen is created.  This
        method never mounts ``root`` into a normal shell and never accepts a
        caller-provided path, argv or environment.
        """
        if not isinstance(request, GitExecutionRequest):
            raise ServiceError("invalid_input", 422)
        bound = self._bound_request(intent, request)
        self._validate_credentials(
            intent, authorization, bound, credential_helper, credential_binding
        )
        operation = bound.git
        if operation.action in ("git.clone", "git.push") and transport is None:
            raise ServiceError("permission_denied", 403)
        if operation.action not in ("git.clone", "git.push") and transport is not None:
            # A network lease must not silently become a local command hook.
            raise ServiceError("invalid_input", 422)

        repository = self._repository_path(intent, operation)
        lock = self._locks.setdefault(repository, asyncio.Lock())
        async with lock:
            if operation.action == "git.clone":
                return await self._clone(
                    intent,
                    authorization,
                    bound,
                    access,
                    credential_helper,
                    credential_binding,
                    transport,
                    repository,
                )
            if operation.action == "git.push":
                return await self._push(
                    intent,
                    authorization,
                    bound,
                    access,
                    credential_helper,
                    credential_binding,
                    transport,
                    repository,
                )
            if operation.action == "git.diff":
                return await self._diff(
                    intent, authorization, bound, access, credential_binding, repository
                )
            return await self._commit(
                intent, authorization, bound, access, credential_binding, repository
            )

    def _bound_request(
        self, intent: AccessIntent, request: GitExecutionRequest
    ) -> GitExecutionRequest:
        try:
            payload = parse_json(intent.arguments_json)
            if not isinstance(payload, dict):
                raise ValueError
            bound = GitExecutionRequest.model_validate(payload)
        except Exception:
            raise ServiceError("invalid_input", 422) from None
        if bound != request or bound.git.action != intent.action:
            raise ServiceError("permission_denied", 403)
        return bound

    @staticmethod
    def _validate_credentials(
        intent: AccessIntent,
        authorization: Ref,
        request: GitExecutionRequest,
        helpers: tuple[str, ...],
        binding: DeliveryBinding | None,
    ) -> None:
        if not isinstance(helpers, tuple) or any(
            not isinstance(helper, str)
            or not helper
            or "\x00" in helper
            or "\r" in helper
            or "\n" in helper
            or len(helper) > 1024
            for helper in helpers
        ):
            raise ServiceError("invalid_input", 422)
        if helpers:
            if request.git.action not in ("git.clone", "git.push") or binding is None:
                raise ServiceError("permission_denied", 403)
            if binding.intent != intent or binding.authorization != authorization:
                raise ServiceError("permission_denied", 403)
        elif binding is not None:
            raise ServiceError("invalid_input", 422)
        if helpers:
            # Git's helper setting can run arbitrary shell programs. Accept
            # only the native operation-private cache produced by our adapter.
            try:
                if len(helpers) != 1:
                    raise ValueError
                parts = shlex.split(helpers[0])
                if (
                    len(parts) != 3
                    or parts[0] != "cache"
                    or not re.fullmatch(r"--timeout=[0-9]{1,3}", parts[1])
                    or not 1 <= int(parts[1].partition("=")[2]) <= 300
                    or not parts[2].startswith("--socket=/")
                ):
                    raise ValueError
                socket = Path(parts[2].partition("=")[2])
                if socket.name != "socket" or socket.resolve() != socket:
                    raise ValueError
                TrustedGitRunner._secure_dir(socket.parent, private=True)
                canonical = f"cache {parts[1]} --socket={shlex.quote(str(socket))}"
                if helpers[0] != canonical:
                    raise ValueError
            except (ValueError, OSError):
                raise ServiceError("invalid_input", 422) from None

    def _repository_path(self, intent: AccessIntent, operation: GitOperation) -> Path:
        digest = hashlib.sha256(operation.repository.encode("ascii")).hexdigest()
        owner = self._safe_component(intent.owner_id)
        project = self._safe_component(intent.project_id)
        owner_dir = self._ensure_scope_dir(self._root / owner)
        project_dir = self._ensure_scope_dir(owner_dir / project)
        return project_dir / f"{digest}.git"

    @staticmethod
    def _safe_component(value: str) -> str:
        if value in {".", ".."} or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value):
            raise ServiceError("invalid_input", 422)
        return value

    def _prepare_root(self) -> None:
        if self._root.exists() or self._root.is_symlink():
            self._secure_dir(self._root, private=True)
            return
        parent = self._root.parent
        self._secure_dir(parent, private=False)
        try:
            self._root.mkdir(mode=0o700)
        except FileExistsError:
            pass
        self._secure_dir(self._root, private=True)

    def _private_dir(self, path: Path) -> Path:
        if path.exists() or path.is_symlink():
            self._secure_dir(path, private=True)
            return path
        try:
            path.mkdir(mode=0o700)
        except FileExistsError:
            pass
        self._secure_dir(path, private=True)
        return path

    def _ensure_scope_dir(self, path: Path) -> Path:
        if path.exists() or path.is_symlink():
            self._secure_dir(path, private=True)
        else:
            try:
                path.mkdir(mode=0o700)
            except FileExistsError:
                pass
            self._secure_dir(path, private=True)
        return path

    @staticmethod
    def _secure_dir(path: Path, *, private: bool) -> None:
        try:
            info = path.lstat()
        except OSError:
            raise ServiceError("execution_error", 500) from None
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise ServiceError("permission_denied", 403)
        if info.st_uid != os.geteuid() if private else info.st_uid not in {os.geteuid(), 0}:
            raise ServiceError("permission_denied", 403)
        if info.st_mode & 0o022:
            raise ServiceError("permission_denied", 403)
        if private and info.st_mode & 0o077:
            raise ServiceError("permission_denied", 403)

    def _validate_executable(self) -> None:
        try:
            info = self._git.lstat()
        except OSError:
            raise ValueError("Git executable is unavailable") from None
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISREG(info.st_mode)
            or not info.st_mode & 0o111
            or info.st_mode & 0o022
            or info.st_uid not in {os.geteuid(), 0}
        ):
            raise ValueError("Git executable is not trusted")

    def _make_base_env(self) -> dict[str, str]:
        path_parts = {str(self._git.parent), "/usr/bin", "/bin"}
        return {
            "PATH": ":".join(sorted(path_parts)),
            "HOME": str(self._home),
            "XDG_CONFIG_HOME": str(self._xdg),
            "TMPDIR": str(self._tmp),
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_SYSTEM": "/dev/null",
            "GIT_TEMPLATE_DIR": str(self._template),
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_ASKPASS": "/bin/false",
            "SSH_ASKPASS": "/bin/false",
            "GIT_SSH_COMMAND": "/bin/false",
            "GIT_PAGER": "cat",
            "PAGER": "cat",
            "GIT_EDITOR": "/bin/false",
            "VISUAL": "/bin/false",
            "EDITOR": "/bin/false",
            "GIT_FLUSH": "1",
            "GIT_PROTOCOL_FROM_USER": "0",
            "GIT_ALLOW_PROTOCOL": "https",
            "GIT_CEILING_DIRECTORIES": str(self._root),
            "LC_ALL": "C",
            "LANG": "C",
            "NO_COLOR": "1",
        }

    def _git_argv(self, args: Iterable[str], helpers: tuple[str, ...] = ()) -> tuple[str, ...]:
        fixed = [
            str(self._git),
            "--no-pager",
            "--no-optional-locks",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "credential.helper=",
            "-c",
            "credential.useHttpPath=true",
            "-c",
            "commit.gpgsign=false",
            "-c",
            "tag.gpgSign=false",
            "-c",
            "protocol.allow=never",
            "-c",
            "protocol.https.allow=always",
            "-c",
            "protocol.file.allow=never",
            "-c",
            "protocol.git.allow=never",
            "-c",
            "protocol.ssh.allow=never",
            "-c",
            "http.followRedirects=false",
            "-c",
            "submodule.recurse=false",
            "-c",
            "diff.external=",
        ]
        for helper in helpers:
            fixed.extend(("-c", f"credential.helper={helper}"))
        values = tuple(args)
        if any(not isinstance(value, str) or "\x00" in value for value in values):
            raise ServiceError("invalid_input", 422)
        return tuple(fixed) + values

    def _register(self, intent: AccessIntent, process: subprocess.Popen[bytes]) -> None:
        scope = (intent.owner_id, intent.project_id)
        with self._active_lock:
            self._active.setdefault(scope, set()).add(process)

    def _unregister(self, intent: AccessIntent, process: subprocess.Popen[bytes]) -> None:
        scope = (intent.owner_id, intent.project_id)
        with self._active_lock:
            processes = self._active.get(scope)
            if processes is None:
                return
            processes.discard(process)
            if not processes:
                del self._active[scope]

    async def _spawn(
        self,
        intent: AccessIntent,
        authorization: Ref,
        operation: GitOperation,
        access: _GitAccess,
        args_builder: Callable[[GitIdentity], Sequence[str]],
        *,
        cwd: Path,
        env_builder: Callable[[GitIdentity], dict[str, str]] | None = None,
        credential_binding: DeliveryBinding | None,
        transport: GitTransport | None,
    ) -> subprocess.Popen[bytes]:
        holder: list[subprocess.Popen[bytes]] = []

        def accept(identity: GitIdentity) -> None:
            args = tuple(args_builder(identity))
            if any("\x00" in value for value in args):
                raise ValueError("invalid Git argument")
            if env_builder is None:
                environment = dict(self._base_env)
            else:
                environment = dict(self._base_env)
                environment.update(env_builder(identity))
            if transport is None:
                process = subprocess.Popen(
                    args,
                    cwd=str(cwd),
                    env=environment,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    shell=False,
                    close_fds=True,
                    start_new_session=True,
                )
            else:
                process = transport.spawn(
                    operation,
                    args,
                    cwd=cwd,
                    env=environment,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
            self._register(intent, process)
            holder.append(process)

        try:
            await access.accept_git(
                authorization,
                intent,
                accept,
                credential_binding=credential_binding,
            )
        except BaseException:
            for process in holder:
                _kill_process(process)
            for process in holder:
                await asyncio.to_thread(_wait_quietly, process)
                self._unregister(intent, process)
            raise
        if len(holder) != 1:
            for process in holder:
                _kill_process(process)
                await asyncio.to_thread(_wait_quietly, process)
                self._unregister(intent, process)
            raise ServiceError("execution_error", 500)
        return holder[0]

    async def _run(
        self,
        intent: AccessIntent,
        authorization: Ref,
        operation: GitOperation,
        access: _GitAccess,
        args_builder: Callable[[GitIdentity], Sequence[str]],
        *,
        cwd: Path,
        input_bytes: bytes = b"",
        env_builder: Callable[[GitIdentity], dict[str, str]] | None = None,
        allowed: frozenset[int] = frozenset({0}),
        credential_binding: DeliveryBinding | None,
        transport: GitTransport | None = None,
    ) -> _Completed:
        process = await self._spawn(
            intent,
            authorization,
            operation,
            access,
            args_builder,
            cwd=cwd,
            env_builder=env_builder,
            credential_binding=credential_binding,
            transport=transport,
        )
        completed = await self._wait(
            intent,
            process,
            input_bytes,
        )
        if completed.returncode not in allowed:
            raise ServiceError("execution_error", 500)
        return completed

    async def _wait(
        self,
        intent: AccessIntent,
        process: subprocess.Popen[bytes],
        input_bytes: bytes,
    ) -> _Completed:
        task = asyncio.create_task(
            asyncio.to_thread(_communicate_bounded, process, input_bytes, _MAX_OUTPUT)
        )
        try:
            try:
                return await asyncio.wait_for(asyncio.shield(task), timeout=self._timeout)
            except TimeoutError:
                _kill_process(process)
                await _reap(task)
                raise ServiceError("execution_error", 504) from None
            except asyncio.CancelledError:
                _kill_process(process)
                await _reap(task)
                raise
            except _BoundedOutput:
                _kill_process(process)
                await _reap(task)
                raise ServiceError("execution_error", 413) from None
            except Exception:
                _kill_process(process)
                await _reap(task)
                raise ServiceError("execution_error", 500) from None
        finally:
            self._unregister(intent, process)

    async def _validate_repo(
        self,
        intent: AccessIntent,
        authorization: Ref,
        operation: GitOperation,
        access: _GitAccess,
        repository: Path,
        credential_binding: DeliveryBinding | None,
    ) -> int:
        self._secure_repository_path(repository)
        config_path = repository / "config"
        self._secure_file(config_path)
        for relative in (
            "objects/info/alternates",
            "objects/info/http-alternates",
            "commondir",
            "config.worktree",
        ):
            candidate = repository / relative
            if candidate.exists() or candidate.is_symlink():
                raise ServiceError("permission_denied", 403)

        config = await self._run(
            intent,
            authorization,
            operation,
            access,
            lambda _identity: self._git_argv(
                ("config", "--local", "--no-includes", "--null", "--list")
            ),
            cwd=repository,
            credential_binding=credential_binding,
        )
        entries = self._parse_config(config.stdout)
        self._validate_config(entries, operation.repository)

        bare = await self._run(
            intent,
            authorization,
            operation,
            access,
            lambda _identity: self._git_argv(("rev-parse", "--is-bare-repository")),
            cwd=repository,
            credential_binding=credential_binding,
        )
        if bare.stdout != b"true\n":
            raise ServiceError("permission_denied", 403)
        object_format = await self._run(
            intent,
            authorization,
            operation,
            access,
            lambda _identity: self._git_argv(("rev-parse", "--show-object-format=storage")),
            cwd=repository,
            credential_binding=credential_binding,
        )
        try:
            value = object_format.stdout.decode("ascii").strip()
        except UnicodeDecodeError:
            raise ServiceError("execution_error", 500) from None
        if value not in {"sha1", "sha256"}:
            raise ServiceError("execution_error", 500)
        return 40 if value == "sha1" else 64

    @staticmethod
    def _parse_config(raw: bytes) -> list[tuple[str, str]]:
        if len(raw) > _MAX_OUTPUT or not raw.endswith(b"\0"):
            raise ServiceError("permission_denied", 403)
        entries: list[tuple[str, str]] = []
        try:
            for record in raw[:-1].split(b"\0"):
                key, separator, value = record.partition(b"\n")
                if not separator or not key or b"\0" in value:
                    raise ValueError
                entries.append((key.decode("ascii").lower(), value.decode("utf-8")))
        except (UnicodeDecodeError, ValueError):
            raise ServiceError("permission_denied", 403) from None
        return entries

    @staticmethod
    def _validate_config(entries: list[tuple[str, str]], expected_origin: str) -> None:
        allowed = {
            "core.repositoryformatversion",
            "core.filemode",
            "core.bare",
            "core.logallrefupdates",
            "core.symlinks",
            "remote.origin.url",
            "remote.origin.fetch",
            "remote.origin.tagopt",
            "extensions.objectformat",
        }
        if any(key not in allowed for key, _value in entries):
            raise ServiceError("permission_denied", 403)
        values: dict[str, list[str]] = {}
        for key, value in entries:
            values.setdefault(key, []).append(value)
        try:
            if values.get("remote.origin.url") != [expected_origin]:
                raise ValueError
            if values.get("core.bare") != ["true"]:
                raise ValueError
            if values.get("core.repositoryformatversion", ["0"])[0] not in {"0", "1"}:
                raise ValueError
            for key in ("core.filemode", "core.logallrefupdates", "core.symlinks"):
                if key in values and any(value not in {"true", "false"} for value in values[key]):
                    raise ValueError
            if "remote.origin.fetch" in values and any(
                value
                not in {
                    "+refs/heads/*:refs/heads/*",
                    "+refs/*:refs/*",
                }
                for value in values["remote.origin.fetch"]
            ):
                raise ValueError
            if "remote.origin.tagopt" in values and values["remote.origin.tagopt"] != ["--no-tags"]:
                raise ValueError
            if "extensions.objectformat" in values and values["extensions.objectformat"] != [
                "sha256"
            ]:
                raise ValueError
        except (ValueError, KeyError):
            raise ServiceError("permission_denied", 403) from None

    def _secure_repository_path(self, repository: Path) -> None:
        if not repository.is_absolute():
            raise ServiceError("permission_denied", 403)
        self._secure_dir(repository, private=False)
        for base, dirs, files in os.walk(repository, topdown=True, followlinks=False):
            base_path = Path(base)
            self._secure_dir(base_path, private=False)
            for name in (*dirs, *files):
                candidate = base_path / name
                try:
                    info = candidate.lstat()
                except OSError:
                    raise ServiceError("permission_denied", 403) from None
                if (
                    stat.S_ISLNK(info.st_mode)
                    or info.st_uid != os.geteuid()
                    or info.st_mode & 0o022
                ):
                    raise ServiceError("permission_denied", 403)
        config_path = repository / "config"
        self._secure_file(config_path)
        for relative in (
            "objects/info/alternates",
            "objects/info/http-alternates",
            "commondir",
            "config.worktree",
        ):
            candidate = repository / relative
            if candidate.exists() or candidate.is_symlink():
                raise ServiceError("permission_denied", 403)

    @staticmethod
    def _secure_file(path: Path) -> None:
        try:
            info = path.lstat()
        except OSError:
            raise ServiceError("permission_denied", 403) from None
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_mode & 0o022
        ):
            raise ServiceError("permission_denied", 403)

    def _reserve_clone(self, repository: Path) -> os.stat_result:
        if repository.exists() or repository.is_symlink():
            raise ServiceError("permission_denied", 403)
        scope = repository.parent
        self._secure_dir(scope, private=True)
        try:
            repository.mkdir(mode=0o700)
        except FileExistsError:
            raise ServiceError("permission_denied", 403) from None
        return repository.lstat()

    async def _clone(
        self,
        intent: AccessIntent,
        authorization: Ref,
        request: GitExecutionRequest,
        access: _GitAccess,
        helpers: tuple[str, ...],
        binding: DeliveryBinding | None,
        transport: GitTransport | None,
        repository: Path,
    ) -> GitExecutionResult:
        operation = request.git
        assert operation.object_id is not None
        assert transport is not None
        created = await asyncio.to_thread(self._reserve_clone, repository)
        branch = self._clone_branch(operation.ref)

        def clone_args(_identity: GitIdentity) -> tuple[str, ...]:
            return self._git_argv(
                (
                    "clone",
                    "--bare",
                    "--single-branch",
                    "--no-tags",
                    "--no-recurse-submodules",
                    "--branch",
                    branch,
                    operation.repository,
                    str(repository),
                ),
                helpers,
            )

        try:
            result = await self._run(
                intent,
                authorization,
                operation,
                access,
                clone_args,
                cwd=repository.parent,
                credential_binding=binding,
                transport=transport,
            )
            del result
            await self._run(
                intent,
                authorization,
                operation,
                access,
                lambda _identity: self._git_argv(
                    (
                        "config",
                        "--local",
                        "--no-includes",
                        "remote.origin.url",
                        operation.repository,
                    )
                ),
                cwd=repository,
                credential_binding=binding,
            )
            length = await self._validate_repo(
                intent, authorization, operation, access, repository, binding
            )
            if len(operation.object_id) != length:
                raise ServiceError("permission_denied", 403)
            ref_result = await self._run(
                intent,
                authorization,
                operation,
                access,
                lambda _identity: self._git_argv(("rev-parse", "--verify", operation.ref)),
                cwd=repository,
                credential_binding=binding,
            )
            actual = self._parse_oid(ref_result.stdout)
            if actual != operation.object_id:
                raise ServiceError("input_revision_conflict", 409)
            return GitExecutionResult(
                operation_id=intent.operation_id,
                action=operation.action,
                object_id=operation.object_id,
                changed=True,
            )
        except BaseException:
            self._remove_created_clone(repository, created)
            raise

    async def _push(
        self,
        intent: AccessIntent,
        authorization: Ref,
        request: GitExecutionRequest,
        access: _GitAccess,
        helpers: tuple[str, ...],
        binding: DeliveryBinding | None,
        transport: GitTransport | None,
        repository: Path,
    ) -> GitExecutionResult:
        operation = request.git
        assert operation.object_id is not None and operation.expected_old_id is not None
        assert transport is not None
        length = await self._validate_repo(
            intent, authorization, operation, access, repository, binding
        )
        if len(operation.object_id) != length or len(operation.expected_old_id) != length:
            raise ServiceError("permission_denied", 403)
        await self._ensure_object(
            intent, authorization, operation, access, repository, operation.object_id, binding
        )
        # Native force-with-lease does not reject a stale expectation when
        # the requested OID is already remote (Git calls that a no-op). Check
        # the exact advertisement too; the push lease still fences any later
        # remote update atomically at receive-pack.
        advertised = await self._run(
            intent,
            authorization,
            operation,
            access,
            lambda _identity: self._git_argv(
                ("ls-remote", "--refs", operation.repository, operation.ref), helpers
            ),
            cwd=repository,
            credential_binding=binding,
            transport=transport,
        )
        expected = (
            b""
            if operation.expected_old_id in _ZERO
            else f"{operation.expected_old_id}\t{operation.ref}\n".encode("ascii")
        )
        if advertised.stdout != expected:
            raise ServiceError("input_revision_conflict", 409)
        result = await self._run(
            intent,
            authorization,
            operation,
            access,
            lambda _identity: self._git_argv(
                (
                    "push",
                    "--porcelain",
                    f"--force-with-lease={operation.ref}:{operation.expected_old_id}",
                    operation.repository,
                    f"{operation.object_id}:{operation.ref}",
                ),
                helpers,
            ),
            cwd=repository,
            allowed=frozenset({0, 1}),
            credential_binding=binding,
            transport=transport,
        )
        if result.returncode != 0:
            if self._push_rejected(result.stdout + result.stderr):
                raise ServiceError("input_revision_conflict", 409)
            raise ServiceError("execution_error", 500)
        changed = self._push_changed(result.stdout)
        return GitExecutionResult(
            operation_id=intent.operation_id,
            action=operation.action,
            object_id=operation.object_id,
            changed=changed,
        )

    async def _diff(
        self,
        intent: AccessIntent,
        authorization: Ref,
        request: GitExecutionRequest,
        access: _GitAccess,
        binding: DeliveryBinding | None,
        repository: Path,
    ) -> GitExecutionResult:
        operation = request.git
        assert operation.object_id is not None and operation.expected_old_id is not None
        length = await self._validate_repo(
            intent, authorization, operation, access, repository, binding
        )
        if len(operation.object_id) != length or len(operation.expected_old_id) != length:
            raise ServiceError("permission_denied", 403)
        await self._ensure_diff_object(
            intent, authorization, operation, access, repository, operation.expected_old_id, binding
        )
        await self._ensure_diff_object(
            intent, authorization, operation, access, repository, operation.object_id, binding
        )
        result = await self._run(
            intent,
            authorization,
            operation,
            access,
            lambda _identity: self._git_argv(
                (
                    "diff",
                    "--no-ext-diff",
                    "--no-textconv",
                    "--quiet",
                    cast(str, operation.expected_old_id),
                    cast(str, operation.object_id),
                    "--",
                )
            ),
            cwd=repository,
            allowed=frozenset({0, 1}),
            credential_binding=binding,
        )
        return GitExecutionResult(
            operation_id=intent.operation_id,
            action=operation.action,
            object_id=operation.object_id,
            changed=result.returncode == 1,
        )

    async def _commit(
        self,
        intent: AccessIntent,
        authorization: Ref,
        request: GitExecutionRequest,
        access: _GitAccess,
        binding: DeliveryBinding | None,
        repository: Path,
    ) -> GitExecutionResult:
        operation = request.git
        assert request.tree_id is not None and operation.expected_old_id is not None
        length = await self._validate_repo(
            intent, authorization, operation, access, repository, binding
        )
        if len(request.tree_id) != length or len(operation.expected_old_id) != length:
            raise ServiceError("permission_denied", 403)
        await self._ensure_type(
            intent, authorization, operation, access, repository, request.tree_id, "tree", binding
        )
        if operation.expected_old_id not in _ZERO:
            await self._ensure_type(
                intent,
                authorization,
                operation,
                access,
                repository,
                operation.expected_old_id,
                "commit",
                binding,
            )
        assert request.message is not None
        commit_args = ["commit-tree", request.tree_id]
        if operation.expected_old_id not in _ZERO:
            commit_args.extend(("-p", operation.expected_old_id))
        commit = await self._run(
            intent,
            authorization,
            operation,
            access,
            lambda _identity: self._git_argv(commit_args),
            cwd=repository,
            input_bytes=request.message.encode("utf-8"),
            env_builder=lambda identity: self._identity_env(identity),
            credential_binding=binding,
        )
        object_id = self._parse_oid(commit.stdout)
        if len(object_id) != length:
            raise ServiceError("execution_error", 500)
        update = await self._run(
            intent,
            authorization,
            operation,
            access,
            lambda _identity: self._git_argv(
                (
                    "update-ref",
                    "--no-deref",
                    operation.ref,
                    object_id,
                    cast(str, operation.expected_old_id),
                )
            ),
            cwd=repository,
            credential_binding=binding,
        )
        del update
        return GitExecutionResult(
            operation_id=intent.operation_id,
            action=operation.action,
            object_id=object_id,
            changed=True,
        )

    @staticmethod
    def _identity_env(identity: GitIdentity) -> dict[str, str]:
        # The caller cannot supply identity through GitExecutionRequest.  This
        # value comes only from the access service's accepted Git grant.
        return {
            "GIT_AUTHOR_NAME": identity.name,
            "GIT_AUTHOR_EMAIL": identity.email,
            "GIT_COMMITTER_NAME": identity.name,
            "GIT_COMMITTER_EMAIL": identity.email,
        }

    async def _ensure_object(
        self,
        intent: AccessIntent,
        authorization: Ref,
        operation: GitOperation,
        access: _GitAccess,
        repository: Path,
        object_id: str,
        binding: DeliveryBinding | None,
    ) -> None:
        if object_id in _ZERO:
            raise ServiceError("invalid_input", 422)
        await self._run(
            intent,
            authorization,
            operation,
            access,
            lambda _identity: self._git_argv(("cat-file", "-e", object_id)),
            cwd=repository,
            credential_binding=binding,
        )

    async def _ensure_diff_object(
        self,
        intent: AccessIntent,
        authorization: Ref,
        operation: GitOperation,
        access: _GitAccess,
        repository: Path,
        object_id: str,
        binding: DeliveryBinding | None,
    ) -> None:
        await self._ensure_type_any(
            intent,
            authorization,
            operation,
            access,
            repository,
            object_id,
            {"commit", "tree"},
            binding,
        )

    async def _ensure_type_any(
        self,
        intent: AccessIntent,
        authorization: Ref,
        operation: GitOperation,
        access: _GitAccess,
        repository: Path,
        object_id: str,
        types: set[str],
        binding: DeliveryBinding | None,
    ) -> None:
        result = await self._run(
            intent,
            authorization,
            operation,
            access,
            lambda _identity: self._git_argv(("cat-file", "-t", object_id)),
            cwd=repository,
            credential_binding=binding,
        )
        try:
            actual = result.stdout.decode("ascii").strip()
        except UnicodeDecodeError:
            raise ServiceError("execution_error", 500) from None
        if actual not in types:
            raise ServiceError("invalid_input", 422)

    async def _ensure_type(
        self,
        intent: AccessIntent,
        authorization: Ref,
        operation: GitOperation,
        access: _GitAccess,
        repository: Path,
        object_id: str,
        expected: str,
        binding: DeliveryBinding | None,
    ) -> None:
        await self._ensure_type_any(
            intent, authorization, operation, access, repository, object_id, {expected}, binding
        )

    @staticmethod
    def _parse_oid(raw: bytes) -> str:
        try:
            text = raw.decode("ascii")
        except UnicodeDecodeError:
            raise ServiceError("execution_error", 500) from None
        if not text.endswith("\n"):
            raise ServiceError("execution_error", 500)
        value = text[:-1]
        if not _OID.fullmatch(value):
            raise ServiceError("execution_error", 500)
        return value

    @staticmethod
    def _push_rejected(raw: bytes) -> bool:
        try:
            text = raw.decode("ascii")
        except UnicodeDecodeError:
            return False
        return any(
            line.lstrip().startswith("!\t") or "[rejected]" in line for line in text.splitlines()
        )

    @staticmethod
    def _push_changed(raw: bytes) -> bool:
        # Porcelain flags include a leading space for a normal fast-forward.
        # Do not lstrip it; a successful push must have exactly one ref status.
        statuses = [line[:1] for line in raw.splitlines() if len(line) > 1 and line[1:2] == b"\t"]
        if len(statuses) != 1 or statuses[0] not in (b" ", b"*", b"+", b"="):
            raise ServiceError("execution_error", 500)
        return statuses[0] != b"="

    @staticmethod
    def _clone_branch(ref: str) -> str:
        if ref.startswith("refs/heads/"):
            return ref[len("refs/heads/") :]
        if ref.startswith("refs/tags/"):
            return ref[len("refs/tags/") :]
        raise ServiceError("invalid_input", 422)

    @staticmethod
    def _remove_created_clone(path: Path, created: os.stat_result) -> None:
        try:
            current = path.lstat()
            if (
                stat.S_ISLNK(current.st_mode)
                or current.st_dev != created.st_dev
                or current.st_ino != created.st_ino
                or current.st_uid != os.geteuid()
            ):
                return
            shutil.rmtree(path)
        except FileNotFoundError:
            return
        except OSError:
            return


def _kill_process(process: subprocess.Popen[bytes]) -> None:
    try:
        pgid = os.getpgid(process.pid)
    except OSError:
        pgid = process.pid
    try:
        if pgid == process.pid:
            os.killpg(pgid, signal.SIGKILL)
        else:
            process.kill()
    except (OSError, ProcessLookupError):
        try:
            process.kill()
        except (OSError, ProcessLookupError):
            pass


def _wait_quietly(process: subprocess.Popen[bytes]) -> None:
    try:
        process.wait(timeout=2)
    except (subprocess.TimeoutExpired, OSError):
        pass


async def _reap(task: asyncio.Task[_Completed]) -> None:
    try:
        await asyncio.shield(task)
    except BaseException:
        pass


def _communicate_bounded(
    process: subprocess.Popen[bytes], input_bytes: bytes, limit: int
) -> _Completed:
    selector = selectors.DefaultSelector()
    output: dict[str, bytearray] = {"stdout": bytearray(), "stderr": bytearray()}
    stdin = process.stdin
    pending = memoryview(input_bytes)
    try:
        if stdin is not None:
            os.set_blocking(stdin.fileno(), False)
            if pending:
                selector.register(stdin, selectors.EVENT_WRITE, "stdin")
            else:
                stdin.close()
                stdin = None
        for name, stream in (("stdout", process.stdout), ("stderr", process.stderr)):
            if stream is not None:
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_READ, name)
        while selector.get_map() or process.poll() is None:
            events = selector.select(0.1)
            if not events and process.poll() is not None:
                # A closed child may still have buffered pipe data.
                for key in tuple(selector.get_map().values()):
                    if key.data == "stdin":
                        selector.unregister(key.fileobj)
                        if stdin is not None:
                            stdin.close()
                        stdin = None
            for key, _mask in events:
                name = cast(str, key.data)
                pipe = cast(BinaryIO, key.fileobj)
                if name == "stdin":
                    assert stdin is not None
                    try:
                        written = os.write(stdin.fileno(), pending)
                    except (BlockingIOError, InterruptedError):
                        continue
                    pending = pending[written:]
                    if not pending:
                        selector.unregister(stdin)
                        stdin.close()
                        stdin = None
                    continue
                try:
                    chunk = os.read(pipe.fileno(), 8192)
                except (BlockingIOError, InterruptedError):
                    continue
                if not chunk:
                    selector.unregister(pipe)
                    pipe.close()
                    continue
                output[name].extend(chunk)
                if len(output["stdout"]) + len(output["stderr"]) > limit:
                    raise _BoundedOutput
        return _Completed(process.wait(), bytes(output["stdout"]), bytes(output["stderr"]))
    except _BoundedOutput:
        _kill_process(process)
        try:
            process.wait(timeout=2)
        except (subprocess.TimeoutExpired, OSError):
            pass
        raise
    finally:
        try:
            selector.close()
        finally:
            for stream in (stdin, process.stdout, process.stderr):
                if stream is not None:
                    try:
                        stream.close()
                    except OSError:
                        pass
