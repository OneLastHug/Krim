"""Operation-private native Git credential-cache, never a generic Shell env.

POSIX trusted host only: the private root and its sockets must not be mounted
into untrusted project processes. I owns that OS boundary and process cleanup.
The only secret transfer is an atomic nonblocking stdin write to a prestarted
native Git helper. No credential is put in argv, environment, disk or results.
"""

from __future__ import annotations

import asyncio
import math
import os
import shlex
import shutil
import stat
import subprocess
import tempfile
from pathlib import Path

from krim_agent.contracts import ServiceError, parse_json
from krim_agent.project_access.git import GitOperation, check_helper_request
from krim_agent.project_access.secrets import DeliveryBinding, SecretMaterial


def _environment(home: Path) -> dict[str, str]:
    return {
        "PATH": "/usr/bin:/bin",
        "HOME": str(home),
        "XDG_CONFIG_HOME": str(home),
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_ASKPASS": "/bin/false",
        "LC_ALL": "C",
    }


def _private_root(root: Path) -> Path:
    if os.name != "posix" or not root.is_absolute():
        raise ServiceError("invalid_input", 422)
    # Parent is operator-controlled; never follow a caller-selected symlink.
    if root.resolve() != root or root.parent.resolve() != root.parent:
        raise ServiceError("permission_denied", 403)
    parent = root.parent.stat()
    if parent.st_uid != os.getuid() or parent.st_mode & 0o022:
        raise ServiceError("permission_denied", 403)
    root.mkdir(mode=0o700, exist_ok=True)
    info = root.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ServiceError("permission_denied", 403)
    return root


class NativeGitCredentialCache:
    """A single operation's SecretSink and native credential helper.

    Construct before entering the repository guard, deliver through the guarded
    ProjectAccessService, then await ready() outside the guard. Pass only helper
    and binding to TrustedGitRunner. Always close in a finally/context manager;
    I calls close at committed revocation too. Short cache TTL is crash fallback.
    """

    def __init__(
        self,
        root: Path,
        binding: DeliveryBinding,
        *,
        git_executable: Path = Path("/usr/bin/git"),
        timeout: int = 30,
    ) -> None:
        if (
            not git_executable.is_absolute()
            or not isinstance(timeout, int)
            or isinstance(timeout, bool)
            or not 1 <= timeout <= 300
        ):
            raise ServiceError("invalid_input", 422)
        arguments = parse_json(binding.intent.arguments_json)
        if not isinstance(arguments, dict) or "git" not in arguments:
            raise ServiceError("invalid_input", 422)
        operation = GitOperation.model_validate(arguments["git"])
        if operation.action not in ("git.clone", "git.push"):
            raise ServiceError("permission_denied", 403)
        self._expected = binding
        self._binding: DeliveryBinding | None = None
        self._accepted = False
        self._delivered = False
        self._closed = False
        self._cleanup_complete = False
        self._git = str(git_executable)
        self._timeout = timeout
        self._directory = Path(tempfile.mkdtemp(prefix="gc-", dir=_private_root(root)))
        self._socket = self._directory / "socket"
        if len(os.fsencode(self._socket)) > 100:
            shutil.rmtree(self._directory)
            raise ServiceError("invalid_input", 422)
        self._env = _environment(self._directory)
        self._request = helper_request(operation)
        self._writer: subprocess.Popen[bytes] | None = None
        failed = False
        try:
            self._writer = subprocess.Popen(
                [
                    self._git,
                    "credential-cache",
                    f"--timeout={timeout}",
                    f"--socket={self._socket}",
                    "store",
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=self._env,
                cwd=self._directory,
                start_new_session=True,
            )
            assert self._writer.stdin is not None
            os.set_blocking(self._writer.stdin.fileno(), False)
        except Exception:
            failed = True
        if failed:
            self.close()
            raise ServiceError("execution_error", 500)

    def __repr__(self) -> str:
        return "NativeGitCredentialCache(<private>)"

    @property
    def binding(self) -> DeliveryBinding:
        if self._closed or self._binding is None:
            raise ServiceError("permission_denied", 403)
        return self._binding

    @property
    def helper(self) -> str:
        _ = self.binding
        return f"cache --timeout={self._timeout} --socket={shlex.quote(str(self._socket))}"

    def deliver(self, binding: DeliveryBinding, material: SecretMaterial) -> None:
        if self._closed or self._accepted or binding != self._expected:
            raise ServiceError("permission_denied", 403)
        writer = self._writer
        if writer is None or writer.stdin is None or writer.poll() is not None:
            raise ServiceError("execution_error", 500)
        # Git helper's response already contains username/password. Prepend only
        # the checked public request fields; retain neither plaintext copy.
        payload = bytearray(self._request.rstrip(b"\n") + b"\n")
        view = material.borrow()
        payload.extend(view)
        view.release()
        self._accepted = True
        failed = False
        try:
            fd = writer.stdin.fileno()
            # PIPE_BUF guarantees all-or-EAGAIN for nonblocking atomic writes.
            if len(payload) > os.fpathconf(fd, "PC_PIPE_BUF"):
                raise ValueError
            if os.write(fd, payload) != len(payload):
                raise ValueError
        except Exception:
            failed = True
        finally:
            payload[:] = b"\x00" * len(payload)
            writer.stdin.close()
        if failed:
            raise ServiceError("execution_error", 500)
        self._delivered = True

    async def ready(self, wait_seconds: float = 5.0) -> None:
        if (
            not math.isfinite(wait_seconds)
            or wait_seconds <= 0
            or not self._delivered
            or self._closed
        ):
            raise ServiceError("invalid_input", 422)
        assert self._writer is not None
        failed = False
        try:
            result = await asyncio.to_thread(self._writer.wait, timeout=wait_seconds)
            if result or self._closed:
                raise ValueError
        except BaseException as exc:
            self.close()
            if isinstance(exc, asyncio.CancelledError):
                raise
            failed = True
        if failed:
            raise ServiceError("execution_error", 500)
        self._binding = self._expected

    def close(self) -> None:
        if self._cleanup_complete:
            return
        self._closed = True
        self._binding = None
        failed = False
        writer = self._writer
        try:
            if writer is not None:
                if writer.stdin is not None and not writer.stdin.closed:
                    writer.stdin.close()
                if writer.poll() is None:
                    writer.kill()
                writer.wait(timeout=5)
            # Exit addresses this operation's exact private socket only.
            subprocess.run(
                [self._git, "credential-cache", f"--socket={self._socket}", "exit"],
                env=self._env,
                cwd=self._directory,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=5,
                check=True,
            )
            shutil.rmtree(self._directory)
        except Exception:
            failed = True
        if failed:
            # Remain unusable; retain the exact resource for an explicit retry.
            raise ServiceError("execution_error", 500)
        self._cleanup_complete = True


def helper_request(operation: GitOperation) -> bytes:
    from urllib.parse import urlsplit

    parsed = urlsplit(operation.repository)
    raw = (f"protocol=https\nhost={parsed.netloc}\npath={parsed.path[1:]}\n\n").encode("ascii")
    check_helper_request(raw, operation.repository)
    return raw
