"""One-shot container secret bootstrap behavior and Docker command binding."""
from __future__ import annotations

import base64
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

from krim_agent.dev_environment import docker
from krim_agent.dev_environment.bootstrap import CWD_EXEC
from krim_agent.dev_environment.model import RuntimeSpec
from krim_agent.development.contracts import (
    AuthorizationRef,
    CommandRequest,
    EnvironmentHandle,
    RunOwnership,
)
from krim_agent.development.ports import EnvironmentRecord
from krim_agent.project_access.process_bootstrap import (
    SECRET_DIR_PREFIX,
    SECRET_USED_PREFIX,
    SECRET_WRITE_CODE,
    secret_token,
    secret_wait_bootstrap,
)


def make_spec(
    tmp_path: Path,
    refs: tuple[str, ...],
    *,
    timeout_seconds: float | None = None,
) -> tuple[RuntimeSpec, CommandRequest]:
    root = tmp_path / "workspace"
    root.mkdir(parents=True, exist_ok=True)
    metadata = root.stat()
    environment = EnvironmentHandle(
        owner_id="owner-1",
        project_id="project-1",
        environment_id="environment-1",
        generation=1,
        state="ready",
    )
    request = CommandRequest(
        operation_id="operation-1",
        environment=environment,
        ownership=RunOwnership(owner_id="owner-1", session_id="session-1", run_id="run-1"),
        authorization=AuthorizationRef(
            actor_id="actor-1",
            actor_generation=1,
            project_id="project-1",
            arguments_digest="a" * 64,
            secret_refs=refs,
        ),
        command="printf ready",
        cwd=".",
        lifecycle="project_service",
        timeout_seconds=timeout_seconds,
    )
    record = EnvironmentRecord(
        environment=environment,
        image_id="sha256:" + "b" * 64,
        content_path=root,
        content_device=metadata.st_dev,
        content_inode=metadata.st_ino,
    )
    return RuntimeSpec(record, request, "process-1"), request


def package(
    token: str,
    refs: tuple[str, ...],
    values: tuple[bytes, ...],
    *,
    expires_at: float | None = None,
) -> bytes:
    return json.dumps(
        {
            "token": token,
            "refs": list(refs),
            "values": [base64.b64encode(value).decode("ascii") for value in values],
            "expires_at": time.time() + 60 if expires_at is None else expires_at,
        },
        separators=(",", ":"),
    ).encode("utf-8")


def _patched_script(code: str, tmp_path: Path) -> str:
    # Substitute each fixed /tmp path so subprocess tests cannot collide.
    code = code.replace(SECRET_DIR_PREFIX, str(tmp_path / "secret-"))
    code = code.replace(SECRET_USED_PREFIX, str(tmp_path / "used-"))
    return code.replace("WAIT_SECONDS=15.0", "WAIT_SECONDS=0.15")


def _run_writer(code: str, data: bytes) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [sys.executable, "-I", "-c", code],
        input=data,
        capture_output=True,
        env={"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
        timeout=5,
        check=False,
    )


def _run_reader(
    code: str,
    token: str,
    refs: tuple[str, ...],
    *args: str,
    timeout: float = 5,
) -> subprocess.CompletedProcess[bytes]:
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "KRIM_SECRET_TOKEN": token,
        "KRIM_SECRET_REFS": json.dumps(list(refs), separators=(",", ":")),
    }
    return subprocess.run(
        [sys.executable, "-I", "-c", code, *args],
        capture_output=True,
        env=env,
        timeout=timeout,
        check=False,
    )


def stat_mode(path: Path) -> int:
    return path.stat().st_mode & 0o777


def test_docker_command_binds_refs_and_timeout_without_extra_cwd_args(tmp_path):
    spec, request = make_spec(tmp_path, ("api-key", "service.token"), timeout_seconds=3.5)
    entrypoint, args = docker.command(spec)

    assert entrypoint == "/usr/bin/timeout"
    assert args[:4] == ["--foreground", "--signal=KILL", "3.500000000s", "/usr/bin/python3"]
    assert args[4:6] == ["-I", "-c"]
    assert args[7:] == [request.cwd, request.command]
    bootstrap = args[6]
    assert "KRIM_SECRET_TOKEN" in bootstrap
    assert "KRIM_SECRET_REFS" in bootstrap
    assert secret_token(request, spec.process_id) in bootstrap
    assert "api-key" in bootstrap and "service.token" in bootstrap
    assert "CWD_EXEC_CODE = " in bootstrap
    assert "COMMAND_TIMEOUT = 3.5" in bootstrap
    assert "fixture-secret" not in bootstrap

    no_secret_spec, no_secret_request = make_spec(tmp_path / "plain", ())
    _, no_secret_args = docker.command(no_secret_spec)
    assert no_secret_args[2] == CWD_EXEC
    assert no_secret_request.command == no_secret_args[4]


@pytest.mark.skipif(os.name != "posix", reason="uses Linux O_NOFOLLOW and exec semantics")
def test_real_subprocess_opens_readonly_fds_then_unlinks_payload(tmp_path):
    spec, request = make_spec(tmp_path, ("alpha-key", "beta-key"))
    token = secret_token(request, spec.process_id)
    refs = request.authorization.secret_refs
    values = (b"synthetic-alpha-secret-7d31", b"synthetic-beta-secret-91af")
    data = package(token, refs, values)
    writer_code = _patched_script(SECRET_WRITE_CODE, tmp_path)
    writer_args = [sys.executable, "-I", "-c", writer_code]

    written = _run_writer(writer_code, data)
    assert written.returncode == 0
    assert written.stdout == written.stderr == b""
    assert all(value not in repr(writer_args).encode() for value in values)

    secret_dir = Path(str(tmp_path / "secret-") + token)
    assert stat_mode(secret_dir) == 0o700
    assert stat_mode(secret_dir / "ready") == 0o600
    assert stat_mode(secret_dir / "secret-0") == 0o600

    workspace = tmp_path / "workspace"
    probe = (
        "import fcntl,hashlib,json,os,sys;"
        "fds=[int(os.environ['KRIM_SECRET_FD_'+str(i)]) for i in range(2)];"
        "values=[os.read(fd,16385) for fd in fds];"
        "print(json.dumps({'hashes':[hashlib.sha256(v).hexdigest() for v in values],"
        "'readonly':[fcntl.fcntl(fd,fcntl.F_GETFL)&os.O_ACCMODE==os.O_RDONLY for fd in fds],"
        "'refs':os.environ.get('KRIM_SECRET_REFS'),"
        "'token_present':'KRIM_SECRET_TOKEN' in os.environ,"
        "'argv':sys.argv,'env':dict(os.environ)},sort_keys=True))"
    )
    shell_command = "/usr/bin/python3 -c " + shlex.quote(probe)
    cwd_exec = CWD_EXEC.replace("'/workspace'", repr(str(workspace)))
    bootstrap = _patched_script(
        secret_wait_bootstrap(request, spec.process_id, cwd_exec, None), tmp_path
    )
    reader_args = [sys.executable, "-I", "-c", bootstrap, ".", shell_command]
    reader = _run_reader(bootstrap, token, refs, ".", shell_command)
    assert reader.returncode == 0, reader.stderr.decode("utf-8", "replace")
    result = json.loads(reader.stdout)
    assert result["hashes"] == [hashlib.sha256(value).hexdigest() for value in values]
    assert result["readonly"] == [True, True]
    assert result["refs"] == json.dumps(list(refs), separators=(",", ":"))
    assert result["token_present"] is False
    assert all(value.decode("ascii") not in json.dumps(result) for value in values)
    assert all(value.decode("ascii") not in repr(reader_args) for value in values)
    assert not secret_dir.exists()
    marker = Path(str(tmp_path / "used-") + token)
    assert marker.is_file() and marker.stat().st_size == 0 and stat_mode(marker) == 0o600

    replay = _run_writer(writer_code, data)
    assert replay.returncode == 78
    assert replay.stdout == replay.stderr == b""


@pytest.mark.skipif(os.name != "posix", reason="uses Linux O_NOFOLLOW and exec semantics")
def test_wrong_token_refs_and_expired_package_are_rejected(tmp_path):
    spec, request = make_spec(tmp_path, ("expected-ref",))
    token = secret_token(request, spec.process_id)
    refs = request.authorization.secret_refs
    value = b"synthetic-rejection-secret"
    writer_code = _patched_script(SECRET_WRITE_CODE, tmp_path)
    reader_code = _patched_script(
        secret_wait_bootstrap(request, spec.process_id, CWD_EXEC, None), tmp_path
    )

    wrong_refs = ("different-ref",)
    written = _run_writer(writer_code, package(token, wrong_refs, (value,)))
    assert written.returncode == 0 and written.stdout == written.stderr == b""
    rejected = _run_reader(reader_code, token, refs)
    assert rejected.returncode == 78 and rejected.stdout == rejected.stderr == b""
    assert not (Path(str(tmp_path / "secret-") + token)).exists()

    wrong_token = "0" * 64 if token != "0" * 64 else "1" * 64
    written = _run_writer(writer_code, package(wrong_token, refs, (value,)))
    assert written.returncode == 0 and written.stdout == written.stderr == b""
    rejected = _run_reader(reader_code, token, refs)
    assert rejected.returncode == 78 and rejected.stdout == rejected.stderr == b""
    assert not (Path(str(tmp_path / "secret-") + token)).exists()
    shutil.rmtree(Path(str(tmp_path / "secret-") + wrong_token), ignore_errors=True)
    (Path(str(tmp_path / "used-") + wrong_token)).unlink(missing_ok=True)

    expiry = time.time() + 1.0
    expiry_token = secret_token(request, "process-expired")
    expiry_reader = _patched_script(
        secret_wait_bootstrap(request, "process-expired", CWD_EXEC, None), tmp_path
    )
    written = _run_writer(
        writer_code, package(expiry_token, refs, (value,), expires_at=expiry)
    )
    assert written.returncode == 0
    time.sleep(max(0, expiry - time.time() + 0.02))
    rejected = _run_reader(expiry_reader, expiry_token, refs)
    assert rejected.returncode == 78 and rejected.stdout == rejected.stderr == b""
    assert value not in rejected.stdout + rejected.stderr
    assert not (Path(str(tmp_path / "secret-") + expiry_token)).exists()

    (Path(str(tmp_path / "used-") + token)).unlink(missing_ok=True)
    (Path(str(tmp_path / "used-") + expiry_token)).unlink(missing_ok=True)


@pytest.mark.skipif(os.name != "posix", reason="uses GNU timeout and Linux exec semantics")
def test_remaining_expiry_caps_user_command_runtime(tmp_path):
    spec, request = make_spec(tmp_path, ("runtime-bound-secret",))
    token = secret_token(request, spec.process_id)
    refs = request.authorization.secret_refs
    value = b"synthetic-runtime-secret"
    expiry = time.time() + 1.5
    writer_code = _patched_script(SECRET_WRITE_CODE, tmp_path)
    written = _run_writer(writer_code, package(token, refs, (value,), expires_at=expiry))
    assert written.returncode == 0 and written.stdout == written.stderr == b""

    workspace = tmp_path / "workspace"
    cwd_exec = CWD_EXEC.replace("'/workspace'", repr(str(workspace)))
    bootstrap = _patched_script(
        secret_wait_bootstrap(request, spec.process_id, cwd_exec, None), tmp_path
    )
    started = time.monotonic()
    result = _run_reader(bootstrap, token, refs, ".", "sleep 5", timeout=5)
    elapsed = time.monotonic() - started
    assert result.returncode != 0
    assert elapsed < 2.5
    assert result.stdout == result.stderr == b""
    assert not (Path(str(tmp_path / "secret-") + token)).exists()
