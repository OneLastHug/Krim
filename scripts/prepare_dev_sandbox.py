#!/usr/bin/env python3
"""Prepare and qualify the dedicated Krim P5 Linux sandbox.

This script never installs packages or reads credentials. `build-rootfs` only
materializes the installed Ubuntu CPython/ELF closure. `qualify` must run as
krim (UID 1000) inside a systemd unit delegated with `Delegate=yes` and
`DelegateSubgroup=app`; it writes a compatible BASE-QUALIFIED profile only
after the real cgroup, pidfd, bubblewrap, rootfs, and seccomp checks pass, then
stays alive so the delegated cgroup inode remains valid.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

ROOT = Path("/home/krim/.local/share/krim-agent/p5-os")
BWRAP = Path("/usr/bin/bwrap")
PYTHON = Path("/home/krim/.venvs/krim-agent/bin/python")
MAX_MANIFEST_FILES = 5000
REQUIRED_CONTROLLERS = {"cpu", "memory", "pids"}
PROFILE_LIMITS = {
    "wall": 20.0,
    "cpu_seconds": 2.0,
    "memory": 134217728,
    "pids": 16,
    "output": 262144,
}
LDD_ARROW = re.compile(r"=>\s+(/[^\s(]+)")
LDD_DIRECT = re.compile(r"^\s*(/[^\s(]+)\s+\(")


class PreparationError(RuntimeError):
    """A prerequisite or a real OS qualification failed."""


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def safe_relative(value: str) -> str:
    """Return a normalized rootfs-relative name or reject path traversal."""
    if not isinstance(value, str) or not value or "\\" in value:
        raise PreparationError("invalid rootfs relative path")
    parsed = PurePosixPath(value)
    if parsed.is_absolute() or any(part in {"", ".", ".."} for part in parsed.parts):
        raise PreparationError("invalid rootfs relative path")
    normalized = parsed.as_posix()
    if normalized != value:
        raise PreparationError("non-canonical rootfs relative path")
    return normalized


def parse_ldd_dependencies(output: str) -> tuple[str, ...]:
    """Extract resolved ELF paths and reject an incomplete dynamic closure."""
    paths: set[str] = set()
    for line in output.splitlines():
        if "=> not found" in line:
            raise PreparationError("ELF dependency is not installed: " + line.strip())
        match = LDD_ARROW.search(line) or LDD_DIRECT.search(line)
        if match:
            paths.add(match.group(1))
    return tuple(sorted(paths))


def stdlib_candidate(relative: str) -> bool:
    """Exclude bytecode, package caches, and third-party package directories."""
    relative = safe_relative(relative)
    parts = PurePosixPath(relative).parts
    return not (
        "__pycache__" in parts
        or "site-packages" in parts
        or "dist-packages" in parts
        or relative in {"sitecustomize.py", "usercustomize.py"}
        or relative.endswith((".pyc", ".pyo"))
    )


def derive_cgroup_root(cgroup_record: str) -> Path:
    """Require the current process to be exactly in the delegated `/app` child."""
    fields = cgroup_record.strip().split(":", 2)
    if len(fields) != 3 or fields[0] != "0" or fields[1] != "":
        raise PreparationError("unified cgroup v2 membership is required")
    rel = fields[2]
    if not rel.startswith("/") or rel.endswith("/") or PurePosixPath(rel).name != "app":
        raise PreparationError("process must run in cgroup_root/app")
    parent = PurePosixPath(rel).parent
    if parent == PurePosixPath("/"):
        raise PreparationError("the delegated cgroup root must be below /sys/fs/cgroup")
    return Path("/sys/fs/cgroup").joinpath(*parent.parts[1:])


def parse_status(raw: str) -> dict[str, str]:
    wanted = {"Uid", "Gid", "CapEff", "CapPrm", "CapAmb", "NoNewPrivs", "Seccomp"}
    return {
        key: value.strip()
        for line in raw.splitlines()
        if ":" in line
        for key, value in [line.split(":", 1)]
        if key in wanted
    }


def write_json_exclusive(path: Path, value: Any, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.parent.is_symlink() or not path.parent.is_dir():
        raise PreparationError("evidence parent is not a regular directory")
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, sort_keys=True, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    path.chmod(mode)


def ensure_private_directory(path: Path, *, mode: int = 0o700, owner: int | None = None) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=mode)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
        raise PreparationError("expected a non-symlink directory: " + str(path))
    if owner is not None and info.st_uid != owner:
        raise PreparationError("unexpected directory owner: " + str(path))
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise PreparationError("directory must not be accessible by group/other: " + str(path))


def allowed_system_source(path: Path) -> bool:
    text = str(path)
    return (
        text == "/usr"
        or text.startswith("/usr/")
        or text == "/lib"
        or text.startswith("/lib/")
        or text == "/lib64"
        or text.startswith("/lib64/")
    )


def copy_material(
    source: Path,
    relative: str,
    role: str,
    rootfs: Path,
    manifest_files: dict[str, dict[str, Any]],
) -> None:
    """Copy one trusted Ubuntu file as a regular, read-only rootfs file."""
    relative = safe_relative(relative)
    resolved = source.resolve(strict=True)
    if not allowed_system_source(resolved) or not resolved.is_file():
        raise PreparationError("material source outside installed system tree: " + str(source))
    source_info = resolved.stat()
    if not stat.S_ISREG(source_info.st_mode):
        raise PreparationError("material source is not a regular file: " + str(source))
    digest = sha256_file(resolved)
    previous = manifest_files.get(relative)
    if previous is not None:
        if previous["sha256"] != digest:
            raise PreparationError("conflicting material bytes for " + relative)
        return
    destination = rootfs.joinpath(*PurePosixPath(relative).parts)
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with resolved.open("rb") as src, destination.open("xb") as dst:
        shutil.copyfileobj(src, dst, length=1024 * 1024)
        dst.flush()
        os.fsync(dst.fileno())
    mode = 0o555 if source_info.st_mode & 0o111 else 0o444
    destination.chmod(mode)
    manifest_files[relative] = {
        "sha256": digest,
        "bytes": source_info.st_size,
        "mode": mode,
        "source": str(source),
        "resolved_source": str(resolved),
        "role": role,
    }
    if len(manifest_files) > MAX_MANIFEST_FILES:
        raise PreparationError("rootfs file count exceeds bounded manifest capacity")


def verify_manifest(rootfs: Path, manifest_files: dict[str, dict[str, Any]]) -> None:
    """Verify every listed file is regular, hash-correct, and in a read-only tree."""
    root_info = rootfs.lstat()
    if not stat.S_ISDIR(root_info.st_mode) or stat.S_IMODE(root_info.st_mode) & 0o222:
        raise PreparationError("rootfs must be a read-only regular directory")
    for name, expected in manifest_files.items():
        relative = safe_relative(name)
        path = rootfs.joinpath(*PurePosixPath(relative).parts)
        parent = rootfs
        for part in PurePosixPath(relative).parts[:-1]:
            parent = parent / part
            parent_info = parent.lstat()
            if (
                not stat.S_ISDIR(parent_info.st_mode)
                or stat.S_ISLNK(parent_info.st_mode)
                or stat.S_IMODE(parent_info.st_mode) & 0o222
            ):
                raise PreparationError("rootfs parent is not a read-only directory: " + str(parent))
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
            raise PreparationError("rootfs file is not a regular non-symlink: " + relative)
        if info.st_size != expected["bytes"] or sha256_file(path) != expected["sha256"]:
            raise PreparationError("rootfs manifest mismatch: " + relative)
        if stat.S_IMODE(info.st_mode) & 0o222:
            raise PreparationError("rootfs file is writable: " + relative)


def _ldd(path: Path) -> tuple[str, ...]:
    completed = subprocess.run(
        ["/usr/bin/ldd", str(path)],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
        env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"},
    )
    if completed.returncode != 0:
        raise PreparationError("ldd failed for installed ELF " + str(path))
    return parse_ldd_dependencies(completed.stdout + "\n" + completed.stderr)


def _package_owner(path: Path) -> str:
    candidates = (path, path.resolve(strict=True))
    for candidate in candidates:
        result = subprocess.run(
            ["/usr/bin/dpkg-query", "-S", str(candidate)],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
            env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"},
        )
        for line in result.stdout.splitlines():
            owner, separator, _ = line.partition(": ")
            if separator and owner:
                package = owner.split(",", 1)[0].split(":", 1)[0]
                if re.fullmatch(r"[a-z0-9][a-z0-9+.-]*", package):
                    return package
    raise PreparationError("dpkg package owner not found for " + str(path))


def _copy_licenses(
    package: str,
    rootfs: Path,
    manifest_files: dict[str, dict[str, Any]],
) -> str:
    source = Path("/usr/share/doc") / package / "copyright"
    if not source.is_file():
        raise PreparationError("installed package copyright missing for " + package)
    relative = "usr/share/doc/" + package + "/copyright"
    copy_material(source, relative, "original Ubuntu package copyright", rootfs, manifest_files)
    return relative


def build_rootfs(root: Path = ROOT) -> dict[str, Any]:
    """Materialize the installed CPython 3.12 and its complete ELF closure."""
    ensure_private_directory(root, owner=os.getuid())
    final_rootfs = root / "rootfs"
    if final_rootfs.exists() or final_rootfs.is_symlink():
        if final_rootfs.is_symlink() or not final_rootfs.is_dir():
            raise PreparationError("existing rootfs is not a regular directory; refusing reuse")
        manifest_path = final_rootfs / "manifest.json"
        if manifest_path.is_symlink() or not manifest_path.is_file():
            raise PreparationError("existing rootfs has no regular manifest; refusing overwrite")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        verify_manifest(final_rootfs, manifest["files"])
        return manifest

    source_prefix = Path(sys.base_prefix).resolve(strict=True)
    stdlib = source_prefix / "lib/python3.12"
    interpreter = Path(getattr(sys, "_base_executable", "/usr/bin/python3.12")).resolve(strict=True)
    if (
        sys.version_info[:2] != (3, 12)
        or not stdlib.is_dir()
        or not allowed_system_source(interpreter)
    ):
        raise PreparationError("installed Ubuntu Python 3.12 base runtime is not available")

    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    staging = root / ("rootfs.staging-" + run_id)
    staging.mkdir(mode=0o700)
    manifest_files: dict[str, dict[str, Any]] = {}
    try:
        copy_material(
            interpreter,
            "opt/python/bin/python3.12",
            "installed CPython 3.12.3",
            staging,
            manifest_files,
        )
        libdir = Path(sysconfig_path("LIBDIR"))
        libpython_name = sysconfig_path("LDLIBRARY")
        libpython = (libdir / libpython_name).resolve(strict=True)
        copy_material(
            libpython,
            "opt/python/lib/" + libpython.name,
            "installed CPython shared runtime",
            staging,
            manifest_files,
        )

        elf_roots = [interpreter, libpython]
        for directory, names, filenames in os.walk(stdlib, followlinks=False):
            names[:] = sorted(
                name
                for name in names
                if name not in {"__pycache__", "site-packages", "dist-packages"}
                and not (Path(directory) / name).is_symlink()
            )
            for filename in sorted(filenames):
                source = Path(directory) / filename
                relative = source.relative_to(stdlib).as_posix()
                if not stdlib_candidate(relative):
                    continue
                target = "opt/python/lib/python3.12/" + relative
                copy_material(
                    source,
                    target,
                    "Python 3.12 standard library",
                    staging,
                    manifest_files,
                )
                resolved = source.resolve(strict=True)
                if ".so" in source.name and resolved not in elf_roots:
                    elf_roots.append(resolved)

        dependencies: dict[str, str] = {}
        for elf in elf_roots:
            for name in _ldd(elf):
                dependency = Path(name)
                resolved = dependency.resolve(strict=True)
                if not allowed_system_source(dependency) and not allowed_system_source(resolved):
                    raise PreparationError("ELF dependency outside /usr,/lib,/lib64: " + name)
                relative = dependency.as_posix().lstrip("/")
                copy_material(
                    dependency,
                    relative,
                    "resolved installed ELF dependency",
                    staging,
                    manifest_files,
                )
                dependencies[relative] = str(resolved)

        packages: dict[str, dict[str, str]] = {}
        for relative in sorted(dependencies):
            source = Path("/") / relative
            package = _package_owner(source)
            copyright_relative = _copy_licenses(package, staging, manifest_files)
            packages[relative] = {
                "package": package,
                "copyright": copyright_relative,
                "copyright_sha256": manifest_files[copyright_relative]["sha256"],
            }

        common_licenses = Path("/usr/share/common-licenses")
        if not common_licenses.is_dir():
            raise PreparationError("Ubuntu common license directory is missing")
        for source in sorted(common_licenses.iterdir()):
            if source.is_file():
                copy_material(
                    source,
                    "usr/share/common-licenses/" + source.name,
                    "original common license text",
                    staging,
                    manifest_files,
                )

        for directory in ("lib", "lib64", "usr", "proc", "dev", "tmp", "work"):
            (staging / directory).mkdir(parents=True, exist_ok=True, mode=0o700)
        manifest = {
            "schema": "krim-dev-rootfs/1",
            "python": {
                "version": sys.version.split()[0],
                "source_prefix": str(source_prefix),
                "source_executable": str(interpreter),
                "executable": "/opt/python/bin/python3.12",
                "libpython": str(libpython),
            },
            "elf_dependencies": dependencies,
            "packages": packages,
            "files": manifest_files,
            "site_packages_included": False,
            "host_directory_mount_required": False,
            "complete": True,
        }
        manifest_path = staging / "manifest.json"
        with manifest_path.open("x", encoding="utf-8") as stream:
            json.dump(manifest, stream, ensure_ascii=False, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        manifest_path.chmod(0o444)
        for directory, names, _ in os.walk(staging, topdown=False, followlinks=False):
            for name in names:
                child = Path(directory) / name
                if child.is_symlink():
                    raise PreparationError("rootfs contains a symlink directory: " + str(child))
                child.chmod(0o555)
            Path(directory).chmod(0o555)
        os.rename(staging, final_rootfs)
        verify_manifest(final_rootfs, manifest_files)
        manifest["manifest_path"] = str(final_rootfs / "manifest.json")
        manifest["manifest_sha256"] = sha256_file(final_rootfs / "manifest.json")
        return manifest
    except BaseException:
        # Preserve the exact, private staging tree for diagnosis; never overwrite it.
        raise


def sysconfig_path(name: str) -> str:
    import sysconfig

    value = sysconfig.get_config_var(name)
    if not value:
        raise PreparationError("Python sysconfig variable unavailable: " + name)
    return str(value)


def load_project_modules() -> None:
    source = Path(__file__).resolve().parents[1] / "src"
    if str(source) not in sys.path:
        sys.path.insert(0, str(source))


def ensure_bwrap() -> tuple[str, str]:
    info = BWRAP.lstat()
    if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode) or info.st_uid != 0:
        raise PreparationError("Ubuntu bwrap must be a root-owned regular file at /usr/bin/bwrap")
    if info.st_mode & 0o6022:
        raise PreparationError("bwrap must not be setuid/setgid or group/other writable")
    result = subprocess.run([str(BWRAP), "--version"], capture_output=True, text=True, timeout=5)
    if result.returncode != 0:
        raise PreparationError("Ubuntu bwrap --version failed")
    return (result.stdout + result.stderr).strip(), sha256_file(BWRAP)


def create_event_recorder(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)

    def record(kind: str, **details: Any) -> None:
        row = {"kind": kind, "monotonic_ns": time.monotonic_ns(), **details}
        with path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
            stream.flush()

    return record


def fds() -> dict[str, str]:
    result: dict[str, str] = {}
    for name in os.listdir("/proc/self/fd"):
        try:
            result[name] = os.readlink("/proc/self/fd/" + name)
        except FileNotFoundError:
            pass
    return result


def owned_children() -> str:
    return Path(f"/proc/self/task/{os.getpid()}/children").read_text().strip()


def cgroup_context(record) -> tuple[Path, dict[str, Any]]:
    cg_text = Path("/proc/self/cgroup").read_text().strip()
    root = derive_cgroup_root(cg_text)
    if not root.is_dir() or root.is_symlink() or not str(root).startswith("/sys/fs/cgroup/"):
        raise PreparationError("delegated cgroup root is missing or outside /sys/fs/cgroup")
    root_info = root.stat()
    controllers = set((root / "cgroup.controllers").read_text().split())
    if not REQUIRED_CONTROLLERS <= controllers:
        raise PreparationError("delegated root lacks cpu/memory/pids controllers")
    if (root / "cgroup.procs").read_text().strip():
        raise PreparationError("delegated cgroup root must be empty; app must be its child")
    enabled = set((root / "cgroup.subtree_control").read_text().split())
    missing = REQUIRED_CONTROLLERS - enabled
    if missing:
        control = root / "cgroup.subtree_control"
        control.write_text(" ".join("+" + name for name in sorted(missing)))
        enabled = set(control.read_text().split())
    if not REQUIRED_CONTROLLERS <= enabled:
        raise PreparationError("delegated root could not enable cpu/memory/pids")
    if not os.access(root, os.W_OK) or not os.access(root / "cgroup.procs", os.W_OK):
        raise PreparationError("krim cannot write the dedicated delegated cgroup")
    app = Path("/sys/fs/cgroup") / Path(cg_text.split(":", 2)[2].lstrip("/"))
    if app.parent != root or not app.is_dir():
        raise PreparationError("current process is not in the delegated /app child")
    record(
        "delegated_cgroup_ready",
        cgroup=cg_text,
        root=str(root),
        root_dev=root_info.st_dev,
        root_inode=root_info.st_ino,
        controllers=sorted(controllers),
        subtree_control=sorted(enabled),
        app=str(app),
    )
    return root, {
        "path": str(root),
        "root_dev": root_info.st_dev,
        "root_inode": root_info.st_ino,
        "controllers": sorted(controllers),
        "subtree_control": sorted(enabled),
        "app_cgroup": cg_text,
    }


def read_line(proc: subprocess.Popen[bytes], timeout: float = 5.0) -> bytes:
    """Read one protocol line without losing bytes buffered past its newline."""
    import select

    if proc.stdout is None:
        raise PreparationError("sandbox child has no protocol stream")
    pending = getattr(proc, "_krim_protocol_pending", None)
    if pending is None:
        pending = bytearray()
        proc._krim_protocol_pending = pending
    deadline = time.monotonic() + timeout
    while True:
        newline = pending.find(b"\n")
        if newline >= 0:
            if newline + 1 > 1024 * 1024:
                raise PreparationError("sandbox child protocol line exceeds limit")
            line = bytes(pending[: newline + 1])
            del pending[: newline + 1]
            return line
        if len(pending) >= 1024 * 1024:
            raise PreparationError("sandbox child protocol line exceeds limit")
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([proc.stdout], [], [], remaining)[0]:
            raise PreparationError("sandbox child did not return a protocol line")
        chunk = os.read(proc.stdout.fileno(), min(65536, 1024 * 1024 - len(pending)))
        if not chunk:
            if pending:
                raise PreparationError("sandbox child returned a truncated protocol line")
            raise PreparationError("sandbox child closed its protocol stream")
        pending.extend(chunk)


def parse_mountinfo(raw: str) -> list[list[str]]:
    return [line.split(" - ")[0].split() for line in raw.splitlines()]


def run_cgroup_pidfd_probe(root: Path, material: Path, record) -> dict[str, Any]:
    load_project_modules()
    from krim_agent.execution.cgroups import Leaf
    from krim_agent.execution.types import Profile

    profile = Profile(
        rootfs=ROOT / "rootfs",
        base=material / "not-used-base.json",
        cgroup_root=root,
        material=material,
        evidence=material / "probe-evidence",
        binary=BWRAP,
    )
    leaf = Leaf(root, material / "tickets", record)
    process: subprocess.Popen[bytes] | None = None
    pidfd: int | None = None
    waited = False
    result: dict[str, Any] = {"limits": {}, "cleanup": {}}
    try:
        leaf.limits(profile)
        code = "import time; time.sleep(30)"
        process = subprocess.Popen(
            [str(PYTHON), "-I", "-B", "-c", code],
            cwd=str(Path(__file__).resolve().parents[1]),
            env={"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8"},
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
        )
        record("probe_child_created", pid=process.pid)
        pidfd = os.pidfd_open(process.pid)
        signal.pidfd_send_signal(pidfd, 0)
        record("probe_pidfd_bound", pid=process.pid, pidfd=pidfd)
        leaf.join(process.pid)
        if str(process.pid) not in leaf.members():
            raise PreparationError("inert child was not joined to its owned cgroup leaf")
        (leaf.path / "cgroup.kill").write_text("1")
        record("probe_cgroup_kill_requested", leaf=str(leaf.path))
        stdout, stderr = process.communicate(timeout=10)
        waited = True
        if process.returncode != -signal.SIGKILL:
            raise PreparationError("cgroup.kill did not terminate the inert child")
        if stdout or stderr:
            raise PreparationError("inert cgroup child unexpectedly produced output")
        result = {
            "identity": {**leaf.identity, "path": str(leaf.path)},
            "limits": {
                name: (leaf.path / name).read_text().strip()
                for name in (
                    "cpu.max",
                    "memory.max",
                    "memory.swap.max",
                    "memory.oom.group",
                    "pids.max",
                )
            },
            "child_pid": process.pid,
            "child_exit_code": process.returncode,
            "pidfd_zero_signal": True,
        }
    finally:
        if process is not None and process.poll() is None:
            if leaf.path.exists():
                (leaf.path / "cgroup.kill").write_text("1")
            try:
                process.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate(timeout=5)
        if process is not None and not waited and process.returncode is not None:
            record("probe_child_waited_during_cleanup", pid=process.pid, rc=process.returncode)
        if pidfd is not None:
            os.close(pidfd)
            record("probe_pidfd_closed", pidfd=pidfd)
        if leaf.path.exists():
            import asyncio

            asyncio.run(leaf.finish())
    result["leaf_removed"] = not leaf.path.exists()
    if not result["leaf_removed"]:
        raise PreparationError("probe cgroup leaf was not removed")
    record("cgroup_pidfd_probe_complete", **result)
    return result


def _probe_payload() -> str:
    return """import importlib, json, os, socket, sys
status = {
    key: value.strip()
    for line in open('/proc/self/status')
    if ':' in line
    for key, value in [line.split(':', 1)]
    if key in {'Uid', 'CapEff', 'NoNewPrivs', 'Seccomp'}
}
for module in ('ssl', 'ctypes', 'zlib', 'bz2', 'lzma', 'unittest', 'xml.etree.ElementTree'):
    importlib.import_module(module)
value = {
    'python': sys.version.split()[0],
    'prefix': sys.prefix,
    'uid': os.getuid(),
    'pid': os.getpid(),
    'ppid': os.getppid(),
    'status': status,
    'uid_map': open('/proc/self/uid_map').read().strip(),
    'namespaces': {
        name: os.readlink('/proc/self/ns/' + name)
        for name in ('user', 'mnt', 'pid', 'net', 'ipc', 'uts')
    },
    'host_home_visible': os.path.exists('/home/krim'),
}
try:
    socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    value['socket_denied'] = False
except PermissionError as error:
    value['socket_denied'] = error.errno == 1
open('/tmp/krim-p5-probe', 'wb').write(b'tmpfs')
open('/work/krim-p5-probe.txt', 'wb').write(b'rootfs-ok')
print(json.dumps(value, sort_keys=True), flush=True)
"""


def run_bwrap_seccomp_probe(
    root: Path,
    cgroup_root: Path,
    material: Path,
    rootfs_manifest: dict[str, Any],
    record,
) -> dict[str, Any]:
    load_project_modules()
    from krim_agent.execution import _broker, seccomp
    from krim_agent.execution.cgroups import Leaf
    from krim_agent.execution.types import Profile

    version, binary_hash = ensure_bwrap()
    filter_bytes = seccomp.program()
    filter_hash = sha256_bytes(filter_bytes)
    job_name = "probe-" + uuid.uuid4().hex
    job = material / job_name
    inputs = job / "input"
    inputs.mkdir(parents=True, mode=0o700)
    denied_fd = job / "pid1-fd"
    denied_fd.mkdir(mode=0o000)
    broker_path = job / "broker.py"
    filter_path = job / "filter.bin"
    request_path = job / "request.json"
    broker_path.write_bytes(Path(_broker.__file__).read_bytes())
    broker_path.chmod(0o444)
    filter_path.write_bytes(filter_bytes)
    filter_path.chmod(0o444)
    request = {"argv": ["-c", _probe_payload()], "cwd": ".", "output": PROFILE_LIMITS["output"]}
    request_path.write_text(json.dumps(request, separators=(",", ":")), encoding="utf-8")
    request_path.chmod(0o444)
    asset_hashes = {
        "broker.py": sha256_file(broker_path),
        "filter.bin": sha256_file(filter_path),
        "request.json": sha256_file(request_path),
    }
    profile = Profile(
        rootfs=ROOT / "rootfs",
        base=material / "not-used-base.json",
        cgroup_root=cgroup_root,
        material=material,
        evidence=material / "probe-evidence",
        binary=BWRAP,
        **PROFILE_LIMITS,
    )
    leaf = Leaf(cgroup_root, material / "tickets", record)
    process: subprocess.Popen[bytes] | None = None
    pidfd: int | None = None
    monitor_status: dict[str, str] = {}
    stdout = bytearray()
    stderr = bytearray()
    exit_message: dict[str, Any] | None = None
    exported: list[str] = []
    probe_succeeded = False
    try:
        leaf.limits(profile)
        bwrap = [
            str(BWRAP),
            "--unshare-all",
            "--uid",
            "0",
            "--gid",
            "0",
            "--cap-drop",
            "ALL",
            "--as-pid-1",
            "--new-session",
            "--die-with-parent",
            "--tmpfs",
            "/",
            "--ro-bind",
            str(ROOT / "rootfs/opt"),
            "/opt",
            "--ro-bind",
            str(ROOT / "rootfs/lib"),
            "/lib",
            "--ro-bind",
            str(ROOT / "rootfs/lib64"),
            "/lib64",
            "--ro-bind",
            str(ROOT / "rootfs/usr"),
            "/usr",
            "--ro-bind",
            str(inputs),
            "/input",
            "--ro-bind",
            str(broker_path),
            "/broker.py",
            "--ro-bind",
            str(filter_path),
            "/filter.bin",
            "--ro-bind",
            str(request_path),
            "/request.json",
            "--proc",
            "/proc",
            "--ro-bind",
            str(denied_fd),
            "/proc/1/fd",
            "--remount-ro",
            "/proc",
            "--dev",
            "/dev",
            "--size",
            "4194304",
            "--tmpfs",
            "/tmp",
            "--size",
            "16777216",
            "--tmpfs",
            "/work",
            "--clearenv",
            "--setenv",
            "LD_LIBRARY_PATH",
            "/opt/python/lib",
            "--setenv",
            "PATH",
            "/opt/python/bin",
            "--remount-ro",
            "/",
            "--chdir",
            "/work",
            "--",
            "/opt/python/bin/python3.12",
            "-I",
            "-B",
            "/broker.py",
        ]
        launcher = (
            'import os,sys;os.write(1,b\'{\\"type\\":\\"launcher_ready\\"}\\n\');'
            "assert os.read(0,1)==b'!';os.execve(sys.argv[1],sys.argv[1:],"
            "{'PATH':'/usr/local/bin:/usr/bin:/bin','LANG':'C.UTF-8'})"
        )
        argv = [sys.executable, "-I", "-B", "-c", launcher, *bwrap]
        env = {
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "LD_LIBRARY_PATH": str(Path(sys.base_prefix) / "lib"),
            "LANG": "C.UTF-8",
        }
        record("sandbox_probe_command", argv=argv, env=env, cwd=str(Path.cwd()))
        process = subprocess.Popen(
            argv,
            cwd=str(Path(__file__).resolve().parents[1]),
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
        )
        pidfd = os.pidfd_open(process.pid)
        signal.pidfd_send_signal(pidfd, 0)
        record("sandbox_probe_process_created", pid=process.pid, pidfd=pidfd)
        first = json.loads(read_line(process))
        if first.get("type") != "launcher_ready":
            raise PreparationError("rootless bwrap launcher handshake failed")
        leaf.join(process.pid)
        process.stdin.write(b"!")
        process.stdin.flush()
        ready = json.loads(read_line(process))
        if ready.get("type") != "ready" or ready.get("pid") != 1 or ready.get("dumpable") != 0:
            raise PreparationError("sandbox broker PID1/dumpable self-check failed")
        status = parse_status(ready.get("status", ""))
        if status.get("NoNewPrivs") != "1" or status.get("Seccomp") != "2":
            raise PreparationError("real broker seccomp/NNP installation failed")
        if int(status.get("CapEff", "1"), 16) != 0:
            raise PreparationError("sandbox broker retained effective capabilities")
        mounts = parse_mountinfo(ready.get("mountinfo", ""))
        root_mount = next((parts for parts in mounts if len(parts) > 5 and parts[4] == "/"), None)
        if root_mount is None or "ro" not in root_mount[5].split(","):
            raise PreparationError("sandbox root mount is not read-only")
        if any(
            len(parts) > 4 and parts[4].startswith(("/home", "/mnt", "/sys")) for parts in mounts
        ):
            raise PreparationError("host home/mount/sys path leaked into sandbox mount namespace")
        monitor_status = status
        record("sandbox_broker_ready", pid=ready["pid"], status=status, root_mount=root_mount)
        process.stdin.write(b"gate\n")
        process.stdin.flush()
        while True:
            message = json.loads(read_line(process))
            if message["type"] == "output":
                data = base64.b64decode(message["data"], validate=True)
                (stdout if message.get("stream") == "stdout" else stderr).extend(data)
            elif message["type"] == "exit":
                exit_message = message
            elif message["type"] == "quiescent":
                break
            else:
                raise PreparationError("unexpected broker payload response")
        if (
            exit_message is None
            or exit_message.get("rc") != 0
            or exit_message.get("code") is not None
        ):
            raise PreparationError("rootfs/seccomp harmless payload did not exit cleanly")
        if stderr:
            raise PreparationError("rootfs/seccomp harmless payload wrote stderr")
        try:
            payload = json.loads(stdout.decode("utf-8"))
        except (UnicodeError, ValueError) as error:
            raise PreparationError("rootfs payload returned invalid JSON") from error
        if (
            payload.get("uid") != 0
            or type(payload.get("pid")) is not int
            or payload["pid"] <= 1
            or payload.get("ppid") != 1
            or not payload.get("prefix", "").startswith("/opt/python")
            or payload.get("host_home_visible") is not False
            or payload.get("socket_denied") is not True
            or payload.get("status", {}).get("NoNewPrivs") != "1"
            or payload.get("status", {}).get("Seccomp") != "2"
        ):
            raise PreparationError("rootfs namespace/seccomp/network-denial self-check failed")
        process.stdin.write(b"export\n")
        process.stdin.flush()
        while True:
            message = json.loads(read_line(process))
            if message["type"] == "file_start":
                exported.append(message["name"])
            elif message["type"] == "export_end":
                break
            elif message["type"] in {"chunk", "file_end"}:
                continue
            else:
                raise PreparationError("unexpected broker export response")
        for path, expected in (
            (broker_path, asset_hashes["broker.py"]),
            (filter_path, asset_hashes["filter.bin"]),
            (request_path, asset_hashes["request.json"]),
        ):
            if sha256_file(path) != expected:
                raise PreparationError("sandbox probe material changed during execution")
        process.stdin.close()
        process.wait(timeout=10)
        trailing_stdout = process.stdout.read()
        trailing_stderr = process.stderr.read()
        if trailing_stdout or trailing_stderr:
            raise PreparationError("unexpected data after broker export_end")
        if process.returncode != 0:
            raise PreparationError("rootfs broker did not exit cleanly")
        import asyncio

        asyncio.run(leaf.finish())
        probe_succeeded = True
    except BaseException as error:
        record(
            "sandbox_probe_failed",
            error_type=type(error).__name__,
            error=str(error),
            stdout=base64.b64encode(stdout).decode(),
            stderr=base64.b64encode(stderr).decode(),
        )
        raise
    finally:
        if process is not None and process.poll() is None:
            if leaf.path.exists():
                (leaf.path / "cgroup.kill").write_text("1")
            if process.stdin is not None and not process.stdin.closed:
                process.stdin.close()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            if process.stdout is not None:
                process.stdout.read()
            if process.stderr is not None:
                process.stderr.read()
        if pidfd is not None:
            os.close(pidfd)
            record("sandbox_probe_pidfd_closed", pidfd=pidfd)
        if leaf.path.exists():
            import asyncio

            asyncio.run(leaf.finish())
        if probe_succeeded:
            # 进程已收束后才恢复宿主清理权限，不改变探针执行时的拒绝目录。
            denied_fd.chmod(0o700)
            shutil.rmtree(job, ignore_errors=False)
        else:
            record("sandbox_probe_material_retained", path=str(job))

    if leaf.path.exists():
        raise PreparationError("sandbox probe cgroup leaf cleanup failed")
    expected_export = ["krim-p5-probe.txt"]
    if exported != expected_export:
        raise PreparationError("sandbox probe artifact export did not match")
    if payload.get("python") != rootfs_manifest["python"]["version"]:
        raise PreparationError("rootfs Python identity mismatch")
    if sha256_file(BWRAP) != binary_hash:
        raise PreparationError("bwrap binary changed during self-check")
    if sha256_bytes(seccomp.program()) != filter_hash:
        raise PreparationError("seccomp program changed during self-check")
    result = {
        "bwrap": str(BWRAP),
        "version": version,
        "binary_sha256": binary_hash,
        "seccomp_program_sha256": filter_hash,
        "broker_status": monitor_status,
        "payload": payload,
        "payload_exit": exit_message,
        "exported": exported,
        "leaf_removed": True,
    }
    record("bwrap_seccomp_rootfs_probe_complete", **result)
    return result


def write_profile_files(
    run_root: Path,
    rootfs: Path,
    cgroup_root: Path,
    cgroup_probe: dict[str, Any],
    namespace_probe: dict[str, Any],
    manifest: dict[str, Any],
    record,
) -> tuple[Path, Path]:
    load_project_modules()
    from krim_agent.execution.types import Profile
    from krim_agent.workspace.targets import WorkspaceConfiguration, environment
    from krim_agent.workspace.types import WorkspaceProfile

    material = run_root / "materials/d1-os"
    evidence = run_root / "evidence/d1-os"
    prep_evidence = run_root / "evidence/prep-base"
    for path in (material, evidence, prep_evidence, run_root / "artifacts"):
        ensure_private_directory(path, owner=os.getuid())
    base_path = prep_evidence / "BASE-QUALIFIED.json"
    root_stat = cgroup_root.stat()
    leaf_identity = cgroup_probe["identity"]
    if Path(leaf_identity["path"]).parent != cgroup_root:
        raise PreparationError("qualification leaf is not directly below cgroup_root")
    if (leaf_identity["root_dev"], leaf_identity["root_inode"]) != (
        root_stat.st_dev,
        root_stat.st_ino,
    ):
        raise PreparationError("cgroup root identity changed during qualification")
    base = {
        "schema": "krim-dev-sandbox-base/1",
        "status": "BASE-QUALIFIED",
        "created_at": datetime.now(UTC).isoformat(),
        "identity": {
            "uid": os.getuid(),
            "gid": os.getgid(),
            "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            "status": parse_status(Path("/proc/self/status").read_text()),
            "cgroup": Path("/proc/self/cgroup").read_text().strip(),
        },
        "rootfs": {
            "path": str(rootfs),
            "manifest": str(rootfs / "manifest.json"),
            "manifest_sha256": sha256_file(rootfs / "manifest.json"),
            "files": len(manifest["files"]),
        },
        "namespace": {
            "binary": str(BWRAP),
            "sha256": namespace_probe["binary_sha256"],
            "version": namespace_probe["version"],
            "probe": namespace_probe,
        },
        "cgroup_pidfd": {
            "identity": leaf_identity,
            "limits": cgroup_probe["limits"],
            "cleanup": {
                "child_exit_code": cgroup_probe["child_exit_code"],
                "leaf_removed": cgroup_probe["leaf_removed"],
                "pidfd_zero_signal": cgroup_probe["pidfd_zero_signal"],
            },
            "delegation": {
                "root": str(cgroup_root),
                "root_dev": root_stat.st_dev,
                "root_inode": root_stat.st_ino,
                "app_cgroup": Path("/proc/self/cgroup").read_text().strip(),
            },
        },
        "source": {
            "project_root": str(Path(__file__).resolve().parents[1]),
            "python": sys.version.split()[0],
            "preparation_script_sha256": sha256_file(Path(__file__)),
        },
    }
    write_json_exclusive(base_path, base)
    execution = Profile(
        rootfs=rootfs,
        base=base_path,
        cgroup_root=cgroup_root,
        material=material,
        evidence=evidence,
        binary=BWRAP,
        **PROFILE_LIMITS,
    )
    profile = WorkspaceProfile(max_tools_per_step=1, environment=environment(execution))
    value = {
        "profile": profile.model_dump(mode="json"),
        "execution": {
            "rootfs": str(execution.rootfs),
            "base": str(execution.base),
            "cgroup_root": str(execution.cgroup_root),
            "material": str(execution.material),
            "evidence": str(execution.evidence),
            "wall": execution.wall,
            "cpu_seconds": execution.cpu_seconds,
            "memory": execution.memory,
            "pids": execution.pids,
            "output": execution.output,
            "binary": str(execution.binary),
        },
        "artifact_root": str(run_root / "artifacts"),
    }
    config_path = run_root / "workspace-profile.json"
    write_json_exclusive(config_path, value)
    WorkspaceConfiguration.load(config_path)
    record(
        "workspace_configuration_validated",
        base=str(base_path),
        workspace_profile=str(config_path),
        artifact_root=value["artifact_root"],
        profile_limits=PROFILE_LIMITS,
    )
    return base_path, config_path


def qualify_and_hold() -> int:
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    run_root = ROOT / "runs" / run_id
    ensure_private_directory(ROOT, owner=1000)
    ensure_private_directory(ROOT / "runs", owner=1000)
    ensure_private_directory(run_root, owner=1000)
    ensure_private_directory(run_root / "materials", owner=1000)
    ensure_private_directory(run_root / "materials/d1-os", owner=1000)
    log_path = run_root / "evidence/preparation/events.jsonl"
    record = create_event_recorder(log_path)
    before_fds = fds()
    base_path: Path | None = None
    profile_path: Path | None = None
    try:
        if os.getuid() != 1000 or os.geteuid() != 1000 or os.geteuid() == 0:
            raise PreparationError("qualify must run as the dedicated non-root krim user UID 1000")
        if sys.version_info[:2] != (3, 12):
            raise PreparationError("qualify requires the installed Python 3.12 runtime")
        status = parse_status(Path("/proc/self/status").read_text())
        if any(int(status.get(key, "1"), 16) != 0 for key in ("CapEff", "CapPrm", "CapAmb")):
            raise PreparationError("parent process has unexpected capabilities")
        if status.get("NoNewPrivs") != "1":
            raise PreparationError("systemd unit must set NoNewPrivileges=yes")
        root, delegation = cgroup_context(record)
        version, binary_hash = ensure_bwrap()
        manifest = build_rootfs(ROOT)
        rootfs = ROOT / "rootfs"
        verify_manifest(rootfs, manifest["files"])
        cgroup_probe = run_cgroup_pidfd_probe(root, run_root / "materials/d1-os", record)
        namespace_probe = run_bwrap_seccomp_probe(
            root, root, run_root / "materials/d1-os", manifest, record
        )
        if fds() != before_fds:
            raise PreparationError(
                "preparation process file descriptors did not return to baseline"
            )
        if owned_children():
            raise PreparationError("qualification process retained an owned child")
        if (root / "cgroup.procs").read_text().strip():
            raise PreparationError("delegated cgroup root is not empty after probes")
        if any(
            name.startswith("krim-d1-") or name.startswith("prep-") for name in os.listdir(root)
        ):
            raise PreparationError("delegated cgroup root contains an unremoved probe leaf")
        if list((run_root / "materials/d1-os/tickets").glob("*")):
            raise PreparationError("cgroup cleanup tickets remain")
        base_path, profile_path = write_profile_files(
            run_root,
            rootfs,
            root,
            cgroup_probe,
            namespace_probe,
            manifest,
            record,
        )
        result = {
            "status": "BASE-QUALIFIED",
            "run_root": str(run_root),
            "base": str(base_path),
            "workspace_profile": str(profile_path),
            "cgroup_root": str(root),
            "delegation": delegation,
            "bwrap_version": version,
            "bwrap_sha256": binary_hash,
            "rootfs_manifest_sha256": sha256_file(rootfs / "manifest.json"),
            "rootfs_files": len(manifest["files"]),
            "cgroup_pidfd": cgroup_probe,
            "namespace_seccomp_rootfs": namespace_probe,
            "service_must_remain_active": True,
        }
        write_json_exclusive(run_root / "evidence/preparation/result.json", result)
        record("qualification_complete", **result)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
        return hold_service(run_root, base_path, profile_path, record)
    except BaseException as error:
        failure = {
            "status": "NOT_READY",
            "error_type": type(error).__name__,
            "error": str(error),
            "run_root": str(run_root),
        }
        record("qualification_failed", **failure)
        try:
            write_json_exclusive(run_root / "evidence/preparation/NOT_READY.json", failure)
        except FileExistsError:
            pass
        if base_path is not None:
            base_path.unlink(missing_ok=True)
        if profile_path is not None:
            profile_path.unlink(missing_ok=True)
        print(json.dumps(failure, ensure_ascii=False, sort_keys=True), flush=True)
        return 1


def hold_service(run_root: Path, base_path: Path, profile_path: Path, record) -> int:
    """Keep the systemd-owned cgroup inode alive; invalidate profile on stop."""
    stopping = False

    def request_stop(signum, _frame):
        nonlocal stopping
        stopping = True
        record("service_stop_requested", signal=signum)

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        while not stopping:
            signal.pause()
    finally:
        # A base record bound to a vanished cgroup inode must never remain usable.
        base_path.unlink(missing_ok=True)
        profile_path.unlink(missing_ok=True)
        record("service_profile_invalidated", reason="delegated systemd service stopped")
        write_json_exclusive(
            run_root / "evidence/preparation/PROFILE-INVALIDATED.json",
            {"status": "PROFILE-INVALIDATED", "reason": "delegated systemd service stopped"},
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("build-rootfs", help="build only the installed-Python rootfs")
    sub.add_parser("qualify", help="qualify from delegated cgroup_root/app and hold it active")
    args = parser.parse_args(argv)
    if args.command == "build-rootfs":
        if os.getuid() != 1000 or os.geteuid() != 1000:
            parser.error("build-rootfs must run as krim UID 1000, not root")
        run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
        ensure_private_directory(ROOT, owner=1000)
        evidence = ROOT / "evidence"
        ensure_private_directory(evidence, owner=1000)
        try:
            manifest = build_rootfs(ROOT)
            result = {
                "status": "ROOTFS-BUILT",
                "rootfs": str(ROOT / "rootfs"),
                "manifest": str(ROOT / "rootfs/manifest.json"),
                "manifest_sha256": sha256_file(ROOT / "rootfs/manifest.json"),
                "files": len(manifest["files"]),
                "python": manifest["python"],
            }
            write_json_exclusive(evidence / ("rootfs-build-" + run_id + ".json"), result)
            print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
            return 0
        except Exception as error:
            result = {
                "status": "NOT_READY",
                "error_type": type(error).__name__,
                "error": str(error),
                "staging_directories": sorted(str(path) for path in ROOT.glob("rootfs.staging-*")),
            }
            write_json_exclusive(evidence / ("rootfs-build-" + run_id + ".json"), result)
            print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
            return 1
    return qualify_and_hold()


if __name__ == "__main__":
    raise SystemExit(main())
