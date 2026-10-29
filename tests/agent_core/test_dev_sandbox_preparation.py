from __future__ import annotations

import hashlib
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import prepare_dev_sandbox as prep


def test_read_line_preserves_protocol_lines_read_ahead_from_pipe() -> None:
    read_fd, write_fd = os.pipe()
    reader = os.fdopen(read_fd, "rb")
    process = SimpleNamespace(stdout=reader)
    try:
        os.write(write_fd, b'{"type":"first"}\n{"type":"second"}\n')
        os.close(write_fd)
        write_fd = -1
        assert prep.read_line(process, timeout=0.1) == b'{"type":"first"}\n'
        assert prep.read_line(process, timeout=0.1) == b'{"type":"second"}\n'
    finally:
        reader.close()
        if write_fd >= 0:
            os.close(write_fd)


def test_parse_ldd_dependencies_ignores_vdso_and_returns_resolved_paths() -> None:
    output = """
\tlinux-vdso.so.1 (0x00007fff0000)
\tlibc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x00007f000000)
\t/lib64/ld-linux-x86-64.so.2 (0x00007f000001)
"""
    assert prep.parse_ldd_dependencies(output) == (
        "/lib/x86_64-linux-gnu/libc.so.6",
        "/lib64/ld-linux-x86-64.so.2",
    )


def test_parse_ldd_rejects_missing_dependency() -> None:
    with pytest.raises(prep.PreparationError, match="not installed"):
        prep.parse_ldd_dependencies("libmissing.so => not found")


def test_safe_relative_rejects_escape_and_noncanonical_paths() -> None:
    assert prep.safe_relative("opt/python/bin/python3.12") == "opt/python/bin/python3.12"
    for value in ("../etc/passwd", "/etc/passwd", "a/./b", "a//b", "a\\b"):
        with pytest.raises(prep.PreparationError):
            prep.safe_relative(value)


def test_stdlib_filter_excludes_bytecode_and_site_packages() -> None:
    assert prep.stdlib_candidate("json/__init__.py")
    assert not prep.stdlib_candidate("json/__pycache__/__init__.cpython-312.pyc")
    assert not prep.stdlib_candidate("site-packages/third_party.py")
    assert not prep.stdlib_candidate("dist-packages/third_party.py")
    assert not prep.stdlib_candidate("module.pyc")
    assert not prep.stdlib_candidate("module.pyo")
    assert not prep.stdlib_candidate("sitecustomize.py")
    assert not prep.stdlib_candidate("usercustomize.py")


def test_derive_cgroup_root_requires_app_subgroup() -> None:
    assert prep.derive_cgroup_root("0::/system.slice/krim-agent-p5.service/app") == Path(
        "/sys/fs/cgroup/system.slice/krim-agent-p5.service"
    )
    for value in (
        "0::/system.slice/krim-agent-p5.service",
        "0::/system.slice/krim-agent-p5.service/app/child",
        "1:name=systemd:/system.slice/krim-agent-p5.service/app",
        "0::relative/app",
    ):
        with pytest.raises(prep.PreparationError):
            prep.derive_cgroup_root(value)


def test_parse_status_extracts_only_security_fields() -> None:
    value = prep.parse_status(
        "Name:\tpython\nUid:\t1000\t1000\t1000\t1000\n"
        "CapEff:\t0000000000000000\nNoNewPrivs:\t1\nSeccomp:\t2\n"
    )
    assert value == {
        "Uid": "1000\t1000\t1000\t1000",
        "CapEff": "0000000000000000",
        "NoNewPrivs": "1",
        "Seccomp": "2",
    }


def test_verify_manifest_accepts_only_readonly_regular_matching_files(tmp_path: Path) -> None:
    rootfs = tmp_path / "rootfs"
    binary = rootfs / "opt/python/bin/python3.12"
    binary.parent.mkdir(parents=True)
    binary.write_bytes(b"trusted runtime")
    binary.chmod(0o555)
    for directory in (binary.parent, binary.parent.parent, binary.parent.parent.parent, rootfs):
        directory.chmod(0o555)
    files = {
        "opt/python/bin/python3.12": {
            "bytes": len(b"trusted runtime"),
            "sha256": hashlib.sha256(b"trusted runtime").hexdigest(),
        }
    }
    prep.verify_manifest(rootfs, files)

    binary.chmod(0o755)
    with pytest.raises(prep.PreparationError, match="writable"):
        prep.verify_manifest(rootfs, files)
    binary.chmod(0o555)
    for directory in (binary.parent, binary.parent.parent, binary.parent.parent.parent, rootfs):
        directory.chmod(0o755)


def test_verify_manifest_rejects_symlinks(tmp_path: Path) -> None:
    rootfs = tmp_path / "rootfs"
    rootfs.mkdir()
    target = tmp_path / "outside"
    target.write_bytes(b"not inside rootfs")
    link = rootfs / "outside"
    link.symlink_to(target)
    rootfs.chmod(0o555)
    files = {
        "outside": {
            "bytes": target.stat().st_size,
            "sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
        }
    }
    with pytest.raises((OSError, prep.PreparationError)):
        prep.verify_manifest(rootfs, files)
    rootfs.chmod(0o755)


def test_write_json_exclusive_does_not_overwrite(tmp_path: Path) -> None:
    path = tmp_path / "evidence.json"
    prep.write_json_exclusive(path, {"status": "NOT_READY"})
    assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        prep.write_json_exclusive(path, {"status": "BASE-QUALIFIED"})
