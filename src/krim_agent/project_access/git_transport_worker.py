"""Fixed trusted host worker: attach one accepted Git CLI and promote clone.

Configuration is operator-written metadata in a private operation directory. It
contains no credentials. This program never evaluates a command string.
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path


def promote(source: Path, target: Path, device: int, inode: int, ref: str, oid: str) -> None:
    # Quarantine does not become a host repository until native Git verifies
    # the exact approved ref. No project can read this operation-only mount.
    checked = subprocess.run(
        [
            "/usr/bin/git",
            "-c",
            f"safe.directory={source}",
            "--git-dir",
            str(source),
            "-c",
            "core.hooksPath=/dev/null",
            "rev-parse",
            "--verify",
            ref,
        ],
        env={
            "PATH": "/usr/bin:/bin",
            "HOME": "/nonexistent",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": "/dev/null",
        },
        capture_output=True,
        timeout=10,
        check=True,
    )
    if checked.stdout != (oid + "\n").encode("ascii"):
        raise ValueError
    advertised = subprocess.run(
        [
            "/usr/bin/git",
            "-c",
            f"safe.directory={source}",
            "--git-dir",
            str(source),
            "-c",
            "core.hooksPath=/dev/null",
            "for-each-ref",
            "--format=%(refname)",
        ],
        env={
            "PATH": "/usr/bin:/bin",
            "HOME": "/nonexistent",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": "/dev/null",
        },
        capture_output=True,
        timeout=10,
        check=True,
    )
    if advertised.stdout != (ref + "\n").encode("ascii"):
        raise ValueError
    current = target.lstat()
    if (current.st_dev, current.st_ino) != (device, inode) or not stat.S_ISDIR(current.st_mode):
        raise ValueError
    if target.is_symlink() or target.resolve() != target or any(target.iterdir()):
        raise ValueError
    for base, dirs, files in os.walk(source, followlinks=False):
        for name in (*dirs, *files):
            value = Path(base) / name
            info = value.lstat()
            if not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
                raise ValueError
    shutil.copytree(source, target, dirs_exist_ok=True)
    for base, _dirs, files in os.walk(target):
        Path(base).chmod(0o700)
        for name in files:
            (Path(base) / name).chmod(0o600)


def main() -> int:
    phase = "config"
    status = None
    try:
        config = Path(sys.argv[1])
        if config.resolve() != config or config.stat().st_mode & 0o077:
            raise ValueError
        value = json.loads(config.read_text())
        status = config.with_suffix(".status")
        phase = "attach"
        # The worker is one process group with Docker attach. The lease's host
        # watch also kills the exact container if this worker is killed.
        result = subprocess.run(
            value["argv"],
            env={"PATH": "/usr/bin:/bin", "HOME": "/nonexistent", "LC_ALL": "C"},
            check=False,
        )
        if result.returncode == 0 and value["promote"] is not None:
            phase = "promote"
            item = value["promote"]
            promote(
                Path(item["source"]),
                Path(item["target"]),
                item["device"],
                item["inode"],
                item["ref"],
                item["object_id"],
            )
        status.write_text(json.dumps({"phase": "complete", "code": result.returncode}))
        return result.returncode
    except Exception:
        if status is not None:
            status.write_text(json.dumps({"phase": phase, "code": 125}))
        # Raw subprocess/daemon output and filesystem paths are not errors to
        # return to a model. The native runner emits its bounded ServiceError.
        return 125


if __name__ == "__main__":
    raise SystemExit(main())
