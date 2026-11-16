"""Diagnose approved namespace/chroot calls without changing the seccomp policy."""

import ctypes
import errno
import json
import os
from pathlib import Path


def status() -> dict[str, str]:
    names = {"Uid", "Gid", "CapEff", "CapPrm", "CapBnd", "NoNewPrivs", "Seccomp", "Seccomp_filters"}
    return {
        k: v.strip()
        for line in Path("/proc/self/status").read_text().splitlines()
        if ":" in line
        for k, v in [line.split(":", 1)]
        if k in names
    }


def main() -> None:
    uid, gid = os.getuid(), os.getgid()
    assert (uid, gid) == (1000, 1000)
    report = {"before": status(), "fdinfo_exists": Path("/proc/self/fdinfo").is_dir()}
    libc = ctypes.CDLL(None, use_errno=True)
    result = libc.unshare(ctypes.c_int(0x10000000))  # CLONE_NEWUSER, already allowed
    report["unshare"] = {"result": result, "errno": ctypes.get_errno() if result else 0}
    if result == 0:
        Path("/proc/self/setgroups").write_text("deny")
        Path("/proc/self/uid_map").write_text(f"0 {uid} 1")
        Path("/proc/self/gid_map").write_text(f"0 {gid} 1")
        report["inside_user_namespace"] = status()
        ctypes.set_errno(0)
        result = libc.chroot(ctypes.c_char_p(b"/proc/self/fdinfo/"))
        error = ctypes.get_errno() if result else 0
        report["chroot"] = {
            "result": result,
            "errno": error,
            "errno_name": errno.errorcode.get(error),
        }
    print(json.dumps(report, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
