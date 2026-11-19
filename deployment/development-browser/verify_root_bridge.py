"""Check root-host to UID1000-container UDS permissions, not preview authorization.

No web listener or app is started. The CHECK callback is an explicit transport
fixture; production recheck belongs to I's original principal/record/lease.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import stat
import uuid
from pathlib import Path

from krim_agent.dev_browser.host_bridge import PreviewConnectBridge
from krim_agent.dev_browser.launcher import DOCKER, SECCOMP_SHA256

HOST = "p-permission-fixture.preview.example"
MOUNTED = "/run/krim-preview/bridge.sock"
CLIENT = r"""
import json,os,socket
assert (os.getuid(),os.getgid()) == (1000,1000)
def check(host):
    with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as s:
        s.settimeout(3)
        s.connect("/run/krim-preview/bridge.sock")
        s.sendall(("KRIM-PREVIEW/1 CHECK "+host+" 443\n").encode())
        return s.recv(3).decode()
assert check("p-permission-fixture.preview.example") == "OK\n"
assert check("p-foreign.preview.example") == "NO\n"
assert check("p-permission-fixture.preview.example") == "NO\n"
print(json.dumps({"uid":os.getuid(),"gid":os.getgid(),"connect":True,
                  "foreign_host_denied":True,"recheck_denied":True}))
"""


async def docker(*args: str, seconds: int = 15) -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        *DOCKER,
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={"PATH": "/usr/bin:/bin", "HOME": "/nonexistent", "LC_ALL": "C"},
    )
    try:
        async with asyncio.timeout(seconds):
            out, err = await proc.communicate()
        return proc.returncode or 0, out.decode(), err.decode()
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()


def prepare(image: str, profile: Path, evidence: Path) -> tuple[Path, int]:

    if os.getuid() != 0 or not re.fullmatch(r"sha256:[a-f0-9]{64}", image):
        raise ValueError("approved_management_identity_required")
    if hashlib.sha256(profile.read_bytes()).hexdigest() != SECCOMP_SHA256:
        raise ValueError("profile_mismatch")
    if evidence.exists():
        raise ValueError("evidence_must_be_new")
    evidence.mkdir(mode=0o700, parents=True)
    root = Path("/run/krim-browser")
    if not root.exists():
        root.mkdir(mode=0o700)
    if root.is_symlink() or root.stat().st_uid != 0 or stat.S_IMODE(root.stat().st_mode) != 0o700:
        raise ValueError("root_bridge_directory_invalid")
    parent = root / ("permission-" + uuid.uuid4().hex)
    parent.mkdir(mode=0o700)
    return parent, parent.stat().st_ino


async def verify(image: str, profile: Path, evidence: Path) -> dict[str, object]:
    parent, parent_inode = prepare(image, profile, evidence)
    path = parent / "bridge.sock"
    cid = None
    calls = 0
    outcome: dict[str, object] = {
        "image": image,
        "profile_sha256": SECCOMP_SHA256,
        "container_removed": False,
        "authorization_is_fixture": True,
    }

    async def authority() -> None:
        nonlocal calls
        calls += 1
        if calls > 1:
            raise PermissionError("fixture_denied")

    try:
        async with PreviewConnectBridge(
            str(path), HOST, authority, expected_uid=1000, expected_gid=1000
        ):
            node = path.lstat()
            assert (node.st_uid, node.st_gid, stat.S_IMODE(node.st_mode)) == (0, 1000, 0o660)
            outcome["socket"] = {
                "uid": node.st_uid,
                "gid": node.st_gid,
                "mode": oct(stat.S_IMODE(node.st_mode)),
            }
            outcome["parent"] = {
                "uid": parent.stat().st_uid,
                "mode": oct(stat.S_IMODE(parent.stat().st_mode)),
            }
            # The host's same UID cannot traverse the private root directory.
            # It must not inherit the container's deliberate one-inode mount.
            code = (
                "import socket,sys; s=socket.socket(socket.AF_UNIX);"
                "\ntry:s.connect(sys.argv[1])"
                "\nexcept PermissionError:sys.exit(0)"
                "\nelse:sys.exit(1)"
            )
            host_attempt = await asyncio.create_subprocess_exec(
                "/usr/bin/python3",
                "-I",
                "-c",
                code,
                str(path),
                user=1000,
                group=1000,
                extra_groups=(),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                async with asyncio.timeout(5):
                    await host_attempt.communicate()
                assert host_attempt.returncode == 0
            finally:
                if host_attempt.returncode is None:
                    host_attempt.kill()
                    await host_attempt.wait()
            outcome["host_uid1000_parent_traversal_denied"] = True
            name = "krim-browser-uds-" + uuid.uuid4().hex
            args = (
                "create",
                "--name=" + name,
                "--pull=never",
                "--runtime=runc",
                "--user=1000:1000",
                "--cap-drop=ALL",
                "--security-opt=no-new-privileges",
                "--security-opt=seccomp=" + str(profile),
                "--network=none",
                "--read-only",
                "--init",
                "--restart=no",
                "--log-driver=none",
                "--memory=1073741824",
                "--memory-swap=1073741824",
                "--cpus=2",
                "--pids-limit=128",
                "--ipc=private",
                "--shm-size=134217728",
                "--ulimit=nofile=256:256",
                "--env=NODE_EXTRA_CA_CERTS=",
                "--mount=type=bind,source="
                + str(path)
                + ",target="
                + MOUNTED
                + ",readonly,bind-propagation=rprivate",
                "--entrypoint=/opt/krim-browser/venv/bin/python",
                image,
                "-I",
                "-c",
                CLIENT,
            )
            rc, out, _ = await docker(*args)
            if rc:
                raise RuntimeError("uds_container_create_failed")
            cid = out.strip()
            if not re.fullmatch(r"[a-f0-9]{64}", cid):
                raise ValueError("container_id_invalid")
            outcome["container_id"] = cid
            rc, out, _ = await docker("inspect", cid)
            assert rc == 0
            record = json.loads(out)[0]
            assert record["Image"] == image and record["Name"] == "/" + name
            assert record["Config"]["User"] == "1000:1000"
            assert record["HostConfig"]["NetworkMode"] == "none"
            mounts = record["Mounts"]
            assert len(mounts) == 1 and mounts[0]["Source"] == str(path) and not mounts[0]["RW"]
            rc, out, err = await docker("start", "--attach", cid, seconds=20)
            (evidence / "client-stdout.txt").write_text(out)
            (evidence / "client-stderr.txt").write_text(err[:4096])
            assert rc == 0 and calls == 2
            outcome["client"] = json.loads(out)
            outcome["authority_checks"] = calls
            assert (
                parent.stat().st_ino == parent_inode
                and stat.S_IMODE(parent.stat().st_mode) == 0o700
            )
    finally:
        try:
            if cid:
                rc, out, _ = await docker("inspect", cid)
                if rc == 0:
                    current = json.loads(out)[0]
                    if current["Image"] != image or current["Name"] != "/" + name:
                        raise RuntimeError("container_binding_changed")
                    rc, _, _ = await docker("rm", "--force", cid)
                    gone, _, _ = await docker("inspect", cid)
                    outcome["container_removed"] = rc == 0 and gone != 0
        finally:
            outcome["socket_removed"] = not path.exists()
            if (
                parent.exists()
                and parent.stat().st_ino == parent_inode
                and not any(parent.iterdir())
            ):
                parent.rmdir()
            outcome["session_directory_removed"] = not parent.exists()
            (evidence / "result.json").write_text(json.dumps(outcome, indent=2) + "\n")
    if not outcome["container_removed"] or not outcome["socket_removed"]:
        raise RuntimeError("uds_cleanup_unconfirmed")
    return outcome


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(verify(args.image, args.profile, args.evidence)), indent=2))
