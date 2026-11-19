"""Run only the fixed no-network browser probe under approved container limits.

No web service, preview listener or model application is started. Requires the
already-approved host Docker management entrypoint and a locally built image.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import time
import uuid
from pathlib import Path

DOCKER = ["/usr/bin/docker", "--host=unix:///var/run/docker.sock"]
# Approved 2026-11-15: only remove chroot's CAP_SYS_CHROOT includes condition.
SECCOMP_SHA = "f922c7c9bfc1ece0b72b244e56dc85756ded002c837fb46054cfd8b275c74b6b"


def run(args: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
    return subprocess.run([*DOCKER, *args], text=True, capture_output=True, check=False, **kwargs)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True)
    parser.add_argument("--seccomp", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument(
        "--probe", choices=("sandbox", "namespace", "ttl", "network"), default="sandbox"
    )
    parser.add_argument("--partial-stdin", action="store_true")
    args = parser.parse_args()
    if not re.fullmatch(r"sha256:[a-f0-9]{64}", args.image):
        raise ValueError("image_must_be_fixed_id")
    if hashlib.sha256(args.seccomp.read_bytes()).hexdigest() != SECCOMP_SHA:
        raise ValueError("seccomp_hash_invalid")
    if args.evidence.exists():
        raise ValueError("evidence_must_be_new")
    args.evidence.mkdir(mode=0o700, parents=True)
    name = "krim-browser-probe-" + uuid.uuid4().hex
    probe = Path(__file__).with_name(args.probe + "_probe.py").read_text()
    command = [
        "create",
        "--name=" + name,
        "--pull=never",
        "--runtime=runc",
        "--user=1000:1000",
        "--cap-drop=ALL",
        "--security-opt=no-new-privileges",
        "--security-opt=seccomp=" + str(args.seccomp),
        "--network=none",
        "--read-only",
        "--init",
        "--restart=no",
        "--log-driver=none",
        "--stop-timeout=1",
        "--no-healthcheck",
        "--memory=1073741824",
        "--memory-swap=1073741824",
        "--cpus=2",
        "--pids-limit=128",
        "--shm-size=134217728",
        "--ipc=private",
        "--ulimit=nofile=256:256",
        "--tmpfs=/tmp:rw,nosuid,nodev,size=134217728,mode=1777",
        "--tmpfs=/tmp/krim-home:rw,nosuid,nodev,size=33554432,uid=1000,gid=1000,mode=700",
        "--env=HOME=/tmp/krim-home",
        "--env=NODE_EXTRA_CA_CERTS=",
        "--entrypoint=/opt/krim-browser/venv/bin/python",
        args.image,
        "-I",
        "-c",
        probe,
    ]
    if args.probe == "ttl":
        command.insert(1, "--interactive")
    elif args.partial_stdin:
        raise ValueError("partial_stdin_only_for_ttl")
    container_id = None
    outcome: dict[str, object] = {
        "name": name,
        "image": args.image,
        "probe_sha256": hashlib.sha256(probe.encode()).hexdigest(),
        "seccomp_sha256": SECCOMP_SHA,
        "started": False,
        "removed": False,
    }
    try:
        created = run(command, timeout=30)
        if created.returncode:
            outcome["create_error"] = created.stderr[:2048]
            return 1
        container_id = created.stdout.strip()
        if not re.fullmatch(r"[a-f0-9]{64}", container_id):
            raise ValueError("container_id_invalid")
        outcome["container_id"] = container_id
        before = json.loads(run(["inspect", container_id], timeout=15).stdout)[0]
        host, config = before["HostConfig"], before["Config"]
        assert before["Image"] == args.image and config["User"] == "1000:1000"
        assert host["NetworkMode"] == "none" and host["ReadonlyRootfs"]
        assert host["Runtime"] == "runc" and host["CapDrop"] == ["ALL"]
        assert not host.get("Privileged") and not host.get("CapAdd")
        assert all(
            m["Type"] == "tmpfs" and m["Destination"] in {"/tmp", "/tmp/krim-home"}
            for m in before["Mounts"]
        )
        assert host["Memory"] == host["MemorySwap"] == 1073741824
        assert host["NanoCpus"] == 2000000000 and host["PidsLimit"] == 128
        outcome["limits"] = {
            key: host[key]
            for key in (
                "NetworkMode",
                "ReadonlyRootfs",
                "Runtime",
                "CapDrop",
                "SecurityOpt",
                "Memory",
                "MemorySwap",
                "NanoCpus",
                "PidsLimit",
                "ShmSize",
                "IpcMode",
                "Ulimits",
                "Tmpfs",
            )
        }
        start = time.monotonic()
        if args.probe == "ttl":
            process = subprocess.Popen(
                [*DOCKER, "start", "--attach", "--interactive", container_id],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                if args.partial_stdin:
                    process.stdin.write('{"op":')
                    process.stdin.flush()
                # Keep the stdin pipe open while waiting; communicate() would
                # close it and falsely turn the idle test into an EOF test.
                process.wait(timeout=125)
                completed = subprocess.CompletedProcess(
                    process.args,
                    process.returncode,
                    process.stdout.read(),
                    process.stderr.read(),
                )
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
                for stream in (process.stdin, process.stdout, process.stderr):
                    stream.close()
        else:
            completed = run(["start", "--attach", container_id], timeout=120)
        outcome["started"] = True
        outcome["elapsed_seconds"] = round(time.monotonic() - start, 3)
        outcome["attach_exit"] = completed.returncode
        # Only the fixed no-network fixture and internal sandbox status are logged.
        (args.evidence / "probe-stdout.txt").write_text(completed.stdout)
        (args.evidence / "probe-stderr.txt").write_text(completed.stderr[:8192])
        after = json.loads(run(["inspect", container_id], timeout=15).stdout)[0]
        outcome["state"] = {
            k: after["State"][k] for k in ("Status", "Running", "ExitCode", "OOMKilled")
        }
        return 0 if completed.returncode == 0 and after["State"]["ExitCode"] == 0 else 1
    finally:
        if container_id is not None:
            current = run(["inspect", container_id], timeout=15)
            if current.returncode == 0:
                record = json.loads(current.stdout)[0]
                if record["Image"] == args.image and record["Name"] == "/" + name:
                    removed = run(["rm", "--force", container_id], timeout=15)
                    outcome["removed"] = (
                        removed.returncode == 0
                        and run(
                            ["inspect", container_id],
                            timeout=15,
                        ).returncode
                        != 0
                    )
        (args.evidence / "result.json").write_text(json.dumps(outcome, indent=2) + "\n")
        print(json.dumps({k: v for k, v in outcome.items() if k != "limits"}, indent=2))
        if container_id is not None and not outcome["removed"]:
            raise RuntimeError("probe_container_cleanup_unconfirmed")


if __name__ == "__main__":
    raise SystemExit(main())
