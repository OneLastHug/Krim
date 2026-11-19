"""Inspect one installed Docker/runsc image without pulling or installing anything.

Run on the trusted Linux host, never expose this Docker access to the agent.
This tool reports compatibility evidence; it does not provision a dev environment.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

CHECK = r'''
import json, os, shutil, subprocess
os.environ["PATH"] = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
names = ["sh", "bash", "git", "node", "npm", "python3"]
paths = {name: shutil.which(name) for name in names}
paths["python3"] = paths["python3"] or shutil.which("python3.12")
versions = {}
for name in ["git", "node", "npm", "python3"]:
    if paths[name]:
        result = subprocess.run([paths[name], "--version"], capture_output=True, text=True)
        versions[name] = {"code": result.returncode, "output": result.stdout.strip()}
print(json.dumps({"uid": os.getuid(), "tools": paths, "versions": versions,
                  "docker_socket_visible": os.path.exists("/var/run/docker.sock")}))
'''


def run(args: list[str], timeout: int = 45) -> dict[str, object]:
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return {"command": args, "code": result.returncode,
                "stdout": result.stdout, "stderr": result.stderr}
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"command": args, "code": None, "error": str(exc)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True, help="An already installed image reference")
    parser.add_argument("--python", default="/usr/bin/python3.12")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    image = run(["docker", "image", "inspect", args.image, "--format", "{{.Id}}"])
    evidence: dict[str, object] = {"image": image, "runtime": "runsc"}
    success = False
    if image["code"] == 0:
        # Use the resolved ID, prohibit implicit pulls, expose no host directory or socket.
        image_id = str(image["stdout"]).strip()
        capability = run([
            "docker", "run", "--rm", "--pull=never", "--runtime=runsc",
            "--network=none", "--read-only", "--cap-drop=ALL",
            "--security-opt=no-new-privileges", "--user=1000:1000",
            "--memory=1g", "--memory-swap=1g", "--cpus=1", "--pids-limit=128",
            "--tmpfs=/tmp:rw,nosuid,nodev,size=64m", image_id, args.python, "-c", CHECK,
        ])
        evidence["capability"] = capability
        if capability["code"] == 0:
            try:
                details = json.loads(str(capability["stdout"]))
                evidence["details"] = details
                success = (all(details["tools"].values())
                           and not details["docker_socket_visible"])
            except (ValueError, KeyError, TypeError):
                success = False
    evidence["toolchain_ready"] = success
    evidence["development_task_verified"] = False
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, ensure_ascii=False, indent=2))
    print(json.dumps(evidence, ensure_ascii=False, indent=2))
    return 0 if success else 2


if __name__ == "__main__":
    raise SystemExit(main())
