"""Inspect/run only the existing approved image; no network or installation."""

import json
import subprocess

DOCKER = ["/usr/bin/docker", "--host=unix:///var/run/docker.sock"]
IMAGE = "sha256:82098beca2bd1c8887f82deb8d2fba13f87da04938463881a5e95b3d346834dc"
LABEL = "d2-git-image-probe-20261115"


def run(*args):
    return subprocess.run(
        [*DOCKER, *args],
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
        env={"PATH": "/usr/bin:/bin", "HOME": "/nonexistent"},
    ).stdout


def main():
    identifier = run(
        "create",
        "--runtime=runsc",
        "--network=none",
        "--cap-drop=ALL",
        "--security-opt=no-new-privileges:true",
        "--read-only",
        "--user=65532:65532",
        "--memory=268435456",
        "--cpus=0.5",
        "--pids-limit=64",
        "--label",
        f"io.krim.git-probe={LABEL}",
        "--entrypoint=/bin/sh",
        IMAGE,
        "-c",
        "command -v git; command -v python3; git --version; python3 --version",
    ).strip()
    assert len(identifier) == 64 and all(c in "0123456789abcdef" for c in identifier)
    try:
        item = json.loads(run("inspect", identifier))[0]
        assert item["Id"] == identifier and item["Image"] == IMAGE
        assert item["Config"]["Labels"]["io.krim.git-probe"] == LABEL
        assert item["Config"]["User"] == "65532:65532"
        host = item["HostConfig"]
        assert host["Runtime"] == "runsc" and host["NetworkMode"] == "none"
        assert host["CapDrop"] == ["ALL"] and not host["Privileged"]
        assert host["ReadonlyRootfs"] and "no-new-privileges:true" in host["SecurityOpt"]
        assert item["Mounts"] == []
        print(
            json.dumps(
                {
                    "container_id": identifier,
                    "image": IMAGE,
                    "output": run("start", "-a", identifier),
                }
            )
        )
    finally:
        item = json.loads(run("inspect", identifier))[0]
        assert item["Id"] == identifier and item["Image"] == IMAGE
        assert item["Config"]["Labels"]["io.krim.git-probe"] == LABEL
        run("rm", "-f", identifier)
        print(json.dumps({"removed": identifier}))


if __name__ == "__main__":
    main()
