"""Start the compatibility probe's web service through the preview runner."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binding", type=Path, required=True)
    args = parser.parse_args()
    binding = json.loads(args.binding.read_text())
    name = binding["container"]
    if not isinstance(name, str) or not re.fullmatch(
        r"krim-dev-probe-[0-9a-f]{12}-(a|restored)", name
    ):
        raise ValueError("not a compatibility probe container")
    details = json.loads(subprocess.check_output(["docker", "inspect", name], text=True))[0]
    if details["Image"] != binding["image"]:
        raise ValueError("probe image binding changed")
    mounts = [item for item in details["Mounts"] if item["Destination"] == "/workspace"]
    if len(mounts) != 1 or mounts[0]["Source"] != binding["project"]:
        raise ValueError("probe project binding changed")
    if details["Config"]["Cmd"] != ["sleep", "infinity"]:
        raise ValueError("not an idle probe environment")
    if not details["State"]["Running"]:
        # Recover the explicitly selected environment after host shutdown. No
        # prior install/build operation is replayed; only this preview starts.
        subprocess.run(["docker", "start", name], check=True)
    os.execvp("docker", ["docker", "exec", name, "node", "/workspace/server.cjs"])


if __name__ == "__main__":
    main()
