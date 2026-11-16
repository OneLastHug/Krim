"""One-shot stdin-to-private-socket provider handoff; never prints credentials."""

from __future__ import annotations

import argparse
import json
import os
import socket
import stat
import struct
import sys
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", type=Path, required=True)
    args = parser.parse_args()
    # Read first: credentials stay in memory, never in argv or a temporary file.
    raw = sys.stdin.buffer.readline(8193)
    if not raw.endswith(b"\n") or len(raw) > 8192:
        raise SystemExit("invalid credential input")
    value = json.loads(raw)
    if not isinstance(value, dict) or not value.get("api_key"):
        raise SystemExit("missing credential")
    deadline = time.monotonic() + 90
    while True:
        try:
            info = args.socket.lstat()
            if (
                not stat.S_ISSOCK(info.st_mode)
                or info.st_uid != os.geteuid()
                or info.st_mode & 0o077
            ):
                raise SystemExit("untrusted credential socket")
            with socket.socket(socket.AF_UNIX) as client:
                client.settimeout(10)
                client.connect(str(args.socket))
                peer = client.getsockopt(
                    socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")
                )
                if struct.unpack("3i", peer)[1] != os.geteuid():
                    raise SystemExit("untrusted credential receiver")
                client.sendall(raw)
                if client.recv(32) != b"accepted\n":
                    raise SystemExit("credential handoff rejected")
            print("Provider credential delivered in memory; no request sent by injector.")
            return
        except FileNotFoundError:
            if time.monotonic() >= deadline:
                raise SystemExit("credential receiver did not start") from None
            time.sleep(0.1)


if __name__ == "__main__":
    main()
