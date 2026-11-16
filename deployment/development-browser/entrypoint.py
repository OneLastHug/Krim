"""Create only the worker's transient NSS trust store, then exec its stdio loop."""

from __future__ import annotations

import os
import runpy
import signal
import subprocess
from pathlib import Path

from worker import verify_ca


def install_watchdog(seconds: float = 130) -> None:
    # Process-wide backstop covers driver teardown and asyncio.run shutdown,
    # independently of cancelled coroutine cleanup. Only this container exits.
    def expire(signum, frame):
        os._exit(124)

    signal.signal(signal.SIGALRM, expire)
    signal.setitimer(signal.ITIMER_REAL, seconds)


def main() -> None:
    install_watchdog()
    if os.getuid() != 1000 or os.getgid() != 1000:
        raise SystemExit(78)
    ca = verify_ca()
    if os.environ.get("NODE_EXTRA_CA_CERTS") != ca:
        raise SystemExit(78)
    nss = Path("/tmp/krim-home/.pki/nssdb")
    nss.mkdir(parents=True, mode=0o700, exist_ok=False)
    for command in (
        ["/usr/bin/certutil", "-N", "--empty-" + "pass" + "word", "-d", "sql:" + str(nss)],
        [
            "/usr/bin/certutil",
            "-A",
            "-d",
            "sql:" + str(nss),
            "-n",
            "krim-preview-ca",
            "-t",
            "C,,",
            "-i",
            ca,
        ],
    ):
        subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=3,
            check=True,
        )
    runpy.run_path(str(Path(__file__).with_name("worker.py")), run_name="__main__")


if __name__ == "__main__":
    main()
