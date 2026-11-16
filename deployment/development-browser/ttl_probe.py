"""Exercise installed IPC with stdin held open; no browser or preview is used."""

import asyncio
import json
import sys
import time

sys.path.insert(0, "/opt/krim-browser/app")
from stdio_channel import FDChannel
from worker import limited_drive


class ProbeSession:
    closed = False

    async def close(self):
        self.closed = True


async def main():
    channel, session = FDChannel(0, 1), ProbeSession()
    started = time.monotonic()
    try:
        # Actual default session TTL, not a shorter fixture-only timeout.
        await limited_drive(channel, session)
        raise RuntimeError("unexpected_input_eof")
    except TimeoutError:
        elapsed = time.monotonic() - started
        assert session.closed and 119 <= elapsed < 125
        assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        print(
            json.dumps(
                {
                    "ok": True,
                    "elapsed_seconds": round(elapsed, 3),
                    "session_closed": True,
                    "pending_tasks": 0,
                }
            ),
            flush=True,
        )
    finally:
        channel.close()


asyncio.run(main())
