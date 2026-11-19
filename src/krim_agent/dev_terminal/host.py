"""Host-only connection to a durable, private terminal supervisor.

I starts this service in its trusted host lifecycle; connecting a new API process
never starts an old command or recreates a lost worker. State directories must
not be mounted in any user execution container.
"""

from __future__ import annotations

import asyncio
import json
import os
import stat
from pathlib import Path
from typing import Any

from .wire import MAX_MESSAGE


class HostWorkerConnection:
    def __init__(self, state_directory: Path) -> None:
        info = state_directory.lstat()
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
                or info.st_mode & 0o077):
            raise PermissionError("terminal state directory is not private")
        self.state_directory = state_directory

    async def __call__(self, payload: dict[str, Any]) -> dict[str, Any]:
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode() + b"\n"
        if len(data) > MAX_MESSAGE:
            raise ValueError("worker message exceeds limit")
        async with asyncio.timeout(35):
            reader, writer = await asyncio.open_unix_connection(
                str(self.state_directory / "control.sock"), limit=MAX_MESSAGE + 1,
            )
            try:
                writer.write(data)
                await writer.drain()
                line = await reader.readline()
                if not line.endswith(b"\n") or len(line) > MAX_MESSAGE:
                    raise ValueError("incomplete worker reply")
                result = json.loads(line)
                if not isinstance(result, dict):
                    raise ValueError("invalid worker reply")
                return result
            finally:
                writer.close()
                await writer.wait_closed()
