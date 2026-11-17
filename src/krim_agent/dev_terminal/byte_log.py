"""Binary output spool; cursors address the original byte stream.

New spools append without a capacity. Explicit finite capacity is supported only
for reading legacy ring records; overwritten legacy bytes remain unavailable.
"""

from __future__ import annotations

import codecs
import os
import stat
from pathlib import Path


class ByteLog:
    """Single-writer append spool, with an explicit legacy ring compatibility mode."""

    def __init__(
        self, path: Path, capacity: int | None = None, *, start: int = 0, end: int = 0,
        recover: bool = False,
    ) -> None:
        if (not 0 <= start <= end
                or capacity is not None and (capacity < 4 or end - start > capacity)):
            raise ValueError("invalid log bounds")
        self.path = path
        self.capacity = capacity
        self.start = start
        self.end = end
        self.fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(self.fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ValueError("invalid output spool")
            if capacity is None:
                if info.st_size < end:
                    raise OSError("incomplete output spool")
                # Append writes never overwrite acknowledged bytes. A worker
                # restart can retain bytes written after its last receipt.
                if recover:
                    self.end = info.st_size
                elif info.st_size != end:
                    raise ValueError("output spool requires recovery")
        except BaseException:
            os.close(self.fd)
            raise

    def append(self, data: bytes) -> None:
        if not data:
            return
        if self.capacity is None:
            self._write(data, self.end)
            self.end += len(data)
            return
        final_end = self.end + len(data)
        retained = data[-self.capacity:]
        logical_start = final_end - len(retained)
        offset = logical_start % self.capacity
        first = min(len(retained), self.capacity - offset)
        self._write(retained[:first], offset)
        if first < len(retained):
            self._write(retained[first:], 0)
        self.end = final_end
        self.start = max(self.start, self.end - self.capacity)

    def _write(self, data: bytes, offset: int) -> None:
        view = memoryview(data)
        while view:
            count = os.pwrite(self.fd, view, offset)
            if count <= 0:
                raise OSError("log write made no progress")
            offset += count
            view = view[count:]

    def read(self, cursor: int, limit: int, *, eof: bool) -> dict[str, object]:
        if cursor < 0 or cursor > self.end:
            raise ValueError("output cursor outside log")
        if not 4 <= limit <= 65536:
            raise ValueError("output page limit outside bounds")
        begin = max(cursor, self.start)
        count = min(limit, self.end - begin)
        if self.capacity is None:
            data = os.pread(self.fd, count, begin)
        else:
            offset = begin % self.capacity
            first = min(count, self.capacity - offset)
            data = os.pread(self.fd, first, offset)
            if count > first:
                data += os.pread(self.fd, count - first, 0)
        if len(data) != count:
            raise OSError("incomplete output spool")
        # Avoid splitting a valid UTF-8 sequence at a page boundary. Arbitrary
        # binary output (or a cursor supplied inside a code point) is replaced.
        if data and (begin + len(data) < self.end or not eof):
            decoder = codecs.getincrementaldecoder("utf-8")("replace")
            text = decoder.decode(data, final=False)
            pending, _ = decoder.getstate()
            consumed = len(data) - len(pending)
        else:
            text = data.decode("utf-8", errors="replace")
            consumed = len(data)
        return {
            "cursor": cursor,
            "next_cursor": begin + consumed,
            "text": text,
            "truncated": cursor < self.start,
            "eof": eof and begin + consumed == self.end,
        }

    def flush(self) -> None:
        os.fsync(self.fd)

    def close(self) -> None:
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1
