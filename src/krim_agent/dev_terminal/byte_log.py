"""Bounded binary output spool; cursors always address the original byte stream."""

from __future__ import annotations

import os
from pathlib import Path


class ByteLog:
    """Single-writer spool with at most capacity bytes on disk.

    The writer serializes append/read/checkpoint on its event loop. Metadata is
    persisted by the worker together with its process receipt. A missing live
    writer never makes stale metadata authoritative for further execution.
    """

    def __init__(
        self, path: Path, capacity: int, *, start: int = 0, end: int = 0,
    ) -> None:
        if capacity < 4 or not 0 <= start <= end or end - start > capacity:
            raise ValueError("invalid log bounds")
        self.path = path
        self.capacity = capacity
        self.start = start
        self.end = end
        flags = os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW
        self.fd = os.open(path, flags, 0o600)

    def append(self, data: bytes) -> None:
        if not data:
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
            import codecs

            decoder = codecs.getincrementaldecoder("utf-8")("replace")
            text = decoder.decode(data, final=False)
            pending, _ = decoder.getstate()
            consumed = len(data) - len(pending)
        else:
            text = data.decode("utf-8", errors="replace")
            consumed = len(data)
        next_cursor = begin + consumed
        return {
            "cursor": cursor,
            "next_cursor": next_cursor,
            "text": text,
            "truncated": cursor < self.start,
            "eof": eof and next_cursor == self.end,
        }

    def flush(self) -> None:
        os.fsync(self.fd)

    def close(self) -> None:
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1
