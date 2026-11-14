from pathlib import Path

import pytest

from krim_agent.dev_terminal.byte_log import ByteLog


def test_byte_cursor_utf8_boundaries(tmp_path: Path) -> None:
    log = ByteLog(tmp_path / "log", 100)
    try:
        content = "ab你好🙂z"
        log.append(content.encode())
        cursor = 0
        pages = []
        while cursor < log.end:
            page = log.read(cursor, 4, eof=True)
            assert page["next_cursor"] > cursor
            cursor = page["next_cursor"]
            pages.append(page["text"])
        assert "".join(pages) == content
        assert cursor == len(content.encode())
        assert log.read(cursor, 4, eof=True)["eof"]
    finally:
        log.close()


def test_ring_wrap_and_truncation(tmp_path: Path) -> None:
    log = ByteLog(tmp_path / "log", 8)
    try:
        log.append(b"abcdef")
        log.append(b"ghijkl")
        page = log.read(0, 8, eof=False)
        assert page == {"cursor": 0, "next_cursor": 12, "text": "efghijkl",
                        "truncated": True, "eof": False}
        assert (tmp_path / "log").stat().st_size <= 8
        log.append(b"0123456789abc")
        page = log.read(12, 8, eof=True)
        assert page["text"] == "56789abc"
        assert page["next_cursor"] == 25
        assert page["truncated"] and page["eof"]
    finally:
        log.close()


def test_partial_utf8_waits_for_complete_character(tmp_path: Path) -> None:
    log = ByteLog(tmp_path / "log", 100)
    try:
        data = "你".encode()
        log.append(data[:2])
        assert log.read(0, 4, eof=False)["next_cursor"] == 0
        log.append(data[2:])
        assert log.read(0, 4, eof=False)["text"] == "你"
        log.append(b"\xff\xe4")
        assert log.read(3, 4, eof=True)["text"] == "��"
        with pytest.raises(ValueError):
            log.read(log.end + 1, 4, eof=True)
    finally:
        log.close()


def test_reopen_spool_preserves_byte_cursor(tmp_path: Path) -> None:
    log = ByteLog(tmp_path / "log", 8)
    log.append(b"abcdefghi")
    log.flush()
    start, end = log.start, log.end
    log.close()
    restored = ByteLog(tmp_path / "log", 8, start=start, end=end)
    try:
        assert restored.read(0, 8, eof=True)["text"] == "bcdefghi"
        assert restored.read(0, 8, eof=True)["truncated"]
    finally:
        restored.close()
