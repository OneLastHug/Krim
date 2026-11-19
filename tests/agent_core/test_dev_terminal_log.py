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


def test_default_log_preserves_all_pages_beyond_old_capacity(tmp_path: Path) -> None:
    path = tmp_path / "log"
    payload = ("中🙂 complete log\n" * 150000).encode()
    log = ByteLog(path)
    try:
        for offset in range(0, len(payload), 32768):
            log.append(payload[offset:offset + 32768])
        assert log.start == 0 and log.end == len(payload)
        assert path.stat().st_size == len(payload)
        cursor = 0
        pages = []
        while cursor < log.end:
            page = log.read(cursor, 65536, eof=True)
            assert not page["truncated"] and page["next_cursor"] > cursor
            cursor = page["next_cursor"]
            pages.append(page["text"])
        assert "".join(pages).encode() == payload
    finally:
        log.close()


def test_append_recovery_keeps_uncheckpointed_tail(tmp_path: Path) -> None:
    path = tmp_path / "log"
    log = ByteLog(path)
    log.append(b"acknowledged")
    checkpoint = log.end
    log.append(b" plus tail")
    log.close()
    restored = ByteLog(path, end=checkpoint, recover=True)
    try:
        assert restored.read(0, 65536, eof=True)["text"] == "acknowledged plus tail"
        assert restored.end == path.stat().st_size
    finally:
        restored.close()


@pytest.mark.parametrize("kind", ["symlink", "hardlink", "fifo"])
def test_spool_rejects_links_and_special_files(tmp_path: Path, kind: str) -> None:
    import os

    target = tmp_path / "target"
    target.write_bytes(b"private")
    path = tmp_path / "log"
    if kind == "symlink":
        path.symlink_to(target)
    elif kind == "hardlink":
        os.link(target, path)
    else:
        os.mkfifo(path)
    with pytest.raises((OSError, ValueError)):
        ByteLog(path)
    assert target.read_bytes() == b"private"


def test_recovery_rejects_missing_acknowledged_bytes(tmp_path: Path) -> None:
    path = tmp_path / "log"
    path.write_bytes(b"short")
    with pytest.raises(OSError, match="incomplete"):
        ByteLog(path, end=10, recover=True)


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
