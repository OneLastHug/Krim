"""源码指纹在装配边界更新，流期间不重复访问文件。"""

import hashlib
from pathlib import Path

import pytest

from krim_agent.source_fingerprints import reset_source_fingerprints, source_digest


def test_unchanged_source_is_read_once(tmp_path, monkeypatch):
    source = tmp_path / "module.py"
    source.write_text("VALUE = 1\n")
    original = Path.read_bytes
    reads = []

    def read(path):
        reads.append(str(path))
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", read)
    expected = hashlib.sha256(b"VALUE = 1\n").hexdigest()
    assert source_digest(source) == expected
    assert source_digest(source) == expected
    assert reads == [str(source)]


def test_source_changes_are_detected_at_next_assembly(tmp_path):
    source = tmp_path / "module.py"
    source.write_text("VALUE = 1\n")
    first = source_digest(source)
    source.write_text("VALUE = 2\n")
    assert source_digest(source) == first
    reset_source_fingerprints()
    assert source_digest(source) != first
    assert source_digest(source) == hashlib.sha256(b"VALUE = 2\n").hexdigest()


def test_distinct_sources_and_missing_files(tmp_path):
    first, second = tmp_path / "first.py", tmp_path / "second.py"
    first.write_text("first")
    second.write_text("second")
    assert source_digest(first) != source_digest(second)
    first.unlink()
    reset_source_fingerprints()
    with pytest.raises(FileNotFoundError):
        source_digest(first)
