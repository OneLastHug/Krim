"""A包无配额文件链路：工具参数越过旧上限、暂存分块上传→流式导入→分页读回。"""

from __future__ import annotations

import base64
import hashlib
import uuid

import pytest

from krim_agent.sandbox.file_models import (
    FileFind,
    FileList,
    FileRead,
    FileReplacePreview,
    FileSearch,
    FileWrite,
)
from krim_agent.sandbox.filesystem import ProjectFiles
from krim_agent.sandbox.transfer_store import TransferStore


def test_tool_models_accept_payloads_beyond_old_caps():
    """写入16KiB、读取offset 4MiB、列表offset 4096等旧应用配额不再约束工具参数。"""
    big = "字" * 20000
    write = FileWrite(path="big.txt", expected_generation=0, expected_sha256=None, content=big)
    assert write.content == big
    read = FileRead(path="big.txt", offset=4 * 1024**2 + 7)
    assert read.offset == 4 * 1024**2 + 7
    listing = FileList(path=".", offset=5000, limit=100)
    assert listing.offset == 5000


def test_page_pagination_params_remain_but_offsets_are_unbounded():
    """合理单页limit保留；offset不再有4096项可访问总量上限。"""
    with pytest.raises(ValueError):
        FileRead(path="big.txt", limit=16385)
    with pytest.raises(ValueError):
        FileList(path=".", limit=101)
    with pytest.raises(ValueError):
        FileSearch(query="needle", limit=101)
    assert FileFind(include_glob="*.py", offset=4096, limit=100).offset == 4096
    assert FileSearch(query="needle", offset=4096, limit=100).offset == 4096
    assert FileFind(include_glob="*.py", offset=1_000_000, limit=1).offset == 1_000_000
    assert FileSearch(query="needle", offset=1_000_000, limit=1).offset == 1_000_000


def test_replace_preview_accepts_old_content_and_occurrence_overlimits():
    """移除find/replacement字节与occurrence总量上限，保留非空find和正occurrence。"""
    preview = FileReplacePreview(
        path="large.txt",
        expected_generation=0,
        expected_sha256="0" * 64,
        find="f" * 5000,
        replacement="r" * 20000,
        occurrence=5000,
    )
    assert len(preview.find) == 5000
    assert len(preview.replacement) == 20000
    assert preview.occurrence == 5000
    with pytest.raises(ValueError):
        FileReplacePreview(
            path="large.txt", expected_generation=0, expected_sha256="0" * 64,
            find="", replacement="x", occurrence=1,
        )
    with pytest.raises(ValueError):
        FileReplacePreview(
            path="large.txt", expected_generation=0, expected_sha256="0" * 64,
            find="x", replacement="x", occurrence=0,
        )


def test_staged_upload_imports_into_project_via_stream_write(tmp_path):
    """暂存库分块上传完成后经chunks()流式导入项目文件；读取无总量上限。"""
    payload = bytes(range(256)) * 4096  # 1MiB
    store = TransferStore(tmp_path / "staging")
    principal = "acct-large"
    chunk_size = 65536
    transfer = store.create(
        principal, len(payload), hashlib.sha256(payload).hexdigest(), "import-1"
    )
    transfer_id = str(transfer["transfer_id"])
    offset = 0
    while offset < len(payload):
        block = payload[offset : offset + chunk_size]
        store.append(principal, transfer_id, offset, block, hashlib.sha256(block).hexdigest())
        offset += len(block)
    content = store.finish(principal, transfer_id)
    assert content.size == len(payload)

    root = tmp_path / "accounts"
    root.mkdir(mode=0o700)
    files = ProjectFiles(root)
    env, project = str(uuid.uuid4()), str(uuid.uuid4())
    files.create(env, project)
    try:
        result = files.stream_write(
            env, project, "imported.bin", store.chunks(principal, content.content_id),
            None, content.sha256,
        )
        assert result == {
            "path": "imported.bin",
            "size": len(payload),
            "sha256": content.sha256,
        }
        gathered = bytearray()
        page_offset = 0
        while True:
            page = files.read(env, project, "imported.bin", page_offset, 16384)
            gathered.extend(base64.b64decode(page["data"]))
            if page["eof"]:
                break
            page_offset += len(base64.b64decode(page["data"]))
        assert bytes(gathered) == payload
        assert b"".join(store.chunks(principal, content.content_id, chunk_size=1 << 20)) == payload
    finally:
        files.close()
