"""A1传输内核聚焦测试：分块上传暂存与有界归档流。"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import tarfile
import threading
import uuid
from collections.abc import Callable
from pathlib import Path

import pytest

from krim_agent.sandbox.archive_io import (
    ArchiveFile,
    ArchiveFormatError,
    ArchiveLimitExceeded,
    ArchiveLimits,
    ArchiveRejected,
    chunk_reader,
    export_archive,
    export_archive_bytes,
    extract_archive,
    read_manifest,
)
from krim_agent.sandbox.ports import ContentInfo, ContentSource
from krim_agent.sandbox.transfer_store import (
    Conflict,
    Expired,
    HashMismatch,
    InvalidRequest,
    NotFound,
    QuotaExceeded,
    TransferStore,
    UnsafePath,
)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class _Clock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _upload(
    store: TransferStore,
    principal: str,
    payload: bytes,
    request_key: str,
    chunk_size: int = 7,
) -> str:
    transfer = store.create(principal, len(payload), _sha(payload), request_key)
    transfer_id = str(transfer["transfer_id"])
    offset = 0
    while offset < len(payload):
        block = payload[offset : offset + chunk_size]
        store.append(principal, transfer_id, offset, block, _sha(block))
        offset += len(block)
    return transfer_id


@pytest.fixture
def staging_root(tmp_path: Path) -> Path:
    return tmp_path / "staging"


@pytest.fixture
def store(staging_root: Path) -> TransferStore:
    return TransferStore(staging_root)


@pytest.fixture
def principal() -> str:
    return "acct-6f1e2f3a-0000-0000-0000-000000000001"


# ---- TransferStore：创建与幂等 ----


def test_create_is_idempotent_on_same_request_key(store: TransferStore, principal: str) -> None:
    digest = _sha(b"abcdefghijkl")
    first = store.create(principal, 12, digest, "rk-fixed")
    second = store.create(principal, 12, digest, "rk-fixed")
    assert second["transfer_id"] == first["transfer_id"]
    assert second["next_offset"] == 0
    transfer_id = str(first["transfer_id"])
    store.append(principal, transfer_id, 0, b"abcdef", _sha(b"abcdef"))
    third = store.create(principal, 12, digest, "rk-fixed")
    assert third["next_offset"] == 6


def test_create_rejects_conflicting_request_key(store: TransferStore, principal: str) -> None:
    store.create(principal, 12, _sha(b"abcdefghijkl"), "rk-fixed")
    with pytest.raises(Conflict):
        store.create(principal, 13, _sha(b"x" * 13), "rk-fixed")


def test_create_validates_arguments(
    store: TransferStore, principal: str, tmp_path: Path
) -> None:
    good = _sha(b"data")
    with pytest.raises(InvalidRequest):
        store.create("", 1, good, "k")
    with pytest.raises(InvalidRequest):
        store.create("a\x00b", 1, good, "k")
    with pytest.raises(InvalidRequest):
        store.create(principal, 1, "nothex", "k")
    with pytest.raises(InvalidRequest):
        store.create(principal, -1, good, "k")
    with pytest.raises(InvalidRequest):
        store.create(principal, 1, good, "")
    tight = TransferStore(tmp_path / "tight", max_bytes=8)
    with pytest.raises(QuotaExceeded):
        tight.create(principal, 9, good, "k")


# ---- TransferStore：顺序追加与重叠核实 ----


def test_append_sequential_progress(store: TransferStore, principal: str) -> None:
    transfer = store.create(principal, 12, _sha(b"abcdefghijkl"), "seq-1")
    transfer_id = str(transfer["transfer_id"])
    state = store.append(principal, transfer_id, 0, b"abcdef", _sha(b"abcdef"))
    assert state["received_bytes"] == 6
    assert state["next_offset"] == 6
    assert state["duplicate"] is False
    state = store.append(principal, transfer_id, 6, b"ghijkl", _sha(b"ghijkl"))
    assert state["received_bytes"] == 12
    assert store.transfer(principal, transfer_id)["status"] == "open"
    with pytest.raises(NotFound):
        store.transfer(principal, uuid.uuid4().hex)


def test_append_duplicate_chunk_is_verified_not_readded(
    store: TransferStore, principal: str
) -> None:
    transfer = store.create(principal, 12, _sha(b"abcdefghijkl"), "dup-1")
    transfer_id = str(transfer["transfer_id"])
    store.append(principal, transfer_id, 0, b"abcdef", _sha(b"abcdef"))
    state = store.append(principal, transfer_id, 0, b"abcdef", _sha(b"abcdef"))
    assert state["duplicate"] is True
    assert state["received_bytes"] == 6
    with pytest.raises(HashMismatch):
        store.append(principal, transfer_id, 0, b"XXXXXX", _sha(b"XXXXXX"))
    with pytest.raises(HashMismatch):
        store.append(principal, transfer_id, 0, b"abcdef", _sha(b"zzzzzz"))
    store.append(principal, transfer_id, 6, b"ghijkl", _sha(b"ghijkl"))
    state = store.append(principal, transfer_id, 6, b"ghijkl", _sha(b"ghijkl"))
    assert state["duplicate"] is True
    assert state["received_bytes"] == 12


def test_append_rejects_gap_and_straddling_overlap(
    store: TransferStore, principal: str
) -> None:
    transfer = store.create(principal, 12, _sha(b"abcdefghijkl"), "gap-1")
    transfer_id = str(transfer["transfer_id"])
    store.append(principal, transfer_id, 0, b"abcdef", _sha(b"abcdef"))
    with pytest.raises(InvalidRequest):
        store.append(principal, transfer_id, 7, b"x", _sha(b"x"))
    with pytest.raises(InvalidRequest):
        store.append(principal, transfer_id, 3, b"abcdef", _sha(b"abcdef"))


def test_append_rejects_chunk_beyond_total(store: TransferStore, principal: str) -> None:
    transfer_id = _upload(store, principal, b"abcdefghijkl", "full-1")
    with pytest.raises(InvalidRequest):
        store.append(principal, transfer_id, 12, b"!", _sha(b"!"))


# ---- TransferStore：完成、取消与内容读取 ----


def test_finish_roundtrip_and_content_source_protocol(
    store: TransferStore, principal: str
) -> None:
    payload = bytes(range(256)) * 3
    transfer_id = _upload(store, principal, payload, "rt-1")
    state = store.transfer(principal, transfer_id)
    assert state["received_bytes"] == len(payload)
    content = store.finish(principal, transfer_id)
    assert isinstance(content, ContentInfo)
    assert content.size == len(payload)
    assert content.sha256 == _sha(payload)
    assert content.principal_id == principal
    assert store.info(principal, content.content_id) == content
    assert b"".join(store.chunks(principal, content.content_id)) == payload
    assert b"".join(store.chunks(principal, content.content_id, chunk_size=17)) == payload

    def consume(source: ContentSource, principal_id: str, content_id: str) -> bytes:
        return b"".join(source.chunks(principal_id, content_id))

    assert consume(store, principal, content.content_id) == payload


def test_finish_rejects_incomplete_then_resumes(store: TransferStore, principal: str) -> None:
    transfer = store.create(principal, 6, _sha(b"abcdef"), "inc-1")
    transfer_id = str(transfer["transfer_id"])
    store.append(principal, transfer_id, 0, b"abc", _sha(b"abc"))
    with pytest.raises(InvalidRequest):
        store.finish(principal, transfer_id)
    store.append(principal, transfer_id, 3, b"def", _sha(b"def"))
    content = store.finish(principal, transfer_id)
    assert content.size == 6


def test_finish_rejects_hash_mismatch_without_creating_content(
    store: TransferStore, principal: str, staging_root: Path
) -> None:
    wrong = _sha(b"not-the-content")
    transfer = store.create(principal, 5, wrong, "mm-1")
    transfer_id = str(transfer["transfer_id"])
    store.append(principal, transfer_id, 0, b"hello", _sha(b"hello"))
    with pytest.raises(HashMismatch):
        store.finish(principal, transfer_id)
    content_dirs = [p for p in staging_root.rglob("contents") if p.is_dir()]
    assert all(not any(child.iterdir()) for child in content_dirs)
    with pytest.raises(NotFound):
        store.info(principal, uuid.uuid4().hex)


def test_finished_upload_replays_to_finished_state(
    store: TransferStore, principal: str
) -> None:
    payload = b"already-done"
    transfer_id = _upload(store, principal, payload, "done-1")
    content = store.finish(principal, transfer_id)
    replay = store.create(principal, len(payload), _sha(payload), "done-1")
    assert replay["status"] == "finished"
    assert replay["content_id"] == content.content_id
    assert replay["next_offset"] == len(payload)


def test_cancel_releases_reservation_and_key(store: TransferStore, principal: str) -> None:
    transfer_id = _upload(store, principal, b"cancel-me", "can-1")
    result = store.cancel(principal, transfer_id)
    assert result["status"] == "cancelled"
    with pytest.raises(NotFound):
        store.append(principal, transfer_id, 0, b"c", _sha(b"c"))
    with pytest.raises(NotFound):
        store.cancel(principal, transfer_id)
    fresh = store.create(principal, 9, _sha(b"cancel-me"), "can-1")
    assert fresh["transfer_id"] != transfer_id


def test_principal_isolation(store: TransferStore, principal: str) -> None:
    transfer_id = _upload(store, principal, b"secret-bytes", "iso-1")
    content = store.finish(principal, transfer_id)
    other = "acct-other"
    with pytest.raises(NotFound):
        store.info(other, content.content_id)
    with pytest.raises(NotFound):
        store.chunks(other, content.content_id)


def test_empty_transfer_roundtrip(store: TransferStore, principal: str) -> None:
    transfer = store.create(principal, 0, _sha(b""), "empty-1")
    transfer_id = str(transfer["transfer_id"])
    content = store.finish(principal, transfer_id)
    assert content.size == 0
    assert b"".join(store.chunks(principal, content.content_id)) == b""


# ---- TransferStore：额度、对象数与TTL ----


def test_byte_budget_reserved_across_transfers(tmp_path: Path) -> None:
    store = TransferStore(tmp_path / "s", max_bytes=100)
    principal = "acct-a"
    first = store.create(principal, 60, _sha(b""), "k1")
    with pytest.raises(QuotaExceeded):
        store.create(principal, 41, _sha(b""), "k2")
    store.cancel(principal, str(first["transfer_id"]))
    third = store.create(principal, 41, _sha(b""), "k3")
    assert third["status"] == "open"


def test_object_count_limit(tmp_path: Path) -> None:
    store = TransferStore(tmp_path / "s", max_files=2)
    principal = "acct-b"
    store.create(principal, 1, _sha(b"a"), "k1")
    second = store.create(principal, 1, _sha(b"b"), "k2")
    with pytest.raises(QuotaExceeded):
        store.create(principal, 1, _sha(b"c"), "k3")
    store.cancel(principal, str(second["transfer_id"]))
    third = store.create(principal, 1, _sha(b"c"), "k3")
    assert third["status"] == "open"


def test_expiry_blocks_append_and_cleanup_sweeps(tmp_path: Path) -> None:
    clock = _Clock()
    store = TransferStore(tmp_path / "s", ttl_seconds=3600, clock=clock)
    principal = "acct-x"
    content_id = ""
    transfer_id = _upload(store, principal, b"persist-me", "exp-1")
    content = store.finish(principal, transfer_id)
    content_id = content.content_id
    second = store.create(principal, 10, _sha(b"0123456789"), "exp-2")
    second_id = str(second["transfer_id"])
    clock.advance(3601)
    with pytest.raises(Expired):
        store.append(principal, second_id, 0, b"0", _sha(b"0"))
    report = store.cleanup_expired()
    assert report["removed_transfers"] == 0
    assert report["removed_contents"] == 1
    assert report["removed_keys"] == 1
    with pytest.raises(NotFound):
        store.info(principal, content_id)
    fresh = store.create(principal, 10, _sha(b"0123456789"), "exp-2")
    assert fresh["status"] == "open"
    assert fresh["transfer_id"] != second_id


def test_cleanup_sweeps_crash_orphaned_key_tmp_files(tmp_path: Path) -> None:
    clock = _Clock()
    store = TransferStore(tmp_path / "s", ttl_seconds=3600, clock=clock)
    principal = "acct-tmp"
    transfer_id = _upload(store, principal, b"keep", "tmp-1")
    keys_dir = next((tmp_path / "s" / "principals").iterdir()) / "keys"
    stale_tmp = keys_dir / ".tmp-crashed"
    stale_tmp.write_bytes(b'{"kind": "request_key"}')
    fresh_tmp = keys_dir / ".tmp-inflight"
    fresh_tmp.write_bytes(b"{}")
    stale_epoch = clock.now - 2 * 3600
    os.utime(stale_tmp, (stale_epoch, stale_epoch))
    os.utime(fresh_tmp, (clock.now, clock.now))
    report = store.cleanup_expired()
    assert not stale_tmp.exists()
    assert fresh_tmp.exists()
    assert report["removed_keys"] == 1
    assert store.transfer(principal, transfer_id)["status"] == "open"
    fresh = store.create(principal, 4, _sha(b"more"), "tmp-2")
    assert fresh["status"] == "open"


@pytest.mark.skipif(os.name == "nt", reason="需要POSIX flock语义")
def test_cleanup_blocks_on_account_lock_and_reads_latest_meta(tmp_path: Path) -> None:
    import fcntl

    clock = _Clock()
    store = TransferStore(tmp_path / "s", ttl_seconds=3600, clock=clock)
    principal = "acct-race"
    transfer_id = str(store.create(principal, 10, _sha(b"0123456789"), "race-1")["transfer_id"])
    base = next((tmp_path / "s" / "principals").iterdir())
    meta_path = base / "transfers" / transfer_id / "meta.json"
    # 让cleanup在取锁前存在的唯一快照显示“已过期”，等价于竞态窗口中的旧值。
    stale = json.loads(meta_path.read_text())
    stale["updated_at"] = clock.now - 7200
    meta_path.write_text(json.dumps(stale))

    lock_fd = os.open(base / ".lock", os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX)
    done = threading.Event()
    report_box: dict[str, object] = {}

    def sweep() -> None:
        report_box["report"] = store.cleanup_expired()
        done.set()

    sweeper = threading.Thread(target=sweep)
    sweeper.start()
    # 持锁期间cleanup必须阻塞在账号flock上，不可能完成扫描删除。
    assert not done.wait(0.3)
    # 写者在锁内刷新进度（等价并发append已落盘的最新meta），随后释放锁。
    fresh = dict(stale)
    fresh["updated_at"] = clock.now
    meta_path.write_text(json.dumps(fresh))
    fcntl.flock(lock_fd, fcntl.LOCK_UN)
    os.close(lock_fd)
    sweeper.join(timeout=5)
    assert not sweeper.is_alive()
    report = report_box["report"]
    assert report["removed_transfers"] == 0
    assert store.transfer(principal, transfer_id)["status"] == "open"


# ---- TransferStore：受管路径硬化（POSIX语义） ----


@pytest.mark.skipif(os.name == "nt", reason="需要POSIX权限语义")
def test_posix_permissions(store: TransferStore, principal: str, staging_root: Path) -> None:
    transfer_id = _upload(store, principal, b"abc", "perm-1")
    store.finish(principal, transfer_id)
    for candidate in staging_root.rglob("*"):
        if candidate.is_dir():
            assert candidate.stat().st_mode & 0o777 == 0o700
    data_files = [p for p in staging_root.rglob("data") if p.is_file()]
    assert len(data_files) == 1
    assert data_files[0].stat().st_mode & 0o777 == 0o600


@pytest.mark.skipif(os.name == "nt", reason="需要POSIX symlink语义")
def test_posix_rejects_symlink_replacement(
    store: TransferStore, principal: str, staging_root: Path
) -> None:
    transfer_id = _upload(store, principal, b"hello world", "sym-1")
    data_files = [p for p in staging_root.rglob("data") if p.is_file()]
    assert len(data_files) == 1
    data_files[0].unlink()
    outside = staging_root.parent / "outside.txt"
    outside.write_bytes(b"secret")
    os.symlink(outside, data_files[0])
    with pytest.raises(UnsafePath):
        store.append(principal, transfer_id, 0, b"hello", _sha(b"hello"))
    store.cancel(principal, transfer_id)

    retry = _upload(store, principal, b"hello world", "sym-2")
    content = store.finish(principal, retry)
    content_data = [p for p in staging_root.rglob("data") if p.is_file()]
    assert len(content_data) == 1
    content_data[0].unlink()
    os.symlink(outside, content_data[0])
    with pytest.raises(UnsafePath):
        store.chunks(principal, content.content_id)


# ---- archive_io：导出 ----


def _files() -> list[ArchiveFile]:
    return [
        ArchiveFile("src/app.py", b"print('hello')\n"),
        ArchiveFile("docs/readme.md", "中文说明内容".encode()),
        ArchiveFile("data.bin", bytes(range(256)) * 4),
    ]


def test_export_roundtrip_manifest_and_extract() -> None:
    files = _files()
    out = io.BytesIO()
    result = export_archive(files, out.write)
    archive = out.getvalue()
    assert result["entry_count"] == 3
    assert result["excluded"] == []
    assert result["total_bytes"] == sum(len(item.data) for item in files)
    assert result["archive_bytes"] == len(archive)
    assert result["sha256"] == _sha(archive)
    manifest = read_manifest(chunk_reader(iter([archive])))
    assert manifest["format"] == "tar"
    entries = list(manifest["entries"])
    assert [entry.name for entry in entries] == sorted(item.name for item in files)
    by_name = {item.name: item for item in files}
    for entry in entries:
        assert entry.sha256 == _sha(by_name[entry.name].data)
        assert entry.size == len(by_name[entry.name].data)
        assert entry.is_dir is False
    collected: dict[str, bytes] = {}
    summary = extract_archive(
        chunk_reader(iter([archive])),
        lambda entry, chunks: collected.update({entry.name: b"".join(chunks)}),
    )
    assert collected == {item.name: item.data for item in files}
    assert {entry.name: entry.sha256 for entry in summary["entries"]} == {
        item.name: _sha(item.data) for item in files
    }


def test_export_gzip_import() -> None:
    packed = gzip.compress(export_archive_bytes(_files()))
    chunks = [packed[index : index + 997] for index in range(0, len(packed), 997)]
    manifest = read_manifest(chunk_reader(iter(chunks)))
    assert manifest["format"] == "gzip"
    assert len(manifest["entries"]) == 3


def test_export_default_exclusions_listed() -> None:
    files = [
        ArchiveFile("main.py", b"x"),
        ArchiveFile(".env", b"X=1"),
        ArchiveFile("certs/server.pem", b"-"),
        ArchiveFile(".git/config", b"[core]"),
        ArchiveFile(".gitignore", b"*.log"),
        ArchiveFile("deploy/.ssh/id_rsa", b"k"),
        ArchiveFile("id_ed25519", b"k"),
        ArchiveFile("notes/.env.local", b"X=2"),
    ]
    sink: Callable[[bytes], object] = io.BytesIO().write
    result = export_archive(files, sink)
    assert set(result["excluded"]) == {
        ".env",
        "certs/server.pem",
        ".git/config",
        "deploy/.ssh/id_rsa",
        "id_ed25519",
        "notes/.env.local",
    }
    assert result["entry_count"] == 2
    keep_secrets = export_archive(files, sink, exclude_secrets=False)
    assert keep_secrets["excluded"] == [".git/config"]
    assert keep_secrets["entry_count"] == 7
    keep_all = export_archive(files, sink, exclude_git=False, exclude_secrets=False)
    assert keep_all["excluded"] == []
    assert keep_all["entry_count"] == 8


@pytest.mark.parametrize(
    "name",
    [
        "../evil",
        "/etc/passwd",
        "a/../../b",
        "a/./b",
        "back\\slash",
        "C:\\temp\\x",
        "colon:name",
        "",
    ],
)
def test_export_rejects_unsafe_names(name: str) -> None:
    with pytest.raises(ArchiveRejected):
        export_archive([ArchiveFile(name, b"x")], io.BytesIO().write)


def test_export_rejects_depth_and_limits() -> None:
    deep = "/".join(f"d{index}" for index in range(17)) + "/f.txt"
    with pytest.raises(ArchiveLimitExceeded):
        export_archive([ArchiveFile(deep, b"x")], io.BytesIO().write)
    small = ArchiveLimits(max_total_bytes=10, max_file_bytes=4, max_entries=2, max_depth=16)
    with pytest.raises(ArchiveLimitExceeded):
        export_archive([ArchiveFile("big.txt", b"x" * 5)], io.BytesIO().write, small)
    with pytest.raises(ArchiveLimitExceeded):
        export_archive(
            [ArchiveFile("a", b"123456"), ArchiveFile("b", b"123456")], io.BytesIO().write, small
        )
    with pytest.raises(ArchiveLimitExceeded):
        export_archive(
            [ArchiveFile("a", b"1"), ArchiveFile("b", b"2"), ArchiveFile("c", b"3")],
            io.BytesIO().write,
            small,
        )
    with pytest.raises(ArchiveRejected):
        export_archive(
            [ArchiveFile("a", b"1"), ArchiveFile("a", b"1")], io.BytesIO().write, small
        )


def test_export_is_deterministic() -> None:
    files = _files()
    first = export_archive_bytes(list(reversed(files)))
    second = export_archive_bytes(files)
    assert first == second
    assert first == export_archive_bytes(files)


# ---- archive_io：导入 ----


def _tar_with(members: list[tuple[tarfile.TarInfo, bytes | None]]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for info, data in members:
            archive.addfile(info, io.BytesIO(data) if data is not None else None)
    return buffer.getvalue()


def _member(
    name: str, mtype: bytes = tarfile.REGTYPE, size: int = 0, linkname: str = ""
) -> tarfile.TarInfo:
    info = tarfile.TarInfo(name)
    info.type = mtype
    info.size = size
    if linkname:
        info.linkname = linkname
    return info


@pytest.mark.parametrize(
    "build",
    [
        lambda: [(_member("../evil", size=3), b"abc")],
        lambda: [(_member("/abs", size=1), b"x")],
        lambda: [(_member("a/../../up", size=1), b"x")],
        lambda: [(_member("link", mtype=tarfile.SYMTYPE, linkname="/etc/passwd"), None)],
        lambda: [(_member("hard", mtype=tarfile.LNKTYPE, linkname="target"), None)],
        lambda: [(_member("pipe", mtype=tarfile.FIFOTYPE), None)],
        lambda: [(_member("dev", mtype=tarfile.CHRTYPE), None)],
    ],
)
def test_import_rejects_dangerous_members(build: Callable[[], list]) -> None:
    seen: list[str] = []

    def sink(name: object, chunks: object) -> None:
        seen.append(str(name))

    with pytest.raises(ArchiveRejected):
        extract_archive(chunk_reader(iter([_tar_with(build())])), sink)
    assert seen == []


def test_import_limit_enforcement() -> None:
    budget = ArchiveLimits(max_total_bytes=10, max_file_bytes=8, max_entries=2, max_depth=16)
    deep = "/".join(f"d{index}" for index in range(17)) + "/f"
    with pytest.raises(ArchiveLimitExceeded):
        read_manifest(chunk_reader(iter([_tar_with([(_member(deep, size=1), b"x")])])), budget)
    with pytest.raises(ArchiveLimitExceeded):
        read_manifest(
            chunk_reader(iter([_tar_with([(_member("big", size=9), b"123456789")])])), budget
        )
    two = _tar_with([(_member("a", size=8), b"12345678"), (_member("b", size=8), b"12345678")])
    with pytest.raises(ArchiveLimitExceeded):
        read_manifest(chunk_reader(iter([two])), budget)
    many = _tar_with([(_member(f"m{index}", size=1), b"x") for index in range(3)])
    with pytest.raises(ArchiveLimitExceeded):
        read_manifest(chunk_reader(iter([many])), budget)

    bomb_budget = ArchiveLimits(
        max_total_bytes=1024 * 1024, max_file_bytes=1024 * 1024, max_entries=100, max_depth=16
    )
    payload = b"\x00" * (12 * 1024 * 1024)
    bomb = gzip.compress(_tar_with([(_member(".env", size=len(payload)), payload)]))
    with pytest.raises(ArchiveLimitExceeded):
        read_manifest(chunk_reader(iter([bomb])), bomb_budget)
    declared_budget = ArchiveLimits(
        max_total_bytes=1024 * 1024, max_file_bytes=4 * 1024 * 1024, max_entries=100, max_depth=16
    )
    declared = _tar_with([(_member("zeros", size=2 * 1024 * 1024), None)])
    with pytest.raises(ArchiveLimitExceeded):
        read_manifest(chunk_reader(iter([declared])), declared_budget)


def test_import_rejects_truncated_archive() -> None:
    raw = export_archive_bytes(_files())
    # 缺失tar结尾结束块可以容忍：已枚举成员的size与摘要仍逐一校验通过。
    tolerant = read_manifest(chunk_reader(iter([raw[:-64]])))
    assert len(tolerant["entries"]) == 3
    # 成员数据被截断必须拒绝。
    with pytest.raises(ArchiveFormatError):
        read_manifest(chunk_reader(iter([raw[: len(raw) // 8]])))


# ---- 组合：导出 → 暂存 → 导入 ----


def test_end_to_end_staged_archive_roundtrip(
    store: TransferStore, principal: str, staging_root: Path
) -> None:
    files = [
        ArchiveFile("app/main.py", b"print('v1')\n"),
        ArchiveFile("app/lib/util.py", b"def f():\n    return 1\n"),
        ArchiveFile("README.md", b"# demo\n"),
    ]
    archive = export_archive_bytes(files)
    transfer_id = _upload(store, principal, archive, "stage-export", chunk_size=1024)
    content = store.finish(principal, transfer_id)
    assert content.sha256 == _sha(archive)
    manifest = read_manifest(chunk_reader(store.chunks(principal, content.content_id)))
    assert manifest["format"] == "tar"
    assert len(manifest["entries"]) == 3
    collected: dict[str, bytes] = {}
    extract_archive(
        chunk_reader(store.chunks(principal, content.content_id)),
        lambda entry, chunks: collected.update({entry.name: b"".join(chunks)}),
    )
    assert collected == {item.name: item.data for item in files}
    remaining = [p for p in staging_root.rglob("data") if p.is_file()]
    assert len(remaining) == 1
    assert remaining[0].parent.parent.name == "contents"
