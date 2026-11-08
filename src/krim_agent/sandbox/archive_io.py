"""基于tar的有界流式归档导出/导入。

导出产出未压缩tar流（成员按名称排序、mtime=0，输出确定性）；导入按magic
识别gzip/bzip2/xz/纯tar并流式解析。全部安全边界：

- 成员名拒绝绝对路径、`.`/`..`段、反斜杠、冒号与NUL；深度、条目数、单文件
  与解压总字节均受限，超限在读取数据前即中止，压缩炸弹无法拖垮宿主。
- 只接受普通文件与目录成员；symlink、hardlink、fifo、设备一律拒绝。
- 解压侧按字节预算硬顶（总预算+头部余量），绝不无界extractall落盘，
  也不执行包内内容；数据去向完全由调用方提供的sink决定。
- 成员数据截断一律拒绝；tar结尾结束块缺失可容忍，已枚举成员的size与
  摘要仍逐一校验。
- `.git`与常见秘密文件默认排除，并在结果清单的excluded中逐项列出。

异常约定：本模块抛ArchiveError子类；调用方sink抛出的异常原样传播
（若sink抛OSError会被流读取的统一映射归为ArchiveFormatError，建议sink
使用领域异常）。
"""

from __future__ import annotations

import bz2
import contextlib
import fnmatch
import gzip
import hashlib
import io
import lzma
import tarfile
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from typing import IO, cast

READ_CHUNK = 64 * 1024
DEFAULT_MAX_TOTAL_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_FILE_BYTES = 4 * 1024 * 1024
DEFAULT_MAX_ENTRIES = 4096
DEFAULT_MAX_DEPTH = 16

_MAGIC_SIZE = 6
_HEADER_SLACK_BYTES = 8 * 1024 * 1024
_GIT_SEGMENTS = frozenset({".git"})
_SECRET_SEGMENTS = frozenset({".ssh", ".gnupg", ".aws"})
_SECRET_PATTERNS = (
    ".env",
    ".env.*",
    "*.pem",
    "*.key",
    "*.p12",
    "*.pfx",
    "*.jks",
    "*.kdbx",
    "id_rsa*",
    "id_dsa*",
    "id_ecdsa*",
    "id_ed25519*",
    ".netrc",
    ".npmrc",
    ".htpasswd",
)


class ArchiveError(Exception):
    """归档错误基类；统一对外错误码由整合者登记。"""


class ArchiveRejected(ArchiveError):
    """危险或不受支持的成员：路径逃逸、链接、设备等。"""


class ArchiveLimitExceeded(ArchiveError):
    """超出条目数、深度、单文件或总字节预算。"""


class ArchiveFormatError(ArchiveError):
    """归档流损坏、截断或无法解析。"""


@dataclass(frozen=True)
class ArchiveLimits:
    """归档读写边界；全部字段必须为正。"""

    max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES
    max_file_bytes: int = DEFAULT_MAX_FILE_BYTES
    max_entries: int = DEFAULT_MAX_ENTRIES
    max_depth: int = DEFAULT_MAX_DEPTH

    def __post_init__(self) -> None:
        if min(
            self.max_total_bytes, self.max_file_bytes, self.max_entries, self.max_depth
        ) <= 0:
            raise ValueError("archive limits must be positive")


@dataclass(frozen=True)
class ArchiveFile:
    """导出输入：安全相对路径与内容字节。"""

    name: str
    data: bytes


@dataclass(frozen=True)
class ArchiveEntry:
    """归档成员清单项；sha256在流消费完成前为空串，目录成员也为空串。"""

    name: str
    size: int
    sha256: str
    mode: int
    is_dir: bool


WriteFn = Callable[[bytes], object]
ReadFn = Callable[[int], bytes]
ExtractSink = Callable[[ArchiveEntry, Iterator[bytes]], object]


def is_excluded_name(name: str, exclude_git: bool = True, exclude_secrets: bool = True) -> bool:
    """按路径段与文件名模式判断成员是否落入默认排除范围。"""
    segments = name.split("/")
    for segment in segments:
        if exclude_git and segment in _GIT_SEGMENTS:
            return True
        if exclude_secrets and segment in _SECRET_SEGMENTS:
            return True
    if exclude_secrets:
        base = segments[-1]
        if any(fnmatch.fnmatch(base, pattern) for pattern in _SECRET_PATTERNS):
            return True
    return False


def _short(name: str) -> str:
    shown = name if len(name) <= 120 else name[:117] + "..."
    return repr(shown)


def _safe_segments(name: str, limits: ArchiveLimits) -> list[str]:
    if not name:
        raise ArchiveRejected("empty member name")
    if name.startswith(("/", "\\")):
        raise ArchiveRejected(f"absolute member path rejected: {_short(name)}")
    if "\x00" in name:
        raise ArchiveRejected("member name contains NUL")
    if "\\" in name or ":" in name:
        raise ArchiveRejected(f"member name contains backslash or colon: {_short(name)}")
    stripped = name[:-1] if name.endswith("/") else name
    segments = stripped.split("/")
    if any(segment in ("", ".", "..") for segment in segments):
        raise ArchiveRejected(f"unsafe member path rejected: {_short(name)}")
    if len(segments) > limits.max_depth:
        raise ArchiveLimitExceeded(f"member depth exceeds {limits.max_depth}: {_short(name)}")
    return segments


class _StreamSink:
    """tarfile所需的最小可写文件对象；透传外部write并统计字节数与摘要。"""

    def __init__(self, write: WriteFn) -> None:
        self._write = write
        self._digest = hashlib.sha256()
        self.bytes_written = 0

    def write(self, data: bytes, /) -> int:
        payload = bytes(data)
        self._write(payload)
        self._digest.update(payload)
        self.bytes_written += len(payload)
        return len(payload)

    def tell(self) -> int:
        return self.bytes_written

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence == 1:
            offset = self.bytes_written + offset
        elif whence != 0:
            raise ArchiveFormatError("archive sink does not support end-relative seeks")
        if offset != self.bytes_written:
            raise ArchiveFormatError("archive sink does not support repositioning")
        return self.bytes_written

    def close(self) -> None:
        return None

    def hexdigest(self) -> str:
        return self._digest.hexdigest()


class _PrefixedReader:
    """原始侧读取器；先回放magic探测阶段预读的前缀，再透传底层流。"""

    def __init__(self, read: ReadFn, prefix: bytes) -> None:
        self._read = read
        self._prefix = prefix

    def read(self, size: int = -1, /) -> bytes:
        if size == 0:
            return b""
        want = READ_CHUNK if size < 0 else size
        if self._prefix:
            head = self._prefix[:want]
            self._prefix = self._prefix[len(head):]
            if len(head) >= want:
                return head
            return head + self._read(want - len(head))
        return self._read(want)

    def seekable(self) -> bool:
        return False


class _BoundedReader:
    """解压侧字节预算读取器；必须位于解压层之上，超限即中止。"""

    def __init__(self, read: ReadFn, hard_cap: int) -> None:
        self._read = read
        self._budget = hard_cap
        self.consumed = 0

    def read(self, size: int = -1, /) -> bytes:
        if size == 0:
            return b""
        want = READ_CHUNK if size < 0 else size
        if self._budget <= 0:
            raise ArchiveLimitExceeded("archive stream exceeds decompressed byte budget")
        data = self._read(min(want, self._budget))
        self.consumed += len(data)
        self._budget -= len(data)
        return data

    def seekable(self) -> bool:
        return False


class _DigestCounter:
    def __init__(self) -> None:
        self._digest = hashlib.sha256()
        self.emitted = 0

    def update(self, block: bytes) -> None:
        self._digest.update(block)
        self.emitted += len(block)

    def hexdigest(self) -> str:
        return self._digest.hexdigest()


def _member_chunks(stream: IO[bytes], size: int, counter: _DigestCounter) -> Iterator[bytes]:
    while counter.emitted < size:
        block = stream.read(min(READ_CHUNK, size - counter.emitted))
        if not block:
            raise ArchiveFormatError("archive member data is truncated")
        counter.update(block)
        yield block


def _read_head(read: ReadFn) -> bytes:
    head = bytearray()
    while len(head) < _MAGIC_SIZE:
        block = read(_MAGIC_SIZE - len(head))
        if not block:
            break
        head.extend(block)
    return bytes(head)


def _detect_format(head: bytes) -> str:
    if head.startswith(b"\x1f\x8b"):
        return "gzip"
    if head.startswith(b"BZh"):
        return "bzip2"
    if head.startswith(b"\xfd7zXZ\x00"):
        return "xz"
    return "tar"


def export_archive(
    files: Sequence[ArchiveFile],
    write: WriteFn,
    limits: ArchiveLimits | None = None,
    exclude_git: bool = True,
    exclude_secrets: bool = True,
) -> dict[str, object]:
    """把文件集写为未压缩tar流；返回条目数、字节数、整体摘要与排除清单。"""
    bounds = limits if limits is not None else ArchiveLimits()
    excluded: list[str] = []
    prepared: list[tuple[str, bytes]] = []
    seen: set[str] = set()
    total = 0
    for item in files:
        _safe_segments(item.name, bounds)
        if is_excluded_name(item.name, exclude_git, exclude_secrets):
            excluded.append(item.name)
            continue
        if item.name in seen:
            raise ArchiveRejected(f"duplicate member name: {_short(item.name)}")
        seen.add(item.name)
        if len(item.data) > bounds.max_file_bytes:
            raise ArchiveLimitExceeded(f"file exceeds per-file limit: {_short(item.name)}")
        total += len(item.data)
        if total > bounds.max_total_bytes:
            raise ArchiveLimitExceeded("export exceeds total byte budget")
        if len(prepared) >= bounds.max_entries:
            raise ArchiveLimitExceeded(f"export exceeds {bounds.max_entries} entries")
        prepared.append((item.name, bytes(item.data)))
    sink = _StreamSink(write)
    archive = tarfile.open(fileobj=cast("IO[bytes]", sink), mode="w", format=tarfile.PAX_FORMAT)
    try:
        for name, payload in sorted(prepared):
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            info.mtime = 0
            info.mode = 0o644
            info.type = tarfile.REGTYPE
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            archive.addfile(info, io.BytesIO(payload))
    finally:
        archive.close()
    return {
        "entry_count": len(prepared),
        "total_bytes": total,
        "archive_bytes": sink.bytes_written,
        "sha256": sink.hexdigest(),
        "excluded": excluded,
    }


def export_archive_bytes(
    files: Sequence[ArchiveFile],
    limits: ArchiveLimits | None = None,
    exclude_git: bool = True,
    exclude_secrets: bool = True,
) -> bytes:
    """export_archive的内存便捷封装，返回完整tar字节。"""
    buffer = bytearray()
    export_archive(files, buffer.extend, limits, exclude_git, exclude_secrets)
    return bytes(buffer)


def read_manifest(
    read: ReadFn,
    limits: ArchiveLimits | None = None,
    exclude_git: bool = True,
    exclude_secrets: bool = True,
) -> dict[str, object]:
    """完整扫描归档并返回条目清单（含逐文件摘要）；不做任何落盘。"""
    bounds = limits if limits is not None else ArchiveLimits()
    return _scan(read, bounds, exclude_git, exclude_secrets, None)


def extract_archive(
    read: ReadFn,
    sink: ExtractSink,
    limits: ArchiveLimits | None = None,
    exclude_git: bool = True,
    exclude_secrets: bool = True,
) -> dict[str, object]:
    """流式解析归档，把每个安全成员交给sink；不落盘、不执行包内内容。

    sink需在返回前完整消费数据迭代器；模块随后强制排干以维持流对齐并
    校验成员完整性。返回清单中的sha256为消费完成后的真实摘要。
    """
    bounds = limits if limits is not None else ArchiveLimits()
    return _scan(read, bounds, exclude_git, exclude_secrets, sink)


def chunk_reader(chunks: Iterator[bytes]) -> ReadFn:
    """把字节块迭代器适配为read(n)风格的顺序读取器。"""
    buffer = bytearray()
    exhausted = False

    def read(size: int, /) -> bytes:
        nonlocal exhausted
        while len(buffer) < size and not exhausted:
            block = next(chunks, None)
            if block is None:
                exhausted = True
            else:
                buffer.extend(block)
        head = bytes(buffer[:size])
        del buffer[:size]
        return head

    return read


def _scan(
    read: ReadFn,
    limits: ArchiveLimits,
    exclude_git: bool,
    exclude_secrets: bool,
    sink: ExtractSink | None,
) -> dict[str, object]:
    head = _read_head(read)
    fmt = _detect_format(head)
    raw = _PrefixedReader(read, head)
    entries: list[ArchiveEntry] = []
    excluded: list[str] = []
    total_data = 0
    index = 0
    try:
        with contextlib.ExitStack() as stack:
            source: IO[bytes] = cast("IO[bytes]", raw)
            if fmt == "gzip":
                gz = gzip.GzipFile(fileobj=source, mode="rb")
                source = cast("IO[bytes]", stack.enter_context(gz))
            elif fmt == "bzip2":
                wrapped = bz2.BZ2File(source, mode="rb")
                source = cast("IO[bytes]", stack.enter_context(wrapped))
            elif fmt == "xz":
                decoded = lzma.LZMAFile(source, mode="rb")
                source = cast("IO[bytes]", stack.enter_context(decoded))
            bounded = _BoundedReader(source.read, limits.max_total_bytes + _HEADER_SLACK_BYTES)
            archive = stack.enter_context(
                tarfile.open(fileobj=cast("IO[bytes]", bounded), mode="r|")
            )
            while True:
                member = archive.next()
                if member is None:
                    break
                index += 1
                if index > limits.max_entries:
                    raise ArchiveLimitExceeded(
                        f"archive holds more than {limits.max_entries} entries"
                    )
                name = member.name[:-1] if member.name.endswith("/") else member.name
                _safe_segments(name, limits)
                if is_excluded_name(name, exclude_git, exclude_secrets):
                    excluded.append(name)
                    continue
                mode = int(member.mode) & 0o7777
                if member.isdir():
                    entry = ArchiveEntry(name=name, size=0, sha256="", mode=mode, is_dir=True)
                    entries.append(entry)
                    if sink is not None:
                        sink(entry, iter(()))
                    continue
                if not member.isreg():
                    raise ArchiveRejected(f"refusing non-regular archive member: {_short(name)}")
                if member.size < 0:
                    raise ArchiveFormatError("archive member declares negative size")
                if member.size > limits.max_file_bytes:
                    raise ArchiveLimitExceeded(f"member exceeds per-file limit: {_short(name)}")
                total_data += member.size
                if total_data > limits.max_total_bytes:
                    raise ArchiveLimitExceeded("archive exceeds total byte budget")
                payload = archive.extractfile(member)
                if payload is None:
                    raise ArchiveFormatError(f"member has no data stream: {_short(name)}")
                counter = _DigestCounter()
                chunks = _member_chunks(payload, member.size, counter)
                if sink is not None:
                    staged = ArchiveEntry(
                        name=name, size=member.size, sha256="", mode=mode, is_dir=False
                    )
                    sink(staged, chunks)
                for _ in chunks:
                    pass
                if counter.emitted != member.size:
                    raise ArchiveFormatError("archive member data is truncated")
                entries.append(
                    ArchiveEntry(
                        name=name, size=member.size, sha256=counter.hexdigest(), mode=mode,
                        is_dir=False,
                    )
                )
    except ArchiveError:
        raise
    except (tarfile.TarError, EOFError, OSError, ValueError, lzma.LZMAError) as exc:
        raise ArchiveFormatError(f"archive stream unreadable: {type(exc).__name__}") from exc
    return {"format": fmt, "entries": entries, "excluded": excluded, "total_bytes": total_data}
