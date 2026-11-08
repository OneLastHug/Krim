"""账号级分块上传暂存库。

所有状态与临时文件都保存在构造时给定的受管root内；principal_id从不进入
文件路径，映射为其SHA256前缀目录；会话与内容一律使用内部UUID（32位十六
进制规范形式）。安全边界：

- 目录0700、文件0600；打开任何受管文件时拒绝symlink、hardlink与特殊文件。
- 顺序append：offset必须等于已收字节数；完全落入已收区间的重复块先核实
  摘要与字节再幂等返回，不重复追加；缝隙、跨区间重叠、超出声明总量或
  摘要不符一律拒绝，且不改动已落盘数据。
- request_key同输入幂等：同键同参数回放当前状态；同键不同参数拒绝。
- finish只表示受管暂存对象完整且与expected_sha256一致，不写入任何项目
  目录；后续写入由授权与租约门内的其他模块完成。
- 每账号按声明total_size预留字节，并受对象数量与TTL约束；元数据为定长
  JSON，不保留逐chunk日志，任意chunk次数不会撑大状态。

并发：POSIX上每账号flock串行化（跨线程与跨进程）；其他平台按单调用方
使用。finish与内容读取为不可变对象访问，无需持锁。
"""

from __future__ import annotations

import errno
import hashlib
import json
import os
import shutil
import stat
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

from krim_agent.sandbox.ports import ContentInfo

try:
    import fcntl as _fcntl

    _HAS_FLOCK = True
except ImportError:  # pragma: no cover — Windows开发环境仅静态加载
    _fcntl = None  # type: ignore[assignment]
    _HAS_FLOCK = False

DEFAULT_MAX_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_FILES = 128
DEFAULT_TTL_SECONDS = 3600
DEFAULT_CHUNK_SIZE = 65536

_MAX_CHUNK_SIZE = 4 * 1024 * 1024
_MAX_NAME_LENGTH = 256
_MAX_META_BYTES = 65536
_IO_CHUNK = 65536
_HEX_DIGITS = frozenset("0123456789abcdefABCDEF")
_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)


class TransferError(Exception):
    """暂存库错误基类；统一对外错误码由整合者登记。"""


class InvalidRequest(TransferError):
    """参数不合法或不满足顺序上传语义。"""


class HashMismatch(TransferError):
    """块或整体摘要与声明不符。"""


class QuotaExceeded(TransferError):
    """超出每账号字节预算或对象数量上限。"""


class NotFound(TransferError):
    """请求的暂存会话或内容不存在。"""


class Expired(TransferError):
    """暂存对象已过TTL并被清除。"""


class Conflict(TransferError):
    """request_key被其他上传占用，或受管状态损坏。"""


class UnsafePath(TransferError):
    """受管root内出现symlink、hardlink或特殊文件。"""


def _validate_principal(principal_id: object) -> str:
    if (
        not isinstance(principal_id, str)
        or not principal_id
        or len(principal_id) > _MAX_NAME_LENGTH
        or "\x00" in principal_id
    ):
        raise InvalidRequest("principal_id must be a non-empty string without NUL")
    return principal_id


def _validate_request_key(request_key: object) -> str:
    if (
        not isinstance(request_key, str)
        or not request_key
        or len(request_key) > _MAX_NAME_LENGTH
        or "\x00" in request_key
    ):
        raise InvalidRequest("request_key must be a non-empty string without NUL")
    return request_key


def _validate_digest(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in _HEX_DIGITS for character in value)
    ):
        raise InvalidRequest(f"{field} must be a 64-character hex digest")
    return value.lower()


def _validate_id(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise InvalidRequest(f"{field} must be a canonical 32-char hex id")
    try:
        parsed = uuid.UUID(value)
    except ValueError:
        raise InvalidRequest(f"{field} must be a canonical 32-char hex id") from None
    if parsed.hex != value:
        raise InvalidRequest(f"{field} must be a canonical 32-char hex id")
    return value


def _validate_size(total_size: object, max_bytes: int) -> int:
    if isinstance(total_size, bool) or not isinstance(total_size, int) or total_size < 0:
        raise InvalidRequest("total_size must be a non-negative integer")
    if total_size > max_bytes:
        raise QuotaExceeded("total_size exceeds the per-account staging budget")
    return total_size


def _validate_offset(offset: object) -> int:
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise InvalidRequest("offset must be a non-negative integer")
    return offset


def _validate_chunk_size(chunk_size: object) -> int:
    if (
        isinstance(chunk_size, bool)
        or not isinstance(chunk_size, int)
        or not 1 <= chunk_size <= _MAX_CHUNK_SIZE
    ):
        raise InvalidRequest(f"chunk_size must be an integer in [1, {_MAX_CHUNK_SIZE}]")
    return chunk_size


def _validate_payload(data: object) -> bytes:
    if isinstance(data, bytearray):
        data = bytes(data)
    if not isinstance(data, bytes) or len(data) == 0:
        raise InvalidRequest("data must be a non-empty bytes object")
    return data


def _require_str(meta: dict[str, object], key: str) -> str:
    value = meta.get(key)
    if not isinstance(value, str):
        raise Conflict(f"staged metadata is corrupt: {key}")
    return value


def _require_int(meta: dict[str, object], key: str) -> int:
    value = meta.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise Conflict(f"staged metadata is corrupt: {key}")
    return value


def _require_number(meta: dict[str, object], key: str) -> float:
    value = meta.get(key)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise Conflict(f"staged metadata is corrupt: {key}")
    return float(value)


def _meta_number(meta: dict[str, object] | None, key: str) -> float | None:
    if meta is None:
        return None
    value = meta.get(key)
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value)


class TransferStore:
    """账号级暂存库；finish只封存暂存内容，绝不触碰项目目录。"""

    def __init__(
        self,
        root: Path,
        max_bytes: int = DEFAULT_MAX_BYTES,
        max_files: int = DEFAULT_MAX_FILES,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if max_bytes <= 0 or max_files <= 0 or ttl_seconds <= 0:
            raise ValueError("max_bytes, max_files and ttl_seconds must be positive")
        self._root = Path(root)
        self._max_bytes = max_bytes
        self._max_files = max_files
        self._ttl_seconds = ttl_seconds
        self._clock: Callable[[], float] = clock if clock is not None else time.time
        self._ensure_dir(self._root)
        self._ensure_dir(self._root / "principals")

    # ---- 公共API ----

    def create(
        self, principal_id: str, total_size: int, expected_sha256: str, request_key: str
    ) -> dict[str, object]:
        """声明一次分块上传；同principal同request_key同参数幂等回放。"""
        _validate_principal(principal_id)
        _validate_request_key(request_key)
        declared = _validate_size(total_size, self._max_bytes)
        digest = _validate_digest(expected_sha256, "expected_sha256")
        base = self._ensure_principal(principal_id)
        key_hash = self._request_key_hash(request_key)
        key_path = self._keys_dir(base) / f"{key_hash}.json"
        fingerprint = hashlib.sha256(f"{declared}\x00{digest}".encode("ascii")).hexdigest()
        with self._locked(base):
            replay = self._replay_request(base, key_path, fingerprint, principal_id)
            if replay is not None:
                return replay
            reserved, count = self._staged_usage(base)
            if count + 1 > self._max_files:
                raise QuotaExceeded("per-account staged object limit reached")
            if reserved + declared > self._max_bytes:
                raise QuotaExceeded("per-account staging byte budget exhausted")
            now = self._clock()
            transfer_id = uuid.uuid4().hex
            transfer_dir = self._transfer_dir(base, transfer_id)
            self._ensure_dir(transfer_dir)
            fd = os.open(
                transfer_dir / "data", os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_CLOEXEC, 0o600
            )
            os.close(fd)
            os.chmod(transfer_dir / "data", 0o600)
            meta: dict[str, object] = {
                "kind": "transfer",
                "transfer_id": transfer_id,
                "principal_id": principal_id,
                "total_size": declared,
                "expected_sha256": digest,
                "received_bytes": 0,
                "created_at": now,
                "updated_at": now,
                "request_key_hash": key_hash,
                "request_fingerprint": fingerprint,
            }
            self._write_json(transfer_dir / "meta.json", meta)
            self._write_json(
                key_path,
                {
                    "kind": "request_key",
                    "fingerprint": fingerprint,
                    "transfer_id": transfer_id,
                    "state": "open",
                    "content_id": None,
                },
            )
            return self._transfer_state(principal_id, meta)

    def append(
        self,
        principal_id: str,
        transfer_id: str,
        offset: int,
        data: bytes,
        chunk_sha256: str,
    ) -> dict[str, object]:
        """顺序追加一块；重复块核实后幂等，错序或摘要不符拒绝。"""
        _validate_principal(principal_id)
        _validate_id(transfer_id, "transfer_id")
        start = _validate_offset(offset)
        payload = _validate_payload(data)
        chunk = _validate_digest(chunk_sha256, "chunk_sha256")
        if hashlib.sha256(payload).hexdigest() != chunk:
            raise HashMismatch("chunk_sha256 does not match chunk bytes")
        base = self._require_principal_dir(principal_id)
        with self._locked(base):
            _transfer_dir, meta = self._load_transfer(base, transfer_id)
            received = _require_int(meta, "received_bytes")
            total = _require_int(meta, "total_size")
            end = start + len(payload)
            if end > total:
                raise InvalidRequest("chunk extends beyond declared total_size")
            data_path = _transfer_dir / "data"
            try:
                fd = self._open_file(data_path, os.O_RDWR)
            except FileNotFoundError as exc:
                raise Conflict("staged data missing; cancel and re-upload") from exc
            try:
                if os.fstat(fd).st_size < received:
                    raise Conflict("staged data shorter than recorded progress")
                duplicate = False
                if start == received:
                    if os.fstat(fd).st_size > received:
                        os.ftruncate(fd, received)
                    self._write_at(fd, start, payload)
                    received = end
                elif end <= received:
                    existing = self._read_at(fd, start, len(payload))
                    if len(existing) != len(payload) or existing != payload:
                        raise HashMismatch("duplicate offset holds different bytes")
                    duplicate = True
                else:
                    raise InvalidRequest("non-sequential chunk: gap or straddling overlap")
                meta["received_bytes"] = received
                meta["updated_at"] = self._clock()
                self._write_json(_transfer_dir / "meta.json", meta)
            finally:
                os.close(fd)
            state = self._transfer_state(principal_id, meta)
            state["duplicate"] = duplicate
            return state

    def finish(self, principal_id: str, transfer_id: str) -> ContentInfo:
        """校验暂存完整性与整体摘要，封存为受管内容；不写项目。"""
        _validate_principal(principal_id)
        _validate_id(transfer_id, "transfer_id")
        base = self._require_principal_dir(principal_id)
        with self._locked(base):
            transfer_dir, meta = self._load_transfer(base, transfer_id)
            received = _require_int(meta, "received_bytes")
            total = _require_int(meta, "total_size")
            if received != total:
                raise InvalidRequest(f"transfer incomplete: {received}/{total} bytes")
            expected = _require_str(meta, "expected_sha256")
            data_path = transfer_dir / "data"
            try:
                fd = self._open_file(data_path, os.O_RDONLY)
            except FileNotFoundError as exc:
                raise Conflict("staged data missing; cancel and re-upload") from exc
            try:
                if os.fstat(fd).st_size != total:
                    raise Conflict("staged data size does not match declared total_size")
                actual = self._hash_fd(fd, total)
                os.fsync(fd)
            finally:
                os.close(fd)
            if actual != expected:
                raise HashMismatch("staged content does not match expected_sha256")
            content_id = uuid.uuid4().hex
            content_dir = self._content_dir(base, content_id)
            self._ensure_dir(content_dir)
            os.replace(data_path, content_dir / "data")
            self._write_json(
                content_dir / "meta.json",
                {
                    "kind": "content",
                    "content_id": content_id,
                    "principal_id": principal_id,
                    "size": total,
                    "sha256": actual,
                    "finished_at": self._clock(),
                },
            )
            key_hash = meta.get("request_key_hash")
            if isinstance(key_hash, str) and key_hash:
                key_path = self._keys_dir(base) / f"{key_hash}.json"
                record = self._read_json(key_path)
                if record is not None:
                    record["state"] = "finished"
                    record["content_id"] = content_id
                    self._write_json(key_path, record)
            shutil.rmtree(transfer_dir)
            return ContentInfo(
                content_id=content_id, principal_id=principal_id, size=total, sha256=actual
            )

    def transfer(self, principal_id: str, transfer_id: str) -> dict[str, object]:
        """查询上传会话当前进度，用于断点续传。"""
        _validate_principal(principal_id)
        _validate_id(transfer_id, "transfer_id")
        base = self._require_principal_dir(principal_id)
        with self._locked(base):
            _transfer_dir, meta = self._load_transfer(base, transfer_id)
        return self._transfer_state(principal_id, meta)

    def cancel(self, principal_id: str, transfer_id: str) -> dict[str, object]:
        """放弃一次未完成上传并释放其预留额度。"""
        _validate_principal(principal_id)
        _validate_id(transfer_id, "transfer_id")
        base = self._require_principal_dir(principal_id)
        with self._locked(base):
            meta = self._read_json(self._transfer_dir(base, transfer_id) / "meta.json")
            if meta is None:
                raise NotFound("no such transfer")
            if _require_str(meta, "kind") != "transfer":
                raise Conflict("staged object kind mismatch")
            self._remove_transfer(base, meta)
            return {"transfer_id": transfer_id, "principal_id": principal_id, "status": "cancelled"}

    def info(self, principal_id: str, content_id: str) -> ContentInfo:
        """返回已完成暂存内容的元信息。"""
        _meta = self._load_content(principal_id, content_id)[1]
        return ContentInfo(
            content_id=_require_str(_meta, "content_id"),
            principal_id=principal_id,
            size=_require_int(_meta, "size"),
            sha256=_require_str(_meta, "sha256"),
        )

    def chunks(
        self, principal_id: str, content_id: str, chunk_size: int = DEFAULT_CHUNK_SIZE
    ) -> Iterator[bytes]:
        """按块流式读取暂存内容；与ports.ContentSource对齐。"""
        size = _validate_chunk_size(chunk_size)
        content_dir, meta = self._load_content(principal_id, content_id)
        total = _require_int(meta, "size")
        try:
            fd = self._open_file(content_dir / "data", os.O_RDONLY)
        except FileNotFoundError as exc:
            raise Conflict("staged content data missing") from exc
        if os.fstat(fd).st_size != total:
            os.close(fd)
            raise Conflict("staged content size mismatch")
        return self._iter_chunks(fd, total, size)

    def cleanup_expired(self) -> dict[str, int]:
        """清除过期上传会话、过期内容、孤儿目录、失效键记录与崩溃遗留临时文件。

        每账号扫描与删除都在该账号的flock内进行，与并发写者串行：先取锁再读
        最新元数据，避免基于过期快照删除恰好被续传刷新的活跃会话或清掉进行
        中的键写入。
        """
        now = self._clock()
        removed_transfers = 0
        removed_contents = 0
        removed_keys = 0
        scanned = 0
        try:
            principals = sorted(os.listdir(self._root / "principals"))
        except (FileNotFoundError, NotADirectoryError):
            principals = []
        for entry in principals:
            base = self._root / "principals" / entry
            try:
                self._check_dir(base)
            except (UnsafePath, FileNotFoundError):
                continue
            scanned += 1
            with self._locked(base):
                for candidate in self._list_children(base / "transfers"):
                    meta = self._read_json(candidate / "meta.json")
                    updated = _meta_number(meta, "updated_at")
                    if meta is not None and meta.get("kind") == "transfer" and updated is not None:
                        expired = now > updated + self._ttl_seconds
                    else:
                        expired = self._is_stale(candidate, now)
                    if expired:
                        if meta is not None:
                            self._remove_transfer(base, meta)
                        else:
                            self._remove_tree(candidate)
                        removed_transfers += 1
                for candidate in self._list_children(base / "contents"):
                    meta = self._read_json(candidate / "meta.json")
                    finished = _meta_number(meta, "finished_at")
                    if meta is not None and meta.get("kind") == "content" and finished is not None:
                        expired = now > finished + self._ttl_seconds
                    else:
                        expired = self._is_stale(candidate, now)
                    if expired:
                        self._remove_tree(candidate)
                        removed_contents += 1
                for key_path in self._list_children(base / "keys"):
                    record = self._read_json(key_path)
                    if record is None:
                        if self._is_stale(key_path, now):
                            self._unlink(key_path)
                            removed_keys += 1
                        continue
                    if self._key_record_alive(base, record):
                        continue
                    self._unlink(key_path)
                    removed_keys += 1
                for tmp_path in self._stale_tmp_files(base / "keys", now):
                    self._unlink(tmp_path)
                    removed_keys += 1
        return {
            "removed_transfers": removed_transfers,
            "removed_contents": removed_contents,
            "removed_keys": removed_keys,
            "scanned_principals": scanned,
        }

    # ---- 路径与锁 ----

    def _principal_dir(self, principal_id: str) -> Path:
        digest = hashlib.sha256(
            b"krim-transfer-v1\x00" + principal_id.encode("utf-8")
        ).hexdigest()
        return self._root / "principals" / digest[:32]

    def _transfer_dir(self, base: Path, transfer_id: str) -> Path:
        return base / "transfers" / transfer_id

    def _content_dir(self, base: Path, content_id: str) -> Path:
        return base / "contents" / content_id

    def _keys_dir(self, base: Path) -> Path:
        return base / "keys"

    @staticmethod
    def _request_key_hash(request_key: str) -> str:
        return hashlib.sha256(
            b"krim-request-key\x00" + request_key.encode("utf-8")
        ).hexdigest()[:32]

    @staticmethod
    def _check_dir(path: Path) -> None:
        try:
            info = os.lstat(path)
        except FileNotFoundError:
            raise
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise UnsafePath(f"managed path is not a real directory: {path.name}")

    def _ensure_dir(self, path: Path) -> None:
        try:
            os.lstat(path)
        except FileNotFoundError:
            os.mkdir(path, 0o700)
        else:
            self._check_dir(path)
        os.chmod(path, 0o700)

    def _ensure_principal(self, principal_id: str) -> Path:
        base = self._principal_dir(principal_id)
        self._ensure_dir(base)
        self._ensure_dir(base / "transfers")
        self._ensure_dir(base / "contents")
        self._ensure_dir(base / "keys")
        return base

    def _require_principal_dir(self, principal_id: str) -> Path:
        base = self._principal_dir(principal_id)
        try:
            self._check_dir(base)
        except FileNotFoundError:
            raise NotFound("no staged objects for this principal") from None
        return base

    @contextmanager
    def _locked(self, base: Path) -> Iterator[None]:
        fd = self._open_file(base / ".lock", os.O_RDWR | os.O_CREAT)
        try:
            if _HAS_FLOCK and _fcntl is not None:
                _fcntl.flock(fd, _fcntl.LOCK_EX)
            yield
        finally:
            os.close(fd)

    @staticmethod
    def _open_file(path: Path, flags: int) -> int:
        try:
            fd = os.open(path, flags | _O_NOFOLLOW | _O_CLOEXEC, 0o600)
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise UnsafePath("symlink refused in managed store") from exc
            raise
        try:
            info = os.fstat(fd)
        except OSError:
            os.close(fd)
            raise
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            os.close(fd)
            raise UnsafePath("managed store refuses hardlinks and special files")
        return fd

    # ---- 元数据读写 ----

    def _write_json(self, path: Path, payload: dict[str, object]) -> None:
        tmp = path.parent / (".tmp-" + uuid.uuid4().hex)
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_CLOEXEC, 0o600)
        try:
            with os.fdopen(fd, "wb", closefd=True) as handle:
                handle.write(json.dumps(payload, sort_keys=True).encode("utf-8"))
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass
            raise
        os.replace(tmp, path)

    def _read_json(self, path: Path) -> dict[str, object] | None:
        try:
            fd = self._open_file(path, os.O_RDONLY)
        except FileNotFoundError:
            return None
        with os.fdopen(fd, "rb", closefd=True) as handle:
            raw = handle.read(_MAX_META_BYTES + 1)
        if len(raw) > _MAX_META_BYTES:
            raise Conflict("staged metadata exceeds bounded size")
        try:
            value: object = json.loads(raw)
        except ValueError as exc:
            raise Conflict("staged metadata is corrupt") from exc
        if not isinstance(value, dict):
            raise Conflict("staged metadata is not an object")
        return value

    @staticmethod
    def _unlink(path: Path) -> None:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass

    @staticmethod
    def _remove_tree(path: Path) -> None:
        try:
            info = os.lstat(path)
        except FileNotFoundError:
            return
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            os.unlink(path)
        else:
            shutil.rmtree(path)

    @staticmethod
    def _list_children(directory: Path) -> list[Path]:
        try:
            names = sorted(os.listdir(directory))
        except (FileNotFoundError, NotADirectoryError):
            return []
        return [directory / name for name in names if not name.startswith(".")]

    def _stale_tmp_files(self, directory: Path, now: float) -> list[Path]:
        """列出崩溃写入遗留、已过TTL的.tmp-*临时文件（仅普通文件）。"""
        try:
            names = sorted(os.listdir(directory))
        except (FileNotFoundError, NotADirectoryError):
            return []
        stale: list[Path] = []
        for name in names:
            if not name.startswith(".tmp-"):
                continue
            candidate = directory / name
            try:
                info = os.lstat(candidate)
            except OSError:
                continue
            if stat.S_ISREG(info.st_mode) and info.st_mtime < now - self._ttl_seconds:
                stale.append(candidate)
        return stale

    def _is_stale(self, path: Path, now: float) -> bool:
        try:
            info = os.lstat(path)
        except OSError:
            return False
        return info.st_mtime < now - self._ttl_seconds

    # ---- 会话与内容装载 ----

    def _load_transfer(self, base: Path, transfer_id: str) -> tuple[Path, dict[str, object]]:
        transfer_dir = self._transfer_dir(base, transfer_id)
        meta = self._read_json(transfer_dir / "meta.json")
        if meta is None:
            raise NotFound("no such transfer")
        if _require_str(meta, "kind") != "transfer":
            raise Conflict("staged object kind mismatch")
        if self._clock() > _require_number(meta, "updated_at") + self._ttl_seconds:
            self._remove_transfer(base, meta)
            raise Expired("transfer expired before completion")
        return transfer_dir, meta

    def _load_content(self, principal_id: str, content_id: str) -> tuple[Path, dict[str, object]]:
        _validate_principal(principal_id)
        _validate_id(content_id, "content_id")
        base = self._principal_dir(principal_id)
        content_dir = self._content_dir(base, content_id)
        meta = self._read_json(content_dir / "meta.json")
        if meta is None:
            raise NotFound("no such staged content")
        if _require_str(meta, "kind") != "content":
            raise Conflict("staged object kind mismatch")
        if self._clock() > _require_number(meta, "finished_at") + self._ttl_seconds:
            self._remove_tree(content_dir)
            raise Expired("staged content expired")
        return content_dir, meta

    def _replay_request(
        self, base: Path, key_path: Path, fingerprint: str, principal_id: str
    ) -> dict[str, object] | None:
        record = self._read_json(key_path)
        if record is None:
            return None
        if _require_str(record, "fingerprint") != fingerprint:
            raise Conflict("request_key already bound to a different upload")
        state = _require_str(record, "state")
        transfer_id = _validate_id(_require_str(record, "transfer_id"), "transfer_id")
        if state == "open":
            try:
                _transfer_dir, meta = self._load_transfer(base, transfer_id)
            except (NotFound, Expired):
                self._unlink(key_path)
                return None
            return self._transfer_state(principal_id, meta)
        if state == "finished":
            content_id = _validate_id(_require_str(record, "content_id"), "content_id")
            try:
                _content_dir, content_meta = self._load_content(principal_id, content_id)
            except (NotFound, Expired):
                self._unlink(key_path)
                return None
            return {
                "transfer_id": transfer_id,
                "principal_id": principal_id,
                "status": "finished",
                "content_id": _require_str(content_meta, "content_id"),
                "total_size": _require_int(content_meta, "size"),
                "expected_sha256": _require_str(content_meta, "sha256"),
                "received_bytes": _require_int(content_meta, "size"),
                "next_offset": _require_int(content_meta, "size"),
            }
        raise Conflict("request_key record state is corrupt")

    def _key_record_alive(self, base: Path, record: dict[str, object]) -> bool:
        state = record.get("state")
        if state == "open":
            transfer_id = record.get("transfer_id")
            if isinstance(transfer_id, str):
                path = self._transfer_dir(base, transfer_id) / "meta.json"
                return self._read_json(path) is not None
        if state == "finished":
            content_id = record.get("content_id")
            if isinstance(content_id, str):
                path = self._content_dir(base, content_id) / "meta.json"
                return self._read_json(path) is not None
        return False

    def _remove_transfer(self, base: Path, meta: dict[str, object]) -> None:
        self._remove_tree(self._transfer_dir(base, _require_str(meta, "transfer_id")))
        key_hash = meta.get("request_key_hash")
        if isinstance(key_hash, str) and key_hash:
            self._unlink(self._keys_dir(base) / f"{key_hash}.json")

    def _staged_usage(self, base: Path) -> tuple[int, int]:
        """返回（已按声明预留的字节, 元数据完好的暂存对象数）。"""
        reserved = 0
        count = 0
        for candidate in self._list_children(base / "transfers"):
            meta = self._read_json(candidate / "meta.json")
            if meta is None or meta.get("kind") != "transfer":
                continue
            size = _meta_number(meta, "total_size")
            if size is not None:
                reserved += int(size)
            count += 1
        for candidate in self._list_children(base / "contents"):
            meta = self._read_json(candidate / "meta.json")
            if meta is None or meta.get("kind") != "content":
                continue
            size = _meta_number(meta, "size")
            if size is not None:
                reserved += int(size)
            count += 1
        return reserved, count

    def _transfer_state(self, principal_id: str, meta: dict[str, object]) -> dict[str, object]:
        received = _require_int(meta, "received_bytes")
        updated = _require_number(meta, "updated_at")
        return {
            "transfer_id": _require_str(meta, "transfer_id"),
            "principal_id": principal_id,
            "status": "open",
            "total_size": _require_int(meta, "total_size"),
            "expected_sha256": _require_str(meta, "expected_sha256"),
            "received_bytes": received,
            "next_offset": received,
            "created_at": _require_number(meta, "created_at"),
            "updated_at": updated,
            "expires_at": updated + self._ttl_seconds,
        }

    # ---- 字节级工具 ----

    @staticmethod
    def _write_at(fd: int, offset: int, data: bytes) -> None:
        os.lseek(fd, offset, os.SEEK_SET)
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            view = view[written:]

    @staticmethod
    def _read_at(fd: int, offset: int, length: int) -> bytes:
        os.lseek(fd, offset, os.SEEK_SET)
        blocks: list[bytes] = []
        remaining = length
        while remaining > 0:
            block = os.read(fd, min(remaining, _IO_CHUNK))
            if not block:
                break
            blocks.append(block)
            remaining -= len(block)
        return b"".join(blocks)

    @staticmethod
    def _hash_fd(fd: int, length: int) -> str:
        os.lseek(fd, 0, os.SEEK_SET)
        digest = hashlib.sha256()
        consumed = 0
        while consumed < length:
            block = os.read(fd, min(_IO_CHUNK, length - consumed))
            if not block:
                break
            digest.update(block)
            consumed += len(block)
        return digest.hexdigest()

    @staticmethod
    def _iter_chunks(fd: int, size: int, chunk_size: int) -> Iterator[bytes]:
        consumed = 0
        try:
            while consumed < size:
                block = os.read(fd, min(chunk_size, size - consumed))
                if not block:
                    raise Conflict("staged content truncated")
                consumed += len(block)
                yield block
        finally:
            os.close(fd)
