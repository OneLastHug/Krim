"""个人MCP凭据保险库；AESGCM认证加密，密文绑定账号上下文，主密钥独立于密文存储。

信封格式 `v1.<keyid>.<base64url(nonce||密文||tag)>`，AAD 覆盖 keyid 与
principal/connection/credential_revision/purpose 的定长编码：跨账号、跨连接、
错凭据版本、错用途或任何篡改在解密时统一按 invalid_input(422) 拒绝，不区分
具体原因、不回传原文。keyid 是密钥指纹（SHA-256 域分隔前缀），密钥本体只接受
32字节，nonce 每次加密由 CSPRNG 新取12字节。主密钥文件要求 0600、普通文件、
单硬链接、全路径无符号链接且属主为当前用户；密钥环支持多密钥读旧写新，rotate
换活跃键并保留旧键，save/load 供轮换持久化；保险库无状态不落密文，主密钥与
密文须由调用方分库存放。cryptography 是可选依赖：导入期不加载，首次加解密时
惰性加载，缺失或损坏时稳定抛 mcp_unavailable(503)，不退化明文、不裸泄后端异常；
解密侧仅真实认证失败（InvalidTag）落入统一的 invalid_input(422)，服务故障不冒充篡改。
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import importlib
import json
import os
import re
import secrets
import stat
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, NoReturn, cast

from pydantic import JsonValue

from krim_agent.account_mcp.contracts import SecretContext
from krim_agent.contracts import ServiceError, parse_json

KEY_BYTES = 32
NONCE_BYTES = 12
TAG_BYTES = 16
# 信封总长上限16KiB；按最长keyid(32字符)推算，明文上限12000时信封必不超限。
MAX_ENVELOPE_BYTES = 16384
MAX_PLAINTEXT_BYTES = 12000
MAX_DECODED_BYTES = NONCE_BYTES + MAX_PLAINTEXT_BYTES + TAG_BYTES
MAX_KEYRING_BYTES = 4096
MAX_KEYS = 16
IDENTITY_LIMIT = 128
PURPOSE_LIMIT = 64
REVISION_LIMIT = 2**31 - 1
ENVELOPE_VERSION = "v1"
KEY_ID = re.compile(r"[A-Za-z0-9_-]{1,32}\Z")
AAD_LABEL = b"krim.secret-aad.v1"
FINGERPRINT_LABEL = b"krim.master-key.v1"

# 惰性持有的 AESGCM 类与 InvalidTag；None 表示尚未加载，加载失败不缓存、按次重试。
_BACKEND: Any = None
_INVALID_TAG: Any = None


def _backend() -> Any:
    global _BACKEND
    if _BACKEND is None:
        try:
            module = importlib.import_module("cryptography.hazmat.primitives.ciphers.aead")
            _BACKEND = module.AESGCM
        except Exception:
            # 缺失或损坏（如原生扩展加载抛OSError）都按服务不可用，不裸泄导入异常。
            raise ServiceError("mcp_unavailable", 503) from None
    return _BACKEND


def _invalid_tag() -> Any:
    """惰性取InvalidTag；取不到时认不出认证失败，解密侧按服务不可用兜底。"""
    global _INVALID_TAG
    if _INVALID_TAG is None:
        try:
            module = importlib.import_module("cryptography.exceptions")
            _INVALID_TAG = module.InvalidTag
        except Exception:
            return None
    return _INVALID_TAG


def _cipher(key: bytes) -> Any:
    """构造AESGCM实例；构造只依赖密钥、与密文无关，失败即服务不可用（统一503）。"""
    try:
        return _backend()(key)
    except ServiceError:
        raise
    except Exception:
        raise ServiceError("mcp_unavailable", 503) from None


def _reject_keyring() -> NoReturn:
    raise ServiceError("storage_unavailable", 503)


def _checked_key(raw: bytes) -> bytes:
    if type(raw) is not bytes or len(raw) != KEY_BYTES:
        raise ServiceError("invalid_input", 422)
    return raw


def _key_id(raw: bytes) -> str:
    """密钥指纹做keyid：确定性且不暴露密钥本体，同密钥得到同id便于幂等部署。"""
    return hashlib.sha256(FINGERPRINT_LABEL + raw).hexdigest()[:16]


def _text(value: object, limit: int) -> bytes:
    """上下文字段严格化：精确str、非空、有界、无控制字符、可无歧义UTF-8。"""
    if type(value) is not str or not value or len(value) > limit:
        raise ServiceError("invalid_input", 422)
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        raise ServiceError("invalid_input", 422) from None
    if any(byte < 0x20 or byte == 0x7F for byte in encoded):
        raise ServiceError("invalid_input", 422)
    return encoded


def _field(value: bytes) -> bytes:
    return b":" + str(len(value)).encode("ascii") + b":" + value


def _aad(key_id: str, context: SecretContext) -> bytes:
    """规范化上下文为定长编码AAD；revision拒绝bool，仅接受有界正整数。"""
    if type(context) is not SecretContext:
        raise ServiceError("invalid_input", 422)
    revision = context.credential_revision
    if type(revision) is not int or not 1 <= revision <= REVISION_LIMIT:
        raise ServiceError("invalid_input", 422)
    return b"".join(
        (
            AAD_LABEL,
            _field(key_id.encode("ascii")),
            _field(_text(context.principal_id, IDENTITY_LIMIT)),
            _field(_text(context.connection_id, IDENTITY_LIMIT)),
            _field(str(revision).encode("ascii")),
            _field(_text(context.purpose, PURPOSE_LIMIT)),
        )
    )


def _decode_payload(payload: str) -> bytes:
    try:
        padded = payload.encode("ascii") + b"=" * (-len(payload) % 4)
        return base64.b64decode(padded, altchars=b"-_", validate=True)
    except ValueError:
        raise ServiceError("invalid_input", 422) from None


@dataclass(frozen=True)
class MasterKeyring:
    """主密钥环：keyid → 32字节密钥；active是唯一写入键，其余键只用于读旧。"""

    active_key_id: str
    keys: Mapping[str, bytes] = field(repr=False)

    def __post_init__(self) -> None:
        if type(self.active_key_id) is not str or KEY_ID.fullmatch(self.active_key_id) is None:
            raise ServiceError("invalid_input", 422)
        if not isinstance(self.keys, Mapping) or not self.keys or len(self.keys) > MAX_KEYS:
            raise ServiceError("invalid_input", 422)
        copied: dict[str, bytes] = {}
        for key_id, raw in self.keys.items():
            if (
                type(key_id) is not str
                or KEY_ID.fullmatch(key_id) is None
                or type(raw) is not bytes
                or len(raw) != KEY_BYTES
                or _key_id(raw) != key_id
            ):
                raise ServiceError("invalid_input", 422)
            copied[key_id] = raw
        if self.active_key_id not in copied:
            raise ServiceError("invalid_input", 422)
        object.__setattr__(self, "keys", MappingProxyType(copied))

    @classmethod
    def create(cls, raw: bytes) -> MasterKeyring:
        key = _checked_key(raw)
        key_id = _key_id(key)
        return cls(key_id, {key_id: key})

    def rotate(self, raw: bytes) -> MasterKeyring:
        """新密钥成为活跃写入键并保留全部旧键；不接受与既有键相同的密钥。"""
        key = _checked_key(raw)
        key_id = _key_id(key)
        if key_id in self.keys:
            raise ServiceError("invalid_input", 422)
        if len(self.keys) >= MAX_KEYS:
            raise ServiceError("input_limit", 422)
        merged = dict(self.keys)
        merged[key_id] = key
        return MasterKeyring(key_id, merged)

    def save(self, path: Path) -> None:
        """原子落盘0600密钥文件；拒绝符号链接路径，写失败清理临时文件。"""
        if not isinstance(path, Path):
            raise ServiceError("invalid_input", 422)
        document = json.dumps(
            {
                "active_key_id": self.active_key_id,
                "keys": {
                    key_id: base64.urlsafe_b64encode(key).decode("ascii")
                    for key_id, key in self.keys.items()
                },
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
        tmp: Path | None = None
        try:
            if path.is_symlink() or any(part.is_symlink() for part in path.parents):
                raise ValueError("符号链接路径")
            tmp = path.parent / f".{path.name}.{os.getpid()}.{secrets.token_hex(6)}.tmp"
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            try:
                view = memoryview(document)
                while view:
                    view = view[os.write(fd, view) :]
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(tmp, path)
            with contextlib.suppress(OSError):
                dir_fd = os.open(path.parent, os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
        except (OSError, ValueError):
            if tmp is not None:
                with contextlib.suppress(OSError):
                    tmp.unlink(missing_ok=True)
            raise ServiceError("storage_unavailable", 503) from None

    @classmethod
    def load(cls, path: Path) -> MasterKeyring:
        if not isinstance(path, Path):
            raise ServiceError("invalid_input", 422)
        value = _read_trusted_json(path)
        if type(value) is not dict or set(value) != {"active_key_id", "keys"}:
            _reject_keyring()
        active = value["active_key_id"]
        entries = value["keys"]
        if type(active) is not str or type(entries) is not dict:
            _reject_keyring()
        if KEY_ID.match(active) is None:
            _reject_keyring()
        if not entries or len(entries) > MAX_KEYS:
            _reject_keyring()
        keys: dict[str, bytes] = {}
        for key_id, encoded in entries.items():
            if type(key_id) is not str or KEY_ID.match(key_id) is None or type(encoded) is not str:
                _reject_keyring()
            try:
                raw = _decode_payload(encoded)
            except ServiceError:
                _reject_keyring()
            if len(raw) != KEY_BYTES or _key_id(raw) != key_id:
                _reject_keyring()
            keys[key_id] = raw
        if active not in keys:
            _reject_keyring()
        try:
            return cls(active, keys)
        except ServiceError:
            _reject_keyring()


def _read_trusted_json(path: Path) -> JsonValue:
    """受信读取：全路径无符号链接、O_NOFOLLOW、普通文件单硬链接、0600、有界。"""
    try:
        if any(part.is_symlink() for part in (path, *path.parents)):
            raise ValueError("符号链接路径")
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            info = os.fstat(fd)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_nlink != 1
                or info.st_mode & 0o077
                or info.st_uid != os.geteuid()
                or info.st_size > MAX_KEYRING_BYTES
            ):
                raise ValueError("主密钥文件不合规")
            raw = os.read(fd, MAX_KEYRING_BYTES + 1)
        finally:
            os.close(fd)
        if len(raw) > MAX_KEYRING_BYTES:
            raise ValueError("主密钥文件超限")
        return parse_json(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError):
        raise ServiceError("storage_unavailable", 503) from None


class AuthenticatedSecretVault:
    """SecretVault 契约的 AESGCM 实现；无状态，不持有密文副本。"""

    def __init__(self, keys: MasterKeyring) -> None:
        if type(keys) is not MasterKeyring:
            raise ServiceError("invalid_input", 422)
        self._keys = keys

    @property
    def active_key_id(self) -> str:
        return self._keys.active_key_id

    def __repr__(self) -> str:
        return f"{type(self).__name__}(active_key_id={self._keys.active_key_id!r})"

    def encrypt(self, context: SecretContext, plaintext: bytes) -> str:
        aad = _aad(self._keys.active_key_id, context)
        if type(plaintext) is not bytes:
            raise ServiceError("invalid_input", 422)
        if len(plaintext) > MAX_PLAINTEXT_BYTES:
            raise ServiceError("input_limit", 422)
        nonce = secrets.token_bytes(NONCE_BYTES)
        cipher = _cipher(self._keys.keys[self._keys.active_key_id])
        try:
            sealed = cast("bytes", cipher.encrypt(nonce, plaintext, aad))
        except Exception:
            # 输入已先校验，此处失败只能是后端故障；按服务不可用，不裸泄异常。
            raise ServiceError("mcp_unavailable", 503) from None
        payload = base64.urlsafe_b64encode(nonce + sealed).rstrip(b"=").decode("ascii")
        return f"{ENVELOPE_VERSION}.{self._keys.active_key_id}.{payload}"

    def decrypt(self, context: SecretContext, ciphertext: str) -> bytes:
        if type(ciphertext) is not str:
            raise ServiceError("invalid_input", 422)
        if len(ciphertext) > MAX_ENVELOPE_BYTES:
            raise ServiceError("invalid_input", 422)
        parts = ciphertext.split(".", 2)
        if len(parts) != 3 or parts[0] != ENVELOPE_VERSION or KEY_ID.match(parts[1]) is None:
            raise ServiceError("invalid_input", 422)
        key_id = parts[1]
        key = self._keys.keys.get(key_id)
        if key is None:
            raise ServiceError("invalid_input", 422)
        decoded = _decode_payload(parts[2])
        if len(decoded) < NONCE_BYTES + TAG_BYTES or len(decoded) > MAX_DECODED_BYTES:
            raise ServiceError("invalid_input", 422)
        aad = _aad(key_id, context)
        # 后端缺失或损坏的503先于认证失败分支抛出，不落入篡改统一的invalid_input。
        cipher = _cipher(key)
        try:
            return cast(
                "bytes", cipher.decrypt(decoded[:NONCE_BYTES], decoded[NONCE_BYTES:], aad)
            )
        except Exception as exc:
            # 仅真实认证失败按篡改统一拒绝：跨上下文、篡改、错键不可区分，不泄原文；
            # 其余异常是服务故障，按mcp_unavailable上抛，不冒充输入问题。
            invalid_tag = _invalid_tag()
            if invalid_tag is not None and isinstance(exc, invalid_tag):
                raise ServiceError("invalid_input", 422) from None
            raise ServiceError("mcp_unavailable", 503) from None

    def reencrypt(self, context: SecretContext, ciphertext: str) -> str:
        """轮换辅助：按旧封套解出原文并立即以当前活跃键重封；原封套保持不变。"""
        return self.encrypt(context, self.decrypt(context, ciphertext))
