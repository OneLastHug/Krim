"""受限 Tavily Web 搜索/提取适配器。"""

from __future__ import annotations

import asyncio
import codecs
import hashlib
import ipaddress
import json
import math
import os
import re
import secrets
import stat
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from tempfile import TemporaryDirectory, TemporaryFile
from typing import Any, BinaryIO, cast
from urllib.parse import urlsplit, urlunsplit

import httpx
from pydantic import Field, JsonValue, SecretStr, field_validator

from krim_agent.contracts import ServiceError, StrictModel

_DEFAULT_ENDPOINT = "https://api.tavily.com"
_VERSION = "krim-tavily/2"
_CONCURRENCY = 4
_MAX_RESULTS = 10
_MAX_CHARS = 15_000
_SEARCH_CONTENT_CHARS = 3_000
_OPERATION_SECONDS = 20.0
_RETRY_ATTEMPTS = 2
_CONNECT_TIMEOUT = 2.0
_READ_TIMEOUT = 8.0
_WRITE_TIMEOUT = 2.0
_POOL_TIMEOUT = 2.0
_RESULT_CHUNK_BYTES = 64 * 1024
_TOOL_JSON_BYTES = 60 * 1024
_RESULT_REF_PATTERN = re.compile(r"^wr_[0-9a-f]{32}$")
_KEY_PATTERN = re.compile(r"^[A-Za-z0-9._~+/-]+=*$")
_HOST_LABEL = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
_PERCENT_ESCAPE = re.compile(r"%(?![0-9a-fA-F]{2})")
_HEX_DIGEST = re.compile(r"^[0-9a-f]{64}$")


class WebAccessConfig(StrictModel):
    """只保存服务端凭据；冻结配置的 repr 不展示密钥或endpoint。"""

    api_key: SecretStr | None = Field(default=None, repr=False)
    endpoint: str = Field(default=_DEFAULT_ENDPOINT, min_length=1, max_length=2048, repr=False)

    @field_validator("api_key")
    @classmethod
    def validate_key(cls, value: SecretStr | None) -> SecretStr | None:
        if value is None:
            return None
        raw = value.get_secret_value()
        if not raw or len(raw) > 4096 or _KEY_PATTERN.fullmatch(raw) is None:
            raise ValueError("无效 Web API 密钥")
        return value

    @field_validator("endpoint")
    @classmethod
    def validate_endpoint(cls, value: str) -> str:
        if (
            not value.isascii()
            or any(char.isspace() or ord(char) < 33 for char in value)
            or any(char in value for char in "\\%?#")
        ):
            raise ValueError("无效 Web 服务地址")
        try:
            parts = urlsplit(value)
            port = parts.port
        except ValueError:
            raise ValueError("无效 Web 服务地址") from None
        host = parts.hostname or ""
        if (
            parts.scheme not in {"http", "https"}
            or not host
            or parts.username is not None
            or parts.password is not None
            or "@" in parts.netloc
            or parts.query
            or parts.fragment
            or ("?" in value or "#" in value)
            or (port is not None and not 1 <= port <= 65535)
        ):
            raise ValueError("无效 Web 服务地址")

        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            normalized_host = host.lower()
            labels = normalized_host.rstrip(".").split(".")
            if (
                not normalized_host.isascii()
                or len(normalized_host) > 253
                or (len(labels) < 2 and normalized_host != "localhost")
                or any(not _HOST_LABEL.fullmatch(label) for label in labels)
            ):
                raise ValueError("无效 Web 服务主机") from None
            local_host = normalized_host.rstrip(".") == "localhost" or normalized_host.endswith(
                ".localhost"
            )
            authority_host = normalized_host
        else:
            normalized_host = address.compressed.lower()
            local_host = address.is_loopback
            authority_host = f"[{normalized_host}]" if address.version == 6 else normalized_host

        if parts.scheme == "http" and not local_host:
            raise ValueError("明文 Web 服务地址仅允许本机")
        path = parts.path.rstrip("/")
        if (
            len(path) > 512
            or re.fullmatch(r"[A-Za-z0-9/_-]*", path) is None
            or "//" in path
            or any(segment in {".", ".."} for segment in path.split("/") if segment)
        ):
            raise ValueError("无效 Web 服务路径")
        authority = authority_host + (f":{port}" if port is not None else "")
        return urlunsplit((parts.scheme, authority, path, "", ""))


@dataclass(frozen=True, slots=True)
class _ResultRecord:
    scope: str
    result_ref: str
    source: str
    title: str
    digest: str
    total_chars: int
    path: Path


class TavilyWebAccess:
    """HTTPX 异步客户端与按运行隔离的磁盘 Web 结果引用。"""

    def __init__(
        self,
        config: WebAccessConfig,
        *,
        client: httpx.AsyncClient | None = None,
        result_root: Path | None = None,
    ):
        self._config = config.model_copy(deep=True)
        self._owns_client = client is None
        self._client = (
            client
            if client is not None
            else httpx.AsyncClient(
                trust_env=False,
                follow_redirects=False,
                timeout=self._timeout(),
                limits=httpx.Limits(
                    max_connections=_CONCURRENCY,
                    max_keepalive_connections=_CONCURRENCY,
                ),
            )
        )
        self._semaphore = asyncio.Semaphore(_CONCURRENCY)
        self._close_lock = asyncio.Lock()
        self._closed = False
        self._private_scope = "private-" + secrets.token_hex(16)
        self._results: dict[tuple[str, str], _ResultRecord] = {}
        self._unpublished: set[Path] = set()
        self._publish_directories: dict[Path, int] = {}
        self._temporary_root: TemporaryDirectory[str] | None = None
        if result_root is None:
            self._temporary_root = TemporaryDirectory(prefix="krim-web-results-")
            self._result_root = Path(self._temporary_root.name)
        else:
            self._result_root = Path(result_root).absolute()
            with self._result_directory(self._result_root, create=True):
                pass

    @property
    def configuration_digest(self) -> str:
        public_configuration = {
            "endpoint": self._config.endpoint,
            "version": _VERSION,
            "limits": {
                "concurrency": _CONCURRENCY,
                "max_results": _MAX_RESULTS,
                "max_chars": _MAX_CHARS,
                "search_content_chars": _SEARCH_CONTENT_CHARS,
                "operation_seconds": _OPERATION_SECONDS,
                "retry_attempts": _RETRY_ATTEMPTS,
                "timeouts": [_CONNECT_TIMEOUT, _READ_TIMEOUT, _WRITE_TIMEOUT, _POOL_TIMEOUT],
                "result_page": "disk-reference/1",
            },
        }
        encoded = json.dumps(
            public_configuration,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    async def search(
        self,
        query: str,
        max_results: int = 5,
        *,
        scope: str | None = None,
    ) -> dict[str, JsonValue]:
        active_scope = self._scope(scope)
        secret = self._config.api_key.get_secret_value() if self._config.api_key else ""
        if not isinstance(query, str) or not query.strip() or (secret and secret in query):
            raise ServiceError("web_invalid_input", 422)
        try:
            query_size = len(query.encode("utf-8"))
        except UnicodeEncodeError:
            raise ServiceError("web_invalid_input", 422) from None
        if query_size > 4096 or any(ord(char) < 32 and char not in "\t\n\r" for char in query):
            raise ServiceError("web_invalid_input", 422)
        if type(max_results) is not int or not 1 <= max_results <= _MAX_RESULTS:
            raise ServiceError("web_invalid_input", 422)

        payload: dict[str, JsonValue] = {
            "query": query,
            "search_depth": "basic",
            "max_results": max_results,
            "include_answer": False,
            "include_raw_content": False,
            "include_usage": True,
        }
        response_data = await self._post("search", payload)
        raw_results = response_data.get("results")
        if not isinstance(raw_results, list):
            raise ServiceError("web_protocol", 502)
        results: list[dict[str, JsonValue]] = []
        provider_truncated = len(raw_results) > max_results
        published: list[_ResultRecord] = []
        try:
            for item in raw_results[:max_results]:
                if not isinstance(item, dict):
                    raise ServiceError("web_protocol", 502)
                title = item.get("title", "")
                url = item.get("url", "")
                content = item.get("content", "")
                if (
                    not isinstance(title, str) or not isinstance(url, str)
                    or not isinstance(content, str)
                ):
                    raise ServiceError("web_protocol", 502)
                safe_title, title_cut = self._clip_metadata(self._redact(title))
                safe_url, url_cut = self._clip_metadata(self._redact(url))
                provider_truncated = provider_truncated or title_cut or url_cut
                record = await self._publish(
                    active_scope, "search", safe_url, safe_title, content,
                )
                published.append(record)
                item["content"] = ""
                del content
                page, metadata = await self._page(record, 0, _SEARCH_CONTENT_CHARS)
                provider_truncated = provider_truncated or metadata["next_offset"] is not None
                results.append({
                    "title": safe_title, "url": safe_url, "content": page,
                    **self._metadata_json(record, metadata),
                })
                item["title"] = ""
                item["url"] = ""
            raw_results.clear()
            response_data["results"] = []
            return self._fit_output("search", results, response_data, provider_truncated)
        except BaseException:
            for record in published:
                self._discard(record)
            raise

    async def fetch(
        self,
        url: str | None = None,
        max_chars: int = _MAX_CHARS,
        *,
        result_ref: str | None = None,
        offset: int = 0,
        scope: str | None = None,
    ) -> dict[str, JsonValue]:
        active_scope = self._scope(scope)
        self._require_configured()
        if type(max_chars) is not int or not 1 <= max_chars <= _MAX_CHARS:
            raise ServiceError("web_invalid_input", 422)
        if type(offset) is not int or offset < 0:
            raise ServiceError("web_invalid_input", 422)
        if (url is None) == (result_ref is None):
            raise ServiceError("web_invalid_input", 422)

        if result_ref is not None:
            if _RESULT_REF_PATTERN.fullmatch(result_ref) is None:
                raise ServiceError("web_invalid_input", 422)
            record = self._lookup(active_scope, result_ref)
            page, metadata = await self._page(record, offset, max_chars)
            return self._fit_output(
                "fetch",
                [
                    {
                        "title": record.title or record.source,
                        "url": record.source,
                        "content": page,
                        **self._metadata_json(record, metadata),
                    }
                ],
                {"usage": None, "request_id": None},
                metadata["next_offset"] is not None,
            )

        assert url is not None
        secret = self._config.api_key.get_secret_value() if self._config.api_key else ""
        if secret and secret in url:
            raise ServiceError("web_invalid_input", 422)
        safe_url = self._public_url(url)
        payload: dict[str, JsonValue] = {"urls": [safe_url], "format": "markdown"}
        response_data = await self._post("extract", payload)
        raw_results = response_data.get("results")
        failed_results = response_data.get("failed_results", [])
        if not isinstance(raw_results, list) or not isinstance(failed_results, list):
            raise ServiceError("web_protocol", 502)
        if failed_results or len(raw_results) != 1 or not isinstance(raw_results[0], dict):
            raise ServiceError("web_extract_failed", 502)

        item = raw_results[0]
        result_url = item.get("url")
        content = item.get("content", item.get("raw_content"))
        title = item.get("title", safe_url)
        if (
            not isinstance(result_url, str)
            or not isinstance(title, str)
            or not isinstance(content, str)
            or not self._same_public_url(result_url, safe_url)
        ):
            raise ServiceError("web_protocol", 502)
        safe_result_url, url_cut = self._clip_metadata(self._redact(result_url))
        safe_title, title_cut = self._clip_metadata(self._redact(title))
        record = await self._publish(
            active_scope, "fetch", safe_result_url, safe_title, content
        )
        item["content"] = ""
        item["raw_content"] = ""
        del content
        raw_results.clear()
        response_data["results"] = []
        try:
            page, metadata = await self._page(record, offset, max_chars)
        except asyncio.CancelledError:
            self._discard(record)
            raise
        return self._fit_output(
            "fetch",
            [
                {
                    "title": safe_title,
                    "url": safe_result_url,
                    "content": page,
                    **self._metadata_json(record, metadata),
                }
            ],
            response_data,
            metadata["next_offset"] is not None or title_cut or url_cut,
        )

    def snapshot(self, scope: str) -> dict[str, Any]:
        """Return JSON-safe metadata; paths and secret-bearing response data never leave storage."""
        active_scope = self._scope(scope)
        entries = [
            {
                "id": record.result_ref,
                "source": record.source,
                "hash": record.digest,
            }
            for (owner, _), record in self._results.items()
            if owner == active_scope
        ]
        entries.sort(key=lambda item: item["id"])
        return {
            "version": "web-results/1",
            "scope_hash": self._scope_digest(active_scope),
            "configuration_digest": self.configuration_digest,
            "results": entries,
        }

    def restore(self, scope: str, state: Mapping[str, Any]) -> None:
        """Restore only references whose host-owned file and digest still match."""
        active_scope = self._scope(scope)
        if not isinstance(state, Mapping):
            raise ServiceError("facts_corrupt", 409)
        if (
            state.get("version") != "web-results/1"
            or state.get("scope_hash") != self._scope_digest(active_scope)
        ):
            raise ServiceError("tool_binding_changed", 409)
        if state.get("configuration_digest") != self.configuration_digest:
            raise ServiceError("tool_binding_changed", 409)
        entries = state.get("results")
        if not isinstance(entries, list):
            raise ServiceError("facts_corrupt", 409)

        restored: list[_ResultRecord] = []
        seen: set[str] = set()
        for entry in entries:
            if not isinstance(entry, Mapping):
                raise ServiceError("facts_corrupt", 409)
            result_ref = entry.get("id")
            source = entry.get("source")
            digest = entry.get("hash")
            if (
                not isinstance(result_ref, str)
                or _RESULT_REF_PATTERN.fullmatch(result_ref) is None
                or result_ref in seen
                or not isinstance(source, str)
                or not source
                or len(source) > 4096
                or not isinstance(digest, str)
                or _HEX_DIGEST.fullmatch(digest) is None
            ):
                raise ServiceError("facts_corrupt", 409)
            seen.add(result_ref)
            path = self._result_path(active_scope, result_ref)
            total_chars, actual_digest = self._verify_result_file(path)
            if actual_digest != digest:
                raise ServiceError("facts_corrupt", 409)
            restored.append(
                _ResultRecord(
                    active_scope,
                    result_ref,
                    source,
                    source,
                    digest,
                    total_chars,
                    path,
                )
            )
        # Restoring an earlier checkpoint replaces this run's view rather than
        # retaining references created after the checkpoint. Other runs remain.
        for key in tuple(self._results):
            if key[0] == active_scope:
                del self._results[key]
        for record in restored:
            self._results[active_scope, record.result_ref] = record

    async def close(self) -> None:
        async with self._close_lock:
            if self._closed:
                return
            self._closed = True
            for path in tuple(self._unpublished):
                self._remove_file(path)
            self._unpublished.clear()
            self._results.clear()
            if self._owns_client:
                await self._client.aclose()
            if self._temporary_root is not None:
                self._temporary_root.cleanup()
                self._temporary_root = None

    def _require_configured(self) -> None:
        if self._closed:
            raise ServiceError("web_closed", 409)
        if not self._config.api_key:
            raise ServiceError("web_config", 422)

    def _scope(self, scope: str | None) -> str:
        active_scope = self._private_scope if scope is None else scope
        if (
            not isinstance(active_scope, str)
            or not active_scope
            or len(active_scope) > 512
            or any(ord(char) < 32 for char in active_scope)
        ):
            raise ServiceError("web_invalid_input", 422)
        return active_scope

    @staticmethod
    def _scope_digest(scope: str) -> str:
        return hashlib.sha256(scope.encode("utf-8")).hexdigest()

    def _result_path(self, scope: str, result_ref: str) -> Path:
        # Neither model-provided scope nor ref is used as a host path component.
        scope_dir = self._result_root / "results" / self._scope_digest(scope)
        ref_name = hashlib.sha256(result_ref.encode("ascii")).hexdigest()
        return scope_dir / ref_name

    @contextmanager
    def _result_directory(self, directory: Path, *, create: bool = False) -> Iterator[int]:
        """Walk from filesystem root using pinned no-follow directory handles."""
        if os.open not in os.supports_dir_fd or not hasattr(os, "O_NOFOLLOW"):
            # Deployed runtimes use POSIX. Never silently fall back to following
            # a model-result path through a Windows junction or symlink.
            raise ServiceError("web_config", 422)
        absolute = directory.absolute()
        current = os.open(absolute.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        walked = Path(absolute.anchor)
        try:
            for component in absolute.parts[1:]:
                if component in {"", ".", ".."}:
                    raise ServiceError("web_config", 422)
                if create:
                    try:
                        os.mkdir(component, mode=0o700, dir_fd=current)
                    except FileExistsError:
                        pass
                child = os.open(
                    component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=current,
                )
                os.close(current)
                current = child
                walked /= component
                if walked == self._result_root or self._result_root in walked.parents:
                    info = os.fstat(current)
                    if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077:
                        raise ServiceError("web_config", 422)
            yield current
        except FileNotFoundError:
            raise ServiceError("not_found", 404) from None
        except OSError:
            raise ServiceError("facts_corrupt", 409) from None
        finally:
            os.close(current)

    @contextmanager
    def _open_result(self, path: Path) -> Iterator[BinaryIO]:
        with self._result_directory(path.parent) as directory:
            try:
                descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
            except FileNotFoundError:
                raise ServiceError("not_found", 404) from None
            except OSError:
                raise ServiceError("facts_corrupt", 409) from None
            with os.fdopen(descriptor, "rb") as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise ServiceError("facts_corrupt", 409)
                yield cast(BinaryIO, stream)

    def _new_result_ref(self, scope: str) -> str:
        while True:
            result_ref = "wr_" + secrets.token_hex(16)
            if (scope, result_ref) not in self._results and not self._result_path(
                scope, result_ref
            ).exists():
                return result_ref

    async def _publish(
        self,
        scope: str,
        operation: str,
        source: str,
        title: str,
        content: str,
    ) -> _ResultRecord:
        # Disk writes run off-loop. A cancelled caller joins the worker and removes
        # any artifact before the unpublished reference can be observed.
        worker = asyncio.create_task(
            asyncio.to_thread(
                self._publish_sync, scope, operation, source, title, content
            )
        )
        try:
            return await asyncio.shield(worker)
        except asyncio.CancelledError:
            try:
                record = await worker
            except BaseException:
                pass
            else:
                self._results.pop((scope, record.result_ref), None)
                self._remove_file(record.path)
            raise

    def _publish_sync(
        self,
        scope: str,
        operation: str,
        source: str,
        title: str,
        content: str,
    ) -> _ResultRecord:
        del operation  # The reference is always read through the same public fetch action.
        if self._closed:
            raise ServiceError("web_closed", 409)
        result_ref = self._new_result_ref(scope)
        path = self._result_path(scope, result_ref)
        temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
        with self._result_directory(path.parent, create=True) as directory:
            self._unpublished.add(temporary)
            self._publish_directories[temporary] = directory
            try:
                digest, total_chars = self._write_result_file(temporary, content)
                if self._closed:
                    raise ServiceError("web_closed", 409)
                os.replace(temporary.name, path.name, src_dir_fd=directory, dst_dir_fd=directory)
                record = _ResultRecord(
                    scope, result_ref, source, title, digest, total_chars, path,
                )
                self._results[scope, result_ref] = record
                return record
            except ServiceError:
                raise
            except (OSError, UnicodeError):
                raise ServiceError("web_network", 503) from None
            finally:
                self._unpublished.discard(temporary)
                self._publish_directories.pop(temporary, None)
                try:
                    os.unlink(temporary.name, dir_fd=directory)
                except FileNotFoundError:
                    pass

    def _write_result_file(self, path: Path, content: str) -> tuple[str, int]:
        digest = hashlib.sha256()
        total_chars = 0
        secret = self._config.api_key.get_secret_value() if self._config.api_key else ""
        overlap = max(0, len(secret) - 1)
        buffer = ""
        position = 0
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
        directory = self._publish_directories.get(path)
        if directory is None:
            raise ServiceError("facts_corrupt", 409)
        fd = os.open(path.name, flags, 0o600, dir_fd=directory)
        with os.fdopen(fd, "wb") as output:
            while position < len(content) or buffer:
                if position < len(content):
                    buffer += content[position : position + _RESULT_CHUNK_BYTES]
                    position += _RESULT_CHUNK_BYTES
                final = position >= len(content)
                split = len(buffer) if final else max(0, len(buffer) - overlap)
                if secret and not final:
                    # Retaining len(secret)-1 characters alone is insufficient:
                    # a full occurrence can straddle the proposed write split.
                    # Keep the whole crossing occurrence for the next chunk.
                    crossing = buffer.find(secret, max(0, split - len(secret) + 1))
                    if crossing != -1 and crossing < split < crossing + len(secret):
                        split = crossing
                if split == 0:
                    continue
                segment, buffer = buffer[:split], buffer[split:]
                safe_segment = segment.replace(secret, "[redacted]") if secret else segment
                encoded = safe_segment.encode("utf-8")
                output.write(encoded)
                digest.update(encoded)
                total_chars += len(safe_segment)
        return digest.hexdigest(), total_chars

    async def _page(
        self,
        record: _ResultRecord,
        offset: int,
        max_chars: int,
    ) -> tuple[str, dict[str, int | None]]:
        try:
            page = await asyncio.to_thread(
                self._read_result_page, record.path, offset, max_chars,
                record.digest, record.total_chars,
            )
        except (OSError, UnicodeError):
            raise ServiceError("facts_corrupt", 409) from None
        consumed = len(page)
        next_offset = offset + consumed if offset + consumed < record.total_chars else None
        return page, {"total_chars": record.total_chars, "next_offset": next_offset}

    def _read_result_page(
        self, path: Path, offset: int, max_chars: int, digest: str, total_chars: int,
    ) -> str:
        # Hash and select the requested page from one safely-opened stream. No
        # content is returned if any byte differs from the immutable reference.
        actual = hashlib.sha256()
        decoder = codecs.getincrementaldecoder("utf-8")()
        position = 0
        parts: list[str] = []
        with self._open_result(path) as source:
            before = os.fstat(source.fileno())
            while True:
                raw = source.read(_RESULT_CHUNK_BYTES)
                text = decoder.decode(raw, final=not raw)
                start = max(0, offset - position)
                end = min(len(text), offset + max_chars - position)
                if start < end:
                    parts.append(text[start:end])
                position += len(text)
                actual.update(raw)
                if not raw:
                    break
            after = os.fstat(source.fileno())
            if (actual.hexdigest() != digest or position != total_chars
                    or (before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                    != (after.st_size, after.st_mtime_ns, after.st_ctime_ns)):
                raise ServiceError("facts_corrupt", 409)
        return "".join(parts)

    @staticmethod
    def _metadata_json(
        record: _ResultRecord,
        metadata: dict[str, int | None],
    ) -> dict[str, JsonValue]:
        return {
            "result_ref": record.result_ref,
            "total_chars": metadata["total_chars"],
            "next_offset": metadata["next_offset"],
        }

    def _lookup(self, scope: str, result_ref: str) -> _ResultRecord:
        record = self._results.get((scope, result_ref))
        if record is None:
            raise ServiceError("not_found", 404)
        if record.scope != scope or record.path != self._result_path(scope, result_ref):
            raise ServiceError("permission_denied", 403)
        return record

    def _verify_result_file(self, path: Path) -> tuple[int, str]:
        digest = hashlib.sha256()
        decoder = codecs.getincrementaldecoder("utf-8")()
        total_chars = 0
        try:
            with self._open_result(path) as source:
                while True:
                    chunk = source.read(_RESULT_CHUNK_BYTES)
                    if not chunk:
                        break
                    digest.update(chunk)
                    total_chars += len(decoder.decode(chunk))
                total_chars += len(decoder.decode(b"", final=True))
        except (OSError, UnicodeError):
            raise ServiceError("facts_corrupt", 409) from None
        return total_chars, digest.hexdigest()

    def _discard(self, record: _ResultRecord) -> None:
        self._results.pop((record.scope, record.result_ref), None)
        self._remove_file(record.path)

    def _remove_file(self, path: Path) -> None:
        try:
            pinned = self._publish_directories.get(path)
            if pinned is not None:
                os.unlink(path.name, dir_fd=pinned)
            else:
                with self._result_directory(path.parent) as directory:
                    os.unlink(path.name, dir_fd=directory)
        except (OSError, ServiceError):
            pass

    async def _post(self, operation: str, payload: dict[str, JsonValue]) -> dict[str, JsonValue]:
        self._require_configured()
        secret = self._config.api_key.get_secret_value() if self._config.api_key else ""
        headers = {"Authorization": f"Bearer {secret}", "Accept": "application/json"}
        endpoint = f"{self._config.endpoint}/{operation}"
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _OPERATION_SECONDS
        try:
            async with asyncio.timeout_at(deadline):
                async with self._semaphore:
                    for attempt in range(_RETRY_ATTEMPTS):
                        status, retry_after, body = await self._request_once(
                            endpoint, headers, payload
                        )
                        if status == 200:
                            assert body is not None
                            try:
                                return self._decode_response(body)
                            finally:
                                body.close()
                        if status == 429 and attempt == 0 and retry_after is not None:
                            remaining = deadline - loop.time()
                            if retry_after < remaining:
                                await asyncio.sleep(retry_after)
                                continue
                        raise self._http_error(status)
        except TimeoutError:
            raise ServiceError("web_timeout", 504) from None
        except httpx.TimeoutException:
            raise ServiceError("web_timeout", 504) from None
        except httpx.HTTPError:
            raise ServiceError("web_network", 503) from None
        raise ServiceError("web_http_error", 502)

    async def _request_once(
        self,
        endpoint: str,
        headers: dict[str, str],
        payload: dict[str, JsonValue],
    ) -> tuple[int, float | None, BinaryIO | None]:
        timeout = self._timeout()
        body: BinaryIO | None = None
        try:
            async with self._client.stream(
                "POST",
                endpoint,
                headers=headers,
                json=payload,
                timeout=timeout,
                follow_redirects=False,
            ) as response:
                status = response.status_code
                if status != 200:
                    return status, self._retry_after(response.headers.get("Retry-After")), None
                body = cast(BinaryIO, TemporaryFile(mode="w+b"))
                async for chunk in response.aiter_bytes():
                    body.write(chunk)
                body.seek(0)
                result = body
                body = None
                return status, None, result
        except BaseException:
            if body is not None:
                body.close()
            raise

    def _decode_response(self, body: BinaryIO) -> dict[str, JsonValue]:
        try:
            body.seek(0)
            decoded = json.load(
                body,
                object_pairs_hook=self._unique_object,
                parse_constant=self._reject_constant,
            )
        except (UnicodeError, ValueError, RecursionError, json.JSONDecodeError):
            raise ServiceError("web_protocol", 502) from None
        if not isinstance(decoded, dict) or decoded.get("error") or decoded.get("success") is False:
            raise ServiceError("web_protocol", 502)
        return cast(dict[str, JsonValue], decoded)

    @staticmethod
    def _unique_object(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    @staticmethod
    def _reject_constant(_value: str) -> None:
        raise ValueError("non-finite JSON number")

    @staticmethod
    def _timeout() -> httpx.Timeout:
        return httpx.Timeout(
            connect=_CONNECT_TIMEOUT,
            read=_READ_TIMEOUT,
            write=_WRITE_TIMEOUT,
            pool=_POOL_TIMEOUT,
        )

    @staticmethod
    def _retry_after(value: str | None) -> float | None:
        if value is None:
            return None
        try:
            seconds = float(value)
        except ValueError:
            try:
                date = parsedate_to_datetime(value)
                if date.tzinfo is None:
                    date = date.replace(tzinfo=UTC)
                seconds = (date - datetime.now(UTC)).total_seconds()
            except (TypeError, ValueError, OverflowError):
                return None
        if not math.isfinite(seconds):
            return None
        return max(0.0, seconds)

    @staticmethod
    def _http_error(status: int) -> ServiceError:
        codes = {
            401: "web_unauthorized",
            402: "web_payment_required",
            429: "web_rate_limited",
            432: "web_usage_limit",
            433: "web_credits_exhausted",
        }
        if 300 <= status < 400:
            code = "web_redirect"
        else:
            code = codes.get(status, "web_http_error")
        return ServiceError(code, status)

    def _redact(self, value: str) -> str:
        secret = self._config.api_key.get_secret_value() if self._config.api_key else ""
        return value.replace(secret, "[redacted]") if secret else value

    @staticmethod
    def _clip_metadata(value: str) -> tuple[str, bool]:
        if len(value) <= 4096:
            return value, False
        return value[:4085] + "[truncated]", True

    def _fit_output(
        self,
        operation: str,
        results: list[dict[str, JsonValue]],
        response_data: dict[str, JsonValue],
        truncated: bool,
    ) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {
            "ok": True,
            "operation": operation,
            "results": cast(JsonValue, results),
            "usage": response_data.get("usage"),
            "request_id": response_data.get("request_id"),
            "truncated": truncated,
        }
        self._redact_json(result)
        try:
            json.dumps(result, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        except (TypeError, ValueError, RecursionError):
            raise ServiceError("web_protocol", 502) from None
        return result

    def _redact_json(self, value: dict[str, JsonValue] | list[JsonValue]) -> None:
        secret = self._config.api_key.get_secret_value() if self._config.api_key else ""
        if not secret:
            return
        pending: list[dict[str, JsonValue] | list[JsonValue]] = [value]
        while pending:
            current = pending.pop()
            if isinstance(current, dict):
                for key, child in current.items():
                    if isinstance(child, str):
                        current[key] = child.replace(secret, "[redacted]")
                    elif isinstance(child, (dict, list)):
                        pending.append(child)
            else:
                for index, child in enumerate(current):
                    if isinstance(child, str):
                        current[index] = child.replace(secret, "[redacted]")
                    elif isinstance(child, (dict, list)):
                        pending.append(child)

    @classmethod
    def _public_url(cls, value: str) -> str:
        if (
            not isinstance(value, str)
            or not value
            or len(value) > 4096
            or any(char.isspace() or ord(char) < 33 for char in value)
            or "\\" in value
            or _PERCENT_ESCAPE.search(value)
        ):
            raise ServiceError("web_invalid_input", 422)
        try:
            parts = urlsplit(value)
            port = parts.port
        except ValueError:
            raise ServiceError("web_invalid_input", 422) from None
        if (
            parts.scheme.lower() not in {"http", "https"}
            or not parts.hostname
            or parts.username is not None
            or parts.password is not None
            or "@" in parts.netloc
            or (port is not None and not 1 <= port <= 65535)
        ):
            raise ServiceError("web_invalid_input", 422)
        host = parts.hostname
        if "%" in host:
            raise ServiceError("web_invalid_input", 422)
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            try:
                ascii_host = host.encode("idna").decode("ascii").lower().rstrip(".")
            except UnicodeError:
                raise ServiceError("web_invalid_input", 422) from None
            labels = ascii_host.split(".")
            if (
                len(ascii_host) > 253
                or len(labels) < 2
                or any(not _HOST_LABEL.fullmatch(label) for label in labels)
                or ascii_host == "localhost"
                or ascii_host.endswith((".localhost", ".local", ".internal", ".test"))
                or all(re.fullmatch(r"(?:[0-9]+|0x[0-9a-f]+)", label) for label in labels)
            ):
                raise ServiceError("web_invalid_input", 422) from None
            normalized_host = ascii_host
            authority_host = normalized_host
        else:
            if not address.is_global:
                raise ServiceError("web_invalid_input", 422)
            normalized_host = address.compressed.lower()
            authority_host = f"[{normalized_host}]" if address.version == 6 else normalized_host
        normalized_port = (
            None
            if (parts.scheme.lower() == "http" and port == 80)
            or (parts.scheme.lower() == "https" and port == 443)
            else port
        )
        authority = authority_host + (f":{normalized_port}" if normalized_port is not None else "")
        return urlunsplit(
            (
                parts.scheme.lower(),
                authority,
                parts.path or "/",
                parts.query,
                "",
            )
        )

    @classmethod
    def _same_public_url(cls, left: str, right: str) -> bool:
        try:
            return cls._public_url(left) == cls._public_url(right)
        except ServiceError:
            return False
