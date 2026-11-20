"""Private, bounded screenshot objects with immutable project/run provenance."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from pathlib import Path
from typing import Any
from uuid import uuid4

from krim_agent.browser.protocol import validate_png
from krim_agent.contracts import ServiceError
from krim_agent.development.contracts import EnvironmentHandle, RunOwnership
from krim_agent.validation.files import directory_fd

_ID = re.compile(r"image-[0-9a-f]{32}\Z")
_LIMIT = 1024 * 1024


class BrowserArtifacts:
    def __init__(self, root: Path):
        self.root = root

    def _root(self) -> int:
        fd = directory_fd(self.root, create=True)
        info = os.fstat(fd)
        if info.st_uid != os.geteuid() or info.st_mode & 0o077:
            os.close(fd)
            raise ServiceError("artifact_invalid")
        return fd

    @staticmethod
    def _read(fd: int, name: str, limit: int) -> bytes:
        handle = os.open(name, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC,
                         dir_fd=fd)
        try:
            info = os.fstat(handle)
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or info.st_uid != os.geteuid() or info.st_mode & 0o077
                    or info.st_size > limit):
                raise ServiceError("artifact_invalid")
            data = bytearray()
            while len(data) <= limit:
                chunk = os.read(handle, min(65536, limit + 1 - len(data)))
                if not chunk:
                    break
                data.extend(chunk)
            if len(data) != info.st_size:
                raise ServiceError("artifact_invalid")
            return bytes(data)
        finally:
            os.close(handle)

    @staticmethod
    def _write(fd: int, name: str, value: bytes) -> None:
        handle = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                         0o600, dir_fd=fd)
        try:
            with os.fdopen(handle, "wb", closefd=False) as output:
                output.write(value)
                output.flush()
                os.fsync(handle)
        finally:
            os.close(handle)

    def save(
        self, data: bytes, environment: EnvironmentHandle, ownership: RunOwnership,
        actor_generation: int, preview_id: str,
    ) -> dict[str, Any]:
        validate_png(data)
        if environment.owner_id != ownership.owner_id:
            raise ServiceError("permission_denied", 403)
        # The directory key is host-derived, never an account or model path.
        scope_key = hashlib.sha256(
            (ownership.owner_id + "\0" + ownership.run_id).encode(),
        ).hexdigest()
        root = self._root()
        directory = -1
        try:
            try:
                os.mkdir(scope_key, 0o700, dir_fd=root)
            except FileExistsError:
                pass
            directory = os.open(scope_key, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
                                | os.O_CLOEXEC, dir_fd=root)
            info = os.fstat(directory)
            if info.st_uid != os.geteuid() or info.st_mode & 0o077:
                raise ServiceError("artifact_invalid")
            names = os.listdir(directory)
            if len(names) >= 128:
                raise ServiceError("artifact_limit", 422)
            reference = "image-" + uuid4().hex
            digest = hashlib.sha256(data).hexdigest()
            metadata = {
                "reference": reference, "environment": environment.model_dump(mode="json"),
                "ownership": ownership.model_dump(mode="json"),
                "actor_generation": actor_generation, "preview_id": preview_id,
                "media_type": "image/png", "size": len(data), "sha256": digest,
            }
            self._write(directory, reference + ".png", data)
            try:
                self._write(directory, reference + ".json",
                            json.dumps(metadata, separators=(",", ":")).encode())
                os.fsync(directory)
            except BaseException:
                os.unlink(reference + ".png", dir_fd=directory)
                raise
            return {"reference": reference, "run_id": ownership.run_id,
                    "project_id": environment.project_id, "media_type": "image/png",
                    "size": len(data), "sha256": digest}
        finally:
            if directory != -1:
                os.close(directory)
            os.close(root)

    def read(
        self, reference: str, environment: EnvironmentHandle, ownership: RunOwnership,
        actor_generation: int,
    ) -> tuple[bytes, dict[str, Any]]:
        if not _ID.fullmatch(reference):
            raise ServiceError("not_found", 404)
        root = self._root()
        directory = -1
        scope_key = hashlib.sha256(
            (ownership.owner_id + "\0" + ownership.run_id).encode(),
        ).hexdigest()
        try:
            directory = os.open(scope_key, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
                                | os.O_CLOEXEC, dir_fd=root)
            metadata = json.loads(self._read(directory, reference + ".json", 8192))
            saved_environment = EnvironmentHandle.model_validate(metadata["environment"])
            if (metadata["reference"] != reference
                    or metadata["ownership"] != ownership.model_dump()
                    or metadata["actor_generation"] != actor_generation
                    or saved_environment.model_dump(exclude={"state"})
                    != environment.model_dump(exclude={"state"})):
                raise ServiceError("permission_denied", 403)
            data = self._read(directory, reference + ".png", _LIMIT)
            if (metadata["size"] != len(data) or metadata["media_type"] != "image/png"
                    or metadata["sha256"] != hashlib.sha256(data).hexdigest()):
                raise ServiceError("artifact_invalid")
            validate_png(data)
            return data, metadata
        except FileNotFoundError:
            raise ServiceError("not_found", 404) from None
        except (OSError, ValueError, KeyError, TypeError):
            raise ServiceError("artifact_invalid") from None
        finally:
            if directory != -1:
                os.close(directory)
            os.close(root)
