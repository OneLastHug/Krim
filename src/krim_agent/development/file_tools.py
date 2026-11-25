"""Model-facing file inputs and a fixed sandbox worker command."""

from __future__ import annotations

import base64
import codecs
import hashlib
import json
import shlex
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from krim_agent.contracts import ServiceError, StrictModel
from krim_agent.sandbox.filesystem import _parts

WORKER = Path(__file__).with_name("file_worker.py")


class FileInput(StrictModel):
    path: str = Field(min_length=1, max_length=512)

    @field_validator("path")
    @classmethod
    def project_path(cls, value: str) -> str:
        try:
            _parts(value)
        except ServiceError as error:
            if error.code != "invalid_input":
                raise
            # Pydantic reports a field error to the existing no-dispatch input
            # recovery path. Protected project paths remain permission errors.
            raise ValueError("Use a valid project-relative path.") from None
        return value


class WriteInput(FileInput):
    content: str
    expected_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


class EditInput(FileInput):
    old_string: str = Field(min_length=1)
    new_string: str
    expected_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    replace_all: bool = False

    @model_validator(mode="after")
    def changed_edit(self) -> EditInput:
        if self.old_string == self.new_string:
            raise ValueError("replacement must differ")
        return self


class FindInput(StrictModel):
    include_glob: str = Field(min_length=1, max_length=256)
    path: str = "."
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=100, ge=1, le=100)


class SearchInput(StrictModel):
    query: str = Field(min_length=1, max_length=256)
    path: str = "."
    include_glob: str | None = None
    mode: Literal["literal", "regex"] = "literal"
    case_sensitive: bool = True
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=20, ge=1, le=50)
    before: int = Field(default=0, ge=0, le=3)
    after: int = Field(default=0, ge=0, le=3)


def worker_source() -> str:
    return WORKER.read_text(encoding="utf-8")


def implementation_digest(source: str) -> str:
    return hashlib.sha256(
        (source + Path(__file__).read_text(encoding="utf-8") + worker_source()).encode()
    ).hexdigest()


def mutation_chunks(request: WriteInput | EditInput, chunk_size: int = 8192) -> Iterator[str]:
    """ASCII JSON chunks for authenticated stdin; chunk_size is a transport window.

    Reiteration produces the identical bytes used by mutation_command's digest.
    Bodies never enter argv, shell syntax or environment variables.
    """
    if not 1 <= chunk_size <= 8192:
        raise ValueError("invalid stdin chunk size")
    payload = {
        "operation": "edit" if isinstance(request, EditInput) else "write",
        **request.model_dump(mode="json"),
    }
    encoder = json.JSONEncoder(ensure_ascii=True, sort_keys=True, separators=(",", ":"),
                               allow_nan=False)
    for part in encoder.iterencode(payload):
        for start in range(0, len(part), chunk_size):
            yield part[start:start + chunk_size]


def mutation_command(request: WriteInput | EditInput, source: str) -> str:
    digest = hashlib.sha256()
    for chunk in mutation_chunks(request):
        digest.update(chunk.encode("ascii"))
    # Only the program and an immutable payload digest are passed through argv.
    command = "exec /usr/bin/python3 -I -c " + shlex.quote(source) + " " + digest.hexdigest()
    if len(command) > 32768:
        raise ServiceError("invalid_input", 422)
    return command


def text_page(value: dict[str, Any]) -> dict[str, Any]:
    raw = base64.b64decode(value["data"], validate=True)
    decoder = codecs.getincrementaldecoder("utf-8")("strict")
    try:
        text = decoder.decode(raw, final=value["eof"])
    except UnicodeDecodeError:
        raise ServiceError("invalid_input", 422) from None
    pending = decoder.getstate()[0]
    consumed = len(raw) - len(pending)
    if raw and not consumed:
        raise ServiceError("invalid_input", 422)
    return {key: item for key, item in value.items() if key != "data"} | {
        "text": text,
        "next_offset": value["offset"] + consumed,
        "encoding": "utf-8",
    }
