"""Model-facing file inputs and a fixed sandbox worker command."""

from __future__ import annotations

import base64
import codecs
import hashlib
import shlex
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from krim_agent.contracts import ServiceError, StrictModel, encoded
from krim_agent.sandbox.filesystem import _parts

WORKER = Path(__file__).with_name("file_worker.py")


class FileInput(StrictModel):
    path: str = Field(min_length=1, max_length=512)

    @field_validator("path")
    @classmethod
    def project_path(cls, value: str) -> str:
        _parts(value)
        return value


class WriteInput(FileInput):
    content: str = Field(max_length=16384)
    expected_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @field_validator("content")
    @classmethod
    def bounded_content(cls, value: str) -> str:
        if len(value.encode("utf-8")) > 16384:
            raise ValueError("content exceeds 16384 UTF-8 bytes")
        return value


class EditInput(FileInput):
    old_string: str = Field(min_length=1, max_length=8192)
    new_string: str = Field(max_length=16384)
    expected_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    replace_all: bool = False

    @model_validator(mode="after")
    def bounded_edit(self) -> EditInput:
        if self.old_string == self.new_string:
            raise ValueError("replacement must differ")
        if len(self.old_string.encode()) + len(self.new_string.encode()) > 16384:
            raise ValueError("edit text exceeds 16384 UTF-8 bytes")
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


def mutation_command(request: WriteInput | EditInput, source: str) -> str:
    payload = {
        "operation": "edit" if isinstance(request, EditInput) else "write",
        **request.model_dump(mode="json"),
    }
    # Encode text once before JSON, so quote/newline density cannot expand its budget.
    for key in ("content", "old_string", "new_string"):
        if key in payload:
            payload[key] = base64.b64encode(str(payload[key]).encode("utf-8")).decode("ascii")
    argument = encoded(payload)
    command = "exec /usr/bin/python3 -I -c " + shlex.quote(source) + " " + shlex.quote(argument)
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
