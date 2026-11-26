"""Fixed standard-library file mutations executed inside the project sandbox.

Only the program and payload digest enter argv. JSON arrives on stdin;
directory descriptors and O_NOFOLLOW keep operations under /workspace.
"""

from __future__ import annotations

import codecs
import difflib
import fcntl
import hashlib
import json
import os
import re
import stat
import sys
import tempfile
import unicodedata
import uuid
from collections import Counter
from collections.abc import Iterator
from typing import Any, BinaryIO, Protocol, cast

CHUNK = 65536
STAGING = ".krim-file-staging"
STAT_TEXT_LIMIT = 1 << 20
STAT_WORK_LIMIT = 5_000_000


def _lines(text: str) -> list[str]:
    """\n-separated lines; a trailing newline closes the last line, not a new one."""
    if not text:
        return []
    pieces = text.split("\n")
    lines = [piece + "\n" for piece in pieces[:-1]]
    if pieces[-1]:
        lines.append(pieces[-1])
    return lines


def line_delta(before: str, after: str) -> tuple[int, int] | None:
    """Real (+added, -removed) counts of the two texts; None beyond the budget.

    The guard bounds SequenceMatcher's worst-case repeated-line matching so
    pathological inputs stay bounded instead of degrading the mutation lock.
    """
    if before == after:
        return 0, 0
    before_lines, after_lines = _lines(before), _lines(after)
    occurrences = Counter(after_lines)
    if sum(occurrences[line] for line in before_lines) > STAT_WORK_LIMIT:
        return None
    matcher = difflib.SequenceMatcher(None, before_lines, after_lines, autojunk=False)
    added = removed = 0
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag != "equal":
            removed += i2 - i1
            added += j2 - j1
    return added, removed


class FileRejected(Exception):
    pass


class Digest(Protocol):
    def update(self, data: bytes, /) -> None: ...


def parts(path: object) -> list[str]:
    if not isinstance(path, str) or len(path.encode()) > 512 or "\\" in path:
        raise FileRejected("invalid_input")
    names = path.split("/")
    if (
        len(names) > 16
        or unicodedata.normalize("NFC", path) != path
        or any(not name or name in {".", ".."} for name in names)
        or any(unicodedata.category(c).startswith("C") for c in path)
    ):
        raise FileRejected("invalid_input")
    if any(
        name.lower() in {".git", ".ssh", ".aws", "credentials", "id_rsa", "id_ed25519"}
        or name.lower() == STAGING
        or name.lower() == ".env"
        or name.lower().startswith(".env.")
        for name in names
    ):
        raise FileRejected("permission_denied")
    return names


def identity(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def transformed(
    source: BinaryIO, request: dict[str, Any], original_hash: Digest,
) -> Iterator[tuple[bytes, int]]:
    """Stream exact UTF-8 edits, retaining only a possible cross-chunk match."""
    old = request["old_string"].encode("utf-8")
    new = request["new_string"].encode("utf-8")
    if not old or old == new:
        raise FileRejected("invalid_input")
    decoder = codecs.getincrementaldecoder("utf-8")("strict")
    pending = b""
    count = 0
    while True:
        chunk = source.read(CHUNK)
        original_hash.update(chunk)
        decoder.decode(chunk, final=not chunk)
        pending += chunk
        start = 0
        while True:
            position = pending.find(old, start)
            if position < 0:
                break
            yield pending[start:position], 0
            yield new, 1
            count += 1
            start = position + len(old)
        # Retain at most len(old)-1 unmatched bytes for the next input chunk.
        end = max(start, len(pending) - len(old) + 1) if chunk else len(pending)
        yield pending[start:end], 0
        pending = pending[end:]
        if not chunk:
            break
    if not count:
        raise FileRejected("text_not_found")
    if count != 1 and not request["replace_all"]:
        raise FileRejected("text_not_unique")


def mutate(root: str, request: dict[str, Any]) -> dict[str, str | int | bool | None]:
    names = parts(request["path"])
    if request["operation"] not in {"write", "edit"}:
        raise FileRejected("invalid_input")
    opened = []
    temporary = None
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK
    try:
        parent = os.open(root, flags)
        opened.append(parent)
        fcntl.flock(parent, fcntl.LOCK_EX)
        try:
            os.mkdir(STAGING, 0o700, dir_fd=parent)
        except FileExistsError:
            pass
        staging = os.open(STAGING, flags, dir_fd=parent)
        opened.append(staging)
        stage_info = os.fstat(staging)
        if stage_info.st_uid != os.geteuid() or stat.S_IMODE(stage_info.st_mode) != 0o700:
            raise FileRejected("artifact_invalid")
        with os.scandir(staging) as leftovers:
            for item in leftovers:
                name = item.name
                if not re.fullmatch(r"\.krim-write-[0-9a-f]{32}", name):
                    raise FileRejected("artifact_invalid")
                info = item.stat(follow_symlinks=False)
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
                    raise FileRejected("artifact_invalid")
                os.unlink(name, dir_fd=staging)
        for name in names[:-1]:
            parent = os.open(name, flags, dir_fd=parent)
            opened.append(parent)
        original = None
        original_fd = None
        try:
            original_fd = os.open(
                names[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent
            )
        except FileNotFoundError:
            if request.get("expected_sha256") is not None or request["operation"] == "edit":
                raise FileRejected("source_changed") from None
        else:
            opened.append(original_fd)
            original = os.fstat(original_fd)
            if not stat.S_ISREG(original.st_mode) or original.st_nlink != 1:
                raise FileRejected("artifact_invalid")
            if request.get("expected_sha256") is None:
                raise FileRejected("source_changed")
            prior_hash = hashlib.sha256()
            with os.fdopen(os.dup(original_fd), "rb") as source:
                while chunk := source.read(CHUNK):
                    prior_hash.update(chunk)
            if (identity(original) != identity(os.fstat(original_fd))
                    or prior_hash.hexdigest() != request["expected_sha256"]):
                raise FileRejected("source_changed")
            os.lseek(original_fd, 0, os.SEEK_SET)
        # Line statistics compare the exact bytes this mutation swaps, read
        # under the same lock; None means the comparison itself is unbounded.
        before_text = "" if original is None else None
        if original is not None and original.st_size <= STAT_TEXT_LIMIT:
            try:
                with os.fdopen(os.dup(original_fd), "rb") as reader:
                    before_text = reader.read(STAT_TEXT_LIMIT).decode("utf-8")
            except UnicodeDecodeError:
                before_text = None
            finally:
                os.lseek(original_fd, 0, os.SEEK_SET)
        temporary = ".krim-write-" + uuid.uuid4().hex
        fd = os.open(
            temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=staging
        )
        original_hash = hashlib.sha256()
        updated_hash = hashlib.sha256()
        size = count = 0
        compare: list[bytes] | None = [] if before_text is not None else None
        try:
            if original is not None:
                os.fchmod(fd, stat.S_IMODE(original.st_mode) & 0o777)
            with os.fdopen(os.dup(fd), "wb") as stream:
                if request["operation"] == "edit":
                    if original_fd is None:
                        raise FileRejected("source_changed")
                    with os.fdopen(os.dup(original_fd), "rb") as source:
                        for data, replacements in transformed(source, request, original_hash):
                            stream.write(data)
                            updated_hash.update(data)
                            size += len(data)
                            count += replacements
                            if compare is not None:
                                compare.append(data)
                                if size > STAT_TEXT_LIMIT:
                                    compare = None  # expansion beyond budget: stop retaining
                else:
                    if original_fd is not None:
                        with os.fdopen(os.dup(original_fd), "rb") as source:
                            while chunk := source.read(CHUNK):
                                original_hash.update(chunk)
                    text = request["content"]
                    for start in range(0, len(text), CHUNK):
                        data = text[start:start + CHUNK].encode("utf-8")
                        stream.write(data)
                        updated_hash.update(data)
                        size += len(data)
                stream.flush()
            if original is not None:
                if original_fd is None or (
                    identity(original) != identity(os.fstat(original_fd))
                    or original_hash.hexdigest() != request["expected_sha256"]
                ):
                    raise FileRejected("source_changed")
            os.fsync(fd)
        finally:
            os.close(fd)
        try:
            current = os.stat(names[-1], dir_fd=parent, follow_symlinks=False)
        except FileNotFoundError:
            current = None
        if (original is None) != (current is None) or (
            original is not None and current is not None and identity(original) != identity(current)
        ):
            raise FileRejected("source_changed")
        if original is None:
            os.link(
                temporary, names[-1], src_dir_fd=staging, dst_dir_fd=parent, follow_symlinks=False
            )
            os.unlink(temporary, dir_fd=staging)
        else:
            os.replace(temporary, names[-1], src_dir_fd=staging, dst_dir_fd=parent)
        temporary = None
        os.fsync(parent)
        # Statistics never affect the committed mutation outcome: any failure
        # to compare leaves the fields unset instead of estimating.
        added = removed = None
        if before_text is not None and size <= STAT_TEXT_LIMIT:
            try:
                after_text = (
                    request["content"] if request["operation"] == "write"
                    else b"".join(compare).decode("utf-8")
                )
                delta = line_delta(before_text, after_text)
            except (UnicodeDecodeError, MemoryError, RecursionError):
                delta = None  # bounded-comparison failure modes; other bugs must surface
            if delta is not None:
                added, removed = delta
        return {
            "path": request["path"],
            "size": size,
            "sha256": updated_hash.hexdigest(),
            "replacements": count,
            "created": original is None,
            "lines_added": added,
            "lines_removed": removed,
        }
    finally:
        if temporary is not None:
            os.unlink(temporary, dir_fd=staging)
        for fd in reversed(opened):
            os.close(fd)


def read_request(stream: BinaryIO, digest: str) -> dict[str, Any]:
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise FileRejected("invalid_input")
    actual = hashlib.sha256()
    with tempfile.TemporaryFile() as staged:
        while chunk := stream.read(CHUNK):
            staged.write(chunk)
            actual.update(chunk)
        if actual.hexdigest() != digest:
            raise FileRejected("input_digest_mismatch")
        staged.seek(0)
        return cast(dict[str, Any], json.load(staged))


def main() -> None:
    try:
        request = read_request(sys.stdin.buffer, sys.argv[1])
        result = mutate("/workspace", request)
        print(json.dumps({"ok": True, **result}))
    except FileRejected as error:
        print(json.dumps({"ok": False, "code": str(error)}))
        raise SystemExit(1) from None
    except UnicodeError:
        print(json.dumps({"ok": False, "code": "not_text"}))
        raise SystemExit(1) from None
    except (ValueError, KeyError, TypeError, IndexError):
        print(json.dumps({"ok": False, "code": "invalid_input"}))
        raise SystemExit(1) from None
    except OSError:
        print(json.dumps({"ok": False, "code": "file_operation_failed"}))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
