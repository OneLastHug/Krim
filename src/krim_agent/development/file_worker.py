"""Fixed standard-library file mutations, executed only inside the project sandbox.

The host quotes this whole program and a JSON payload, never interpolates source.
Directory descriptors and O_NOFOLLOW keep every operation under /workspace.
"""

from __future__ import annotations

import base64
import fcntl
import hashlib
import json
import os
import re
import stat
import sys
import unicodedata
import uuid

MAX_FILE = 1024 * 1024
STAGING = ".krim-file-staging"


class FileRejected(Exception):
    pass


def parts(path):
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


def identity(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def mutate(root, request):
    names = parts(request["path"])
    opened = []
    temporary = None
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK
    try:
        parent = os.open(root, flags)
        opened.append(parent)
        # Serialize dedicated mutations across runs against the same directory inode.
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
        # Only our reserved, exact names; the project lock excludes live file workers.
        leftovers = os.listdir(staging)
        if len(leftovers) > 1024:
            raise FileRejected("artifact_limit")
        for name in leftovers:
            if not re.fullmatch(r"\.krim-write-[0-9a-f]{32}", name):
                raise FileRejected("artifact_invalid")
            info = os.stat(name, dir_fd=staging, follow_symlinks=False)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
                raise FileRejected("artifact_invalid")
            os.unlink(name, dir_fd=staging)
        for name in names[:-1]:
            parent = os.open(name, flags, dir_fd=parent)
            opened.append(parent)
        original = None
        content = None
        try:
            fd = os.open(names[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        except FileNotFoundError:
            if request.get("expected_sha256") is not None or request["operation"] == "edit":
                raise FileRejected("source_changed") from None
        else:
            try:
                original = os.fstat(fd)
                if not stat.S_ISREG(original.st_mode) or original.st_nlink != 1:
                    raise FileRejected("artifact_invalid")
                if original.st_size > MAX_FILE:
                    raise FileRejected("artifact_limit")
                with os.fdopen(os.dup(fd), "rb") as stream:
                    content = stream.read(MAX_FILE + 1)
                if identity(original) != identity(os.fstat(fd)) or len(content) > MAX_FILE:
                    raise FileRejected("source_changed")
                if hashlib.sha256(content).hexdigest() != request.get("expected_sha256"):
                    raise FileRejected("source_changed")
            finally:
                os.close(fd)
        if request["operation"] == "edit":
            text = content.decode("utf-8")
            count = text.count(request["old_string"])
            if count == 0:
                raise FileRejected("text_not_found")
            if count != 1 and not request["replace_all"]:
                raise FileRejected("text_not_unique")
            updated = text.replace(
                request["old_string"], request["new_string"], -1 if request["replace_all"] else 1
            ).encode("utf-8")
        else:
            count = 0
            updated = request["content"].encode("utf-8")
        if len(updated) > MAX_FILE:
            raise FileRejected("artifact_limit")
        temporary = ".krim-write-" + uuid.uuid4().hex
        fd = os.open(
            temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=staging
        )
        try:
            # Preserve executable mode on existing source files, never set special bits.
            if original is not None:
                os.fchmod(fd, stat.S_IMODE(original.st_mode) & 0o777)
            with os.fdopen(os.dup(fd), "wb") as stream:
                stream.write(updated)
                stream.flush()
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
            # No-clobber publication, even when a concurrent process creates the target.
            os.link(
                temporary, names[-1], src_dir_fd=staging, dst_dir_fd=parent, follow_symlinks=False
            )
            os.unlink(temporary, dir_fd=staging)
        else:
            os.replace(temporary, names[-1], src_dir_fd=staging, dst_dir_fd=parent)
        temporary = None
        os.fsync(parent)
        return {
            "path": request["path"],
            "size": len(updated),
            "sha256": hashlib.sha256(updated).hexdigest(),
            "replacements": count,
            "created": original is None,
        }
    finally:
        if temporary is not None:
            os.unlink(temporary, dir_fd=staging)
        for fd in reversed(opened):
            os.close(fd)


def main():
    try:
        request = json.loads(sys.argv[1])
        for key in ("content", "old_string", "new_string"):
            if key in request:
                request[key] = base64.b64decode(request[key], validate=True).decode("utf-8")
        result = mutate("/workspace", request)
        print(json.dumps({"ok": True, **result}))
    except FileRejected as error:
        print(json.dumps({"ok": False, "code": str(error)}))
        raise SystemExit(1) from None
    except UnicodeError:
        print(json.dumps({"ok": False, "code": "not_text"}))
        raise SystemExit(1) from None
    except OSError:
        print(json.dumps({"ok": False, "code": "file_operation_failed"}))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
