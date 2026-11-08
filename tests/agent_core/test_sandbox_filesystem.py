"""真实Linux目录适配：路径/链接拒绝、读写上限与原子替换。"""

import base64
import hashlib
import os
import uuid

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.sandbox.filesystem import ProjectFiles


@pytest.fixture
def files(tmp_path):
    root = tmp_path / "accounts"
    root.mkdir(mode=0o700)
    adapter = ProjectFiles(root)
    env, project = str(uuid.uuid4()), str(uuid.uuid4())
    adapter.create(env, project)
    content = root / env / "projects" / project / "content"
    try:
        yield adapter, env, project, content
    finally:
        adapter.close()


def test_create_read_write_and_list(files):
    fs, env, project, content = files
    fs.create(env, project)
    first = fs.write(env, project, "中文.txt", "你好世界".encode(), None)
    chunk = fs.read(env, project, "中文.txt", 0, 3)
    assert base64.b64decode(chunk["data"]) == "你".encode()
    assert chunk["eof"] is False
    assert chunk["sha256"] == first["sha256"]
    assert fs.read(env, project, "中文.txt", 12)["eof"] is True
    updated = fs.write(env, project, "中文.txt", b"new", first["sha256"])
    assert updated["sha256"] == hashlib.sha256(b"new").hexdigest()
    assert (content / "中文.txt").read_bytes() == b"new"
    assert fs.list(env, project)["entries"][0]["name"] == "中文.txt"
    with pytest.raises(ServiceError, match="artifact_conflict"):
        fs.write(env, project, "中文.txt", b"bad", None)
    with pytest.raises(ServiceError, match="artifact_conflict"):
        fs.write(env, project, "missing.txt", b"bad", "0" * 64)
    assert (content / "中文.txt").read_bytes() == b"new"


@pytest.mark.parametrize(
    "name",
    [
        "../escape",
        "/etc/passwd",
        "x/../y",
        "x//y",
        "a\b",
        "a\x00b",
        ".env",
        "sub/.env.local",
        ".git/config",
        ".ssh/key",
        "credentials",
        "id_rsa",
        "a/" * 17 + "b",
    ],
)
def test_path_and_secret_rejection(files, name):
    fs, env, project, _ = files
    with pytest.raises(ServiceError):
        fs.read(env, project, name)
    with pytest.raises(ServiceError):
        fs.write(env, project, name, b"denied", None)


@pytest.mark.parametrize("kind", ["symlink", "hardlink", "fifo", "directory"])
def test_non_regular_file_is_not_followed(files, tmp_path, kind):
    fs, env, project, content = files
    secret = tmp_path / "outside.txt"
    secret.write_bytes(b"outside")
    target = content / "bad"
    if kind == "symlink":
        target.symlink_to(secret)
    elif kind == "hardlink":
        os.link(secret, target)
    elif kind == "fifo":
        os.mkfifo(target)
    else:
        target.mkdir()
    with pytest.raises(ServiceError, match="artifact_invalid"):
        fs.read(env, project, "bad")
    with pytest.raises(ServiceError):
        fs.write(env, project, "bad", b"changed", None)
    assert secret.read_bytes() == b"outside"


def test_directory_symlink_and_root_replacement_rejected(files, tmp_path):
    fs, env, project, content = files
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret").write_bytes(b"outside")
    (content / "link").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ServiceError, match="artifact_invalid"):
        fs.read(env, project, "link/secret")
    fs.root.rename(tmp_path / "moved-accounts")
    fs.root.mkdir()
    with pytest.raises(ServiceError, match="artifact_invalid"):
        fs.list(env, project)


def test_limits_and_hidden_names(files):
    fs, env, project, content = files
    (content / ".env").write_bytes(b"private")
    (content / ".git").mkdir()
    assert fs.list(env, project)["entries"] == []
    (content / "large").write_bytes(b"x" * (4 * 1024**2 + 1))
    with pytest.raises(ServiceError, match="artifact_limit"):
        fs.read(env, project, "large")
    with pytest.raises(ServiceError, match="invalid_input"):
        fs.write(env, project, "new", b"x" * (1024**2 + 1), None)
    for value in [True, -1, 65537]:
        with pytest.raises(ServiceError):
            fs.read(env, project, "large", limit=value)
    (content / "second").touch()
    page = fs.list(env, project, limit=1)
    assert page["entries"] == [{"name": "large", "type": "file", "size": 4 * 1024**2 + 1}]
    assert page["offset"] == 0
    assert page["next_offset"] == 1
    assert page["total"] == 2
    tail = fs.list(env, project, limit=1, offset=1)
    assert tail["entries"][0]["name"] == "second"
    assert tail["next_offset"] is None
    with pytest.raises(ServiceError, match="invalid_input"):
        fs.create("../bad", project)
    fs.close()
    with pytest.raises(ServiceError, match="closed"):
        fs.list(env, project)


def test_list_pagination_stability(files):
    fs, env, project, content = files
    for name in ["b.txt", "a.txt", "c.txt", "e.txt", "d.txt"]:
        (content / name).write_bytes(b"data")
    (content / ".env").write_bytes(b"secret")
    (content / ".git").mkdir()
    (content / "link").symlink_to(content / "a.txt")
    full = fs.list(env, project)
    names = [entry["name"] for entry in full["entries"]]
    assert names == ["a.txt", "b.txt", "c.txt", "d.txt", "e.txt", "link"]
    assert full["total"] == 6
    assert full["next_offset"] is None
    page = fs.list(env, project, limit=2, offset=0)
    assert [entry["name"] for entry in page["entries"]] == ["a.txt", "b.txt"]
    assert page["next_offset"] == 2
    middle = fs.list(env, project, limit=2, offset=2)
    assert [entry["name"] for entry in middle["entries"]] == ["c.txt", "d.txt"]
    assert middle["next_offset"] == 4
    tail = fs.list(env, project, limit=2, offset=4)
    assert [entry["name"] for entry in tail["entries"]] == ["e.txt", "link"]
    assert tail["next_offset"] is None
    assert fs.list(env, project, limit=2, offset=99)["entries"] == []
    for kwargs in [{"limit": 0}, {"limit": 101}, {"offset": -1}, {"offset": True}, {"limit": True}]:
        with pytest.raises(ServiceError, match="invalid_input"):
            fs.list(env, project, **kwargs)


def test_search_matches_boundaries_and_flags(files):
    fs, env, project, content = files
    (content / "src").mkdir()
    (content / "src" / "code.py").write_bytes(b"hello world\nHELLO again\nbye\n")
    (content / "notes.txt").write_bytes(b"hello there\n" + b"x" * 300 + b" hello tail\n")
    result = fs.search(env, project, "hello", limit=10)
    assert [(m["path"], m["line"]) for m in result["matches"]] == [
        ("notes.txt", 1),
        ("notes.txt", 2),
        ("src/code.py", 1),
    ]
    assert result["matches"][0]["preview"] == "hello there"
    assert result["matches"][1]["preview"] == "x" * 256
    assert result["truncated"] is False
    assert result["skipped"] == 0
    assert result["byte_cap_reached"] is False
    limited = fs.search(env, project, "hello", limit=1)
    assert len(limited["matches"]) == 1
    assert limited["truncated"] is True
    scoped = fs.search(env, project, "hello", path="src")
    assert scoped["matches"] == [{"path": "src/code.py", "line": 1, "preview": "hello world"}]
    for bad in ["", "x" * 257, "🦀" * 65, 5, None, True]:
        with pytest.raises(ServiceError, match="invalid_input"):
            fs.search(env, project, bad)
    for bad_limit in [0, 101, "5", True]:
        with pytest.raises(ServiceError, match="invalid_input"):
            fs.search(env, project, "hello", limit=bad_limit)
    with pytest.raises(ServiceError, match="not_found"):
        fs.search(env, project, "hello", path="missing")
    with pytest.raises(ServiceError, match="permission_denied"):
        fs.search(env, project, "hello", path=".git")


def test_search_skips_secrets_links_and_binary(files, tmp_path):
    fs, env, project, content = files
    outside = tmp_path / "outside.txt"
    outside.write_bytes(b"needle in the dark\n")
    outside_dir = tmp_path / "outsidedir"
    outside_dir.mkdir()
    (outside_dir / "inside.txt").write_bytes(b"needle hidden\n")
    (content / ".env").write_bytes(b"needle env\n")
    (content / ".git").mkdir()
    (content / ".git" / "config").write_bytes(b"needle git\n")
    (content / "sub").mkdir()
    (content / "sub" / ".env.local").write_bytes(b"needle local\n")
    (content / "link.txt").symlink_to(outside)
    (content / "ldir").symlink_to(outside_dir, target_is_directory=True)
    os.link(outside, content / "hard.txt")
    (content / "bin.dat").write_bytes(b"needle \xff\xfe broken")
    (content / "ok.txt").write_bytes(b"the needle is here\n")
    result = fs.search(env, project, "needle")
    assert [m["path"] for m in result["matches"]] == ["ok.txt"]
    assert outside.read_bytes() == b"needle in the dark\n"
    assert (outside_dir / "inside.txt").read_bytes() == b"needle hidden\n"


def test_search_skips_oversize_and_respects_byte_cap(files):
    fs, env, project, content = files
    (content / "big.txt").write_bytes(b"needle\n" + b"x" * (4 * 1024**2))
    (content / "small.txt").write_bytes(b"needle here\n")
    result = fs.search(env, project, "needle")
    assert [m["path"] for m in result["matches"]] == ["small.txt"]
    assert result["skipped"] == 1
    assert result["byte_cap_reached"] is False
    for i in range(9):
        pad = bytes([97 + i]) * (4 * 1024**2 - 7)
        (content / f"cap_{i:02d}.txt").write_bytes(b"needle\n" + pad)
    capped = fs.search(env, project, "needle", limit=100)
    expected = {f"cap_{i:02d}.txt" for i in range(8)}
    assert {m["path"] for m in capped["matches"]} == expected
    assert capped["skipped"] == 3
    assert capped["byte_cap_reached"] is True
    assert capped["scanned_bytes"] == 32 * 1024**2


def test_search_and_list_entry_hard_cap(files):
    fs, env, project, content = files
    for i in range(4097):
        (content / f"f{i:05d}.txt").write_bytes(b"needle\n")
    with pytest.raises(ServiceError, match="artifact_limit"):
        fs.search(env, project, "absent")
    with pytest.raises(ServiceError, match="artifact_limit"):
        fs.list(env, project)


def test_mkdir_creates_recursively_and_is_idempotent(files):
    fs, env, project, content = files
    assert fs.mkdir(env, project, "a/b/c") == {"path": "a/b/c", "created": True}
    assert (content / "a" / "b" / "c").is_dir()
    assert ((content / "a").stat().st_mode & 0o777) == 0o700
    assert fs.mkdir(env, project, "a/b/c") == {"path": "a/b/c", "created": False}
    assert fs.mkdir(env, project, "a/b/d/e")["created"] is True
    fs.write(env, project, "a/b/c/file.txt", b"data", None)
    assert fs.read(env, project, "a/b/c/file.txt")["size"] == 4


def test_mkdir_rejects_escape_and_occupied_names(files, tmp_path):
    fs, env, project, content = files
    bad_paths = ["../escape", "a/../b", "a/b/../../c", ".", "", ".git/cache", "x/.env"]
    bad_paths.append("a/" * 17 + "b")
    for bad in bad_paths:
        with pytest.raises(ServiceError):
            fs.mkdir(env, project, bad)
    (content / "occupied").write_bytes(b"file")
    with pytest.raises(ServiceError, match="artifact_invalid"):
        fs.mkdir(env, project, "occupied/sub")
    (content / "sneaky").symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ServiceError, match="artifact_invalid"):
        fs.mkdir(env, project, "sneaky/sub")
    assert not (tmp_path / "sub").exists()


def test_remove_requires_exact_hash_and_rejects_specials(files, tmp_path):
    fs, env, project, content = files
    written = fs.write(env, project, "gone.txt", b"payload", None)
    outside = tmp_path / "outside.txt"
    outside.write_bytes(b"outside")
    (content / "dir").mkdir()
    (content / "link.txt").symlink_to(outside)
    os.link(outside, content / "hard.txt")
    os.mkfifo(content / "pipe")
    empty_hash = hashlib.sha256(b"").hexdigest()
    for name in ["dir", "link.txt", "hard.txt", "pipe"]:
        with pytest.raises(ServiceError, match="artifact_invalid"):
            fs.remove(env, project, name, empty_hash)
    with pytest.raises(ServiceError, match="artifact_conflict"):
        fs.remove(env, project, "gone.txt", "0" * 64)
    assert (content / "gone.txt").read_bytes() == b"payload"
    assert outside.read_bytes() == b"outside"
    for bad in ["", "xyz", "0" * 63, "A" * 64, None, 7]:
        with pytest.raises(ServiceError, match="invalid_input"):
            fs.remove(env, project, "gone.txt", bad)
    with pytest.raises(ServiceError, match="not_found"):
        fs.remove(env, project, "missing.txt", empty_hash)
    assert fs.remove(env, project, "gone.txt", written["sha256"]) == {
        "path": "gone.txt",
        "deleted": True,
    }
    assert not (content / "gone.txt").exists()
