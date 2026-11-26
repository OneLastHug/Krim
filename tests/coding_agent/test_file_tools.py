"""Project file tool contracts: text, version checks, exact edits and path isolation."""

import base64
import hashlib
import os
import shlex

import pytest
from pydantic import ValidationError

from krim_agent.contracts import ServiceError
from krim_agent.development.file_tools import (
    EditInput,
    WriteInput,
    mutation_chunks,
    mutation_command,
    text_page,
    worker_source,
)
from krim_agent.development.file_worker import FileRejected, mutate


def sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def write(path="a.py", content="before\n", expected=None):
    return {"operation": "write", "path": path, "content": content, "expected_sha256": expected}


def edit(digest, **kwargs):
    return {
        "operation": "edit",
        "path": "a.py",
        "expected_sha256": digest,
        "old_string": "before",
        "new_string": "after",
        "replace_all": False,
        **kwargs,
    }


def test_create_read_edit_preserves_mode_and_rejects_stale(tmp_path):
    result = mutate(tmp_path, write())
    assert result["created"] and result["sha256"] == sha(b"before\n")
    path = tmp_path / "a.py"
    path.chmod(0o755)
    changed = mutate(tmp_path, edit(result["sha256"]))
    assert changed["replacements"] == 1 and path.read_text() == "after\n"
    assert path.stat().st_mode & 0o777 == 0o755
    with pytest.raises(FileRejected, match="source_changed"):
        mutate(tmp_path, write(content="stale", expected=result["sha256"]))
    assert path.read_text() == "after\n"
    with pytest.raises(FileRejected, match="source_changed"):
        mutate(tmp_path, write(content="no implicit overwrite"))


def test_exact_edit_requires_unique_match_unless_explicit(tmp_path):
    result = mutate(tmp_path, write(content="before before\n"))
    with pytest.raises(FileRejected, match="text_not_unique"):
        mutate(tmp_path, edit(result["sha256"]))
    with pytest.raises(FileRejected, match="text_not_found"):
        mutate(tmp_path, edit(result["sha256"], old_string="missing"))
    changed = mutate(tmp_path, edit(result["sha256"], replace_all=True))
    assert changed["replacements"] == 2
    assert (tmp_path / "a.py").read_text() == "after after\n"


@pytest.mark.parametrize(
    "path", ["../outside", "/tmp/out", "a/../../out", "a\\b", ".git/config", ".env", ".env.local"]
)
def test_forbidden_path_rejected_in_host_and_worker(tmp_path, path):
    with pytest.raises((ServiceError, ValidationError)):
        WriteInput(path=path, content="x")
    with pytest.raises(FileRejected):
        mutate(tmp_path, write(path=path))


@pytest.mark.parametrize("path", ["/tmp/probe.txt", "../outside", "a//b", "a\\\\b"])
def test_invalid_path_is_a_field_error_for_model_recovery(path):
    from krim_agent.tools.input_errors import ToolInputError

    with pytest.raises(ValidationError) as raised:
        WriteInput.model_validate({"path": path, "content": "probe"}, strict=True)
    detail = ToolInputError(raised.value, WriteInput.model_json_schema()).details
    assert detail["code"] == "invalid_input" and detail["executor_started"] is False
    assert detail["errors"][0]["field"] == ["path"]
    assert path not in str(detail)


def test_protected_project_path_stays_a_permission_error():
    with pytest.raises(ServiceError) as raised:
        WriteInput(path=".env", content="probe")
    assert raised.value.code == "permission_denied"


@pytest.mark.parametrize("kind", ["symlink", "parent_symlink", "hardlink", "fifo"])
def test_links_and_nonregular_files_never_mutated(tmp_path, kind):
    outside = tmp_path / "outside"
    outside.mkdir()
    original = outside / "file"
    original.write_text("private")
    path = tmp_path / "a.py"
    target = "a.py"
    if kind == "symlink":
        path.symlink_to(original)
    elif kind == "parent_symlink":
        path.symlink_to(outside, target_is_directory=True)
        target = "a.py/file"
    elif kind == "hardlink":
        os.link(original, path)
    else:
        os.mkfifo(path)
    with pytest.raises((OSError, FileRejected)):
        mutate(tmp_path, write(path=target, expected=sha(b"private")))
    assert original.read_text() == "private"


def test_create_race_cannot_clobber_concurrent_file(tmp_path, monkeypatch):
    real_link = os.link

    def raced_link(source, destination, **kwargs):
        (tmp_path / "a.py").write_text("concurrent")
        return real_link(source, destination, **kwargs)

    monkeypatch.setattr(os, "link", raced_link)
    with pytest.raises(FileExistsError):
        mutate(tmp_path, write())
    assert (tmp_path / "a.py").read_text() == "concurrent"
    assert not list((tmp_path / ".krim-file-staging").iterdir())


def test_interrupted_staging_is_cleaned_and_hidden(tmp_path):
    import uuid

    from krim_agent.sandbox.filesystem import ProjectFiles
    env, project = str(uuid.uuid4()), str(uuid.uuid4())
    files = ProjectFiles(tmp_path)
    try:
        files.create(env, project)
        root = tmp_path / env / "projects" / project / "content"
        stage = root / ".krim-file-staging"
        stage.mkdir(mode=0o700)
        leftover = stage / (".krim-write-" + "a" * 32)
        leftover.write_text("partial uncommitted content")
        assert files.list(env, project)["entries"] == []
        assert files.find(env, project, include_glob="**/*")["files"] == []
        with pytest.raises(ServiceError, match="permission_denied"):
            files.read(env, project, ".krim-file-staging/" + leftover.name)
        mutate(root, write())
        assert not list(stage.iterdir())
        assert (root / "a.py").read_text() == "before\n"
        direct = files.search(env, project, "before", path="a.py")
        assert direct["total_matches"] == 1
        assert direct["matches"][0]["path"] == "a.py"
        assert direct["matches"][0]["line"] == 1
        nested = root / "nested"
        nested.mkdir()
        (nested / "test.txt").write_text("first\ngamma\nlast\n")
        scoped = files.search(env, project, "gamma", path="nested/test.txt", before=1, after=1)
        assert scoped["matches"] == [{"path": "nested/test.txt", "line": 2,
                                       "preview": "gamma", "before": ["first"], "after": ["last"]}]
        (root / "linked").symlink_to(nested / "test.txt")
        with pytest.raises(ServiceError, match="artifact_invalid"):
            files.search(env, project, "gamma", path="linked")
    finally:
        files.close()


def test_invalid_utf8_and_nonunique_large_edit_preserve_original(tmp_path):
    path = tmp_path / "a.py"
    original = b"a" * (2 * 1024 * 1024) + b"\xff"
    path.write_bytes(original)
    with pytest.raises(UnicodeError):
        mutate(tmp_path, edit(sha(original), old_string="a", replace_all=True))
    assert path.read_bytes() == original
    text = "before" + "x" * (2 * 1024 * 1024) + "before"
    path.write_text(text)
    with pytest.raises(FileRejected, match="text_not_unique"):
        mutate(tmp_path, edit(sha(text.encode())))
    assert path.read_text() == text
    assert not list((tmp_path / ".krim-file-staging").iterdir())


def test_concurrent_source_change_after_read_is_rejected(tmp_path, monkeypatch):
    import krim_agent.development.file_worker as worker

    original = "before" + "x" * (2 * 1024 * 1024)
    mutate(tmp_path, write(content=original))
    real_transform = worker.transformed

    def raced_transform(source, request, original_hash):
        yield from real_transform(source, request, original_hash)
        (tmp_path / "a.py").write_text("concurrent")

    monkeypatch.setattr(worker, "transformed", raced_transform)
    with pytest.raises(FileRejected, match="source_changed"):
        mutate(tmp_path, edit(sha(original.encode())))
    assert (tmp_path / "a.py").read_text() == "concurrent"
    assert not list((tmp_path / ".krim-file-staging").iterdir())


def test_stale_hash_precedes_edit_matching(tmp_path):
    mutate(tmp_path, write(content="current content"))
    with pytest.raises(FileRejected, match="source_changed"):
        mutate(tmp_path, edit(sha(b"old version")))


def test_read_utf8_boundary_and_binary_rejection():
    raw = "a中".encode()
    value = {
        "data": base64.b64encode(raw[:3]).decode(),
        "offset": 0,
        "eof": False,
        "sha256": sha(raw),
        "size": len(raw),
    }
    page = text_page(value)
    assert page["text"] == "a" and page["next_offset"] == 1 and "data" not in page
    rest = text_page(
        {**value, "data": base64.b64encode(raw[1:]).decode(), "offset": 1, "eof": True}
    )
    assert rest["text"] == "中" and rest["next_offset"] == 4
    with pytest.raises(ServiceError, match="invalid_input"):
        text_page({**value, "data": "/w==", "eof": True})


def test_quoted_payload_round_trip_and_command_bound():
    import io

    from krim_agent.development.file_worker import read_request

    request = WriteInput(path="quote'file.py", content="'\"$()`\\\n" * 300000)
    command = mutation_command(request, worker_source())
    argv = shlex.split(command)
    assert argv[:4] == ["exec", "/usr/bin/python3", "-I", "-c"]
    chunks = list(mutation_chunks(request))
    assert all(len(chunk.encode()) <= 8192 for chunk in chunks)
    payload = "".join(chunks).encode()
    assert read_request(io.BytesIO(payload), argv[-1])["content"] == request.content
    assert len(command) < 32768
    assert request.content not in command
    with pytest.raises(FileRejected, match="input_digest_mismatch"):
        read_request(io.BytesIO(payload + b" "), argv[-1])
    small = mutation_command(WriteInput(path="a.py", content="a"), worker_source())
    assert len(small) == len(command)


def test_worker_process_consumes_large_stdin_before_mutating(tmp_path):
    import json
    import subprocess
    import sys

    # Execute the shipped worker with only the sandbox root replaced for the test.
    source = worker_source().replace('mutate("/workspace", request)',
                                     f"mutate({str(tmp_path)!r}, request)")
    request = WriteInput(path="a.py", content="large 中😀\n" * 180000)
    argv = shlex.split(mutation_command(request, source))[1:]
    argv[0] = sys.executable
    payload = "".join(mutation_chunks(request)).encode("ascii")
    completed = subprocess.run(argv, input=payload, capture_output=True, check=True, timeout=30)
    result = json.loads(completed.stdout)
    assert result["ok"] and result["sha256"] == sha(request.content.encode())
    assert (tmp_path / "a.py").read_text() == request.content
    rejected = subprocess.run(argv, input=payload + b" ", capture_output=True, timeout=30)
    assert rejected.returncode == 1
    assert json.loads(rejected.stdout)["code"] == "input_digest_mismatch"
    assert (tmp_path / "a.py").read_text() == request.content


def test_large_body_supported_and_noop_rejected():
    with pytest.raises(ValidationError):
        EditInput(path="a.py", old_string="x", new_string="x", expected_sha256="0" * 64)
    assert len(WriteInput(path="a.py", content="中" * 600000).content) == 600000


def test_large_edit_crosses_stream_boundaries(tmp_path):
    before = "中" * 500000 + "needle" + "😀" * 500000
    result = mutate(tmp_path, write(content=before))
    replacement = "replacement" * 10000
    changed = mutate(tmp_path, edit(result["sha256"], old_string="中needle😀",
                                    new_string=replacement))
    expected = before.replace("中needle😀", replacement)
    assert (tmp_path / "a.py").read_text() == expected
    assert changed["sha256"] == sha(expected.encode())
    assert changed["size"] == len(expected.encode())
    assert changed["replacements"] == 1


@pytest.mark.parametrize("old,new,text", [
    ("aba", "X", "ababa" * 100000),
    ("a" * 100000, "X", "a" * 200001),
    ("boundary", "", "a" * 65533 + "boundary" + "z" * 65536),
])
def test_streaming_replacement_matches_python(tmp_path, old, new, text):
    result = mutate(tmp_path, write(content=text))
    changed = mutate(tmp_path, edit(result["sha256"], old_string=old,
                                    new_string=new, replace_all=True))
    assert (tmp_path / "a.py").read_text() == text.replace(old, new)
    assert changed["replacements"] == text.count(old)


def test_line_stats_count_real_before_after_text(tmp_path):
    created = mutate(tmp_path, write(content="中文\ntest\n"))
    assert created["created"] and (created["lines_added"], created["lines_removed"]) == (2, 0)
    changed = mutate(tmp_path, edit(created["sha256"], old_string="中文",
                                    new_string="漢字\nsecond"))
    assert (tmp_path / "a.py").read_text() == "漢字\nsecond\ntest\n"
    assert (changed["lines_added"], changed["lines_removed"]) == (2, 1)
    expanded = mutate(tmp_path, edit(changed["sha256"], old_string="漢字\nsecond",
                                     new_string="x\ny\nz"))
    assert (expanded["lines_added"], expanded["lines_removed"]) == (3, 2)


def test_line_stats_cover_replace_all_empty_and_trailing_newline(tmp_path):
    result = mutate(tmp_path, write(content="x\nx\n"))
    changed = mutate(tmp_path, edit(result["sha256"], old_string="x",
                                    new_string="y\nz", replace_all=True))
    assert changed["replacements"] == 2
    assert (changed["lines_added"], changed["lines_removed"]) == (4, 2)
    empty = mutate(tmp_path, write(path="empty.py", content=""))
    assert (empty["lines_added"], empty["lines_removed"]) == (0, 0)
    closed = mutate(tmp_path, write(path="nl.py", content="a"))
    opened = mutate(tmp_path, write(path="nl.py", expected=closed["sha256"], content="a\n"))
    assert (opened["lines_added"], opened["lines_removed"]) == (1, 1)


def test_line_stats_follow_actual_diff_not_parameter_counts(tmp_path):
    pair = mutate(tmp_path, write(path="pair.py", content="a\nb\n"))
    merged = mutate(tmp_path, edit(pair["sha256"], path="pair.py",
                                   old_string="a\nb", new_string="b"))
    assert (tmp_path / "pair.py").read_text() == "b\n"
    assert (merged["lines_added"], merged["lines_removed"]) == (0, 1)


def test_line_stats_are_null_when_comparison_is_unbounded(tmp_path):
    oversized = "a" * ((1 << 20) + 1)
    result = mutate(tmp_path, write(content=oversized))
    assert result["lines_added"] is None and result["lines_removed"] is None
    shrunk = mutate(tmp_path, write(content="x", expected=result["sha256"]))
    assert shrunk["lines_added"] is None and shrunk["lines_removed"] is None
    reshrunk = mutate(tmp_path, write(content="y", expected=shrunk["sha256"]))
    assert (reshrunk["lines_added"], reshrunk["lines_removed"]) == (1, 1)
    big = mutate(tmp_path, write(content=oversized, expected=reshrunk["sha256"]))
    assert big["lines_added"] is None and big["lines_removed"] is None
    edited = mutate(tmp_path, edit(big["sha256"], old_string="aaa", new_string="bbb",
                                   replace_all=True))
    assert edited["replacements"] > 0
    assert edited["lines_added"] is None and edited["lines_removed"] is None


def test_line_stats_stay_null_for_binary_existing_content(tmp_path):
    raw = b"\xff\xfe binary"
    (tmp_path / "a.py").write_bytes(raw)
    result = mutate(tmp_path, write(expected=sha(raw), content="text\n"))
    assert result["created"] is False
    assert result["lines_added"] is None and result["lines_removed"] is None
    assert (tmp_path / "a.py").read_text() == "text\n"


def test_line_stats_bound_expansion_and_identical_rewrite(tmp_path):
    identical = mutate(tmp_path, write(content="same\n"))
    again = mutate(tmp_path, write(content="same\n", expected=identical["sha256"]))
    assert (again["lines_added"], again["lines_removed"]) == (0, 0)
    result = mutate(tmp_path, write(content="word\n", expected=again["sha256"]))
    grown = mutate(tmp_path, edit(result["sha256"], old_string="word",
                                  new_string="w" * ((1 << 20) + 10)))
    assert grown["size"] > (1 << 20)
    assert grown["lines_added"] is None and grown["lines_removed"] is None
    assert (tmp_path / "a.py").read_text() == "w" * ((1 << 20) + 10) + "\n"
