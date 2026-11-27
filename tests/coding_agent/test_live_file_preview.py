"""File counts must change before tools finish, without mutation or fake receipts."""
import asyncio
import hashlib
import json

import pytest

from krim_agent.contracts import Message
from krim_agent.providers.types import ModelToolCall, ModelToolInput
from krim_agent.runtime.file_preview import FileChangePreviews, PartialFileArguments
from krim_agent.runtime.model_step import ModelStep


def digest(text):
    return hashlib.sha256(text.encode()).hexdigest()


def test_incremental_decoder_keeps_unicode_escapes_and_arbitrary_field_order():
    args = {"content": '中文😀\nquote " and \\ slash\n尾行', "path": "文档.txt"}
    raw = json.dumps(args, ensure_ascii=True)
    decoder = PartialFileArguments()
    growing = []
    for char in raw:
        decoder.feed(char)
        values, closed = decoder.snapshot()
        if "content" in values:
            growing.append(values["content"])
        assert not decoder.invalid
    assert decoder.finished
    assert decoder.snapshot() == (args, {"content", "path"})
    assert len(set(growing)) > 8
    assert all(args["content"].startswith(text) for text in growing)


def test_new_file_counts_grow_from_complete_lines_before_parameter_completion():
    preview = FileChangePreviews([])
    first = preview.update("a", "write", '{"path":"a.txt","content":"一行\\n', False, None, 0)
    second = preview.update("a", "write", '第二行😀\\n', False, None, 0.25)
    third = preview.update("a", "write", '尾', False, None, 0.5)
    final = preview.update("a", "write", '行"}', True,
                           {"path": "a.txt", "content": "一行\n第二行😀\n尾行"}, 0.6)
    assert first["lines_added"] == 1 and not first["complete"]
    assert second["lines_added"] == 2 and second["lines_removed"] == 0
    assert third is None  # unfinished tail is not counted twice
    assert final["lines_added"] == 3 and final["complete"]
    assert [first["revision"], second["revision"], final["revision"]] == [1, 2, 3]
    assert "content" not in repr(final)


def test_partial_overwrite_does_not_claim_missing_tail_deleted():
    original = "same\nold\nkeep\n"
    messages = [
        Message(role="assistant", content="", tool_calls=[{
            "id": "prior", "name": "write", "arguments": {"path": "a", "content": original},
        }]),
        Message(role="tool", call_id="prior", content=json.dumps({
            "ok": True, "sha256": digest(original),
        })),
    ]
    previews = FileChangePreviews(messages)
    header = json.dumps({"path": "a", "expected_sha256": digest(original)})[:-1]
    first = previews.update("b", "write", header + ',"content":"same\\n', False, None, 0)
    second = previews.update("b", "write", 'new\\n', False, None, 0.3)
    assert (first["lines_added"], first["lines_removed"]) == (0, 0)
    assert (second["lines_added"], second["lines_removed"]) == (1, 0)
    final = previews.update("b", "write", '"}', True, {
        "path": "a", "expected_sha256": digest(original), "content": "same\nnew\n",
    }, 0.4)
    assert (final["lines_added"], final["lines_removed"]) == (1, 2)


def test_edit_can_preview_insertions_and_deleted_lines():
    p = FileChangePreviews([])
    prefix = '{"path":"a","old_string":"old\\nremove\\n","new_string":"'
    first = p.update("edit1", "edit", prefix + 'new\\n', False, None, 0)
    second = p.update("edit1", "edit", 'extra\\nmore\\n', False, None, 0.3)
    final = p.update("edit1", "edit", '"}', True, {
        "path": "a", "old_string": "old\nremove\n", "new_string": "new\nextra\nmore\n",
    }, 0.4)
    assert (first["lines_added"], first["lines_removed"]) == (1, 2)
    assert (second["lines_added"], second["lines_removed"]) == (3, 2)
    assert (final["lines_added"], final["lines_removed"]) == (3, 2)
    deletion = p.update("edit2", "edit", "", True, {
        "path": "a", "old_string": "one\ntwo\n", "new_string": "",
    }, 0.5)
    assert (deletion["lines_added"], deletion["lines_removed"]) == (0, 2)


@pytest.mark.parametrize("path", ["../bad", "/tmp/bad", ".env", ".git/config"])
def test_preview_rejects_protected_and_outside_paths(path):
    assert FileChangePreviews([]).update("a", "write", "", True, {
        "path": path, "content": "x\n",
    }, 0) is None


def test_removal_operand_grows_before_old_string_and_call_finish():
    p = FileChangePreviews([])
    first = p.update("r", "edit", '{"path":"a","old_string":"a\\n',
                     False, None, 0)
    second = p.update("r", "edit", 'b\\n', False, None, 0.3)
    assert first["lines_added"] is None and first["lines_removed"] == 1
    assert second["lines_removed"] == 2 and not second["complete"]
    final = p.update("r", "edit", '\",\"new_string\":\"\"}', True,
                     {"path": "a", "old_string": "a\nb\n", "new_string": ""}, 0.4)
    assert final["lines_added"] == 0 and final["lines_removed"] == 2


def test_invalid_duplicate_keys_do_not_keep_a_live_count():
    d = PartialFileArguments()
    d.feed('{"path":"one","path":"two","content":"x"}')
    assert d.invalid


def test_calls_are_isolated_and_revision_is_per_call():
    p = FileChangePreviews([])
    a = p.update("one", "write", '{"path":"a","content":"a\\n', False, None, 0)
    b = p.update("two", "write", '{"path":"b","content":"b\\nc\\n', False, None, 0)
    a2 = p.update("one", "write", 'd\\n', False, None, 0.3)
    assert a["lines_added"] == 1 and b["lines_added"] == 2 and a2["lines_added"] == 2
    assert a2["revision"] == 2 and b["revision"] == 1


def test_stream_id_changes_retain_decoder_and_rebind_one_preview():
    p = FileChangePreviews([])
    first = p.update("wire", "write", '{"path":"a","content":"a\\n',
                     False, None, 0, "slot-0")
    second = p.update("wire-1", "write", 'b\\n', False, None, 0.3, "slot-0")
    assert first["provider_call_id"] == "wire"
    assert second["provider_call_id"] == "wire-1"
    assert second["replaces_provider_call_id"] == "wire"
    assert second["lines_added"] == 2 and second["revision"] == 2
    final = p.update("wire-1", "write", "", True, {
        "path": "a", "content": "a\nb\n",
    }, 0.4)
    assert final["revision"] == 3


def test_insert_prefix_is_not_counted_as_deleted_old_line():
    old = "a\nb\nc\n"
    messages = [
        Message(role="assistant", content="", tool_calls=[{
            "id": "read", "name": "read", "arguments": {"path": "a"},
        }]),
        Message(role="tool", call_id="read", content=json.dumps({
            "text": old, "offset": 0, "eof": True, "sha256": digest(old),
        })),
    ]
    prefix = json.dumps({"path": "a", "expected_sha256": digest(old)})[:-1]
    result = FileChangePreviews(messages).update(
        "w", "write", prefix + ',"content":"a\\nx\\n', False, None, 0,
    )
    assert (result["lines_added"], result["lines_removed"]) == (1, 0)


async def test_model_step_emits_multiple_counts_while_provider_still_running():
    observed = []
    second_seen, release = asyncio.Event(), asyncio.Event()

    class Provider:
        async def stream(self, messages, tools):
            yield ModelToolInput(id="wire", name="write", delta='{"path":"a","content":"a\\n')
            await asyncio.sleep(0.22)
            yield ModelToolInput(id="wire", name="write", delta='b\\n')
            await release.wait()
            yield ModelToolInput(id="wire", name="write", delta='"}', complete=True)
            yield ModelToolCall(id="wire", name="write", arguments={
                "path": "a", "content": "a\nb\n",
            })

        async def close(self):
            pass

    async def emit(kind, data):
        observed.append((kind, data))
        if kind == "file_change_preview" and data["lines_added"] == 2:
            second_seen.set()
        return object()

    step = ModelStep(Provider(), emit, lambda: None, max_steps=2,
                     max_output=1024, deadline=None, workspace=False)
    task = asyncio.create_task(step.call([Message(role="user", content="go")], [],
                                         max_group=None, remaining_tools=None))
    try:
        await asyncio.wait_for(second_seen.wait(), 3)
        assert not task.done()
        assert not any(kind == "model_end" for kind, data in observed)
        counts = [d["lines_added"] for k, d in observed if k == "file_change_preview"]
        assert counts == [1, 2]
    finally:
        release.set()
    result = await asyncio.wait_for(task, 3)
    assert len(result.calls) == 1
    end_index = next(i for i, (kind, _) in enumerate(observed) if kind == "model_end")
    assert all(i < end_index for i, (k, _) in enumerate(observed) if k == "file_change_preview")
    assert '"content"' not in repr([d for k, d in observed if k == "file_change_preview"])
