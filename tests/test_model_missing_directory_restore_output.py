"""真实长UTF-8路径差异须在原JSON上限内完整分页，不能静默少给。"""

import json
from datetime import UTC, datetime

import pytest
from test_model_missing_directory_restores import PREVIEW, QUERY, seed_target, setup

from krim_agent.protocol.models import StreamFrame, check_json
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.tools.registry import ToolRegistry
from krim_agent.tools.workspace_checkpoints import missing_directory_restore_control_tools
from krim_agent.workspace_missing_directory_restore import MissingDirectoryRestoreView
from krim_agent.workspace_missing_directory_restore_model import (
    MissingDirectoryRestoreQueryResult,
    bounded_missing_directory_restore_result,
)


class DiffProvider:
    name = "offline-long-missing-diff"

    def __init__(self):
        self.calls, self.target_id, self.planned = 0, None, None
        self.paths, self.pages = [], []
        self.replies = []

    async def stream(self, messages, tools):
        self.calls += 1
        if self.calls > 1:
            self.replies.append(messages[-1].content)
        if self.calls == 1:
            yield ModelToolCall(
                id="long-preview",
                name=PREVIEW,
                arguments={
                    "target": {"kind": "checkpoint", "identifier": self.target_id},
                    "request_key": "long-preview",
                },
            )
            return
        if self.calls == 2:
            self.planned = MissingDirectoryRestoreView.model_validate_json(messages[-1].content)
            offset = 0
        else:
            result = MissingDirectoryRestoreQueryResult.model_validate_json(messages[-1].content)
            self.pages.append(result.diff)
            self.paths.extend(item.path for item in result.diff.entries)
            offset = result.diff.next_offset
            if offset is None:
                yield ModelText(text="原缺失差异全部分页读取")
                return
        yield ModelToolCall(
            id="long-query-" + str(self.calls),
            name=QUERY,
            arguments={
                "kind": "diff",
                "plan_id": self.planned.plan_id,
                "offset": offset,
                "limit": 128,
            },
        )


async def test_large_unicode_missing_diff_reads_all_entries_with_original_output_budget(
    runtime, tmp_path
):
    provider = DiffProvider()
    service, agent, _ = await setup(runtime, tmp_path, provider)
    try:
        folder = tmp_path / "tree" / ("甲" * 50) / ("乙" * 50)
        folder.mkdir(parents=True)
        for index in range(128):
            (folder / ("文" * 50 + f"-{index:03d}")).write_bytes(b"\0\xff")
        provider.target_id = await seed_target(agent, tmp_path)
        result = await agent.execute("完整读取长路径缺失差异", idempotency_key="bounded-diff")
        assert result.status == "completed", (result, provider.calls, provider.replies)
        assert len(provider.pages) >= 2
        assert (
            len(provider.paths)
            == len(set(provider.paths))
            == provider.planned.target_snapshot.entry_count
        )
        assert provider.paths == sorted(provider.paths)
        assert not (tmp_path / "tree").exists(), "读取差异不能发布目录"
    finally:
        await service.close()


def synthetic_history(index):
    """协议字节边界的合成合法回执，独立于真实捕获/持久存储验收。"""
    root = "tree/" + "/".join(letter * 50 for letter in ("甲", "乙", "丙"))
    fingerprint = "a" * 64
    return MissingDirectoryRestoreView(
        plan_id=f"plan-{index}",
        scope_id="session:original",
        target={"kind": "checkpoint", "identifier": "target"},
        root_path=root,
        workspace_key="files",
        resource_fingerprint=fingerprint,
        target_snapshot={
            "snapshot_fingerprint": fingerprint,
            "entry_count": 1,
            "file_count": 0,
            "directory_count": 1,
            "byte_size": 0,
        },
        baseline={"kind": "absent", "fingerprint": fingerprint},
        request_key="原预览" * 40,
        execute_key="原执行" * 40,
        revision=3,
        phase="finished",
        status="done",
        operation_id=f"operation-{index}",
        receipt={
            "root_path": root,
            "status": "done",
            "before_absence_fingerprint": fingerprint,
            "target_fingerprint": fingerprint,
            "after_fingerprint": fingerprint,
            "changed": True,
            "error_code": None,
            "recovery_required": False,
        },
        cleanup_pending=False,
        cancel_requested=False,
        expires_at=2_000_000_000.0,
        error_code=None,
        created_at=datetime(2026, 10, 16, tzinfo=UTC),
        updated_at=datetime(2026, 10, 16, tzinfo=UTC),
    )


def test_history_byte_boundary_preserves_all_unicode_receipts_and_real_tool_frames():
    original = [synthetic_history(index) for index in range(32)]
    definition = missing_directory_restore_control_tools()[2]

    def envelope(value):
        result = ToolRegistry._normalize(definition, value)
        StreamFrame(
            kind="chunk",
            seq=0,
            data={
                "type": "tool_result",
                "call_id": "c" * 128,
                "name": QUERY,
                **result.model_dump(mode="json"),
            },
        )

    full = MissingDirectoryRestoreQueryResult(kind="history", entries=original, offset=0, limit=32)
    with pytest.raises(ValueError):
        check_json(full.model_dump(mode="json"))
    offset, seen, pages = 0, [], []
    while True:
        selected = MissingDirectoryRestoreQueryResult(
            kind="history", entries=original[offset : offset + 32], offset=offset, limit=32
        )
        selected = bounded_missing_directory_restore_result(selected, validate_envelope=envelope)
        value = selected.model_dump(mode="json")
        check_json(value)
        envelope(value)
        assert MissingDirectoryRestoreQueryResult.model_validate_json(json.dumps(value)) == selected
        pages.append(selected)
        seen.extend(selected.entries)
        if selected.next_offset is None:
            break
        assert selected.next_offset == offset + len(selected.entries) > offset
        offset = selected.next_offset
    assert len(pages) >= 2 and seen == original
