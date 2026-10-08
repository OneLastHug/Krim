"""原生修改清单的完整分页、跨连接失效、真实顺序及轻量读取。"""

import base64
import json
from dataclasses import dataclass

import pytest
from sqlalchemy import event, insert, update

from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.database import AgentSessionRow, WorkspaceEditProposalRow
from krim_agent.persistence.workspace_edits import WorkspaceEditStore
from krim_agent.persistence.workspace_list import NativeChangeListStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_change import FileChangeSpec, WorkspaceChangeAction
from krim_agent.workspace_writer import WorkspaceWriter


@dataclass
class Listing:
    database: Database
    edits: WorkspaceEditStore
    store: NativeChangeListStore
    scope: str
    topic: str

    async def historical(self, count, *, status="applied", offset=0, scope=None):
        # 0024升级的历史账本没有效果序。私有内容故意不满足新版快照结构，
        # 清单应只读轻量事实，不能要求重新解码旧文件内容或完整Diff。
        records = [
            {
                "proposal_id": f"legacy-{number:04}",
                "scope_id": scope or self.scope,
                "topic_id": self.topic,
                "request_key": f"preview-{number}",
                "request_hash": "a" * 64,
                "resource_fingerprint": "b" * 64,
                "workspace_key": "files",
                "path": f"历史/{number}.txt",
                "prepared": {"private": "不能作为公开清单返回"},
                "preview": {"operation": "write", "diff": "不应读取的完整差异"},
                "before_content": b"secret-before",
                "after_content": b"secret-after",
                "status": status,
                "revision": 2,
                "expires_at": 1.0,
            }
            for number in range(offset, offset + count)
        ]
        async with self.database.sessions.begin() as session:
            await session.execute(insert(WorkspaceEditProposalRow), records)
        return [record["proposal_id"] for record in records]


@pytest.fixture
async def listing(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'native-list.sqlite'}")
    await database.initialize()
    topic = await KrimRepository(database).create_topic("原生修改清单")
    edits = WorkspaceEditStore(database)
    try:
        yield Listing(database, edits, NativeChangeListStore(edits), "topic:" + topic.id, topic.id)
    finally:
        await database.close()


@pytest.mark.parametrize("count", [33, 65])
async def test_every_historical_item_is_reachable_without_silent_truncation(listing, count):
    expected = await listing.historical(count)
    cursor, found, sizes, revisions = None, [], [], set()
    while True:
        page = await listing.store.page(listing.scope, cursor=cursor)
        assert page.source == "workspace" and page.total == count
        assert page.filter == "active" and page.scope_id == listing.scope
        assert all(item.order_known is False and item.apply_sequence is None for item in page.items)
        assert all(item.path.startswith("历史/") for item in page.items)
        found.extend(item.proposal_id for item in page.items)
        sizes.append(len(page.items))
        revisions.add(page.snapshot_revision)
        cursor = page.next_cursor
        if cursor is None:
            break
    assert found == expected and len(set(found)) == count
    assert sizes == ([32, 1] if count == 33 else [32, 32, 1])
    assert len(revisions) == 1


async def test_filters_partition_actual_states_and_share_scope_snapshot(listing):
    statuses = [
        "proposed",
        "applying",
        "applied",
        "undoing",
        "undone",
        "keeping",
        "kept",
        "uncertain",
        "rejected",
    ]
    for number, status in enumerate(statuses):
        await listing.historical(1, status=status, offset=number)
    active = await listing.store.page(listing.scope)
    history = await listing.store.page(listing.scope, filter="history")
    combined = await listing.store.page(listing.scope, filter="all")
    assert {item.status for item in active.items} == set(statuses) - {
        "proposed",
        "rejected",
        "undone",
    }
    assert [item.status for item in history.items] == ["undone"]
    assert combined.total == active.total + history.total == 7
    assert active.snapshot_revision == history.snapshot_revision == combined.snapshot_revision


@pytest.mark.parametrize("hidden_status", ["proposed", "rejected"])
async def test_unlisted_proposals_invalidate_cursor_from_another_connection(listing, hidden_status):
    await listing.historical(33)
    first = await listing.store.page(listing.scope)
    other = Database(str(listing.database.engine.url))
    await other.initialize()
    try:
        topic = listing.topic
        second_store = Listing(
            other, WorkspaceEditStore(other), listing.store, listing.scope, topic
        )
        await second_store.historical(1, status=hidden_status, offset=33)
        with pytest.raises(ServiceError) as stale:
            await listing.store.page(listing.scope, cursor=first.next_cursor)
        assert stale.value.code == "workspace_list_stale" and stale.value.status == 409
        refreshed = await listing.store.page(listing.scope)
        assert refreshed.total == first.total
        assert refreshed.snapshot_revision != first.snapshot_revision
    finally:
        await other.close()


async def test_revision_change_invalidates_old_page_even_when_status_unchanged(listing):
    ids = await listing.historical(33)
    first = await listing.store.page(listing.scope)
    async with listing.database.sessions.begin() as session:
        await session.execute(
            update(WorkspaceEditProposalRow)
            .where(WorkspaceEditProposalRow.proposal_id == ids[-1])
            .values(revision=4)
        )
    with pytest.raises(ServiceError) as stale:
        await listing.store.page(listing.scope, cursor=first.next_cursor)
    assert stale.value.code == "workspace_list_stale"


async def test_cursor_is_bound_to_scope_and_filter(listing):
    await listing.historical(33)
    first = await listing.store.page(listing.scope)
    other = await KrimRepository(listing.database).create_topic("其他作用域")
    for scope, filter in [("topic:" + other.id, "active"), (listing.scope, "all")]:
        with pytest.raises(ServiceError) as invalid:
            await listing.store.page(scope, filter=filter, cursor=first.next_cursor)
        assert invalid.value.code == "workspace_list_cursor_invalid"
    assert len((await listing.store.page(listing.scope, cursor=first.next_cursor)).items) == 1


@pytest.mark.parametrize("cursor", ["", "!bad", "a" * 1025, "e30", "bnVsbA", "W10", "eA"])
async def test_malformed_and_oversized_cursor_is_rejected(listing, cursor):
    with pytest.raises(ServiceError) as invalid:
        await listing.store.page(listing.scope, cursor=cursor)
    assert invalid.value.code == "workspace_list_cursor_invalid" and invalid.value.status == 422


@pytest.mark.parametrize("position", [True, -1, 0, 33, 5000])
async def test_cursor_position_is_strict_and_within_snapshot(listing, position):
    await listing.historical(33)
    page = await listing.store.page(listing.scope)
    raw = base64.urlsafe_b64decode(page.next_cursor + "=" * (-len(page.next_cursor) % 4))
    value = json.loads(raw)
    value["position"] = position
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    cursor = base64.urlsafe_b64encode(encoded).decode().rstrip("=")
    with pytest.raises(ServiceError) as invalid:
        await listing.store.page(listing.scope, cursor=cursor)
    assert invalid.value.code == "workspace_list_cursor_invalid"


@pytest.mark.parametrize("limit", [0, 33, True, 1.5])
async def test_invalid_page_size_is_explicitly_rejected(listing, limit):
    with pytest.raises(ServiceError) as invalid:
        await listing.store.page(listing.scope, limit=limit)
    assert invalid.value.code == "workspace_list_limit_invalid"


async def test_page_size_can_change_without_changing_frozen_order(listing):
    expected = await listing.historical(4)
    first = await listing.store.page(listing.scope, limit=1)
    second = await listing.store.page(listing.scope, cursor=first.next_cursor, limit=3)
    assert [item.proposal_id for item in first.items + second.items] == expected
    assert second.next_cursor is None


async def test_empty_scope_is_stable_and_missing_or_managed_scopes_are_rejected(listing):
    first = await listing.store.page(listing.scope)
    second = await listing.store.page(listing.scope)
    assert first == second and first.total == 0 and first.items == [] and first.next_cursor is None
    for scope, code in [
        ("invalid", "workspace_scope_invalid"),
        ("topic:missing", "topic_not_found"),
    ]:
        with pytest.raises(ServiceError) as invalid:
            await listing.store.page(scope)
        assert invalid.value.code == code
    with pytest.raises(ServiceError) as invalid_filter:
        await listing.store.page(listing.scope, filter="unknown")
    assert invalid_filter.value.code == "workspace_list_filter_invalid"


async def test_closed_foreign_owned_session_retains_readable_history(listing):
    scope = "session:closed-session"
    async with listing.database.sessions.begin() as session:
        session.add(
            AgentSessionRow(
                session_id="closed-session",
                agent_id="agent",
                topic_id=listing.topic,
                name="历史Agent",
                persistent=True,
                definition_hash="c" * 64,
                status="closed",
                owner_id="foreign-owner",
            )
        )
    expected = await listing.historical(1, status="undone", scope=scope)
    page = await listing.store.page(scope, filter="history")
    assert [item.proposal_id for item in page.items] == expected
    with pytest.raises(ServiceError) as managed:
        await listing.store.page(listing.scope)
    assert managed.value.code == "workspace_scope_managed"


async def test_snapshot_is_one_lightweight_statement_and_uses_callers_transaction(listing):
    ids = await listing.historical(1)
    statements = []

    def capture(connection, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    async with listing.database.sessions.begin() as session:
        await session.execute(
            update(WorkspaceEditProposalRow)
            .where(WorkspaceEditProposalRow.proposal_id == ids[0])
            .values(status="kept", revision=5)
        )
        event.listen(listing.database.engine.sync_engine, "before_cursor_execute", capture)
        try:
            _, items = await listing.store.snapshot(session, listing.scope)
        finally:
            event.remove(listing.database.engine.sync_engine, "before_cursor_execute", capture)
        assert items[0].status == "kept" and items[0].revision == 5
        assert len(statements) == 1
        sql = statements[0].lower()
        assert "json_extract" in sql
        assert not any(field in sql for field in ["before_content", "after_content", "prepared"])
        # JSON只提取operation；未把整个preview列作为返回字段。
        assert ", workspace_edit_proposals.preview," not in sql
        assert "secret" not in items[0].model_dump_json()


async def test_explicit_limit_includes_hidden_proposals_without_partial_success(listing):
    await listing.historical(5000, status="proposed")
    page = await listing.store.page(listing.scope)
    assert page.total == 0
    await listing.historical(1, offset=5000, status="rejected")
    with pytest.raises(ServiceError) as bounded:
        await listing.store.page(listing.scope)
    assert bounded.value.code == "workspace_list_too_large" and bounded.value.status == 409


async def test_real_apply_order_overrides_preview_order_and_undo_does_not_reorder(
    listing, tmp_path
):
    root = tmp_path / "files"
    root.mkdir()
    workspace = Workspace(root, writable=True)
    writer = WorkspaceWriter(workspace)
    try:
        prepared = [
            writer.prepare(
                FileChangeSpec(
                    path=f"{number}.txt", operation="write", content="new", expected_sha256=None
                )
            )
            for number in range(3)
        ]
        proposals = [
            (await listing.edits.preview(listing.scope, "files", workspace.fingerprint, str(i), p))[
                0
            ]
            for i, p in enumerate(prepared)
        ]
        order = [2, 0, 1]
        for number in order:
            proposal, plan = proposals[number], prepared[number]
            attempt, _ = await listing.edits.reserve(
                listing.scope,
                WorkspaceChangeAction(
                    proposal_id=proposal.view.proposal_id,
                    action="apply",
                    expected_revision=0,
                    request_key=f"apply-{number}",
                ),
                workspace.fingerprint,
                plan,
            )
            result = writer.apply(plan)
            assert result.receipt.status == "done"
            await listing.edits.settle(
                listing.scope, attempt.view.operation_id, result.receipt, result.recovery
            )
            if result.recovery is not None:
                writer.finalize(plan, result.recovery)
            await listing.edits.mark_cleaned(listing.scope, attempt.view.operation_id)
        await listing.historical(1)
        before = await listing.store.page(listing.scope)
        expected = [proposals[number].view.proposal_id for number in reversed(order)]
        assert [item.proposal_id for item in before.items[:3]] == expected
        assert all(item.order_known for item in before.items[:3])
        assert before.items[-1].order_known is False
        current = await listing.edits.get(listing.scope, proposals[2].view.proposal_id)
        reverse = writer.prepare_restore(prepared[2])
        attempt, _ = await listing.edits.reserve(
            listing.scope,
            WorkspaceChangeAction(
                proposal_id=current.view.proposal_id,
                action="undo",
                expected_revision=current.view.revision,
                request_key="undo",
            ),
            workspace.fingerprint,
            reverse,
        )
        result = writer.apply(reverse)
        assert result.receipt.status == "done"
        await listing.edits.settle(
            listing.scope, attempt.view.operation_id, result.receipt, result.recovery
        )
        if result.recovery is not None:
            writer.finalize(reverse, result.recovery)
        await listing.edits.mark_cleaned(listing.scope, attempt.view.operation_id)
        after = await listing.store.page(listing.scope, filter="all")
        assert [item.proposal_id for item in after.items[:3]] == expected
        assert after.items[2].status == "undone"
        assert after.items[2].apply_sequence == before.items[2].apply_sequence
        assert after.snapshot_revision != before.snapshot_revision
    finally:
        workspace.close()
