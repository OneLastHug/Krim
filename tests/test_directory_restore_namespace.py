"""不同资源名的同一真实目录仍与非空Move/Exchange双向排他。"""

import pytest
from test_directory_checkpoint_capture_store import capture_context as capture_context
from test_directory_restore_execution import execute_request, ready

from krim_agent.persistence.workspace_tree_exchanges import WorkspaceTreeExchangeStore
from krim_agent.persistence.workspace_tree_moves import WorkspaceTreeMoveStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace, WorkspaceBinding
from krim_agent.workspace_tree_exchange import WorkspaceTreeExchangeWriter
from krim_agent.workspace_tree_exchange_types import TreeExchangeAction, TreeExchangePreviewRequest
from krim_agent.workspace_tree_move import TreeMoveAction, TreeMovePreviewRequest
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter


@pytest.mark.parametrize("kind", ["move", "exchange"])
@pytest.mark.parametrize("first", ["restore", "neighbor"])
async def test_move_exchange_physical_aliases_block_both_admission_orders(
    capture_context, kind, first
):
    store, scope, binding, _, planned = await ready(capture_context)
    with Workspace(binding.workspace.root, writable=True) as other_workspace:
        alias = WorkspaceBinding("alias", "2", other_workspace, "b" * 64)
        if kind == "move":
            writer = WorkspaceTreeWriter(other_workspace)
            before = writer.reader.capture("tree")
            change = writer.prepare("tree", "moved", expected_fingerprint=before.fingerprint)
            neighbor = WorkspaceTreeMoveStore(store.edits)
            proposal, _ = await neighbor.preview(
                scope,
                alias,
                TreeMovePreviewRequest(
                    source_path="tree",
                    target_path="moved",
                    expected_fingerprint=before.fingerprint,
                    request_key="aliased-move",
                ),
                change,
            )
            action = TreeMoveAction(
                proposal_id=proposal.view.proposal_id,
                action="apply",
                expected_revision=proposal.view.revision,
                request_key="aliased-move",
            )
        else:
            (other_workspace.root / "other").mkdir()
            (other_workspace.root / "other/binary").write_bytes(b"other\0\xff")
            writer = WorkspaceTreeExchangeWriter(other_workspace)
            left = writer.trees.reader.capture("tree")
            right = writer.trees.reader.capture("other")
            change = writer.prepare(
                "tree", "other", expected_left=left.fingerprint, expected_right=right.fingerprint
            )
            neighbor = WorkspaceTreeExchangeStore(store.edits)
            proposal, _ = await neighbor.preview(
                scope,
                alias,
                TreeExchangePreviewRequest(
                    left_path="tree",
                    right_path="other",
                    expected_left=left.fingerprint,
                    expected_right=right.fingerprint,
                    request_key="aliased-exchange",
                ),
                change,
            )
            action = TreeExchangeAction(
                proposal_id=proposal.view.proposal_id,
                action="apply",
                expected_revision=proposal.view.revision,
                request_key="aliased-exchange",
            )
        if first == "restore":
            await store.start(scope, binding, execute_request(planned), qualify=lambda: None)
            with pytest.raises(ServiceError) as denied:
                await neighbor.reserve(scope, action, alias.fingerprint, change)
            assert denied.value.code == "workspace_directory_restore_busy"
        else:
            await neighbor.reserve(scope, action, alias.fingerprint, change)
            with pytest.raises(ServiceError) as denied:
                await store.start(scope, binding, execute_request(planned), qualify=lambda: None)
            assert denied.value.code == f"workspace_tree_{kind}_busy"
        assert (other_workspace.root / "tree/nested/nul").read_bytes() == b"current\x80\0"
        assert not (other_workspace.root / "moved").exists()
