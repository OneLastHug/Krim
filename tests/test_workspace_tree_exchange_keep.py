"""真实双完整树keep只核实原actual after，0namespace且保全原effect。"""

import copy
import os
import sys
import time
from dataclasses import replace
from threading import Event

import pytest
from test_workspace_tree_exchange import prepare, seed

import krim_agent.workspace_tree_exchange as module
from krim_agent.persistence.tree_exchange_codec import encode_exchange_effect
from krim_agent.protocol.domain import ServiceError
from krim_agent.workspace import Workspace
from krim_agent.workspace_tree_exchange import WorkspaceTreeExchangeWriter

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="真实Linux交换/树锁")


def forbid_replay(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("保留核实不得执行rename/apply/prepare或探测namespace syscall")

    for name in ("rename_at", "_rename_function"):
        monkeypatch.setattr(module, name, forbidden)
    for name in ("apply", "prepare", "prepare_undo"):
        monkeypatch.setattr(WorkspaceTreeExchangeWriter, name, forbidden)


def test_keep_returns_exact_original_effect_and_encoded_dual_after_without_namespace(
    tmp_path, monkeypatch
):
    seed(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeExchangeWriter(workspace)
        change = prepare(writer)
        effect = writer.apply(change)
        assert effect.receipt.status == "done"
        encoded = encode_exchange_effect(change, effect)
        inodes = [
            (tmp_path / path).stat().st_ino for path in ("holder/live", "new-parent/prepared")
        ]
        forbid_replay(monkeypatch)
        permissions = []
        kept = writer.verify_keep(change, effect, authorize=lambda: permissions.append(True))
        assert kept is effect and kept.receipt.changed and permissions == [True, True]
        assert encode_exchange_effect(change, kept) == encoded
        assert [
            (tmp_path / path).stat().st_ino for path in ("holder/live", "new-parent/prepared")
        ] == inodes
        assert (tmp_path / "holder/live/sub/data.bin").read_bytes() == b"\x80\x00new"
        assert (tmp_path / "new-parent/prepared/sub/data.bin").read_bytes() == b"\0\xff\x80"


@pytest.mark.parametrize(
    "field,value",
    [
        ("status", "uncertain"),
        ("changed", False),
        ("error_code", "forged"),
        ("recovery_required", True),
        ("operation_token", "other"),
        ("left_path", "other"),
        ("right_path", "other"),
        ("left_before_fingerprint", "a" * 64),
        ("right_before_fingerprint", "b" * 64),
        ("left_after_fingerprint", "c" * 64),
        ("right_after_fingerprint", "d" * 64),
    ],
)
def test_keep_rejects_each_substituted_original_receipt_field(tmp_path, monkeypatch, field, value):
    seed(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeExchangeWriter(workspace)
        change = prepare(writer)
        effect = writer.apply(change)
        forged = replace(effect, receipt=replace(effect.receipt, **{field: value}))
        forbid_replay(monkeypatch)
        result = writer.verify_keep(change, forged)
        assert result.receipt.status == "rejected" and not result.receipt.changed
        assert result.receipt.error_code == "workspace_tree_exchange_receipt"


@pytest.mark.parametrize("fault", ["left_missing", "right_missing", "swapped", "mixed"])
def test_keep_rejects_missing_or_spliced_full_actual_after(tmp_path, monkeypatch, fault):
    seed(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeExchangeWriter(workspace)
        change = prepare(writer)
        effect = writer.apply(change)
        if fault.endswith("missing"):
            forged = replace(effect, **{f"observed_{fault.split('_')[0]}": None})
        else:
            left = effect.observed_right if fault == "swapped" else change.left
            right = effect.observed_left if fault == "swapped" else effect.observed_right
            forged = replace(
                effect,
                observed_left=left,
                observed_right=right,
                receipt=replace(
                    effect.receipt,
                    left_after_fingerprint=left.fingerprint,
                    right_after_fingerprint=right.fingerprint,
                ),
            )
        forbid_replay(monkeypatch)
        result = writer.verify_keep(change, forged)
        assert result.receipt.status == "rejected" and not result.receipt.changed
        assert result.receipt.error_code == "workspace_tree_exchange_receipt"


@pytest.mark.parametrize("side", ["left", "right"])
@pytest.mark.parametrize("fault", ["bytes", "inode", "mode", "version", "added", "parent"])
def test_keep_requires_exact_original_after_on_both_trees(tmp_path, monkeypatch, side, fault):
    seed(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeExchangeWriter(workspace)
        change = prepare(writer)
        effect = writer.apply(change)
        path = tmp_path / ("holder/live" if side == "left" else "new-parent/prepared")
        file = path / "sub/data.bin"
        if fault == "bytes":
            file.write_bytes(b"changed")
        elif fault == "inode":
            contents, mode = file.read_bytes(), file.stat().st_mode & 0o777
            file.rename(tmp_path / "retained.bin")
            file.write_bytes(contents)
            file.chmod(mode)
        elif fault == "mode":
            file.chmod(0o644)
        elif fault == "version":
            info = file.stat()
            os.utime(file, ns=(info.st_atime_ns, info.st_mtime_ns + 1_000_000))
        elif fault == "added":
            (path / "extra.txt").write_text("extra")
        else:
            parent = path.parent
            parent.rename(tmp_path / "retained-parent")
            parent.mkdir()
        forbid_replay(monkeypatch)
        result = writer.verify_keep(change, effect)
        assert result.receipt.status == "rejected" and not result.receipt.changed
        assert result.receipt.error_code in {
            "workspace_tree_exchange_conflict",
            "workspace_parent_changed",
        }


@pytest.mark.parametrize("phase", ["cancel", "first_authority", "last_authority", "last_mutation"])
def test_keep_preserves_cancel_and_last_authority_and_rechecks_after_callback(
    tmp_path, monkeypatch, phase
):
    seed(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeExchangeWriter(workspace)
        change = prepare(writer)
        effect = writer.apply(change)
        abort = Event()
        checks = 0

        def authorize():
            nonlocal checks
            checks += 1
            if phase == "first_authority" or phase == "last_authority" and checks == 2:
                raise ServiceError("original_authority_lost", "原当前资格失效", 409)
            if checks == 2 and phase == "cancel":
                abort.set()
            if checks == 2 and phase == "last_mutation":
                (tmp_path / "new-parent/prepared/sub/data.bin").write_bytes(b"raced")

        forbid_replay(monkeypatch)
        result = writer.verify_keep(change, effect, cancelled=abort.is_set, authorize=authorize)
        assert result.receipt.status == "rejected" and not result.receipt.changed
        assert result.receipt.error_code == (
            "workspace_write_cancelled"
            if phase == "cancel"
            else "workspace_tree_exchange_conflict"
            if phase == "last_mutation"
            else "original_authority_lost"
        )


def test_keep_does_not_grant_readonly_workspace_or_replace_original_material(tmp_path, monkeypatch):
    seed(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeExchangeWriter(workspace)
        change = prepare(writer)
        effect = writer.apply(change)
    forbid_replay(monkeypatch)
    with Workspace(tmp_path, writable=False) as workspace:
        with pytest.raises(ServiceError) as refused:
            WorkspaceTreeExchangeWriter(workspace).verify_keep(copy.deepcopy(change), effect)
        assert refused.value.code == "workspace_binding_changed"


def test_keep_deadline_includes_authority_wait_and_rejects_without_namespace(tmp_path, monkeypatch):
    seed(tmp_path)
    with Workspace(tmp_path, writable=True) as workspace:
        writer = WorkspaceTreeExchangeWriter(workspace)
        change = prepare(writer)
        effect = writer.apply(change)
        writer.lock_timeout = 0.01
        forbid_replay(monkeypatch)
        result = writer.verify_keep(change, effect, authorize=lambda: time.sleep(0.02))
        assert result.receipt.status == "rejected" and not result.receipt.changed
        assert result.receipt.error_code == "workspace_tree_move_timeout"


async def test_original_sealed_source_sql_keep_uses_same_physical_effect_and_logical_false(
    runtime, tmp_path, monkeypatch
):
    from test_tree_exchange_origins import prepared
    from test_workspace_tree_exchange_ledger import action, publish

    (
        service,
        agent,
        scope,
        trees,
        plan,
        binding,
        writer,
        seals,
        store,
        request,
        change,
    ) = await prepared(runtime, tmp_path)
    try:
        proposal, _ = await store.preview(scope, binding, request, change)
        apply, _ = await store.reserve(scope, action(proposal.view), binding.fingerprint, change)
        effect = await publish(store, writer, scope, apply)
        await store.settle(scope, apply.view.operation_id, effect)
        current = (await store.proposal(scope, proposal.view.proposal_id)).view
        keep, _ = await store.reserve(
            scope, action(current, "keep", "keep"), binding.fingerprint, change
        )
        forbid_replay(monkeypatch)
        verified = writer.verify_keep(change, effect)
        assert verified is effect
        settled = await store.settle(scope, keep.view.operation_id, verified)
        assert settled.effect == effect and settled.effect.receipt.changed
        assert settled.view.status == "done" and not settled.view.receipt.changed
        source = (await store.proposal(scope, proposal.view.proposal_id)).view
        assert source.origin == "completed_tree" and source.source_plan_id == plan.plan_id
    finally:
        await service.close()
