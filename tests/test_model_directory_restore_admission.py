"""丢失准入回程的收尾必须匹配原完整请求、来源、阶段和owner。"""

from dataclasses import replace

from test_directory_restore_execution import execute_request, prepared
from test_directory_restore_model_sources import model_ready
from test_directory_restore_plans import request
from test_model_directory_checkpoint_store import context as context

from krim_agent.workspace_directory_restore import DirectoryRestoreExecute


async def test_preview_cleanup_rejects_same_source_changed_ttl_or_target_before_exact_cancel(
    context,
):
    _, _, binding, scope, source, _ = context
    store, ready = await model_ready(context)
    original = request(ready.target.identifier, "unreturned-preview")
    actual = replace(source, call_id="unreturned")
    planned, _ = await store.reserve(
        scope, binding, original, model_source=actual, qualify=lambda: None
    )
    for altered in (
        original.model_copy(update={"ttl_seconds": original.ttl_seconds + 1}),
        original.model_copy(
            update={"target": original.target.model_copy(update={"identifier": "other"})}
        ),
    ):
        assert await store.cancel_model_admission(scope, actual, "preview", altered) is None
        assert (await store.get(scope, planned.plan_id)).phase == "preparing"
    assert (
        await store.cancel_model_admission(
            scope, replace(actual, call_id="other"), "preview", original
        )
        is None
    )
    closed = await store.cancel_model_admission(scope, actual, "preview", original)
    assert closed.phase == "cancelled" and not closed.cleanup_pending
    assert await store.cancel_model_admission(scope, actual, "preview", original) == closed


async def test_execute_cleanup_compares_entire_original_request_and_never_cancels_new_verify(
    context,
):
    _, _, binding, scope, source, _ = context
    store, planned = await model_ready(context)
    execute_source = replace(source, call_id="original-execute")
    op, fence, _, change, _ = await prepared(
        store, scope, binding, planned, model_source=execute_source
    )
    original = execute_request(planned)
    for altered in (
        original.model_copy(update={"request_key": "other-execute"}),
        original.model_copy(update={"expected_revision": original.expected_revision + 1}),
    ):
        assert await store.cancel_model_admission(scope, execute_source, "execute", altered) is None
        assert (await store.get(scope, planned.plan_id)).phase == "executing"
    late_preview = await store.cancel_model_admission(
        scope, source, "preview", request(planned.target.identifier)
    )
    assert late_preview.phase == "executing"
    await store.record_intent(scope, fence, binding, change, qualify=lambda: None)
    await store.fail(scope, fence, "lost-original-dispatch")
    verify_source = replace(source, call_id="current-verify")
    await store.claim_verify(
        scope, op.operation_id, binding, model_source=verify_source, qualify=lambda: None
    )
    stale = await store.cancel_model_admission(scope, execute_source, "execute", original)
    assert stale.phase == "executing", stale
    assert (
        await store.cancel_model_admission(scope, execute_source, "verify", op.operation_id) is None
    )
    assert (
        await store.cancel_model_admission(scope, verify_source, "verify", "other-operation")
        is None
    )
    closed = await store.cancel_model_admission(scope, verify_source, "verify", op.operation_id)
    assert closed.phase == "uncertain" and closed.operation_id == op.operation_id
    assert await store.source(scope, planned.plan_id, "execute") == execute_source
    assert await store.source(scope, planned.plan_id, "verify") == verify_source


async def test_execute_exact_cleanup_closes_only_own_active_owner(context):
    _, _, binding, scope, source, _ = context
    store, planned = await model_ready(context)
    actual = replace(source, call_id="execute-close")
    wanted = DirectoryRestoreExecute(
        plan_id=planned.plan_id, expected_revision=planned.revision, request_key="same-execute"
    )
    operation, _ = await store.start(
        scope, binding, wanted, model_source=actual, qualify=lambda: None
    )
    closed = await store.cancel_model_admission(scope, actual, "execute", wanted)
    assert closed.phase == "cancelled" and closed.operation_id == operation.operation_id
    assert not closed.cleanup_pending
