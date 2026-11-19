"""SDK loop under the real RunService/SQLite approval and session state machine."""

import asyncio
from pathlib import Path

import pytest

from krim_agent.config import Settings
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import ApprovalReply, ServiceError, parse_json
from krim_agent.runtime.sdk_loop import DevelopmentRunScope, DevelopmentSDKLoop
from krim_agent.runtime.service import RunBinding, RunService
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage import sessions as session_sql
from tests.coding_agent.sdk_fixtures import setup
from tests.coding_agent.test_development_storage import prepare
from tests.coding_agent.test_sdk_host_bridge import HostScript


async def application(path: Path, *, approval: bool):
    store, repositories, record, seeded = await prepare(path)
    initial = await store.snapshot(seeded.ownership.run_id)
    workspace = initial.session_context.workspace_id
    harness = setup(approval=approval)
    executor = harness.adapter().executor
    provider = HostScript()
    settings = Settings(
        run_timeout=30, max_request_bytes=65536, history_compaction=False,
        development_history_compaction=False,
    )
    cleanup = []

    async def scope(run_id, check, budget):
        snapshot = await store.snapshot(run_id)
        owner = seeded.ownership.model_copy(update={
            'run_id': run_id, 'session_id': snapshot.session_context.session_id,
        })
        check()
        return DevelopmentRunScope(record.environment, owner, None)

    async def close_run(run_id):
        cleanup.append(run_id)

    loop = DevelopmentSDKLoop(store, provider, executor, settings,
                              resolve_scope=scope, repositories=repositories,
                              cleanup_run=close_run, descriptor={'version': 'development-test/1'})
    binding = RunBinding.freeze('demo', loop, settings)
    target = parse_json(binding.descriptor_json)
    service = RunService(store, loop, settings, target, {'demo': binding})
    session = await session_sql.create_session(
        store, workspace, record.environment.owner_id, SessionCreate(request_key='sdk-session'),
        target, ContextPolicy(), settings,
    )
    turn = await session_sql.submit(
        store, session.id,
        TurnRequest(text='go', request_key='sdk-turn', expected_revision=0), settings,
    )
    run = await service.start_turn(turn.id)
    return store, service, harness, provider, run, cleanup


async def test_sdk_run_service_owns_start_and_finish_once(tmp_path: Path) -> None:
    store, service, harness, provider, run, cleanup = await application(
        tmp_path / 'run.sqlite', approval=False)
    try:
        result = await service.wait(run.id)
        assert result.status == 'completed', result.code
        assert harness.counts['write'] == 1 and provider.calls == 2
        snapshot = await store.snapshot(run.id)
        assert sum(e.kind == 'terminal' for e in snapshot.events) == 1
        assert cleanup == [run.id]
    finally:
        await service.close()
        await store.close()


@pytest.mark.parametrize('decision', ['allow', 'deny', 'stop'])
async def test_sdk_approval_uses_real_service_resume_and_stop(
    tmp_path: Path, decision: str,
) -> None:
    store, service, harness, provider, run, cleanup = await application(
        tmp_path / 'approval.sqlite', approval=True)
    try:
        parked = await service.wait(run.id)
        assert parked.status == 'awaiting', parked.code
        assert harness.counts['write'] == 0 and provider.calls == 1
        assert cleanup == []
        pending = await service.approvals(run.id)
        assert len(pending) == 1
        request = pending[0]
        reply = ApprovalReply(request_key='sdk-approval', call_id=request['call_id'],
                              tool_hash=request['tool_hash'],
                              arguments_hash=request['arguments_hash'], allow=decision == 'allow')
        if decision == 'stop':
            await service.cancel(run.id)
            with pytest.raises(ServiceError, match='approval_unavailable'):
                await service.reply_approval(run.id, request['request_id'], reply)
            assert harness.counts['write'] == 0
        else:
            await service.reply_approval(run.id, request['request_id'], reply)
            result = await asyncio.wait_for(service.wait(run.id), 10)
            assert result.status == 'completed', result.code
            assert harness.counts['write'] == (1 if decision == 'allow' else 0)
            assert provider.calls == 2
        assert cleanup == [run.id]
    finally:
        await service.close()
        await store.close()
