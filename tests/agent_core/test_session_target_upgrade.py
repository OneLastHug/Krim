"""新用户turn边界的实现摘要升级：白名单差异、busy栅栏、重放不升级。"""

import copy
import json
import time
from uuid import uuid4

from sqlalchemy import insert, select, update

from krim_agent.contracts import ServiceError
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.interaction_models import interaction_requests
from krim_agent.storage.models import runs, sessions, turns


def _stale_implementation(current: dict) -> dict:
    """把现行描述符回写成旧代码的实现摘要值；外部契约字段保持一致。"""
    stale = copy.deepcopy(current)
    stale["pre"]["digest"] = "0" * 64
    stale["post"]["digest"] = "1" * 64
    stale["policy"]["digest"] = "2" * 64
    stale["provider"]["digest"] = "3" * 64
    stale["provider"]["implementation_digest"] = "4" * 64
    stale["execution_kernel"]["digest"] = "5" * 64
    for index, tool in enumerate(stale["tools"]):
        tool["implementation_digest"] = str(index) * 64
    return stale


async def _freeze_target(app, session_id: str, target: dict) -> None:
    async with app.store.lock, app.store.engine.begin() as conn:
        await conn.execute(
            update(sessions).where(sessions.c.id == session_id).values(target=target)
        )


async def _session_target(app, session_id: str) -> dict:
    async with app.store.lock, app.store.engine.begin() as conn:
        return (
            await conn.execute(select(sessions.c.target).where(sessions.c.id == session_id))
        ).scalar_one()


async def _run_target(app, run_id: str) -> dict:
    async with app.store.lock, app.store.engine.begin() as conn:
        return (
            await conn.execute(select(runs.c.target).where(runs.c.id == run_id))
        ).scalar_one()


async def test_implementation_drift_upgrades_at_new_user_turn(tmp_path, wire):
    database = tmp_path / "upgrade.sqlite"
    async with wire.application(database) as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="s"))
        first = await app.sessions.submit(
            LOCAL, "local", session.id, TurnRequest(
                request_key="t1", text="/add 2 3", expected_revision=0,
            )
        )
        first = await app.sessions.wait(LOCAL, "local", session.id, first.id)
        assert first.status == "completed"
        current = json.loads(app.service.bindings["target-http"].descriptor_json)
        stale = _stale_implementation(current)
        await _freeze_target(app, session.id, stale)
        run_target_before = await _run_target(app, first.run_id)

        second = await app.sessions.submit(
            LOCAL, "local", session.id, TurnRequest(
                request_key="t2", text="hi", expected_revision=1,
            )
        )
        second = await app.sessions.wait(LOCAL, "local", session.id, second.id)
        assert second.status == "completed"
        stored = await _session_target(app, session.id)
        assert stored == current
        assert await _run_target(app, first.run_id) == run_target_before
        # /add工具回合各产生两次模型请求（tool_calls往返）
        assert len(wire.requests) == 3


async def test_contract_drift_keeps_frozen_and_fails_dispatch(tmp_path, wire):
    database = tmp_path / "contract.sqlite"
    async with wire.application(database) as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="s"))
        first = await app.sessions.submit(
            LOCAL, "local", session.id, TurnRequest(
                request_key="t1", text="/add 2 3", expected_revision=0,
            )
        )
        first = await app.sessions.wait(LOCAL, "local", session.id, first.id)
        assert first.status == "completed"
        current = json.loads(app.service.bindings["target-http"].descriptor_json)
        stale = copy.deepcopy(current)
        stale["system_prompt"] = "旧系统提示"
        await _freeze_target(app, session.id, stale)

        second = await app.sessions.submit(
            LOCAL, "local", session.id, TurnRequest(
                request_key="t2", text="hi", expected_revision=1,
            )
        )
        second = await app.sessions.wait(LOCAL, "local", session.id, second.id)
        assert second.status == "failed" and second.code == "target_binding_changed"
        assert await _session_target(app, session.id) == stale
        assert len(wire.requests) == 2


async def test_replay_and_busy_fence_never_upgrade(tmp_path, wire):
    database = tmp_path / "fence.sqlite"
    async with wire.application(database) as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="s"))
        first = await app.sessions.submit(
            LOCAL, "local", session.id, TurnRequest(
                request_key="t1", text="/add 2 3", expected_revision=0,
            )
        )
        first = await app.sessions.wait(LOCAL, "local", session.id, first.id)
        assert first.status == "completed"
        current = json.loads(app.service.bindings["target-http"].descriptor_json)
        stale = _stale_implementation(current)
        await _freeze_target(app, session.id, stale)

        replayed = await app.sessions.submit(
            LOCAL, "local", session.id, TurnRequest(
                request_key="t1", text="/add 2 3", expected_revision=0,
            )
        )
        assert replayed.id == first.id
        assert await _session_target(app, session.id) == stale

        async with app.store.lock, app.store.engine.begin() as conn:
            await conn.execute(
                update(sessions).where(sessions.c.id == session.id).values(dispatch_paused=True)
            )
            await conn.execute(insert(turns).values(
                id=str(uuid4()), session_id=session.id, workspace_id=session.workspace_id,
                owner_id=session.owner_id, request_key="held", request_hash="0" * 64,
                text="held", status="queued", ordinal=99, accepted_revision=0, run_id=None,
                context_snapshot=None, context_fingerprint=None, transcript=None,
                projection_version="test", code=None, created_at=time.time(),
                started_at=None, finished_at=None,
            ))
        second = await app.sessions.submit(
            LOCAL, "local", session.id, TurnRequest(
                request_key="t2", text="hi", expected_revision=1,
            )
        )
        assert second.status == "queued"
        assert await _session_target(app, session.id) == stale

        async with app.store.lock, app.store.engine.begin() as conn:
            await conn.execute(insert(interaction_requests).values(
                id=str(uuid4()), session_id=session.id, workspace_id=session.workspace_id,
                owner_id=session.owner_id, turn_id=str(uuid4()), run_id=str(uuid4()),
                request_key="held-approval", kind="approval", status="pending",
                revision=0, mode_revision=0, payload={}, created_at=time.time(),
                updated_at=time.time(),
            ))
        third = await app.sessions.submit(
            LOCAL, "local", session.id, TurnRequest(
                request_key="t3", text="hi", expected_revision=2,
            )
        )
        assert third.status == "queued"
        assert await _session_target(app, session.id) == stale


def test_whitelist_rejects_config_and_contract_digests():
    current = {
        "schema_version": 2,
        "key": "target-development",
        "version": "dev/1",
        "provider": {"id": "p", "name": "p", "behavior_version": "http-sse/1",
                     "implementation_digest": "a", "digest": "a", "config": {"model": "m"}},
        "system_prompt": "sp",
        "prompt_fingerprint": "f1",
        "tools": [{"name": "add"}],
        "policy": {"id": "k", "version": "1", "digest": "b"},
        "pre": {"id": "pre", "version": "1", "digest": "c"},
        "post": {"id": "post", "version": "1", "digest": "d"},
        "limits": {"max_request_bytes": 4096},
        "limits_fingerprint": "f2",
        "agent_loop": "ai-python/0.9.0-shared-host/1",
        "execution_kernel": {"version": "sdk-execution/1", "digest": "e"},
        "development": {"version": "dev-runtime/1", "image_id": "img",
                        "config_fingerprint": "f3"},
    }

    def drifted(**changes):
        stale = copy.deepcopy(current)
        for dotted, value in changes.items():
            section, field = dotted.split("__", 1)
            if field == "key":
                stale[section] = value
            else:
                stale[section][field] = value
        return stale

    assert session_sql.implementation_only_upgrade(
        drifted(provider__digest="x", provider__implementation_digest="y",
                policy__digest="x", pre__digest="x", post__digest="x",
                execution_kernel__digest="x"),
        current,
    )
    assert not session_sql.implementation_only_upgrade(
        drifted(policy__id="policy-moved"), current
    )
    assert not session_sql.implementation_only_upgrade(
        drifted(provider__config={"model": "other"}), current
    )
    assert not session_sql.implementation_only_upgrade(
        drifted(provider__behavior_version="http-sse/2"), current
    )
    assert not session_sql.implementation_only_upgrade(
        drifted(tools__key=[{"name": "add", "extra": 1}]), current
    )
    assert not session_sql.implementation_only_upgrade(
        drifted(limits__key={"max_request_bytes": 8192}), current
    )
    assert not session_sql.implementation_only_upgrade(
        drifted(development__key={"version": "dev-runtime/2", "image_id": "img",
                                  "config_fingerprint": "f3"}),
        current,
    )
    assert not session_sql.implementation_only_upgrade(
        drifted(execution_kernel__version="sdk-execution/2"), current
    )
    assert not session_sql.implementation_only_upgrade(
        drifted(pre__id="pre-moved"), current
    )
    assert not session_sql.implementation_only_upgrade(
        {**current, "surprise": 1}, current
    )
    assert not session_sql.implementation_only_upgrade(
        current, {**current, "surprise": 1}
    )
    assert not session_sql.implementation_only_upgrade(
        drifted(provider__extra_digest="z"), current
    )

    native = {"name": "add", "version": "1", "implementation_digest": "g",
              "concurrency": "one", "input_schema": {}, "output_schema": {}}
    external = dict(native, source={"kind": "mcp"}, policy_version="external/1")
    tools_current = {**current, "tools": [native, external]}
    assert session_sql.implementation_only_upgrade(
        {**tools_current, "tools": [dict(native, implementation_digest="h"), external]},
        tools_current,
    )
    assert not session_sql.implementation_only_upgrade(
        {**tools_current, "tools": [native, dict(external, implementation_digest="h")]},
        tools_current,
    )
    assert not session_sql.implementation_only_upgrade(
        {**tools_current, "tools": [dict(native, input_schema={"type": "object"}), external]},
        tools_current,
    )
    assert not session_sql.implementation_only_upgrade(
        {**tools_current, "tools": [dict(native, name="renamed"), external]},
        tools_current,
    )
    assert not session_sql.implementation_only_upgrade(
        {**tools_current, "tools": [native]},
        tools_current,
    )
    assert not session_sql.implementation_only_upgrade(
        {**tools_current, "tools": [external, native]},
        tools_current,
    )


async def test_submit_upgrade_is_atomic_with_turn_insert(tmp_path, wire):
    """升级与新turn同事务：revision冲突拒绝时不产生任何目标改写。"""
    database = tmp_path / "atomic.sqlite"
    async with wire.application(database) as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="s"))
        current = json.loads(app.service.bindings["target-http"].descriptor_json)
        stale = _stale_implementation(current)
        await _freeze_target(app, session.id, stale)
        try:
            await session_sql.submit(
                app.store, session.id, TurnRequest(
                    request_key="t1", text="/add 2 3", expected_revision=7,
                ), app.settings, upgrade_target=current,
            )
        except ServiceError as error:
            assert error.code == "revision_conflict"
        else:
            raise AssertionError("revision_conflict expected")
        assert await _session_target(app, session.id) == stale
