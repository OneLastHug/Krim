"""Real ordinary-turn admission/source persistence and explicit schema compatibility."""

from __future__ import annotations

import pytest
from sqlalchemy import select, update

from krim_agent.accounts.service import IdentityService
from krim_agent.config import Settings
from krim_agent.context.serialization import encoded, fingerprint
from krim_agent.context.types import (
    ContextPolicy,
    OrdinarySessionContext,
    SessionContext,
)
from krim_agent.contracts import Message, RunRequest, ServiceError
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.account_models import accounts
from krim_agent.storage.models import runs, turns
from krim_agent.storage.session_context import OrdinaryCompactionSource
from krim_agent.storage.sqlite import SQLiteStore
from tests.coding_agent.test_ordinary_context_source import _build_source


@pytest.mark.parametrize(
    "target, expected",
    [
        ({"limits": {"max_request_bytes": 900000}}, 900000),
        ({"limits": {"max_request_bytes": None, "http": {"max_request_bytes": 750000}}}, 750000),
        ({"limits": {"max_request_bytes": 900000, "http": {"max_request_bytes": 750000}}}, 750000),
        ({"tools": []}, ContextPolicy().max_bytes),
    ],
)
def test_request_capacity_uses_frozen_target_declarations(target, expected):
    policy = ContextPolicy(max_bytes=1000000, reserved_output_tokens=1)
    config = Settings(max_request_bytes=None)
    assert session_sql.request_limit(config, policy, target) == expected
    assert (
        session_sql.request_limit(
            config.model_copy(update={"max_request_bytes": 600000}), policy, target
        )
        <= 600000
    )


def test_request_capacity_rejects_invalid_frozen_limit():
    with pytest.raises(ServiceError, match="facts_corrupt"):
        session_sql.request_limit(
            Settings(), ContextPolicy(), {"limits": {"max_request_bytes": "unlimited"}}
        )


@pytest.fixture
async def store(tmp_path):
    value = await SQLiteStore.open(tmp_path / "ordinary-admission.sqlite")
    try:
        yield value
    finally:
        await value.close()


def settings(*, compact=True):
    return Settings(
        history_compaction=compact, context_max_bytes=10000, context_reserved_output_tokens=1
    )


async def create(store, *, owner="local", workspace="local", config=None):
    config = config or settings()
    target = {"provider": {"name": "ordinary-test"}, "tools": []}
    view = await session_sql.create_session(
        store,
        workspace,
        owner,
        SessionCreate(request_key="session", target_key="demo"),
        target,
        ContextPolicy(max_bytes=10000, reserved_output_tokens=1),
        config,
    )
    return view, target, config


async def start(store, session, target, config, *, revision=0, text="current", key="current"):
    turn = await session_sql.submit(
        store,
        session.id,
        TurnRequest(text=text, request_key=key, expected_revision=revision),
        config,
    )
    request = RunRequest(text=text, request_key="ac-turn." + turn.id, target=session.target_key)
    run, created = await store.begin_turn_run(turn.id, request, target, config)
    assert created
    return turn, run


async def test_ordinary_default_admission_uses_explicit_schema4_and_is_idempotent(store):
    session, target, config = await create(store)
    turn, run = await start(store, session, target, config)
    snap = await store.snapshot(run.id)
    assert isinstance(snap.session_context, OrdinarySessionContext)
    frozen = snap.session_context.frozen
    assert frozen.current.content == "current" and frozen.sources == ()
    assert frozen.authorization_generation is None
    assert frozen.target_key == "demo"
    async with store.engine.connect() as conn:
        raw = (await conn.execute(select(turns).where(turns.c.id == turn.id))).mappings().one()
    assert raw["context_snapshot"]["schema_version"] == 4
    assert raw["context_snapshot"]["frozen"]["schema_version"] == 4
    assert "messages" not in raw["context_snapshot"]["frozen"]
    assert raw["context_fingerprint"] == fingerprint(frozen.model_dump(mode="json"))
    assert snap.target["context"] == {
        "schema_version": 4,
        "source_fingerprint": frozen.source_fingerprint,
        "frozen_fingerprint": raw["context_fingerprint"],
    }
    second, created = await store.begin_turn_run(turn.id, snap.request, target, config)
    assert not created and second.id == run.id
    await store.set_running(run.id)
    source = OrdinaryCompactionSource(frozen, legacy_actor=LOCAL)
    await store.work_read(lambda conn: source.verify(conn, run.id))


async def test_ordinary_disabled_snapshot_remains_v1_after_default_is_enabled(store):
    session, target, config = await create(store, config=settings(compact=False))
    turn, run = await start(store, session, target, config)
    original = await store.snapshot(run.id)
    assert isinstance(original.session_context, SessionContext)
    assert original.target["context"]["schema_version"] == 1
    await store.set_running(run.id)
    await store.finish(run.id, "completed")
    enabled = settings()
    second, current = await start(
        store, session, target, enabled, revision=1, text="continue", key="second"
    )
    snap = await store.snapshot(current.id)
    assert isinstance(snap.session_context, OrdinarySessionContext)
    assert len(snap.session_context.frozen.sources) == 1
    reread = await store.snapshot(run.id)
    assert isinstance(reread.session_context, SessionContext)
    assert reread.session_context.context == original.session_context.context


async def test_ordinary_long_history_admitted_without_copying_transcripts(store):
    _session, current_turn, current_run, source, _history = await _build_source(
        store,
        source_count=1025,
    )
    # The fixture current run is only to create 1025 immutable historical turns;
    # finish it before submitting through the real admission transaction.
    await store.finish(current_run, "completed")
    async with store.engine.begin() as conn:
        await conn.execute(
            update(turns)
            .where(turns.c.id != current_turn)
            .values(
                transcript=[Message(role="user", content="x" * 2500).model_dump(mode="json")],
            )
        )
    session = await session_sql.get_session(store, source.session_id)
    target = {"tools": []}
    turn, run = await start(store, session, target, settings(), revision=0)
    snap = await store.snapshot(run.id)
    assert isinstance(snap.session_context, OrdinarySessionContext)
    assert len(snap.session_context.frozen.sources) == 1026
    async with store.engine.connect() as conn:
        raw = await conn.scalar(select(turns.c.context_snapshot).where(turns.c.id == turn.id))
    assert "messages" not in raw["frozen"] and len(str(raw)) < 400000


async def test_unbounded_submission_and_explicit_legacy_limits_preserve_utf8_bytes(store):
    session, _target, config = await create(store)
    first = await session_sql.submit(
        store,
        session.id,
        TurnRequest(text="汉字", request_key="first", expected_revision=0),
        config,
    )
    assert first.ordinal == 1
    limited = config.model_copy(update={"session_max_history_bytes": 7})
    with pytest.raises(ServiceError, match="input_limit"):
        await session_sql.submit(
            store,
            session.id,
            TurnRequest(text="文", request_key="too-large", expected_revision=1),
            limited,
        )
    capped_queue = config.model_copy(update={"session_max_queued": 1})
    with pytest.raises(ServiceError, match="capacity"):
        await session_sql.submit(
            store,
            session.id,
            TurnRequest(text="go", request_key="queue", expected_revision=1),
            capped_queue,
        )
    second = await session_sql.submit(
        store,
        session.id,
        TurnRequest(text="文", request_key="unlimited", expected_revision=1),
        config,
    )
    assert second.ordinal == 2


async def test_ordinary_incomplete_predecessor_cannot_be_frozen_as_history(store):
    session, target, config = await create(store)
    await session_sql.submit(
        store,
        session.id,
        TurnRequest(text="pending", request_key="pending", expected_revision=0),
        config,
    )
    second = await session_sql.submit(
        store, session.id, TurnRequest(text="next", request_key="next", expected_revision=1), config
    )
    with pytest.raises(ServiceError, match="capacity"):
        await store.begin_turn_run(
            second.id,
            RunRequest(text="next", request_key="ac-turn." + second.id, target="demo"),
            target,
            config,
        )
    assert (await session_sql.get_turn(store, second.id)).run_id is None


async def test_ordinary_admission_rejects_current_input_over_real_model_capacity(store):
    session, target, config = await create(store)
    turn = await session_sql.submit(
        store,
        session.id,
        TurnRequest(text="x" * 12000, request_key="too-large", expected_revision=0),
        config,
    )
    request = RunRequest(text=turn.text, request_key="ac-turn." + turn.id, target="demo")
    with pytest.raises(ServiceError, match="context_budget"):
        await store.begin_turn_run(turn.id, request, target, config)
    assert (await session_sql.get_turn(store, turn.id)).run_id is None
    assert not (await store.work_read(lambda conn: conn.scalar(select(runs.c.id))))


async def test_ordinary_managed_owner_generation_is_frozen_and_revocation_refused(store):
    identity = await IdentityService.open(store)
    try:
        bootstrap = await identity.bootstrap("ordinary", request_key="bootstrap")
        actor = identity.authenticate("Bearer " + str(bootstrap.token))
        session, target, config = await create(
            store, owner=actor.principal_id, workspace=actor.memberships[0].workspace_id
        )
        turn, run = await start(store, session, target, config)
        snap = await store.snapshot(run.id)
        assert snap.session_context.frozen.authorization_generation == actor.generation
        await store.set_running(run.id)
        await store.finish(run.id, "completed")
        async with store.engine.begin() as conn:
            await conn.execute(
                update(accounts)
                .where(accounts.c.principal_id == actor.principal_id)
                .values(status="disabled")
            )
        pending = await session_sql.submit(
            store,
            session.id,
            TurnRequest(text="next", request_key="next", expected_revision=1),
            config,
        )
        with pytest.raises(ServiceError, match="permission_denied"):
            await store.begin_turn_run(
                pending.id,
                RunRequest(text="next", request_key="ac-turn." + pending.id, target="demo"),
                target,
                config,
            )
        assert (await session_sql.get_turn(store, pending.id)).run_id is None
    finally:
        await identity.close()


@pytest.mark.parametrize("damage", ["descriptor-version", "frozen-version", "fingerprint"])
async def test_ordinary_snapshot_corruption_never_falls_back_to_v1(store, damage):
    session, target, config = await create(store)
    turn, run = await start(store, session, target, config)
    async with store.engine.begin() as conn:
        if damage == "descriptor-version":
            raw = await conn.scalar(select(runs.c.target).where(runs.c.id == run.id))
            raw["context"]["schema_version"] = 1
            await conn.execute(update(runs).where(runs.c.id == run.id).values(target=raw))
        else:
            raw = await conn.scalar(select(turns.c.context_snapshot).where(turns.c.id == turn.id))
            if damage == "frozen-version":
                raw["frozen"]["schema_version"] = 1
            else:
                raw["frozen"]["current"]["content"] = "tampered"
            await conn.execute(
                update(turns).where(turns.c.id == turn.id).values(context_snapshot=raw)
            )
    with pytest.raises(ServiceError, match="facts_corrupt"):
        await store.snapshot(run.id)


@pytest.mark.parametrize("marker", ["purpose", "legacy-start", "legacy-text-only"])
async def test_ordinary_finish_transcript_filters_summary_model_end_without_purpose(store, marker):
    session, target, config = await create(store)
    turn, run = await start(store, session, target, config)
    await store.set_running(run.id)
    await store.append(
        run.id,
        "model_start",
        {"step_index": 0, **({"purpose": "summary"} if marker != "legacy-text-only" else {})},
    )
    if marker != "purpose":
        await store.append(run.id, "summary_text", {"step_index": 0, "text": "SUMMARY_SENTINEL"})
    await store.append(
        run.id,
        "text",
        {
            "step_index": 0,
            "text": "SUMMARY_SENTINEL",
            **({"purpose": "summary"} if marker == "purpose" else {}),
        },
    )
    await store.append(
        run.id,
        "model_end",
        {
            "step_index": 0,
            "tool_calls": [],
            **({"purpose": "summary"} if marker == "purpose" else {}),
        },
    )
    await store.append(run.id, "model_start", {"step_index": 1, "purpose": "task"})
    await store.append(run.id, "text", {"step_index": 1, "text": "final answer"})
    await store.append(run.id, "model_end", {"step_index": 1, "tool_calls": []})
    await store.finish(run.id, "completed")
    transcript = await store.work_read(
        lambda conn: conn.scalar(select(turns.c.transcript).where(turns.c.id == turn.id))
    )
    assert [Message.model_validate_json(encoded(item)) for item in transcript] == [
        Message(role="user", content="current"),
        Message(role="assistant", content="final answer"),
    ]
    snap = await store.snapshot(run.id)
    assert isinstance(snap.session_context, OrdinarySessionContext)
