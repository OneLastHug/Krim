"""Real SQLite coverage for ordinary, paged session compaction sources."""

from __future__ import annotations

import time
from uuid import uuid4

import pytest
from sqlalchemy import insert, select, update

from krim_agent.config import Settings
from krim_agent.context import builder
from krim_agent.context.serialization import fingerprint
from krim_agent.context.types import (
    CompactionPolicy,
    ContextPolicy,
    OrdinaryContextState,
    PreparedContext,
)
from krim_agent.contracts import Message, ServiceError
from krim_agent.security import LOCAL, Membership, PrincipalContext
from krim_agent.sessions.types import SessionCreate
from krim_agent.storage import sessions as session_sql
from krim_agent.storage.account_models import accounts
from krim_agent.storage.models import run_events, runs, sessions, turns
from krim_agent.storage.session_context import (
    OrdinaryCompactionSource,
    freeze_sources,
    make_frozen,
    state_from_raw,
)
from krim_agent.storage.sqlite import SQLiteStore


def _tool_transcript(turn_id: str) -> list[Message]:
    call_id = "call-" + turn_id
    return [
        Message(role="user", content="question " + turn_id),
        Message(
            role="assistant",
            content="checking",
            tool_calls=[{"id": call_id, "name": "lookup", "arguments": {"key": turn_id}}],
        ),
        Message(role="tool", call_id=call_id, content='{"value":"kept"}'),
        Message(role="assistant", content="answer " + turn_id),
    ]


def _turn_values(
    *,
    turn_id: str,
    session_id: str,
    workspace_id: str,
    owner_id: str,
    request_key: str,
    text: str,
    status: str,
    ordinal: int,
    run_id: str | None,
    transcript: list[Message] | None,
    code: str | None = None,
) -> dict[str, object]:
    now = time.time()
    return {
        "id": turn_id,
        "session_id": session_id,
        "workspace_id": workspace_id,
        "owner_id": owner_id,
        "request_key": request_key,
        "request_hash": fingerprint({"text": text, "request_key": request_key}),
        "text": text,
        "status": status,
        "ordinal": ordinal,
        "accepted_revision": max(0, ordinal - 1),
        "run_id": run_id,
        "context_snapshot": None,
        "context_fingerprint": None,
        "transcript": (
            None
            if transcript is None
            else [message.model_dump(mode="json") for message in transcript]
        ),
        "projection_version": builder.VERSION,
        "code": code,
        "created_at": now,
        "started_at": now if run_id is not None else None,
        "finished_at": now if status not in {"queued", "running"} else None,
    }


async def _create_session(
    store: SQLiteStore,
    actor: PrincipalContext,
    *,
    workspace_id: str,
    target_key: str = "demo",
) -> str:
    policy = ContextPolicy(max_bytes=65536, max_messages=64, reserved_output_tokens=64)
    view = await session_sql.create_session(
        store,
        workspace_id,
        actor.principal_id,
        SessionCreate(request_key="ordinary-source-session-" + uuid4().hex, target_key=target_key),
        {"tools": []},
        policy,
        Settings(),
    )
    return view.id


async def _build_source(
    store: SQLiteStore,
    *,
    actor: PrincipalContext = LOCAL,
    workspace_id: str = "local",
    source_count: int = 2,
    target_key: str = "demo",
    static_actor: bool = False,
) -> tuple[str, str, str, OrdinaryCompactionSource, list[list[Message] | None]]:
    """Insert real rows, then persist schema-4 state as the host admission would."""
    session_id = await _create_session(
        store, actor, workspace_id=workspace_id, target_key=target_key
    )
    owner_id = actor.principal_id
    current_turn_id, current_run_id = str(uuid4()), str(uuid4())
    history_ids = [str(uuid4()) for _ in range(source_count)]
    histories: list[list[Message] | None] = []
    turn_rows: list[dict[str, object]] = []
    run_rows: list[dict[str, object]] = []
    event_rows: list[dict[str, object]] = []
    now = time.time()

    for index, turn_id in enumerate(history_ids):
        if index == 0:
            transcript = _tool_transcript(turn_id)
            status, code, history_run_id = "completed", None, str(uuid4())
        elif index == 1:
            transcript, status, code, history_run_id = None, "cancelled", "cancelled", None
        else:
            transcript = [Message(role="user", content=f"history {index}")]
            status, code, history_run_id = "completed", None, None
        histories.append(transcript)
        turn_rows.append(
            _turn_values(
                turn_id=turn_id,
                session_id=session_id,
                workspace_id=workspace_id,
                owner_id=owner_id,
                request_key=f"history-{index}",
                text=f"history input {index}",
                status=status,
                ordinal=index + 1,
                run_id=history_run_id,
                transcript=transcript,
                code=code,
            )
        )
        if history_run_id is not None:
            key = "ac-turn." + turn_id
            run_rows.append(
                {
                    "id": history_run_id,
                    "request_key": key,
                    "request_hash": "0" * 64,
                    "request": {
                        "text": f"history input {index}",
                        "request_key": key,
                        "target": target_key,
                    },
                    "target": {"tools": []},
                    "status": "completed",
                    "trace_id": uuid4().hex,
                    "root_span_id": uuid4().hex[:16],
                    "created_at": now,
                    "finished_at": now,
                    "code": None,
                    "stop_requested": False,
                }
            )
            event_rows.extend(
                [
                    {
                        "run_id": history_run_id,
                        "seq": 0,
                        "event_id": "start",
                        "kind": "run_started",
                        "data": {},
                        "at": now,
                    },
                    {
                        "run_id": history_run_id,
                        "seq": 7,
                        "event_id": "terminal",
                        "kind": "terminal",
                        "data": {"status": "completed"},
                        "at": now,
                    },
                ]
            )

    current_text = "current request"
    current_key = "ac-turn." + current_turn_id
    run_rows.append(
        {
            "id": current_run_id,
            "request_key": current_key,
            "request_hash": "0" * 64,
            "request": {"text": current_text, "request_key": current_key, "target": target_key},
            "target": {"provider": {"id": "fixture"}, "tools": []},
            "status": "running",
            "trace_id": uuid4().hex,
            "root_span_id": uuid4().hex[:16],
            "created_at": now,
            "finished_at": None,
            "code": None,
            "stop_requested": False,
        }
    )
    turn_rows.append(
        _turn_values(
            turn_id=current_turn_id,
            session_id=session_id,
            workspace_id=workspace_id,
            owner_id=owner_id,
            request_key="current-request",
            text=current_text,
            status="running",
            ordinal=source_count + 1,
            run_id=current_run_id,
            transcript=None,
        )
    )

    async with store.lock, store.engine.begin() as conn:
        if run_rows:
            await conn.execute(insert(runs), run_rows)
        if turn_rows:
            await conn.execute(insert(turns), turn_rows)
        if event_rows:
            await conn.execute(insert(run_events), event_rows)
        sources = await freeze_sources(conn, session_id, current_turn_id)
        policy = ContextPolicy(max_bytes=65536, max_messages=64, reserved_output_tokens=64)
        compaction = CompactionPolicy()
        tools: list[dict] = []
        frozen = make_frozen(
            owner_id=owner_id,
            workspace_id=workspace_id,
            session_id=session_id,
            turn_id=current_turn_id,
            run_id=current_run_id,
            target_key=target_key,
            sources=sources,
            current=Message(role="user", content=current_text),
            system_prompt="ordinary system",
            extensions={"version": "extensions/2", "revision": 0, "items": []},
            policy=policy,
            compaction=compaction,
            tool_fingerprint=fingerprint(tools),
            authorization_generation=(None if actor.legacy or static_actor else actor.generation),
        )
        frozen_hash = fingerprint(frozen.model_dump(mode="json"))
        state = OrdinaryContextState(frozen=frozen).model_dump(mode="json")
        target = {
            "provider": {"id": "fixture"},
            "tools": tools,
            "context": {
                "schema_version": 4,
                "source_fingerprint": frozen.source_fingerprint,
                "frozen_fingerprint": frozen_hash,
            },
        }
        await conn.execute(
            update(turns)
            .where(turns.c.id == current_turn_id)
            .values(context_snapshot=state, context_fingerprint=frozen_hash)
        )
        await conn.execute(update(runs).where(runs.c.id == current_run_id).values(target=target))

    return (
        session_id,
        current_turn_id,
        current_run_id,
        OrdinaryCompactionSource(
            frozen, legacy_actor=actor if actor.legacy or static_actor else None
        ),
        histories,
    )


async def _finish_current(store: SQLiteStore, run_id: str, turn_id: str) -> None:
    now = time.time()
    async with store.lock, store.engine.begin() as conn:
        await conn.execute(
            update(runs).where(runs.c.id == run_id).values(status="completed", finished_at=now)
        )
        await conn.execute(
            update(turns).where(turns.c.id == turn_id).values(status="completed", finished_at=now)
        )
        await conn.execute(
            insert(run_events).values(
                run_id=run_id,
                seq=0,
                event_id="terminal:" + run_id,
                kind="terminal",
                data={"status": "completed"},
                at=now,
            )
        )


@pytest.fixture
async def local_store(tmp_path):
    store = await SQLiteStore.open(tmp_path / "ordinary-context.sqlite")
    try:
        yield store
    finally:
        await store.close()


async def test_freeze_page_tool_groups_and_reopen_prepared_state(local_store, tmp_path):
    store = local_store
    _session_id, turn_id, run_id, source, transcripts = await _build_source(store)
    assert len(source.source_refs) == 2
    assert source.source_refs[0]["terminal_seq"] == 7
    assert source.source_refs[1]["terminal_seq"] == -1
    async with store.lock, store.engine.begin() as conn:
        await source.verify(conn, run_id)
        page = await source.page(conn, run_id, 0, 16)
        assert page[0]["messages"] == transcripts[0]
        assert page[0]["messages"][1].tool_calls == [
            {
                "id": "call-" + source.source_refs[0]["turn_id"],
                "name": "lookup",
                "arguments": {"key": source.source_refs[0]["turn_id"]},
            }
        ]
        assert page[0]["messages"][2].call_id == page[0]["messages"][1].tool_calls[0]["id"]
        assert [item.role for item in page[0]["messages"]] == [
            "user",
            "assistant",
            "tool",
            "assistant",
        ]
        assert page[1]["messages"] == [
            Message(role="user", content="history input 1"),
            Message(role="user", content="[不可信历史状态数据] cancelled: cancelled"),
        ]
        prepared = PreparedContext(
            messages=(
                Message(role="system", content=source.frozen.system_prompt),
                source.frozen.current,
            ),
            sources=source.frozen.sources,
            policy=source.compaction_policy,
            source_fingerprint=source.source_fingerprint,
            summary_hash=None,
            input_bytes=128,
            estimated_tokens=32,
        )
        await source.save_prepared(conn, run_id, prepared)
        assert await source.read_prepared(conn, run_id) == prepared
        raw = (
            (
                await conn.execute(
                    select(turns.c.context_snapshot, turns.c.context_fingerprint).where(
                        turns.c.id == turn_id
                    )
                )
            )
            .mappings()
            .one()
        )
        persisted = state_from_raw(raw["context_snapshot"], raw["context_fingerprint"])
        assert persisted.prepared == prepared
        assert "messages" not in persisted.frozen.model_dump(mode="json")

    # A new Store and source instance must recover from the persisted anchor.
    database = tmp_path / "ordinary-context.sqlite"
    await store.close()
    reopened = await SQLiteStore.open(database)
    try:
        state = await reopened.work_read(lambda conn: _load_state(conn, turn_id))
        resumed = OrdinaryCompactionSource(state.frozen, legacy_actor=LOCAL)
        async with reopened.lock, reopened.engine.begin() as conn:
            await resumed.verify_frozen(conn, run_id)
            assert await resumed.read_prepared(conn, run_id) == prepared
            corrupted = state.model_dump(mode="json")
            corrupted["prepared_hash"] = "0" * 64
            await conn.execute(
                update(turns).where(turns.c.id == turn_id).values(context_snapshot=corrupted)
            )
            with pytest.raises(ServiceError, match="facts_corrupt"):
                await resumed.read_prepared(conn, run_id)
    finally:
        await reopened.close()


async def _load_state(conn, turn_id: str) -> OrdinaryContextState:
    row = (
        (
            await conn.execute(
                select(turns.c.context_snapshot, turns.c.context_fingerprint).where(
                    turns.c.id == turn_id
                )
            )
        )
        .mappings()
        .one()
    )
    return state_from_raw(row["context_snapshot"], row["context_fingerprint"])


@pytest.mark.parametrize("damage", ["transcript", "tool_group", "terminal_seq"])
async def test_source_rejects_history_tampering(local_store, damage):
    store = local_store
    _session_id, _turn_id, run_id, source, _transcripts = await _build_source(store)
    ref = source.source_refs[0]
    async with store.lock, store.engine.begin() as conn:
        if damage == "transcript":
            await conn.execute(
                update(turns)
                .where(turns.c.id == ref["turn_id"])
                .values(
                    transcript=[Message(role="user", content="changed").model_dump(mode="json")]
                )
            )
        elif damage == "tool_group":
            broken = _tool_transcript(str(ref["turn_id"]))[:2]
            await conn.execute(
                update(turns)
                .where(turns.c.id == ref["turn_id"])
                .values(transcript=[message.model_dump(mode="json") for message in broken])
            )
        else:
            await conn.execute(
                update(run_events)
                .where(
                    run_events.c.run_id == ref["run_id"],
                    run_events.c.event_id == "terminal",
                )
                .values(kind="text")
            )
        with pytest.raises(ServiceError, match="facts_corrupt"):
            await source.page(conn, run_id, 0, 16)


async def test_verify_frozen_survives_terminal_but_live_verify_does_not(local_store):
    store = local_store
    _session_id, turn_id, run_id, source, _transcripts = await _build_source(store)
    await _finish_current(store, run_id, turn_id)
    async with store.lock, store.engine.begin() as conn:
        await source.verify_frozen(conn, run_id)
        with pytest.raises(ServiceError, match="permission_denied"):
            await source.verify(conn, run_id)


@pytest.mark.parametrize("drift", ["closed_session", "stopped_run"])
async def test_live_verify_requires_open_session_and_running_unstopped_run(local_store, drift):
    store = local_store
    session_id, _turn_id, run_id, source, _transcripts = await _build_source(store)
    async with store.lock, store.engine.begin() as conn:
        if drift == "closed_session":
            await conn.execute(
                update(sessions).where(sessions.c.id == session_id).values(status="closed")
            )
        else:
            await conn.execute(update(runs).where(runs.c.id == run_id).values(stop_requested=True))
        with pytest.raises(ServiceError, match="permission_denied"):
            await source.verify(conn, run_id)


@pytest.mark.parametrize("legacy", [False, True])
async def test_static_source_requires_explicit_owner_with_workspace_write_access(
    local_store, legacy
):
    store = local_store
    actor = PrincipalContext(
        "alice", 1, (Membership(workspace_id="alpha", role="member"),), legacy=legacy
    )
    _session_id, _turn_id, run_id, source, _transcripts = await _build_source(
        store, actor=actor, workspace_id="alpha", static_actor=True
    )
    assert source.frozen.authorization_generation is None
    denied_actors = [
        (None, "permission_denied"),
        (
            PrincipalContext("intruder", 1, (Membership(workspace_id="alpha", role="admin"),)),
            "permission_denied",
        ),
        (
            PrincipalContext("alice", 1, (Membership(workspace_id="other", role="member"),)),
            "not_found",
        ),
        (
            PrincipalContext("alice", 1, (Membership(workspace_id="alpha", role="viewer"),)),
            "permission_denied",
        ),
    ]
    async with store.lock, store.engine.begin() as conn:
        for supplied_actor, code in denied_actors:
            with pytest.raises(ServiceError, match=code):
                await OrdinaryCompactionSource(source.frozen, legacy_actor=supplied_actor).verify(
                    conn, run_id
                )
        await source.verify(conn, run_id)
        page = await source.page(conn, run_id, 0, 16)
        assert len(page) == 2
        assert [message.role for message in page[0]["messages"]] == [
            "user",
            "assistant",
            "tool",
            "assistant",
        ]


async def test_managed_owner_generation_is_rechecked_on_every_live_verify(local_store):
    from krim_agent.accounts.service import IdentityService

    store = local_store
    identities = await IdentityService.open(store)
    try:
        await identities.bootstrap("ordinary context", request_key="bootstrap-ordinary")
        actor = identities.principals[0]
        workspace = actor.memberships[0].workspace_id
        _session_id, _turn_id, run_id, source, _transcripts = await _build_source(
            store, actor=actor, workspace_id=workspace
        )
        source = OrdinaryCompactionSource(source.frozen, legacy_actor=actor)
        async with store.lock, store.engine.begin() as conn:
            await source.verify(conn, run_id)
            await conn.execute(
                update(accounts)
                .where(accounts.c.principal_id == actor.principal_id)
                .values(authorization_generation=actor.generation + 1)
            )
            with pytest.raises(ServiceError, match="permission_denied"):
                await source.verify(conn, run_id)
    finally:
        await identities.close()


async def test_ordinary_source_rejects_legacy_schema_without_reinterpreting_it(local_store):
    _session, _turn, _run, source, _histories = await _build_source(local_store)
    raw = OrdinaryContextState(frozen=source.frozen).model_dump(mode="json")
    raw["schema_version"] = 1
    with pytest.raises(ServiceError, match="facts_corrupt"):
        state_from_raw(raw, fingerprint(source.frozen.model_dump(mode="json")))
    raw = OrdinaryContextState(frozen=source.frozen).model_dump(mode="json")
    raw["frozen"]["schema_version"] = 1
    # Even an internally consistent hash cannot change a declared schema version.
    with pytest.raises(ServiceError, match="facts_corrupt"):
        state_from_raw(raw, fingerprint(raw["frozen"]))


@pytest.mark.parametrize("path", ["state", "frozen"])
async def test_ordinary_source_requires_explicit_persisted_schema_version(local_store, path):
    _session, _turn, _run, source, _histories = await _build_source(local_store)
    raw = OrdinaryContextState(frozen=source.frozen).model_dump(mode="json")
    document = raw if path == "state" else raw["frozen"]
    del document["schema_version"]
    with pytest.raises(ServiceError, match="facts_corrupt"):
        state_from_raw(raw, fingerprint(raw["frozen"]))


async def test_freeze_preserves_more_than_1024_source_turns(local_store):
    store = local_store
    _session, _turn, run_id, source, _histories = await _build_source(store, source_count=1025)
    assert len(source.source_refs) == 1025
    async with store.lock, store.engine.connect() as conn:
        await source.verify(conn, run_id)
        last_page = await source.page(conn, run_id, 1024, 16)
    assert len(last_page) == 1
    assert last_page[0]["ref"] == source.source_refs[-1]
