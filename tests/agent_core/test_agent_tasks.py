"""D4终态/核验的真实Store消费者；模型文字不能替代业务Proof。"""

import asyncio
import base64
import json
from typing import Any, cast

import pytest
from fixtures.workspace_server import WorkspaceWire, workspace_application
from test_tool_pipeline import approve_fixture, manual_turn
from test_tool_pipeline import data_case as data_case

from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import ServiceError
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate
from krim_agent.workspace.types import Completion, CompletionCondition, WorkspaceTurnRequest


@pytest.fixture(scope="module", autouse=True)
def preserve_final_failure_databases() -> None:
    """只在原件的逐字节副本上打开mode=ro并一致backup；旧失败库不recover。"""
    import hashlib
    import os
    import sqlite3
    import stat
    from contextlib import closing
    from pathlib import Path

    root = Path(os.environ["KRIM_D4_ROOT"])
    index = root / "evidence/d4-final-failure-original-copies/index.json"
    if not index.exists():
        return
    records = json.loads(index.read_bytes())
    out = Path(os.environ["KRIM_D4_EVIDENCE"]) / "prior-final-failures"
    out.mkdir()
    completed = []
    for position, record in enumerate(records):
        source = Path(record["shadow_database"])
        assert stat.S_ISREG(source.lstat().st_mode)
        backup = out / (str(position) + ".sqlite")
        with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)) as reader:
            with closing(sqlite3.connect(backup)) as writer, writer:
                reader.backup(writer)
        for file in record["files"]:
            original = Path(file["source"])
            assert stat.S_ISREG(original.lstat().st_mode)
            assert hashlib.sha256(original.read_bytes()).hexdigest() == file["sha256"]
        completed.append(
            record
            | {
                "readonly_backup": str(backup),
                "recover_calls": 0,
                "backup_sha256": hashlib.sha256(backup.read_bytes()).hexdigest(),
            }
        )
    (out / "index.json").write_text(json.dumps(completed, ensure_ascii=False, indent=2) + "\n")


@pytest.mark.parametrize(
    "case",
    [
        "pass-next-turn",
        "missing-artifact",
        "zero-tests",
        "nonzero-exit",
        "unknown-receipt",
        "unclean-receipt",
        "condition-drift",
        "after-proof-drift",
        "terminal-sql-drift",
        "cancel-verifier",
        "repeat-close-terminal",
        "commit-ack-unknown",
        "restart-pending",
    ],
)
async def test_long_chat_to_sandbox_artifact_and_next_turn(
    data_case: Any, tmp_path: Any, monkeypatch: Any, case: Any
) -> Any:
    if case == "unknown-receipt":
        await observation_fault_cases(data_case, tmp_path, monkeypatch)
        return
    if case == "unclean-receipt":
        await unclean_command_case(data_case, tmp_path, monkeypatch)
        return
    if case == "cancel-verifier":
        await cancel_verifier_case(data_case, tmp_path, monkeypatch)
        return
    if case == "commit-ack-unknown":
        await commit_ack_case(data_case, tmp_path, monkeypatch)
        return
    if case == "restart-pending":
        await restart_case(data_case, tmp_path)
        return
    holder: dict[str, Any] = {}
    app, wire, provider = await workspace_application(data_case, tmp_path, wire=WorkspaceWire(case))
    holder["app"] = app
    original_profile = app.work.profile
    notes = data_case[1]
    if case != "repeat-close-terminal":
        cast(Any, approve_fixture)(app, monkeypatch)
        if case == "condition-drift":
            actual_append = app.store.append

            async def appended(run_id: Any, kind: Any, data: Any, **kwargs: Any) -> Any:
                value = await actual_append(run_id, kind, data, **kwargs)
                if kind == "tool_end" and data.get("name") == "command_run":
                    holder["timeline"].append("actual-command-tool_end-returned-before-fence")
                    changed = original_profile.model_dump(mode="json")
                    changed["runtime"]["approval_timeout_seconds"] = 29.0
                    monkeypatch.setattr(
                        app.work,
                        "profile",
                        type(original_profile).model_validate_json(json.dumps(changed)),
                    )
                return value

            monkeypatch.setattr(app.store, "append", appended)
        session = await app.sessions.create(
            LOCAL,
            "local",
            SessionCreate(
                request_key="main-session",
                target_key="target-workspace",
                context_policy=ContextPolicy(
                    max_bytes=18000, max_messages=32, reserved_output_tokens=512
                ),
            ),
        )
        previous: list[Any] = []
        if case == "pass-next-turn":
            for index in range(8):
                history_request = WorkspaceTurnRequest(
                    request_key="history-" + str(index),
                    text="SENTINEL_" + str(index) + "=" + str(index + 11) + " " + "x" * 1500,
                    expected_revision=index,
                    completion=Completion(),
                )
                old = await app.sessions.submit(LOCAL, "local", session.id, history_request)
                previous.append(await app.sessions.wait(LOCAL, "local", session.id, old.id))
        binding = app.service.bindings["target-workspace"]
        verifier = binding.loop._verifier
        original_verify = verifier.verify
        replaced: list[Any] = []

        async def rejected(*args: Any, **kwargs: Any) -> Any:
            replaced.append("replacement-entered")
            raise ServiceError("target_binding_changed")

        timeline: list[Any] = []
        holder["timeline"] = timeline
        original_finish = app.store.finish

        async def finish(run_id: Any, status: Any, code: Any = None) -> Any:
            if status == "completed" and case == "after-proof-drift":
                timeline.append("engine-returned-before-original-finish")
                monkeypatch.setattr(verifier, "verify", rejected)
            return await original_finish(run_id, status, code)

        if case == "after-proof-drift":
            monkeypatch.setattr(app.store, "finish", finish)
        if case == "terminal-sql-drift":
            from sqlalchemy.ext.asyncio import AsyncConnection

            execute = AsyncConnection.execute

            async def query(conn: Any, statement: Any, *args: Any, **kwargs: Any) -> Any:
                value = await execute(conn, statement, *args, **kwargs)
                fields = {
                    getattr(key, "name", key): getattr(value, "value", None)
                    for key, value in getattr(statement, "_values", {}).items()
                }
                verification_value = fields.get("verification")
                if (
                    not timeline
                    and getattr(statement, "is_update", False)
                    and getattr(getattr(statement, "table", None), "name", None) == "work_turns"
                    and isinstance(verification_value, dict)
                    and verification_value["state"] == "passed"
                ):
                    timeline.append(
                        {
                            "window": "actual-verification-UPDATE-returned",
                            "connection": id(conn),
                            "in_transaction": conn.in_transaction(),
                        }
                    )
                    monkeypatch.setattr(verifier, "verify", rejected)
                return value

            monkeypatch.setattr(AsyncConnection, "execute", query)
        request = WorkspaceTurnRequest(
            request_key="main",
            text="D4_TASK",
            expected_revision=len(previous),
            completion=Completion(
                required_paths=(
                    "missing.json" if case == "missing-artifact" else "calc.py",
                    "answer.json",
                ),
                conditions=(
                    CompletionCondition(
                        id="answer",
                        kind="json_equal",
                        path="answer.json",
                        pointer="/answer",
                        expected=42,
                    ),
                    CompletionCondition(
                        id="function", kind="ast_symbols", path="calc.py", symbols=("add",)
                    ),
                ),
            ),
        )
        turn = await app.sessions.submit(LOCAL, "local", session.id, request)
        finished = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
        snapshot = await app.store.snapshot(finished.run_id)
        notes["main"] = {
            "run_id": finished.run_id,
            "turn_id": turn.id,
            "request": request.model_dump(mode="json"),
            "status": finished.status,
            "code": finished.code,
            "events": [event.model_dump(mode="json") for event in snapshot.events],
        }
        from krim_agent.sdk.workspace import WorkspaceSDK

        sdk = WorkspaceSDK(app)
        view = await sdk.get(session.id, turn.id)
        notes["main"]["view"] = view.model_dump(mode="json")
        notes["main"]["timeline"] = timeline
        notes["main"]["replacement_entries"] = replaced
        if case != "pass-next-turn":
            assert finished.status == "failed", finished.model_dump_json()
            assert finished.code == (
                "target_binding_changed"
                if case in {"condition-drift", "after-proof-drift", "terminal-sql-drift"}
                else "completion_unverified"
            )
            assert view.verification is not None and view.verification.state != "passed"
            assert replaced == []
            if case in {"condition-drift", "after-proof-drift", "terminal-sql-drift"}:
                assert timeline
            monkeypatch.setattr(verifier, "verify", original_verify)
            if case == "condition-drift":
                monkeypatch.setattr(app.work, "profile", original_profile)
            if case == "terminal-sql-drift":
                terminals = [event for event in snapshot.events if event.kind == "terminal"]
                assert len(terminals) == 1
                assert terminals[0].data == {"status": "failed", "code": "target_binding_changed"}
            command = [effect for effect in view.effects if effect.kind == "command"]
            assert len(command) == 1
            assert command[0].cleanup == "clean" and command[0].exit_code is not None
            if case == "terminal-sql-drift":
                await late_run_deadlines_case(app, notes, monkeypatch)
            return
        assert finished.status == "completed", finished.model_dump_json()
        assert finished.code is None
        assert view.verification is not None
        assert view.verification.state == "passed"
        assert view.compactions and all(item.status == "completed" for item in view.compactions)
        assert any(
            event.kind == "model_start" and event.data.get("purpose") == "summary"
            for event in snapshot.events
        )
        assert len(view.effects) == 3
        command = [effect for effect in view.effects if effect.kind == "command"]
        assert len(command) == 1
        assert command[0].exit_code == 0 and command[0].cleanup == "clean"
        revision_id = view.verification.revision_id
        assert revision_id is not None
        artifact = await sdk.artifact(session.id, revision_id)
        assert artifact.producer_run_id == finished.run_id
        assert artifact.producer_call_id == command[0].call_id
        assert (await sdk.by_key(session.id, request.request_key)).turn_id == turn.id
        assert provider.calls == len(wire.requests)
        assert len([event for event in snapshot.events if event.kind == "workspace_started"]) == 1
        assert wire.summaries and all("tools" not in item["request"] for item in wire.summaries)
        assert all("SENTINEL_" in item["answer"] for item in wire.summaries)
        assert [
            await app.sessions.turn(LOCAL, "local", session.id, old.id) for old in previous
        ] == previous
        count = len(wire.requests)
        summary_count = len(wire.summaries)
        follow = await app.sessions.submit(
            LOCAL,
            "local",
            session.id,
            WorkspaceTurnRequest(
                request_key="next",
                text="D4_NEXT " + revision_id,
                expected_revision=9,
                completion=Completion(),
            ),
        )
        follow = await app.sessions.wait(LOCAL, "local", session.id, follow.id)
        facts = await app.service.result(follow.run_id)
        assert follow.code == "completion_unverified"
        assert len(facts.tools) == 1 and facts.tools[0].name == "artifact_read"
        assert facts.tools[0].executor_started is True
        assert json.loads(base64.b64decode(facts.tools[0].value["data"])) == {"answer": 42}
        assert facts.tools[0].value["revision_id"] == revision_id
        assert len(wire.requests) == count + 2 + len(wire.summaries) - summary_count
        notes["main"]["next_turn"] = {
            "run_id": follow.run_id,
            "facts": facts.model_dump(mode="json"),
            "summary_requests": wire.summaries,
        }
        return
    run, binding, access = await cast(Any, manual_turn)(app, notes, monkeypatch, age=181)
    # 无Proof的非终态completed必须经原Writer失败关闭，而不是先写passed。
    finished = await app.store.finish(run.id, "completed")
    assert finished.status == "failed"
    assert finished.code == "completion_unverified"
    snapshot = await app.store.snapshot(run.id)
    terminal = [event for event in snapshot.events if event.kind == "terminal"]
    assert len(terminal) == 1
    assert terminal[0].data == {"status": "failed", "code": "completion_unverified"}
    assert provider.calls == 0
    assert wire.requests == []
    original = finished.model_dump(mode="json")
    entries: list[Any] = []

    async def terminal_rejected(*args: Any, **kwargs: Any) -> Any:
        entries.append("must-not-reverify-terminal")
        raise ServiceError("target_binding_changed")

    monkeypatch.setattr(binding.loop._verifier, "verify", terminal_rejected)
    for status, code in [("completed", None), ("aborted", "cancelled"), ("failed", "run_timeout")]:
        assert (await app.store.finish(run.id, status, code)).model_dump(mode="json") == original
    assert entries == [] and provider.calls == 0


async def unclean_command_case(data_case: Any, tmp_path: Any, monkeypatch: Any) -> Any:
    from fixtures.workspace_server import command_consumer

    from krim_agent.execution.cgroups import Leaf
    from krim_agent.storage import work

    actual_finish = Leaf.finish
    block = True
    calls: list[Any] = []

    async def finish(leaf: Any) -> Any:
        if block:
            calls.append({"ticket": str(leaf.ticket), "identity": leaf.identity})
            raise ServiceError("sandbox_cleanup_failed")
        return await cast(Any, actual_finish)(leaf)

    monkeypatch.setattr(Leaf, "finish", finish)
    app, wire, provider = await workspace_application(data_case, tmp_path)
    cast(Any, approve_fixture)(app, monkeypatch)
    notes = data_case[1]
    try:
        run, access, before, initial, invocation, context, executor = await command_consumer(
            app, notes, "unclean"
        )
        result = await executor.execute(invocation, context)
        assert result.status == "failed"
        assert app.runner._resources and not app.runner.closed
        for _ in range(2):
            with pytest.raises(ServiceError):
                await app.runner.close()
            assert not app.runner.closed and not app.runner._owner.closed
        await app.store.finish(run.id, "failed", result.code)
        effect = await app.store.work_read(
            lambda conn: work.effect(conn, access, invocation.call_id)
        )
        assert effect["receipt"]["cleanup"] == "unknown"
        assert effect["receipt"]["exit_code"] is None
        assert (await app.artifacts.head(access)) == before
        notes["unclean"] = {
            "run_id": run.id,
            "call_id": invocation.call_id,
            "effect": effect,
            "cleanup_block_calls": calls,
            "resources_before_release": len(app.runner._resources),
        }
    finally:
        block = False
        await app.runner.close()
    assert app.runner.closed and not app.runner._resources
    assert provider.calls == len(wire.requests) == 0


async def cancel_verifier_case(data_case: Any, tmp_path: Any, monkeypatch: Any) -> Any:
    import threading

    from krim_agent.storage.sqlite import SQLiteStore
    from krim_agent.workspace import verification

    entered, release, finished = asyncio.Event(), threading.Event(), threading.Event()
    loop = asyncio.get_running_loop()
    actual_thread = asyncio.to_thread
    worker_ids: list[Any] = []

    async def thread(function: Any, *args: Any, **kwargs: Any) -> Any:
        if function is verification._condition:

            def actual() -> Any:
                worker_ids.append(threading.get_ident())
                loop.call_soon_threadsafe(entered.set)
                assert release.wait(5)
                try:
                    return function(*args, **kwargs)
                finally:
                    finished.set()

            return await actual_thread(actual)
        return await actual_thread(function, *args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", thread)
    app, wire, provider = await workspace_application(data_case, tmp_path, wire=WorkspaceWire())
    cast(Any, approve_fixture)(app, monkeypatch)
    session = await app.sessions.create(
        LOCAL, "local", SessionCreate(request_key="cancel-verifier", target_key="target-workspace")
    )
    turn = await app.sessions.submit(
        LOCAL,
        "local",
        session.id,
        WorkspaceTurnRequest(
            request_key="cancel",
            text="D4_TASK",
            expected_revision=0,
            completion=Completion(
                conditions=(
                    CompletionCondition(
                        id="answer",
                        kind="json_equal",
                        path="answer.json",
                        pointer="/answer",
                        expected=42,
                    ),
                )
            ),
        ),
    )
    controls: list[Any] = []
    outcomes: list[Any] = []
    stop_delivered = asyncio.Event()
    actual_stop = app.store.request_stop

    async def stopped(run_id: Any) -> Any:
        value = await actual_stop(run_id)
        stop_delivered.set()
        return value

    monkeypatch.setattr(app.store, "request_stop", stopped)
    try:
        async with asyncio.timeout(60):
            await entered.wait()
        current = await app.sessions.turn(LOCAL, "local", session.id, turn.id)
        controls = [asyncio.create_task(app.service.cancel(current.run_id)) for _ in range(2)]
        controls += [asyncio.create_task(app.close()) for _ in range(2)]
        async with asyncio.timeout(5):
            await stop_delivered.wait()
        assert app.service.tasks[current.run_id].cancelling() > 0
        assert not finished.is_set()
        assert not all(task.done() for task in controls)
    finally:
        release.set()
        if controls:
            outcomes = await asyncio.gather(*controls, return_exceptions=True)
        else:
            await app.close()
            outcomes = []
    assert finished.is_set() and worker_ids
    assert all(task.done() for task in controls)
    errors = [type(value).__name__ for value in outcomes if isinstance(value, BaseException)]
    assert errors == []
    reader = await SQLiteStore.open(tmp_path / "pipeline.sqlite", readonly=True)
    try:
        row = (await reader.snapshot(current.run_id)).run
        assert row.status == "aborted" and row.code == "cancelled"
    finally:
        await reader.close()
    data_case[1]["cancel_verifier"] = {
        "run_id": current.run_id,
        "worker_thread_ids": worker_ids,
        "thread_finished": finished.is_set(),
        "controls_joined": all(task.done() for task in controls),
        "control_errors": errors,
        "run": row.model_dump(mode="json"),
        "replay_count": 0,
    }
    assert provider.calls == len(wire.requests)


async def commit_ack_case(data_case: Any, tmp_path: Any, monkeypatch: Any) -> Any:
    from contextvars import ContextVar

    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncTransaction

    from krim_agent.sdk.workspace import WorkspaceSDK
    from krim_agent.workspace import verification

    app, wire, provider = await workspace_application(data_case, tmp_path, wire=WorkspaceWire())
    cast(Any, approve_fixture)(app, monkeypatch)
    marker = ContextVar[str | None]("d4-real-commit-observer", default=None)
    lost: set[str] = set()
    timeline: list[Any] = []
    connections: dict[int, Any] = {}
    captured: list[Any] = []
    entries: list[Any] = []
    negative = False
    diagnostics: list[Any] = []
    data_case[1]["commit_diagnostics"] = diagnostics

    def state(where: str) -> None:
        dispatcher = app.sessions._dispatcher
        error = dispatcher.exception() if dispatcher.done() and not dispatcher.cancelled() else None
        diagnostics.append(
            {
                "where": where,
                "healthy": app.store.healthy,
                "session_failure": app.sessions.failure,
                "dispatcher_done": dispatcher.done(),
                "dispatcher_error": repr(error),
                "lost_phases": sorted(lost),
            }
        )

    verifier = app.service.bindings["target-workspace"].loop._verifier
    original_verify = verifier.verify
    real_finish = app.store.finish
    query, exit_ = AsyncConnection.execute, AsyncTransaction.__aexit__

    async def rejected(*args: Any, **kwargs: Any) -> Any:
        entries.append("replacement-entered")
        raise ServiceError("target_binding_changed")

    async def finish(run_id: Any, status: Any, code: Any = None) -> Any:
        phase = marker.get() or ("negative" if negative else "positive")
        token = marker.set(phase)
        handoff = verification._HANDOFF.get()
        if handoff is not None:
            captured.append(handoff)
        if phase == "negative" and status == "completed":
            monkeypatch.setattr(verifier, "verify", rejected)
        try:
            return await real_finish(run_id, status, code)
        finally:
            state("finish-return:" + phase)
            if handoff is not None:
                assert not handoff.active and handoff.store is None and handoff.issuer is None
                assert handoff.producer is None
            marker.reset(token)

    async def execute(conn: Any, statement: Any, *args: Any, **kwargs: Any) -> Any:
        value = await query(conn, statement, *args, **kwargs)
        phase = marker.get()
        fields = {
            getattr(key, "name", key): getattr(item, "value", None)
            for key, item in getattr(statement, "_values", {}).items()
        }
        if phase and (
            "verification" in fields
            or phase == "existing"
            and getattr(statement, "is_select", False)
        ):
            connections[id(conn)] = phase
        return value

    async def committed(transaction: Any, exc_type: Any, exc_value: Any, traceback: Any) -> Any:
        await exit_(transaction, exc_type, exc_value, traceback)
        value = None
        phase = marker.get()
        if (
            exc_type is None
            and phase is not None
            and phase not in lost
            and connections.get(id(transaction.connection)) == phase
        ):
            assert not transaction.connection.in_transaction()
            lost.add(phase)
            timeline.append(
                {
                    "phase": phase,
                    "window": "actual-COMMIT-returned",
                    "connection": id(transaction.connection),
                }
            )
            raise OSError("synthetic lost acknowledgement after actual COMMIT")
        return value

    monkeypatch.setattr(app.store, "finish", finish)
    monkeypatch.setattr(AsyncConnection, "execute", execute)
    monkeypatch.setattr(AsyncTransaction, "__aexit__", committed)
    results: list[Any] = []
    for key in ["positive", "negative"]:
        negative = key == "negative"
        state("before-create:" + key)
        session = await app.sessions.create(
            LOCAL, "local", SessionCreate(request_key=key, target_key="target-workspace")
        )
        body = WorkspaceTurnRequest(
            request_key=key,
            text="D4_TASK",
            expected_revision=0,
            completion=Completion(required_paths=("calc.py", "answer.json")),
        )
        turn = await app.sessions.submit(LOCAL, "local", session.id, body)
        value = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
        # 终态行已提交不等于实际finish/ack确认和producer已收束。
        joined = await app.service.wait(value.run_id)
        assert joined.id == value.run_id and joined.status == value.status
        assert value.status == ("failed" if negative else "completed")
        assert value.code == ("target_binding_changed" if negative else None)
        results.append((session, turn, body, value))
    assert lost == {"positive", "negative"} and len(captured) == 2
    count = len(wire.requests)
    token = marker.set("existing")
    old_context = verification._HANDOFF.set(captured[0])
    captured[0].run_deadline = asyncio.get_running_loop().time() - 1
    try:
        first = results[0][3]
        existing = await app.store.finish(first.run_id, "aborted", "cancelled")
        assert existing.status == "completed" and existing.code is None
        assert existing.id == first.run_id
    finally:
        marker.reset(token)
        verification._HANDOFF.reset(old_context)
    assert lost == {"positive", "negative", "existing"}
    assert entries == [] and len(wire.requests) == count
    sdk = WorkspaceSDK(app)
    for session, turn, body, value in results:
        found = await sdk.by_key(session.id, body.request_key)
        assert found.turn_id == turn.id and found.run_id == value.run_id
        assert found.original_request == body
        snapshot = await app.store.snapshot(value.run_id)
        terminal = [event for event in snapshot.events if event.kind == "terminal"]
        assert len(terminal) == 1
        assert terminal[0].data == {"status": value.status, "code": value.code}
        assert len([event for event in snapshot.events if event.kind == "workspace_started"]) == 1
    assert len(wire.requests) == count
    monkeypatch.setattr(verifier, "verify", original_verify)
    data_case[1]["commit_ack"] = {
        "timeline": timeline,
        "replacement_entries": entries,
        "run_ids": [value.run_id for _, _, _, value in results],
        "original_keys": [body.request_key for _, _, body, _ in results],
        "provider_calls_before_after_lookup": count,
        "same_pair_confirmed": True,
        "revoked_handoffs": [not handle.active for handle in captured],
    }


async def late_run_deadlines_case(app: Any, notes: Any, monkeypatch: Any) -> Any:
    """真实原Run期限：只构造已过140秒的初始Run，不伪造fence时钟或延长期限。"""
    import time
    from contextvars import ContextVar
    from types import SimpleNamespace

    from sqlalchemy.ext.asyncio import AsyncConnection

    import krim_agent.storage.sqlite as sqlite_module
    from krim_agent.workspace import verification

    original_begin, original_finish = app.store.begin_turn_run, app.store.finish
    original_row, original_query = app.store._row, AsyncConnection.execute
    original_clock = cast(Any, sqlite_module).time
    phase = ContextVar[str | None]("d4-terminal-deadline-observer", default=None)
    armed = set()
    fired = set()
    windows: list[Any] = []

    async def begin(*args: Any, **kwargs: Any) -> Any:
        monkeypatch.setattr(sqlite_module, "time", SimpleNamespace(time=lambda: time.time() - 140))
        try:
            return await original_begin(*args, **kwargs)
        finally:
            monkeypatch.setattr(sqlite_module, "time", original_clock)

    mode = "after-engine-return"

    async def finish(run_id: Any, status: Any, code: Any = None) -> Any:
        token = phase.set(mode)
        try:
            handoff = verification._HANDOFF.get()
            if status == "completed" and mode == "after-engine-return":
                assert handoff is not None and run_id in app.service._finalizing
                windows.append(
                    {
                        "phase": mode,
                        "run_id": run_id,
                        "before": time.monotonic(),
                        "original_run_deadline": handoff.run_deadline,
                        "issuer_active": handoff.active,
                    }
                )
                await asyncio.sleep(max(0, handoff.run_deadline - time.monotonic()) + 0.01)
                fired.add(mode)
            return await original_finish(run_id, status, code)
        finally:
            phase.reset(token)

    async def query(conn: Any, statement: Any, *args: Any, **kwargs: Any) -> Any:
        value = await original_query(conn, statement, *args, **kwargs)
        fields = {
            getattr(key, "name", key): getattr(item, "value", None)
            for key, item in getattr(statement, "_values", {}).items()
        }
        verification_value = fields.get("verification")
        if phase.get() == "final-row-return" and isinstance(verification_value, dict):
            if verification_value["state"] == "passed":
                armed.add(id(conn))
        return value

    async def row(conn: Any, run_id: Any) -> Any:
        value = await original_row(conn, run_id)
        if (
            phase.get() == "final-row-return"
            and id(conn) in armed
            and value["status"] == "completed"
            and "final-row-return" not in fired
        ):
            handoff = verification._HANDOFF.get()
            assert handoff is not None
            fired.add("final-row-return")
            windows.append(
                {
                    "phase": "final-row-return",
                    "run_id": run_id,
                    "actual_SQL_row_returned": True,
                    "before": time.monotonic(),
                    "original_run_deadline": handoff.run_deadline,
                    "connection": id(conn),
                }
            )
            await asyncio.sleep(max(0, handoff.run_deadline - time.monotonic()) + 0.01)
        return value

    monkeypatch.setattr(app.store, "begin_turn_run", begin)
    monkeypatch.setattr(app.store, "finish", finish)
    monkeypatch.setattr(app.store, "_row", row)
    monkeypatch.setattr(AsyncConnection, "execute", query)
    for mode in ["after-engine-return", "final-row-return"]:
        session = await app.sessions.create(
            LOCAL, "local", SessionCreate(request_key=mode, target_key="target-workspace")
        )
        turn = await app.sessions.submit(
            LOCAL,
            "local",
            session.id,
            WorkspaceTurnRequest(
                request_key=mode,
                text="D4_TASK",
                expected_revision=0,
                completion=Completion(required_paths=("calc.py", "answer.json")),
            ),
        )
        value = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
        assert value.status == "failed" and value.code == "run_timeout"
        snapshot = await app.store.snapshot(value.run_id)
        terminal = [event for event in snapshot.events if event.kind == "terminal"]
        assert len(terminal) == 1 and terminal[0].data == {
            "status": "failed",
            "code": "run_timeout",
        }
        assert len([event for event in snapshot.events if event.kind == "workspace_started"]) == 1
    assert fired == {"after-engine-return", "final-row-return"}
    assert len(windows) == 2 and all(
        time.monotonic() >= item["original_run_deadline"] for item in windows
    )
    notes["late_run_deadlines"] = windows


async def restart_case(data_case: Any, tmp_path: Any) -> Any:
    import hashlib
    import os
    import signal
    import sqlite3
    import subprocess
    import sys
    from contextlib import closing
    from dataclasses import asdict
    from pathlib import Path

    from test_session_read_sides import Connections
    from test_tool_pipeline import os_profile

    from krim_agent.sdk.workspace import WorkspaceSDK
    from krim_agent.storage.sqlite import SQLiteStore
    from krim_agent.workspace.targets import WorkspaceConfiguration, environment
    from krim_agent.workspace.types import WorkspaceProfile

    notes = data_case[1]
    await data_case[0].close()
    execution = cast(Any, os_profile)(tmp_path / "worker")
    configuration = WorkspaceConfiguration(
        WorkspaceProfile(max_tools_per_step=4, environment=environment(execution)),
        execution,
        tmp_path / "worker-objects",
    )
    database, marker = tmp_path / "restart.sqlite", tmp_path / "worker-marker.json"
    config = tmp_path / "trusted-configuration.json"
    config.write_text(
        json.dumps(
            {
                "profile": configuration.profile.model_dump(mode="json"),
                "execution": asdict(execution),
                "artifact_root": str(configuration.artifact_root),
            },
            default=str,
        )
    )
    config.chmod(0o600)
    argv = [
        sys.executable,
        "-I",
        "-B",
        str(Path.cwd() / "tests/agent_core/fixtures/workspace_server.py"),
        "crash",
        str(database),
        str(marker),
        str(config),
    ]
    env = {
        key: os.environ[key]
        for key in [
            "LD_LIBRARY_PATH",
            "TMPDIR",
            "HOME",
            "KRIM_D4_ROOT",
            "KRIM_D4_EVIDENCE",
            "KRIM_D2_ROOT",
            "KRIM_D2_EVIDENCE",
            "KRIM_D1_DATA_ROOT",
            "KRIM_D1_EVIDENCE",
        ]
    }
    env.update(PATH="/usr/local/bin:/usr/bin:/bin", PYTHONDONTWRITEBYTECODE="1")
    pidfd = None
    worker = None
    with (
        (tmp_path / "worker.stdout").open("xb") as stdout,
        (tmp_path / "worker.stderr").open("xb") as stderr,
    ):
        try:
            worker = await asyncio.to_thread(
                subprocess.Popen, argv, cwd=Path.cwd(), env=env, stdout=stdout, stderr=stderr
            )
            pidfd = os.pidfd_open(worker.pid)
            async with asyncio.timeout(60):
                while not marker.exists():
                    assert worker.poll() is None
                    await asyncio.sleep(0.02)
            actual = json.loads(marker.read_bytes())
            assert actual["pid"] == worker.pid and actual["phase"] == "payload_output"
            signal.pidfd_send_signal(pidfd, signal.SIGKILL)
            rc = await asyncio.to_thread(worker.wait, timeout=5)
            assert rc == -9 and worker.returncode == -9
        finally:
            if worker is not None and worker.poll() is None:
                if pidfd is not None:
                    signal.pidfd_send_signal(pidfd, signal.SIGKILL)
                await asyncio.to_thread(worker.wait, timeout=5)
            if pidfd is not None:
                os.close(pidfd)
    app, wire, provider = await workspace_application(
        data_case, tmp_path, configuration=configuration, database=database
    )
    sdk = WorkspaceSDK(app)
    view = await sdk.by_key(actual["session_id"], actual["request"]["request_key"])
    assert view.run_id == actual["run_id"] and view.turn_id == actual["turn_id"]
    assert view.phase == "interrupted" and view.verification is not None
    assert view.verification.state == "unknown"
    command = [effect for effect in view.effects if effect.kind == "command"]
    assert len(command) == 1 and command[0].exit_code is None
    assert command[0].cleanup == "unknown"
    assert app.runner.recovered
    assert not list((execution.material / "tickets").iterdir())
    assert provider.calls == len(wire.requests) == 0
    repeated = await sdk.by_key(actual["session_id"], actual["request"]["request_key"])
    assert repeated == view and provider.calls == 0
    notes["restart"] = {
        "argv": argv,
        "cwd": str(Path.cwd()),
        "safe_env": env,
        "worker_pid": worker.pid,
        "waited_pid": worker.pid,
        "returncode": worker.returncode,
        "pidfd_opened": pidfd,
        "pidfd_closed": True,
        "internal_worker_finally": "unknown",
        "database": str(database),
        "marker": actual,
        "recovery": app.runner.recovered,
        "work": view.model_dump(mode="json"),
        "parent_provider_calls": provider.calls,
        "replay_count": 0,
        "tickets_empty": True,
    }
    await app.close()
    readonly = await SQLiteStore.open(database, readonly=True)
    notes.setdefault("store_probes", []).append((readonly, cast(Any, Connections)(readonly)))
    try:
        assert (await readonly.snapshot(actual["run_id"])).run.status == "interrupted"
    finally:
        await readonly.close()
    backup = tmp_path / "restart-consistent-readonly.sqlite"
    with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as reader:
        with closing(sqlite3.connect(backup)) as writer, writer:
            reader.backup(writer)
    notes["restart"]["readonly_backup"] = str(backup)
    notes["restart"]["backup_sha256"] = hashlib.sha256(backup.read_bytes()).hexdigest()


async def observation_fault_cases(data_case: Any, tmp_path: Any, monkeypatch: Any) -> Any:
    """已知Receipt与可选观察的真实SQL/Runner责任边界；子向量不冒充pytest参数。"""
    import sqlite3
    import sys
    import types
    from contextlib import closing

    from fixtures.workspace_server import command_consumer
    from sqlalchemy import func, select
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncTransaction
    from test_tool_pipeline import os_profile

    import krim_agent.workspace as package
    from krim_agent.execution.linux import SandboxRunner
    from krim_agent.storage.models import run_events
    from krim_agent.workspace import verification
    from krim_agent.workspace.targets import WorkspaceConfiguration, environment
    from krim_agent.workspace.types import WorkspaceProfile

    notes = data_case[1]
    vectors = [
        "export-unknown",
        "helper",
        "module",
        "parse",
        "event_limit",
        "output_limit",
        "key_conflict",
        "io",
        "savepoint",
        "commit",
    ]
    notes["observation_vectors"] = []

    async def vector(fault: Any) -> Any:
        folder = tmp_path / fault
        folder.mkdir()
        entries: list[Any] = []
        received: list[Any] = []
        windows: list[dict[str, Any]] = []
        holder: dict[str, Any] = {}
        fired = False
        with monkeypatch.context() as change:
            actual_run = SandboxRunner.run
            actual_init = SandboxRunner.__init__
            if fault == "export-unknown":

                def init(runner: Any, *args: Any, **kwargs: Any) -> Any:
                    original_hook = kwargs.get("hook")

                    async def export(phase: Any) -> Any:
                        if original_hook:
                            await original_hook(phase)
                        if phase == "export_end":
                            windows.append({"event": "real-export-end-before-Receipt-return"})
                            active = holder["app"]
                            changed = active.work.profile.model_dump(mode="json")
                            changed["runtime"]["approval_timeout_seconds"] = 29.0
                            change.setattr(
                                active.work,
                                "profile",
                                type(active.work.profile).model_validate_json(json.dumps(changed)),
                            )

                    kwargs["hook"] = export
                    actual_init(runner, *args, **kwargs)

                change.setattr(SandboxRunner, "__init__", init)

            async def replacement(*args: Any, **kwargs: Any) -> Any:
                entries.append("replaced-helper-entered")
                raise ServiceError("completion_unverified")

            async def run(runner: Any, request: Any) -> Any:
                receipt = await actual_run(runner, request)
                received.append(
                    {
                        "exit_code": receipt.exit_code,
                        "output_complete": receipt.output_complete,
                        "cleanup": receipt.cleanup,
                    }
                )
                if fault == "helper":
                    change.setattr(verification, "command_observation", replacement)
                elif fault == "module":
                    proxy = types.ModuleType(verification.__name__)
                    proxy.__dict__.update(vars(verification))
                    cast(Any, proxy).command_observation = replacement
                    change.setitem(sys.modules, verification.__name__, proxy)
                    change.setattr(package, "verification", proxy)
                return receipt  # 原Runner的原对象，不编造或替换任何退出/清理字段。

            change.setattr(SandboxRunner, "run", run)
            execution = cast(Any, os_profile)(folder)
            configuration = WorkspaceConfiguration(
                WorkspaceProfile(max_tools_per_step=4, environment=environment(execution)),
                execution,
                folder / "objects",
            )

            async def pre(invocation: Any) -> Any:
                if fault == "key_conflict" and invocation.name == "command_run":
                    await holder["app"].store.append(
                        invocation.run_id,
                        "workspace_command_observation",
                        {"fixture_conflict": True},
                        event_id="workspace-command-observation:" + invocation.call_id,
                    )
                return None

            app, wire, provider = await workspace_application(
                data_case,
                folder,
                configuration=configuration,
                database=folder / "pipeline.sqlite",
                pre=pre,
            )
            holder["app"] = app
            cast(Any, approve_fixture)(app, change)
            real_event = app.store._event
            maximum = app.store.max_events

            async def event(
                conn: Any,
                run_id: Any,
                kind: Any,
                data: Any,
                event_id: Any,
                **kwargs: Any,
            ) -> Any:
                if (
                    fault == "event_limit"
                    and kind == "workspace_receipt"
                    and data.get("name") == "command_run"
                ):
                    count = await conn.scalar(
                        select(func.count())
                        .select_from(run_events)
                        .where(run_events.c.run_id == run_id)
                    )
                    app.store.max_events = count + 2
                if kind != "workspace_command_observation":
                    return await real_event(conn, run_id, kind, data, event_id, **kwargs)
                windows.append(
                    {
                        "event": "observation-attempt",
                        "connection": id(conn),
                        "transaction": conn.in_transaction(),
                    }
                )
                if fault == "output_limit":
                    return await real_event(
                        conn,
                        run_id,
                        kind,
                        data | {"synthetic_oversize": "x" * 65536},
                        event_id,
                        **kwargs,
                    )
                if fault == "io":
                    await real_event(conn, run_id, kind, data, event_id, **kwargs)
                    raise OSError("synthetic I/O failure after real observation INSERT")
                try:
                    return await real_event(conn, run_id, kind, data, event_id, **kwargs)
                finally:
                    app.store.max_events = maximum

            change.setattr(app.store, "_event", event)
            real_sql = AsyncConnection.exec_driver_sql

            async def sql(conn: Any, statement: Any, *args: Any, **kwargs: Any) -> Any:
                nonlocal fired
                if fault == "savepoint" and statement == "SAVEPOINT krim_d4_observation":
                    fired = True
                    windows.append({"event": "savepoint-request-rejected-by-real-SQLite"})
                    return await real_sql(conn, "SAVEPOINT", *args, **kwargs)
                return await real_sql(conn, statement, *args, **kwargs)

            change.setattr(AsyncConnection, "exec_driver_sql", sql)
            original_exit = AsyncTransaction.__aexit__

            async def exit_(transaction: Any, exc_type: Any, exc_value: Any, traceback: Any) -> Any:
                nonlocal fired
                if fault == "commit" and exc_type is None and windows and not fired:
                    if any(
                        item.get("connection") == id(transaction.connection) for item in windows
                    ):
                        fired = True
                        error = OSError("synthetic failure at COMMIT boundary")
                        await original_exit(transaction, OSError, error, None)
                        windows.append({"event": "real-outer-ROLLBACK-completed"})
                        raise error
                return await original_exit(transaction, exc_type, exc_value, traceback)

            change.setattr(AsyncTransaction, "__aexit__", exit_)
            result = None
            try:
                (
                    current,
                    access,
                    before,
                    initial,
                    invocation,
                    context,
                    executor,
                ) = await command_consumer(
                    app, notes, "observation-" + fault, ambiguous=fault == "parse"
                )
                result = await executor.execute(invocation, context)
                assert result.status == "failed"
                if fault == "export-unknown":
                    assert received == [] and windows
                else:
                    assert len(received) == 1 and received[0] == {
                        "exit_code": 0,
                        "output_complete": True,
                        "cleanup": "clean",
                    }
                assert entries == []
                await app.store.finish(current.id, "failed", result.code)
                snapshot = await app.store.snapshot(current.id)
                from krim_agent.storage import work

                effect = await app.store.work_read(
                    lambda conn: work.effect(conn, access, invocation.call_id)
                )
                receipts = [item for item in snapshot.events if item.kind == "workspace_receipt"]
                observation = [
                    item for item in snapshot.events if item.kind == "workspace_command_observation"
                ]
                if fault in {"io", "savepoint", "commit", "export-unknown"}:
                    if fault != "export-unknown":
                        assert result.code == "storage_unavailable"
                        assert not receipts
                    assert effect["receipt"]["exit_code"] is None
                    assert effect["receipt"]["cleanup"] == "unknown"
                else:
                    assert len(receipts) == 1
                    assert effect["receipt"]["exit_code"] == 0
                    assert effect["receipt"]["output_complete"] is True
                    assert effect["receipt"]["cleanup"] == "clean"
                    if fault == "key_conflict":
                        assert len(observation) == 1
                        assert observation[0].data == {"fixture_conflict": True}
                assert (await app.artifacts.head(access)) == before
                assert (await app.artifacts.get(access, initial.revision_id)) == initial
                assert (
                    len([item for item in snapshot.events if item.kind == "workspace_started"]) == 1
                )
                assert provider.calls == len(wire.requests) == 0
                notes["observation_vectors"].append(
                    {
                        "fault": fault,
                        "layer": "actual ToolExecutor/Store/Runner; no model and no forged Receipt",
                        "database": str(folder / "pipeline.sqlite"),
                        "run_id": current.id,
                        "call_id": invocation.call_id,
                        "head_before_after": before.revision_id,
                        "known_runner_return": received,
                        "effect": effect,
                        "events": [item.model_dump(mode="json") for item in snapshot.events],
                        "replacement_entries": entries,
                        "windows": windows,
                        "result": result.model_dump(mode="json"),
                        "replay_count": 0,
                    }
                )
            finally:
                await app.close()
            database = folder / "pipeline.sqlite"
            backup = folder / "readonly-consistent-backup.sqlite"
            with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as reader:
                with closing(sqlite3.connect(backup)) as writer, writer:
                    reader.backup(writer)
            notes["observation_vectors"][-1]["readonly_backup"] = str(backup)

    for fault in vectors:
        await vector(fault)
