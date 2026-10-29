"""H1真实消费者草稿；本初始检查点只作者编写，尚未收集/执行。"""

import asyncio
import json
import os
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

import pytest
from sqlalchemy import event, func, select

# 仅source测试复用既有真实fixture；installed程序不导入tests或source。
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "agent_core"))
from fixtures.session_server import WireFixture
from test_tool_pipeline import data_case as data_case

from krim_agent.contracts import ServiceError
from krim_agent.evaluation.adapters.krim_outbox import ActorViews, KrimOutboxPort
from krim_agent.evaluation.app import open_eval
from krim_agent.evaluation.continuous_metrics import continuous_rules
from krim_agent.evaluation.continuous_types import (
    ConfirmationRequest,
    ContinuousRequest,
    FreezeConfirmed,
)
from krim_agent.evaluation.domain import DatasetRef, DatasetSpec
from krim_agent.evaluation.hot_observations import HotObservationFlow
from krim_agent.evaluation.hot_types import (
    CapturePolicy,
    FeedbackSignal,
    delivery_key,
    import_request,
)
from krim_agent.evaluation.repository import meta
from krim_agent.security import LOCAL, Membership, PrincipalContext
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage import observation_outbox as sql
from krim_agent.storage.models import run_events


@dataclass
class World:
    source: object
    evaluation: object
    wire: object
    port: KrimOutboxPort
    actors: dict
    views: ActorViews
    flow: HotObservationFlow
    root: Path
    sequence: int = 0

    async def run(self, text="/add 2 3"):
        self.sequence += 1
        session = await self.source.sessions.create(
            LOCAL,
            "local",
            SessionCreate(request_key=f"source-session-{self.sequence}", target_key="demo"),
        )
        turn = await self.source.sessions.submit(
            LOCAL,
            "local",
            session.id,
            TurnRequest(request_key="source-turn", text=text, expected_revision=session.revision),
        )
        ended = await self.source.sessions.wait(LOCAL, "local", session.id, turn.id)
        assert ended.run_id is not None
        return await self.source.store.snapshot(ended.run_id)

    async def feedback(self, run_id, actor="alice", key="feedback", reason="incorrect"):
        return await self.port.feedback(
            self.actors[actor], run_id, FeedbackSignal(key=key, reason=reason)
        )

    async def counters(self):
        async def operation(conn):
            return (
                await conn.execute(
                    select(sql.capture_policies.c.counters).where(
                        sql.capture_policies.c.workspace_id == "local"
                    )
                )
            ).scalar_one()

        return await self.source.store.work_read(operation)


@asynccontextmanager
async def world(root, **changes):
    root.mkdir(parents=True, exist_ok=True)
    before = asyncio.all_tasks()
    fd_before = len(os.listdir("/proc/self/fd"))
    wire = await WireFixture().start()
    context = wire.application(root / "source.sqlite")
    source = evaluation = value = None
    opened_evaluation = None
    facts = {
        "source_database": str(root / "source.sqlite"),
        "eval_database": str(root / "eval.sqlite"),
        "fd_before": fd_before,
    }
    try:
        source = await context.__aenter__()
        evaluation = await open_eval(root / "eval.sqlite")
        opened_evaluation = evaluation
        async with evaluation.repository.engine.begin() as conn:
            marker = (await conn.execute(select(meta.c.body))).scalar_one()
        actors = {"local": LOCAL}
        for actor in ["alice", "bob", "consumer"]:
            actors[actor] = PrincipalContext(
                actor, 1, (Membership(workspace_id="local", role="admin"),)
            )
        policy = CapturePolicy.model_validate({"enabled": True, **changes})
        port = KrimOutboxPort(source.store, source.sessions, actors["consumer"], policy)
        await port.configure()
        views = ActorViews(port, evaluation.repository, evaluation.kernel, actors)
        flow = HotObservationFlow(
            port,
            evaluation.repository,
            evaluation.kernel,
            marker["repository_id"],
            views.resolve,
            views.binding,
            1,
        )
        value = World(source, evaluation, wire, port, actors, views, flow, root)
        yield value
        facts.update(
            source_requests=len(wire.requests),
            target_calls=value.evaluation.owner.spawn_calls,
            source_policy=policy.model_dump(mode="json"),
        )
    finally:
        if value is not None:
            evaluation = value.evaluation
        owners = {
            id(owner): owner for owner in (opened_evaluation, evaluation) if owner is not None
        }
        facts["eval_owners_before_close"] = [_hot_eval_owner(owner) for owner in owners.values()]
        if evaluation is not None:
            await evaluation.close()
            facts.update(
                eval_closed=evaluation.repository.closed,
                eval_writer_fd=evaluation.repository._writer_fd,
                eval_producers_done=all(t.done() for t in evaluation.kernel.tasks.values()),
                actual_eval_id=id(evaluation),
                actual_repository_id=id(evaluation.repository),
                actual_kernel_id=id(evaluation.kernel),
                target_calls=evaluation.owner.spawn_calls,
            )
        for owner in owners.values():
            if owner is not evaluation and not owner.repository.closed:
                await owner.close()
        facts["eval_owners_after_close"] = [_hot_eval_owner(owner) for owner in owners.values()]
        if source is not None:
            await context.__aexit__(None, None, None)
            facts.update(
                source_closed=source.store.closed,
                source_writer_fd=source.store.fd,
                source_producers_done=all(t.done() for t in source.service.tasks.values()),
                dispatcher_done=source.sessions._dispatcher.done(),
            )
        await wire.close()
        facts.update(source_requests=len(wire.requests), source_executions=len(wire.executions))
        facts.update(
            pending=[
                repr(t)
                for t in asyncio.all_tasks()
                if t not in before and t is not asyncio.current_task() and not t.done()
            ],
            fd_after=len(os.listdir("/proc/self/fd")),
        )
        (root / "hot-resources.json").write_text(json.dumps(facts, indent=2))
        assert not facts["pending"] and facts["fd_after"] <= fd_before
        if evaluation is not None:
            assert facts["eval_closed"] and facts["eval_writer_fd"] is None
            assert all(
                owner["closed"] and owner["writer_fd"] is None and owner["producers_done"]
                for owner in facts["eval_owners_after_close"]
            )
        if source is not None:
            assert (
                facts["source_closed"]
                and facts["source_producers_done"]
                and facts["dispatcher_done"]
            )


async def receipt_rows(w):
    async with w.evaluation.repository.engine.begin() as conn:
        from krim_agent.evaluation.repository import source_observations

        return list((await conn.execute(select(source_observations.c.body))).scalars())


async def workspace_terminal(tmp_path, monkeypatch, data_case, case):
    """独立H1消费者：真实D4核验、SQL漂移和同SAVEPOINT捕获回滚。"""
    import base64

    from fixtures.workspace_server import WorkspaceWire, workspace_application
    from sqlalchemy.ext.asyncio import AsyncConnection
    from test_tool_pipeline import approve_fixture

    from krim_agent.context.types import ContextPolicy
    from krim_agent.evaluation.trace_cases import observe_snapshot
    from krim_agent.sdk.workspace import WorkspaceSDK
    from krim_agent.workspace.types import Completion, CompletionCondition, WorkspaceTurnRequest

    assert case in {"pass-next-turn", "terminal-sql-drift"}
    app, wire, provider = await workspace_application(data_case, tmp_path, wire=WorkspaceWire(case))
    port = KrimOutboxPort(app.store, app.sessions, LOCAL, CapturePolicy(enabled=True))
    notes = data_case[1]
    evidence = {
        "case": case,
        "database": str(notes["database"]),
        "sql": [],
        "drift": [],
        "replacement_entries": [],
        "error": None,
        "connection_map": [],
        "executions": {},
        "engine": id(app.store.engine.sync_engine),
        "sql_boundary": "after_cursor_execute success; transaction writes are not commit evidence",
    }
    verifier = app.service.bindings["target-workspace"].loop._verifier
    original_verify, execute = verifier.verify, AsyncConnection.execute

    async def rejected(*args, **kwargs):
        evidence["replacement_entries"].append("replacement-entered")
        raise ServiceError("target_binding_changed")

    async def query(conn, statement, *args, **kwargs):
        result = await execute(conn, statement, *args, **kwargs)
        fields = {
            getattr(key, "name", key): getattr(value, "value", None)
            for key, value in getattr(statement, "_values", {}).items()
        }
        table = getattr(getattr(statement, "table", None), "name", None)
        sync_id = id(conn.sync_connection)
        execution_id = id(getattr(result, "context", None))
        index = evidence["executions"].get(str(execution_id))
        returned = evidence["sql"][index] if index is not None else None
        if conn.engine is app.store.engine:
            evidence["connection_map"].append(
                {
                    "async_connection": id(conn),
                    "sync_connection": sync_id,
                    "execution_id": execution_id,
                    "sql_index": index,
                    "in_transaction": conn.in_transaction(),
                }
            )
        if (
            case == "terminal-sql-drift"
            and not evidence["drift"]
            and conn.engine is app.store.engine
            and getattr(statement, "is_update", False)
            and table == "capture_policies"
            and "counters" in fields
            and isinstance(fields["counters"], dict)
            and result.rowcount == 1
            and returned is not None
            and returned["kind"] == "capture-counter-update"
            and returned["connection"] == sync_id
            and conn.in_transaction()
        ):
            same_connection = [item for item in evidence["sql"] if item["connection"] == sync_id]
            savepoints = [item for item in same_connection if item["kind"] == "savepoint"]
            attempt = [
                item
                for item in same_connection
                if savepoints and savepoints[-1]["index"] < item["index"] <= returned["index"]
            ]
            terminals = [item for item in attempt if item["kind"] == "terminal-event"]
            verified = [
                item
                for item in attempt
                if item["kind"] == "verification-update" and item["verification_state"] == "passed"
            ]
            captures = [item for item in attempt if item["kind"] == "outbox-insert"]
            counters = [item for item in attempt if item["kind"] == "capture-counter-update"]
            run_updates = [
                item
                for item in attempt
                if item["table"] == "runs"
                and item["operation"] == "update"
                and item["status"] == "completed"
            ]
            turn_updates = [
                item
                for item in attempt
                if item["table"] == "turns"
                and item["operation"] == "update"
                and item["status"] == "completed"
            ]
            if (
                len(savepoints)
                == len(terminals)
                == len(verified)
                == len(captures)
                == len(counters)
                == len(run_updates)
                == len(turn_updates)
                == 1
                and not any(item["kind"] == "rollback" for item in attempt)
                and terminals[0]["terminal_status"] == "completed"
                and terminals[0]["run_id"]
                == verified[0]["run_id"]
                == captures[0]["run_id"]
                == run_updates[0]["run_id"]
                and turn_updates[0]["turn_id"] is not None
                and terminals[0]["event_id"] == captures[0]["terminal_event_id"]
                and terminals[0]["capture_id"] == captures[0]["capture_id"]
                and captures[0]["workspace_id"] == returned["workspace_id"]
                and savepoints[0]["index"]
                < terminals[0]["index"]
                < verified[0]["index"]
                < captures[0]["index"]
                < returned["index"]
                and terminals[0]["index"] < run_updates[0]["index"] < captures[0]["index"]
                and terminals[0]["index"] < turn_updates[0]["index"] < captures[0]["index"]
                and returned["counters"]
                == fields["counters"]
                == {"seen": 1, "enqueued": 1, "dropped": 0, "acked": 0}
                and all(
                    item["rowcount"] == 1
                    for item in terminals
                    + verified
                    + captures
                    + counters
                    + run_updates
                    + turn_updates
                )
                and all(item["in_transaction"] for item in attempt)
            ):
                evidence["drift"].append(
                    {
                        "window": "capture-counter-UPDATE-returned",
                        "connection": id(conn),
                        "sync_connection": sync_id,
                        "execution_id": execution_id,
                        "after_sql_index": returned["index"],
                        "run_id": captures[0]["run_id"],
                        "capture_id": captures[0]["capture_id"],
                        "terminal_event_id": captures[0]["terminal_event_id"],
                        "counter_values": dict(fields["counters"]),
                        "in_transaction": conn.in_transaction(),
                    }
                )
                monkeypatch.setattr(verifier, "verify", rejected)
        return result

    def observed(conn, cursor, statement, parameters, context, executemany):
        compiled = getattr(getattr(context, "compiled", None), "statement", None)
        table = getattr(getattr(compiled, "table", None), "name", None)
        operation = (
            "insert"
            if getattr(compiled, "is_insert", False)
            else ("update" if getattr(compiled, "is_update", False) else "other")
        )
        values = getattr(context, "compiled_parameters", None) or []
        bound = values[0] if values else {}
        identifiers = {}
        for name, value in bound.items():
            column = name.rsplit("_", 1)[0] if name.rsplit("_", 1)[-1].isdigit() else name
            if column in {
                "id",
                "run_id",
                "turn_id",
                "session_id",
                "workspace_id",
                "capture_id",
                "terminal_event_id",
                "event_id",
                "policy_fingerprint",
                "body_hash",
            }:
                identifiers[name] = value if isinstance(value, str) and len(value) <= 128 else None
        run_id = bound.get("run_id", bound.get("run_id_1"))
        if table == "runs":
            run_id = bound.get("id_1", bound.get("id", run_id))
        terminal_data = bound.get("data")
        terminal_data = terminal_data if isinstance(terminal_data, dict) else {}
        disposition = terminal_data.get("observation_capture")
        disposition = disposition if isinstance(disposition, dict) else {}
        verification = bound.get("verification")
        verification = verification if isinstance(verification, dict) else {}
        body = bound.get("body")
        body = body if table == "observation_outbox" and isinstance(body, dict) else {}
        counter_values = bound.get("counters")
        counter_values = (
            {key: counter_values.get(key) for key in ("seen", "enqueued", "dropped", "acked")}
            if isinstance(counter_values, dict)
            else None
        )
        kind = "sql"
        if statement.lstrip().startswith("SAVEPOINT krim_d4_terminal"):
            kind = "savepoint"
        if statement.lstrip().startswith("ROLLBACK TO krim_d4_terminal"):
            kind = "rollback"
        if table == "run_events" and operation == "insert" and bound.get("kind") == "terminal":
            kind = "terminal-event"
        if table == "work_turns" and operation == "update" and "verification" in bound:
            kind = "verification-update"
        if table == "observation_outbox" and operation == "insert":
            kind = "outbox-insert"
        if table == "capture_policies" and operation == "update" and "counters" in bound:
            kind = "capture-counter-update"
        index = len(evidence["sql"])
        evidence["sql"].append(
            {
                "index": index,
                "kind": kind,
                "statement": statement,
                "table": table,
                "operation": operation,
                "connection": id(conn),
                "engine": id(conn.engine),
                "execution_id": id(context),
                "dbapi_connection": id(conn.connection.dbapi_connection),
                "in_transaction": conn.in_transaction(),
                "bindings": identifiers,
                "parameter_sets": len(values),
                "executemany": executemany,
                "rowcount": cursor.rowcount,
                "run_id": run_id,
                "workspace_id": bound.get("workspace_id", bound.get("workspace_id_1")),
                "turn_id": bound.get("id_1") if table == "turns" else None,
                "event_id": bound.get("event_id"),
                "terminal_status": terminal_data.get("status"),
                "terminal_code": terminal_data.get("code"),
                "status": bound.get("status"),
                "capture_id": bound.get("capture_id", disposition.get("capture_id")),
                "terminal_event_id": bound.get("terminal_event_id"),
                "snapshot_hash": body.get("snapshot_hash"),
                "verification_state": verification.get("state"),
                "counters": counter_values,
            }
        )
        evidence["executions"][str(id(context))] = index

    event.listen(app.store.engine.sync_engine, "after_cursor_execute", observed)
    try:
        approve_fixture(app, monkeypatch)
        await port.configure()
        monkeypatch.setattr(AsyncConnection, "execute", query)
        session = await app.sessions.create(
            LOCAL,
            "local",
            SessionCreate(
                request_key="hot-workspace",
                target_key="target-workspace",
                context_policy=ContextPolicy(
                    max_bytes=18000, max_messages=32, reserved_output_tokens=512
                ),
            ),
        )
        request = WorkspaceTurnRequest(
            request_key="hot-main",
            text="D4_TASK",
            expected_revision=0,
            completion=Completion(
                required_paths=("calc.py", "answer.json"),
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
        assert finished.run_id is not None
        await app.service.wait(finished.run_id)
        snapshot = await app.store.snapshot(finished.run_id)
        sdk = WorkspaceSDK(app)
        view = await sdk.get(session.id, turn.id)
        captures = await port.rows()
        terminals = [item for item in snapshot.events if item.kind == "terminal"]
        assert len(captures) == len(terminals) == 1
        row, terminal = captures[0], terminals[0]
        assert row["run_id"] == finished.run_id and row["terminal_event_id"] == terminal.event_id
        assert terminal.data["status"] == snapshot.run.status == finished.status
        assert terminal.data["code"] == snapshot.run.code == finished.code
        assert terminal.data["observation_capture"]["capture_id"] == row["capture_id"]
        assert row["body"]["snapshot_hash"] == observe_snapshot(snapshot).snapshot_fingerprint
        assert row["phase"] == "pending"
        assert len(view.effects) == 3
        commands = [effect for effect in view.effects if effect.kind == "command"]
        assert len(commands) == 1 and commands[0].cleanup == "clean" and commands[0].exit_code == 0
        assert provider.calls == len(wire.requests) == 4
        assert evidence["replacement_entries"] == []

        async def counters(conn):
            return (await conn.execute(select(sql.capture_policies.c.counters))).scalar_one()

        counts = await app.store.work_read(counters)
        assert counts == {"seen": 1, "enqueued": 1, "dropped": 0, "acked": 0}
        inserts = [
            item
            for item in evidence["sql"]
            if "INSERT INTO observation_outbox" in item["statement"]
        ]
        updates = [
            item
            for item in evidence["sql"]
            if "UPDATE capture_policies" in item["statement"] and "counters=" in item["statement"]
        ]
        if case == "terminal-sql-drift":
            assert finished.status == "failed" and finished.code == "target_binding_changed"
            assert view.verification is not None and view.verification.state != "passed"
            assert len(evidence["drift"]) == 1 and evidence["drift"][0]["in_transaction"]
            assert len(inserts) == len(updates) == 2
            assert any(
                "ROLLBACK TO krim_d4_terminal" in item["statement"] for item in evidence["sql"]
            )
            assert all(item["in_transaction"] for item in inserts + updates)
            drift = evidence["drift"][0]
            same_connection = [
                item for item in evidence["sql"] if item["connection"] == drift["sync_connection"]
            ]
            savepoints = [item for item in same_connection if item["kind"] == "savepoint"]
            rollbacks = [item for item in same_connection if item["kind"] == "rollback"]
            terminal_attempts = [
                item for item in same_connection if item["kind"] == "terminal-event"
            ]
            verified = [
                item
                for item in same_connection
                if item["kind"] == "verification-update" and item["verification_state"] == "passed"
            ]
            run_updates = [
                item
                for item in same_connection
                if item["table"] == "runs"
                and item["operation"] == "update"
                and item["status"] in {"completed", "failed"}
            ]
            turn_updates = [
                item
                for item in same_connection
                if item["table"] == "turns"
                and item["operation"] == "update"
                and item["status"] in {"completed", "failed"}
            ]
            assert len(savepoints) == len(rollbacks) == len(verified) == 1
            assert len(terminal_attempts) == len(run_updates) == len(turn_updates) == 2
            assert [item["terminal_status"] for item in terminal_attempts] == [
                "completed",
                "failed",
            ]
            assert terminal_attempts[1]["terminal_code"] == "target_binding_changed"
            assert [item["status"] for item in run_updates] == ["completed", "failed"]
            assert [item["status"] for item in turn_updates] == ["completed", "failed"]
            assert savepoints[0]["index"] < terminal_attempts[0]["index"] < verified[0]["index"]
            assert verified[0]["index"] < inserts[0]["index"] < updates[0]["index"]
            assert updates[0]["index"] == drift["after_sql_index"] < rollbacks[0]["index"]
            assert (
                rollbacks[0]["index"]
                < terminal_attempts[1]["index"]
                < inserts[1]["index"]
                < updates[1]["index"]
            )
            assert terminal_attempts[0]["index"] < run_updates[0]["index"] < inserts[0]["index"]
            assert terminal_attempts[0]["index"] < turn_updates[0]["index"] < inserts[0]["index"]
            assert terminal_attempts[1]["index"] < run_updates[1]["index"] < inserts[1]["index"]
            assert terminal_attempts[1]["index"] < turn_updates[1]["index"] < inserts[1]["index"]
            assert drift["window"] == "capture-counter-UPDATE-returned"
            assert any(
                item["async_connection"] == drift["connection"]
                and item["sync_connection"] == drift["sync_connection"]
                and item["execution_id"] == drift["execution_id"]
                and item["sql_index"] == drift["after_sql_index"]
                for item in evidence["connection_map"]
            )
            assert all(
                item["connection"] == drift["sync_connection"]
                and item["engine"] == evidence["engine"]
                for item in inserts + updates + terminal_attempts + verified
            )
            assert (
                len(
                    {
                        item["dbapi_connection"]
                        for item in inserts + updates + terminal_attempts + verified
                    }
                )
                == 1
            )
            assert all(
                item["rowcount"] == 1
                for item in inserts
                + updates
                + terminal_attempts
                + verified
                + run_updates
                + turn_updates
            )
            assert all(
                item["run_id"] == finished.run_id
                for item in terminal_attempts + verified + inserts + run_updates
            )
            assert all(item["event_id"] == terminal.event_id for item in terminal_attempts)
            assert all(item["terminal_event_id"] == terminal.event_id for item in inserts)
            assert all(
                item["capture_id"] == row["capture_id"] for item in inserts + terminal_attempts
            )
            assert all(item["workspace_id"] == row["workspace_id"] for item in inserts + updates)
            assert all(item["turn_id"] == turn.id for item in turn_updates)
            assert all(item["counters"] == counts for item in updates)
            assert drift["run_id"] == finished.run_id and drift["capture_id"] == row["capture_id"]
            assert (
                drift["terminal_event_id"] == terminal.event_id
                and drift["counter_values"] == counts
            )
            assert inserts[1]["snapshot_hash"] == row["body"]["snapshot_hash"]
            assert inserts[0]["snapshot_hash"] != inserts[1]["snapshot_hash"]
            evidence["sequence_proof"] = {
                "savepoint": savepoints[0]["index"],
                "positive_terminal": terminal_attempts[0]["index"],
                "verification": verified[0]["index"],
                "positive_insert": inserts[0]["index"],
                "counter_return_before_drift": drift["after_sql_index"],
                "rollback": rollbacks[0]["index"],
                "negative_terminal": terminal_attempts[1]["index"],
                "negative_insert": inserts[1]["index"],
                "negative_counter": updates[1]["index"],
            }
        else:
            assert finished.status == "completed" and finished.code is None
            assert view.verification is not None and view.verification.state == "passed"
            assert len(inserts) == len(updates) == 1 and evidence["drift"] == []
            revision = await sdk.artifact(session.id, view.verification.revision_id)
            assert revision.producer_run_id == finished.run_id
            assert revision.producer_call_id == commands[0].call_id
        monkeypatch.setattr(verifier, "verify", original_verify)
        before_calls = provider.calls
        for status, code in [(finished.status, finished.code), ("aborted", "cancelled")]:
            assert (await app.store.finish(finished.run_id, status, code)) == snapshot.run
        assert await port.rows() == captures and await app.store.work_read(counters) == counts
        assert provider.calls == before_calls and evidence["replacement_entries"] == []
        assert (await sdk.by_key(session.id, request.request_key)).turn_id == turn.id
        evidence.update(
            run_id=finished.run_id,
            terminal=terminal.model_dump(mode="json"),
            snapshot_hash=row["body"]["snapshot_hash"],
            capture=row,
            counters=counts,
            command=commands[0].model_dump(mode="json"),
            provider_calls=provider.calls,
            view=view.model_dump(mode="json"),
        )
        notes["main"] = {
            "run_id": finished.run_id,
            "turn_id": turn.id,
            "status": finished.status,
            "code": finished.code,
            "h1": evidence,
        }
        if case == "pass-next-turn":
            revision_id = view.verification.revision_id
            follow = await app.sessions.submit(
                LOCAL,
                "local",
                session.id,
                WorkspaceTurnRequest(
                    request_key="hot-next",
                    text="D4_NEXT " + revision_id,
                    expected_revision=1,
                    completion=Completion(),
                ),
            )
            follow = await app.sessions.wait(LOCAL, "local", session.id, follow.id)
            await app.service.wait(follow.run_id)
            facts = await app.service.result(follow.run_id)
            assert follow.code == "completion_unverified"
            assert len(facts.tools) == 1 and facts.tools[0].name == "artifact_read"
            assert facts.tools[0].executor_started is True
            assert json.loads(base64.b64decode(facts.tools[0].value["data"])) == {"answer": 42}
            assert facts.tools[0].value["revision_id"] == revision_id
            assert provider.calls == before_calls + 2 == len(wire.requests)
            after = await port.rows()
            assert len(after) == 2 and row in after
            next_row = next(item for item in after if item["run_id"] == follow.run_id)
            next_snapshot = await app.store.snapshot(follow.run_id)
            assert (
                next_row["body"]["snapshot_hash"]
                == observe_snapshot(next_snapshot).snapshot_fingerprint
            )
            assert (await app.store.work_read(counters))["seen"] == 2
            evidence["next_turn"] = {
                "run_id": follow.run_id,
                "facts": facts.model_dump(mode="json"),
                "capture": next_row,
            }
    except BaseException as error:
        evidence["error"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        monkeypatch.setattr(verifier, "verify", original_verify)
        monkeypatch.setattr(AsyncConnection, "execute", execute)
        event.remove(app.store.engine.sync_engine, "after_cursor_execute", observed)
        try:
            await app.close()
        finally:
            evidence.update(
                store_closed=app.store.closed,
                producers_done=all(task.done() for task in app.service.tasks.values()),
                dispatcher_done=app.sessions._dispatcher.done(),
                provider_active=provider.active,
            )
            (tmp_path / "hot-workspace.json").write_text(json.dumps(evidence, indent=2))
        assert (
            evidence["store_closed"] and evidence["producers_done"] and evidence["dispatcher_done"]
        )
        assert evidence["provider_active"] == 0


@pytest.mark.parametrize(
    "kind",
    [
        "commit",
        "rollback",
        "return_lost",
        "verifier_rejected",
        "recover_interrupted",
        "workspace_completed",
    ],
)
async def test_run_terminal_outbox_atomic(tmp_path, monkeypatch, data_case, kind):
    if kind in {"workspace_completed", "verifier_rejected"}:
        await workspace_terminal(
            tmp_path,
            monkeypatch,
            data_case,
            "pass-next-turn" if kind == "workspace_completed" else "terminal-sql-drift",
        )
        return
    if kind == "recover_interrupted":
        from test_hot_observation_storage import recover_real_source

        facts = await recover_real_source(tmp_path)
        assert facts["status"] == "interrupted" and facts["captures"] == 1
        assert facts["provider_replay"] == 0 and facts["producer_wait"] == -9
        assert facts["producer_internal_cleanup"] == "unknown"
        return
    async with world(tmp_path) as w:
        seen = []
        source_engine = w.source.store.engine.sync_engine
        rollback = {
            "database": str(tmp_path / "source.sqlite"),
            "engine": id(source_engine),
            "timeline": [],
            "terminal_writes": [],
            "insert_faults": [],
            "finish": [],
            "snapshots": [],
            "confirmations": [],
            "confirmation_sql": [],
            "run_wait": [],
            "stage": "rollback call; World close follows test finally",
            "unknown": [],
        }
        finishing, confirming, listeners = set(), set(), []
        original_finish = w.source.store.finish
        original_snapshot = w.source.store.snapshot
        original_read = w.source.store.work_read
        original_run_wait = w.source.service.wait

        def exception_record(error):
            return {
                "type": type(error).__name__,
                "code": getattr(error, "code", None),
                "message": str(error),
                "identity": id(error),
            }

        def reject(conn, cursor, statement, parameters, context, executemany):
            values = getattr(context, "compiled_parameters", None) or []
            bound = values[0] if values else {}
            if "INSERT INTO run_events" in statement and bound.get("kind") == "terminal":
                rollback["terminal_writes"].append(
                    {
                        "run_id": bound.get("run_id"),
                        "event_id": bound.get("event_id"),
                        "connection": id(conn),
                        "dbapi_connection": id(conn.connection.dbapi_connection),
                        "in_transaction": conn.in_transaction(),
                    }
                )
                rollback["timeline"].append("terminal-insert-returned")
            if "INSERT INTO observation_outbox" in statement:
                seen.append(statement)
                rollback["insert_faults"].append(
                    {
                        "statement": statement,
                        "run_id": bound.get("run_id"),
                        "capture_id": bound.get("capture_id"),
                        "terminal_event_id": bound.get("terminal_event_id"),
                        "connection": id(conn),
                        "dbapi_connection": id(conn.connection.dbapi_connection),
                        "in_transaction": conn.in_transaction(),
                        "healthy": w.source.store.healthy,
                    }
                )
                rollback["timeline"].append("outbox-insert-returned-before-fault")
                raise OSError("real terminal transaction after INSERT")

        def confirmation_sql(conn, cursor, statement, parameters, context, executemany):
            if id(conn) in confirming:
                rollback["confirmation_sql"].append(
                    {
                        "statement": statement,
                        "connection": id(conn),
                        "in_transaction": conn.in_transaction(),
                    }
                )

        async def observed_snapshot(run_id):
            result = await original_snapshot(run_id)
            if run_id in finishing:
                link = result.session_context
                rollback["snapshots"].append(
                    {
                        "run_id": run_id,
                        "status": result.run.status,
                        "code": result.run.code,
                        "finished_at": result.run.finished_at,
                        "session_id": None if link is None else link.session_id,
                        "turn_id": None if link is None else link.turn_id,
                        "terminal_count": sum(item.kind == "terminal" for item in result.events),
                        "healthy": w.source.store.healthy,
                    }
                )
                rollback["timeline"].append("original-finish-first-snapshot-returned")
            return result

        async def observed_read(operation):
            if "finish.<locals>" not in operation.__qualname__:
                return await original_read(operation)

            async def read_confirmation(conn):
                identity = id(conn.sync_connection)
                confirming.add(identity)
                record = {"connection": identity, "in_transaction": conn.in_transaction()}
                rollback["confirmations"].append(record)
                rollback["timeline"].append("original-confirmation-entered")
                try:
                    result = await operation(conn)
                except BaseException as error:
                    confirming.discard(identity)
                    record["error"] = exception_record(error)
                    rollback["timeline"].append("original-confirmation-error")
                    raise
                confirming.discard(identity)
                record["result"] = result
                rollback["timeline"].append("original-confirmation-returned")
                return result

            return await original_read(read_confirmation)

        async def observed_finish(run_id, status, code=None):
            producer = w.source.service.tasks.get(run_id)
            record = {
                "run_id": run_id,
                "selected_status": status,
                "selected_code": code,
                "producer": None if producer is None else id(producer),
                "current_task": id(asyncio.current_task()),
            }
            rollback["finish"].append(record)
            finishing.add(run_id)
            rollback["timeline"].append("original-finish-entered")
            try:
                result = await original_finish(run_id, status, code)
            except BaseException as error:
                finishing.discard(run_id)
                record.update(error=exception_record(error), healthy=w.source.store.healthy)
                rollback["timeline"].append("original-finish-error")
                raise
            finishing.discard(run_id)
            record.update(result=result.model_dump(mode="json"), healthy=w.source.store.healthy)
            rollback["timeline"].append("original-finish-returned")
            return result

        async def observed_run_wait(run_id):
            producer = w.source.service.tasks.get(run_id)
            record = {
                "run_id": run_id,
                "producer": None if producer is None else id(producer),
                "done_at_entry": None if producer is None else producer.done(),
                "caller": id(asyncio.current_task()),
            }
            rollback["run_wait"].append(record)
            rollback["timeline"].append("session-called-original-run-wait")
            try:
                result = await original_run_wait(run_id)
            except BaseException as error:
                record["error"] = exception_record(error)
                record["done_after_wait"] = None if producer is None else producer.done()
                if producer is not None and producer.done() and not producer.cancelled():
                    observed = producer.exception()
                    record["producer_exception"] = (
                        None if observed is None else exception_record(observed)
                    )
                rollback["timeline"].append("original-run-wait-error")
                raise
            record["result"] = result.model_dump(mode="json")
            rollback["timeline"].append("original-run-wait-returned")
            return result

        original = w.source.store._write
        lost = []

        async def return_lost(operation):
            value = await original(operation)
            if "finish.<locals>.operation" in operation.__qualname__ and not lost:
                lost.append(True)
                raise OSError("actual terminal commit return lost")
            return value

        if kind == "rollback":
            event.listen(source_engine, "after_cursor_execute", reject)
            listeners.append(("after_cursor_execute", reject))
            event.listen(source_engine, "before_cursor_execute", confirmation_sql)
            listeners.append(("before_cursor_execute", confirmation_sql))
            monkeypatch.setattr(w.source.store, "finish", observed_finish)
            monkeypatch.setattr(w.source.store, "snapshot", observed_snapshot)
            monkeypatch.setattr(w.source.store, "work_read", observed_read)
            monkeypatch.setattr(w.source.service, "wait", observed_run_wait)
        if kind == "return_lost":
            monkeypatch.setattr(w.source.store, "_write", return_lost)
        try:
            if kind == "rollback":
                with pytest.raises(ServiceError, match="storage_unavailable") as returned_error:
                    await w.run()
                assert seen
                assert await w.port.rows() == []
                assert (await w.counters())["seen"] == 0

                async def count_terminal(conn):
                    return await conn.scalar(
                        select(func.count())
                        .select_from(run_events)
                        .where(run_events.c.kind == "terminal")
                    )

                assert await w.source.store.work_read(count_terminal) == 0
                assert len(rollback["finish"]) == len(rollback["terminal_writes"]) == len(seen) == 1
                assert len(rollback["insert_faults"]) == len(rollback["snapshots"]) == 1
                run_id = rollback["insert_faults"][0]["run_id"]
                producer = w.source.service.tasks[run_id]
                assert producer.done() and not producer.cancelled()
                assert returned_error.value is producer.exception()
                error_identity = id(returned_error.value)
                assert rollback["finish"][0]["error"]["identity"] == error_identity
                assert rollback["finish"][0]["error"]["code"] == "storage_unavailable"
                assert rollback["finish"][0]["healthy"] is False
                assert (
                    rollback["finish"][0]["producer"]
                    == rollback["finish"][0]["current_task"]
                    == id(producer)
                )
                assert rollback["finish"][0]["selected_status"] == "completed"
                assert (
                    rollback["terminal_writes"][0]["run_id"]
                    == rollback["finish"][0]["run_id"]
                    == run_id
                )
                assert (
                    rollback["terminal_writes"][0]["connection"]
                    == rollback["insert_faults"][0]["connection"]
                )
                assert (
                    rollback["terminal_writes"][0]["dbapi_connection"]
                    == rollback["insert_faults"][0]["dbapi_connection"]
                )
                assert (
                    rollback["terminal_writes"][0]["in_transaction"]
                    and rollback["insert_faults"][0]["in_transaction"]
                )
                assert len(rollback["run_wait"]) == 1
                waited = rollback["run_wait"][0]
                assert waited["run_id"] == run_id and waited["producer"] == id(producer)
                assert waited["done_at_entry"] and waited["done_after_wait"]
                assert (
                    waited["error"]["identity"]
                    == waited["producer_exception"]["identity"]
                    == error_identity
                )
                assert len(rollback["confirmations"]) == 1
                assert rollback["confirmations"][0]["result"] is False
                assert rollback["confirmation_sql"] and all(
                    item["statement"].lstrip().upper().startswith("SELECT")
                    and item["in_transaction"]
                    for item in rollback["confirmation_sql"]
                )
                assert rollback["timeline"] == [
                    "original-finish-entered",
                    "terminal-insert-returned",
                    "outbox-insert-returned-before-fault",
                    "original-finish-first-snapshot-returned",
                    "original-confirmation-entered",
                    "original-confirmation-returned",
                    "original-finish-error",
                    "session-called-original-run-wait",
                    "original-run-wait-error",
                ]
                snapshot = await original_snapshot(run_id)
                link = snapshot.session_context
                assert link is not None
                turn = await w.source.sessions.turn(
                    LOCAL, link.workspace_id, link.session_id, link.turn_id
                )
                assert turn.run_id == snapshot.run.id == run_id
                assert snapshot.run.status in {"queued", "running"} and turn.status in {
                    "queued",
                    "running",
                }
                assert snapshot.run.finished_at is None and turn.finished_at is None
                assert snapshot.run.code is None and turn.code is None
                assert rollback["snapshots"][0]["run_id"] == run_id
                assert rollback["snapshots"][0]["turn_id"] == turn.id
                assert rollback["snapshots"][0]["session_id"] == turn.session_id
                assert rollback["snapshots"][0]["terminal_count"] == 0
                assert w.source.store.healthy is False
                tasks_before = dict(w.source.service.tasks)
                with pytest.raises(ServiceError, match="storage_unavailable"):
                    await w.source.sessions.create(
                        LOCAL,
                        "local",
                        SessionCreate(request_key="rollback-refused-admission", target_key="demo"),
                    )
                assert w.source.service.tasks == tasks_before
                assert len(w.wire.requests) == 2 and len(w.wire.executions) == 1
                assert w.evaluation.owner.spawn_calls == 0
                assert len(rollback["finish"]) == len(rollback["terminal_writes"]) == len(seen) == 1
                rollback.update(
                    run_id=run_id,
                    turn_id=turn.id,
                    session_id=turn.session_id,
                    caller_error=exception_record(returned_error.value),
                    producer={
                        "identity": id(producer),
                        "done": producer.done(),
                        "cancelled": producer.cancelled(),
                        "exception": exception_record(producer.exception()),
                    },
                    persisted_run=snapshot.run.model_dump(mode="json"),
                    persisted_turn=turn.model_dump(mode="json"),
                    healthy=w.source.store.healthy,
                    admission="actual SessionService.create refused storage_unavailable",
                    source_requests=len(w.wire.requests),
                    source_executions=len(w.wire.executions),
                    eval_target_calls=w.evaluation.owner.spawn_calls,
                )
            else:
                snap = await w.run()
                rows = await w.port.rows()
                # 首个预定行为RED：真实终态后缺少同事务outbox，而非导入/材料失败。
                assert len(rows) == 1
                terminal = next(e for e in snap.events if e.kind == "terminal")
                assert (
                    rows[0]["run_id"] == snap.run.id
                    and rows[0]["terminal_event_id"] == terminal.event_id
                )
                assert terminal.data["observation_capture"]["capture_id"] == rows[0]["capture_id"]
                assert (await w.counters())["seen"] == 1
                if kind == "return_lost":
                    assert lost and snap.run.status == "completed"
        finally:
            if kind == "rollback":
                for name, callback in listeners:
                    event.remove(source_engine, name, callback)
                monkeypatch.setattr(w.source.store, "finish", original_finish)
                monkeypatch.setattr(w.source.store, "snapshot", original_snapshot)
                monkeypatch.setattr(w.source.store, "work_read", original_read)
                monkeypatch.setattr(w.source.service, "wait", original_run_wait)
                run_id = rollback["finish"][0]["run_id"] if rollback["finish"] else None
                producer = w.source.service.tasks.get(run_id)
                rollback["producer_at_test_finally"] = {
                    "identity": None if producer is None else id(producer),
                    "done": None if producer is None else producer.done(),
                    "cancelled": None if producer is None else producer.cancelled(),
                }
                if producer is not None and producer.done() and not producer.cancelled():
                    error = producer.exception()
                    rollback["producer_at_test_finally"]["exception"] = (
                        None if error is None else exception_record(error)
                    )
                rollback.update(
                    healthy_at_test_finally=w.source.store.healthy,
                    source_requests_at_test_finally=len(w.wire.requests),
                    source_executions_at_test_finally=len(w.wire.executions),
                    eval_target_calls_at_test_finally=w.evaluation.owner.spawn_calls,
                )
                rollback["unknown"] = [
                    name
                    for name in ("finish", "snapshots", "confirmations", "run_wait")
                    if not rollback[name]
                ]
                (tmp_path / "rollback-causality.json").write_text(json.dumps(rollback, indent=2))


@pytest.mark.parametrize(
    "kind",
    [
        "sampled",
        "not_sampled",
        "failure_override",
        "feedback_override",
        "feedback_repeat",
        "feedback_conflict",
        "cross_scope",
        "source_binding_drift",
        "redaction",
        "two_authors_same_key",
        "admin_consumer",
        "multi_feedback_revision",
    ],
)
async def test_outbox_sampling_feedback_scope_redaction(tmp_path, monkeypatch, kind):
    excluded = kind in {"not_sampled", "feedback_override", "failure_override"}
    async with world(tmp_path, numerator=0 if excluded else 1, denominator=1) as w:
        snap = await w.run("/fail" if kind == "failure_override" else "/echo PRIVATE-DO-NOT-EXPORT")
        before_calls = len(w.wire.requests)
        if kind == "cross_scope":
            foreign = KrimOutboxPort(
                w.source.store,
                w.source.sessions,
                LOCAL,
                CapturePolicy(workspace_id="foreign", scope="foreign", enabled=True),
            )
            with pytest.raises(ServiceError, match="not_found"):
                await foreign.rows()
            assert len(w.wire.requests) == before_calls
            return
        if kind in {
            "feedback_override",
            "feedback_repeat",
            "feedback_conflict",
            "two_authors_same_key",
            "admin_consumer",
            "multi_feedback_revision",
        }:
            first = await w.feedback(snap.run.id, key="same")
            if kind == "feedback_repeat":
                assert await w.feedback(snap.run.id, key="same") == first
            if kind == "feedback_conflict":
                with pytest.raises(ServiceError, match="key_conflict"):
                    await w.feedback(snap.run.id, key="same", reason="unsafe")
            if kind in {"two_authors_same_key", "multi_feedback_revision"}:
                await w.feedback(snap.run.id, actor="bob", key="same")
            if kind == "multi_feedback_revision":
                await w.feedback(snap.run.id, actor="bob", key="other")
        if kind == "source_binding_drift":
            original = w.port.claim

            async def drift(**kwargs):
                result = await original(**kwargs)
                w.views.actors = {"local": LOCAL}
                return result

            monkeypatch.setattr(w.port, "claim", drift)
            with pytest.raises(ServiceError, match="target_binding_changed"):
                await w.flow.consume_once()
            assert await receipt_rows(w) == []
            return
        receipts = await w.flow.consume_once()
        if kind == "not_sampled":
            assert receipts == [] and (await w.counters())["enqueued"] == 0
        else:
            assert receipts
            assert len({r.observation_id for r in receipts}) == 1
            observations = [r for r in await receipt_rows(w) if r.get("kind") == "observation"]
            assert len(observations) == 1
            assert observations[0]["eligibility"] == "needs_context"
            if kind == "redaction":
                assert "PRIVATE-DO-NOT-EXPORT" not in json.dumps(observations)
                assert "PRIVATE-DO-NOT-EXPORT" not in json.dumps(await w.port.rows())
            feedback = [r for r in receipts if r.feedback_key is not None]
            if kind in {
                "feedback_override",
                "feedback_repeat",
                "feedback_conflict",
                "admin_consumer",
            }:
                assert len(feedback) == 1 and feedback[0].actor == "alice"
                assert feedback[0].actor != w.port.principal.principal_id
            if kind == "two_authors_same_key":
                assert {r.actor for r in feedback} == {"alice", "bob"}
                assert len({r.candidate_id for r in feedback}) == 2
                assert len({r.key for r in feedback}) == 2 and all(
                    len(r.key) == 48 for r in feedback
                )
            if kind == "multi_feedback_revision":
                assert len(feedback) == 3 and len({r.candidate_id for r in feedback}) == 3
            for r in feedback:
                candidate = await w.evaluation.repository.candidate("local", r.candidate_id)
                assert candidate["signals"][0]["actor"] == r.actor
        assert len(w.wire.requests) == before_calls and w.evaluation.owner.spawn_calls == 0
        assert await w.flow.consume_once() == []


@pytest.mark.parametrize(
    "kind",
    [
        "queue_full",
        "history_full",
        "claim_race",
        "import_return_lost",
        "ack_return_lost",
        "cancel_claim",
        "cancel_import",
        "cancel_ack",
        "restart",
        "terminal_repeat",
        "partial_feedback_commit_lost",
        "feedback_ack_race",
        "actor_readonly_resume",
    ],
)
async def test_outbox_backpressure_and_delivery(tmp_path, monkeypatch, kind):
    async with world(tmp_path, pending_limit=2, retained_limit=4) as w:
        snap = await w.run()
        if kind in {"queue_full", "history_full"}:
            if kind == "history_full":
                await w.flow.consume_once()
            for _ in range(2 if kind == "queue_full" else 4):
                more = await w.run()
                assert more.run.status == "completed"
                if kind == "history_full":
                    await w.flow.consume_once()
            counters = await w.counters()
            assert counters["dropped"] == 1
            assert len(await w.port.rows()) == (2 if kind == "queue_full" else 4)
            return
        await w.feedback(snap.run.id, key="A")
        if kind == "claim_race":
            a, b = await asyncio.gather(w.port.claim(), w.port.claim())
            assert len(a) + len(b) == 1
            return
        before = len(w.wire.requests)
        if kind == "terminal_repeat":
            await w.source.store.finish(snap.run.id, snap.run.status, snap.run.code)
            await w.source.store.finish(snap.run.id, snap.run.status, snap.run.code)
            assert (await w.counters())["seen"] == 1 and len(await w.port.rows()) == 1
        if kind == "restart":
            await w.evaluation.close()
            w.evaluation = await open_eval(tmp_path / "eval.sqlite")
            views = ActorViews(w.port, w.evaluation.repository, w.evaluation.kernel, w.actors)
            w.flow = HotObservationFlow(
                w.port,
                w.evaluation.repository,
                w.evaluation.kernel,
                w.flow.destination_id,
                views.resolve,
                views.binding,
                1,
            )
        if kind == "import_return_lost":
            original = w.evaluation.repository._write
            fired = []

            async def lost(operation):
                result = await original(operation)
                if not fired:
                    fired.append(True)
                    raise OSError("actual destination commit return lost")
                return result

            monkeypatch.setattr(w.evaluation.repository, "_write", lost)
        if kind == "partial_feedback_commit_lost":
            await w.feedback(snap.run.id, actor="bob", key="B")
            original = w.evaluation.repository.import_observations
            writes = []
            claimed = []
            claim_rows = w.port.claim

            async def remember_claim(**kwargs):
                result = await claim_rows(**kwargs)
                claimed.extend(result)
                return result

            monkeypatch.setattr(w.port, "claim", remember_claim)

            async def partial(scope, key, digest, rows=None, guard=lambda: None):
                result = await original(scope, key, digest, rows, guard)
                if rows is not None:
                    writes.append(key)
                    if len(writes) == 2:
                        raise OSError("actor A committed; return lost before B")
                return result

            monkeypatch.setattr(w.evaluation.repository, "import_observations", partial)
            with pytest.raises(OSError, match="actor A committed"):
                await w.flow.consume_once()
            assert (await w.port.rows())[0]["phase"] != "acked"
            assert len(writes) == 2 and len(w.wire.requests) == before
            assert w.evaluation.owner.spawn_calls == 0
            assert len(claimed) == 1
            frozen = await w.port.units(claimed[0])
            states = [value["state"] for value in frozen.values()]
            assert states == ["complete", "submitted", "not_started"]
            with pytest.raises(ServiceError, match="execution_unknown"):
                await w.flow.recover_claim(claimed[0])
            assert len(writes) == 2 and await w.port.units(claimed[0]) == frozen
            from sqlalchemy import update

            async with w.source.store.engine.begin() as conn:
                changed = await conn.execute(
                    update(sql.observation_outbox)
                    .where(sql.observation_outbox.c.capture_id == claimed[0].record.capture_id)
                    .values(claim_until=0)
                )
                assert changed.rowcount == 1
            recovered = await w.flow.consume_once()
            assert len(recovered) == 3 and {item.actor for item in recovered} == {
                "local",
                "alice",
                "bob",
            }
            assert len(writes) == len(set(writes)) == 3
            assert len(claimed) == 2 and claimed[1].generation == claimed[0].generation + 1
            assert {item.key for item in recovered} == set(frozen)
            assert len({item.observation_id for item in recovered}) == 1
            assert (await w.port.rows())[0]["phase"] == "acked"
            assert len(w.wire.requests) == before and w.evaluation.owner.spawn_calls == 0
            return
        if kind in {"feedback_ack_race", "ack_return_lost", "actor_readonly_resume"}:
            original = w.port.ack
            held = []
            delivered = []

            async def ack(claim, receipts):
                held.append(claim)
                delivered.append(receipts)
                if kind == "feedback_ack_race":
                    await w.feedback(snap.run.id, actor="bob", key="new-R")
                if kind == "actor_readonly_resume":
                    raise OSError("before source ack; destination receipts known")
                await original(claim, receipts)
                if kind == "ack_return_lost":
                    raise OSError("source ack committed; return lost")

            monkeypatch.setattr(w.port, "ack", ack)
            with pytest.raises((ServiceError, OSError)):
                await w.flow.consume_once()
            if kind == "ack_return_lost":
                assert (await w.port.rows())[0]["phase"] == "acked"
            elif kind == "feedback_ack_race":
                row = (await w.port.rows())[0]
                assert row["feedback_revision"] == 2 and row["phase"] != "acked"
                with pytest.raises(ServiceError, match="revision_conflict"):
                    await original(held[0], [])
                recovered = await w.flow.consume_once()
                assert len(recovered) == 3 and held[1].revision == 2
                assert held[1].generation == held[0].generation + 1
                original_receipts = {item.key: item for item in delivered[0]}
                assert {
                    item.key: item for item in recovered if item.key in original_receipts
                } == original_receipts
                assert (await w.port.rows())[0]["phase"] == "acked"
            else:
                reader = await open_eval(tmp_path / "eval.sqlite", readonly=True)
                try:
                    views = ActorViews(w.port, reader.repository, reader.kernel, w.actors)
                    flow = HotObservationFlow(
                        w.port,
                        reader.repository,
                        reader.kernel,
                        w.flow.destination_id,
                        views.resolve,
                        views.binding,
                        1,
                    )
                    recovered = await flow.recover_claim(held[0])
                    assert len(recovered) == 2 and recovered[1].actor == "alice"
                    assert reader.owner is None
                finally:
                    await reader.close()
            assert len(w.wire.requests) == before and w.evaluation.owner.spawn_calls == 0
            return
        if kind.startswith("cancel_"):
            entered, release = asyncio.Event(), asyncio.Event()
            target = w.port if kind != "cancel_import" else w.evaluation.repository
            name = (
                "claim"
                if kind == "cancel_claim"
                else "ack"
                if kind == "cancel_ack"
                else "import_observations"
            )
            original = getattr(target, name)

            async def barrier(*args, **kwargs):
                value = await original(*args, **kwargs)
                entered.set()
                await release.wait()
                return value

            monkeypatch.setattr(target, name, barrier)
            task = asyncio.create_task(w.flow.consume_once())
            try:
                async with asyncio.timeout(5):
                    await entered.wait()
                task.cancel()
            finally:
                release.set()
                await asyncio.gather(task, return_exceptions=True)
            assert task.done() and len(w.wire.requests) == before
            if kind == "cancel_ack":
                assert (await w.port.rows())[0]["phase"] == "acked"
            else:
                assert (await w.port.rows())[0]["phase"] != "acked"
            return
        receipts = await w.flow.consume_once()
        assert len(receipts) == 2 and {r.actor for r in receipts} == {"local", "alice"}
        assert len(w.wire.requests) == before and w.evaluation.owner.spawn_calls == 0
        assert await w.flow.consume_once() == []


@pytest.mark.parametrize("kind", ["unconfirmed", "manual_reconstructed"])
async def test_hot_candidate_to_frozen_dataset_and_report(tmp_path, kind):
    from importlib.resources import files

    async with world(tmp_path) as w:
        snap = await w.run()
        await w.feedback(snap.run.id)
        receipts = await w.flow.consume_once()
        chosen = next(r for r in receipts if r.feedback_key is not None)
        draft = await w.evaluation.repository.candidate("local", chosen.candidate_id)
        assert draft["eligibility"] == "needs_context"
        request = FreezeConfirmed(
            request_key="hot-freeze",
            name="hot-rebuilt",
            version="1",
            confirmations=[chosen.candidate_id],
        )
        if kind == "unconfirmed":
            with pytest.raises(ServiceError):
                await w.evaluation.continuous.freeze_dataset(request)
            assert w.evaluation.owner.spawn_calls == 0
            return
        body = json.loads(
            files("krim_agent.evaluation.examples").joinpath("reconciliation.v1.json").read_text()
        )
        dataset = DatasetSpec.model_validate(body | {"cases": body["cases"][:1]})
        confirmed = await w.evaluation.continuous.confirm_case(
            ConfirmationRequest(
                request_key="human-rebuild",
                candidate_id=chosen.candidate_id,
                expected_revision=draft["revision"],
                expected_hash=draft["hash"],
                case=dataset.cases[0],
                context="manual_reconstructed",
                reason="人工提供新的完整初态与独立参考",
            )
        )
        frozen = await w.evaluation.continuous.freeze_dataset(
            request.model_copy(update={"confirmations": [confirmed["id"]]})
        )
        task = await w.evaluation.continuous.submit(
            ContinuousRequest(
                request_key="hot-execute",
                mode="execute",
                dataset=DatasetRef.model_validate(frozen["dataset_ref"]),
                dataset_envelope_fp=frozen["dataset_envelope_fp"],
                metrics=continuous_rules(w.evaluation.repository.metric_registry),
            )
        )
        terminal = await w.evaluation.kernel.wait(task["id"])
        report = await w.evaluation.repository.report(task["id"])
        assert terminal["status"] == "completed" and report["planned_count"] == 1
        assert w.evaluation.owner.spawn_calls == 1
        original = await w.evaluation.repository.observation("local", chosen.observation_id)
        assert (
            original["eligibility"] == "needs_context"
            and original["output"]["cleanup"] == "unknown"
        )
        before_calls = len(w.wire.requests)
        await w.feedback(snap.run.id, actor="bob", key="after-human-confirmation")
        delivered = await w.flow.consume_once()
        assert len(delivered) == 3
        assert next(item for item in delivered if item.key == chosen.key) == chosen
        assert len(w.wire.requests) == before_calls and w.evaluation.owner.spawn_calls == 1
        claims = await w.port.rows()
        if claims:
            from krim_agent.evaluation.hot_types import CaptureRecord

            record = CaptureRecord.model_validate(claims[0]["body"])
            a = import_request(record, w.flow.destination_id, "alice", record.feedback[0])
            b = import_request(record, w.flow.destination_id, "alice", record.feedback[0])
            assert a == b and len(a.request_key) == 48
            assert a.request_key != delivery_key(
                record.source_id, record.capture_id, w.flow.destination_id, "local", "base", "alice"
            )


def _hot_eval_owner(evaluation):
    """记录传入的实际owner；restart前后对象分别保留，不能借旧对象证明新对象关闭。"""
    owner = evaluation.owner
    return {
        "application_id": id(evaluation),
        "repository_id": id(evaluation.repository),
        "kernel_id": id(evaluation.kernel),
        "owner_id": None if owner is None else id(owner),
        "closed": evaluation.repository.closed,
        "writer_fd": evaluation.repository._writer_fd,
        "producers_done": all(task.done() for task in evaluation.kernel.tasks.values()),
        "producers": [
            {"id": id(task), "done": task.done(), "cancelled": task.cancelled()}
            for task in evaluation.kernel.tasks.values()
        ],
        "spawn_calls": None if owner is None else owner.spawn_calls,
        "processes": []
        if owner is None
        else [
            {
                "pid": work.process.pid,
                "wait": work.process.returncode,
                "pidfd": work.pidfd,
                "collector_done": work.collector is not None and work.collector.done(),
                "stdout_eof": work.stdout_eof,
                "stderr_eof": work.stderr_eof,
                "streams_done": all(task.done() for task in work.stream_tasks),
            }
            for work in owner.jobs.values()
        ],
    }


@pytest.mark.parametrize("kind", ["request_drift", "target_drift"])
async def test_terminal_confirmation_snapshot_integrity(tmp_path, monkeypatch, kind):
    """真实finish已提交丢回程；首读之后才改自有SQL，第二读必须只读拒绝。"""
    from sqlalchemy import update

    from krim_agent.contracts import fingerprint
    from krim_agent.storage.models import runs

    async with world(tmp_path) as w:
        entered, release = asyncio.Event(), asyncio.Event()
        source = w.source
        original_write, original_snapshot = source.store._write, source.store.snapshot
        original_read, original_finish = source.store.work_read, source.store.finish
        committed, first_snapshots, confirmations, finish_errors = [], [], [], []
        confirmation_connections = set()
        evidence = {
            "kind": kind,
            "database": str(tmp_path / "source.sqlite"),
            "timeline": [],
            "confirmation_sql": [],
            "error": None,
        }
        consumer = producer = None

        async def lost(operation):
            result = await original_write(operation)
            if "finish.<locals>.operation" in operation.__qualname__ and not committed:
                committed.append(result.id)
                evidence["timeline"].append(
                    {"window": "actual-finish-commit-returned", "run_id": result.id}
                )
                raise OSError("actual finish committed; caller lost return")
            return result

        async def snapshot(run_id):
            result = await original_snapshot(run_id)
            if (
                committed == [run_id]
                and not first_snapshots
                and asyncio.current_task() is source.service.tasks.get(run_id)
            ):
                first_snapshots.append(result)
                evidence["timeline"].append(
                    {
                        "window": "actual-first-snapshot-returned",
                        "run_id": run_id,
                        "request": result.request.model_dump(mode="json"),
                        "target": result.target,
                    }
                )
                entered.set()
                await release.wait()
            return result

        async def read(operation):
            if "finish.<locals>" not in operation.__qualname__:
                return await original_read(operation)

            async def observed(conn):
                identity = id(conn.sync_connection)
                confirmation_connections.add(identity)
                evidence["timeline"].append(
                    {
                        "window": "actual-second-read-entered",
                        "connection": identity,
                        "in_transaction": conn.in_transaction(),
                    }
                )
                try:
                    result = await operation(conn)
                    confirmations.append(result)
                    evidence["timeline"].append(
                        {"window": "actual-second-read-returned", "result": result}
                    )
                    return result
                finally:
                    confirmation_connections.discard(identity)

            return await original_read(observed)

        async def finish(*args, **kwargs):
            try:
                return await original_finish(*args, **kwargs)
            except ServiceError as error:
                finish_errors.append(
                    {
                        "code": error.code,
                        "healthy": source.store.healthy,
                        "producer": id(asyncio.current_task()),
                    }
                )
                raise

        def sql_observer(conn, cursor, statement, parameters, context, executemany):
            if id(conn) in confirmation_connections:
                evidence["confirmation_sql"].append(
                    {
                        "statement": statement,
                        "connection": id(conn),
                        "in_transaction": conn.in_transaction(),
                    }
                )

        monkeypatch.setattr(source.store, "_write", lost)
        monkeypatch.setattr(source.store, "snapshot", snapshot)
        monkeypatch.setattr(source.store, "work_read", read)
        monkeypatch.setattr(source.store, "finish", finish)
        event.listen(source.store.engine.sync_engine, "before_cursor_execute", sql_observer)
        try:
            consumer = asyncio.create_task(w.run())
            async with asyncio.timeout(5):
                await entered.wait()
            assert len(committed) == len(first_snapshots) == 1
            frozen = first_snapshots[0]
            run_id = committed[0]
            producer = source.service.tasks[run_id]
            assert not producer.done()
            before_calls, before_tools = len(w.wire.requests), len(w.wire.executions)
            before_target = w.evaluation.owner.spawn_calls
            async with source.store.engine.begin() as conn:
                if kind == "request_drift":
                    changed = frozen.request.model_dump(mode="json")
                    changed["text"] += " drift after first read"
                    assert type(frozen.request).model_validate(changed)
                    assert fingerprint(changed) != fingerprint(
                        frozen.request.model_dump(mode="json")
                    )
                    result = await conn.execute(
                        update(runs).where(runs.c.id == run_id).values(request=changed)
                    )
                else:
                    changed = json.loads(json.dumps(frozen.target))
                    assert type(changed["limits"]["max_tools_per_step"]) is int
                    assert changed["limits"]["max_tools_per_step"] == 4
                    changed["limits"]["max_tools_per_step"] = 4.0
                    # Python的宽相等会漏掉4/4.0；规范JSON指纹必须区分两者。
                    assert changed == frozen.target and fingerprint(changed) != fingerprint(
                        frozen.target
                    )
                    result = await conn.execute(
                        update(runs).where(runs.c.id == run_id).values(target=changed)
                    )
                assert result.rowcount == 1
            evidence["timeline"].append(
                {
                    "window": "actual-drift-committed",
                    "field": kind,
                    "value": changed,
                    "source_calls": before_calls,
                    "tools": before_tools,
                    "target_calls": before_target,
                }
            )
            release.set()
            async with asyncio.timeout(5):
                outcomes = await asyncio.gather(producer, consumer, return_exceptions=True)
            assert producer.done() and consumer.done()
            assert (
                isinstance(outcomes[0], ServiceError) and outcomes[0].code == "storage_unavailable"
            )
            # Session轮询可以先看到已提交Turn；失败证明取自原finish而非猜caller必抛。
            evidence["caller"] = {
                "type": type(outcomes[1]).__name__,
                "code": getattr(outcomes[1], "code", None),
                "returned_snapshot": not isinstance(outcomes[1], BaseException),
            }
            assert confirmations == [False]
            assert finish_errors == [
                {"code": "storage_unavailable", "healthy": False, "producer": id(producer)}
            ]
            assert source.store.healthy is False
            assert evidence["confirmation_sql"]
            assert all(
                item["statement"].lstrip().upper().startswith("SELECT") and item["in_transaction"]
                for item in evidence["confirmation_sql"]
            )
            windows = [item["window"] for item in evidence["timeline"]]
            assert windows == [
                "actual-finish-commit-returned",
                "actual-first-snapshot-returned",
                "actual-drift-committed",
                "actual-second-read-entered",
                "actual-second-read-returned",
            ]
            assert len(w.wire.requests) == before_calls == 2
            assert len(w.wire.executions) == before_tools == 1
            assert w.evaluation.owner.spawn_calls == before_target == 0
            captures = await w.port.rows()
            assert len(captures) == 1 and captures[0]["run_id"] == run_id
            assert (await w.counters()) == {"seen": 1, "enqueued": 1, "dropped": 0, "acked": 0}
            async with source.store.engine.begin() as conn:
                actual = (
                    (await conn.execute(select(runs).where(runs.c.id == run_id))).mappings().one()
                )
                terminals = (
                    (
                        await conn.execute(
                            select(run_events).where(
                                run_events.c.run_id == run_id, run_events.c.kind == "terminal"
                            )
                        )
                    )
                    .mappings()
                    .all()
                )
            assert len(terminals) == 1 and terminals[0]["event_id"] == "terminal:" + run_id
            assert actual["status"] == frozen.run.status == "completed"
            assert actual["finished_at"] == frozen.run.finished_at
            assert fingerprint(
                actual["request" if kind == "request_drift" else "target"]
            ) == fingerprint(changed)
            evidence.update(
                run_id=run_id,
                producer_id=id(producer),
                producer_done=producer.done(),
                consumer_done=consumer.done(),
                finish_errors=finish_errors,
                confirmation_results=confirmations,
                healthy=source.store.healthy,
                captures=captures,
                source_requests=len(w.wire.requests),
                source_executions=len(w.wire.executions),
                target_calls=w.evaluation.owner.spawn_calls,
            )
        except BaseException as error:
            evidence["error"] = {"type": type(error).__name__, "message": str(error)}
            raise
        finally:
            release.set()
            if consumer is not None and not consumer.done():
                consumer.cancel()
            tasks = [task for task in (producer, consumer) if task is not None]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            event.remove(source.store.engine.sync_engine, "before_cursor_execute", sql_observer)
            monkeypatch.setattr(source.store, "_write", original_write)
            monkeypatch.setattr(source.store, "snapshot", original_snapshot)
            monkeypatch.setattr(source.store, "work_read", original_read)
            monkeypatch.setattr(source.store, "finish", original_finish)
            evidence["joined"] = [
                {"id": id(task), "done": task.done(), "cancelled": task.cancelled()}
                for task in tasks
            ]
            (tmp_path / "confirmation-integrity.json").write_text(json.dumps(evidence, indent=2))


@pytest.mark.parametrize(
    "kind",
    ["terminal_base", "before_manifest", "completion_growth", "long_identity"],
)
async def test_hot_delivery_capacity_admission(tmp_path, monkeypatch, kind):
    """真实准入与交付边界；只记录原方法返回后的持久事实，不投影容量结论。"""
    from collections import Counter

    from sqlalchemy.pool import Pool

    from krim_agent.evaluation.domain import clone, neutral_fingerprint
    from krim_agent.evaluation.hot_types import CaptureRecord, receipts_root, request_hash
    from krim_agent.execution.linux import fds

    evidence = {
        "kind": kind,
        "states": [],
        "feedback_attempts": [],
        "claims": [],
        "save_attempts": [],
        "acks": [],
        "pool": [],
        "evidence_errors": [],
        "error": None,
    }
    primary = None
    value = provider = None
    listeners = []
    before_fds = fds()
    before_tasks = asyncio.all_tasks()

    def canonical(value):
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        )

    def source_rows_json(rows):
        if type(rows) is not list:
            raise ValueError("capacity_evidence_rows_type")
        projected = []
        for row in rows:
            if type(row) is not dict:
                raise ValueError("capacity_evidence_row_type")
            converted = {}
            for key, item in row.items():
                if not isinstance(key, str):
                    raise ValueError("capacity_evidence_non_string_column")
                native_key = str.__str__(key)
                if type(native_key) is not str or native_key in converted:
                    raise ValueError("capacity_evidence_column_collision")
                converted[native_key] = item
            projected.append(converted)
        return clone(projected)

    def note_error(error):
        return {
            "type": type(error).__name__,
            "code": getattr(error, "code", None),
            "message": str(error),
        }

    for event_name in ["connect", "checkout", "reset", "checkin", "close"]:

        def observed(*args, event_name=event_name):
            row = {"event": event_name, "connection": id(args[0])}
            if event_name == "reset":
                row["asyncio_safe"] = args[2].asyncio_safe
            evidence["pool"].append(row)

        event.listen(Pool, event_name, observed)
        listeners.append((event_name, observed))

    changes = {}
    if kind == "terminal_base":
        # 每个组件小于255字节，路径相对且不重复；只增加合法策略元数据。
        changes["artifact_paths"] = [
            f"reports/{index:02d}/" + "artifact-" * 12 + "value.json" for index in range(16)
        ]

    try:
        async with world(tmp_path, **changes) as w:
            value = w
            provider = w.source.service.bindings["demo"].loop.provider
            evidence["provider_identity"] = {
                "binding": "demo",
                "object": id(provider),
                "type": type(provider).__name__,
            }
            if kind == "long_identity":
                actor = "a" * 64
                principal = PrincipalContext(
                    actor, 1, (Membership(workspace_id="local", role="admin"),)
                )
                w.actors[actor] = principal
                w.views.actors[actor] = principal
            else:
                actor = "alice"

            async def state(stage):
                source_rows = source_rows_json(await w.port.rows())
                destination = clone(await receipt_rows(w))
                current = {
                    "rows": source_rows,
                    "counters": clone(await w.counters()),
                    "destination": destination,
                    "provider_calls": provider.calls,
                    "wire_requests": len(w.wire.requests),
                    "tool_executions": len(w.wire.executions),
                    "target_calls": w.evaluation.owner.spawn_calls,
                }
                sizes = [
                    {
                        "capture_id": row["capture_id"],
                        "record_bytes": len(canonical(row["body"]).encode()),
                        "delivery_bytes": len(canonical(row["delivery"]).encode()),
                        "combined_bytes": len(
                            canonical({"record": row["body"], "delivery": row["delivery"]}).encode()
                        ),
                    }
                    for row in source_rows
                ]
                evidence["states"].append({"stage": stage, "state": current, "bytes": sizes})
                assert all(item["combined_bytes"] <= 4096 for item in sizes)
                return current

            original_claim = w.port.claim
            original_save = w.port.save_unit
            original_ack = w.port.ack

            async def observed_claim(**kwargs):
                claims = await original_claim(**kwargs)
                current = await state("claim-returned")
                for claim in claims:
                    assert len(current["rows"]) == 1
                    row = current["rows"][0]
                    assert claim.record == CaptureRecord.model_validate(row["body"])
                    assert claim.revision == row["feedback_revision"] == len(claim.record.feedback)
                    assert claim.generation == row["claim_generation"]
                    assert claim.token == row["claim_token"] and row["phase"] == "claimed"
                    actors = [(claim.record.policy.observer, 1, None)] + [
                        (item.actor, item.generation, item) for item in claim.record.feedback
                    ]
                    requests = []
                    for unit_actor, generation, feedback in actors:
                        binding = w.views.resolve(unit_actor, generation, claim)
                        binding.check()
                        request = import_request(
                            claim.record, w.flow.destination_id, unit_actor, feedback
                        )
                        requests.append(
                            {
                                "actor": unit_actor,
                                "generation": generation,
                                "feedback_key": None if feedback is None else feedback.key,
                                "key": request.request_key,
                                "request": request.model_dump(mode="json"),
                                "request_hash": request_hash(request, binding.view),
                            }
                        )
                    evidence["claims"].append(
                        {
                            "capture_id": claim.record.capture_id,
                            "R": claim.revision,
                            "generation": claim.generation,
                            "token": claim.token,
                            "requests": requests,
                            "request_manifest": w.flow.request_manifest(claim),
                        }
                    )
                return claims

            async def observed_save(claim, key, incoming):
                attempt = {
                    "R": claim.revision,
                    "generation": claim.generation,
                    "key": key,
                    "input": clone(incoming),
                    "before": await state("before-save"),
                }
                evidence["save_attempts"].append(attempt)
                try:
                    result = await original_save(claim, key, incoming)
                except BaseException as error:
                    attempt["error"] = note_error(error)
                    try:
                        attempt["after_error"] = await state("save-rejected")
                    except BaseException as sampling_error:
                        evidence["evidence_errors"].append(note_error(sampling_error))
                    raise
                attempt["after"] = await state("save-" + incoming["state"])
                return result

            async def observed_ack(claim, receipts):
                saved = await w.port.units(claim)
                expected = evidence["claims"][-1]["requests"]
                assert list(saved) == [item["key"] for item in expected]
                assert [receipt.key for receipt in receipts] == list(saved)
                destinations = []
                for item, receipt in zip(expected, receipts, strict=True):
                    unit = saved[item["key"]]
                    body = receipt.model_dump(mode="json")
                    assert unit["request"] == item["request"]
                    assert unit["request_hash"] == receipt.request_hash == item["request_hash"]
                    assert unit["actor"] == receipt.actor == item["actor"]
                    assert unit["generation"] == item["generation"]
                    assert receipt.feedback_key == item["feedback_key"]
                    assert unit["state"] == "complete" and len(unit["receipt_hash"]) == 64
                    assert unit["receipt_hash"] == neutral_fingerprint(body)
                    rows = await w.evaluation.repository.import_observations(
                        claim.record.scope, receipt.key, receipt.request_hash, rows=None
                    )
                    assert rows is not None and len(rows) == 1
                    assert rows[0]["id"] == receipt.observation_id
                    assert rows[0]["content_hash"] == receipt.observation_hash
                    candidate = await w.evaluation.repository.candidate(
                        claim.record.scope, receipt.candidate_id
                    )
                    assert candidate["observation_id"] == receipt.observation_id
                    assert candidate["observation_hash"] == receipt.observation_hash
                    destinations.append(
                        {"receipt": body, "original_key_rows": clone(rows), "candidate": candidate}
                    )
                before = await state("before-ack")
                result = await original_ack(claim, receipts)
                after = await state("after-ack")
                row = after["rows"][0]
                assert row["phase"] == "acked" and row["ack_root"] == receipts_root(receipts)
                assert row["feedback_revision"] == claim.revision
                assert row["claim_generation"] == claim.generation
                assert row["delivery"] == before["rows"][0]["delivery"]
                evidence["acks"].append(
                    {
                        "R": claim.revision,
                        "generation": claim.generation,
                        "receipts": [receipt.model_dump(mode="json") for receipt in receipts],
                        "destination": destinations,
                        "source_ack": row["ack_root"],
                    }
                )
                return result

            monkeypatch.setattr(w.port, "claim", observed_claim)
            monkeypatch.setattr(w.port, "save_unit", observed_save)
            monkeypatch.setattr(w.port, "ack", observed_ack)

            snap = await w.run()
            assert snap.run.status == "completed"
            terminals = [item for item in snap.events if item.kind == "terminal"]
            assert len(terminals) == 1
            evidence["terminal"] = terminals[0].model_dump(mode="json")
            evidence["policy"] = w.port.policy.model_dump(mode="json")
            initial = await state("terminal-returned")
            baseline_calls = (
                provider.calls,
                len(w.wire.requests),
                len(w.wire.executions),
                w.evaluation.owner.spawn_calls,
            )
            assert provider.calls > 0 and provider.active == 0
            assert w.evaluation.owner.spawn_calls == 0
            if kind == "terminal_base":
                assert terminals[0].data["observation_capture"]["decision"]["kind"] == "overflow"
                assert initial["rows"] == []
                assert initial["counters"] == {
                    "seen": 1,
                    "enqueued": 0,
                    "dropped": 1,
                    "acked": 0,
                }
                assert await w.flow.consume_once() == []
                assert await w.flow.consume_once() == []
                assert await state("overflow-no-pending") == initial
            else:
                assert len(initial["rows"]) == 1
                assert initial["rows"][0]["phase"] == "pending"
                base_receipts = []
                base_delivery = None
                if kind == "completion_growth":
                    base_receipts = await w.flow.consume_once()
                    assert len(base_receipts) == 1 and base_receipts[0].feedback_key is None
                    base_state = await state("base-complete")
                    assert base_state["rows"][0]["phase"] == "acked"
                    base_delivery = clone(base_state["rows"][0]["delivery"])
                    assert base_delivery["manifest"]["revision"] == 0
                    assert all(
                        unit["state"] == "complete" and len(unit["receipt_hash"]) == 64
                        for unit in base_delivery["units"].values()
                    )
                accepted = []
                rejected = None
                for index in range(1, 10):
                    key = f"{index:02d}" + "k" * 62 if kind == "long_identity" else f"fb-{index}"
                    assert len(actor) <= 64 and len(key) <= 64
                    if kind == "long_identity":
                        assert len(actor) == len(key) == 64
                    before = await state("feedback-before")
                    if kind in {"before_manifest", "long_identity"}:
                        assert "manifest" not in before["rows"][0]["delivery"]
                    attempt = {"actor": actor, "key": key, "before": before}
                    evidence["feedback_attempts"].append(attempt)
                    try:
                        feedback = await w.feedback(snap.run.id, actor=actor, key=key)
                    except ServiceError as error:
                        attempt["error"] = note_error(error)
                        attempt["after"] = await state("feedback-rejected")
                        assert error.code == "capacity"
                        assert attempt["after"] == before
                        rejected = attempt
                        break
                    after = await state("feedback-accepted")
                    attempt["after"] = after
                    attempt["accepted"] = feedback.model_dump(mode="json")
                    accepted.append(feedback)
                    old = before["rows"][0]
                    row = after["rows"][0]
                    assert row["body"]["feedback"] == (
                        old["body"]["feedback"] + [feedback.model_dump(mode="json")]
                    )
                    assert row["feedback_revision"] == feedback.revision == len(accepted)
                    assert {
                        key: item for key, item in row["body"].items() if key != "feedback"
                    } == {key: item for key, item in old["body"].items() if key != "feedback"}
                    for field in [
                        "capture_id",
                        "source_id",
                        "workspace_id",
                        "run_id",
                        "terminal_event_id",
                        "policy_fingerprint",
                    ]:
                        assert row[field] == old[field]
                    assert row["phase"] == "pending"
                    for old_key, old_unit in old["delivery"]["units"].items():
                        assert row["delivery"]["units"][old_key] == old_unit
                    assert row["claim_generation"] == old["claim_generation"]
                    assert after["counters"] == {
                        **before["counters"],
                        "enqueued": before["counters"]["enqueued"] + int(old["phase"] == "acked"),
                    }
                evidence["accepted_feedback"] = [item.model_dump(mode="json") for item in accepted]
                assert accepted
                # 不预演新容量helper；真实freeze/complete若失败，保留真实capacity异常。
                receipts = await w.flow.consume_once()
                assert rejected is not None
                assert [(item.actor, item.feedback_key) for item in receipts] == [
                    ("local", None),
                    *[(item.actor, item.key) for item in accepted],
                ]
                final = await state("all-accepted-acked")
                row = final["rows"][0]
                assert row["phase"] == "acked" and row["feedback_revision"] == len(accepted)
                assert row["delivery"]["manifest"] == {
                    "revision": len(accepted),
                    "keys": [item.key for item in receipts],
                }
                assert all(
                    unit["state"] == "complete" and len(unit["receipt_hash"]) == 64
                    for unit in row["delivery"]["units"].values()
                )
                if base_delivery is not None:
                    for key, unit in base_delivery["units"].items():
                        assert row["delivery"]["units"][key] == unit
                    assert receipts[0] == base_receipts[0]
                    assert [item["generation"] for item in evidence["claims"]] == [1, 2]
                else:
                    assert [item["generation"] for item in evidence["claims"]] == [1]
                assert evidence["acks"][-1]["R"] == len(accepted)
                assert row["ack_root"] == receipts_root(receipts)
                assert await w.flow.consume_once() == []
                assert await w.flow.consume_once() == []
                assert await state("repeat-zero-replay") == final
            assert (
                provider.calls,
                len(w.wire.requests),
                len(w.wire.executions),
                w.evaluation.owner.spawn_calls,
            ) == baseline_calls
            evidence["calls_after_delivery"] = {
                "provider": provider.calls,
                "wire": len(w.wire.requests),
                "tools": len(w.wire.executions),
                "target": w.evaluation.owner.spawn_calls,
            }
    except BaseException as error:
        primary = error
        evidence["error"] = note_error(error)
        raise
    finally:
        for event_name, callback in reversed(listeners):
            event.remove(Pool, event_name, callback)
        try:
            after_fds = fds()
            evidence["fds_before"] = before_fds
            evidence["fds_after"] = after_fds
            evidence["pending"] = [
                repr(task)
                for task in asyncio.all_tasks()
                if (
                    task not in before_tasks
                    and task is not asyncio.current_task()
                    and not task.done()
                )
            ]
            if value is not None:
                evidence["closed"] = {
                    "source": value.source.store.closed,
                    "source_writer_fd": value.source.store.fd,
                    "source_producers_done": all(
                        task.done() for task in value.source.service.tasks.values()
                    ),
                    "dispatcher_done": value.source.sessions._dispatcher.done(),
                    "eval": value.evaluation.repository.closed,
                    "eval_writer_fd": value.evaluation.repository._writer_fd,
                    "eval_owner": _hot_eval_owner(value.evaluation),
                    "wire_active": len(value.wire.active),
                    "wire_errors": list(value.wire.errors),
                }
                if primary is None:
                    assert evidence["closed"]["source"] and evidence["closed"]["eval"]
                    assert str(value.source.store.fd) not in after_fds
                    assert evidence["closed"]["eval_writer_fd"] is None
                    assert evidence["closed"]["source_producers_done"]
                    assert evidence["closed"]["dispatcher_done"]
                    assert evidence["closed"]["eval_owner"]["producers_done"]
                    assert evidence["closed"]["wire_active"] == 0
                    assert evidence["closed"]["wire_errors"] == []
            if provider is not None:
                evidence["provider_after_close"] = {
                    "calls": provider.calls,
                    "active": provider.active,
                    "closed": provider.closed,
                    "streams_closed": provider.streams_closed,
                }
                if primary is None:
                    assert provider.closed and provider.active == 0
                    assert provider.streams_closed == provider.calls
            counts = Counter((item["event"], item["connection"]) for item in evidence["pool"])
            evidence["pool_counts"] = dict(Counter(item["event"] for item in evidence["pool"]))
            if primary is None:
                assert evidence["pool"] and after_fds == before_fds and not evidence["pending"]
                for connection in {item["connection"] for item in evidence["pool"]}:
                    assert counts["connect", connection] == counts["close", connection] > 0
                    assert counts["checkout", connection] == counts["checkin", connection]
                assert all(
                    item["asyncio_safe"] for item in evidence["pool"] if item["event"] == "reset"
                )
        except BaseException as sampling_error:
            evidence["evidence_errors"].append(note_error(sampling_error))
            if primary is None:
                primary = sampling_error
                evidence["error"] = note_error(sampling_error)
                raise
            primary.add_note("capacity resource evidence: " + str(sampling_error))
        finally:
            try:
                text = json.dumps(evidence, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
                await asyncio.to_thread(
                    (tmp_path / "delivery-capacity.json").write_text, text, encoding="utf-8"
                )
            except BaseException as io_error:
                if primary is None:
                    raise
                primary.add_note("capacity evidence JSON write: " + str(io_error))
