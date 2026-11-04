"""D3真实同Run/TCP摘要消费者；不复制Provider/Store/压缩实现。"""

import asyncio
import hashlib
import json
import os
import re
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, cast

import pytest
from sqlalchemy import select
from test_tool_pipeline import application_case as original_application_case
from test_tool_pipeline import data_case as data_case
from test_tool_pipeline import http_suite as original_http_suite

from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import encoded
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate
from krim_agent.storage.models import compactions, turns, work_turns
from krim_agent.workspace.types import Completion, WorkspaceTurnRequest

application_case: Callable[..., Awaitable[Any]] = original_application_case
http_suite: Callable[[], Any] = original_http_suite
HTTPFixture = http_suite().HTTPFixture
frame, choice = http_suite().frame, http_suite().choice


@pytest.fixture(scope="module", autouse=True)
def preserve_interrupted_worker_database() -> None:
    """只读一致backup，不recover原超时库，也不把进程退出当内部finally。"""
    import sqlite3
    import stat

    root = Path(os.environ["KRIM_D3_ROOT"])
    old = root/"evidence/d3/d3-budget-01"
    source = old/"tmp/test_compactor_budget_fence_ca7/pipeline.sqlite"
    for parent in reversed(source.parents):
        assert stat.S_ISDIR(parent.lstat().st_mode)
    assert stat.S_ISREG(source.lstat().st_mode)
    command = json.loads((old/"command.json").read_bytes())
    selectors = [arg for arg in command["product_argv"] if arg.startswith("tests/")]
    assert len(selectors) == 8 and selectors[-1].endswith("[commit-cancel]")
    completed = [json.loads(p.read_bytes()) for p in (old/"cases").iterdir()
                 if stat.S_ISREG(p.lstat().st_mode) and p.suffix == ".json"]
    assert len(completed) == 7
    assert str(source) not in [case["database"] for case in completed]
    out = Path(os.environ["KRIM_D3_EVIDENCE"])/"interrupted-worker"
    out.mkdir()
    backup = out/"pipeline-readonly-backup.sqlite"
    hashes = {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
              for path in [source, source.with_name(source.name+"-wal")]
              if path.exists() and stat.S_ISREG(path.lstat().st_mode)}
    reader = sqlite3.connect(source.as_uri()+"?mode=ro", uri=True)
    writer = sqlite3.connect(backup)
    try:
        reader.backup(writer)
        runs = reader.execute("SELECT id,status,code FROM runs").fetchall()
        pending = reader.execute("SELECT run_id,pass_index,status FROM compactions").fetchall()
    finally:
        writer.close()
        reader.close()
    assert all(hashlib.sha256(Path(path).read_bytes()).hexdigest() == digest
               for path, digest in hashes.items())
    (out/"mapping.json").write_text(json.dumps({"source": str(source), "backup": str(backup),
        "node": selectors[-1],
        "mapping_basis": ("8 ordered selectors; 7 completed node receipts; "
                          "remaining deterministic pytest tmp index7"),
        "runs": runs, "compactions": pending,
        "source_hashes": hashes, "recover_called": False, "replay_count": 0,
        "worker_exit": -15, "internal_finally": "unknown"}, indent=2))


class SummaryWire(HTTPFixture):
    """答案和摘要只由实际收到的前史哨兵生成；无预设成功结果。"""
    def __init__(self) -> None:
        super().__init__()
        self.summary_bodies: list[dict[str, Any]] = []
        self.task_bodies: list[dict[str, Any]] = []

    def script(self, body: Any) -> Any:
        text = encoded(body["messages"])
        pairs = dict(re.findall(r"SENTINEL_(\d+)=(\d+)", text))
        summary = body["messages"][0]["content"].startswith("Krim compaction/1")
        if summary:
            assert "tools" not in body
            self.summary_bodies.append(body)
            output = ";".join("SENTINEL_"+key+"="+pairs[key] for key in sorted(pairs))
        else:
            self.task_bodies.append(body)
            output = str(sum(int(value) for value in pairs.values()))
        return [frame(choice({"content": output})), frame(choice(finish="stop")),
                frame({"choices": [], "usage": {"prompt_tokens": 3,
                       "completion_tokens": 2, "total_tokens": 5}}), frame("[DONE]")]


async def app_history(
    data_case: Any, tmp_path: Path, *, count: int = 8, padding: int = 1500,
    max_bytes: int = 18000,
) -> tuple[Any, Any, Any, Any, Any]:
    wire = SummaryWire()
    app, wire, provider = await application_case(data_case, tmp_path, wire=wire)
    session = await app.sessions.create(LOCAL, "local", SessionCreate(
        request_key="compaction-session", target_key="target-workspace",
        context_policy=ContextPolicy(max_bytes=max_bytes, max_messages=32,
                                     reserved_output_tokens=512)))
    for index in range(count):
        request = WorkspaceTurnRequest(request_key="prior-"+str(index),
            text="SENTINEL_"+str(index)+"="+str(index+11)+" "+"x"*padding,
            expected_revision=index, completion=Completion())
        turn = await app.sessions.submit(LOCAL, "local", session.id, request)
        await app.sessions.wait(LOCAL, "local", session.id, turn.id)
    async with app.store.engine.connect() as conn:
        prior = [dict(row) for row in (await conn.execute(
            select(turns).where(turns.c.session_id == session.id).order_by(turns.c.ordinal)
        )).mappings()]
    return app, wire, provider, session, prior


async def test_auto_compaction_preserves_tail_and_source(data_case: Any, tmp_path: Path) -> None:
    app, wire, provider, session, before = await app_history(data_case, tmp_path)
    seen = len(wire.requests)
    request = WorkspaceTurnRequest(request_key="current", text="sum actual sentinels "+"x"*5000,
        expected_revision=8, completion=Completion())
    turn = await app.sessions.submit(LOCAL, "local", session.id, request)
    finished = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
    facts = await app.service.result(finished.run_id)
    snapshot = await app.store.snapshot(finished.run_id)
    async with app.store.engine.connect() as conn:
        rows = [dict(row) for row in (await conn.execute(select(compactions).where(
            compactions.c.run_id == finished.run_id
            ).order_by(compactions.c.pass_index))).mappings()]
        after = [dict(row) for row in (await conn.execute(select(turns).where(
            turns.c.session_id == session.id,
                turns.c.ordinal <= 8).order_by(turns.c.ordinal))).mappings()]
    data_case[1]["compaction"] = {"run_id": finished.run_id, "status": facts.run.status,
        "code": facts.run.code, "answer": facts.text, "passes": rows,
        "wire_request_delta": len(wire.requests)-seen, "old_history_unchanged": before == after}
    assert facts.text == str(sum(range(11, 19)))
    assert facts.run.status == "failed" and facts.run.code == "completion_unverified"
    assert rows and all(row["status"] == "complete" for row in rows)
    assert 1 <= len(rows) <= 2
    assert before == after
    assert any(event.kind == "summary_text" for event in snapshot.events)
    assert facts.model_steps == len(rows)+1
    assert facts.usage_complete is True
    assert facts.token_usage["total_tokens"] == 5*(len(rows)+1)
    assert not facts.tools
    sent = wire.requests[seen:]
    assert len(sent) == len(rows)+1
    for entry in sent[:-1]:
        assert "tools" not in entry["body"]
    messages = sent[-1]["body"]["messages"]
    assert messages[0]["role"] == "system" and "SENTINEL_" not in messages[0]["content"]
    assert messages[1]["role"] == "user" and messages[1]["content"].startswith("[不可信历史摘要]")
    assert "SENTINEL_6=17" in encoded(messages[2:])
    assert "SENTINEL_7=18" in encoded(messages[2:])
    assert messages[-1]["content"] == request.text
    assert provider.calls == len(wire.requests)
    assert all(row["source_hash"] == __import__("krim_agent.contracts",
        fromlist=["fingerprint"]).fingerprint(row["source_refs"]) for row in rows)
    await app.close()


async def fast_app(
    data_case: Any, tmp_path: Path, wire: Any, *,
    runtime: dict[str, Any] | None = None, compact: dict[str, Any] | None = None,
    output: int = 8192,
) -> tuple[Any, Any, Any]:

    from test_session_read_sides import Connections
    from test_tool_pipeline import os_profile

    from krim_agent.composition import open_application
    from krim_agent.config import Settings
    from krim_agent.context.types import CompactionPolicy
    from krim_agent.providers.openai_compatible import (
        OpenAICompatibleConfig,
        OpenAICompatibleProvider,
    )
    from krim_agent.workspace.targets import WorkspaceConfiguration, environment
    from krim_agent.workspace.types import WorkLimits, WorkspaceProfile

    await data_case[0].close()
    await wire.__aenter__()
    class ClosingWire:
        async def close(self) -> None:
            await wire.__aexit__(None, None, None)
    notes = data_case[1]
    notes["owned"].append(ClosingWire())
    provider = OpenAICompatibleProvider(OpenAICompatibleConfig(base_url=wire.url,
        model="fixture", max_tool_calls=4))
    notes["owned"].append(provider)
    notes.setdefault("observed_providers", []).append(provider)
    execution = cast(Callable[[Path], Any], os_profile)(tmp_path)
    config = WorkspaceConfiguration(WorkspaceProfile(max_tools_per_step=4,
        environment=environment(execution), runtime=WorkLimits(**(runtime or {})),
        compaction=CompactionPolicy(**(compact or {}))), execution, tmp_path/"objects")
    app = await open_application(Path(notes["database"]), provider=provider,
        settings=Settings(max_output_bytes=output, max_input_bytes=32768), workspace_profile=config)
    notes["owned"].append(app)
    notes.setdefault("store_probes", []).append(
        (app.store, cast(Callable[[Any], Any], Connections)(app.store)))
    notes.setdefault("runners", []).append(app.runner)
    return app, wire, provider


async def cancelled_history(
    app: Any, *, count: int = 8, padding: int = 1500, max_bytes: int = 18000,
) -> Any:
    """真实排队后取消的Turn历史，无伪造Run/模型输出；预算负例用它节约模型重放。"""
    from krim_agent.sessions.types import ControlRequest

    session = await app.sessions.create(LOCAL, "local", SessionCreate(
        request_key="cancelled-history", target_key="target-workspace",
        context_policy=ContextPolicy(max_bytes=max_bytes, max_messages=32,
            reserved_output_tokens=512)))
    async with app.service._admission:
        for index in range(count):
            current = await app.sessions.get(LOCAL, "local", session.id)
            turn = await app.sessions.submit(LOCAL, "local", session.id, WorkspaceTurnRequest(
                request_key="history-"+str(index),
                    text="SENTINEL_"+str(index)+"="+str(index+11)+" "+"x"*padding,
                expected_revision=current.revision, completion=Completion()))
            current = await app.sessions.get(LOCAL, "local", session.id)
            await app.sessions.control(LOCAL, "local", session.id, "cancel_turn",
                ControlRequest(request_key="cancel-"+str(index),
                    expected_revision=current.revision), turn_id=turn.id)
    return await app.sessions.get(LOCAL, "local", session.id)


CANCEL_CASES = [phase+"-"+action for phase in ["start", "first", "commit"]
                for action in ["cancel", "repeat-cancel", "close", "repeat-close"]]


@pytest.mark.parametrize("case", ["tool", "steps", "bytes", "deadline", "partial-usage",
    "stream", "client", "compactor", "builder", *CANCEL_CASES])
async def test_compactor_budget_fence_cancel_and_usage(
    data_case: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str,
) -> None:
    from krim_agent.context import builder, compaction
    from krim_agent.contracts import ServiceError

    wire = SummaryWire()
    original_script = wire.script
    def script(body: Any) -> Any:
        result = original_script(body)
        summary = body["messages"][0]["content"].startswith("Krim compaction/1")
        if case == "tool" and summary:
            return [frame(choice({"tool_calls": [http_suite().d2_call(0)]})),
                    frame(choice(finish="tool_calls")), frame("[DONE]")]
        if case == "partial-usage":
            return result[:2]+[frame({"choices": [], "usage": {"prompt_tokens": 3}}),
                frame("[DONE]")]
        if case == "bytes" and not summary:
            return [frame(choice({"content": "z"*120})), frame(choice(finish="stop")),
                frame("[DONE]")]
        return result
    cast(Any, wire).script = script
    app, wire, provider = await fast_app(data_case, tmp_path, wire,
        runtime={"model_steps": 1} if case == "steps" else {"run_timeout_seconds": 0.1}
            if case == "deadline" else None,
        output=128 if case == "bytes" else 8192)
    session = await cancelled_history(app)
    entered, release = asyncio.Event(), asyncio.Event()
    notes = data_case[1]
    old_append = app.store.append
    entries, replacements, closes = [], [], []
    async def forbidden(*args: Any, **kwargs: Any) -> Any:
        entries.append("replacement")
        raise ServiceError("target_binding_changed")
    async def append(run_id: Any, kind: Any, data: Any, **kwargs: Any) -> Any:
        event = await old_append(run_id, kind, data, **kwargs)
        if kind == "model_start" and data.get("purpose") == "summary":
            notes["current_run_id"] = run_id
            if case.startswith("start-"):
                entered.set()
                await release.wait()
            if case == "stream":
                monkeypatch.setattr(provider, "stream", forbidden)
            elif case == "client":
                import httpx
                replacements.append(provider._client)
                provider._client = httpx.AsyncClient(trust_env=False)
            elif case == "compactor":
                monkeypatch.setattr(compaction.Compactor, "complete", forbidden)
            elif case == "builder":
                monkeypatch.setattr(builder, "check_budget", forbidden)
        return event
    monkeypatch.setattr(app.store, "append", append)
    if case.startswith("first-"):
        # 装配后替换stream会被封印拒绝，故用真实TCP首响应屏障。
        wire.step_release[0].clear()
    if case.startswith("commit-"):
        # 实际SQL UPDATE前屏障：完整模型流结束后、摘要提交前。
        from sqlalchemy.ext.asyncio import AsyncConnection
        execute = AsyncConnection.execute
        async def query(conn: Any, statement: Any, *args: Any, **kwargs: Any) -> Any:
            if getattr(statement, "is_update", False) and getattr(getattr(statement, "table",
                None), "name", None) == "compactions":
                values = getattr(statement, "_values", {})
                if any(getattr(key, "name", key) == "output" for key in values):
                    entered.set()
                    await release.wait()
            return await execute(conn, statement, *args, **kwargs)
        monkeypatch.setattr(AsyncConnection, "execute", query)
    try:
        current = await app.sessions.get(LOCAL, "local", session.id)
        turn = await app.sessions.submit(LOCAL, "local", session.id, WorkspaceTurnRequest(
            request_key="budget-current", text="sum", expected_revision=current.revision,
                completion=Completion()))
        if case.startswith("first-"):
            async with asyncio.timeout(5):
                await wire.step_entered[0].wait()
            entered.set()
        if case in CANCEL_CASES:
            async with asyncio.timeout(5):
                await entered.wait()
            run_id = notes["current_run_id"]
            if case.startswith("commit-"):
                producer = app.service.tasks[run_id]
                producer.cancel()
                if "repeat" in case:
                    producer.cancel()
            action = case.split("-", 1)[1]
            if "close" in action:
                closes.append(asyncio.create_task(app.close()))
                if action.startswith("repeat"):
                    closes.append(asyncio.create_task(app.close()))
            else:
                closes.append(asyncio.create_task(app.service.cancel(run_id)))
                if action.startswith("repeat"):
                    closes.append(asyncio.create_task(app.service.cancel(run_id)))
            await asyncio.sleep(0)
            release.set()
            for event in wire.step_release:
                event.set()
            await asyncio.gather(*closes)
            if app.store.closed:
                from krim_agent.storage.sqlite import SQLiteStore
                reader = await SQLiteStore.open(Path(notes["database"]), readonly=True)
                try:
                    snapshot = await reader.snapshot(run_id)
                    async with reader.engine.connect() as conn:
                        rows = [dict(row) for row in (await conn.execute(select(compactions).where(
                            compactions.c.run_id == run_id))).mappings()]
                    assert all(row["status"] != "complete" for row in rows)
                finally:
                    await reader.close()
            else:
                snapshot = await app.store.snapshot(run_id)
                async with app.store.engine.connect() as conn:
                    rows = [dict(row) for row in (await conn.execute(select(compactions).where(
                        compactions.c.run_id == run_id))).mappings()]
                    prepared = await conn.scalar(select(work_turns.c.prepared_context).where(
                        work_turns.c.run_id == run_id))
                assert all(row["status"] != "complete" for row in rows)
                assert prepared is None
            assert snapshot.run.status == "aborted"
            assert not any(e.kind == "text" for e in snapshot.events)
        else:
            finished = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
            snapshot = await app.store.snapshot(finished.run_id)
            code = snapshot.run.code
            if case in {"stream", "client", "compactor", "builder"}:
                assert code == ("context_binding_changed" if case == "builder"
                    else "target_binding_changed") and not entries
                assert len(wire.requests) == 0
            elif case == "tool":
                assert code == "summary_protocol"
            elif case == "steps":
                assert code == "step_limit"
            elif case == "bytes":
                assert code == "output_limit"
            elif case == "deadline":
                assert code == "run_timeout"
            elif case == "partial-usage":
                facts = await app.service.result(finished.run_id)
                assert facts.usage_complete is False and facts.token_usage["total_tokens"] is None
        from krim_agent.observability import project_trace
        trace = project_trace(snapshot)
        ended = {e.data["span_id"] for e in snapshot.events if e.kind == "model_end"}
        assert all(span.end is None and span.status == "unknown" for span in trace.spans
                   if span.kind == "model" and span.span_id not in ended)
        assert not [e for e in snapshot.events if e.kind == "tool_intent"]
        notes["budget"] = {"case": case, "run_id": snapshot.run.id,
            "code": snapshot.run.code, "requests": len(wire.requests),
                "replacement_entries": entries,
            "events": [e.model_dump(mode="json") for e in snapshot.events]}
    finally:
        if replacements:
            await provider._client.aclose()
            provider._client = replacements[0]
        release.set()
        for event in wire.step_release:
            event.set()
        await asyncio.gather(*closes, return_exceptions=True)
        await app.close()


@pytest.mark.parametrize("case", ["twice", "empty", "oversize", "bad-frame", "max-passes",
                                  "tail", "idempotent", "lost-return"])
async def test_repeat_failure_and_oversize_never_destroy_history(data_case: Any, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch, case: str) -> None:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncTransaction

    from krim_agent.context import compaction
    from krim_agent.contracts import ServiceError

    wire = SummaryWire()
    captured = []
    initial = compaction.Compactor.__init__
    def constructed(instance: Any, *args: Any, **kwargs: Any) -> None:
        initial(instance, *args, **kwargs)
        captured.append(instance)
    monkeypatch.setattr(compaction.Compactor, "__init__", constructed)
    app, wire, provider = await fast_app(data_case, tmp_path, wire,
        compact={"max_passes": 1} if case == "max-passes" else None)
    session = await cancelled_history(app, count=12 if case in {"twice", "max-passes"} else 2
        if case == "tail" else 8,
        padding=2500 if case in {"twice", "max-passes"} else 12000 if case == "tail" else 1500)
    old_rows = []
    if case in {"empty", "oversize", "bad-frame"}:
        current = await app.sessions.get(LOCAL, "local", session.id)
        first = await app.sessions.submit(LOCAL, "local", session.id, WorkspaceTurnRequest(
            request_key="first-good", text="sum", expected_revision=current.revision,
                completion=Completion()))
        await app.sessions.wait(LOCAL, "local", session.id, first.id)
        async with app.store.engine.connect() as conn:
            old_rows = [dict(r) for r in (await conn.execute(select(compactions))).mappings()]
        assert old_rows and all(r["status"] == "complete" for r in old_rows)
        from krim_agent.sessions.types import ControlRequest
        async with app.service._admission:
            for offset in range(2):
                state = await app.sessions.get(LOCAL, "local", session.id)
                extra = await app.sessions.submit(LOCAL, "local", session.id, WorkspaceTurnRequest(
                    request_key="extra-"+str(offset),
                        text="SENTINEL_"+str(offset+8)+"="+str(offset+19)+" "+"x"*3000,
                    expected_revision=state.revision, completion=Completion()))
                state = await app.sessions.get(LOCAL, "local", session.id)
                await app.sessions.control(LOCAL, "local", session.id, "cancel_turn",
                    ControlRequest(request_key="extra-cancel-"+str(offset),
                        expected_revision=state.revision), turn_id=extra.id)
        original = wire.script
        def failed(body: Any) -> Any:
            if body["messages"][0]["content"].startswith("Krim compaction/1"):
                if case == "bad-frame":
                    return [b"data: {invalid\n\n"]
                return [frame(choice({"content": "" if case == "empty" else "超"*1000})),
                        frame(choice(finish="stop")), frame("[DONE]")]
            return original(body)
        cast(Any, wire).script = failed
    lost: list[str] = []
    commit_connections: list[Any] = []
    if case == "lost-return":
        execute, exit_transaction = AsyncConnection.execute, AsyncTransaction.__aexit__
        async def query(conn: Any, statement: Any, *args: Any, **kwargs: Any) -> Any:
            result = await execute(conn, statement, *args, **kwargs)
            if getattr(statement, "is_update", False) and getattr(getattr(statement, "table",
                None), "name", None) == "compactions":
                if any(getattr(k, "name", k) == "output" for k in getattr(statement, "_values",
                    {})):
                    commit_connections.append(conn)
            return result
        async def exit_(transaction: Any, *args: Any) -> Any:
            result = await cast(Callable[..., Awaitable[Any]], exit_transaction)(transaction, *args)
            if args[0] is None and not lost and transaction.connection in commit_connections:
                lost.append("actual-COMMIT-returned")
                raise OSError("synthetic summary commit acknowledgement loss")
            return result
        monkeypatch.setattr(AsyncConnection, "execute", query)
        monkeypatch.setattr(AsyncTransaction, "__aexit__", exit_)
    async with app.store.engine.connect() as conn:
        old_history = [dict(r)
            for r in (await conn.execute(select(turns).order_by(turns.c.ordinal))).mappings()]
    current = await app.sessions.get(LOCAL, "local", session.id)
    request = WorkspaceTurnRequest(request_key="repeat-current", text="sum",
        expected_revision=current.revision, completion=Completion())
    seen = len(wire.requests)
    turn = await app.sessions.submit(LOCAL, "local", session.id, request)
    finished = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
    facts = await app.service.result(finished.run_id)
    async with app.store.engine.connect() as conn:
        rows = [dict(r) for r in (await conn.execute(select(compactions).where(
            compactions.c.run_id == finished.run_id
            ).order_by(compactions.c.pass_index))).mappings()]
        all_rows = [dict(r) for r in (await conn.execute(select(compactions))).mappings()]
        history = [dict(r)
            for r in (await conn.execute(select(turns).where(
                turns.c.ordinal < turn.ordinal).order_by(turns.c.ordinal))).mappings()]
    assert old_history == history
    assert all(row in all_rows for row in old_rows)
    assert not facts.tools
    if case == "twice":
        assert len(rows) == 2 and all(r["status"] == "complete" for r in rows)
        assert rows[1]["source_refs"][:len(rows[0]["source_refs"])] == rows[0]["source_refs"]
        assert len(rows[1]["source_refs"]) == 10
        assert facts.text == str(sum(range(11, 23)))
    elif case in {"empty", "oversize", "bad-frame"}:
        assert facts.run.code in {"summary_failed", "provider_protocol"}
        assert rows and all(r["status"] == "failed" for r in rows)
        assert facts.text == ""
    elif case == "max-passes":
        assert facts.run.code == "compaction_budget"
        assert len(rows) == 1 and rows[0]["status"] == "complete"
        assert len(wire.requests)-seen == 1
    elif case == "tail":
        assert facts.run.code == "context_budget" and rows == []
        assert len(wire.requests)-seen == 0
    else:
        assert facts.run.code == "completion_unverified" and len(rows) == 1
        selected = next(c for c in captured if c.run_id == finished.run_id)
        before_requests = len(wire.requests)
        again = await selected.complete(0, rows[0]["output"])
        assert again["output_hash"] == rows[0]["output_hash"]
        with pytest.raises(ServiceError, match="key_conflict"):
            await selected.complete(0, "different content")
        replay = await app.sessions.submit(LOCAL, "local", session.id, request)
        assert replay.id == turn.id and replay.run_id == finished.run_id
        assert len(wire.requests) == before_requests
        if case == "lost-return":
            assert lost == ["actual-COMMIT-returned"]
    data_case[1]["repeat_compaction"] = {"case": case, "run_id": finished.run_id,
        "code": facts.run.code, "rows": rows, "old_available_rows": old_rows,
        "old_history_unchanged": history == old_history, "lost_return": lost,
        "requests": len(wire.requests)-seen}
    assert provider.calls == len(wire.requests)
    await app.close()


async def test_legacy_facts_and_http_binding_upgrade(data_case: Any, tmp_path: Path) -> None:
    """固定8963055源码真实生成旧script/HTTP事实，不用旧digest常量代替执行。"""
    import subprocess
    import sys

    from test_session_read_sides import Connections

    from krim_agent.composition import open_application
    from krim_agent.contracts import RunRequest
    from krim_agent.providers.openai_compatible import OpenAICompatibleConfig
    from krim_agent.sessions.types import TurnRequest

    root = Path(os.environ["KRIM_D3_ROOT"])
    material = root/"materials/product/m1-source"
    assert (material/"src/krim_agent/providers/openai_compatible.py").is_file()
    worker = tmp_path/"legacy-worker.py"
    program = r'''
import asyncio,json,sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, cast
sys.path.insert(0,str(Path(sys.argv[1])/'src'))
from krim_agent.composition import open_application
from krim_agent.contracts import RunRequest,encoded
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate,TurnRequest
from krim_agent.evaluation.types import FreezeRequest,EvalRequest,rule
async def main():
 result={}
 for kind in ['script','http']:
  path=Path(sys.argv[2])/(kind+'.sqlite')
  options={'http_config':OpenAICompatibleConfig(base_url=sys.argv[3],
      model='fixture')} if kind=='http' else {}
  async with await open_application(path,**options) as app:
   target='target-http' if kind=='http' else 'demo'
   request=RunRequest(text='/add 2 3',request_key='old-'+kind,target=target)
   run=await app.service.start(request);await app.service.wait(run.id)
   session=await app.sessions.create(LOCAL,'local',SessionCreate(request_key='old-session',
       target_key=target))
   turn=await app.sessions.submit(LOCAL,'local',session.id,TurnRequest(request_key='old-turn',
       text='/echo previous',expected_revision=0))
   turn=await app.sessions.wait(LOCAL,'local',session.id,turn.id)
   value={'database':str(path),'run_id':run.id,'request':request.model_dump(mode='json'),
       'session':session.model_dump(mode='json'),'turn':turn.model_dump(mode='json'),
           'facts':(await app.service.result(run.id)).model_dump(mode='json'),
               'trace':(await app.service.trace(run.id)).model_dump(mode='json'),
                   'snapshot_target':(await app.store.snapshot(run.id)).target,
                       'context':(await app.store.snapshot(
                        turn.run_id)).session_context.model_dump(mode='json')}
   if kind=='http':
    dataset=await app.evaluations.freeze(FreezeRequest.model_validate({'name':'legacy-http',
        'version':'1','cases':[{'case_id':'sum','source_kind':'manual','input':'/add 2 3',
            'reason':'synthetic arithmetic','reference':{'expected':'5',
                'expected_status':'completed','expected_code':None,'expected_tools':None,
                    'trajectory':None,'expected_schema':None}}]}))
    evaluation=await app.evaluations.start(EvalRequest.model_validate({
     'request_key':'old-eval',
     'dataset':{k:getattr(dataset,k) for k in ['name','version','fingerprint']},
     'target_key':'target-http','rules':[rule('status_code').model_dump(mode='json')],
     'execution':{'repetitions':1,'concurrency':1,'item_timeout_seconds':12,
                  'scorer_timeout_seconds':1,'batch_timeout_seconds':600}}))
    await app.evaluations.wait(evaluation.id)
    value['evaluation_id']=evaluation.id
    value['evaluation']=await app.store.eval_snapshot(evaluation.id)
   result[kind]=value
 import krim_agent
 result['consumer']=krim_agent.__file__
 Path(sys.argv[4]).write_text(json.dumps(result,default=lambda x:x.model_dump(mode='json')
     if hasattr(x,'model_dump') else str(x)))
asyncio.run(main())
'''
    worker.write_text(program)
    fixture = await HTTPFixture().__aenter__()
    class CloseFixture:
        async def close(self) -> None:
            await fixture.__aexit__(None, None, None)
    notes = data_case[1]
    notes["owned"].append(CloseFixture())
    output = tmp_path/"legacy.json"
    argv = [sys.executable, "-I", "-B", str(worker), str(material), str(tmp_path), fixture.url,
        str(output)]
    env = {"PATH": "/usr/local/bin:/usr/bin:/bin",
        "LD_LIBRARY_PATH": str(Path(sys.base_prefix)/"lib"),
           "TMPDIR": os.environ["TMPDIR"], "PYTHONDONTWRITEBYTECODE": "1"}
    process = await asyncio.to_thread(subprocess.Popen, argv, cwd=Path.cwd(), env=env,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    pidfd = os.pidfd_open(process.pid)
    try:
        stdout, stderr = await asyncio.to_thread(process.communicate, timeout=60)
    finally:
        if process.poll() is None:
            process.kill()
            await asyncio.to_thread(process.wait)
        os.close(pidfd)
    (tmp_path/"legacy-stdout.txt").write_bytes(stdout)
    (tmp_path/"legacy-stderr.txt").write_bytes(stderr)
    (tmp_path/"legacy-command.json").write_text(json.dumps({"argv": argv, "cwd": str(Path.cwd()),
        "env": env, "pid": process.pid, "rc": process.returncode,
            "waited": process.poll() is not None,
        "pidfd_closed": True, "script_sha256": hashlib.sha256(program.encode()).hexdigest()}))
    assert process.returncode == 0, stderr.decode()
    old = json.loads(output.read_bytes())
    assert Path(old["consumer"]).is_relative_to(material/"src")
    seen = len(fixture.requests)
    for kind in ["script", "http"]:
        saved = old[kind]
        http_config = (OpenAICompatibleConfig(base_url=fixture.url, model="fixture")
                       if kind == "http" else None)
        app = await open_application(Path(saved["database"]), http_config=http_config)
        assert app.sessions is not None
        notes["owned"].append(app)
        notes.setdefault("store_probes", []).append(
        (app.store, cast(Callable[[Any], Any], Connections)(app.store)))
        assert (await app.service.result(saved["run_id"])).model_dump(mode="json") == saved["facts"]
        assert (await app.service.trace(saved["run_id"])).model_dump(mode="json") == saved["trace"]
        assert (await app.store.snapshot(saved["run_id"])).target == saved["snapshot_target"]
        old_context = (await app.store.snapshot(saved["turn"]["run_id"])).session_context
        assert old_context is not None
        assert old_context.model_dump(mode="json") == saved["context"]
        replay = await app.service.start(RunRequest.model_validate(saved["request"]))
        assert replay.id == saved["run_id"] and len(fixture.requests) == seen
        if kind == "http":
            assert await app.store.eval_snapshot(saved["evaluation_id"]) == saved["evaluation"]
            current = json.loads(app.service.bindings["target-http"].descriptor_json)
            provider_file = Path.cwd()/"src/krim_agent/providers/openai_compatible.py"
            assert (current["provider"]["implementation_digest"]
                    == hashlib.sha256(provider_file.read_bytes()).hexdigest())
            assert (current["provider"]["implementation_digest"]
                    != saved["snapshot_target"]["provider"]["implementation_digest"])
            turn = await app.sessions.submit(LOCAL, "local", saved["session"]["id"], TurnRequest(
                request_key="new-on-old", text="/echo refused", expected_revision=1))
            failed = await app.sessions.wait(LOCAL, "local", saved["session"]["id"], turn.id)
            assert failed.code == "target_binding_changed" and failed.run_id is None
            assert len(fixture.requests) == seen
            session = await app.sessions.create(LOCAL, "local",
                SessionCreate(request_key="new-http-session", target_key="target-http"))
            turn = await app.sessions.submit(LOCAL, "local", session.id, TurnRequest(
                request_key="explicit-new", text="/echo new", expected_revision=0))
            finished = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
            assert finished.run_id is not None
            assert (await app.service.result(finished.run_id)).text == "new"
            assert len(fixture.requests) == seen+2
        else:
            assert (json.loads(app.service.bindings["demo"].descriptor_json)
                    == saved["snapshot_target"])
        await app.close()
    notes["legacy_upgrade"] = {"material": str(material), "consumer": old["consumer"],
        "databases": [old[k]["database"] for k in ["script", "http"]],
        "worker": str(tmp_path/"legacy-command.json"), "old_requests": seen,
        "explicit_new_requests": len(fixture.requests)-seen, "facts_and_trace_exact": True}


@pytest.mark.parametrize("phase", ["initial", "page", "commit", "cache", "empty", "request",
                                   "confirmed"])
async def test_source_anchor_rejects_valid_reference_drift(
    data_case: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: str,
) -> None:
    """实际Store故障只改本轮source_refs；保留损坏备份后恢复夹具，原finish真实收束。"""
    import sqlite3

    from sqlalchemy import update

    from krim_agent.context import compaction
    from krim_agent.contracts import ServiceError, fingerprint
    from krim_agent.storage import work
    from krim_agent.workspace.types import WorkFrame

    captured: list[Any] = []
    initial = compaction.Compactor.__init__

    def constructed(instance: Any, *args: Any, **kwargs: Any) -> None:
        initial(instance, *args, **kwargs)
        captured.append(instance)

    monkeypatch.setattr(compaction.Compactor, "__init__", constructed)
    app, wire, provider = await fast_app(data_case, tmp_path, SummaryWire())
    session = await cancelled_history(
        app, count=18 if phase == "page" else 2 if phase == "request" else 8,
        padding=100 if phase == "page" else 40 if phase == "request" else 1500)
    if phase == "cache":
        state = await app.sessions.get(LOCAL, "local", session.id)
        prior = await app.sessions.submit(LOCAL, "local", session.id, WorkspaceTurnRequest(
            request_key="cached-good", text="sum", expected_revision=state.revision,
            completion=Completion()))
        await app.sessions.wait(LOCAL, "local", session.id, prior.id)
    async with app.store.engine.connect() as conn:
        original_history = [dict(r) for r in (await conn.execute(select(turns).where(
            turns.c.session_id == session.id).order_by(turns.c.ordinal))).mappings()]
        available = [dict(r) for r in (await conn.execute(select(compactions))).mappings()]
    if phase == "cache":
        assert available and all(r["status"] == "complete" for r in available)
    old_read, old_write = app.store.work_read, app.store.work_write
    old_finish, old_append = app.store.finish, app.store.append
    armed, injected, restored = True, False, False
    original_refs: list[Any] = []
    fault: dict[str, Any] = {}
    seen = len(wire.requests)

    async def state_of(run_id: str) -> dict[str, Any]:
        async def read(conn: Any) -> dict[str, Any]:
            row = (await conn.execute(select(work_turns).where(
                work_turns.c.run_id == run_id))).mappings().one()
            rows = [dict(r) for r in (await conn.execute(select(compactions).where(
                compactions.c.run_id == run_id))).mappings()]
            return {"refs": row["source_refs"], "prepared": row["prepared_context"],
                    "compactions": rows}
        return cast(dict[str, Any], await old_read(read))

    async def inject(run_id: str, refs: Any) -> None:
        nonlocal injected, original_refs
        injected = True
        original_refs = json.loads(json.dumps(list(refs)))
        changed = ([] if phase == "empty" else original_refs[:-1]
                   if phase in {"page", "cache"} else list(reversed(original_refs)))
        assert changed != original_refs
        fault.update({"run_id": run_id, "anchor": fingerprint(original_refs),
                      "original_refs": original_refs, "drifted_refs": changed,
                      "before": await state_of(run_id),
                      "request_count_at_fault": len(wire.requests)})
        async def write(conn: Any) -> bool:
            await conn.execute(update(work_turns).where(work_turns.c.run_id == run_id).values(
                source_refs=changed))
            return True
        await old_write(write, lambda: asyncio.sleep(0, result=None))
        actual = await old_read(lambda conn: work.load(conn, run_id))
        assert actual is not None and list(actual.source_refs) == changed
        fault["individual_references_valid"] = True

    async def preserve_restore(run_id: str) -> None:
        nonlocal restored
        if not injected or restored:
            return
        fault["observed_before_fixture_restore"] = await state_of(run_id)
        fault["requests_before_fixture_restore"] = len(wire.requests)
        source = Path(data_case[1]["database"])
        destination = tmp_path/"anchor-fault-readonly-backup.sqlite"
        def copy() -> None:
            reader = sqlite3.connect(source.as_uri()+"?mode=ro", uri=True)
            writer = sqlite3.connect(destination)
            try:
                reader.backup(writer)
            finally:
                writer.close()
                reader.close()
        await asyncio.to_thread(copy)
        fault["fault_database"] = str(source)
        fault["fault_backup"] = str(destination)
        fault["fault_backup_sha256"] = hashlib.sha256(destination.read_bytes()).hexdigest()
        async def restore(conn: Any) -> bool:
            await conn.execute(update(work_turns).where(work_turns.c.run_id == run_id).values(
                source_refs=original_refs))
            return True
        await old_write(restore, lambda: asyncio.sleep(0, result=None))
        restored = True
        fault["fixture_restored_only_injected_refs"] = True

    async def read(operation: Any) -> Any:
        result = await old_read(operation)
        if armed and not injected:
            if phase in {"initial", "empty"} and isinstance(result, WorkFrame):
                run_id = await old_read(lambda conn: conn.scalar(select(work_turns.c.run_id).where(
                    work_turns.c.turn_id == result.turn_id)))
                await inject(run_id, result.source_refs)
            elif phase == "page" and operation.__name__ == "page":
                await inject(captured[-1].run_id, captured[-1].frame.source_refs)
            elif phase == "cache" and operation.__name__ == "reusable":
                await inject(captured[-1].run_id, captured[-1].frame.source_refs)
        return result

    async def append(run_id: str, kind: str, data: Any, **kwargs: Any) -> Any:
        event = await old_append(run_id, kind, data, **kwargs)
        if armed and not injected and (
            phase == "commit" and kind == "model_end" and data.get("purpose") == "summary"
            or phase == "request" and kind == "model_start" and data.get("purpose") == "task"
        ):
            await inject(run_id, captured[-1].frame.source_refs)
        return event

    async def finish(run_id: str, status: Any, code: Any = None) -> Any:
        if injected and not restored:
            fault["actual_run_outcome_before_fixture_cleanup"] = {"status": status, "code": code}
            await preserve_restore(run_id)
        return await old_finish(run_id, status, code)

    monkeypatch.setattr(app.store, "work_read", read)
    monkeypatch.setattr(app.store, "append", append)
    monkeypatch.setattr(app.store, "finish", finish)
    try:
        state = await app.sessions.get(LOCAL, "local", session.id)
        turn = await app.sessions.submit(LOCAL, "local", session.id, WorkspaceTurnRequest(
            request_key="anchor-current", text="sum actual sentinels",
            expected_revision=state.revision, completion=Completion()))
        ended = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
        if phase == "confirmed":
            selected = captured[-1]
            before = await state_of(ended.run_id)
            assert before["compactions"] and before["compactions"][0]["status"] == "complete"
            await inject(ended.run_id, selected.frame.source_refs)
            refused = False
            try:
                await selected.complete(0, before["compactions"][0]["output"])
            except ServiceError as error:
                refused = error.code == "facts_corrupt"
            finally:
                await preserve_restore(ended.run_id)
            fault["completed_pass_confirmation_refused"] = refused
        else:
            refused = ended.code == "facts_corrupt"
        assert injected and restored and fault["individual_references_valid"]
        async with app.store.engine.connect() as conn:
            history = [dict(r) for r in (await conn.execute(select(turns).where(
                turns.c.session_id == session.id, turns.c.ordinal < turn.ordinal
            ).order_by(turns.c.ordinal))).mappings()]
            rows = [dict(r) for r in (await conn.execute(select(compactions))).mappings()]
        assert history == original_history
        assert all(row in rows for row in available)
        data_case[1]["anchor_fault"] = {"phase": phase, "seen_before": seen, **fault}
        assert refused
        if phase != "confirmed":
            observed = fault["observed_before_fixture_restore"]
            assert observed["prepared"] == fault["before"]["prepared"]
            assert not any(row["status"] == "complete" for row in observed["compactions"])
            assert len(wire.requests)-seen == (1 if phase == "commit" else 0)
        else:
            assert len(wire.requests) == fault["request_count_at_fault"]
    finally:
        if injected and not restored:
            await preserve_restore(fault["run_id"])
        await app.close()


@pytest.mark.parametrize("kind", ["plain", "escaped"])
async def test_large_history_is_bounded_without_encoded_overflow(
    data_case: Any, tmp_path: Path, kind: str,
) -> None:
    from krim_agent.contracts import Message
    from krim_agent.sessions.types import ControlRequest

    app, wire, provider = await fast_app(data_case, tmp_path, SummaryWire())
    session = await cancelled_history(app, count=8, padding=1500)
    state = await app.sessions.get(LOCAL, "local", session.id)
    first = await app.sessions.submit(LOCAL, "local", session.id, WorkspaceTurnRequest(
        request_key="prior-summary", text="sum", expected_revision=state.revision,
        completion=Completion()))
    await app.sessions.wait(LOCAL, "local", session.id, first.id)
    async with app.store.engine.connect() as conn:
        available = [dict(r) for r in (await conn.execute(select(compactions))).mappings()]
    assert available and all(r["status"] == "complete" for r in available)
    async with app.service._admission:
        for index in range(122):
            state = await app.sessions.get(LOCAL, "local", session.id)
            content = (("x"*10000 if kind == "plain" else "汉🙂\n\\\""*700)
                       if index < 120 else "short tail")
            text = "SENTINEL_"+str(index+8)+"="+str(index+19)+" "+content
            assert len(text.encode()) <= 32768
            queued = await app.sessions.submit(LOCAL, "local", session.id, WorkspaceTurnRequest(
                request_key="large-history-"+str(index), text=text,
                expected_revision=state.revision, completion=Completion()))
            state = await app.sessions.get(LOCAL, "local", session.id)
            await app.sessions.control(LOCAL, "local", session.id, "cancel_turn", ControlRequest(
                request_key="large-cancel-"+str(index), expected_revision=state.revision),
                turn_id=queued.id)
    async with app.store.engine.connect() as conn:
        before = [dict(r) for r in (await conn.execute(select(turns).where(
            turns.c.session_id == session.id).order_by(turns.c.ordinal))).mappings()]
    full = [{"role": "user", "content": r["text"]} for r in before]
    canonical_bytes = len(json.dumps(full, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":"), allow_nan=False).encode())
    assert canonical_bytes > 1048576
    assert all(len(r["text"].encode()) <= 32768 for r in before)
    assert all(len(r["text"].encode()) < 100 for r in before[-2:])
    seen = len(wire.requests)
    state = await app.sessions.get(LOCAL, "local", session.id)
    turn = await app.sessions.submit(LOCAL, "local", session.id, WorkspaceTurnRequest(
        request_key="large-current", text="sum", expected_revision=state.revision,
        completion=Completion()))
    ended = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
    async with app.store.engine.connect() as conn:
        after = [dict(r) for r in (await conn.execute(select(turns).where(
            turns.c.session_id == session.id, turns.c.ordinal < turn.ordinal
        ).order_by(turns.c.ordinal))).mappings()]
        rows = [dict(r) for r in (await conn.execute(select(compactions))).mappings()]
        prepared = await conn.scalar(select(work_turns.c.prepared_context).where(
            work_turns.c.run_id == ended.run_id))
    async with app.store.engine.connect() as conn:
        conditions = await conn.scalar(select(work_turns.c.conditions).where(
            work_turns.c.run_id == ended.run_id))
    policy = conditions["context"]["policy"]
    sizes = [len(encoded(r["body"]).encode()) for r in wire.requests[seen:]]
    working_sizes = [len(encoded({"messages": [Message.model_validate(m).model_dump(mode="json")
        for m in r["body"]["messages"]], "tools": []}).encode()) for r in wire.requests[seen:]]
    data_case[1]["large_history"] = {"kind": kind, "run_id": ended.run_id,
        "canonical_bytes": canonical_bytes, "code": ended.code,
        "request_bytes": sizes, "working_input_bytes": working_sizes, "policy": policy,
        "actual_requests": len(sizes), "old_summary_rows": available,
        "new_summary_rows": [r for r in rows if r["run_id"] == ended.run_id],
        "history_unchanged": before == after, "prepared": prepared}
    assert before == after
    assert all(r in rows for r in available)
    assert ended.code == "compaction_budget"
    assert 0 < len(sizes) <= 2
    assert all(size+1024 <= provider.config.max_request_bytes for size in sizes)
    assert all(size+1024+policy["reserved_output_tokens"]*4 <= policy["max_bytes"]
               for size in working_sizes)
    assert all("tools" not in r["body"] for r in wire.requests[seen:])
    assert not any(e.kind == "tool_intent" for e in (await app.store.snapshot(ended.run_id)).events)
    assert prepared is None
    await app.close()


@pytest.mark.parametrize("kind", ["utf8", "escaping", "tool-group"])
async def test_incremental_measure_matches_wire_bytes(
    data_case: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str,
) -> None:
    from krim_agent.context import compaction
    from krim_agent.contracts import Message

    captured: list[Any] = []
    initial = compaction.Compactor.__init__
    def constructed(instance: Any, *args: Any, **kwargs: Any) -> None:
        initial(instance, *args, **kwargs)
        captured.append(instance)
    monkeypatch.setattr(compaction.Compactor, "__init__", constructed)
    app, wire, provider = await fast_app(data_case, tmp_path, SummaryWire())
    session = await cancelled_history(app, count=2, padding=40)
    state = await app.sessions.get(LOCAL, "local", session.id)
    turn = await app.sessions.submit(LOCAL, "local", session.id, WorkspaceTurnRequest(
        request_key="measure-current", text="汉🙂\n\\\"", expected_revision=state.revision,
        completion=Completion()))
    ended = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
    selected = captured[-1]
    async with app.store.engine.connect() as conn:
        prepared = await conn.scalar(select(work_turns.c.prepared_context).where(
            work_turns.c.run_id == ended.run_id))
    prepared_messages = [Message.model_validate(message) for message in prepared["messages"]]
    actual = wire.requests[-1]["body"]["messages"]
    actual_messages = [Message.model_validate(message) for message in actual]
    assert len(actual_messages) == len(prepared_messages)
    base_system, marker, _budget_tail = actual_messages[0].content.partition(
        "\n\nKrim request budget/1:"
    )
    assert marker and base_system == prepared_messages[0].content
    assert [message.model_dump(mode="json") for message in actual_messages[1:]] == [
        message.model_dump(mode="json") for message in prepared_messages[1:]
    ]
    prepared_tools = selected.tools if wire.requests[-1]["body"].get("tools") else []
    canonical = len(encoded({
        "messages": [message.model_dump(mode="json") for message in actual_messages],
        "tools": prepared_tools,
    }).encode())
    assert prepared["input_bytes"] == canonical
    assert prepared["estimated_tokens"] == (canonical+3)//4
    messages = list(prepared_messages)
    if kind == "tool-group":
        messages += [Message(role="assistant", content="组", tool_calls=[
            {"id": "measure-call", "name": "synthetic", "arguments": {"text": "汉🙂\\\""}}]),
            Message(role="tool", call_id="measure-call", content="结果\n")]
    content = "汉🙂" if kind == "utf8" else "\n\\\"\t"
    messages += [Message(role="user", content=content*100)]
    budget = selected.step.request_budget(selected.max_group, selected.remaining_tools)
    request_messages = selected.step.with_budget_prompt(messages, budget)
    request_tools = selected.tools if budget.max_tool_calls_this_step > 0 else []
    expected = len(encoded({
        "messages": [message.model_dump(mode="json") for message in request_messages],
        "tools": request_tools,
    }).encode())
    assert selected.measure(messages, selected.tools)[0] == expected
    huge = [messages[0], *[Message(role="user", content=content*1000) for _ in range(240)]]
    huge_request = selected.step.with_budget_prompt(huge, budget)
    large = len(json.dumps({
        "messages": [message.model_dump(mode="json") for message in huge_request],
        "tools": request_tools,
    }, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode())
    assert large > 1048576
    data_case[1]["measure"] = {"kind": kind, "run_id": ended.run_id,
        "prepared_bytes": canonical, "expected_legal": expected, "expected_large": large}
    assert selected.measure(huge, selected.tools) == (large, True)
    assert not any(e.kind == "tool_intent" for e in (await app.store.snapshot(ended.run_id)).events)
    await app.close()
