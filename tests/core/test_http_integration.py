"""HTTP目标与原内核的确定性接缝；只用真实本机TCP及合成库。"""

import asyncio
import hashlib
import json
import os
import socket
import sys
from argparse import Namespace
from contextlib import nullcontext
from contextvars import ContextVar
from dataclasses import replace
from pathlib import Path

import anyio
import httpx
import pytest
import uvicorn
from fixtures.http_provider_server import HTTPFixture
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncTransaction

from krim_agent.api import create_app
from krim_agent.cli import execute
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import RunEvent, RunRequest, RunView, ServiceError
from krim_agent.evaluation.types import EvalRequest, FreezeRequest, rule
from krim_agent.providers.fake import FakeProvider
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig, OpenAICompatibleProvider
from krim_agent.runtime.facts import project_facts
from krim_agent.sdk import EvaluationSDK
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.tools.builtins import builtin_registry
from krim_agent.tools.registry import ToolRegistry

ROOT = Path(__file__).resolve().parents[2]


def counted_registry(calls):
    def tracked(spec):
        async def execute(arguments):
            calls.append(spec.name)
            return await spec.executor(arguments)

        return replace(spec, executor=execute)

    return ToolRegistry(tuple(tracked(spec) for spec, _ in builtin_registry()._frozen))


async def test_http_borrowed_provider_snapshot_is_truthful(tmp_path):
    async with HTTPFixture() as fixture:
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(base_url=fixture.url, model="fixture")
        )
        try:
            async with await open_application(
                tmp_path / "borrowed.sqlite", provider=provider
            ) as app:
                view = await app.service.start(RunRequest(text="/add 2 3", request_key="borrowed"))
                assert (await app.service.wait(view.id)).status == "completed"
                facts = await app.service.result(view.id)
                assert facts.text == "5" and facts.model_steps == 2 and len(facts.tools) == 1
                assert len(fixture.requests) == 2
                target = (await app.store.snapshot(view.id)).target
                assert target["provider"]["config"].get("model") == "fixture", (
                    "真实HTTP模型身份不能写成demo mode"
                )
                assert target["provider"]["config"].get("endpoint_fingerprint")
            assert not provider.closed
        finally:
            await provider.close()


@pytest.mark.parametrize("step", [0, 1])
@pytest.mark.parametrize("changed", ["stream", "config", "client"])
async def test_http_binding_change_after_await_stops_dispatch(tmp_path, step, changed):
    async with HTTPFixture() as fixture:
        provider = OpenAICompatibleProvider(
            OpenAICompatibleConfig(base_url=fixture.url, model="fixture")
        )
        tools = []
        app = await open_application(
            tmp_path / "binding.sqlite", provider=provider, registry=counted_registry(tools)
        )
        entered, release = asyncio.Event(), asyncio.Event()
        original = app.store.append
        replacement_client = None
        replaced_entries = []

        async def append(run_id, kind, data, *args, **kwargs):
            result = await original(run_id, kind, data, *args, **kwargs)
            if kind == "model_start" and data["step_index"] == step:
                entered.set()
                await release.wait()
            return result

        app.store.append = append
        try:
            view = await app.service.start(RunRequest(text="/add 2 3", request_key="barrier"))
            async with asyncio.timeout(3):
                await entered.wait()
            before = (len(fixture.requests), len(tools))
            assert before == ((0, 0) if step == 0 else (1, 1))
            if changed == "stream":
                stream = provider.stream

                async def replacement(messages, schemas):
                    replaced_entries.append(1)
                    async for event in stream(messages, schemas):
                        yield event

                provider.stream = replacement
            elif changed == "config":
                provider.config = provider.config.model_copy(update={"model": "changed"})
            else:
                replacement_client = httpx.AsyncClient(base_url=fixture.url + "/", trust_env=False)
                provider._client = replacement_client
            release.set()
            final = await app.service.wait(view.id)
            assert (
                final.status,
                final.code,
                len(fixture.requests),
                len(tools),
                len(replaced_entries),
            ) == ("failed", "target_binding_changed", *before, 0)
            snapshot = await app.store.snapshot(view.id)
            assert any(
                e.kind == "model_start" and e.data["step_index"] == step for e in snapshot.events
            )
            assert not any(
                e.kind == "model_end" and e.data["step_index"] == step for e in snapshot.events
            )
        finally:
            release.set()
            await app.close()
            await provider.close()
            if replacement_client is not None:
                await replacement_client.aclose()
        assert provider.active == 0 and all(t.done() for t in app.service.tasks.values())


async def freeze_one(app, name="m3"):
    return await app.evaluations.freeze(
        FreezeRequest.model_validate(
            {
                "name": name,
                "version": "1",
                "cases": [
                    {
                        "case_id": "sum",
                        "source_kind": "manual",
                        "input": "/add 2 3",
                        "reference": {
                            "expected": "5",
                            "expected_status": "completed",
                            "expected_code": None,
                            "expected_tools": None,
                            "trajectory": None,
                            "expected_schema": None,
                        },
                        "reason": "人工核算2加3等于5",
                    }
                ],
            }
        )
    )


def evaluation_request(dataset, key):
    return EvalRequest.model_validate(
        {
            "request_key": key,
            "dataset": {k: getattr(dataset, k) for k in ("name", "version", "fingerprint")},
            "target_key": "target-http",
            "rules": [rule(n).model_dump(mode="json") for n in ("status_code", "exact_match")],
            "execution": {
                "repetitions": 1,
                "concurrency": 1,
                "item_timeout_seconds": 12,
                "scorer_timeout_seconds": 1,
                "batch_timeout_seconds": 600,
            },
        }
    )


@pytest.mark.parametrize("field,value", [("max_output_tokens", 256), ("read_timeout_seconds", 4.0)])
async def test_http_eval_reuses_shared_admission_and_original_keys(
    tmp_path, monkeypatch, field, value
):
    from krim_agent import composition

    calls = []
    monkeypatch.setattr(composition, "builtin_registry", lambda: counted_registry(calls))
    database = tmp_path / "gates.sqlite"
    reports = []
    async with HTTPFixture() as fixture:
        for index in range(3):
            parameters = {field: value} if index == 2 else {}
            config = OpenAICompatibleConfig(base_url=fixture.url, model="fixture", **parameters)
            async with await open_application(database, http_config=config) as app:
                dataset = await freeze_one(app)
                request = evaluation_request(dataset, "gate-" + str(index))
                view = await app.evaluations.start(request)
                assert (await app.evaluations.start(request)).id == view.id
                assert (await app.evaluations.wait(view.id)).status == "completed"
                report = await app.evaluations.report(view.id)
                assert report["complete"] and report["summary"]["cell_coverage"] == 1
                assert report["items"][0]["quality"] == "passed"
                reports.append(report)
                if index == 2:
                    sdk = EvaluationSDK(app.evaluations)
                    same = await sdk.compare(reports[0]["eval_id"], reports[1]["eval_id"])
                    changed = await sdk.compare(reports[0]["eval_id"], reports[2]["eval_id"])
                    (tmp_path / "observed.json").write_text(
                        json.dumps(
                            {
                                "reports": reports,
                                "same": same,
                                "changed": changed,
                                "requests": fixture.requests,
                                "tool_entries": calls,
                            },
                            ensure_ascii=False,
                            indent=2,
                        )
                    )
                    assert same["status"] == "pass"
                    assert changed["status"] == "invalid" and changed["reasons"] == [
                        {"code": "incompatible_execution_fingerprint"}
                    ]
                    assert reports[0]["execution_fingerprint"] != report["execution_fingerprint"]
        assert calls == ["add"] * 3
        assert len(fixture.requests) == 6
        assert (
            sum(m["role"] == "tool" for r in fixture.requests for m in r["body"]["messages"]) == 3
        )


async def test_http_target_two_steps_one_add_and_frozen_identity(tmp_path, monkeypatch):
    from krim_agent import composition

    tools = []
    monkeypatch.setattr(composition, "builtin_registry", lambda: counted_registry(tools))
    async with HTTPFixture(expected_key="SYNTHETIC_OUTBOUND") as fixture:
        config = OpenAICompatibleConfig(
            base_url=fixture.url, model="fixture", api_key="SYNTHETIC_OUTBOUND"
        )
        async with await open_application(tmp_path / "http.sqlite", http_config=config) as app:
            request = RunRequest(text="/add 2 3", request_key="http", target="target-http")
            view = await app.service.start(request)
            assert (await app.service.start(request)).id == view.id
            assert (await app.service.wait(view.id)).status == "completed"
            facts = await app.service.result(view.id)
            trace = await app.service.trace(view.id)
            snapshot = await app.store.snapshot(view.id)
            assert facts.text == "5" and facts.model_steps == 2 and len(facts.tools) == 1
            assert (
                tools == ["add"]
                and len(fixture.requests) == 2
                and facts.token_usage["total_tokens"] == 10
                and facts.usage_complete
            )
            assert all(r["authorization_matches"] for r in fixture.requests)
            assert (
                facts.tools[0].provider_call_id == "wire-call-1"
                and facts.tools[0].executor_started is True
            )
            assert trace.complete and trace.run_id == view.id
            assert (
                snapshot.target["limits"]["http"]["max_output_tokens"]
                == fixture.requests[0]["body"]["max_tokens"]
            )
            assert (
                snapshot.target["limits"]["http"]["read_timeout_seconds"]
                == app.service.bindings["target-http"].loop.provider._client.timeout.read
            )
            public = json.dumps(snapshot.target) + trace.model_dump_json()
            assert "SYNTHETIC_OUTBOUND" not in public and fixture.url not in public
            denied = await app.service.start(
                RunRequest(text="/note welcome", request_key="denied", target="target-http")
            )
            assert (await app.service.wait(denied.id)).code == "permission_denied"
            assert tools == ["add"]
            (tmp_path / "observed.json").write_text(
                json.dumps(
                    {
                        "target": snapshot.target,
                        "facts": facts.model_dump(mode="json"),
                        "trace": trace.model_dump(mode="json"),
                        "requests": fixture.requests,
                        "tools": tools,
                    },
                    ensure_ascii=False,
                )
            )


async def test_http_shared_capacity_optional_target_and_restart(tmp_path):
    path = tmp_path / "capacity.sqlite"
    async with HTTPFixture() as fixture:
        settings = Settings(database=path, max_active_runs=1)
        config = OpenAICompatibleConfig(base_url=fixture.url, model="fixture")
        async with await open_application(path, settings=settings, http_config=config) as app:
            occupying = await app.service.start(RunRequest(text="/slow", request_key="occupied"))
            await app.provider.entered.wait()
            with pytest.raises(ServiceError, match="capacity"):
                await app.service.start(
                    RunRequest(text="/add 2 3", request_key="full", target="target-http")
                )
            dataset = await freeze_one(app)
            request = evaluation_request(dataset, "capacity-eval")
            evaluation = await app.evaluations.start(request)
            await app.evaluations.wait(evaluation.id)
            assert fixture.requests == []
            await app.service.cancel(occupying.id)
            good = RunRequest(text="/add 2 3", request_key="good", target="target-http")
            view = await app.service.start(good)
            await app.service.wait(view.id)
            assert len(fixture.requests) == 2
        async with await open_application(path) as app:
            assert (await app.service.start(good)).id == view.id
            assert (await app.evaluations.start(request)).id == evaluation.id
            assert (await app.service.result(view.id)).text == "5"
            with pytest.raises(ServiceError, match="invalid_input"):
                await app.service.start(
                    RunRequest(text="new", request_key="missing", target="target-http")
                )
            with pytest.raises(ServiceError, match="invalid_input"):
                await app.evaluations.start(
                    request.model_copy(update={"request_key": "missing-eval"})
                )
            with pytest.raises(ServiceError, match="key_conflict"):
                await app.service.start(good.model_copy(update={"text": "different"}))
            assert app.provider.calls == 0 and len(fixture.requests) == 2


async def test_initialization_failure_closes_only_owned_resources(tmp_path, monkeypatch):
    from krim_agent import composition

    created = []

    class Tracked(OpenAICompatibleProvider):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            created.append(self)

    def fail(*args, **kwargs):
        raise ServiceError("execution_error")

    monkeypatch.setattr(composition, "OpenAICompatibleProvider", Tracked)
    monkeypatch.setattr(composition, "RunService", fail)
    borrowed = FakeProvider()
    async with HTTPFixture() as fixture:
        config = OpenAICompatibleConfig(base_url=fixture.url, model="fixture")
        with pytest.raises(ServiceError, match="execution_error"):
            await open_application(tmp_path / "init.sqlite", provider=borrowed, http_config=config)
        assert created and all(p.closed and p._owned_client.is_closed for p in created)
        assert not borrowed.closed and fixture.requests == []
        second = await SQLiteStore.open(tmp_path / "init.sqlite")
        await second.close()
    await borrowed.close()
    invalid = OpenAICompatibleConfig.model_construct(base_url="https://u:p@bad", model="fixture")
    with pytest.raises(ServiceError, match="provider_config"):
        await open_application(tmp_path / "invalid.sqlite", http_config=invalid)
    assert not (tmp_path / "invalid.sqlite").exists()


@pytest.mark.parametrize("action", ["cancel", "timeout", "close"])
async def test_http_cancel_timeout_close_waits_stream(tmp_path, action):
    async with HTTPFixture(mode="hold") as fixture:
        app = await open_application(
            tmp_path / "stream.sqlite",
            http_config=OpenAICompatibleConfig(
                base_url=fixture.url,
                model="fixture",
                read_timeout_seconds=0.1 if action == "timeout" else 5.0,
            ),
        )
        provider = app.service.bindings["target-http"].loop.provider
        try:
            view = await app.service.start(
                RunRequest(text="hold", request_key="held", target="target-http")
            )
            await fixture.entered.wait()
            if action == "cancel":
                assert (await app.service.cancel(view.id)).status == "aborted"
                assert (await app.service.cancel(view.id)).status == "aborted"
            elif action == "timeout":
                assert (await app.service.wait(view.id)).code == "provider_timeout"
            else:
                await asyncio.gather(app.close(), app.close())
            assert app.service.tasks[view.id].done() and provider.active == 0
            await asyncio.wait_for(fixture.idle.wait(), 2)
            assert fixture.closed_connections == 1
        finally:
            await app.close()
        assert provider.closed and app.store.closed


async def test_http_cancel_timeout_close_and_db_failure_join_resources(tmp_path, monkeypatch):
    from krim_agent import composition

    entered = {name: asyncio.Event() for name in ["bad", "held"]}
    cleaning, release, failed = (asyncio.Event() for _ in range(3))
    order = []
    registry = builtin_registry()

    def wrapped(spec):
        async def execute(arguments):
            entered[arguments.text].set()
            try:
                await asyncio.Event().wait()
            finally:
                if arguments.text == "held":
                    cleaning.set()
                    await release.wait()
                order.append("clean-" + arguments.text)

        return replace(spec, executor=execute) if spec.name == "echo" else spec

    monkeypatch.setattr(
        composition,
        "builtin_registry",
        lambda: ToolRegistry(tuple(wrapped(spec) for spec, _ in registry._frozen)),
    )
    path = tmp_path / "close.sqlite"
    async with HTTPFixture() as fixture:
        app = await open_application(
            path, http_config=OpenAICompatibleConfig(base_url=fixture.url, model="fixture")
        )
        provider = app.service.bindings["target-http"].loop.provider
        original_event, original_close, provider_close = (
            app.store._event,
            app.store.close,
            provider.close,
        )

        async def close_provider():
            order.append("provider-close")
            await provider_close()

        async def close_store():
            order.append("store-close")
            await original_close()

        provider.close, app.store.close = close_provider, close_store
        closing = []
        try:
            bad = await app.service.start(
                RunRequest(text="/echo bad", request_key="bad", target="target-http")
            )
            held = await app.service.start(
                RunRequest(text="/echo held", request_key="held", target="target-http")
            )
            async with asyncio.timeout(3):
                await asyncio.gather(*(e.wait() for e in entered.values()))

            async def fail_terminal(conn, run_id, kind, *args, **kwargs):
                if run_id == bad.id and kind == "terminal":
                    failed.set()
                    raise OSError("SYNTHETIC_FAILURE")
                return await original_event(conn, run_id, kind, *args, **kwargs)

            app.store._event = fail_terminal
            closing = [asyncio.create_task(app.close()) for _ in range(2)]
            async with asyncio.timeout(3):
                await asyncio.gather(cleaning.wait(), failed.wait())
            done, _ = await asyncio.wait(closing, timeout=0.05)
            assert not done and not provider.closed and not app.store.closed
            assert not app.service.tasks[held.id].done()
            with pytest.raises(ServiceError, match="database_busy"):
                await SQLiteStore.open(path)
            closing[0].cancel()
            await asyncio.sleep(0)
            closing[0].cancel()
            assert not closing[0].done()
            release.set()
            outcomes = await asyncio.gather(*closing, return_exceptions=True)
            assert all(isinstance(v, ServiceError) for v in outcomes)
            assert (
                all(t.done() for t in app.service.tasks.values())
                and provider.closed
                and provider.active == 0
                and app.store.closed
            )
            assert (
                order.index("clean-held")
                < order.index("provider-close")
                < order.index("store-close")
            )
            second = await SQLiteStore.open(path)
            await second.close()
            assert len(fixture.requests) == 2
            (tmp_path / "cleanup.json").write_text(
                json.dumps(
                    {
                        "order": order,
                        "tasks_done": True,
                        "writer_rejected_then_admitted": True,
                        "requests": len(fixture.requests),
                        "provider_active": provider.active,
                    }
                )
            )
        finally:
            release.set()
            app.store._event = original_event
            await asyncio.gather(*app.service.tasks.values(), *closing, return_exceptions=True)
            await asyncio.gather(app.close(), return_exceptions=True)


@pytest.mark.parametrize("point", ["begin", "tool_end", "terminal", "transaction-failure"])
async def test_lost_admission_and_result_commit_returns_use_original_ids(
    tmp_path, monkeypatch, point
):
    from krim_agent import composition

    tools = []
    monkeypatch.setattr(composition, "builtin_registry", lambda: counted_registry(tools))
    async with HTTPFixture() as fixture:
        app = await open_application(
            tmp_path / "commit.sqlite",
            http_config=OpenAICompatibleConfig(base_url=fixture.url, model="fixture"),
        )
        original = app.store._write
        lost = False

        async def lose(operation):
            nonlocal lost
            result = await original(operation)
            if not lost and (
                (point == "begin" and isinstance(result, tuple))
                or (
                    point == "terminal"
                    and isinstance(result, RunView)
                    and result.status == "completed"
                )
                or (isinstance(result, RunEvent) and result.kind == point)
            ):
                lost = True
                raise OSError("LOST_RETURN_SECRET")
            return result

        if point == "transaction-failure":
            event = app.store._event

            async def fail(conn, run_id, kind, *args, **kwargs):
                result = await event(conn, run_id, kind, *args, **kwargs)
                if kind == "tool_intent":
                    raise OSError("TRANSACTION_SECRET")
                return result

            app.store._event = fail
        else:
            app.store._write = lose
        try:
            request = RunRequest(text="/add 2 3", request_key="original", target="target-http")
            view = await app.service.start(request)
            await asyncio.gather(app.service.wait(view.id), return_exceptions=True)
            snapshot = await app.store.snapshot(view.id)
            if point == "transaction-failure":
                assert tools == [] and not app.store.healthy
                assert not any(e.kind == "tool_intent" for e in snapshot.events)
                with pytest.raises(ServiceError, match="storage_unavailable"):
                    await app.service.start(request.model_copy(update={"request_key": "new"}))
            else:
                assert lost and snapshot.run.status == "completed" and tools == ["add"]
                assert len([e for e in snapshot.events if e.kind == "tool_end"]) == 1
                assert (await app.service.start(request)).id == view.id and len(
                    fixture.requests
                ) == 2
        finally:
            await asyncio.gather(app.close(), return_exceptions=True)
        assert app.store.closed


@pytest.mark.parametrize("phase,tool_count", [("http", 0), ("before", 0), ("after", 1)])
async def test_sigkill_windows_restart_interrupts_with_zero_replay(tmp_path, phase, tool_count):
    root = ROOT
    database, counter, ready = (
        tmp_path / name for name in ["crash.sqlite", "counter.json", "ready.json"]
    )
    async with HTTPFixture(mode="hold" if phase == "http" else "normal") as fixture:
        with (tmp_path / "worker.log").open("w") as log:
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-I",
                "-B",
                str(root / "tests/core/fixtures/http_crash_worker.py"),
                "--database",
                str(database),
                "--counter",
                str(counter),
                "--ready",
                str(ready),
                "--phase",
                phase,
                "--base-url",
                fixture.url,
                cwd=tmp_path,
                env=dict(os.environ, KRIM_GUIDE_SOURCE=str(root / "src")),
                stdout=log,
                stderr=log,
            )
            try:
                async with asyncio.timeout(6):
                    while not ready.exists():
                        assert process.returncode is None, (tmp_path / "worker.log").read_text()
                        await asyncio.sleep(0.01)
                    await fixture.entered.wait()
                identity = json.loads(ready.read_text())
                assert (
                    json.loads(counter.read_text())["tools"] == tool_count
                    and len(fixture.requests) == 1
                )
                process.kill()
                assert await process.wait() == -9
            finally:
                if process.returncode is None:
                    process.kill()
                    await process.wait()
        reader = await SQLiteStore.open(database, readonly=True)
        try:
            snapshot = await reader.snapshot(identity["run_id"])
            assert snapshot.run.status == "running"
            facts = project_facts(snapshot)
            if phase != "http":
                assert (
                    facts.tools[0].executor_started is None
                    and facts.tools[0].result_available is None
                )
                assert not any(e.kind == "tool_end" for e in snapshot.events)
        finally:
            await reader.close()
        async with await open_application(database) as app:
            view = await app.service.by_key("http-crash-original")
            facts = await app.service.result(view.id)
            trace = await app.service.trace(view.id)
            assert facts.run.status == "interrupted" and not trace.complete
            if phase != "http":
                assert (
                    facts.tools[0].status == "unknown"
                    and facts.tools[0].executor_started is None
                    and facts.tools[0].result_available is None
                )
            await app.service.events(view.id)
            assert (
                await app.service.start(
                    RunRequest(
                        text="/add 2 3", request_key="http-crash-original", target="target-http"
                    )
                )
            ).id == view.id
            assert app.provider.calls == 0
        assert len(fixture.requests) == 1 and json.loads(counter.read_text())["tools"] == tool_count
        (tmp_path / "observed.json").write_text(
            json.dumps(
                {
                    "phase": phase,
                    "exit": process.returncode,
                    "provider_requests": 1,
                    "external_tools": tool_count,
                    "facts": facts.model_dump(mode="json"),
                    "trace": trace.model_dump(mode="json"),
                    "replay": 0,
                }
            )
        )


async def test_http_errors_usage_and_secrets_remain_truthful(tmp_path, caplog):
    secret = "SYNTHETIC_PRIVATE_HTTP_8b94"
    async with HTTPFixture(mode="no-usage", expected_key="SYNTHETIC_OUTBOUND_KEY") as fixture:
        async with await open_application(
            tmp_path / "privacy.sqlite",
            http_config=OpenAICompatibleConfig(
                base_url=fixture.url, model="fixture", api_key="SYNTHETIC_OUTBOUND_KEY"
            ),
        ) as app:
            view = await app.service.start(
                RunRequest(text="/echo " + secret, request_key="secret", target="target-http")
            )
            await app.service.wait(view.id)
            facts = await app.service.result(view.id)
            trace = await app.service.trace(view.id)
            assert secret in facts.text and facts.token_usage is None and not facts.usage_complete
            api = create_app(application=app)
            async with (
                api.router.lifespan_context(api),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=api), base_url="http://127.0.0.1"
                ) as client,
            ):
                health = (await client.get("/healthz")).json()
                assert health["http_enabled"] is True and health["provider"] == "script+http"
                error = (
                    await client.post("/v1/runs", json={"text": "", "request_key": "bad"})
                ).text
            public = trace.model_dump_json() + json.dumps(health) + error + caplog.text
            assert (
                secret not in public
                and "SYNTHETIC_OUTBOUND_KEY" not in public
                and fixture.url not in public
            )
            assert len(fixture.requests) == 2


class SnapshotConnections:
    """实例级真实连接事件；不以dispose/GC或closed标记代替归还。"""

    def __init__(self, store):
        self.events = []
        self.pending = set()
        self.unsafe = 0

        def checkout(conn, record, proxy):
            self.pending.add(id(record))
            self.record("checkout", connection=id(record))

        def checkin(conn, record):
            self.pending.discard(id(record))
            self.record("checkin", connection=id(record))

        def reset(conn, record, state):
            self.unsafe += int(not state.asyncio_safe)
            self.record("reset", connection=id(record), asyncio_safe=state.asyncio_safe)

        def close(conn, record):
            self.record("close", connection=id(record))

        for name, callback in [
            ("checkout", checkout),
            ("checkin", checkin),
            ("reset", reset),
            ("close", close),
        ]:
            event.listen(store.engine.sync_engine, name, callback)

    def record(self, kind, **values):
        try:
            task = asyncio.current_task()
        except RuntimeError:
            task = None
        self.events.append(
            {"kind": kind, "task": id(task), "name": task.get_name() if task else None, **values}
        )


@pytest.mark.parametrize("stage", ["enter", "query", "exit"])
@pytest.mark.parametrize("cancellation", ["once", "repeated", "anyio"])
async def test_snapshot_cancellation_joins_real_connection(
    tmp_path, monkeypatch, stage, cancellation
):
    path = tmp_path / "static.sqlite"
    writer = await SQLiteStore.open(path)
    view, _ = await writer.begin(RunRequest(text="static", request_key="static"), {})
    await writer.close()
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    store = await SQLiteStore.open(path, readonly=True)
    probe = SnapshotConnections(store)
    entered, release = asyncio.Event(), asyncio.Event()
    scope = None

    async def barrier():
        probe.record("barrier", stage=stage)
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            probe.record("barrier-cancelled")
            raise
        probe.record("released")

    start, execute, exit_ = (
        AsyncTransaction.start,
        AsyncConnection.execute,
        AsyncTransaction.__aexit__,
    )

    async def entering(transaction, *args, **kwargs):
        result = await start(transaction, *args, **kwargs)
        if transaction.connection.engine is store.engine and stage == "enter":
            await barrier()  # 真实checkout/BEGIN已完成，但__aenter__尚未返回。
        return result

    async def querying(conn, *args, **kwargs):
        result = await execute(conn, *args, **kwargs)
        if conn.engine is store.engine and stage == "query" and not entered.is_set():
            await barrier()  # 真实SELECT已执行，其await仍未向snapshot返回。
        return result

    async def exiting(transaction, *args, **kwargs):
        if transaction.connection.engine is store.engine and stage == "exit":
            await barrier()
        return await exit_(transaction, *args, **kwargs)

    monkeypatch.setattr(AsyncTransaction, "start", entering)
    monkeypatch.setattr(AsyncConnection, "execute", querying)
    monkeypatch.setattr(AsyncTransaction, "__aexit__", exiting)

    async def caller():
        nonlocal scope
        with anyio.CancelScope() as scope:
            try:
                await store.snapshot(view.id)
            except asyncio.CancelledError:
                probe.record("cancel-delivered", lock_held=store.lock.locked())
                raise
        probe.record("caller-exited")

    task = asyncio.create_task(caller(), name="snapshot-caller")
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert probe.pending, "屏障必须在真实连接checkout以后"
        if cancellation == "anyio":
            scope.cancel()
        else:
            task.cancel()
        await asyncio.sleep(0.02)
        if cancellation == "repeated":
            task.cancel()
            await asyncio.sleep(0.02)
        assert not task.done(), "释放前调用者已结束：读取未先收束"
        assert not any(e["kind"] == "cancel-delivered" for e in probe.events)
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        probe.record("joined", lock_held=store.lock.locked())
        (tmp_path / "connections.json").write_text(json.dumps(probe.events, indent=2))
        await store.close()
    kinds = [e["kind"] for e in probe.events]
    assert "cancel-delivered" in kinds and kinds.index("checkin") < kinds.index("cancel-delivered")
    assert not probe.pending and probe.unsafe == 0 and not store.lock.locked()
    assert all(not e["lock_held"] for e in probe.events if "lock_held" in e)
    cleanup_ids = {e["task"] for e in probe.events if e["kind"] == "barrier"}
    assert cleanup_ids != {id(task)}
    assert not cleanup_ids.intersection(id(t) for t in asyncio.all_tasks() if not t.done())
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


async def test_snapshot_read_boundaries_preserve_persisted_facts(tmp_path, monkeypatch):
    path = tmp_path / "boundaries.sqlite"
    writer = await SQLiteStore.open(path)
    view, _ = await writer.begin(RunRequest(text="unchanged", request_key="boundaries"), {})
    expected = await writer.snapshot(view.id)
    await writer.close()
    before = path.read_bytes()
    store = await SQLiteStore.open(path, readonly=True)
    try:
        assert await store.snapshot(view.id) == expected
        with pytest.raises(ServiceError, match="not_found"):
            await store.snapshot("absent")

        async def broken(conn, run_id):
            raise OSError("private failure")

        monkeypatch.setattr(store, "_snapshot", broken)
        with pytest.raises(ServiceError, match="storage_unavailable"):
            await store.snapshot(view.id)
    finally:
        await store.close()
        await store.close()
    with pytest.raises(ServiceError, match="closed"):
        await store.snapshot(view.id)
    assert path.read_bytes() == before


async def test_http_stream_reader_disconnect_does_not_cancel_run(tmp_path, monkeypatch):
    class Server(uvicorn.Server):
        def capture_signals(self):
            return nullcontext()

    calls = []
    async with HTTPFixture(mode="hold") as fixture:
        app = await open_application(
            tmp_path / "sse.sqlite",
            registry=counted_registry(calls),
            http_config=OpenAICompatibleConfig(base_url=fixture.url, model="fixture"),
        )
        probe = SnapshotConnections(app.store)
        reading = ContextVar("sse_read", default=False)
        entered, release, exited = asyncio.Event(), asyncio.Event(), asyncio.Event()
        original_stream, original_snapshot = app.service.stream, app.store._snapshot
        sse_task = None

        async def tracked_stream(*args, **kwargs):
            nonlocal sse_task
            sse_task = asyncio.current_task()
            token = reading.set(True)
            source = original_stream(*args, **kwargs)
            try:
                async for value in source:
                    yield value
            finally:
                await source.aclose()
                reading.reset(token)
                probe.record("sse-exited", lock_held=app.store.lock.locked())
                exited.set()

        async def snapshot(conn, run_id):
            result = await original_snapshot(conn, run_id)
            if reading.get() and not entered.is_set():
                probe.record("sse-snapshot-query")
                entered.set()
                await release.wait()
                probe.record("sse-read-released")
            return result

        monkeypatch.setattr(app.service, "stream", tracked_stream)
        monkeypatch.setattr(app.store, "_snapshot", snapshot)
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(16)
        listener.setblocking(False)
        port = listener.getsockname()[1]
        server = Server(
            uvicorn.Config(create_app(application=app), log_level="critical", access_log=False)
        )
        task = asyncio.create_task(server.serve(sockets=[listener]))
        reader = None
        disconnect = asyncio.Event()
        try:
            async with asyncio.timeout(3):
                while not server.started:
                    if task.done():
                        await task
                    await asyncio.sleep(0.005)
            async with httpx.AsyncClient(
                base_url=f"http://127.0.0.1:{port}", trust_env=False, timeout=3
            ) as client:
                response = await client.post(
                    "/v1/runs",
                    json={"text": "hold", "request_key": "sse", "target": "target-http"},
                )
                view = response.json()
                await fixture.entered.wait()

                async def consume():
                    async with client.stream("GET", "/v1/runs/" + view["id"] + "/stream"):
                        await disconnect.wait()

                reader = asyncio.create_task(consume())
                await asyncio.wait_for(entered.wait(), 2)
                assert probe.pending and sse_task is not None
                disconnect.set()
                await reader
                await asyncio.sleep(0.02)
                assert sse_task.cancelling(), "真实TCP断开必须触发StreamingResponse取消域"
                assert not exited.is_set() and not sse_task.done()
                assert app.store.lock.locked() and probe.pending
                assert not app.service.tasks[view["id"]].done()
                assert len(fixture.requests) == 1 and calls == []
                release.set()
                await asyncio.wait_for(exited.wait(), 2)
                assert not probe.pending and probe.unsafe == 0
                kinds = [e["kind"] for e in probe.events]
                assert max(i for i, k in enumerate(kinds) if k == "checkin") < kinds.index(
                    "sse-exited"
                )
                assert (await client.get("/v1/runs/" + view["id"])).json()["status"] == "running"
                assert len(fixture.requests) == 1 and calls == []
                assert (await client.post("/v1/runs/" + view["id"] + "/cancel")).json()[
                    "status"
                ] == "aborted"
                assert app.service.tasks[view["id"]].done()
                await asyncio.wait_for(fixture.idle.wait(), 2)
                assert fixture.active == 0 and len(fixture.requests) == 1 and calls == []
                with pytest.raises(ServiceError, match="database_busy"):
                    await SQLiteStore.open(tmp_path / "sse.sqlite")
        finally:
            disconnect.set()
            release.set()
            if reader is not None:
                await asyncio.gather(reader, return_exceptions=True)
            server.should_exit = True
            await asyncio.wait_for(task, 3)
            listener.close()
            await app.close()
            await app.close()
            probe.record("owner-closed")
            (tmp_path / "connections.json").write_text(json.dumps(probe.events, indent=2))
        assert not probe.pending and probe.unsafe == 0
        assert sum(e["kind"] == "checkout" for e in probe.events) == sum(
            e["kind"] == "checkin" for e in probe.events
        )
        second = await SQLiteStore.open(tmp_path / "sse.sqlite")
        await second.close()
        probe_socket = socket.socket()
        probe_socket.settimeout(0.2)
        try:
            assert probe_socket.connect_ex(("127.0.0.1", port)) != 0
        finally:
            probe_socket.close()
    assert fixture.active == 0 and not fixture.tasks and not fixture.writers


async def test_http_cli_local_remote_and_readonly_ownership(tmp_path, monkeypatch):
    async with HTTPFixture() as fixture:
        monkeypatch.setenv("KRIM_AGENT_HTTP_BASE_URL", fixture.url)
        monkeypatch.setenv("KRIM_AGENT_HTTP_MODEL", "fixture")
        path = tmp_path / "cli.sqlite"
        value = await execute(
            Namespace(
                command="run",
                url=None,
                database=str(path),
                target="target-http",
                text="/add 2 3",
                key="cli",
            )
        )
        assert value["run"]["status"] == "completed" and value["text"] == "5"
        assert len(fixture.requests) == 2, "本机CLI显式HTTP目标必须真实派发，不能落回demo"
        monkeypatch.setenv("KRIM_AGENT_HTTP_BASE_URL", "https://u:p@bad")
        cold = await execute(
            Namespace(command="get", url=None, database=str(path), id=value["run"]["id"])
        )
        assert cold["id"] == value["run"]["id"] and len(fixture.requests) == 2
        with pytest.raises(ServiceError, match="readonly"):
            await execute(
                Namespace(command="eval", operation="cancel", url=None, database=str(path), id="x")
            )
