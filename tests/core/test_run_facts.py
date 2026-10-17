import asyncio
import hashlib
import json
import os
import sys
from pathlib import Path

import pytest

from krim_agent.composition import open_application
from krim_agent.contracts import RunRequest
from krim_agent.runtime.facts import project_facts
from krim_agent.storage.sqlite import SQLiteStore

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.asyncio
@pytest.mark.parametrize("phase,expected_calls", [("before", 0), ("after", 1)])
async def test_real_sigkill_intent_windows_remain_unknown_and_never_replay(
    tmp_path, phase, expected_calls
):
    db = tmp_path / "crash.db"
    counter = tmp_path / "counter.json"
    ready = tmp_path / "ready.json"
    root = ROOT
    env = dict(os.environ, KRIM_GUIDE_SOURCE=str(root / "src"))
    with (tmp_path / "worker.log").open("w") as log:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            "-B",
            str(root / "tests/core/fixtures/crash_worker.py"),
            str(db),
            str(counter),
            str(ready),
            phase,
            cwd=tmp_path,
            env=env,
            stdout=log,
            stderr=log,
        )
        try:
            async with asyncio.timeout(4):
                while not ready.exists():
                    assert process.returncode is None, (tmp_path / "worker.log").read_text()
                    await asyncio.sleep(0.01)
            identity = json.loads(ready.read_text())
            counts = json.loads(counter.read_text())
            assert counts == {"calls": expected_calls, "provider_calls": 1}
            process.kill()
            assert await asyncio.wait_for(process.wait(), 3) == -9
        finally:
            if process.returncode is None:
                process.kill()
                await asyncio.wait_for(process.wait(), 3)
    before = hashlib.sha256(db.read_bytes()).hexdigest()
    reader = await SQLiteStore.open(db, readonly=True)
    try:
        snapshot = await reader.snapshot(identity["run_id"])
        assert snapshot.run.status == "running"
        assert any(e.kind == "tool_intent" for e in snapshot.events)
        assert all(e.kind != "tool_end" for e in snapshot.events)
        facts = project_facts(snapshot)
        assert facts.tools[0].executor_started is None and facts.tools[0].result_available is None
    finally:
        await reader.close()
    assert hashlib.sha256(db.read_bytes()).hexdigest() == before
    async with await open_application(db) as app:
        run = await app.service.by_key("crash-original")
        facts = await app.service.result(run.id)
        trace = await app.service.trace(run.id)
        assert facts.run.status == "interrupted" and facts.tools[0].status == "unknown"
        assert facts.tools[0].executor_started is None and facts.tools[0].result_available is None
        assert not trace.complete and any(s.kind == "tool" and s.end is None for s in trace.spans)
        await app.service.events(run.id)
        await app.service.result(run.id)
        await app.service.by_key("crash-original")
        assert app.provider.calls == 0
    assert json.loads(counter.read_text()) == counts
    async with await open_application(db) as app:
        assert (await app.service.result(identity["run_id"])).run.status == "interrupted"
        assert app.provider.calls == 0 and json.loads(counter.read_text()) == counts


@pytest.mark.asyncio
async def test_private_output_secret_and_exception_do_not_leak_into_trace(tmp_path, caplog):
    secret = "SYNTHETIC_SECRET_9f31_Authorization_cookie"
    async with await open_application(tmp_path / "privacy.db") as app:
        run = await app.service.start(RunRequest(text="/echo " + secret, request_key="privacy"))
        await app.service.wait(run.id)
        facts = await app.service.result(run.id)
        trace = await app.service.trace(run.id)
        assert secret in facts.text
        assert secret not in trace.model_dump_json() and secret not in caplog.text
        assert facts.token_usage is None and facts.usage_complete is False
