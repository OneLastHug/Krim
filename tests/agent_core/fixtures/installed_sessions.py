"""同包/旧包安装身份与只读回滚驱动；不注入源码路径。"""

import argparse
import asyncio
import hashlib
import importlib.metadata
import json
import sys
import uuid
from pathlib import Path

import krim_agent
from krim_agent.composition import open_application
from krim_agent.contracts import RunRequest
from krim_agent.evaluation.types import EvalRequest, FreezeRequest, rule
from krim_agent.runtime.facts import project_facts
from krim_agent.storage.sqlite import SQLiteStore


def identity():
    module = Path(krim_agent.__file__).resolve()
    assert sys.flags.isolated == 1
    assert module.is_relative_to(Path(sys.prefix).resolve())
    assert "site-packages" in module.parts
    assert not any(str(p).endswith("source/src") for p in sys.path)
    assert importlib.metadata.version("krim-agent") == krim_agent.__version__
    return {
        "prefix": sys.prefix,
        "module": str(module),
        "version": krim_agent.__version__,
        "isolated": sys.flags.isolated,
    }


def reference():
    return {
        "expected": "5",
        "expected_status": "completed",
        "expected_code": None,
        "expected_tools": None,
        "trajectory": None,
        "expected_schema": None,
    }


async def legacy(app):
    run = await app.service.start(RunRequest(text="/add 2 3", request_key="baseline-run"))
    await app.service.wait(run.id)
    facts = await app.service.result(run.id)
    assert facts.text == "5" and facts.model_steps == 2 and len(facts.tools) == 1
    candidate = await app.evaluations.candidate(run.id, True)
    assert candidate.eligibility == "eligible"
    dataset = await app.evaluations.freeze(
        FreezeRequest.model_validate(
            {
                "name": "baseline",
                "version": "1",
                "cases": [
                    {
                        "case_id": "one",
                        "source_kind": "run",
                        "source_run_id": run.id,
                        "candidate_fingerprint": candidate.candidate_fingerprint,
                        "selected_input": "/add 2 3",
                        "reason": "人工核算5",
                        "reference": reference(),
                    }
                ],
            }
        )
    )
    evaluation = await app.evaluations.start(
        EvalRequest.model_validate(
            {
                "request_key": "baseline-eval",
                "dataset": {k: getattr(dataset, k) for k in ["name", "version", "fingerprint"]},
                "target_key": "target-v2",
                "rules": [
                    rule("status_code").model_dump(mode="json"),
                    rule("exact_match").model_dump(mode="json"),
                ],
                "execution": {
                    "repetitions": 1,
                    "concurrency": 1,
                    "item_timeout_seconds": 12,
                    "scorer_timeout_seconds": 1,
                    "batch_timeout_seconds": 600,
                },
            }
        )
    )
    await app.evaluations.wait(evaluation.id)
    report = await app.evaluations.report(evaluation.id)
    assert report["complete"] is True
    return {
        "run_id": run.id,
        "facts": facts.model_dump(mode="json"),
        "candidate": candidate.model_dump(mode="json"),
        "eval_id": evaluation.id,
        "report_hash": report["report_hash"],
        "dataset_fingerprint": dataset.fingerprint,
    }


async def main(args):
    record = identity()
    database = Path(args.database or Path(args.evidence) / "installed.sqlite")
    seed = json.loads(await asyncio.to_thread(Path(args.seed).read_text)) if args.seed else None
    if args.mode == "rollback-read":
        assert krim_agent.__version__ == "0.2.0a1"
        before = await asyncio.to_thread(database.read_bytes)
        store = await SQLiteStore.open(database, readonly=True)
        try:
            assert store.schema_version == 2
            assert (
                project_facts(await store.snapshot(seed["run_id"])).model_dump(mode="json")
                == seed["facts"]
            )
            assert (await store.eval_snapshot(seed["eval_id"]))["report_hash"] == seed[
                "report_hash"
            ]
        finally:
            await store.close()
        assert await asyncio.to_thread(database.read_bytes) == before
        async with await open_application(database) as old_app:
            original = await old_app.service.start(
                RunRequest(text="/add 2 3", request_key="baseline-run")
            )
            assert original.id == seed["run_id"]
            assert old_app.provider.calls == 0
            report = await old_app.evaluations.report(seed["eval_id"])
            assert report["report_hash"] == seed["report_hash"]
        assert old_app.provider.active == 0 and old_app.store.closed
        assert await asyncio.to_thread(database.read_bytes) == before
        return record | {
            "database_sha256": hashlib.sha256(before).hexdigest(),
            "readonly_unchanged": True,
            "writer_original_key_zero_replay": True,
        }
    async with await open_application(database) as app:
        if args.mode == "baseline":
            assert krim_agent.__version__ == "0.2.0a1" and app.store.schema_version == 2
            return record | await legacy(app)
        assert krim_agent.__version__ == "0.3.0a1" and app.store.schema_version == 3
        if args.mode == "upgrade":
            assert (await app.service.result(seed["run_id"])).model_dump(mode="json") == seed[
                "facts"
            ]
            assert (await app.evaluations.report(seed["eval_id"]))["report_hash"] == seed[
                "report_hash"
            ]
            assert app.provider.calls == 0
        from krim_agent.sdk import SessionSDK
        from krim_agent.sessions.types import SessionCreate, TurnRequest

        sdk = SessionSDK(app.sessions)
        session = await sdk.create(SessionCreate(request_key=uuid.uuid4().hex, title="安装验收"))
        ids = []
        for index, text in enumerate(["/echo hello", "/add 2 3"]):
            current = await sdk.get(session.id)
            turn = await sdk.submit(
                session.id,
                TurnRequest(request_key=str(index), text=text, expected_revision=current.revision),
            )
            turn = await sdk.wait(session.id, turn.id)
            assert turn.status == "completed"
            result = await sdk.result(session.id, turn.id)
            assert result.model_steps == 2 and len(result.tools) == 1
            assert (
                result.tools[0].executor_started is True
                and result.tools[0].result_available is True
            )
            ids.append(turn.id)
        assert result.text == "5" and len(await sdk.history(session.id)) == 2
        assert app.provider.calls == 4
        snapshot = await app.store.snapshot(turn.run_id)
        assert snapshot.session_context.context.sources[0].turn_id == ids[0]
        assert snapshot.session_context.context.messages[1].content == "/echo hello"
        candidate = await app.evaluations.candidate(turn.run_id)
        assert (
            candidate.eligibility == "needs_context" and candidate.observed["status"] == "completed"
        )
        if args.mode == "verify":
            old = await legacy(app)
            record.update(old)
        record.update(
            session_id=session.id,
            turn_ids=ids,
            provider_calls=app.provider.calls,
            database=str(database),
        )
    assert app.provider.active == 0 and app.store.closed
    assert all(task.done() for task in app.service.tasks.values())
    assert app.sessions._dispatcher.done()
    before = await asyncio.to_thread(database.read_bytes)
    readonly = await SQLiteStore.open(database, readonly=True)
    try:
        from krim_agent.sdk import SessionSDK

        assert len(await SessionSDK(readonly).history(session.id)) == 2
    finally:
        await readonly.close()
    assert await asyncio.to_thread(database.read_bytes) == before
    probe = await SQLiteStore.open(database)
    await probe.close()
    assert probe.closed
    return record | {"closed": True, "readonly_unchanged": True, "second_writer": True}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--database")
    parser.add_argument(
        "--mode", choices=["verify", "baseline", "upgrade", "rollback-read"], default="verify"
    )
    parser.add_argument("--seed")
    args = parser.parse_args()
    result = asyncio.run(main(args))
    root = Path(args.evidence)
    root.mkdir(parents=True, exist_ok=True)
    output = root / (args.mode + ".json")
    assert not output.exists(), "evidence must not be overwritten"
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(result, ensure_ascii=False))
