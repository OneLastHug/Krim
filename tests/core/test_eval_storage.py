"""Legacy evaluation history is read-only; schema migration stays atomic."""

import hashlib
import sqlite3

import pytest

from krim_agent.storage.sqlite import SCHEMA_VERSION, SQLiteStore


def v1(path):
    from sqlalchemy.dialects.sqlite import dialect
    from sqlalchemy.schema import CreateTable

    from krim_agent.storage.models import run_events, runs
    from krim_agent.storage.sqlite import APPLICATION_ID

    with sqlite3.connect(path) as conn:
        conn.execute(str(CreateTable(runs).compile(dialect=dialect())))
        conn.execute(str(CreateTable(run_events).compile(dialect=dialect())))
        conn.execute(f"PRAGMA application_id={APPLICATION_ID}")
        conn.execute("PRAGMA user_version=1")


@pytest.mark.asyncio
async def test_v1_upgrade_atomic_and_readonly_pure(tmp_path):
    from krim_agent.contracts import ServiceError

    path = tmp_path / "v1.sqlite"
    v1(path)
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    reader = await SQLiteStore.open(path, readonly=True)
    try:
        assert reader.schema_version == 1
        with pytest.raises(ServiceError, match="schema_upgrade_required"):
            await reader.eval_by_key("absent")
    finally:
        await reader.close()
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    store = await SQLiteStore.open(path)
    try:
        async with store.engine.begin() as conn:
            version = (await conn.exec_driver_sql("PRAGMA user_version")).scalar_one()
        assert version == SCHEMA_VERSION
        async with store.engine.begin() as conn:
            tables = (
                (await conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE type='table'"))
                .scalars()
                .all()
            )
        from tests.agent_core.test_extension_migration import HISTORICAL_TABLES

        assert set(tables) == HISTORICAL_TABLES[SCHEMA_VERSION]
        assert {
            "runs",
            "run_events",
            "dataset_revisions",
            "evaluations",
            "eval_items",
            "sessions",
            "turns",
            "session_controls",
            "work_sessions",
            "work_turns",
            "compactions",
            "tool_effects",
            "artifact_revisions",
        } <= set(tables)
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_migration_failure_keeps_v1(tmp_path, monkeypatch):
    from krim_agent.contracts import ServiceError
    from krim_agent.storage.models import dataset_revisions

    path = tmp_path / "rollback.sqlite"
    v1(path)

    async def fail(conn):
        await conn.run_sync(dataset_revisions.create)
        raise OSError("SYNTHETIC_DDL_FAILURE")

    monkeypatch.setattr(SQLiteStore, "_upgrade", staticmethod(fail))
    with pytest.raises(ServiceError, match="database_invalid"):
        await SQLiteStore.open(path)
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 1
        assert {
            r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        } == {"runs", "run_events"}


async def legacy_request(app, key: str):
    from krim_agent.evaluation.types import EvalRequest, FreezeRequest, rule

    revision = await app.evaluations.freeze(
        FreezeRequest.model_validate(
            {
                "name": "legacy-eval-fixture",
                "version": "1",
                "cases": [
                    {
                        "case_id": "one",
                        "source_kind": "manual",
                        "input": "/add 2 3",
                        "reason": "historical score fixture",
                        "reference": {
                            "expected": "5",
                            "expected_status": "completed",
                            "expected_code": None,
                            "expected_tools": None,
                            "trajectory": None,
                            "expected_schema": None,
                        },
                    }
                ],
            }
        )
    )
    return EvalRequest.model_validate(
        {
            "request_key": key,
            "dataset": {k: getattr(revision, k) for k in ["name", "version", "fingerprint"]},
            "target_key": "target-v2",
            "rules": [rule("status_code").model_dump(mode="json")],
            "execution": {
                "repetitions": 1,
                "concurrency": 1,
                "item_timeout_seconds": 12,
                "scorer_timeout_seconds": 1,
                "batch_timeout_seconds": 600,
            },
        }
    )


async def seed_legacy_report(app, key: str, *, score_value: int = 1):
    """Explicit historical fixture; it inserts a stored report without starting a run."""
    from krim_agent.evaluation import scorers
    from krim_agent.evaluation.types import rule

    request = await legacy_request(app, key)
    target = {"key": "target-v2", "version": "target-v2/1", "schema_version": 2}
    snapshot, created = await app.store.begin_eval(
        request, target, request.execution.model_dump(mode="json")
    )
    assert created
    await app.store.set_eval_running(snapshot["id"])
    score = scorers.result(rule("status_code"), "scored", score_value).model_dump(mode="json")
    await app.store.commit_item(
        snapshot["id"],
        "one",
        0,
        {
            "output": None,
            "scores": [score],
            "quality": "passed" if score_value else "failed",
            "code": None,
        },
    )
    return request, snapshot["id"]


@pytest.mark.asyncio
async def test_legacy_eval_start_is_retired_before_new_admission(tmp_path):
    from krim_agent.composition import open_application
    from krim_agent.contracts import ServiceError

    async with await open_application(tmp_path / "retired.sqlite") as app:
        request = await legacy_request(app, "retired-start")
        assert app.evaluations.targets() == []
        with pytest.raises(ServiceError) as retired:
            await app.evaluations.start(request)
        assert retired.value.code == "execution_retired" and retired.value.status == 410
        assert await app.store.eval_by_key(request.request_key) is None
        assert app.service.tasks == {}


@pytest.mark.asyncio
async def test_legacy_scores_remain_readable_without_reexecution(tmp_path):
    from krim_agent.composition import open_application
    from krim_agent.contracts import ServiceError

    path = tmp_path / "history.sqlite"
    async with await open_application(path) as app:
        request, eval_id = await seed_legacy_report(app, "historical-score")
        before = await app.evaluations.report(eval_id)
        assert before["complete"] is True
        assert before["items"][0]["scores"][0]["value"] == 1
        report_hash = before["report_hash"]

    async with await open_application(path) as reopened:
        assert reopened.evaluations.targets() == []
        assert (await reopened.evaluations.get(eval_id)).status == "completed"
        report = await reopened.evaluations.report(eval_id)
        assert report["report_hash"] == report_hash
        assert report["items"][0]["scores"][0]["value"] == 1
        before_tasks = dict(reopened.service.tasks)
        with pytest.raises(ServiceError) as retired:
            await reopened.evaluations.start(request)
        assert retired.value.code == "execution_retired" and retired.value.status == 410
        assert reopened.service.tasks == before_tasks == {}
        assert (await reopened.evaluations.report(eval_id))["report_hash"] == report_hash
