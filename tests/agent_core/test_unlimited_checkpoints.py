"""unlimited 策略检查点：内容/总量/条目数不设应用门槛，默认不自动清理历史。

对照有限协议见 test_sandbox_checkpoints.py（旧额度语义显式 limited）。
"""

import time
import uuid

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.sandbox.checkpoints import ShadowCheckpoints
from krim_agent.sandbox.quota import QuotaLimits


@pytest.fixture
def unlimited(tmp_path):
    root = tmp_path / "checkpoints"
    root.mkdir(mode=0o700)
    return ShadowCheckpoints(root), str(uuid.uuid4()), str(uuid.uuid4())


def _object_missing(store, env, commit):
    repo = store.root / env / "objects.git"
    output = store._git(repo, ["cat-file", "--batch-check"], (commit + chr(10)).encode())
    return output.strip().endswith(b"missing")


def _age(patch, seconds):
    now = time.time()
    patch.setattr(time, "time", lambda: now + seconds)


def test_capture_allows_large_files_and_read_back(unlimited):
    store, env, project = unlimited
    big = b"x" * (4 * 1024**2 + 1)  # 超过旧 MAX_FILE 单文件边界
    files = {"big.bin": big, "data/second.bin": b"y" * (3 * 1024**2), "空文件": b""}
    row = store.capture(env, project, files, [], QuotaLimits())
    assert row["total_bytes"] == len(big) + 3 * 1024**2
    assert store.content(env, project, row["checkpoint_id"], "big.bin") == big
    assert store.content(env, project, row["checkpoint_id"], "data/second.bin") == files[
        "data/second.bin"
    ]
    assert store.content(env, project, row["checkpoint_id"], "空文件") == b""


def test_full_history_beyond_twenty_is_kept_without_auto_prune(unlimited, monkeypatch):
    store, env, project = unlimited
    limits = QuotaLimits()
    ids = [
        store.capture(env, project, {f"f{i:02d}.txt": str(i).encode()}, [], limits)[
            "checkpoint_id"
        ]
        for i in range(21)
    ]
    assert [row["checkpoint_id"] for row in store.list(env, project)] == ids
    # 远超默认 7 天保留期后再捕获，历史仍完整：捕获不做期限/数量自动清理。
    _age(monkeypatch, 30 * 86400)
    newest = store.capture(env, project, {"f21.txt": b"21"}, [], limits)
    listed = {row["checkpoint_id"] for row in store.list(env, project)}
    assert newest["checkpoint_id"] in listed
    assert set(ids) <= listed


def test_unlimited_limits_produce_no_automatic_cleanup_candidates(unlimited, monkeypatch):
    store, env, project = unlimited
    limits = QuotaLimits()
    first = store.capture(env, project, {"a": b"old"}, [], limits)
    _age(monkeypatch, 30 * 86400)
    second = store.capture(env, project, {"a": b"new"}, [], limits)
    # unlimited 的数值字段是账面值：预览不生成期限/数量/水位候选，空计划确认是无害 no-op。
    plan = store.cleanup_preview(env, project, QuotaLimits())
    assert plan["candidates"] == []
    assert plan["protected"] == []
    result = store.cleanup_confirm(env, project, plan["plan_id"], plan["plan_digest"], "sched:1")
    assert result["status"] == "completed" and result["removed"] == []
    listed = {row["checkpoint_id"] for row in store.list(env, project)}
    assert {first["checkpoint_id"], second["checkpoint_id"]} <= listed


def test_manual_cleanup_with_explicit_limited_criteria_removes_expired(unlimited, monkeypatch):
    store, env, project = unlimited
    limits = QuotaLimits()
    first = store.capture(env, project, {"a": b"old"}, [], limits)
    _age(monkeypatch, 30)
    second = store.capture(env, project, {"a": b"new"}, [], limits)
    # 用户明确的生命周期选择：显式 limited 标准仍可生成候选并两阶段确认。
    criteria = QuotaLimits(resource_policy="limited", checkpoint_retention_seconds=1)
    plan = store.cleanup_preview(env, project, criteria)
    assert [c["checkpoint_id"] for c in plan["candidates"]] == [first["checkpoint_id"]]
    assert plan["candidates"][0]["reason"] == "expired"
    result = store.cleanup_confirm(env, project, plan["plan_id"], plan["plan_digest"], "manual:1")
    assert result["removed"] == [first["checkpoint_id"]]
    assert [row["checkpoint_id"] for row in store.list(env, project)] == [second["checkpoint_id"]]


def test_cleanup_plan_holds_1001_candidates_and_large_record(unlimited, monkeypatch):
    store, env, project = unlimited
    rows = [
        {
            "checkpoint_id": str(uuid.uuid4()),
            "commit": "a" * 40,
            "pinned": False,
            "references": [],
            "created_at": 0.0,
            "total_bytes": 100,
        }
        for _ in range(1001)
    ]
    monkeypatch.setattr(store, "_metadata", lambda repo, project_id: [dict(r) for r in rows])
    criteria = QuotaLimits(resource_policy="limited", checkpoint_retention_seconds=1)
    plan = store.cleanup_preview(env, project, criteria)
    # 候选数量与计划记录大小不再有 1000 项/64KiB 硬拒绝。
    assert len(plan["candidates"]) == 1001
    assert plan["logical_bytes"] == 1001 * 100
    record = store._read_record(
        store._records(env) / "plans" / project / (plan["plan_id"] + ".json")
    )
    assert record["plan_id"] == plan["plan_id"] and len(record["candidates"]) == 1001


def test_zero_budgets_still_disable_capture(unlimited):
    store, env, project = unlimited
    for limits in [
        QuotaLimits(checkpoint_bytes=0),
        QuotaLimits(checkpoint_count=0),
        QuotaLimits(checkpoint_retention_seconds=0),  # 旧禁用语义：未配置保留期即不启用
    ]:
        with pytest.raises(ServiceError, match="sandbox_limit"):
            store.capture(env, project, {"a": b"ok"}, [], limits)
    assert store.list(env, project) == []


def test_prune_under_unlimited_never_deletes_by_retention_or_count(unlimited, monkeypatch):
    """unlimited 的账面数值（含 0 禁用）不是清理授权：prune/_prune 一律不删引用，
    只物理回收不可达对象；按期限/数量清理必须显式 limited 标准。"""
    store, env, project = unlimited
    limits = QuotaLimits()
    first = store.capture(env, project, {"a": b"old"}, [], limits)
    _age(monkeypatch, 10**6)
    second = store.capture(env, project, {"a": b"new"}, [], limits)
    repo = store.root / env / "objects.git"
    orphan = store._oid(repo, ["hash-object", "-w", "--stdin"], b"orphan")
    for criteria in [
        QuotaLimits(),
        QuotaLimits(checkpoint_retention_seconds=1),
        QuotaLimits(checkpoint_count=1),
        QuotaLimits(checkpoint_bytes=0),
        QuotaLimits(checkpoint_count=0),
        QuotaLimits(checkpoint_retention_seconds=0),
    ]:
        assert store.prune(env, project, criteria)["count"] == 2
    assert _object_missing(store, env, orphan)  # 物理回收不可达对象仍执行
    listed = {row["checkpoint_id"] for row in store.list(env, project)}
    assert listed == {first["checkpoint_id"], second["checkpoint_id"]}
    assert store.content(env, project, first["checkpoint_id"], "a") == b"old"
    # 显式 limited 标准才按期限/数量清理。
    assert store.prune(
        env, project, QuotaLimits(resource_policy="limited", checkpoint_retention_seconds=1)
    )["count"] == 1
    assert [row["checkpoint_id"] for row in store.list(env, project)] == [
        second["checkpoint_id"]
    ]
    assert _object_missing(store, env, first["commit"])
