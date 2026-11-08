"""真实Git对象：独立检查点、按持有者引用、legacy保留、引用GC与恢复预览。"""

import time
import uuid

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.sandbox import checkpoints as checkpoint_module
from krim_agent.sandbox.checkpoints import ShadowCheckpoints
from krim_agent.sandbox.filesystem import ProjectFiles
from krim_agent.sandbox.quota import QuotaLimits


@pytest.fixture
def checkpoints(tmp_path):
    root = tmp_path / "checkpoints"
    root.mkdir(mode=0o700)
    return ShadowCheckpoints(root), str(uuid.uuid4()), str(uuid.uuid4())


def _refs(store, env, pattern):
    repo = store.root / env / "objects.git"
    return store._git(repo, ["for-each-ref", "--format=%(refname)", pattern]).decode().splitlines()


def _object_missing(store, env, commit):
    repo = store.root / env / "objects.git"
    output = store._git(repo, ["cat-file", "--batch-check"], (commit + chr(10)).encode())
    return output.strip().endswith(b"missing")


def _age(patch, seconds):
    now = time.time()
    patch.setattr(time, "time", lambda: now + seconds)


def test_capture_unchanged_content_recovery_and_preview(checkpoints):
    store, env, project = checkpoints
    limits = QuotaLimits()
    row = store.capture(env, project, {"src/a.py": b"print(1)", "空文件": b""}, [], limits)
    assert store.content(env, project, row["checkpoint_id"], "src/a.py") == b"print(1)"
    again = store.capture(env, project, {"空文件": b"", "src/a.py": b"print(1)"}, [], limits)
    assert again["checkpoint_id"] == row["checkpoint_id"] and again["unchanged"]
    assert len(store.list(env, project)) == 1
    preview = store.preview_restore(
        env, project, row["checkpoint_id"], {"src/a.py": b"print(2)", "later.txt": b"later"}
    )
    assert {item["path"] for item in preview["changes"]} == {"src/a.py", "空文件", "later.txt"}
    # 当前新增文件默认保留：不构成删除授权，单列 preserved。
    assert next(i for i in preview["changes"] if i["path"] == "later.txt")["action"] == "keep"
    assert preview["preserved"] == ["later.txt"]
    assert preview["requires_confirmation"] is True
    with pytest.raises(ServiceError, match="not_found"):
        store.content(env, str(uuid.uuid4()), row["checkpoint_id"], "src/a.py")


def test_reference_owners_coexist_and_release_only_own(checkpoints):
    store, env, project = checkpoints
    row = store.capture(env, project, {"a": b"one"}, [], QuotaLimits(checkpoint_count=1))
    checkpoint = row["checkpoint_id"]
    job, other, restore = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
    store.reference(env, project, checkpoint, "job", job)
    store.reference(env, project, checkpoint, "user", "owner")
    store.reference(env, project, checkpoint, "restore-target", restore)
    store.reference(env, project, checkpoint, "job", job)  # 同持有者重复引用幂等
    listed = next(r for r in store.list(env, project) if r["checkpoint_id"] == checkpoint)
    assert listed["references"] == [
        {"kind": "job", "owner": job},
        {"kind": "restore-target", "owner": restore},
        {"kind": "user", "owner": "owner"},
    ]
    assert listed["pinned"] is True
    store.reference(env, project, checkpoint, "job", other)
    store.release_reference(env, project, checkpoint, "job", job)
    remaining = next(r for r in store.list(env, project) if r["checkpoint_id"] == checkpoint)
    assert {"kind": "job", "owner": other} in remaining["references"]
    assert remaining["pinned"] is True
    # 引用保护越过期限清理；释放后即可回收。
    with pytest.MonkeyPatch.context() as patch:
        _age(patch, 10**6)
        assert store.prune(env, project, QuotaLimits(checkpoint_retention_seconds=1))["count"] == 1
    store.release_reference(env, project, checkpoint, "user", "owner")
    assert next(r for r in store.list(env, project))["pinned"] is False
    assert _refs(store, env, f"refs/holds/{project}/{checkpoint}/user/") == []
    store.release_reference(env, project, checkpoint, "user", "owner")  # 缺失引用幂等 no-op
    # 非法持有者：kind 白名单、user 固定 owner、其余严格 UUID，不接受路径。
    for kind, owner in [
        ("job", "../../etc"),
        ("job", "not-a-uuid"),
        ("user", str(uuid.uuid4())),
        ("unknown", str(uuid.uuid4())),
        ("user", "owner/extra"),
    ]:
        with pytest.raises(ServiceError, match="invalid_input"):
            store.reference(env, project, checkpoint, kind, owner)


def test_unknown_job_reference_protects_from_prune_and_gc(checkpoints):
    store, env, project = checkpoints
    row = store.capture(env, project, {"a": b"keep"}, [], QuotaLimits())
    checkpoint, commit = row["checkpoint_id"], row["commit"]
    repo = store.root / env / "objects.git"
    owner = str(uuid.uuid4())
    ref = f"refs/holds/{project}/{checkpoint}/job/{owner}"
    store._git(repo, ["update-ref", ref, commit])
    with pytest.MonkeyPatch.context() as patch:
        _age(patch, 10**6)
        assert store.prune(env, project, QuotaLimits(checkpoint_retention_seconds=1))["count"] == 1
    preview = store.cleanup_preview(env, project, QuotaLimits(checkpoint_retention_seconds=1))
    assert preview["candidates"] == []
    assert preview["protected"] == [checkpoint]
    assert preview["plan_digest"] and preview["expires_at"] > preview["created_at"]
    store._git(repo, ["update-ref", "-d", ref])
    assert next(r for r in store.list(env, project))["pinned"] is False
    with pytest.MonkeyPatch.context() as patch:
        _age(patch, 10**6 + 1)
        assert store.prune(env, project, QuotaLimits(checkpoint_retention_seconds=1))["count"] == 0


def test_legacy_pin_protected_until_explicit_migration(checkpoints):
    store, env, project = checkpoints
    limits = QuotaLimits(checkpoint_retention_seconds=1)
    row = store.capture(env, project, {"a": b"legacy"}, [], limits)
    checkpoint, commit = row["checkpoint_id"], row["commit"]
    repo = store.root / env / "objects.git"
    legacy = f"refs/retained/{project}/{checkpoint}"
    store._git(repo, ["update-ref", legacy, commit])
    with pytest.MonkeyPatch.context() as patch:
        _age(patch, 10)
        assert store.prune(env, project, limits)["count"] == 1
    listed = next(r for r in store.list(env, project))
    assert listed["pinned"] is True and listed["references"] == []
    # pin(False) 只删用户引用，legacy 保留直至显式迁移。
    store.pin(env, project, checkpoint, pinned=False)
    assert _refs(store, env, legacy) == [legacy]
    assert next(r for r in store.list(env, project))["pinned"] is True
    with pytest.MonkeyPatch.context() as patch:
        _age(patch, 11)
        assert store.prune(env, project, limits)["count"] == 1
    # 显式迁移后失去保护，可按期限回收。
    store._git(repo, ["update-ref", "-d", legacy])
    assert next(r for r in store.list(env, project))["pinned"] is False
    with pytest.MonkeyPatch.context() as patch:
        _age(patch, 12)
        assert store.prune(env, project, limits)["count"] == 0
    assert _object_missing(store, env, commit)


def test_count_limit_pin_and_actual_object_gc(checkpoints):
    store, env, project = checkpoints
    limits = QuotaLimits(checkpoint_count=1)
    first = store.capture(env, project, {"a": b"one"}, [], limits)
    store.pin(env, project, first["checkpoint_id"], pinned=True)
    with pytest.raises(ServiceError, match="sandbox_limit"):
        store.capture(env, project, {"a": b"two"}, [], limits)
    assert store.content(env, project, first["checkpoint_id"], "a") == b"one"
    store.pin(env, project, first["checkpoint_id"], pinned=False)
    second = store.capture(env, project, {"a": b"two"}, [], limits)
    assert len(store.list(env, project)) == 1
    assert store.content(env, project, second["checkpoint_id"], "a") == b"two"
    with pytest.raises(ServiceError, match="not_found"):
        store.content(env, project, first["checkpoint_id"], "a")
    assert _object_missing(store, env, first["commit"])


def test_expiry_is_not_allowed_to_remove_pinned_content(checkpoints, monkeypatch):
    store, env, project = checkpoints
    limits = QuotaLimits(checkpoint_retention_seconds=1)
    row = store.capture(env, project, {"keep": b"important"}, [], limits)
    store.pin(env, project, row["checkpoint_id"], pinned=True)
    _age(monkeypatch, 10)
    assert store.prune(env, project, limits)["count"] == 1
    store.pin(env, project, row["checkpoint_id"], pinned=False)
    assert store.prune(env, project, limits)["count"] == 0


def test_reject_secret_paths_and_oversized_capture(checkpoints):
    store, env, project = checkpoints
    for path in ["../escape", ".git/config", ".env", "/etc/passwd"]:
        with pytest.raises(ServiceError):
            store.capture(env, project, {path: b"secret"}, [], QuotaLimits())
    with pytest.raises(ServiceError, match="artifact_limit"):
        store.capture(env, project, {"big": b"x" * (4 * 1024**2 + 1)}, [], QuotaLimits())
    with pytest.raises(ServiceError, match="sandbox_limit"):
        store.capture(env, project, {"a": b"ok"}, [], QuotaLimits(checkpoint_count=0))


def test_snapshot_exclusions_and_user_git_configuration_are_not_executed(
    checkpoints, tmp_path, monkeypatch
):
    store, env, project = checkpoints
    accounts = tmp_path / "accounts"
    accounts.mkdir()
    files = ProjectFiles(accounts)
    files.create(env, project)
    content = accounts / env / "projects" / project / "content"
    marker = tmp_path / "must-not-run"
    (content / ".git").mkdir()
    (content / ".git/config").write_text("[core]\nfsmonitor = touch " + str(marker))
    (content / ".env").write_text("secret")
    (content / "node_modules").mkdir()
    (content / "node_modules/lib").write_text("dependency")
    (content / "safe.txt").write_text("safe")
    (content / "outside").symlink_to("/etc/passwd")
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "core.fsmonitor")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", "touch " + str(marker))
    try:
        frozen = files.snapshot(env, project)
        assert set(frozen["files"]) == {"safe.txt"}
        assert {i["path"] for i in frozen["excluded"]} == {
            ".git",
            ".env",
            "node_modules",
            "outside",
        }
        row = store.capture(env, project, frozen["files"], frozen["excluded"], QuotaLimits())
        assert store.content(env, project, row["checkpoint_id"], "safe.txt") == b"safe"
        assert not marker.exists()
        assert (content / ".git/config").read_text().startswith("[core]")
    finally:
        files.close()


def test_cleanup_confirm_removes_candidates_and_objects(checkpoints):
    store, env, project = checkpoints
    limits = QuotaLimits()
    first = store.capture(env, project, {"a": b"old"}, [], limits)
    with pytest.MonkeyPatch.context() as patch:
        _age(patch, 30)
        second = store.capture(env, project, {"a": b"new"}, [], limits)
        store.pin(env, project, second["checkpoint_id"], pinned=True)
        plan = store.cleanup_preview(
            env, project, QuotaLimits(checkpoint_retention_seconds=1, checkpoint_count=1)
        )
    assert [c["checkpoint_id"] for c in plan["candidates"]] == [first["checkpoint_id"]]
    assert plan["candidates"][0]["reason"] == "expired"
    assert plan["protected"] == [second["checkpoint_id"]]
    assert plan["logical_bytes"] == len(b"old")
    key = "cleanup-confirm-key-1"
    result = store.cleanup_confirm(env, project, plan["plan_id"], plan["plan_digest"], key)
    assert result["status"] == "completed"
    assert result["removed"] == [first["checkpoint_id"]]
    assert result["references_removed"] == [f"refs/krim/{project}/{first['checkpoint_id']}"]
    assert _refs(store, env, f"refs/krim/{project}/") == [
        f"refs/krim/{project}/{second['checkpoint_id']}"
    ]
    assert _object_missing(store, env, first["commit"])
    assert not _object_missing(store, env, second["commit"])
    assert result["physical_bytes_after"] <= result["physical_bytes_before"]
    assert result["physical_reclaim_bytes"] >= 0
    # 同键同计划幂等：返回同一份结果，不再改动对象库。
    again = store.cleanup_confirm(env, project, plan["plan_id"], plan["plan_digest"], key)
    assert again == result
    # 同键不同计划冲突。
    other = store.cleanup_preview(env, project, QuotaLimits(checkpoint_retention_seconds=1))
    with pytest.raises(ServiceError, match="key_conflict"):
        store.cleanup_confirm(env, project, other["plan_id"], other["plan_digest"], key)
    # 摘要不匹配拒绝执行。
    with pytest.raises(ServiceError, match="artifact_conflict"):
        store.cleanup_confirm(env, project, plan["plan_id"], "0" * 64, "cleanup-request-2")
    # 非法 request_key。
    for bad in ["", "x" * 129, "含非ASCII键"]:
        with pytest.raises(ServiceError, match="invalid_input"):
            store.cleanup_confirm(env, project, plan["plan_id"], plan["plan_digest"], bad)


def test_cleanup_candidate_changed_conflicts_without_deletion(checkpoints):
    store, env, project = checkpoints
    row = store.capture(env, project, {"a": b"one"}, [], QuotaLimits())
    checkpoint, commit = row["checkpoint_id"], row["commit"]
    other = store.capture(env, project, {"b": b"two"}, [], QuotaLimits())
    plan = store.cleanup_preview(
        env, project, QuotaLimits(checkpoint_retention_seconds=1, checkpoint_count=1)
    )
    assert [c["checkpoint_id"] for c in plan["candidates"]] == [checkpoint]
    # 候选被引用保护后计划失效：零删除。
    job = str(uuid.uuid4())
    store.reference(env, project, checkpoint, "job", job)
    with pytest.raises(ServiceError, match="revision_conflict"):
        store.cleanup_confirm(env, project, plan["plan_id"], plan["plan_digest"], "conflict-1")
    assert _refs(store, env, f"refs/krim/{project}/") == sorted(
        [
            f"refs/krim/{project}/{checkpoint}",
            f"refs/krim/{project}/{other['checkpoint_id']}",
        ]
    )
    assert not _object_missing(store, env, commit)
    # 计划生成后新增保护根：任何变化即冲突。
    store.release_reference(env, project, checkpoint, "job", job)
    store.pin(env, project, other["checkpoint_id"], pinned=True)
    with pytest.raises(ServiceError, match="revision_conflict"):
        store.cleanup_confirm(env, project, plan["plan_id"], plan["plan_digest"], "conflict-2")
    assert not _object_missing(store, env, commit)
    # 候选引用被外部删除：零删除，需重新生成计划。
    store.pin(env, project, other["checkpoint_id"], pinned=False)
    repo = store.root / env / "objects.git"
    store._git(repo, ["update-ref", "-d", f"refs/krim/{project}/{checkpoint}"])
    with pytest.raises(ServiceError, match="revision_conflict"):
        store.cleanup_confirm(env, project, plan["plan_id"], plan["plan_digest"], "conflict-3")
    assert not _object_missing(store, env, commit)
    # 计划过期拒绝执行。
    plan = store.cleanup_preview(
        env, project, QuotaLimits(checkpoint_retention_seconds=1, checkpoint_count=1)
    )
    with pytest.MonkeyPatch.context() as patch:
        _age(patch, 901)
        with pytest.raises(ServiceError, match="approval_expired"):
            store.cleanup_confirm(env, project, plan["plan_id"], plan["plan_digest"], "conflict-4")
    # 候选超过64KiB的计划拒绝落盘。
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(checkpoint_module, "PLAN_MAX_BYTES", 64)
        with pytest.raises(ServiceError, match="artifact_limit"):
            store.cleanup_preview(
                env, project, QuotaLimits(checkpoint_retention_seconds=1, checkpoint_count=1)
            )


def test_gc_failure_saves_reclaim_pending_and_same_key_retry(checkpoints):
    store, env, project = checkpoints
    row = store.capture(env, project, {"a": b"pending"}, [], QuotaLimits())
    checkpoint, commit = row["checkpoint_id"], row["commit"]
    with pytest.MonkeyPatch.context() as patch:
        _age(patch, 10)
        plan = store.cleanup_preview(env, project, QuotaLimits(checkpoint_retention_seconds=1))
    assert [c["checkpoint_id"] for c in plan["candidates"]] == [checkpoint]
    original = store._reclaim
    attempts = []

    def flaky(repo):
        attempts.append(repo)
        if len(attempts) == 1:
            raise ServiceError("sandbox_backend_unavailable", 503)
        original(repo)

    store._reclaim = flaky
    try:
        with pytest.raises(ServiceError, match="sandbox_backend_unavailable"):
            store.cleanup_confirm(env, project, plan["plan_id"], plan["plan_digest"], "retry-key")
        # 引用已按事务删除，但对象尚未回收；回执记录 reclaim_pending。
        assert _refs(store, env, f"refs/krim/{project}/") == []
        assert not _object_missing(store, env, commit)
        receipt_dir = store.root / env / "maintenance" / "gc" / "receipts" / project
        assert any(file.name.endswith(".json") for file in receipt_dir.iterdir())
        # 已确认且引用已撤销的原操作，不因预览过期失去继续回收的能力。
        with pytest.MonkeyPatch.context() as patch:
            _age(patch, 3600)
            result = store.cleanup_confirm(
                env, project, plan["plan_id"], plan["plan_digest"], "retry-key"
            )
    finally:
        del store._reclaim
    assert result["status"] == "completed"
    assert result["removed"] == [checkpoint]
    assert _object_missing(store, env, commit)
    assert len(attempts) == 2


def test_active_holds_fill_count_budget_before_publishing_new_checkpoint(checkpoints):
    import uuid

    store, env, project = checkpoints
    limits = QuotaLimits(checkpoint_count=1)
    original = store.capture(env, project, {"a": b"protected"}, [], limits)
    store.reference(env, project, original["checkpoint_id"], "job", str(uuid.uuid4()))
    with pytest.raises(ServiceError, match="sandbox_limit"):
        store.capture(env, project, {"a": b"new"}, [], limits)
    rows = store.list(env, project)
    assert len(rows) == 1 and rows[0]["checkpoint_id"] == original["checkpoint_id"]


def test_capture_high_watermark_refuses_new_content_but_can_reuse_unchanged(
    checkpoints, monkeypatch
):
    store, env, project = checkpoints
    limits = QuotaLimits()
    first = store.capture(env, project, {"a": b"same"}, [], limits)
    monkeypatch.setattr(store, "_size", lambda repo: int(limits.checkpoint_bytes * 0.9))
    assert (
        store.capture(env, project, {"a": b"same"}, [], limits)["checkpoint_id"]
        == first["checkpoint_id"]
    )
    with pytest.raises(ServiceError, match="sandbox_limit"):
        store.capture(env, project, {"a": b"changed"}, [], limits)
