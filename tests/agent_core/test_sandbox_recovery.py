"""逐文件恢复：真实文件/Git和回执丢失窗口；不重放用户命令。"""

import json
import time
import uuid

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.sandbox.checkpoints import ShadowCheckpoints
from krim_agent.sandbox.filesystem import ProjectFiles
from krim_agent.sandbox.leases import ProjectLeases
from krim_agent.sandbox.quota import QuotaLimits
from krim_agent.sandbox.recovery import Recovery


class Broker:
    def __init__(self, root):
        self.environments = root / "environments"
        self.environments.mkdir()
        self.state = root / "state"
        self.state.mkdir()
        self.env, self.project = str(uuid.uuid4()), str(uuid.uuid4())
        self.files = ProjectFiles(self.environments)
        self.files.create(self.env, self.project)
        self.path = self.environments / self.env / "projects" / self.project / "content"
        (self.environments / self.env / "checkpoints").mkdir(mode=0o700)
        locks = root / "leases"
        locks.mkdir()
        self.leases = ProjectLeases(locks)
        self.quota = self
        self._save(
            "project-" + self.project + ".json",
            {
                "project_id": self.project,
                "environment_id": self.env,
                "generation": 0,
                "status": "ready",
            },
        )
        self.recovery = Recovery(self)

    def _environment(self, env):
        assert env == self.env
        return {"limits": QuotaLimits().model_dump(mode="json"), "project_number": 100}

    def _project(self, env, project):
        assert env == self.env and project == self.project
        return self._read("project-" + project + ".json")

    def _read(self, name):
        p = self.state / name
        return json.loads(p.read_text()) if p.exists() else None

    def _save(self, name, row):
        data = json.dumps(row)
        p = self.state / (name + ".tmp")
        p.write_text(data)
        p.replace(self.state / name)

    def usage(self, number):
        return {
            "environment_bytes": 4096,
            "byte_limit": 5 * 1024**3,
            "environment_inodes": 20,
            "inode_limit": 100000,
        }

    def checkpoint(self, files, excluded=None):
        store = ShadowCheckpoints(self.environments / self.env / "checkpoints")
        return store.capture(self.env, self.project, files, excluded or [], QuotaLimits())


@pytest.fixture
def setup(tmp_path):
    broker = Broker(tmp_path)
    try:
        yield broker, broker.recovery, broker.env, broker.project
    finally:
        broker.leases.close()
        broker.files.close()


def choose(plan):
    return [s["step_id"] for s in plan["changes"] if s["selected_by_default"]]


def confirm(r, e, p, plan, selected=None):
    return r.confirm(
        e,
        p,
        plan["plan_id"],
        plan["plan_digest"],
        str(uuid.uuid4()),
        plan["generation"],
        selected if selected is not None else choose(plan),
    )


def test_restore_preserves_later_files_and_backs_up_current_state(setup):
    b, r, e, p = setup
    ck = b.checkpoint({"a.txt": b"old", "src/new.txt": b"restored"})
    (b.path / "a.txt").write_bytes(b"current")
    (b.path / "later.txt").write_bytes(b"keep")
    plan = r.preview(e, p, ck["checkpoint_id"])
    assert plan["preserved"] == ["later.txt"]
    assert not next(s for s in plan["changes"] if s["path"] == "later.txt")["selected_by_default"]
    result = confirm(r, e, p, plan)
    while result["status"] == "paused":
        result = r.resume(e, p, result["operation_id"])
    assert result["status"] == "completed" and result["progress"]["verified"] == 2
    assert (b.path / "a.txt").read_bytes() == b"old"
    assert (b.path / "later.txt").read_bytes() == b"keep"
    assert (b.path / "src/new.txt").read_bytes() == b"restored"
    assert b._project(e, p)["generation"] == 1 and b.leases.status(p)["status"] == "idle"
    assert r.store(e).content(e, p, result["before_checkpoint"], "a.txt") == b"current"


def test_preview_drift_conflicts_before_any_file_is_touched(setup):
    b, r, e, p = setup
    ck = b.checkpoint({"a": b"old a", "b": b"old b"})
    (b.path / "a").write_bytes(b"new a")
    (b.path / "b").write_bytes(b"new b")
    plan = r.preview(e, p, ck["checkpoint_id"])
    (b.path / "b").write_bytes(b"external change")
    result = confirm(r, e, p, plan)
    assert result["status"] == "conflict" and (b.path / "a").read_bytes() == b"new a"
    assert b._project(e, p)["generation"] == 0 and b.leases.status(p)["status"] == "idle"


def test_file_durable_crash_reconciles_without_replacing_it_again(setup):
    b, r, e, p = setup
    ck = b.checkpoint({"a": b"old a", "b": b"old b"})
    (b.path / "a").write_bytes(b"new a")
    (b.path / "b").write_bytes(b"new b")
    plan = r.preview(e, p, ck["checkpoint_id"])

    def crash(phase, row):
        if phase == "file_durable":
            raise RuntimeError("lost receipt")

    r.point = crash
    result = confirm(r, e, p, plan)
    assert result["status"] == "paused"
    first = next(s for s in result["steps"] if s["status"] == "applying")["path"]
    inode = (b.path / first).stat().st_ino
    assert b.leases.status(p)["status"] == "blocked"
    resumed = Recovery(b).resume(e, p, result["operation_id"])
    assert resumed["status"] == "completed" and (b.path / first).stat().st_ino == inode
    assert (b.path / "a").read_bytes() == b"old a" and (b.path / "b").read_bytes() == b"old b"


def test_partial_conflict_can_be_abandoned_without_rollback(setup):
    b, r, e, p = setup
    ck = b.checkpoint({"a": b"old"})
    (b.path / "a").write_bytes(b"current")
    plan = r.preview(e, p, ck["checkpoint_id"])

    def crash(phase, row):
        if phase == "file_durable":
            raise RuntimeError("interrupted")

    r.point = crash
    partial = confirm(r, e, p, plan)
    (b.path / "a").write_bytes(b"latest edit")
    assert r.reconcile(e, p, partial["operation_id"])["conflicts"] == ["a"]
    abandoned = r.abandon(e, p, partial["operation_id"])
    assert abandoned["status"] == "abandoned_partial"
    assert (b.path / "a").read_bytes() == b"latest edit" and b.leases.status(p)["status"] == "idle"


def test_explicit_delete_is_selected_and_backed_up(setup):
    b, r, e, p = setup
    ck = b.checkpoint({"keep": b"old"})
    (b.path / "keep").write_bytes(b"old")
    (b.path / "extra").write_bytes(b"extra")
    plan = r.preview(e, p, ck["checkpoint_id"])
    selected = [s["step_id"] for s in plan["changes"] if s["action"] == "delete"]
    result = confirm(r, e, p, plan, selected)
    assert result["status"] == "completed" and not (b.path / "extra").exists()
    assert r.store(e).content(e, p, result["before_checkpoint"], "extra") == b"extra"


def test_repeated_confirm_never_executes_next_batch(setup):
    b, r, e, p = setup
    ck = b.checkpoint({f"{i:02d}": b"old" for i in range(10)})
    for i in range(10):
        (b.path / f"{i:02d}").write_bytes(b"current")
    plan = r.preview(e, p, ck["checkpoint_id"])
    op = str(uuid.uuid4())
    args = (e, p, plan["plan_id"], plan["plan_digest"], op, plan["generation"], choose(plan))
    first = r.confirm(*args)
    assert first["status"] == "paused" and first["progress"]["verified"] == 1
    assert r.confirm(*args)["progress"]["verified"] == 1
    while r.status(e, p, op)["status"] == "paused":
        r.resume(e, p, op)
    assert r.status(e, p, op)["status"] == "completed"


def test_space_and_plan_expiry_leave_original_file(setup, monkeypatch):
    b, r, e, p = setup
    ck = b.checkpoint({"a": b"old"})
    (b.path / "a").write_bytes(b"current")
    plan = r.preview(e, p, ck["checkpoint_id"])
    monkeypatch.setattr(
        b,
        "usage",
        lambda n: {
            "environment_bytes": 1000,
            "byte_limit": 1001,
            "environment_inodes": 20,
            "inode_limit": 100000,
        },
    )
    result = confirm(r, e, p, plan)
    assert result["status"] == "failed_preparation" and (b.path / "a").read_bytes() == b"current"
    now = time.time()
    monkeypatch.setattr(time, "time", lambda: now + 1000)
    with pytest.raises(ServiceError, match="revision_conflict"):
        confirm(r, e, p, plan)


def test_recorded_temporary_inode_is_cleaned_after_restart_without_touching_user_tmp(setup):
    b, r, e, p = setup
    ck = b.checkpoint({"a": b"old"})
    (b.path / "a").write_bytes(b"current")
    (b.path / "user.tmp").write_bytes(b"user")
    plan = r.preview(e, p, ck["checkpoint_id"])

    def interrupted(phase, row):
        if phase == "temporary_recorded":
            raise RuntimeError("stopped before temp content")

    r.point = interrupted
    partial = confirm(r, e, p, plan)
    assert partial["status"] == "paused"
    temporary = partial["steps"][0]["temporary"]
    # 一般异常清理了临时文件；模拟进程退出时留下相同日志inode。
    assert not (b.path / temporary["name"]).exists()
    assert Recovery(b).resume(e, p, partial["operation_id"])["status"] == "completed"
    assert (b.path / "user.tmp").read_bytes() == b"user"
    assert (b.path / "a").read_bytes() == b"old"


def test_delete_interrupted_after_unlink_is_not_deleted_twice(setup):
    b, r, e, p = setup
    ck = b.checkpoint({"keep": b"keep"})
    (b.path / "keep").write_bytes(b"keep")
    (b.path / "extra").write_bytes(b"extra")
    plan = r.preview(e, p, ck["checkpoint_id"])
    selected = [s["step_id"] for s in plan["changes"] if s["action"] == "delete"]

    def interrupted(phase, row):
        if phase == "file_durable":
            raise RuntimeError("unlink durable, receipt lost")

    r.point = interrupted
    partial = confirm(r, e, p, plan, selected)
    assert partial["status"] == "paused" and not (b.path / "extra").exists()
    r = Recovery(b)
    assert r.resume(e, p, partial["operation_id"])["status"] == "completed"
    assert (b.path / "keep").read_bytes() == b"keep"


def test_crash_after_backup_before_generation_bump_advances_before_resume(setup):
    b, r, e, p = setup
    ck = b.checkpoint({"a": b"old"})
    (b.path / "a").write_bytes(b"current")
    plan = r.preview(e, p, ck["checkpoint_id"])

    class ProcessCrash(BaseException):
        pass

    def crashed(phase, row):
        if phase == "before_saved":
            raise ProcessCrash()

    r.point = crashed
    with pytest.raises(ProcessCrash):
        confirm(r, e, p, plan)
    row = next(
        json.loads(f.read_text())
        for f in b.state.glob("restore-*.json")
        if not f.name.startswith("restore-plan-")
    )
    # 模拟旧进程的flock句柄由内核收回，active持久记录不改成idle。
    b.leases.close()
    assert b._project(e, p)["generation"] == 0
    b.leases = ProjectLeases(b.leases.root)
    resumed = Recovery(b).resume(e, p, row["operation_id"])
    assert resumed["status"] == "completed"
    assert b._project(e, p)["generation"] == 1
    assert (b.path / "a").read_bytes() == b"old"


def test_expired_completed_restore_protection_releases_only_its_own_root(setup, monkeypatch):
    from krim_agent.sandbox.maintenance import release_completed_restore_roots

    b, r, e, p = setup
    ck = b.checkpoint({"a": b"old"})
    (b.path / "a").write_bytes(b"new")
    plan = r.preview(e, p, ck["checkpoint_id"])
    completed = confirm(r, e, p, plan)
    before = completed["before_checkpoint"]
    r.store(e).pin(e, p, before, pinned=True)
    now = time.time()
    monkeypatch.setattr(time, "time", lambda: now + 90000)
    b.recovery = r
    release_completed_restore_roots(b)
    row = next(v for v in r.store(e).list(e, p) if v["checkpoint_id"] == before)
    assert row["pinned"]
    assert not any(
        v["kind"] == "restore-before" and v["owner"] == completed["operation_id"]
        for v in row["references"]
    )


def test_unknown_or_paused_restore_reference_is_not_expired_by_gc(setup, monkeypatch):
    from krim_agent.sandbox.maintenance import release_completed_restore_roots

    b, r, e, p = setup
    ck = b.checkpoint({"a": b"old"})
    (b.path / "a").write_bytes(b"new")
    plan = r.preview(e, p, ck["checkpoint_id"])

    def interrupted(phase, row):
        if phase == "file_durable":
            raise RuntimeError("paused")

    r.point = interrupted
    paused = confirm(r, e, p, plan)
    before = paused["before_checkpoint"]
    now = time.time()
    monkeypatch.setattr(time, "time", lambda: now + 900000)
    b.recovery = r
    release_completed_restore_roots(b)
    row = next(v for v in r.store(e).list(e, p) if v["checkpoint_id"] == before)
    assert any(v["kind"] == "restore-before" for v in row["references"])


def test_kernel_zero_limits_are_unlimited_and_restore_completes(setup, monkeypatch):
    """XFS 读回 byte_limit/inode_limit=0 表示无限：headroom 分量跳过，restore 正常完成。

    同时覆盖大文件链路：>4MiB 检查点内容经 content() 读回并 restore_write 落盘。
    """
    b, r, e, p = setup
    big = b"x" * (4 * 1024**2 + 1)
    ck = b.checkpoint({"a": b"old", "sub/b": b"old b", "big.bin": big})
    (b.path / "sub").mkdir()
    (b.path / "a").write_bytes(b"current")
    (b.path / "sub/b").write_bytes(b"cur b")
    (b.path / "big.bin").write_bytes(b"partial")
    monkeypatch.setattr(
        b,
        "usage",
        lambda n: {
            "environment_bytes": 10 * 1024**3,
            "byte_limit": 0,
            "environment_inodes": 100000,
            "inode_limit": 0,
        },
    )
    result = confirm(r, e, p, r.preview(e, p, ck["checkpoint_id"]))
    while result["status"] == "paused":
        result = r.resume(e, p, result["operation_id"])
    assert result["status"] == "completed"
    assert (b.path / "a").read_bytes() == b"old"
    assert (b.path / "sub/b").read_bytes() == b"old b"
    assert (b.path / "big.bin").read_bytes() == big


def test_zero_byte_limit_skips_bytes_but_positive_inode_headroom_is_enforced(
    setup, monkeypatch
):
    b, r, e, p = setup
    ck = b.checkpoint({"a": b"old"})
    (b.path / "a").write_bytes(b"current")
    monkeypatch.setattr(
        b,
        "usage",
        lambda n: {
            "environment_bytes": 1024**3,
            "byte_limit": 0,  # 字节分量无限：跳过
            "environment_inodes": 9999,
            "inode_limit": 10000,  # 有限额分量继续比较并拒绝
        },
    )
    result = confirm(r, e, p, r.preview(e, p, ck["checkpoint_id"]))
    assert result["status"] == "failed_preparation"
    assert (b.path / "a").read_bytes() == b"current"
    assert result["before_checkpoint"] is None


def test_record_limits_without_policy_stay_limited_in_restore(setup, monkeypatch):
    """旧记录缺 resource_policy 按 limited 解读：恢复前备份捕获仍执行有限协议修剪。

    unlimited 捕获面建立 21 个未保护历史；若 record 面被静默升级为 unlimited，
    备份捕获不会按 count=20 修剪。恢复完成后历史收敛到 20 项即证明 limited 生效。
    """
    b, r, e, p = setup
    base = b._environment(e)
    legacy = {k: v for k, v in base["limits"].items() if k != "resource_policy"}
    monkeypatch.setattr(b, "_environment", lambda env: {**base, "limits": legacy})
    ids = [b.checkpoint({f"f{i:02d}.txt": str(i).encode()})["checkpoint_id"] for i in range(21)]
    (b.path / "f20.txt").write_bytes(b"changed")
    plan = r.preview(e, p, ids[-1])
    result = confirm(r, e, p, plan)
    while result["status"] == "paused":
        result = r.resume(e, p, result["operation_id"])
    assert result["status"] == "completed"
    assert (b.path / "f20.txt").read_bytes() == b"20"
    listed = [row["checkpoint_id"] for row in r.store(e).list(e, p)]
    # limited 备份捕获后按 count=20 修剪（静默 unlimited 则保持 22 项）。
    # 同秒时间戳下并列由 UUID 决定排序，被删的两项不绑定具体 id。
    assert len(listed) == 20
    assert result["before_checkpoint"] in listed  # restore-before 引用保护
    assert ids[-1] in listed  # restore-target 引用保护


def test_limited_record_restore_completes_within_its_budget(setup, monkeypatch):
    b, r, e, p = setup
    base = b._environment(e)
    legacy = {k: v for k, v in base["limits"].items() if k != "resource_policy"}
    monkeypatch.setattr(b, "_environment", lambda env: {**base, "limits": legacy})
    ck = b.checkpoint({"a": b"old"})
    (b.path / "a").write_bytes(b"current")
    (b.path / "later").write_bytes(b"keep")
    result = confirm(r, e, p, r.preview(e, p, ck["checkpoint_id"]))
    while result["status"] == "paused":
        result = r.resume(e, p, result["operation_id"])
    assert result["status"] == "completed"
    assert (b.path / "a").read_bytes() == b"old" and (b.path / "later").read_bytes() == b"keep"
    assert r.store(e).content(e, p, result["before_checkpoint"], "a") == b"current"


def test_preview_and_ledger_carry_full_diff_beyond_256_files(setup):
    """>256 文件差异不设 preview/账本总量上限；preserved/excluded 完整返回。

    批次窗口（每批一步后 paused）保留：本例只验证账本完整落盘可续，
    全量恢复由既有小规模用例覆盖。
    """
    b, r, e, p = setup
    files = {f"d{i:03d}.txt": f"old{i}".encode() for i in range(257)}
    excluded = [{"path": f"cache/{i:03d}.bin", "reason": "pattern"} for i in range(200)]
    ck = b.checkpoint(files, excluded)
    for name, body in files.items():
        (b.path / name).write_bytes(body.replace(b"old", b"new"))
    (b.path / "extra.txt").write_bytes(b"extra")
    plan = r.preview(e, p, ck["checkpoint_id"])
    assert len(plan["changes"]) == 258  # 257 写 + extra.txt 删除
    assert plan["preserved"] == ["extra.txt"]
    # excluded 是当前快照现场（本例项目内无敏感项 → 空），target_excluded 是检查点目标。
    assert plan["excluded"] == [] and len(plan["target_excluded"]) == 200
    operation = str(uuid.uuid4())
    result = r.confirm(
        e,
        p,
        plan["plan_id"],
        plan["plan_digest"],
        operation,
        plan["generation"],
        [s["step_id"] for s in plan["changes"] if s["action"] == "write"],
    )
    # 大账本（>64KiB 旧上限）完整落盘并可按原操作读取续作；账本只含已选的 257 个写步骤。
    assert result["progress"]["total"] == 257
    assert result["status"] == "paused" and result["progress"]["verified"] >= 1
    resumed_view = r.status(e, p, operation)
    assert resumed_view["progress"]["total"] == 257
    assert len(resumed_view["steps"]) == 257
