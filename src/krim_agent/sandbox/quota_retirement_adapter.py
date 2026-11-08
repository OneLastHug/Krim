"""BrokerRetirementPort：RetirementPort协议对现有StorageBroker的生产适配。

主会话接线：broker.quota_retirements = QuotaRetirement(BrokerRetirementPort(broker))；
C=0且已登记checkpoint_number的quota_preview转retirements.preview并标retirement=true，
控制面按保存的标记调用quota_retirement_confirm/status/cancel。本模块只实现端口，
不挂路由、不改broker与现有quota/GC共享文件。

适配要点（与quota_management/quota_retirement同一口径）：
- 全部持久化经Broker既有记录面（broker._read/_save与env记录、accounts.json），
  不直接写SQLite、不绕过维护锁；journal键只接受本协议的plan/operation形状
  （严格UUID），拒绝任意路径。
- 维护门：操作记录首次落盘（confirm）即绑定 env.quota_operation=operation_id
  （quota_state=retiring）；命令准入（jobs/recovery/provision_project）与例行GC
  由此停止。apply_data_limit成功读回后才放门（quota_state=retired）；
  cancel只清除本操作持有的门。变更动作入口都重绑门并拒绝他人持门。
- 隔离证明基于证据而非字段：旧project id内核读回0 inode且0字节，检查点区内
  无任何引用、对象内容、未确认GC意图或reclaim_pending回执。隔离动作把账户目录
  （含仓库骨架与维护记录）整体归档进有界管理区并重标记到data project，再移除
  空壳checkpoints目录；失败保留现场，不做自动补偿。
- 并发边界：quarantine与release全程持检查点维护flock（与ShadowCheckpoints/
  QuotaChanges同一文件）；broker的dispatch_lock不覆盖checkpoint清理（独立线程、
  checkpoint_gate并发2），已在途GC只能由这把锁排除。inspect的写者快照形状
  不符按故障拒绝，不把异常或缺字段当作空闲。
- 预留释放幂等：同一operation_id只注销一次checkpoint_number（env记录
  checkpoint_released_by），其他操作重复释放拒绝；释放前重核隔离与0 inode。
  kernel_zero先重核，再以bhard=0/ihard=0清除旧限额——XFS该值为unlimited，
  读回0不当作0硬限额。
- inspect的refs/objects/gc来自检查点区实况枚举；checkpoint_zone以内核读回与
  内容枚举共同判定，无法就绪时精确报错，不伪造blocked。
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import stat
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

from krim_agent.contracts import ServiceError
from krim_agent.sandbox.checkpoints import OWNER_KINDS, ShadowCheckpoints
from krim_agent.sandbox.quota import QuotaLimits
from krim_agent.sandbox.quota_management import RESERVATION_CEILING
from krim_agent.sandbox.quota_retirement import RetirementPort
from krim_agent.sandbox.xfs import internal_id, private_directory

if TYPE_CHECKING:
    from krim_agent.sandbox.broker import StorageBroker

ARCHIVE_DIR = "maintenance-archives"
ARCHIVE_PREFIX = "checkpoint-retirement-"
_JOURNAL_KEY = re.compile(
    r"quota-retire-(plan-)?[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z"
)
_ARCHIVE_BYTES = 4 * 1024**2
_ARCHIVE_ENTRIES = 2000
_ARCHIVE_DIRS = 64
_REF_LIMIT = 2000
_RECEIPT_LIMIT = 512
_ENTRY_LIMIT = 20000
_ALLOWED_ACCOUNT = {"objects.git", "maintenance", "maintenance.lock"}


def _exists(path: Path) -> bool:
    """仅缺失返回False；不可读（权限/IO/类型异常）按故障fail-closed，不当作缺失。"""
    try:
        os.lstat(path)
    except FileNotFoundError:
        return False
    except OSError:
        raise ServiceError("sandbox_backend_unavailable", 503) from None
    return True


class BrokerRetirementPort(RetirementPort):
    """把QuotaRetirement的存储意图落到现有Broker状态面；不引入第二套持久化。"""

    def __init__(self, broker: StorageBroker) -> None:
        self.broker = broker

    # ---------- 只读快照 ----------

    @staticmethod
    def _zone_number(record: dict[str, Any]) -> int | None:
        """检查点区project id：在役用checkpoint_number，退役后用checkpoint_retired_number。"""
        registered = record.get("checkpoint_number")
        if registered is not None:
            return int(registered)
        retired = record.get("checkpoint_retired_number")
        return None if retired is None else int(retired)

    def inspect(self, environment_id: str) -> dict[str, Any]:
        environment_id = internal_id(environment_id)
        record = self._environment_record(environment_id)
        number = int(record["project_number"])
        zone_number = self._zone_number(record)
        data = self.broker.quota.usage(number)
        zone_usage = None if zone_number is None else self.broker.quota.usage(zone_number)
        refs, objects, gc = self._checkpoint_content(environment_id)
        busy = self.broker.quota_changes._active_writers(environment_id)
        if busy is not None and not (
            isinstance(busy.get("jobs"), list)
            and isinstance(busy.get("recovery"), list)
            and isinstance(busy.get("leases"), list)
            and isinstance(busy.get("maintenance_lock"), bool)
        ):
            # 写者快照形状不符按故障处理：缺字段或异常都不得当作空闲。
            raise ServiceError("facts_corrupt")
        lock_held = self._maintenance_lock_held(environment_id)
        active: dict[str, Any] = (
            dict(busy)
            if busy is not None
            else {"jobs": [], "recovery": [], "leases": []}
        )
        active["maintenance_lock"] = lock_held
        return {
            "environment_id": environment_id,
            "record": dict(record),
            "usage": self._usage_view(data, zone_usage),
            "refs": refs,
            "objects": objects,
            "gc": gc,
            "active": active,
            "stopped_proof": {
                "idle": busy is None and not lock_held,
                "checked_at": time.time(),
                "busy": busy,
            },
            "checkpoint_zone": self._zone_state(
                environment_id, zone_number, zone_usage
            ),
            "actual": self._actual_view(data, zone_usage),
            "reservation": self._reservation_view(environment_id, record),
        }

    def verify(self, environment_id: str) -> dict[str, Any]:
        environment_id = internal_id(environment_id)
        record = self._environment_record(environment_id)
        number = int(record["project_number"])
        zone_number = self._zone_number(record)
        data = self.broker.quota.usage(number)
        zone_usage = None if zone_number is None else self.broker.quota.usage(zone_number)
        return {
            "record": dict(record),
            "actual": self._actual_view(data, zone_usage),
            "checkpoint_zone": self._zone_state(
                environment_id, zone_number, zone_usage
            ),
            "reservation": self._reservation_view(environment_id, record),
        }

    # ---------- journal ----------

    def journal_get(self, key: str) -> dict[str, Any] | None:
        return self.broker._read(self._journal_name(key))

    def journal_put(self, key: str, record: dict[str, Any]) -> None:
        name = self._journal_name(key)
        if not isinstance(record, dict):
            raise ServiceError("invalid_input", 422)
        self.broker._save(name, record)
        if not key.startswith("quota-retire-plan-"):
            self._sync_gate(record)

    @staticmethod
    def _journal_name(key: str) -> str:
        if type(key) is not str or _JOURNAL_KEY.fullmatch(key) is None:
            raise ServiceError("invalid_input", 422)
        return key + ".json"

    # ---------- 隔离 ----------

    def quarantine_empty_checkpoint_root(
        self, environment_id: str, operation_id: str
    ) -> dict[str, Any]:
        environment_id = internal_id(environment_id)
        operation_id = internal_id(operation_id)
        self._bind_gate(environment_id, operation_id)
        record = self._environment_record(environment_id)
        self._assert_gate(operation_id, record)
        registered = record.get("checkpoint_number")
        if registered is None:
            raise ServiceError("invalid_input", 422)
        checkpoint_number = int(registered)
        root = self.broker.environments / environment_id / "checkpoints"
        account = root / environment_id
        archived: str | None = None
        if _exists(account):
            # 全程持检查点维护flock（与ShadowCheckpoints/QuotaChanges同一文件）：
            # broker RPC的dispatch_lock不覆盖checkpoint清理（独立线程并发），
            # 已在途GC只能靠这把锁排除，不能只靠quota_operation门。
            with self.broker.quota_changes._maintenance(environment_id):
                self._assert_relinquishable(environment_id, root, account)
                archived = self._archive_account(environment_id, operation_id, account)
                self._reclaim_project_flag(environment_id, root)
                usage = self.broker.quota.usage(checkpoint_number)
        else:
            # 账户目录缺失即无锁文件，不存在持锁在途GC；重标记与移壳仍须完成并读回。
            self._reclaim_project_flag(environment_id, root)
            usage = self.broker.quota.usage(checkpoint_number)
        if usage["environment_inodes"] != 0 or usage["environment_bytes"] != 0:
            # 归档与移壳之后旧project id仍占用：存在未知残留，如实失败供核实。
            raise ServiceError("artifact_conflict", 409)
        return {
            "quarantined": True,
            "inode_count": 0,
            "managed_path": archived,
            "environment_id": environment_id,
            "operation_id": operation_id,
        }

    def _assert_relinquishable(
        self, environment_id: str, root: Path, account: Path
    ) -> None:
        """持锁重核：零引用、零对象内容、零未确认GC；未知顶层文件不得归档。"""
        refs, objects, gc = self._checkpoint_content(environment_id)
        if (
            refs["live_checkpoints"]
            or refs["protected"]
            or refs["legacy_retained"]
            or objects["physical_bytes"]
            or gc["intents"]
            or gc["pending"]
        ):
            raise ServiceError("artifact_conflict", 409)
        with os.scandir(account) as entries:
            unknown = [entry.name for entry in entries if entry.name not in _ALLOWED_ACCOUNT]
        if unknown:
            raise ServiceError("artifact_conflict", 409)

    def _archive_account(
        self, environment_id: str, operation_id: str, account: Path
    ) -> str:
        """把账户目录整体移入有界管理区（rename保留inode；project id随后统一重标记）。"""
        total = 0
        count = 0
        for directory, folders, files in os.walk(account):
            count += len(folders) + len(files)
            if count > _ARCHIVE_ENTRIES:
                raise ServiceError("artifact_limit")
            for name in files:
                total += os.lstat(Path(directory) / name).st_size
        if total > _ARCHIVE_BYTES:
            raise ServiceError("artifact_limit")
        parent = self.broker.environments / environment_id / ARCHIVE_DIR
        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        private_directory(parent)
        target = parent / (ARCHIVE_PREFIX + operation_id)
        if _exists(target):
            # 同操作号已有归档而账户仍在原位：状态异常，不覆盖已有归档。
            raise ServiceError("key_conflict", 409)
        os.rename(account, target)
        return target.relative_to(self.broker.environments).as_posix()

    def _reclaim_project_flag(self, environment_id: str, root: Path) -> None:
        """归档树重标记到data project，再移除空壳checkpoints目录（最后一批旧flag inode）。"""
        parent = self.broker.environments / environment_id / ARCHIVE_DIR
        if _exists(parent):
            number = int(self._environment_record(environment_id)["project_number"])
            archives = sorted(os.listdir(parent))
            if len(archives) > _ARCHIVE_DIRS:
                raise ServiceError("artifact_limit")
            for name in archives:
                if name.startswith(ARCHIVE_PREFIX):
                    self.broker.quota.command(f"project -s -p {parent / name} {number}")
        if _exists(root):
            with os.scandir(root) as entries:
                leftover = next(iter(entries), None)
            if leftover is not None:
                raise ServiceError("artifact_conflict", 409)
            os.rmdir(root)

    # ---------- 预算释放与限额 ----------

    def release_quota_allocation(
        self, environment_id: str, operation_id: str, *, kernel_zero: bool
    ) -> dict[str, Any]:
        environment_id = internal_id(environment_id)
        operation_id = internal_id(operation_id)
        self._bind_gate(environment_id, operation_id)
        record = self._environment_record(environment_id)
        self._assert_gate(operation_id, record)
        registered = record.get("checkpoint_number")
        if registered is None:
            released_by = record.get("checkpoint_released_by")
            if released_by == operation_id:
                # 回包丢失续作：本操作已提交释放，按实际状态幂等返回，不再重放。
                return {"checkpoint_released": True, "kernel_zero": kernel_zero}
            if released_by is None:
                raise ServiceError("invalid_input", 422)
            raise ServiceError("key_conflict", 409)
        checkpoint_number = int(registered)
        account = (
            self.broker.environments / environment_id / "checkpoints" / environment_id
        )
        if _exists(account / "maintenance.lock"):
            # 释放同样持检查点维护flock：内容证据核查与kernel_zero不得与在途GC交错。
            with self._existing_lock(account):
                return self._release_locked(
                    environment_id, operation_id, record, checkpoint_number, kernel_zero
                )
        return self._release_locked(
            environment_id, operation_id, record, checkpoint_number, kernel_zero
        )

    def _release_locked(
        self,
        environment_id: str,
        operation_id: str,
        record: dict[str, Any],
        checkpoint_number: int,
        kernel_zero: bool,
    ) -> dict[str, Any]:
        # 首次释放：锁内重核隔离证据（kernel_zero与keep模式同样要求0 inode）。
        zone_usage = self.broker.quota.usage(checkpoint_number)
        zone = self._zone_state(environment_id, checkpoint_number, zone_usage)
        if not zone["quarantined"] or zone["inode_count"] != 0:
            raise ServiceError("artifact_conflict", 409)
        if kernel_zero:
            # XFS limit 0=unlimited：仅在隔离且0 inode核实后清除旧限额；读回0非硬限额。
            self.broker.quota.command(f"limit -p bhard=0 ihard=0 {checkpoint_number}")
            cleared = self.broker.quota.usage(checkpoint_number)
            if cleared["byte_limit"] != 0 or cleared["inode_limit"] != 0:
                raise ServiceError("sandbox_backend_unavailable", 503)
            if cleared["environment_inodes"] != 0:
                raise ServiceError("artifact_conflict", 409)
        updated = {
            key: value for key, value in record.items() if key != "checkpoint_number"
        }
        # 登记注销但保留退役区号：verify据此继续读回旧project id的0 inode证据。
        updated["checkpoint_retired_number"] = checkpoint_number
        updated["checkpoint_released_by"] = operation_id
        self.broker._save("env-" + environment_id + ".json", updated)
        return {
            "checkpoint_released": True,
            "kernel_zero": kernel_zero,
            "checkpoint_number": checkpoint_number,
        }

    @contextmanager
    def _existing_lock(self, account: Path) -> Iterator[None]:
        """仅对已存在的维护锁文件持锁（非阻塞）；缺失即无在途GC，不创建新树。"""
        fd = os.open(account / "maintenance.lock", os.O_RDWR | os.O_NOFOLLOW)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ServiceError("facts_corrupt")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ServiceError("workspace_busy") from None
            yield
        finally:
            os.close(fd)

    def apply_data_limit(
        self, environment_id: str, operation_id: str, byte_limit: int, inode_limit: int
    ) -> dict[str, Any]:
        environment_id = internal_id(environment_id)
        operation_id = internal_id(operation_id)
        if (
            type(byte_limit) is not int
            or type(inode_limit) is not int
            or byte_limit <= 0
            or inode_limit <= 0
            or byte_limit % 1024 != 0  # 内核KiB粒度：非对齐输入静默截断即误差，直接拒绝
        ):
            raise ServiceError("invalid_input", 422)
        self._bind_gate(environment_id, operation_id)
        record = self._environment_record(environment_id)
        self._assert_gate(operation_id, record)
        if record.get("checkpoint_number") is not None:
            # 步骤顺序由协议保证：预算未释放不得越过release直接改data限额。
            raise ServiceError("artifact_conflict", 409)
        if record.get("checkpoint_released_by") != operation_id:
            raise ServiceError("key_conflict", 409)
        limits = record.get("limits")
        if not isinstance(limits, dict):
            raise ServiceError("facts_corrupt")
        number = int(record["project_number"])
        desired = {
            **limits,
            "environment_bytes": byte_limit,
            "environment_inodes": inode_limit,
            "checkpoint_bytes": 0,
            "checkpoint_count": 0,
        }
        try:
            applied = QuotaLimits.model_validate(desired)
        except ValueError:
            raise ServiceError("invalid_input", 422) from None
        # 先完成节点预留账本，再写内核并读回，最后才落盘放门：
        # _reserve失败时门保持绑定，内核未动；内核先于账本失败则预留超前（保守方向），
        # 两种中断都在门内可核实并按同操作幂等续作。
        self.broker.quota_changes._reserve(
            environment_id, applied.model_dump(mode="json"), larger_only=False
        )
        self.broker.quota.command(
            f"limit -p bhard={byte_limit // 1024}k ihard={inode_limit} {number}"
        )
        usage = self.broker.quota.usage(number)
        if usage["byte_limit"] != byte_limit or usage["inode_limit"] != inode_limit:
            raise ServiceError("sandbox_backend_unavailable", 503)
        updated = {
            key: value
            for key, value in record.items()
            if key not in {"quota_operation", "checkpoint_bytes"}
        }
        updated["quota_state"] = "retired"
        updated["limits"] = applied.model_dump(mode="json")
        updated["data_bytes"] = usage["byte_limit"]
        self.broker._save("env-" + environment_id + ".json", updated)
        return {"byte_limit": usage["byte_limit"], "inode_limit": usage["inode_limit"]}

    # ---------- 维护门 ----------

    def _bind_gate(self, environment_id: str, operation_id: str) -> None:
        record = self._environment_record(environment_id)
        held = record.get("quota_operation")
        if held == operation_id:
            return
        if held is not None:
            raise ServiceError("workspace_busy")
        if record.get("quota_state") == "retired":
            return  # 已完结退役：终态读回或重试路径不再绑门
        self.broker._save(
            "env-" + environment_id + ".json",
            {**record, "quota_operation": operation_id, "quota_state": "retiring"},
        )

    def _sync_gate(self, record: dict[str, Any]) -> None:
        environment_id = record.get("environment_id")
        operation_id = record.get("operation_id")
        state = record.get("retirement_state")
        if (
            type(environment_id) is not str
            or type(operation_id) is not str
            or type(state) is not str
        ):
            raise ServiceError("facts_corrupt")
        current = self._environment_record(environment_id)
        held = current.get("quota_operation")
        if state == "cancelled":
            if held == operation_id:
                self.broker._save(
                    "env-" + environment_id + ".json",
                    {
                        key: value
                        for key, value in current.items()
                        if key not in {"quota_operation", "quota_state"}
                    },
                )
            return
        if state == "retired" or held not in {None, operation_id}:
            return
        if held == operation_id or current.get("quota_state") == "retired":
            return  # 已持门，或apply_data_limit已放门（quota_state=retired）不重绑
        self.broker._save(
            "env-" + environment_id + ".json",
            {**current, "quota_operation": operation_id, "quota_state": "retiring"},
        )

    def _assert_gate(self, operation_id: str, record: dict[str, Any]) -> None:
        if record.get("quota_operation") not in {None, operation_id}:
            raise ServiceError("workspace_busy")

    def _maintenance_lock_held(self, environment_id: str) -> bool:
        lock = (
            self.broker.environments
            / environment_id
            / "checkpoints"
            / environment_id
            / "maintenance.lock"
        )
        try:
            fd = os.open(lock, os.O_RDWR | os.O_NOFOLLOW)
        except FileNotFoundError:
            return False
        except OSError:
            return True  # 无法核实视为持锁：没有停写证明就不开始
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                return True
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(fd, fcntl.LOCK_UN)
            return False
        finally:
            os.close(fd)

    # ---------- 状态视图 ----------

    def _environment_record(self, environment_id: str) -> dict[str, Any]:
        record = self.broker._read("env-" + environment_id + ".json")
        if record is None or record.get("status") != "ready":
            raise ServiceError("sandbox_backend_unavailable", 503)
        return record

    @staticmethod
    def _actual_view(
        data: dict[str, int], checkpoint: dict[str, int] | None
    ) -> dict[str, Any]:
        return {
            "data": {"byte_limit": data["byte_limit"], "inode_limit": data["inode_limit"]},
            "checkpoint": None
            if checkpoint is None
            else {
                "byte_limit": checkpoint["byte_limit"],
                "inode_limit": checkpoint["inode_limit"],
            },
        }

    @staticmethod
    def _usage_view(
        data: dict[str, int], checkpoint: dict[str, int] | None
    ) -> dict[str, Any]:
        def used(row: dict[str, int]) -> dict[str, int]:
            return {
                "environment_bytes": row["environment_bytes"],
                "environment_inodes": row["environment_inodes"],
            }

        account = dict(data)
        if checkpoint is not None:
            account = {key: account[key] + checkpoint[key] for key in account}
        return {
            "data": used(data),
            "checkpoint": None if checkpoint is None else used(checkpoint),
            "account": used(account),
            "measured_at": time.time(),
        }

    def _reservation_view(
        self, environment_id: str, record: dict[str, Any]
    ) -> dict[str, Any]:
        registry = self.broker._read("accounts.json") or {"allocations": {}}
        allocations = registry.get("allocations", {})
        reserved = 0
        mine = 0
        for key, allocation in allocations.items():
            limits = allocation.get("limits", {}) if isinstance(allocation, dict) else {}
            value = int(limits.get("environment_bytes") or 0)
            reserved += value
            if key == environment_id:
                mine = value
        info = os.statvfs(self.broker.storage)
        ceiling = int(info.f_blocks * info.f_frsize * RESERVATION_CEILING)
        limits = record.get("limits")
        budget = (
            int(limits.get("checkpoint_bytes") or 0)
            if isinstance(limits, dict) and record.get("checkpoint_number") is not None
            else 0
        )
        return {
            "ceiling_bytes": ceiling,
            "reserved_bytes": reserved,
            "mine_bytes": mine,
            "checkpoint_budget_bytes": budget,
            "checkpoint_released": record.get("checkpoint_number") is None,
            "released_by_operation": record.get("checkpoint_released_by"),
        }

    def _zone_state(
        self,
        environment_id: str,
        checkpoint_number: int | None,
        zone_usage: dict[str, int] | None,
    ) -> dict[str, Any]:
        """隔离判定取自证据：内核0 inode/0字节且区内无引用、对象内容与未确认GC。

        路径是否存在不构成证据：目录重建（如checkpoint_list经_account重建骨架）
        不带旧flag也无内容，不破坏0 inode判定；带flag或有内容则内核读回如实变非零。
        """
        if checkpoint_number is None:
            return {"quarantined": False, "inode_count": 0, "managed_path": None}
        used_inodes = int(zone_usage["environment_inodes"]) if zone_usage else 0
        used_bytes = int(zone_usage["environment_bytes"]) if zone_usage else 0
        if used_inodes == 0 and used_bytes == 0:
            refs, objects, gc = self._checkpoint_content(environment_id)
            empty = not (
                refs["live_checkpoints"]
                or refs["protected"]
                or refs["legacy_retained"]
                or objects["physical_bytes"]
                or gc["intents"]
                or gc["pending"]
            )
        else:
            empty = False
        return {
            "quarantined": empty,
            "inode_count": used_inodes,
            "managed_path": self._managed_archive(environment_id),
        }

    def _managed_archive(self, environment_id: str) -> str | None:
        parent = self.broker.environments / environment_id / ARCHIVE_DIR
        if not _exists(parent):
            return None
        names = sorted(name for name in os.listdir(parent) if name.startswith(ARCHIVE_PREFIX))
        if not names:
            return None
        return (Path(ARCHIVE_DIR) / names[0]).as_posix()

    # ---------- 检查点内容枚举 ----------

    def _store(self, environment_id: str) -> ShadowCheckpoints:
        return ShadowCheckpoints(self.broker.environments / environment_id / "checkpoints")

    def _checkpoint_content(
        self, environment_id: str
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        """枚举检查点区实况：引用分类、对象字节与未确认GC；区缺失时全部为空。"""
        refs: dict[str, Any] = {
            "protected": [],
            "legacy_retained": 0,
            "hold_kinds": {},
            "live_checkpoints": 0,
        }
        objects: dict[str, Any] = {"physical_bytes": 0}
        gc: dict[str, Any] = {"pending": [], "intents": 0}
        account = (
            self.broker.environments / environment_id / "checkpoints" / environment_id
        )
        if not _exists(account):
            return refs, objects, gc
        repo = account / "objects.git"
        if _exists(repo):
            store = self._store(environment_id)
            holds, retained, live = self._enumerate_refs(store, repo)
            legacy = [checkpoint for _, checkpoint in retained]
            refs["protected"] = sorted(
                {checkpoint for _, checkpoint, _ in holds} | set(legacy)
            )
            refs["legacy_retained"] = len(retained)
            refs["live_checkpoints"] = live
            kinds: dict[str, int] = {}
            for _, _checkpoint, kind in holds:
                kinds[kind] = kinds.get(kind, 0) + 1
            refs["hold_kinds"] = kinds
            objects["physical_bytes"] = self._object_bytes(repo)
        records = account / "maintenance" / "gc"
        gc["intents"] = self._count_intents(records)
        gc["pending"] = self._pending_receipts(records)
        return refs, objects, gc

    def _enumerate_refs(
        self, store: ShadowCheckpoints, repo: Path
    ) -> tuple[list[tuple[str, str, str]], list[tuple[str, str]], int]:
        """全库引用枚举并严格分类；未知形状拒绝，不做宽松归档。"""
        listing = store._git(repo, ["for-each-ref", "--format=%(refname)"]).decode().splitlines()
        if len(listing) > _REF_LIMIT:
            raise ServiceError("artifact_limit")
        holds: list[tuple[str, str, str]] = []
        retained: list[tuple[str, str]] = []
        live = 0
        for refname in listing:
            parts = refname.split("/")
            if len(parts) == 6 and parts[1] == "holds":
                _, _, project, checkpoint, kind, owner = parts
                try:
                    internal_id(project)
                    internal_id(checkpoint)
                except ServiceError:
                    raise ServiceError("facts_corrupt") from None
                if kind not in OWNER_KINDS or not owner:
                    raise ServiceError("facts_corrupt")
                if kind != "user":
                    try:
                        if str(uuid.UUID(owner)) != owner:
                            raise ValueError("owner不是规范UUID")
                    except (ValueError, AttributeError):
                        raise ServiceError("facts_corrupt") from None
                holds.append((project, checkpoint, kind))
            elif len(parts) == 4 and parts[1] == "retained":
                try:
                    internal_id(parts[2])
                    internal_id(parts[3])
                except ServiceError:
                    raise ServiceError("facts_corrupt") from None
                retained.append((parts[2], parts[3]))
            elif len(parts) == 4 and parts[1] == "krim":
                try:
                    internal_id(parts[2])
                    internal_id(parts[3])
                except ServiceError:
                    raise ServiceError("facts_corrupt") from None
                live += 1
            else:
                raise ServiceError("facts_corrupt")
        return holds, retained, live

    def _object_bytes(self, repo: Path) -> int:
        """对象库内容字节（objects子树、不含info索引与结构骨架）；索引非内容。"""
        store = repo / "objects"
        total = 0
        count = 0
        for directory, folders, files in os.walk(store):
            folders[:] = [name for name in folders if name != "info"]
            count += len(folders) + len(files)
            if count > _ENTRY_LIMIT:
                raise ServiceError("artifact_limit")
            for name in files:
                total += os.lstat(Path(directory) / name).st_blocks * 512
        return total

    def _count_intents(self, records: Path) -> int:
        intents = records / "intents"
        if not _exists(intents):
            return 0
        count = 0
        for _, _, files in os.walk(intents):
            count += len(files)
            if count > _RECEIPT_LIMIT:
                raise ServiceError("artifact_limit")
        return count

    def _pending_receipts(self, records: Path) -> list[str]:
        """回执扫描：status=reclaim_pending的plan_id；扫描有界，超出即拒绝。"""
        receipts = records / "receipts"
        if not _exists(receipts):
            return []
        reader = _ReceiptReader(records)
        pending: list[str] = []
        count = 0
        for _, _, files in os.walk(receipts):
            count += len(files)
            if count > _RECEIPT_LIMIT:
                raise ServiceError("artifact_limit")
        for directory, _, files in os.walk(receipts):
            for name in files:
                record = reader._read_record(Path(directory) / name)
                if record.get("status") == "reclaim_pending":
                    plan = record.get("plan_id")
                    if type(plan) is str:
                        pending.append(plan)
        return sorted(pending)


class _ReceiptReader:
    """有界JSON维护记录读取（与ShadowCheckpoints._read_record同规则，不持锁）。"""

    def __init__(self, root: Path) -> None:
        self.root = root

    def _read_record(self, path: Path) -> dict[str, Any]:
        try:
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        except FileNotFoundError:
            return {}
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 65536:
                raise ServiceError("artifact_invalid")
            data = os.read(fd, info.st_size + 1)
        finally:
            os.close(fd)
        if len(data) > 65536:
            raise ServiceError("artifact_limit")
        try:
            record = json.loads(data)
        except ValueError:
            raise ServiceError("facts_corrupt") from None
        if type(record) is not dict:
            raise ServiceError("facts_corrupt")
        return record
