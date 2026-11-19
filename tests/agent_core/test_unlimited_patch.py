"""A补丁无上限：PatchRequest/PatchPlan越过旧内容上限，Accepted读回>4MiB分页完整。

模型层用最小参数构造验证上限移除与保留的结构保护；链路层复用真实
ProjectFiles + ProjectLeases，只替换broker桩，数据量保持在数MiB量级。
"""

from __future__ import annotations

import hashlib
import time
import uuid
from typing import Any

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.sandbox.edit_models import PatchPlan, PatchRequest
from krim_agent.sandbox.file_editing import ProjectFileEditor
from krim_agent.sandbox.filesystem import ProjectFiles
from krim_agent.sandbox.leases import ProjectLeases
from krim_agent.sandbox.port_adapters import AcceptedProjectMutation
from krim_agent.sandbox.ports import ProjectRef

GENERATION = 3
LINE = "字" * 100 + "\n"  # 301字节UTF-8，天然码点对齐
PAGE = 65536  # AcceptedProjectMutation单页窗口


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class _Broker:
    """最小broker桩：_project行静态返回generation，files为真实ProjectFiles。"""

    def __init__(self, files: ProjectFiles) -> None:
        self.files: Any = files
        self.row = {"generation": GENERATION}

    def _project(self, environment_id: str, project_id: str) -> dict[str, Any]:
        return self.row


class _RecordingFiles:
    """包装真实ProjectFiles.read并记录分页参数；其余属性透传。"""

    def __init__(self, inner: ProjectFiles) -> None:
        self._inner = inner
        self.calls: list[tuple[int, int]] = []

    def read(
        self, environment_id: str, project_id: str, path: str, offset: int = 0, limit: int = PAGE
    ) -> dict[str, Any]:
        self.calls.append((offset, limit))
        return self._inner.read(environment_id, project_id, path, offset=offset, limit=limit)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


@pytest.fixture
def workspace(tmp_path):
    root = tmp_path / "accounts"
    root.mkdir(mode=0o700)
    files = ProjectFiles(root)
    env, selected = str(uuid.uuid4()), str(uuid.uuid4())
    files.create(env, selected)
    locks = tmp_path / "leases"
    locks.mkdir()
    leases = ProjectLeases(locks)
    lease = leases.claim(selected, str(uuid.uuid4()), deadline=time.time() + 120)
    ref = ProjectRef(env, env, selected, GENERATION)
    broker = _Broker(files)
    port = AcceptedProjectMutation(broker, ref, lease)
    try:
        yield files, broker, port, ref, lease
    finally:
        lease.release(clean=True)
        leases.close()
        files.close()


def test_patch_request_accepts_parameters_beyond_old_caps() -> None:
    """offset/delete越过4MiB、replacement越过16KiB均为合法参数。"""
    request = PatchRequest(
        path="big.md",
        expected_sha256="0" * 64,
        expected_generation=GENERATION,
        offset=4 * 1024**2 + 7,
        delete_length=4 * 1024**2 + 9,
        replacement="字" * 6000,
    )
    assert request.offset == 4 * 1024**2 + 7
    assert request.delete_length == 4 * 1024**2 + 9
    assert len(request.replacement.encode("utf-8")) == 18000


def test_patch_request_keeps_structural_guards() -> None:
    """负offset、负delete_length、无效UTF-8、越界路径与坏摘要仍被拒绝。"""
    base = {
        "path": "big.md",
        "expected_sha256": "0" * 64,
        "expected_generation": 0,
        "offset": 0,
        "delete_length": 0,
        "replacement": "",
    }
    for field in ("offset", "delete_length"):
        with pytest.raises(ValueError):
            PatchRequest(**{**base, field: -1})
    with pytest.raises(ValueError):
        PatchRequest(**{**base, "replacement": "\udce9"})
    with pytest.raises(ServiceError):
        PatchRequest(**{**base, "path": "../escape.md"})
    with pytest.raises(ValueError):
        PatchRequest(**{**base, "expected_sha256": "z" * 64})


def test_patch_plan_accepts_large_replacement_bytes() -> None:
    """PatchPlan的replacement_bytes不再受16KiB上限，非UTF-8字节仍拒绝。"""
    fields = {
        "plan_id": "a" * 32,
        "principal_id": "p1",
        "environment_id": "e1",
        "project_id": "pr1",
        "generation": 0,
        "path": "big.md",
        "source_sha256": "0" * 64,
        "result_sha256": "1" * 64,
        "offset": 0,
        "delete_length": 0,
    }
    plan = PatchPlan(**fields, replacement_bytes=("字" * 6000).encode())
    assert len(plan.replacement_bytes) == 18000
    with pytest.raises(ValueError):
        PatchPlan(**fields, replacement_bytes=b"\xff\xfe\x00")


def test_accepted_read_returns_whole_file_beyond_four_mib(workspace) -> None:
    """AcceptedProjectMutation.read对超过原4MiB上限的文件按页完整读回，缺失仍为None。"""
    files, broker, port, ref, lease = workspace
    big = (LINE * 15000).encode()  # 4,515,000字节 > 4MiB
    assert len(big) > 4 * 1024**2
    files.write(ref.environment_id, ref.project_id, "big.md", big, None)
    assert port.read(ref, "big.md") == big
    assert port.read(ref, "missing.md") is None


def test_accepted_read_keeps_single_page_window(workspace) -> None:
    """读回保持65536字节单页窗口，按页推进至EOF。"""
    files, broker, port, ref, lease = workspace
    payload = bytes(range(256)) * 800  # 204800字节 → 4页
    files.write(ref.environment_id, ref.project_id, "paged.bin", payload, None)
    recorder = _RecordingFiles(files)
    broker.files = recorder
    assert port.read(ref, "paged.bin") == payload
    assert [offset for offset, _ in recorder.calls] == [0, PAGE, 2 * PAGE, 3 * PAGE]
    assert all(limit == PAGE for _, limit in recorder.calls)


def test_patch_beyond_old_caps_lands_through_accepted_port(workspace) -> None:
    """补丁同时越过旧offset/delete/replacement上限并经真实端口落地。"""
    files, broker, port, ref, lease = workspace
    big = (LINE * 15000).encode()
    files.write(ref.environment_id, ref.project_id, "big.md", big, None)
    editor = ProjectFileEditor(port)
    splice = 301 * 14000  # 行边界offset，越过原4MiB上限
    assert splice > 4 * 1024**2
    replacement = "新" * 6000 + "\n"  # 18001字节 > 16KiB
    plan = editor.plan_patch(
        ref,
        expected_generation=GENERATION,
        path="big.md",
        expected_sha256=digest(big),
        offset=splice,
        delete_length=301 * 10,
        replacement=replacement,
    )
    editor.apply_patch(plan, port_operation_id=lease.lease_id)
    after = big[:splice] + replacement.encode() + big[splice + 3010:]
    assert port.read(ref, "big.md") == after
    delete_length = splice + len(replacement.encode()) + 301 * 100  # after的行边界，越过4MiB
    assert delete_length > 4 * 1024**2
    plan = editor.plan_patch(
        ref,
        expected_generation=GENERATION,
        path="big.md",
        expected_sha256=digest(after),
        offset=0,
        delete_length=delete_length,
        replacement="",
    )
    editor.apply_patch(plan, port_operation_id=lease.lease_id)
    assert port.read(ref, "big.md") == after[delete_length:]


def test_accepted_read_conflict_guards_preserved(workspace) -> None:
    """页间hash不一致、空未结束页、EOF摘要不符、generation中途变更仍被拒绝。"""
    files, broker, port, ref, lease = workspace
    payload = bytes(range(256)) * 300  # 76800字节 → 2页
    files.write(ref.environment_id, ref.project_id, "two.bin", payload, None)
    whole_digest = digest(payload)

    class _ShaFlip(_RecordingFiles):
        def read(self, environment_id, project_id, path, offset=0, limit=PAGE):
            chunk = super().read(environment_id, project_id, path, offset=offset, limit=limit)
            if len(self.calls) > 1:
                chunk["sha256"] = "0" * 64  # 第二页起hash与首页不一致
            return chunk

    broker.files = _ShaFlip(files)
    with pytest.raises(ServiceError, match="artifact_conflict"):
        port.read(ref, "two.bin")

    class _EmptyPage(_RecordingFiles):
        def read(self, environment_id, project_id, path, offset=0, limit=PAGE):
            if self.calls:
                return {
                    "path": path,
                    "offset": offset,
                    "size": len(payload),
                    "eof": False,
                    "sha256": whole_digest,
                    "stat_token": [0] * 5,
                    "data": "",
                }
            return super().read(environment_id, project_id, path, offset=offset, limit=limit)

    broker.files = _EmptyPage(files)
    with pytest.raises(ServiceError, match="artifact_conflict"):
        port.read(ref, "two.bin")

    class _WrongDigest(_RecordingFiles):
        def read(self, environment_id, project_id, path, offset=0, limit=PAGE):
            chunk = super().read(environment_id, project_id, path, offset=offset, limit=limit)
            chunk["sha256"] = digest(payload[:10])  # 页间一致但与完整内容不符
            return chunk

    broker.files = _WrongDigest(files)
    with pytest.raises(ServiceError, match="artifact_conflict"):
        port.read(ref, "two.bin")

    class _GenerationBump(_RecordingFiles):
        def __init__(self, inner: ProjectFiles, row: dict[str, Any]) -> None:
            super().__init__(inner)
            self._row = row

        def read(self, environment_id, project_id, path, offset=0, limit=PAGE):
            chunk = super().read(environment_id, project_id, path, offset=offset, limit=limit)
            if chunk["eof"] is False:
                self._row["generation"] += 1  # 首页后并发变更generation
            return chunk

    broker.files = _GenerationBump(files, broker.row)
    with pytest.raises(ServiceError, match="input_revision_conflict"):
        port.read(ref, "two.bin")
