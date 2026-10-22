"""AC-M1独立合成夹具；仅导入本轮发行源码。"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import pytest
import pytest_asyncio
from fixtures.session_server import WireFixture


@pytest_asyncio.fixture
async def wire():
    value = await WireFixture().start()
    try:
        yield value
    finally:
        await value.close()


@pytest_asyncio.fixture
async def os_evidence_root():
    """仅返回D1证据根；不请求wire、不装配应用或启动资源。"""
    import os

    root = Path(os.environ["KRIM_D1_EVIDENCE"])
    assert root.is_absolute() and root.is_dir()
    yield root


@pytest.fixture(scope="session")
def legacy_schema3(tmp_path_factory):
    """独立进程只导入固定8963055归档，不复用当前模块缓存。"""
    import json
    import os
    import subprocess

    root = tmp_path_factory.mktemp("m1-real")
    database, receipt = root / "legacy.sqlite", root / "seed.json"
    argv = [
        sys.executable,
        "-I",
        "-B",
        str(Path.cwd() / "tests/agent_core/fixtures/workspace_crash_worker.py"),
        "legacy",
        str(database),
        str(receipt),
    ]
    env = {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "LD_LIBRARY_PATH": str(Path(sys.base_prefix) / "lib"),
        "TMPDIR": os.environ["TMPDIR"],
        "KRIM_D1_DATA_ROOT": os.environ["KRIM_D1_DATA_ROOT"],
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    process = subprocess.Popen(
        argv, cwd=Path.cwd(), env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    try:
        stdout, stderr = process.communicate(timeout=60)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
    (root / "stdout.txt").write_bytes(stdout)
    (root / "stderr.txt").write_bytes(stderr)
    (root / "command.json").write_text(
        json.dumps(
            {
                "argv": argv,
                "cwd": str(Path.cwd()),
                "env": env,
                "rc": process.returncode,
                "pid": process.pid,
                "waited": process.poll() is not None,
            }
        )
    )
    assert process.returncode == 0, stderr.decode()
    seed = json.loads(receipt.read_text())
    assert seed["version"] == 3
    assert "materials/product/m1-source/src" in seed["source"]
    return database, seed


@pytest_asyncio.fixture(autouse=True)
async def data_resource_receipt(request, monkeypatch):
    """只观测D1四节点；包装真实open，不替换Store/连接/Runner实现。"""
    if request.node.name.split("[")[0] not in {
        "test_schema4_upgrade_atomic_and_legacy_rows_readonly",
        "test_work_link_commit_unknown_and_restart_zero_replay",
        "test_snapshot_publication_and_bounded_reads",
        "test_nonregular_paths_capacity_and_concurrent_publish",
    }:
        yield
        return
    import asyncio
    import hashlib
    import json
    import os

    from test_session_read_sides import Connections

    from krim_agent.execution.linux import fds
    from krim_agent.storage.sqlite import SQLiteStore

    original = SQLiteStore.open
    probes = []
    before = fds()
    existing = asyncio.all_tasks()

    async def opened(cls, *args, **kwargs):
        store = await original(*args, **kwargs)
        probes.append((store, Connections(store)))
        return store

    monkeypatch.setattr(SQLiteStore, "open", classmethod(opened))
    yield
    unexpected = [
        t
        for t in asyncio.all_tasks()
        if t is not asyncio.current_task() and t not in existing and not t.done()
    ]
    result = {
        "node": request.node.nodeid,
        "stores": [
            {
                "closed": s.closed,
                "pending": sorted(p.pending),
                "unsafe_gc": p.unsafe,
                "events": p.events,
            }
            for s, p in probes
        ],
        "fds_before": before,
        "fds_after": fds(),
        "fds_equal": fds() == before,
        "tmp_path": str(request.node.funcargs.get("tmp_path")),
        "unexpected_tasks": [t.get_name() for t in unexpected],
    }
    folder = Path(os.environ["KRIM_D1_EVIDENCE"]) / "resources"
    folder.mkdir(exist_ok=True)
    (folder / (hashlib.sha256(request.node.nodeid.encode()).hexdigest() + ".json")).write_text(
        json.dumps(result, indent=2)
    )
    assert all(s.closed and not p.pending and p.unsafe == 0 for s, p in probes)
    assert result["fds_equal"]
    assert not unexpected
