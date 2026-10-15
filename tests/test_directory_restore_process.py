"""持久Store不同SQL/物理点真正SIGKILL，由另一进程只读恢复同原操作。"""

import asyncio
import json
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux SIGKILL")


@pytest.mark.parametrize(
    "point", ["baseline", "stage", "prepared", "intent", "namespace", "namespace_mutated"]
)
async def test_persistent_store_sigkill_same_material_operation_and_zero_replay(tmp_path, point):
    script = Path(__file__).parent / "fixtures/directory_restore_store_process.py"
    producer = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(script),
        "produce",
        point,
        str(tmp_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    recovery = None
    try:
        line = await asyncio.wait_for(producer.stdout.readline(), 20)
        if not line:
            _, errors = await producer.communicate()
            raise AssertionError(errors.decode())
        signal = json.loads(line)
        assert signal["pid"] == producer.pid and signal["point"] == point
        producer.kill()
        _, errors = await asyncio.wait_for(producer.communicate(), 5)
        assert producer.returncode == -9, errors.decode()
        if point == "namespace_mutated":
            (tmp_path / "files/tree/nested/nul").write_bytes(b"later\0mutation")
        metadata = json.loads((tmp_path / "metadata.json").read_text())
        recovery = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(script),
            "recover",
            point,
            str(tmp_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, errors = await asyncio.wait_for(recovery.communicate(), 20)
        assert recovery.returncode == 0, errors.decode()
        proof = json.loads(output)
        assert proof["pid"] != signal["pid"] and proof["replays"] == 0
        assert proof["operation_id"] == metadata["operation_id"]
        assert proof["plan_id"] == metadata["plan_id"] and proof["natural_lease_expiry"]
        assert proof["binary_hex"] == (
            b"later\0mutation".hex()
            if point == "namespace_mutated"
            else "00ff"
            if point == "namespace"
            else b"current\x80\0".hex()
        )
        print(json.dumps({"producer": signal, "recovery": proof}))
    finally:
        for process in (producer, recovery):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
