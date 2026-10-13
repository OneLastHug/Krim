"""首次封存commit后SIGKILL，另一进程读原材料/身份/来源，0重观察。"""

import asyncio
import json
import shutil
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="真实Linux隔离和SIGKILL",
)


async def test_sealed_original_tree_sigkill_other_process_same_material_and_zero_replay(tmp_path):
    (tmp_path / "files").mkdir()
    fixtures = Path(__file__).parent / "fixtures"
    produced = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(fixtures / "completed_tree_seal_produce.py"),
        str(tmp_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    recovered = None
    try:
        line = await asyncio.wait_for(produced.stdout.readline(), 25)
        if not line:
            _, error = await produced.communicate()
            raise AssertionError(error.decode())
        metadata = json.loads(line)
        assert metadata["pid"] == produced.pid
        produced.kill()
        _, error = await asyncio.wait_for(produced.communicate(), 5)
        assert produced.returncode == -9, error.decode()
        url = f"sqlite+aiosqlite:///{tmp_path / 'state.sqlite'}"
        recovered = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(fixtures / "completed_tree_seal_cold.py"),
            url,
            str(tmp_path / "files"),
            metadata["scope"],
            metadata["plan_id"],
            metadata["resource_fingerprint"],
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, error = await asyncio.wait_for(recovered.communicate(), 20)
        assert recovered.returncode == 0, error.decode()
        proof = json.loads(output)
        assert (
            proof["fingerprint"] == metadata["fingerprint"]
            and proof["proof_hash"] == metadata["proof_hash"]
        )
        assert (
            proof["inode"] == metadata["inode"]
            and proof["operation_ids"] == metadata["operation_ids"]
        )
        assert proof["replay_calls"] == 0 and proof["bytes_hex"] == "00ff"
        print(json.dumps({"produced": metadata, "recovered": proof}))
    finally:
        for process in (produced, recovered):
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
