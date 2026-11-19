"""Packaged host components must not depend on deployment paths or Playwright."""

import asyncio
import os
import subprocess
import sys
from pathlib import Path

import pytest

from krim_agent.dev_browser import host_bridge
from krim_agent.dev_browser.fixed_proxy import ProxyError
from krim_agent.dev_browser.host_bridge import PreviewConnectBridge


def test_host_imports_with_only_installed_style_src_path(tmp_path):
    src = Path(__file__).parents[2] / "src"
    code = (
        "import sys;sys.path.insert(0,sys.argv[1]);"
        "from krim_agent.dev_browser.host_bridge import PreviewConnectBridge;"
        "from krim_agent.dev_browser.launcher import WorkerSpec;"
        "assert 'worker' not in sys.modules;"
        "assert not any(n.startswith('playwright') for n in sys.modules);"
        "assert not any('deployment' in p for p in sys.path)"
    )
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, str(src)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
async def test_nonroot_bridge_rejects_foreign_peer_identity_before_creating_socket(
    tmp_path, monkeypatch
):
    if os.getuid() == 0:
        pytest.skip("this branch specifically requires an unprivileged process")
    monkeypatch.setattr(host_bridge, "RUN_ROOT", tmp_path)
    tmp_path.chmod(0o700)
    bridge = PreviewConnectBridge(
        str(tmp_path / "new.sock"),
        "p-fixture.preview.example",
        asyncio.sleep,
        expected_uid=os.getuid() + 1,
        expected_gid=os.getgid(),
    )
    with pytest.raises(ProxyError, match="bridge_owner_mismatch"):
        async with bridge:
            pytest.fail("unexpected bridge")
    assert not bridge.socket_path.exists()
