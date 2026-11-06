"""源码阶段检查；安装包逐字核对由 installed_demo 的独立-I验收补齐。"""

import subprocess
import sys
from pathlib import Path


def test_cold_imports_are_pure_and_use_no_old_modules(tmp_path):
    root = Path(__file__).resolve().parents[2]
    code = (
        "import sys;sys.path.insert(0," + repr(str(root / "src")) + ");"
        "import httpx;httpx.AsyncClient.__init__=lambda *a,**k:"
        "(_ for _ in ()).throw(AssertionError('cold client'));"
        "import socket;socket.socket.connect=lambda *a,**k:"
        "(_ for _ in ()).throw(AssertionError('cold network'));"
        "import krim_agent,krim_agent.providers,krim_agent.runtime,krim_agent.tools;"
        "import krim_agent.sdk,krim_agent.api;import krim_agent.providers.openai_compatible;"
        "assert not any(any(k==p or k.startswith(p+'.') for p in ["
        "'krim_agent.persistence','krim_agent.agents','krim_agent.protocol',"
        "'krim_agent.evaluation.recovery','krim_agent.evaluation.judge',"
        "'krim_agent.evaluation.conversation']"
        ") for k in sys.modules)"
    )
    before = list(tmp_path.iterdir())
    result = subprocess.run(
        [sys.executable, "-I", "-B", "-c", code], cwd=tmp_path, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    assert list(tmp_path.iterdir()) == before


def test_old_entrypoint_bytes_unchanged():
    root = Path(__file__).resolve().parents[2]
    before = subprocess.check_output(
        [
            "git",
            "--no-optional-locks",
            "-C",
            str(root),
            "show",
            "289af1e40b962187d36d206326f08c02496de61b:src/krim_agent/__main__.py",
        ]
    )
    assert (root / "src/krim_agent/__main__.py").read_bytes() == before
