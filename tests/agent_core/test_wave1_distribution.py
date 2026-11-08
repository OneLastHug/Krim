"""首轮模块必须进入发行白名单，optional依赖不能偷偷变为全局依赖。"""

import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_wave1_modules_are_explicitly_shipped():
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())
    shipped = set(project["tool"]["hatch"]["build"]["targets"]["wheel"]["only-include"])
    required = {
        "sandbox/ports.py", "sandbox/port_adapters.py", "sandbox/transfer_store.py",
        "sandbox/archive_io.py", "sandbox/edit_models.py", "sandbox/file_editing.py",
        "sandbox/runtime_profiles.py", "sandbox/quota_retirement.py",
        "sandbox/quota_retirement_adapter.py", "account_mcp/__init__.py",
        "account_mcp/contracts.py", "account_mcp/secrets.py", "network/__init__.py",
        "network/egress_policy.py", "network/http_transport.py",
    }
    assert {"src/krim_agent/" + name for name in required} <= shipped
    assert all((ROOT / "src/krim_agent" / name).is_file() for name in required)
    assert project["project"]["optional-dependencies"]["personal-mcp"] == [
        "mcp==1.30.0", "cryptography==50.0.2"
    ]
    assert not any(item.startswith(("mcp", "cryptography"))
                   for item in project["project"]["dependencies"])
