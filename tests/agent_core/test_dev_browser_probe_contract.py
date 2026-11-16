"""No Docker execution: fixed probe collection and command scope checks."""

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).parents[2]
DEPLOY = ROOT / "deployment/development-browser"


def test_probe_does_not_import_playwright_until_executed(tmp_path):
    spec = importlib.util.spec_from_file_location("f_fixed_probe", DEPLOY / "sandbox_probe.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    fake = tmp_path / "status"
    fake.write_text("Name:\trenderer\nSeccomp:\t2\nSeccomp_filters:\t3\nSecret:\tprivate\n")
    assert module.selected_status(fake) == {
        "Name": "renderer",
        "Seccomp": "2",
        "Seccomp_filters": "3",
    }
    assert "playwright.async_api" not in sys.modules


def test_no_network_probe_keeps_approved_isolation_and_cleans_exact_id():
    source = (DEPLOY / "verify_container.py").read_text()
    for flag in (
        "--network=none",
        "--runtime=runc",
        "--cap-drop=ALL",
        "--read-only",
        "--user=1000:1000",
        "--memory=1073741824",
        "--cpus=2",
        "--pull=never",
    ):
        assert flag in source
    assert '["rm", "--force", container_id]' in source
    assert 'record["Image"] == args.image' in source
    assert 'record["Name"] == "/" + name' in source
    probe = (DEPLOY / "sandbox_probe.py").read_text()
    assert "chromium_sandbox=True" in probe
    assert "chrome://sandbox" in probe
    assert "Seccomp_filters" in probe
    assert "browser_processes_after_close" in probe
    assert "ignore_https_errors=False" in probe
    assert "https://" not in probe
