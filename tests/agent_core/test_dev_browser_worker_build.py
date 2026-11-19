"""Offline source closure and launch envelope tests; never build/run Docker."""

import hashlib
import json
import socket
import sys
from pathlib import Path

import pytest

from krim_agent.dev_browser import launcher
from krim_agent.dev_browser.launcher import WorkerSpec

DEPLOY = Path(__file__).parents[2] / "deployment/development-browser"
sys.path.insert(0, str(DEPLOY))
from build_context import MODULES, copy_application, import_smoke, native_path  # noqa: E402


def test_copied_application_imports_without_full_repository_fallback(tmp_path):
    app = tmp_path / "app"
    copy_application(app)
    import_smoke(app)
    source = DEPLOY.parents[1] / "src/krim_agent"
    for name in MODULES:
        assert (app / "krim_agent" / name).read_bytes() == (source / name).read_bytes()
    (app / "krim_agent/dev_browser/policy.py").unlink()
    with pytest.raises(ValueError, match="worker_import_closure_failed"):
        import_smoke(app)


def test_native_path_applies_to_downloads_and_build_context(tmp_path):
    for path in (Path("relative"), Path("/mnt/f/downloads"), tmp_path / ".." / "other"):
        with pytest.raises(ValueError, match="native_nonsymlink"):
            native_path(path)
    link = tmp_path / "link"
    link.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ValueError):
        native_path(link / "below")
    native_path(tmp_path / "new")


def test_downloads_and_requirements_match_exact_versions_hashes_and_sizes():
    downloads = json.loads((DEPLOY / "worker-downloads.json").read_text())
    wheels = [x for x in downloads if x["kind"] == "wheel"]
    assert len(wheels) == 8 and len(downloads) == 11
    assert sum(x["bytes"] for x in wheels) == 50701500
    requirement_lines = set((DEPLOY / "worker-requirements.txt").read_text().splitlines())
    for item in wheels:
        assert (
            item["name"] + "==" + item["version"] + " --hash=sha256:" + item["sha256"]
        ) in requirement_lines
        assert item["url"].startswith("https://files.pythonhosted.org/")
        assert item["url"].endswith("/" + item["filename"])
        assert len(item["sha256"]) == 64


def test_launcher_pins_runtime_egress_and_only_mounts_socket_and_ca(tmp_path, monkeypatch):
    # Constructor validation is tested independently; local test root replaces
    # only the deployment's fixed /run prefix, never the generated flags.
    path = tmp_path / "bridge.sock"
    sock = socket.socket(socket.AF_UNIX)
    sock.bind(str(path))
    ca = tmp_path / "ca.pem"
    ca.write_bytes(b"public test certificate bytes")
    seccomp = tmp_path / "seccomp.json"
    seccomp.write_bytes(b"fixed test profile")
    monkeypatch.setattr(launcher, "RUN_ROOT", tmp_path)
    monkeypatch.setattr(
        launcher, "SECCOMP_SHA256", hashlib.sha256(seccomp.read_bytes()).hexdigest()
    )
    spec = WorkerSpec(
        image_id="sha256:" + "a" * 64,
        name="krim-browser-" + "b" * 32,
        origin="https://p-fixed.preview.example",
        control_origin="https://control.example:8443",
        socket_path=path,
        ca_path=ca,
        ca_sha256=hashlib.sha256(ca.read_bytes()).hexdigest(),
        seccomp_path=seccomp,
    )
    try:
        args = spec.create_args()
        for item in (
            "--runtime=runc",
            "--network=none",
            "--pull=never",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--read-only",
            "--user=1000:1000",
            "--memory=1073741824",
            "--memory-swap=1073741824",
            "--ipc=private",
        ):
            assert item in args
        mounts = [x for x in args if x.startswith("--mount=")]
        assert len(mounts) == 2 and all(",readonly," in x for x in mounts)
        assert not any("docker.sock" in x or "/workspace" in x for x in mounts)
        assert args[-1] == spec.image_id
        ca.write_bytes(b"changed certificate")
        with pytest.raises(ValueError, match="worker_ca_invalid"):
            spec.create_args()
    finally:
        sock.close()
