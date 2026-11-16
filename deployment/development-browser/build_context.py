"""Prepare approved data and trusted source for a completely offline build."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

MODULES = (
    "__init__.py",
    "contracts.py",
    "browser/__init__.py",
    "browser/protocol.py",
    "browser/egress.py",
    "browser/manager.py",
    "network/__init__.py",
    "network/egress_policy.py",
    "dev_browser/__init__.py",
    "dev_browser/policy.py",
    "dev_browser/session.py",
    "dev_browser/context.py",
    "dev_browser/downloads.py",
    "dev_browser/fixed_proxy.py",
    "dev_browser/host_bridge.py",
    "dev_browser/launcher.py",
)
WORKER_FILES = ("worker.py", "stdio_channel.py", "entrypoint.py")


def native_path(path: Path) -> None:
    if (
        not path.is_absolute()
        or path.is_relative_to(Path("/mnt"))
        or any(part == ".." for part in path.parts)
        or any(p.is_symlink() for p in (path, *path.parents))
    ):
        raise ValueError("build_requires_native_nonsymlink_path")


def copy_application(app: Path) -> None:
    """Copy exactly the reviewed import closure; never synthesize shared modules."""
    here = Path(__file__).resolve().parent
    source = here.parents[1] / "src/krim_agent"
    app.mkdir(mode=0o755, parents=True, exist_ok=False)
    for name in WORKER_FILES:
        shutil.copyfile(here / name, app / name)
    for name in MODULES:
        path = source / name
        if path.is_symlink() or not path.is_file():
            raise ValueError("source_missing")
        out = app / "krim_agent" / name
        out.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
        shutil.copyfile(path, out)
    # A trusted build may run with umask 077; copied code must still be readable
    # by the image's UID 1000, without writable source or mutable imports.
    for path in (app, *app.rglob("*")):
        path.chmod(0o755 if path.is_dir() else 0o644)


def import_smoke(app: Path) -> None:
    """Isolated subprocess cannot fall back to the repository's full src tree."""
    script = (
        "import sys,pathlib; sys.dont_write_bytecode=True;"
        "root=pathlib.Path(sys.argv[1]);sys.path.insert(0,str(root));"
        "import worker,entrypoint;from krim_agent.dev_browser import BrowserSession;"
        "from krim_agent.browser.protocol import validate_png;"
        "assert all(pathlib.Path(m.__file__).resolve().is_relative_to(root.resolve()) "
        "for n,m in sys.modules.items() if n.startswith('krim_agent') and hasattr(m,'__file__'))"
    )
    result = subprocess.run(
        [sys.executable, "-I", "-c", script, str(app)],
        cwd=app.parent,
        capture_output=True,
        text=True,
        check=False,
        timeout=15,
    )
    if result.returncode:
        raise ValueError("worker_import_closure_failed")


def create_context(destination: Path, downloads: Path) -> None:
    here = Path(__file__).resolve().parent
    native_path(destination)
    native_path(downloads)
    if destination.exists():
        raise ValueError("build_context_must_be_new")
    specs = json.loads((here / "worker-downloads.json").read_text())
    verified = []
    for item in specs:
        parent = {"wheel": "wheelhouse", "deb": "debs", "profile": "profile"}[item["kind"]]
        path = downloads / parent / item["sha256"] / item["filename"]
        native_path(path)
        if not path.is_file() or path.stat().st_size != item["bytes"]:
            raise ValueError("download_invalid")
        if hashlib.sha256(path.read_bytes()).hexdigest() != item["sha256"]:
            raise ValueError("download_invalid")
        verified.append((item, parent, path))
    destination.mkdir(mode=0o700, parents=True)
    for name in ("Dockerfile", "worker-requirements.txt"):
        shutil.copyfile(here / name, destination / name)
    app = destination / "app"
    copy_application(app)
    import_smoke(app)
    for parent in ("wheelhouse", "debs"):
        (destination / parent).mkdir(mode=0o700)
    for item, parent, path in verified:
        if item["kind"] == "profile":
            continue
        shutil.copyfile(path, destination / parent / item["filename"])
    manifest = {
        str(p.relative_to(destination)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(destination.rglob("*"))
        if p.is_file()
    }
    (destination / "context-sha256.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--downloads", type=Path, required=True)
    args = parser.parse_args()
    create_context(args.destination, args.downloads)
