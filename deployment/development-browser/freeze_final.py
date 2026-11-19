"""Freeze one current source list and one independent evidence list; no copies."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from check_final import DEPLOY_FILES

IMAGE_ID = "sha256:c418264cf5fbbf1f578f7404d6e3c7d02edbf88f6505a29d2429e9aeb6e3060a"
PROFILE = "f922c7c9bfc1ece0b72b244e56dc85756ded002c837fb46054cfd8b275c74b6b"


def entries(paths: list[Path], root: Path) -> list[dict[str, object]]:
    result = []
    for path in sorted(set(paths)):
        if not path.is_file() or path.is_symlink():
            raise ValueError("final_file_missing_or_unsafe")
        result.append(
            {
                "path": path.relative_to(root).as_posix(),
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    return result


def main() -> None:
    here = Path(__file__).resolve().parent
    root = here.parents[1]
    sources = sorted((root / "src/krim_agent/dev_browser").glob("*.py"))
    tests = sorted((root / "tests/agent_core").glob("test_dev_browser_*.py"))
    files = entries([*sources, *tests, *(here / name for name in DEPLOY_FILES)], root)
    banned = (
        "evidence/",
        "installation-evidence",
        "__pycache__",
        ".patch",
        "APPROVAL",
        "verification.txt",
    )
    if any(any(word in str(item["path"]) for word in banned) for item in files):
        raise ValueError("historical_material_in_final_source_list")
    payload = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
    final = {
        "version": "F-runtime-final-v1",
        "base": "ea11d6462c7272f1c175d8cecb97f69ff93ffe67",
        "source_set_sha256": hashlib.sha256(payload).hexdigest(),
        "files": files,
        "installed_image_id": IMAGE_ID,
        "seccomp_sha256": PROFILE,
        "wheel_additions": [p.relative_to(root).as_posix() for p in sources],
        "requires_I_changes": ["wheel whitelist", "existing host controller/composition"],
        "host_imports": [
            "krim_agent.dev_browser.host_bridge.PreviewConnectBridge",
            "krim_agent.dev_browser.launcher.WorkerSpec",
        ],
        "no_shared_files_changed": True,
    }
    (here / "F-final-manifest.json").write_text(json.dumps(final, indent=2) + "\n")
    evidence = list((here / "final-evidence").glob("**/*"))
    evidence = [p for p in evidence if p.is_file()]
    evidence.extend(
        here / "chroot-acceptance-evidence" / name
        for name in (
            "sandbox-06/result.json",
            "sandbox-06/probe-stdout.txt",
            "fixed-component.png",
            "network-01/result.json",
            "network-01/probe-stdout.txt",
            "ttl-partial-01/result.json",
            "ttl-partial-01/probe-stdout.txt",
            "ttl-idle-01/result.json",
            "ttl-idle-01/probe-stdout.txt",
            "cleanup-verified.json",
        )
    )
    progress = root / "docs/coding-agent/F-progress.md"
    evidence.append(progress)
    report = {
        "version": "F-runtime-final-evidence-v1",
        "files": entries(evidence, root),
        "source_set_sha256": final["source_set_sha256"],
        "scope": "Chromium sandbox and root UDS passed; TLS/viewer integration pending",
        "progress_is_git_ignored": True,
    }
    (here / "F-final-evidence-manifest.json").write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                "source_files": len(files),
                "evidence_files": len(report["files"]),
                "source_set_sha256": final["source_set_sha256"],
            }
        )
    )


if __name__ == "__main__":
    main()
