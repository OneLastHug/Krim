"""Run E tests against an explicit read-only C0 source tree without copying DTOs."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--contracts-root",
        type=Path,
        required=True,
        help="I-provided source root containing krim_agent/development",
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(root / "src"))
    import krim_agent

    shared = args.contracts_root.resolve() / "krim_agent"
    paths = [
        shared / "development" / name
        for name in (
            "__init__.py",
            "contracts.py",
            "ports.py",
            "preview_ports.py",
        )
    ]
    before = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    # Import only the authoritative shared package. Do not expose the rest of
    # I's source tree as an accidental fallback for E or other implementation.
    package_path = shared / "development"
    spec = importlib.util.spec_from_file_location(
        "krim_agent.development",
        package_path / "__init__.py",
        submodule_search_locations=[str(package_path)],
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("C0 source package unavailable")
    package = importlib.util.module_from_spec(spec)
    sys.modules["krim_agent.development"] = package
    spec.loader.exec_module(package)
    krim_agent.development = package
    print("Read-only C0:", before, flush=True)
    import pytest

    result = pytest.main(
        ["-q", *map(str, sorted((root / "tests" / "agent_core").glob("test_dev_preview_*.py")))]
    )
    after = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    if before != after:
        print("C0 source changed during tests; result is not a pinned-contract verification.")
        return 2
    return result


if __name__ == "__main__":
    raise SystemExit(main())
