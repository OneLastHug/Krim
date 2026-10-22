"""FINAL精确声明与有限安装入口；不把材料或ImportError算作行为RED。"""

import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

from krim_agent import __version__
from krim_agent.api.server import create_app

ROOT = Path(__file__).resolve().parents[2]


def test_release_04_and_exact_75() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())
    grant = json.loads(Path(os.environ["KRIM_M2_FINAL_GRANT"]).read_bytes())
    assert project["project"]["version"] == __version__ == create_app().version == "0.4.0a1"
    declared = project["tool"]["hatch"]["build"]["targets"]["wheel"]["only-include"]
    assert declared == grant["package_set"] and len(declared) == 75
    assert sum(name.endswith(".py") for name in declared) == 71
    assert all((ROOT / name).is_file() for name in declared)


def test_installed_driver_cli_rejects_mixed_or_unknown(tmp_path: Path) -> None:
    driver = ROOT / "tests/agent_core/fixtures/installed_workspace.py"
    vectors = [
        ["--database", str(tmp_path / "must-not-exist.sqlite")],
        ["--evidence", str(tmp_path), "--port", "8876", "--database", str(tmp_path / "x"),
         "--wheel", str(tmp_path / "x.whl")],
        ["--module", "krim_agent"],
    ]
    before = set(tmp_path.iterdir())
    for args in vectors:
        result = subprocess.run([sys.executable, "-I", "-B", str(driver), *args],
            cwd=tmp_path, capture_output=True, text=True)
        assert result.returncode == 2 and "usage:" in result.stderr
    assert set(tmp_path.iterdir()) == before


def test_serve_without_installation_identity_opens_no_resources(tmp_path: Path) -> None:
    driver = ROOT / "tests/agent_core/fixtures/installed_workspace.py"
    database = tmp_path / "must-not-exist.sqlite"
    evidence_before = sorted(p.name for p in tmp_path.iterdir())
    result = subprocess.run([sys.executable, "-I", "-B", str(driver), "--database",
        str(database), "--port", "8876", "--evidence", str(tmp_path)],
        cwd=tmp_path, capture_output=True, text=True)
    assert result.returncode != 0 and "installation_identity" in result.stderr
    assert not database.exists()
    assert sorted(p.name for p in tmp_path.iterdir()) == evidence_before
    assert hashlib.sha256(driver.read_bytes()).hexdigest()


def test_incorrect_installation_identity_rejected_before_resources(tmp_path: Path) -> None:
    """纯metadata边界；不冒称从已安装解释器启动的正向服务证明。"""
    path = ROOT / "tests/agent_core/fixtures/installed_workspace.py"
    spec = importlib.util.spec_from_file_location("final_identity_unit", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    grant = json.loads(Path(os.environ["KRIM_M2_FINAL_GRANT"]).read_bytes())
    receipt = tmp_path / "wrong-identity.json"
    receipt.write_text('{"kind":"wrong"}')
    isolated_grant = grant | {"installation_identity_receipt": str(receipt)}
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    with pytest.raises(ValueError, match="installation_identity"):
        module.installed_identity(Path(grant["acceptance_root"]), isolated_grant)
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == before


def _consumer_module() -> object:
    """只载入driver的标准库定义，不执行main或产品资源。"""
    path = ROOT / "tests/agent_core/fixtures/installed_workspace.py"
    spec = importlib.util.spec_from_file_location("recovery_consumer_metadata", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _identity_tree(folder: Path) -> dict[str, str]:
    import stat

    values: dict[str, str] = {}
    stack = [folder]
    while stack:
        for path in stack.pop().iterdir():
            info = path.lstat()
            name = str(path.relative_to(folder))
            if stat.S_ISDIR(info.st_mode):
                values[name] = "directory"
                stack.append(path)
            else:
                assert stat.S_ISREG(info.st_mode)
                values[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return values


def _identity_evidence(name: str, value: dict[str, object]) -> None:
    folder = Path(os.environ["KRIM_AGENT_CORE_EVIDENCE"]) / "identity-cases"
    folder.mkdir(exist_ok=True)
    with (folder / (name + ".json")).open("x") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)


@pytest.mark.parametrize("vector", [
    "mixed-mode", "unknown-option", "duplicate-option", "old-install-mode",
])
def test_consumer_cli_modes_reject_without_resources(tmp_path: Path, vector: str) -> None:
    """真实IP -I子进程的CLI拒绝；不触发旧安装门禁。"""
    import signal
    import time

    h = Path(os.environ["KRIM_M2_FINAL_ROOT"])
    database = tmp_path / "must-not-exist.sqlite"
    args = {
        "mixed-mode": ["--qualify-consumer", "--database", str(database),
                       "--evidence", str(tmp_path)],
        "unknown-option": ["--module", "krim_agent"],
        "duplicate-option": ["--qualify-consumer", "--evidence", str(tmp_path),
                             "--evidence", str(tmp_path)],
        "old-install-mode": ["--evidence", str(tmp_path), "--materials", str(tmp_path),
                             "--root", str(h), "--wheel", str(tmp_path / "unused.whl"),
                             "--contract", os.environ["KRIM_M2_FINAL_GRANT"]],
    }[vector]
    env = {name: os.environ[name] for name in (
        "PATH", "LANG", "LD_LIBRARY_PATH", "PYTHONDONTWRITEBYTECODE",
        "KRIM_M2_FINAL_ROOT", "KRIM_M2_FINAL_GRANT", "HOME", "TMPDIR"
    )}
    argv = [str(h / "installed-env/bin/python"), "-I", "-B",
            str(ROOT / "tests/agent_core/fixtures/installed_workspace.py"), *args]
    before, fd_before = _identity_tree(tmp_path), sorted(os.listdir("/proc/self/fd"))
    started, forced = time.time(), False
    with subprocess.Popen(argv, cwd=tmp_path, env=env, stdin=subprocess.DEVNULL,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
        pidfd = os.pidfd_open(process.pid)
        try:
            try:
                stdout, stderr = process.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                forced = True
                signal.pidfd_send_signal(pidfd, signal.SIGTERM)
                stdout, stderr = process.communicate(timeout=10)
            rc = process.wait()
        finally:
            os.close(pidfd)
    fd_after = sorted(os.listdir("/proc/self/fd"))
    _identity_evidence(vector, {"layer": "real installed -I CLI rejection", "argv": argv,
        "cwd": str(tmp_path), "env": env, "start": started, "end": time.time(),
        "pid": process.pid, "waited_pid": process.pid, "rc": rc, "forced": forced,
        "pidfd_closed": True, "stdout": stdout.decode(), "stderr": stderr.decode(),
        "FD_before": fd_before, "FD_after": fd_after, "before": before,
        "after": _identity_tree(tmp_path), "database": None})
    assert not forced
    assert rc == 2 and "usage:" in stderr.decode()
    assert _identity_tree(tmp_path) == before
    assert fd_before == fd_after
    assert not database.exists()


@pytest.mark.parametrize("vector", [
    "missing", "tampered", "old-only", "ambiguous", "stale-driver", "stale-e2e", "changed-package",
])
def test_fresh_consumer_identity_rejects_before_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, vector: str
) -> None:
    """纯guard状态边界：负例只改私有副本；完整实读环境由唯一Q命令证明。"""
    from types import SimpleNamespace

    module = _consumer_module()
    grant = json.loads(Path(os.environ["KRIM_M2_FINAL_GRANT"]).read_bytes())
    qualified = json.loads(Path(grant["consumer_identity_receipt"]).read_bytes())
    schema = json.loads(Path(grant["consumer_identity_contract_file"]).read_bytes())
    root = tmp_path / "isolated-metadata"
    q = root / "evidence/final/final-directory01-consumer"
    q.mkdir(parents=True)
    b = root / "evidence/final/final-recovery01-first"
    b.mkdir()
    candidate = json.loads(json.dumps(qualified))
    candidate["acceptance_root"] = str(root)
    aliases = {
        "receipt": "CONSUMER-IDENTITY.json", "validation_command": "command.json",
        "validation_result": "result.json", "completion_seal": "CONSUMER-COMPLETE.json",
        "source_manifest": "source-manifest.json",
    }
    for key, name in aliases.items():
        original = Path(schema[key])
        (q / name).write_bytes(original.read_bytes())
        schema[key] = str(q / name)
    schema_path = root / "consumer-schema.json"
    schema_path.write_text(json.dumps(schema))
    grant.update(acceptance_root=str(root),
        installation_identity_receipt=schema["receipt"],
        consumer_identity_receipt=schema["receipt"],
        installation_identity_contract_file=str(schema_path),
        consumer_identity_contract_file=str(schema_path))
    observed = json.loads(json.dumps(candidate))
    source_copy = root / "observed-consumer.txt"
    if vector in ("stale-driver", "stale-e2e"):
        key = "driver" if vector == "stale-driver" else "e2e"
        source_copy.write_bytes(Path(candidate["consumer"][key]["path"]).read_bytes())
        candidate["consumer"][key]["path"] = str(source_copy)
        observed = json.loads(json.dumps(candidate))
        source_copy.write_bytes(source_copy.read_bytes() + b"\nchanged consumer\n")
    elif vector == "changed-package":
        source_copy.write_bytes(b"changed installed member")
    receipt_path = Path(schema["receipt"])
    receipt_path.write_text(json.dumps(candidate))
    complete = json.loads(Path(schema["completion_seal"]).read_bytes())
    complete["consumer_sha256"] = hashlib.sha256(receipt_path.read_bytes()).hexdigest()
    Path(schema["completion_seal"]).write_text(json.dumps(complete))

    if vector == "missing":
        receipt_path.unlink()
    elif vector == "tampered":
        receipt_path.write_text(receipt_path.read_text() + " ")
    elif vector == "old-only":
        legacy = Path(grant["inherited_installation"]["identity_receipt"]["copy_path"])
        receipt_path.write_bytes(legacy.read_bytes())
    elif vector == "ambiguous":
        grant["installation_identity_receipt"] = str(root / "other-receipt.json")

    counts = {"serve": 0, "owner_start": 0, "observer": 0}

    def observe(_root: Path, _grant: dict[str, object]) -> object:
        counts["observer"] += 1
        value = json.loads(json.dumps(observed))
        if vector in ("stale-driver", "stale-e2e"):
            key = "driver" if vector == "stale-driver" else "e2e"
            value["consumer"][key]["sha256"] = hashlib.sha256(source_copy.read_bytes()).hexdigest()
        elif vector == "changed-package":
            member = next(iter(value["installation"]["package_files"]))
            value["installation"]["package_files"][member]["site_sha256"] = (
                hashlib.sha256(source_copy.read_bytes()).hexdigest())
        return value

    def forbidden_serve(*_args: object, **_kwargs: object) -> None:
        counts["serve"] += 1
        raise AssertionError("product entry must remain unopened")

    def forbidden_start(*_args: object, **_kwargs: object) -> None:
        counts["owner_start"] += 1
        raise AssertionError("owner entry must remain unopened")

    monkeypatch.setattr(module, "consumer_facts", observe)
    monkeypatch.setattr(module, "serve", forbidden_serve)
    monkeypatch.setattr(getattr(module, "Owner"), "start", forbidden_start)
    monkeypatch.setattr(module, "context", lambda: (root, grant))
    monkeypatch.setattr(module, "arguments", lambda: SimpleNamespace(
        evidence=b, database=b / "browser.sqlite", port=8876, qualify_consumer=None))
    expected = {
        "missing": "installation_identity: .*No such file", "tampered": "consumer seal",
        "old-only": "consumer fields", "ambiguous": "receipt aliases",
        "stale-driver": "consumer bytes changed: driver",
        "stale-e2e": "consumer bytes changed: e2e", "changed-package": "installation bytes changed",
    }[vector]
    before, fd_before = _identity_tree(root), sorted(os.listdir("/proc/self/fd"))
    with pytest.raises(ValueError, match=expected) as rejected:
        getattr(module, "main")()
    fd_after = sorted(os.listdir("/proc/self/fd"))
    assert counts["serve"] == counts["owner_start"] == 0
    assert counts["observer"] == (1 if vector in (
        "stale-driver", "stale-e2e", "changed-package") else 0)
    assert _identity_tree(root) == before
    assert fd_before == fd_after
    assert not (b / "browser.sqlite").exists()
    _identity_evidence(vector, {"layer": "pure metadata gate with resource-entry counters",
        "fresh_observation_source": "unique real Q; changed private files rehashed",
        "qualification_receipt": qualified["qualification"], "case_root": str(root),
        "database": None, "rejection": str(rejected.value), "counts": counts,
        "before": before, "after": _identity_tree(root),
        "FD_before": fd_before, "FD_after": fd_after})


def test_inherited_installation_qualifies_current_consumer() -> None:
    """只核唯一真实Q结果；不重跑安装、CLI、例子或rollback。"""
    import krim_agent

    gpath = Path(os.environ["KRIM_M2_FINAL_GRANT"])
    grant = json.loads(gpath.read_bytes())
    h = Path(grant["acceptance_root"])
    c = Path(grant["consumer_identity_receipt"])
    value = json.loads(c.read_bytes())
    seal = json.loads((c.parent / "CONSUMER-COMPLETE.json").read_bytes())
    result = json.loads((c.parent / "result.json").read_bytes())
    assert value["kind"] == "M2_FINAL_CONSUMER_QUALIFICATION_V1"
    assert type(value["schema_version"]) is int and value["schema_version"] == 1
    assert value["scope"] == "consumer-only"
    assert value["grant_sha256"] == hashlib.sha256(gpath.read_bytes()).hexdigest()
    assert value["installation"]["interpreter"]["isolated"] == 1
    assert value["installation"]["interpreter"]["prefix"] == str(h / "installed-env")
    assert value["installation"]["interpreter"]["path"] == str(h / "installed-env/bin/python")
    assert value["installation"]["site_root"].startswith(str(h / "installed-env") + "/")
    assert len(value["installation"]["package_files"]) == 75
    assert seal["consumer_sha256"] == hashlib.sha256(c.read_bytes()).hexdigest()
    result_hash = hashlib.sha256((c.parent / "result.json").read_bytes()).hexdigest()
    assert seal["result_sha256"] == result_hash
    assert result["exit_code"] == 0 and result["kind"] == "consumer-qualify"
    assert len(value["inherited"]) == 14
    for key, item in value["inherited"].items():
        copied, original = Path(item["copy_path"]), Path(item["original_path"])
        assert hashlib.sha256(original.read_bytes()).hexdigest() == item["sha256"]
        assert copied.read_bytes() == original.read_bytes(), key
    legacy = json.loads(Path(value["inherited"]["identity_receipt"]["copy_path"]).read_bytes())
    assert len(legacy) == 13 and legacy["kind"] == "M2_FINAL_INSTALLED_IDENTITY_V1"
    for name, item in value["installation"]["package_files"].items():
        assert item["source_sha256"] == item["wheel_sha256"] == item["site_sha256"]
        assert item["site_sha256"] == legacy["package_files"][name]["site_sha256"]
    for key, name in [("driver", "tests/agent_core/fixtures/installed_workspace.py"),
                      ("e2e", "frontend/e2e/agent-workspace.spec.ts")]:
        source_hash = hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
        assert value["consumer"][key]["sha256"] == source_hash
    assert Path(krim_agent.__file__).is_relative_to(ROOT / "src")
    _identity_evidence("inherited", {"layer": "read unique actual qualification",
        "receipt": str(c), "seal": seal, "qualification": value["qualification"],
        "installation": value["installation"], "pytest_product_consumer": krim_agent.__file__,
        "database": None, "provider_calls": 0, "tool_calls": 0})
