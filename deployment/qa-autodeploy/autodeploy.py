#!/usr/bin/env python3
"""Krim QA autodeploy: poll GitHub qa branch, build+verify+switch (krim_qa_autodeploy).

Mirror of krim_autodeploy.py but:
- tracks commits/qa instead of main deployments
- STATE=/var/lib/krimdemo/qa-autodeploy, ledger qa-ledger.jsonl
- releases named agent-core-qa-<sha>-auto, service krim-qa.service, drop-in dir for it
- port 18771, DB /var/lib/krimdemo/qa-demo/krimdemo.sqlite
- deploy(sha, first=False): when first=True, skips drop-in/service restart (used by
  installer to build the initial release; service unit points at it directly)
"""

from __future__ import annotations

import fcntl
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

REPO = "OneLastHug/Krim"
STATE = Path("/var/lib/krimdemo/qa-autodeploy")
LEDGER = STATE / "qa-ledger.jsonl"
RELEASES = Path("/opt/krimdemo/releases")
BASE = RELEASES / "agent-core-main-378f4e3-20261106"
PY312 = BASE / "venv/bin/python3.12"
SERVICE = "krim-qa.service"
DROPIN_DIR = Path("/etc/systemd/system/krim-qa.service.d")
AUTO_DROPIN_PREFIX = "zzzzz-qa-auto-"
DBDIR = Path("/var/lib/krimdemo/qa-demo")
HEALTH = "http://127.0.0.1:18771/healthz"
API_QA_SHA = f"https://api.github.com/repos/{REPO}/commits/qa"

TESTS_WHITELIST = [
    "tests/core/test_contracts.py",
    "tests/core/test_runtime.py",
    "tests/core/test_storage.py",
    "tests/agent_core/test_sessions.py",
    "tests/agent_core/test_session_controls.py",
]


def log(event: str, **kw) -> None:
    rec = {"at": time.strftime("%Y-%m-%d %H:%M:%S"), "ts": int(time.time()), "event": event, **kw}
    with LEDGER.open("a") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(json.dumps(rec, ensure_ascii=False), flush=True)


def http_json(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": "krim-qa-autodeploy"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.load(resp)


def current_deployed_sha() -> str | None:
    try:
        return (STATE / "deployed.sha").read_text().strip()
    except OSError:
        return None


def resolve_qa_sha() -> str | None:
    try:
        obj = http_json(API_QA_SHA)
        return str(obj.get("sha") or "") or None
    except Exception as exc:
        log("commits-api-error", error=str(exc)[:200])
        return None


def download_tarball(sha: str, dest: Path) -> None:
    url = f"https://codeload.github.com/{REPO}/tar.gz/{sha}"
    req = urllib.request.Request(url, headers={"User-Agent": "krim-qa-autodeploy"})
    with urllib.request.urlopen(req, timeout=300) as resp, dest.open("wb") as fh:
        shutil.copyfileobj(resp, fh)


def extract(src: Path, dest: Path) -> None:
    subprocess.run(["tar", "-xzf", str(src), "-C", str(dest)], check=True)


def run(cmd: list[str], cwd: Path | None = None, env: dict | None = None, timeout: int = 1200) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True, timeout=timeout)


def preflight(rel: Path, work: Path) -> None:
    """Start the exact final runtime against SQLite backups, never live data."""
    import pwd
    import socket
    import sqlite3
    probe = (work / "runtime-probe").resolve()
    probe.mkdir()
    user = pwd.getpwnam("krimdemo")
    versions = {}
    for db in DBDIR.glob("krimdemo*.sqlite"):
        with sqlite3.connect("file:" + str(db) + "?mode=ro", uri=True) as source:
            versions[db.name] = source.execute("PRAGMA user_version").fetchone()[0]
            with sqlite3.connect(probe / db.name) as target:
                source.backup(target)
    for path in [probe, *probe.iterdir()]:
        os.chown(path, user.pw_uid, user.pw_gid)
        path.chmod(0o700 if path.is_dir() else 0o600)
    port = 18779
    with socket.socket() as check:
        check.bind(("127.0.0.1", port))
    unit_name = "krim-qa-preflight.service"
    unit = Path("/run/systemd/system") / unit_name
    original = subprocess.check_output(["systemctl", "cat", SERVICE], text=True)
    unit.write_text(original + f"\n[Service]\nExecStart=\nExecStart={rel}/venv/bin/python -I -B -m krim_agent serve --database {probe}/krimdemo.sqlite --port {port}\nWorkingDirectory={probe}\nReadWritePaths=\nReadWritePaths={probe}\nEnvironment=LD_LIBRARY_PATH={rel}/python/lib\nRestart=no\nIPAddressAllow=\nIPAddressAllow=localhost\nIPAddressDeny=any\n")
    try:
        subprocess.run(["systemctl", "daemon-reload"], check=True)
        subprocess.run(["systemctl", "start", unit_name], check=True)
        ok = False
        for _ in range(20):
            time.sleep(1)
            try:
                if http_json(f"http://127.0.0.1:{port}/healthz").get("status") == "healthy":
                    ok = True
                    break
            except Exception:
                pass
        if not ok:
            raise RuntimeError("final runtime failed database-copy startup; inspect preflight journal")
        for name, version in versions.items():
            with sqlite3.connect(probe / name) as conn:
                if conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise RuntimeError("database-copy integrity failed")
                if conn.execute("PRAGMA user_version").fetchone()[0] != version:
                    raise RuntimeError("schema migration requires explicit reviewed deployment; automatic cutover refused")
        log("runtime-preflight-passed", release=str(rel), schema_versions=versions)
    finally:
        subprocess.run(["systemctl", "stop", unit_name], check=False)
        unit.unlink(missing_ok=True)
        subprocess.run(["systemctl", "daemon-reload"], check=False)


def deploy(sha: str, first: bool = False) -> bool:
    short = sha[:7]
    work = STATE / f"build-{short}-{int(time.time())}"
    rel = RELEASES / f"agent-core-qa-{short}-auto"
    srcdir = None
    previous_dropins = {p: p.read_bytes() for p in DROPIN_DIR.glob(AUTO_DROPIN_PREFIX + "*.conf")}
    switched = False
    try:
        work.mkdir(parents=True)
        # 1. fetch + extract
        tar = work / "src.tar.gz"
        download_tarball(sha, tar)
        extract(tar, work)
        srcdir = next(p for p in work.iterdir() if p.is_dir() and p.name.startswith("Krim-"))
        log("source-ready", sha=sha, size=tar.stat().st_size)

        # 2. build wheel with release python
        venv = work / "buildvenv"
        r = run([str(PY312), "-m", "venv", str(venv)])
        if r.returncode:
            log("build-venv-failed", err=r.stderr[-300:]); return False
        r = run([str(venv / "bin/pip"), "install", "-q", "--no-cache-dir", "build"], timeout=600)
        if r.returncode:
            log("build-deps-failed", err=r.stderr[-300:]); return False
        log("building-wheel", sha=sha)
        r = run([str(venv / "bin/python"), "-m", "build", "--wheel", "--outdir", str(work)], cwd=srcdir, timeout=1500)
        if r.returncode:
            log("build-wheel-failed", err=r.stderr[-500:]); return False
        wheel = next(work.glob("krim_agent-*.whl"))
        log("wheel-built", wheel=wheel.name)

        # 3. verify: install wheel, import smoke, whitelist tests
        r = run([str(venv / "bin/pip"), "install", "-q", "--no-cache-dir", str(wheel)], timeout=600)
        if r.returncode:
            log("wheel-install-failed", err=r.stderr[-300:]); return False
        r2 = run([str(venv / "bin/python"), "-I", "-c", "import krim_agent; print('ok')"])
        if r2.returncode:
            log("wheel-import-failed", err=r2.stderr[-300:]); return False
        test_files = [t for t in TESTS_WHITELIST if (srcdir / t).exists()]
        if test_files:
            r = run([str(venv / "bin/pip"), "install", "-q", "--no-cache-dir", "pytest", "pytest-asyncio"], timeout=600)
            if r.returncode:
                log("pytest-install-failed", err=r.stderr[-300:]); return False
            test_env = {**os.environ, "KRIM_SAFE_EVAL_ROOT": str(work / "safe-root"),
                        "PYTHONDONTWRITEBYTECODE": "1"}
            (work / "safe-root").mkdir(exist_ok=True)
            log("running-tests", sha=sha, n=len(test_files))
            r3 = run([str(venv / "bin/python"), "-m", "pytest", "-q", "-p", "no:cacheprovider",
                      "--basetemp=" + str(work / "pytest")] + test_files, cwd=srcdir, env=test_env, timeout=1500)
            if r3.returncode:
                log("verify-tests-failed", tail=r3.stdout[-600:], err=r3.stderr[-300:]); return False
            log("tests-passed", sha=sha)
        else:
            log("tests-skipped-no-files", sha=sha)

        # 4. stage release
        if rel.exists():
            shutil.rmtree(rel)
        rel.mkdir(parents=True)
        shutil.copytree(BASE / "python", rel / "python")
        r = run([str(PY312), "-m", "venv", str(rel / "venv")])
        if r.returncode:
            raise RuntimeError("final venv creation failed: " + r.stderr[-300:])
        final_python = str(rel / "venv/bin/python")
        r = run([final_python, "-m", "pip", "install", "-q", "--no-cache-dir", str(wheel), "mcp==1.30.0"], timeout=900)
        if r.returncode:
            raise RuntimeError("final dependency installation failed: " + r.stderr[-500:])
        r = run([final_python, "-m", "pip", "check"])
        if r.returncode:
            raise RuntimeError("final dependency check failed: " + r.stdout[-300:])
        r = run([final_python, "-I", "-B", "-c", "import krim_agent.cli; print('cli-import-ok')"])
        if r.returncode:
            raise RuntimeError("final CLI import failed: " + r.stderr[-500:])
        (rel / "release.json").write_text(json.dumps({
            "revision": sha, "auto": True, "wheel": wheel.name, "branch": "qa",
            "deployed": time.strftime("%Y-%m-%d %H:%M:%S")}, ensure_ascii=False))
        log("release-staged", sha=sha, release=str(rel))

        if first:
            shutil.rmtree(work, ignore_errors=True)
            return True

        # Check final runtime and current schema on backups before touching service.
        preflight(rel, work)
        # 5. switch: consistent SQLite backup, drop-in, restart, health check
        import sqlite3
        for db in DBDIR.glob("krimdemo*.sqlite"):
            with sqlite3.connect("file:" + str(db) + "?mode=ro", uri=True) as source:
                with sqlite3.connect(db.with_suffix(db.suffix + f".bak-qa-{short}")) as target:
                    source.backup(target)
        DROPIN_DIR.mkdir(parents=True, exist_ok=True)
        prev_auto = sorted(DROPIN_DIR.glob(AUTO_DROPIN_PREFIX + "*.conf"))
        dropin = DROPIN_DIR / f"{AUTO_DROPIN_PREFIX}{short}.conf"
        switched = True
        dropin.write_text(
            "[Service]\n"
            f"# qa auto deploy {short}\n"
            "ExecStart=\n"
            f"ExecStart={rel}/venv/bin/python -I -B -m krim_agent serve "
            f"--database {DBDIR}/krimdemo.sqlite --port 18771\n"
            f"Environment=LD_LIBRARY_PATH={rel}/python/lib\n")
        for p in prev_auto:
            if p != dropin:
                p.unlink()
        subprocess.run(["systemctl", "daemon-reload"], check=True)
        subprocess.run(["systemctl", "restart", SERVICE], check=True)
        ok = False
        for _ in range(15):
            time.sleep(2)
            try:
                with urllib.request.urlopen(HEALTH, timeout=4) as resp:
                    if json.load(resp).get("status") == "healthy":
                        ok = True
                        break
            except Exception:
                pass
        if not ok:
            raise RuntimeError("health check failed after switch")
        log("deployed", sha=sha, release=str(rel))
        shutil.rmtree(work, ignore_errors=True)
        return True
    except Exception as exc:
        log("deploy-failed", sha=sha, error=str(exc)[:400])
        if switched:
            for p in DROPIN_DIR.glob(AUTO_DROPIN_PREFIX + "*.conf"):
                p.unlink()
            for p, data in previous_dropins.items():
                p.write_bytes(data)
            subprocess.run(["systemctl", "daemon-reload"], check=False)
            subprocess.run(["systemctl", "restart", SERVICE], check=False)
            try:
                recovered = http_json(HEALTH).get("status") == "healthy"
            except Exception:
                recovered = False
            log("rollback-healthy" if recovered else "rollback-failed", sha=sha)
        else:
            log("serving-release-unchanged", sha=sha)
        shutil.rmtree(work, ignore_errors=True)
        if rel.exists():
            try:
                if (STATE / "deployed.sha").read_text().strip() != sha:
                    shutil.rmtree(rel, ignore_errors=True)
            except OSError:
                shutil.rmtree(rel, ignore_errors=True)
        return False


def main() -> int:
    STATE.mkdir(parents=True, exist_ok=True)
    lock_fh = (STATE / "run.lock").open("a")
    try:
        fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("another instance running", flush=True)
        return 0
    prev = current_deployed_sha()
    sha = resolve_qa_sha()
    if not sha:
        log("no-signal", reason="commits qa api unavailable")
        return 0
    if sha == prev:
        return 0
    log("signal", sha=sha, prev=prev)
    ok = deploy(sha)
    if ok:
        (STATE / "deployed.sha").write_text(sha)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
