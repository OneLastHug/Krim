"""Host browser ownership and cleanup; no real browser or Docker in this suite."""

import asyncio
import sys
import tempfile
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.development import browser_downloads
from krim_agent.development.browser_worker import BrowserWorkers
from krim_agent.development.configuration import DevelopmentBrowserConfiguration
from krim_agent.development.contracts import EnvironmentHandle, RunOwnership
from krim_agent.security import PrincipalContext


@pytest.fixture
def rig(tmp_path, monkeypatch):
    order = []
    revoked = asyncio.Event()
    exchange_started = asyncio.Event()
    exchange_release = asyncio.Event()
    create_started = asyncio.Event()
    create_release = asyncio.Event()
    create_release.set()
    environment = EnvironmentHandle(owner_id="owner", project_id="project",
                                    environment_id="env", generation=1, state="ready")
    ownership = RunOwnership(owner_id="owner", session_id="session", run_id="run")
    principal = PrincipalContext("owner", 1, ())

    class Service:
        @asynccontextmanager
        async def route(self, preview, viewer):
            assert preview == "preview" and viewer == principal
            owner = asyncio.current_task()
            order.append("route-enter")
            try:
                yield SimpleNamespace(handle=SimpleNamespace(environment=environment)), object()
            finally:
                assert asyncio.current_task() is owner
                order.append("route-exit")

        async def recheck(self, *args):
            if revoked.is_set():
                raise ServiceError("permission_denied")

    class Bridge:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            self.owner = asyncio.current_task()
            order.append("bridge-enter")
            return self

        async def __aexit__(self, *args):
            assert asyncio.current_task() is self.owner
            order.append("bridge-exit")

    @dataclass
    class Spec:
        image_id: str
        name: str
        origin: str
        control_origin: str
        socket_path: Path
        ca_path: Path
        ca_sha256: str
        seccomp_path: Path

    package = ModuleType("krim_agent.dev_browser")
    package.__path__ = []
    bridge = ModuleType("krim_agent.dev_browser.host_bridge")
    bridge.PreviewConnectBridge = Bridge
    launcher = ModuleType("krim_agent.dev_browser.launcher")
    launcher.WorkerSpec = Spec
    monkeypatch.setitem(sys.modules, "krim_agent.dev_browser", package)
    monkeypatch.setitem(sys.modules, "krim_agent.dev_browser.host_bridge", bridge)
    monkeypatch.setitem(sys.modules, "krim_agent.dev_browser.launcher", launcher)

    async def issue(preview, viewer):
        assert viewer == principal
        return SimpleNamespace(ticket="fixture-private-ticket")

    app = SimpleNamespace(service=Service(), issue_bootstrap=issue, origins=SimpleNamespace(
        host=lambda value: "p.example", origin=lambda value: "https://p.example",
        control_origin="https://control.example",
    ))
    workers = BrowserWorkers(app, DevelopmentBrowserConfiguration(
        image_id="sha256:" + "a" * 64, ca_path=tmp_path / "ca.pem",
        ca_sha256="b" * 64, seccomp_path=tmp_path / "seccomp.json",
    ))
    control = SimpleNamespace(cleanup_fail=False, block_action=False, bad_bootstrap=False)

    async def create(session, spec):
        create_started.set()
        await create_release.wait()
        session.container_id = "c" * 64
        order.append("create")

    async def verify(session, spec):
        assert session.container_id == "c" * 64

    async def attach(session):
        order.append("attach")

    async def exchange(session, command):
        if command["op"] == "bootstrap":
            assert command["ticket"] == "fixture-private-ticket"
            return {"ok": True, "ready": not control.bad_bootstrap}
        exchange_started.set()
        if control.block_action:
            await exchange_release.wait()
        return {"ok": True, "result": {"done": command["op"]}}

    async def cleanup(session, spec):
        order.append("cleanup")
        if control.cleanup_fail:
            raise ServiceError("browser_worker_failed")
        session.container_id = None

    monkeypatch.setattr(workers, "_directory", lambda: Path(tempfile.mkdtemp(dir=tmp_path)))
    monkeypatch.setattr(workers, "_create", create)
    monkeypatch.setattr(workers, "_verify_container", verify)
    monkeypatch.setattr(workers, "_attach", attach)
    monkeypatch.setattr(workers, "_exchange", exchange)
    monkeypatch.setattr(workers, "_cleanup", cleanup)
    return SimpleNamespace(**locals())


async def test_browser_single_owner_and_cleanup_order(rig):
    r = rig
    opened = await r.workers.open(r.principal, r.environment, r.ownership, "preview")
    assert "ticket" not in opened
    with pytest.raises(ServiceError, match="capacity"):
        await r.workers.open(r.principal, r.environment, r.ownership, "preview")
    result = await r.workers.action(opened["browser_id"], r.principal, r.environment,
                                    r.ownership, {"op": "click", "selector": "button"})
    assert result == {"done": "click"}
    foreign = r.ownership.model_copy(update={"run_id": "other"})
    with pytest.raises(ServiceError, match="permission_denied"):
        await r.workers.action(opened["browser_id"], r.principal, r.environment, foreign,
                               {"op": "click"})
    await r.workers.close()
    assert not r.workers.sessions
    assert r.order[-3:] == ["cleanup", "bridge-exit", "route-exit"]


async def test_resize_action_reaches_worker_for_fixed_presets(rig):
    r = rig
    opened = await r.workers.open(r.principal, r.environment, r.ownership, "preview")
    for preset in ("desktop", "mobile"):
        result = await r.workers.action(
            opened["browser_id"], r.principal, r.environment, r.ownership,
            {"op": "resize", "preset": preset},
        )
        assert result == {"done": "resize"}
    await r.workers.close()
    assert not r.workers.sessions


@pytest.mark.parametrize("revoke", [False, True])
async def test_download_descriptor_stays_private_and_revocation_interrupts_stream(
    rig, monkeypatch, revoke,
):
    r = rig
    r.app.service.routes = object()
    started, interrupted = asyncio.Event(), asyncio.Event()

    async def exchange(session, command):
        if command["op"] == "bootstrap":
            return {"ok": True, "ready": True}
        assert command == {"op": "download_context", "path": "/image", "cookie_mode": "context"}
        return {"ok": True, "result": {"path": "/image", "cookie_mode": "context",
                                        "cookie_header": "app=private-session"}}

    async def fetch(routes, lease, authorize, **values):
        assert values["descriptor"]["cookie_header"] == "app=private-session"
        await authorize()
        started.set()
        if revoke:
            try:
                await asyncio.Event().wait()
            finally:
                interrupted.set()
        return {"status": 200, "size": 5, "media_type": "image/png", "sha256": "a" * 64}

    monkeypatch.setattr(r.workers, "_exchange", exchange)
    monkeypatch.setattr(browser_downloads, "inspect_download", fetch)
    opened = await r.workers.open(r.principal, r.environment, r.ownership, "preview")
    task = asyncio.create_task(r.workers.action(opened["browser_id"], r.principal, r.environment,
        r.ownership, {"op": "download", "path": "/image", "cookie_mode": "context"}))
    await asyncio.wait_for(started.wait(), 2)
    if revoke:
        r.revoked.set()
        with pytest.raises(ServiceError):
            await asyncio.wait_for(task, 3)
        assert interrupted.is_set()
    else:
        result = await task
        assert set(result) == {"status", "size", "media_type", "sha256"}
        assert "private-session" not in str(result)
    await r.workers.close()
    assert not r.workers.sessions
    assert r.order[-3:] == ["cleanup", "bridge-exit", "route-exit"]


async def test_revocation_interrupts_blocked_browser_action(rig):
    r = rig
    opened = await r.workers.open(r.principal, r.environment, r.ownership, "preview")
    r.control.block_action = True
    task = asyncio.create_task(r.workers.action(
        opened["browser_id"], r.principal, r.environment, r.ownership, {"op": "wait"},
    ))
    await asyncio.wait_for(r.exchange_started.wait(), 2)
    r.revoked.set()
    with pytest.raises(ServiceError):
        await asyncio.wait_for(task, 3)
    assert not r.workers.sessions
    assert r.order[-3:] == ["cleanup", "bridge-exit", "route-exit"]


async def test_cancel_during_create_waits_and_cleans_exact_result(rig):
    r = rig
    r.create_release.clear()
    task = asyncio.create_task(r.workers.open(r.principal, r.environment, r.ownership, "preview"))
    await asyncio.wait_for(r.create_started.wait(), 2)
    task.cancel()
    r.create_release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 3)
    assert not r.workers.sessions
    assert "cleanup" in r.order


async def test_failed_bootstrap_cleans_worker_before_exiting_lease(rig):
    r = rig
    r.control.bad_bootstrap = True
    with pytest.raises(ServiceError):
        await r.workers.open(r.principal, r.environment, r.ownership, "preview")
    assert not r.workers.sessions
    assert r.order[-3:] == ["cleanup", "bridge-exit", "route-exit"]


async def test_cleanup_failure_keeps_capacity_reserved(rig):
    r = rig
    await r.workers.open(r.principal, r.environment, r.ownership, "preview")
    r.control.cleanup_fail = True
    with pytest.raises(ServiceError):
        await r.workers.close()
    assert len(r.workers.sessions) == 1
    assert next(iter(r.workers.sessions.values())).container_id == "c" * 64
    r.control.cleanup_fail = False
    await r.workers.close()
    assert not r.workers.sessions
    assert r.order.count("cleanup") == 2


@pytest.mark.parametrize("prefix", ["", "error: ", "Error response from daemon: "])
@pytest.mark.parametrize("kind", ["No such object: ", "no such container: "])
def test_absence_requires_exact_created_identifier(prefix, kind):
    from krim_agent.dev_environment.docker import Reply

    cid = "c" * 64
    assert BrowserWorkers._absent(Reply(1, "[]\n", prefix + kind + cid + "\n"), cid)
    assert not BrowserWorkers._absent(Reply(1, "[]", prefix + kind + "d" * 64), cid)
    assert not BrowserWorkers._absent(Reply(1, "[]", "daemon unavailable: " + cid), cid)
    assert not BrowserWorkers._absent(Reply(0, "[]", prefix + kind + cid), cid)


async def test_cleanup_retry_accepts_only_proven_absent_created_container(rig):
    from krim_agent.dev_environment.docker import Reply
    from krim_agent.development.browser_worker import BrowserSession

    r = rig
    session = BrowserSession("browser-retry", r.principal, r.environment, r.ownership,
                             "preview", asyncio.get_running_loop().create_future())
    session.container_id = "c" * 64
    calls = []

    async def run(args):
        calls.append(args)
        return Reply(1, "[]\n", "error: no such object: " + "c" * 64 + "\n")

    r.monkeypatch.setattr(r.workers.runner, "run", run)
    r.monkeypatch.setattr(r.workers, "_verify_container",
                         BrowserWorkers._verify_container.__get__(r.workers))
    await BrowserWorkers._cleanup(r.workers, session, object())
    assert session.container_id is None
    assert calls == [("inspect", "c" * 64)]
