"""SessionService无限制配置回归：None感知ContextPolicy、可空超时、分页与join清理。"""

import asyncio
from types import SimpleNamespace

import pytest

import krim_agent.sessions.service as session_module
from krim_agent.context.types import ContextPolicy
from krim_agent.contracts import ServiceError
from krim_agent.sessions.service import SessionService
from krim_agent.sessions.types import SessionCreate


class SpyTimeout:
    """记录reschedule的timeout替身；when/reschedule(None)语义与真实timeout一致。"""

    def __init__(self, seconds):
        self.seconds = seconds
        self.deadline = float("inf") if seconds is not None else None
        self.reschedules = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    def when(self):
        return self.deadline

    def reschedule(self, when):
        self.reschedules.append(when)
        self.deadline = when


def bare_service(**attrs):
    service = object.__new__(SessionService)
    service.identities = None
    for key, value in attrs.items():
        setattr(service, key, value)
    return service


def principal(pid="p1", role="admin"):
    def member(_workspace, write=False, approve=False):
        return SimpleNamespace(role=role)

    def owns(*_args, **_kwargs):
        return None

    return SimpleNamespace(principal_id=pid, member=member, owns=owns)


def install_sql(monkeypatch, **functions):
    monkeypatch.setattr(session_module, "sql", SimpleNamespace(**functions))


@pytest.mark.parametrize(
    ("configured_bytes", "inherited_messages", "requested", "accepted", "expected_bytes"),
    [
        (None, None, None, True, 262144),
        (None, None, {"max_bytes": 16384, "max_messages": 64}, True, 16384),
        (None, None, {"max_bytes": 262145, "max_messages": None}, False, None),
        (65536, None, {"max_bytes": 65536, "max_messages": None}, True, 65536),
        (500000, None, {"max_bytes": 262144, "max_messages": None}, True, 262144),
        (65536, None, {"max_bytes": 16384, "max_messages": None}, True, 16384),
        (65536, None, {"max_bytes": 65537, "max_messages": None}, False, None),
        (65536, None, {"max_bytes": 16384, "max_messages": 64}, True, 16384),
        (65536, 64, {"max_bytes": 16384, "max_messages": 64}, True, 16384),
        (65536, 64, {"max_bytes": 16384, "max_messages": 32}, True, 16384),
        (65536, 64, {"max_bytes": 16384, "max_messages": None}, False, None),
        (65536, 64, {"max_bytes": 16384, "max_messages": 65}, False, None),
    ],
)
async def test_create_policy_none_aware(
    monkeypatch, configured_bytes, inherited_messages, requested, accepted, expected_bytes
):
    settings = SimpleNamespace(
        context_max_bytes=configured_bytes,
        context_max_messages=inherited_messages,
        context_reserved_output_tokens=2048,
    )
    binding = SimpleNamespace(
        assert_current=lambda: None,
        loop=SimpleNamespace(provider=object()),
        settings=SimpleNamespace(request_capacity=lambda _provider: 262144),
        descriptor_json='{"target": {}}',
    )
    service = bare_service(
        store=SimpleNamespace(healthy=True),
        runs=SimpleNamespace(bindings={"target-v2": binding}),
        settings=settings,
        closed=False,
        failure=None,
    )
    captured = {}

    async def by_key(*_args):
        raise ServiceError("not_found")

    async def create_session(_store, workspace, owner, _body, descriptor, policy, run_settings):
        captured.update(
            workspace=workspace, owner=owner, descriptor=descriptor, policy=policy,
            settings=run_settings,
        )
        return SimpleNamespace(id="session-1")

    install_sql(monkeypatch, by_key=by_key, create_session=create_session)
    request = SessionCreate(
        request_key="k1",
        target_key="target-v2",
        context_policy=ContextPolicy(**requested) if requested is not None else None,
    )
    if not accepted:
        with pytest.raises(ServiceError) as error:
            await service.create(principal(), "local", request)
        assert error.value.code == "invalid_input" and error.value.status == 422
        assert not captured
        return
    view = await service.create(principal(), "local", request)
    assert view.id == "session-1"
    assert captured["policy"].max_messages == (
        requested["max_messages"] if requested is not None else inherited_messages
    )
    assert captured["policy"].max_bytes == expected_bytes
    assert captured["descriptor"] == {"target": {}}
    assert captured["settings"] is settings


@pytest.mark.parametrize("method", ["list", "turns"])
async def test_read_side_accepts_offsets_over_100000(monkeypatch, method):
    calls = []

    async def list_sessions(_store, workspace, owner, admin, offset, limit, *, view):
        calls.append((workspace, owner, admin, offset, limit, view))
        return []

    async def list_turns(_store, session_id, offset, limit):
        calls.append((session_id, offset, limit))
        return []

    install_sql(monkeypatch, list_sessions=list_sessions, list_turns=list_turns)
    service = bare_service(store=object())

    async def get_session(*_args, **_kwargs):
        return SimpleNamespace(workspace_id="local", owner_id="p1")

    service.get = get_session
    if method == "list":
        assert await service.list(principal(), "local", 200001, 50) == []
        assert calls == [("local", "p1", True, 200001, 50, "active")]
    else:
        assert await service.turns(principal(), "local", "session-1", 100001, 50) == []
        assert calls == [("session-1", 100001, 50)]


@pytest.mark.parametrize(
    ("method", "kwargs"),
    [
        ("list", {"offset": -1}),
        ("list", {"offset": True}),
        ("list", {"offset": 1.5}),
        ("list", {"limit": 0}),
        ("list", {"limit": 51}),
        ("list", {"limit": True}),
        ("list", {"limit": 2.5}),
        ("turns", {"offset": -1}),
        ("turns", {"offset": False}),
        ("turns", {"offset": 0.0}),
        ("turns", {"limit": 0}),
        ("turns", {"limit": 51}),
        ("turns", {"limit": 1.5}),
    ],
)
async def test_read_side_rejects_bad_offset_and_limit(monkeypatch, method, kwargs):
    async def reject(*_args, **_kwargs):
        raise AssertionError("非法offset/limit必须在SQL之前拒绝")

    install_sql(monkeypatch, list_sessions=reject, list_turns=reject)
    service = bare_service(store=object())

    async def get_session(*_args, **_kwargs):
        return SimpleNamespace(workspace_id="local", owner_id="p1")

    service.get = get_session
    call = service.list if method == "list" else service.turns
    if method == "list":
        positional = (principal(), "local")
        defaults = {"offset": 0, "limit": 20}
    else:
        positional = (principal(), "local", "session-1")
        defaults = {"offset": 0, "limit": 20}
    with pytest.raises(ServiceError) as error:
        await call(*positional, **(defaults | kwargs))
    assert error.value.code == "invalid_input"


async def test_join_session_walks_pages_and_cancels_running_runs(monkeypatch):
    total = 107

    def view(i):
        if i in {0, 1, 50}:
            return SimpleNamespace(status="running", run_id=f"run-{i}")
        if i == 2:
            return SimpleNamespace(status="completed", run_id="run-ghost")
        return SimpleNamespace(status="queued", run_id=None)

    calls = []

    async def list_turns(_store, session_id, offset, limit):
        calls.append((session_id, offset, limit))
        assert limit == 50
        return [view(i) for i in range(offset, min(offset + limit, total))]

    ghost = asyncio.get_running_loop().create_future()
    ghost.set_result(None)
    pending = {
        f"run-{i}": asyncio.get_running_loop().create_future() for i in (0, 1, 50)
    }
    cancels = []

    async def cancel(run_id):
        cancels.append(run_id)
        pending[run_id].set_result(None)

    service = bare_service(
        store=object(),
        runs=SimpleNamespace(tasks={**pending, "run-ghost": ghost}, cancel=cancel),
    )
    install_sql(monkeypatch, list_turns=list_turns)
    try:
        await service._join_session("session-1")
    finally:
        await asyncio.gather(*pending.values(), return_exceptions=True)
    assert calls == [("session-1", 0, 50), ("session-1", 50, 50), ("session-1", 100, 50)]
    assert cancels == ["run-0", "run-1", "run-50"]
    assert all(task.done() for task in {**pending, "run-ghost": ghost}.values())


async def test_join_session_propagates_cancel_failures(monkeypatch):
    error = ServiceError("not_found")

    async def list_turns(_store, _session_id, offset, limit):
        return [
            SimpleNamespace(status="running", run_id=f"run-{i}")
            for i in range(offset, min(offset + limit, 103))
        ]

    late = asyncio.get_running_loop().create_future()
    cancels = []

    async def cancel(run_id):
        cancels.append(run_id)
        if run_id == "run-2":
            late.set_result(None)
        if run_id == "run-1":
            raise error

    service = bare_service(
        store=object(),
        runs=SimpleNamespace(tasks={"run-2": late}, cancel=cancel),
    )
    install_sql(monkeypatch, list_turns=list_turns)
    with pytest.raises(ServiceError) as raised:
        await service._join_session("session-1")
    assert raised.value is error
    assert cancels == [f"run-{i}" for i in range(103)]
    assert late.done()


def waiting_service(monkeypatch, queue_timeout, run_timeout, read_turn, tasks=None, wait_run=None):
    service = bare_service(
        settings=SimpleNamespace(session_queue_timeout=queue_timeout, run_timeout=run_timeout),
        turn=read_turn,
    )
    spies, sleeps, waits = [], [], []

    def factory(seconds):
        spy = SpyTimeout(seconds)
        spies.append(spy)
        return spy

    async def counting_sleep(seconds):
        sleeps.append(seconds)
        await asyncio.sleep(seconds)

    async def recorded_wait(run_id):
        waits.append(run_id)

    monkeypatch.setattr(
        session_module,
        "asyncio",
        SimpleNamespace(
            timeout=factory,
            sleep=counting_sleep,
            get_running_loop=asyncio.get_running_loop,
        ),
    )
    service.runs = SimpleNamespace(tasks=tasks or {}, wait=wait_run or recorded_wait)
    return service, spies, sleeps, waits


@pytest.mark.parametrize(
    ("queue_timeout", "run_timeout", "budget"),
    [(None, None, None), (None, 5.0, None), (5.0, None, None), (5.0, 5.0, 40.0)],
)
async def test_wait_accepts_nullable_timeout_combinations(
    monkeypatch, queue_timeout, run_timeout, budget
):
    terminal = SimpleNamespace(status="completed", run_id=None)

    async def read_turn(*_args):
        return terminal

    service, spies, _sleeps, _waits = waiting_service(
        monkeypatch, queue_timeout, run_timeout, read_turn
    )
    assert await service.wait(None, "local", "session-1", "turn-1") is terminal
    assert spies[0].seconds == pytest.approx(budget)
    assert spies[0].reschedules == []


async def test_wait_unlimited_queued_read_gets_no_fake_deadline(monkeypatch):
    reads = []

    async def read_turn(*_args):
        reads.append(1)
        if len(reads) <= 2:
            return SimpleNamespace(status="queued", run_id=None)
        return SimpleNamespace(status="completed", run_id=None)

    service, spies, sleeps, _waits = waiting_service(monkeypatch, None, 5.0, read_turn)
    result = await service.wait(None, "local", "session-1", "turn-1")
    assert result.status == "completed"
    assert spies[0].seconds is None
    assert spies[0].reschedules == []
    assert len(sleeps) == 2


async def test_wait_parked_run_with_done_producer_completes(monkeypatch):
    producer = asyncio.create_task(asyncio.sleep(0))
    await producer
    reads = []

    async def read_turn(*_args):
        reads.append(1)
        if len(reads) == 1:
            return SimpleNamespace(status="running", run_id="run-9")
        return SimpleNamespace(status="completed", run_id="run-9")

    service, spies, sleeps, waits = waiting_service(
        monkeypatch, 5.0, 5.0, read_turn, tasks={"run-9": producer}
    )
    result = await service.wait(None, "local", "session-1", "turn-1")
    assert result.status == "completed"
    assert producer.done() and not producer.cancelled()
    assert spies[0].reschedules == [None]
    assert waits == ["run-9"]
    assert sleeps == [0.02]
