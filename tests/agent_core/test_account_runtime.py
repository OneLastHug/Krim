"""账号运行时授权验收：边界认证、SSE复核、effect围栏、派发门与撤销收敛。

托管身份用接口替身驱动本文件的运行时行为；不导入 accounts 模块，
IdentityService自身的契约由 test_account_api/test_account_cli 覆盖。
"""

from __future__ import annotations

import asyncio
import socket
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI, Request
from pydantic import SecretStr
from uvicorn import Config, Server

from krim_agent.api.boundary import LocalBoundary, _FailGate
from krim_agent.api.server import stream_recheck as legacy_stream_recheck
from krim_agent.api.sessions import stream_recheck as session_stream_recheck
from krim_agent.config import Settings
from krim_agent.contracts import RunEvent, ServiceError
from krim_agent.evaluation.automation import EvaluationAutomation
from krim_agent.runtime.service import RunBinding
from krim_agent.security import AccessPolicy, Credential, Membership, PrincipalContext
from krim_agent.sessions.types import SessionCreate, TurnRequest, TurnView
from krim_agent.storage import sessions as session_sql
from krim_agent.tools.builtins import demo_policy
from krim_agent.workspace.operations import WorkspaceOperations
from krim_agent.workspace.types import (
    ArtifactRevision,
    Completion,
    ExecutionAccess,
    GrantSnapshot,
    WorkFrame,
    WorkspaceProfile,
    WorkspaceTurnRequest,
)


def directory() -> dict[str, PrincipalContext]:
    return {
        "alice": PrincipalContext("alice", 1, (Membership(workspace_id="alpha", role="admin"),)),
        "bob": PrincipalContext("bob", 1, (Membership(workspace_id="alpha", role="member"),)),
        "viewer": PrincipalContext(
            "viewer", 1, (Membership(workspace_id="alpha", role="viewer"),)
        ),
    }


def runtime_policy(actors: dict[str, PrincipalContext] | None = None) -> AccessPolicy:
    actors = actors if actors is not None else directory()
    return AccessPolicy(
        credentials=tuple(
            Credential(
                principal_id=actor.principal_id,
                token=SecretStr("legacy-" + actor.principal_id),
                generation=actor.generation,
                memberships=actor.memberships,
            )
            for actor in actors.values()
        )
    )


class StubIdentities:
    """IdentityService最小接口替身：authenticate/resolve/validate/principals。"""

    managed = True

    def __init__(
        self,
        actors: dict[str, PrincipalContext],
        tokens: dict[str, str] | None = None,
    ) -> None:
        self.directory = dict(actors)
        self.tokens: dict[str, str] = dict(tokens or {})
        self.blocked: set[str] = set()
        self.calls = 0
        self.authenticate_failure = False
        self.validate_failure = False

    def authenticate(self, authorization: str) -> PrincipalContext:
        self.calls += 1
        if self.authenticate_failure:
            raise ServiceError("unauthorized", 401)
        principal_id = self.tokens.get(authorization)
        if principal_id is None or principal_id in self.blocked:
            raise ServiceError("unauthorized", 401)
        return self.directory[principal_id]

    def resolve(self, principal_id: str) -> PrincipalContext:
        if principal_id in self.blocked or principal_id not in self.directory:
            raise ServiceError("permission_denied")
        return self.directory[principal_id]

    def validate(self, principal: PrincipalContext) -> PrincipalContext:
        if self.validate_failure:
            raise ServiceError("unauthorized", 401)
        current = self.resolve(principal.principal_id)
        if current != principal:
            raise ServiceError("unauthorized", 401)
        return current

    @property
    def principals(self) -> tuple[PrincipalContext, ...]:
        return tuple(self.directory[actor] for actor in sorted(self.directory))

    def rotate(self, old: str, new: str) -> None:
        principal_id = self.tokens.pop(old)
        self.tokens[new] = principal_id

    async def close(self) -> None:
        return None


async def wait_turn(store, turn_id: str, statuses: set[str]):
    async with asyncio.timeout(15):
        while True:
            view = await session_sql.get_turn(store, turn_id)
            if view.status in statuses:
                return view
            await asyncio.sleep(0.02)


async def wait_until(predicate) -> None:
    async with asyncio.timeout(8.0):
        while True:
            value = predicate()
            if asyncio.iscoroutine(value):
                value = await value
            if value:
                return
            await asyncio.sleep(0.02)


def boundary_api(identities, settings: Settings) -> FastAPI:
    api = FastAPI()

    @api.get("/probe")
    async def probe(request: Request):
        actor = getattr(request.state, "principal", None)
        return {
            "principal": getattr(actor, "principal_id", None),
            "generation": getattr(actor, "generation", None),
            "authorization": getattr(request.state, "authorization", None),
        }

    api.add_middleware(LocalBoundary, settings=settings)
    api.state.application = SimpleNamespace(identities=identities, access_policy=None)
    return api


async def test_boundary_managed_authenticates_without_fallback(tmp_path):
    stub = StubIdentities(directory(), {"Bearer alice-managed": "alice"})
    settings = Settings(database=tmp_path / "boundary.sqlite", api_key=SecretStr("global-key"))
    api = boundary_api(stub, settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api), base_url="http://krim.test"
    ) as client:
        ok = await client.get("/probe", headers={"Authorization": "Bearer alice-managed"})
        assert ok.status_code == 200
        assert ok.json()["principal"] == "alice"
        assert ok.json()["generation"] == 1
        # 认证信息保存在请求状态供SSE复核；本断言只用合成token。
        assert ok.json()["authorization"] == "Bearer alice-managed"
        wrong = await client.get("/probe", headers={"Authorization": "Bearer nobody"})
        assert wrong.status_code == 401
        # 旧全局key在托管模式下不再被接受（认证失败不回落）。
        legacy = await client.get("/probe", headers={"Authorization": "Bearer global-key"})
        assert legacy.status_code == 401
        missing = await client.get("/probe")
        assert missing.status_code == 401


async def test_boundary_managed_throttle_is_bounded(tmp_path):
    stub = StubIdentities(directory(), {"Bearer alice-managed": "alice"})
    api = boundary_api(stub, Settings(database=tmp_path / "throttle.sqlite"))
    attacker = httpx.ASGITransport(app=api, client=("9.9.9.9", 40000))
    neighbor = httpx.ASGITransport(app=api, client=("10.0.0.1", 40001))
    async with httpx.AsyncClient(transport=attacker, base_url="http://krim.test") as client:
        for _ in range(_FailGate._LIMIT + 16):
            response = await client.get("/probe", headers={"Authorization": "Bearer attack"})
            assert response.status_code == 401
    # 有界分桶：攻击地址达到容量后不再调用authenticate。
    assert stub.calls == _FailGate._LIMIT
    # 单一坏客户端不锁死其他地址：合法token照常认证成功。
    async with httpx.AsyncClient(transport=neighbor, base_url="http://krim.test") as client:
        ok = await client.get("/probe", headers={"Authorization": "Bearer alice-managed"})
        assert ok.status_code == 200 and ok.json()["principal"] == "alice"


async def test_boundary_legacy_mode_unchanged(tmp_path):
    stub = StubIdentities(directory(), {})
    stub.managed = False
    settings = Settings(database=tmp_path / "legacy.sqlite", api_key=SecretStr("global-key"))
    api = boundary_api(stub, settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api), base_url="http://krim.test"
    ) as client:
        ok = await client.get("/probe", headers={"Authorization": "Bearer global-key"})
        assert ok.status_code == 200
        assert ok.json()["principal"] == "local"
        denied = await client.get("/probe", headers={"Authorization": "Bearer other"})
        assert denied.status_code == 401


def request_scope(application, authorization: str | None) -> dict:
    state: dict = {}
    if authorization is not None:
        state["authorization"] = authorization
    return {"type": "http", "app": application, "state": state}


async def test_session_stream_recheck_covers_rotation_disable_and_ownership():
    stub = StubIdentities(
        directory(),
        {"Bearer alice-managed": "alice", "Bearer bob-managed": "bob"},
    )
    sessions = SimpleNamespace(
        store=SimpleNamespace(closed=False),
        identities=None,
        failure=None,
        closed=False,
    )

    async def get(current, workspace, session_id, **_):
        # 模拟SessionService.get：先成员校验，再按owner/admin视图授权他人会话。
        membership = current.member(workspace)
        if workspace == "alpha" and session_id == "s-1" and (
            current.principal_id == "alice" or membership.role == "admin"
        ):
            return SimpleNamespace(id=session_id, owner_id="alice")
        raise ServiceError("not_found", 404)

    sessions.get = get
    api = FastAPI()
    api.state.application = SimpleNamespace(
        identities=stub, access_policy=None, sessions=sessions
    )

    recheck = session_stream_recheck(
        Request(request_scope(api, "Bearer alice-managed")), "alpha", "s-1"
    )
    assert recheck is not None and await recheck() is True
    # 轮换：旧token立即失效。
    stub.rotate("Bearer alice-managed", "Bearer alice-managed-2")
    assert await recheck() is False
    fresh = session_stream_recheck(
        Request(request_scope(api, "Bearer alice-managed-2")), "alpha", "s-1"
    )
    assert fresh is not None and await fresh() is True
    # 他人会话：member不能读他人会话的流；admin可以；降member即终止。
    bob_stream = session_stream_recheck(
        Request(request_scope(api, "Bearer bob-managed")), "alpha", "s-1"
    )
    assert bob_stream is not None and await bob_stream() is False
    stub.directory["bob"] = PrincipalContext(
        "bob", 1, (Membership(workspace_id="alpha", role="admin"),)
    )
    promoted = session_stream_recheck(
        Request(request_scope(api, "Bearer bob-managed")), "alpha", "s-1"
    )
    assert promoted is not None and await promoted() is True
    stub.directory["bob"] = directory()["bob"]
    assert await promoted() is False
    # alice自己降member后仍能读自己的会话流（owner视图）。
    stub.directory["alice"] = PrincipalContext(
        "alice", 1, (Membership(workspace_id="alpha", role="member"),)
    )
    assert await fresh() is True
    # 成员移除：认证仍有效但workspace授权撤销。
    stub.directory["alice"] = PrincipalContext("alice", 1, ())
    assert await fresh() is False
    # 停用：认证直接失败。
    stub.blocked.add("alice")
    assert await fresh() is False
    # 未保存authorization的连接（历史会话/直连）在托管模式下立即终止。
    assert await session_stream_recheck(
        Request(request_scope(api, None)), "alpha", "s-1"
    )() is False  # type: ignore[union-attr]
    # legacy模式保持None（原行为）。
    stub.managed = False
    assert session_stream_recheck(Request(request_scope(api, "Bearer x")), "alpha", "s-1") is None


async def test_legacy_stream_recheck_covers_revocation_and_session_access():
    actors = {
        "alice": PrincipalContext(
            "alice", 1, (Membership(workspace_id="alpha", role="admin"),), True
        ),
    }
    stub = StubIdentities(actors, {"Bearer alice-managed": "alice"})
    api = FastAPI()
    snapshot_target = {"run_id": "run-1"}

    async def snapshot(run_id):
        return SimpleNamespace(
            session_context=SimpleNamespace(workspace_id="alpha", session_id="s-1")
            if snapshot_target.get("run_id") == run_id
            else None
        )

    async def get_session(principal, workspace, session_id, **_):
        if session_id == "s-1" and principal.member(workspace):
            return SimpleNamespace(owner_id="alice")
        raise ServiceError("not_found", 404)

    api.state.application = SimpleNamespace(
        identities=stub,
        access_policy=None,
        store=SimpleNamespace(snapshot=snapshot),
        sessions=SimpleNamespace(get=get_session),
    )
    recheck = legacy_stream_recheck(Request(request_scope(api, "Bearer alice-managed")), "run-1")
    assert recheck is not None and await recheck() is True
    stub.rotate("Bearer alice-managed", "Bearer next")
    assert await recheck() is False
    # 非legacy主体不再访问v1流。
    stub.directory["alice"] = PrincipalContext(
        "alice", 1, (Membership(workspace_id="alpha", role="admin"),), False
    )
    fresh = legacy_stream_recheck(Request(request_scope(api, "Bearer next")), "run-1")
    assert fresh is not None and await fresh() is False
    stub.directory["alice"] = actors["alice"]
    revived = legacy_stream_recheck(Request(request_scope(api, "Bearer next")), "run-1")
    assert revived is not None and await revived() is True


async def test_managed_session_stream_terminates_on_rotation(tmp_path, wire, monkeypatch):
    stub = StubIdentities(
        directory(),
        {"Bearer alice-managed": "alice", "Bearer bob-managed": "bob"},
    )
    async with wire.application(
        tmp_path / "managed-stream.sqlite", access_policy=runtime_policy()
    ) as app:
        app.identities = stub
        alice = stub.directory["alice"]
        session = await app.sessions.create(
            alice, "alpha", SessionCreate(request_key="sse", target_key="demo")
        )
        turn = await app.sessions.submit(
            alice,
            "alpha",
            session.id,
            TurnRequest(request_key="sse-turn", text="/add 2 3", expected_revision=0),
        )
        finished = await wait_turn(app.store, turn.id, {"completed"})
        assert finished.run_id is not None

        async def endless(run_id, from_seq=0):
            for index in range(500):
                yield RunEvent(
                    run_id=run_id,
                    seq=from_seq + index,
                    event_id=f"synthetic-{index}",
                    kind="message_update",
                    data={},
                )
                await asyncio.sleep(0.01)

        monkeypatch.setattr(app.service, "stream", endless)
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(16)
        listener.setblocking(False)
        server = Server(Config(create_app_local(app), log_level="warning", access_log=False))
        owner = asyncio.create_task(server.serve(sockets=[listener]), name="runtime-stream-api")
        try:
            async with asyncio.timeout(5):
                while not server.started:
                    if owner.done():
                        await owner
                    await asyncio.sleep(0.01)
            async with httpx.AsyncClient(
                base_url=f"http://127.0.0.1:{listener.getsockname()[1]}",
                trust_env=False,
                timeout=10,
            ) as client:
                collected: list[str] = []

                async def consume():
                    async with client.stream(
                        "GET",
                        f"/v2/workspaces/alpha/sessions/{session.id}"
                        f"/turns/{turn.id}/stream",
                        headers={"Authorization": "Bearer alice-managed"},
                    ) as response:
                        assert response.status_code == 200
                        async for line in response.aiter_lines():
                            collected.append(line)
                            if "unauthorized" in line:
                                return

                reader = asyncio.create_task(consume())
                await wait_until(lambda: len(collected) >= 2)
                # 轮换后旧凭据的持续事件流在发送节点终止。
                stub.rotate("Bearer alice-managed", "Bearer alice-managed-2")
                await asyncio.wait_for(reader, 10)
                assert any("unauthorized" in line for line in collected)
        finally:
            server.should_exit = True
            await asyncio.wait_for(owner, 10)
            listener.close()


def create_app_local(app):
    from krim_agent.api.server import create_app

    return create_app(application=app)


def workspace_profile() -> WorkspaceProfile:
    return WorkspaceProfile(environment={}, max_tools_per_step=1)


def work_frame(actor: str, grant: GrantSnapshot) -> WorkFrame:
    session_id = str(uuid.uuid4())
    return WorkFrame(
        session_id=session_id,
        turn_id=str(uuid.uuid4()),
        actor_id=actor,
        grant=grant,
        original_request=WorkspaceTurnRequest(
            request_key="frame",
            text="t",
            expected_revision=0,
            completion=Completion(),
        ),
        input_revision=ArtifactRevision(
            revision_id=str(uuid.uuid4()),
            session_id=session_id,
            parent_id=None,
            revision=0,
            source_kind="input",
            request_key="frame",
            request_hash="0" * 64,
            manifest=(),
            manifest_hash="0" * 64,
            producer_run_id=None,
            producer_call_id=None,
        ),
        source_refs=(),
        subject={},
        conditions={},
        effective_fingerprint="0" * 64,
        phase="running",
        verification=None,
    )


def binding_loop(operations: WorkspaceOperations, settings: Settings) -> SimpleNamespace:
    """RunBinding.runtime_identities所需的最小loop形状；仅取对象身份。"""
    provider = SimpleNamespace(stream=lambda self=None: None)
    executor = SimpleNamespace(
        registry=SimpleNamespace(),
        policy=demo_policy,
        pre=None,
        post=None,
        middleware=(),
        wrap=None,
        execute=lambda invocation, parameters, context: None,
    )
    verifier = SimpleNamespace(
        verify=lambda *args, **kwargs: None,
        descriptor=lambda: {},
        binding_token=lambda: "token",
        assert_current=lambda: None,
    )
    return SimpleNamespace(
        provider=provider,
        executor=executor,
        settings=settings,
        work=operations,
        _verifier=verifier,
    )


def artifact_stub() -> SimpleNamespace:
    return SimpleNamespace(
        fd=3,
        root=Path("/tmp"),
        _blob=lambda *args: None,
        _stage=lambda *args: None,
        input=lambda *args: None,
        publish=lambda *args: None,
        _owned=lambda *args: None,
    )


def test_workspace_fence_and_dispatch_use_live_authorization(tmp_path):
    stub = StubIdentities(directory(), {"Bearer alice-managed": "alice"})
    operations = WorkspaceOperations(
        None, artifact_stub(), workspace_profile(), identities=stub
    )
    alice = stub.directory["alice"]
    grant = ExecutionAccess(alice, "alpha", str(uuid.uuid4())).grant()
    frame = work_frame("alice", grant)
    assert operations.principal(frame) == alice
    scope = operations.scope(frame, lambda: None, time.monotonic() + 30)
    scope.check()
    # 改名不改变PrincipalContext：围栏继续通过（无generation/membership漂移）。
    scope.check()
    # 轮换：generation与memberships不变，已接受任务继续。
    assert stub.resolve("alice") == alice
    scope.check()
    # 成员降级：下一授权节点按新权限复核，围栏拒绝旧快照。
    stub.directory["alice"] = PrincipalContext(
        "alice", 1, (Membership(workspace_id="alpha", role="viewer"),)
    )
    with pytest.raises(ServiceError, match="target_binding_changed"):
        scope.check()
    # 成员完全移除：授权读取本身失败，同样不放行。
    stub.directory["alice"] = PrincipalContext("alice", 1, ())
    with pytest.raises(ServiceError, match="not_found"):
        scope.check()
    # 停用：实时解析失败，不再放行任何副作用。
    stub.blocked.add("alice")
    with pytest.raises(ServiceError, match="permission_denied"):
        operations.principal(frame)


def test_workspace_legacy_operations_unchanged():
    operations = WorkspaceOperations(None, artifact_stub(), workspace_profile())
    grant = GrantSnapshot(
        actor_id="local",
        generation=1,
        workspace_id="local",
        operations=("read", "write", "exec", "approve"),
    )
    frame = work_frame("local", grant)
    assert operations.principal(frame).principal_id == "local"


def test_runtime_binding_survives_rotation_and_renames(tmp_path):
    stub = StubIdentities(directory(), {"Bearer alice-managed": "alice"})
    settings = Settings(database=tmp_path / "binding.sqlite")
    operations = WorkspaceOperations(
        None, artifact_stub(), workspace_profile(), identities=stub
    )
    loop = binding_loop(operations, settings)
    frozen = RunBinding.runtime_identities(loop)
    # 轮换/改名只改变服务内部状态：稳定引用下绑定指纹不漂移。
    stub.rotate("Bearer alice-managed", "Bearer alice-managed-2")
    stub.directory["alice"] = PrincipalContext(
        "alice", 1, (Membership(workspace_id="alpha", role="admin"),)
    )
    assert RunBinding.runtime_identities(loop) == frozen
    # 替换服务对象属于环境漂移：绑定必须检测到。
    operations.identities = StubIdentities(directory(), {})
    assert RunBinding.runtime_identities(loop) != frozen


def turn_view(owner: str, workspace: str = "alpha") -> TurnView:
    return TurnView(
        id=str(uuid.uuid4()),
        session_id=str(uuid.uuid4()),
        workspace_id=workspace,
        owner_id=owner,
        ordinal=0,
        request_key="queued",
        text="x",
        accepted_revision=0,
        status="queued",
        run_id=None,
        context_fingerprint=None,
        transcript=None,
        code=None,
        created_at=0.0,
        started_at=None,
        finished_at=None,
    )


async def test_managed_dispatch_validates_queue_owner(tmp_path, wire):
    stub = StubIdentities(
        directory(),
        {"Bearer alice-managed": "alice", "Bearer bob-managed": "bob"},
    )
    async with wire.application(
        tmp_path / "dispatch.sqlite", access_policy=runtime_policy()
    ) as app:
        app.sessions.identities = stub
        alice, bob = stub.directory["alice"], stub.directory["bob"]
        # 派发门单元：按owner实时复核，不重放已撤销主体。
        view = turn_view("alice")
        assert app.sessions._dispatch_block(view) is None
        stub.blocked.add("alice")
        assert app.sessions._dispatch_block(view) == "permission_denied"
        stub.blocked.clear()
        app.sessions._revoked.add("alice")
        assert app.sessions._dispatch_block(view) == "permission_denied"
        app.sessions._revoked.discard("alice")
        assert app.sessions._dispatch_block(turn_view("bob")) is None
        # viewer失去写授权：其queued demo/http任务不再派发。
        assert app.sessions._dispatch_block(turn_view("viewer")) == "permission_denied"
        # 入队前实时复核：托管校验拒绝的主体不能再排队。
        session = await app.sessions.create(
            alice, "alpha", SessionCreate(request_key="blocked", target_key="demo")
        )
        stub.validate_failure = True
        with pytest.raises(ServiceError, match="permission_denied"):
            await app.sessions.submit(
                alice,
                "alpha",
                session.id,
                TurnRequest(request_key="blocked-2", text="/add 2 3", expected_revision=0),
            )
        # 统一入口验证：旧快照主体经SDK直调读会话同样拒绝（不只submit）。
        with pytest.raises(ServiceError, match="permission_denied"):
            await app.sessions.get(alice, "alpha", session.id)
        # 解除停用后同一主体恢复正常派发；不影响bob并行完成。
        stub.validate_failure = False
        second = await app.sessions.submit(
            alice,
            "alpha",
            session.id,
            TurnRequest(request_key="blocked-3", text="/add 2 3", expected_revision=0),
        )
        bob_session = await app.sessions.create(
            bob, "alpha", SessionCreate(request_key="bob", target_key="demo")
        )
        bob_turn = await app.sessions.submit(
            bob,
            "alpha",
            bob_session.id,
            TurnRequest(request_key="bob-turn", text="/add 2 3", expected_revision=0),
        )
        assert (await wait_turn(app.store, second.id, {"completed"})).status == "completed"
        assert (await wait_turn(app.store, bob_turn.id, {"completed"})).status == "completed"


async def test_rotation_keeps_accepted_runs_and_invalidates_old_key(tmp_path, wire):
    stub = StubIdentities(directory(), {"Bearer alice-managed": "alice"})
    async with wire.application(
        tmp_path / "rotate.sqlite", access_policy=runtime_policy()
    ) as app:
        app.identities = stub
        alice = stub.directory["alice"]
        session = await app.sessions.create(
            alice, "alpha", SessionCreate(request_key="rotate", target_key="demo")
        )
        turn = await app.sessions.submit(
            alice,
            "alpha",
            session.id,
            TurnRequest(request_key="rotate-hold", text="/hold", expected_revision=0),
        )
        running = await wait_turn(app.store, turn.id, {"running"})
        assert running.run_id is not None
        # 同账号轮换：只换凭据，不改generation/memberships，不触发运行时撤销。
        stub.rotate("Bearer alice-managed", "Bearer alice-rotated")
        await asyncio.sleep(0.2)
        view = await session_sql.get_turn(app.store, turn.id)
        assert view.status == "running" and view.run_id == running.run_id
        assert not app.sessions._revoked
        # 旧密钥立即不能发起新请求；新密钥归属同一账号。
        api = create_app_local(app)
        async with api.router.lifespan_context(api), httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://krim.test"
        ) as client:
            old = await client.get("/v2/me", headers={"Authorization": "Bearer alice-managed"})
            assert old.status_code == 401
            new = await client.get("/v2/me", headers={"Authorization": "Bearer alice-rotated"})
            assert new.status_code == 200
            assert new.json()["principal_id"] == "alice"
        # 已接受任务继续执行直到完成。
        wire.release.set()
        completed = await wait_turn(app.store, turn.id, {"completed"})
        assert completed.run_id == running.run_id


async def test_revoke_principal_cancels_only_target(tmp_path, wire):
    stub = StubIdentities(
        directory(),
        {"Bearer alice-managed": "alice", "Bearer bob-managed": "bob"},
    )
    async with wire.application(
        tmp_path / "revoke.sqlite", access_policy=runtime_policy()
    ) as app:
        app.sessions.identities = stub
        alice, bob = stub.directory["alice"], stub.directory["bob"]
        alice_session = await app.sessions.create(
            alice, "alpha", SessionCreate(request_key="alice", target_key="demo")
        )
        running = await app.sessions.submit(
            alice,
            "alpha",
            alice_session.id,
            TurnRequest(request_key="alice-hold", text="/hold", expected_revision=0),
        )
        running = await wait_turn(app.store, running.id, {"running"})
        assert running.run_id is not None
        revision = (await session_sql.get_session(app.store, alice_session.id)).revision
        queued = await app.sessions.submit(
            alice,
            "alpha",
            alice_session.id,
            TurnRequest(
                request_key="alice-queued",
                text="/add 2 3",
                expected_revision=revision,
            ),
        )
        bob_session = await app.sessions.create(
            bob, "alpha", SessionCreate(request_key="bob", target_key="demo")
        )
        bob_turn = await app.sessions.submit(
            bob,
            "alpha",
            bob_session.id,
            TurnRequest(request_key="bob-hold", text="/hold", expected_revision=0),
        )
        await wait_turn(app.store, bob_turn.id, {"running"})

        result = await app.sessions.revoke_principal("alice")
        # 阻止排队并请求取消该主体全部活跃任务；返回前结果已知。
        assert result["principal_id"] == "alice"
        assert result["cancelled_runs"] == [running.run_id]
        assert result["failed_queued"] == [queued.id]
        cancelled = await wait_turn(app.store, running.id, {"cancelled"})
        assert cancelled.code == "cancelled"
        failed = await session_sql.get_turn(app.store, queued.id)
        assert failed.status == "failed" and failed.code == "permission_denied"
        # 其他主体不受影响：bob的任务仍在执行。
        bob_view = await session_sql.get_turn(app.store, bob_turn.id)
        assert bob_view.status == "running"
        # 幂等：重复撤销不再产生动作。
        again = await app.sessions.revoke_principal("alice")
        assert again["cancelled_runs"] == [] and again["failed_queued"] == []
        # 撤销门仅在收敛期间生效：重新认证的主体立即恢复正常排队派发
        # （重新启用/新签发key场景），不永久拉黑。
        assert "alice" not in app.sessions._revoked
        resumed = await app.sessions.submit(
            alice,
            "alpha",
            alice_session.id,
            TurnRequest(
                request_key="alice-resumed",
                text="/add 2 3",
                expected_revision=(
                    await session_sql.get_session(app.store, alice_session.id)
                ).revision,
            ),
        )
        assert (await wait_turn(app.store, resumed.id, {"completed"})).status == "completed"
        wire.release.set()
        completed = await wait_turn(app.store, bob_turn.id, {"completed"})
        assert completed.status == "completed"


def stub_automation(identities, principals=()):
    product = SimpleNamespace(
        evaluation=SimpleNamespace(repository=SimpleNamespace(closed=False), kernel=None),
        runs=SimpleNamespace(),
        sessions=SimpleNamespace(store=SimpleNamespace(closed=False)),
    )
    return EvaluationAutomation(
        product, principals, clock=lambda: 1_800_000_000.0, poll_interval=0.05,
        identities=identities,
    )


def test_evaluation_requires_catalog_or_managed_service():
    product = SimpleNamespace(
        evaluation=SimpleNamespace(repository=SimpleNamespace(closed=False), kernel=None),
        runs=SimpleNamespace(),
        sessions=SimpleNamespace(store=SimpleNamespace(closed=False)),
    )
    with pytest.raises(ServiceError, match="invalid_input"):
        EvaluationAutomation(product, ())
    managed = stub_automation(StubIdentities(directory(), {}))
    assert managed._directory().keys() == {"alice", "bob", "viewer"}
    legacy = EvaluationAutomation(product, tuple(directory().values()))
    assert legacy._directory().keys() == {"alice", "bob", "viewer"}


async def test_evaluation_trust_checks_use_live_directory():
    stub = StubIdentities(directory(), {})
    auto = stub_automation(stub)
    alice = stub.directory["alice"]
    assert auto._trusted(alice, "alpha") == alice
    # 未知主体与viewer写权限都拒绝。
    viewer = PrincipalContext("viewer", 1, (Membership(workspace_id="alpha", role="viewer"),))
    with pytest.raises(ServiceError, match="permission_denied"):
        auto._trusted(viewer, "alpha", write=True)
    # 旧generation快照被拒绝，不因角色变更自动沿用旧授权。
    stale = PrincipalContext(
        "alice", 2, (Membership(workspace_id="alpha", role="admin"),)
    )
    with pytest.raises(ServiceError, match="permission_denied"):
        auto._trusted(stale, "alpha")
    # 停用主体的调度actor实时失败。
    stub.blocked.add("alice")
    with pytest.raises(ServiceError, match="permission_denied"):
        auto._trusted(alice, "alpha")
    with pytest.raises(ServiceError, match="permission_denied"):
        auto._schedule_actor({"actor": "alice"}, "alpha")
    stub.blocked.clear()
    # 新账号加入目录后立即可见（目录不冻结）。
    stub.directory["carol"] = PrincipalContext(
        "carol", 1, (Membership(workspace_id="beta", role="member"),)
    )
    assert auto._directory().keys() == {"alice", "bob", "carol", "viewer"}
    assert sorted(auto._scopes().values()) == ["alpha", "beta"]
    assert auto._schedule_actor({"actor": "carol"}, "beta") == stub.directory["carol"]


async def test_evaluation_cancel_principal_selects_by_actor():
    stub = StubIdentities(directory(), {})
    auto = stub_automation(stub)
    held_a = asyncio.create_task(asyncio.sleep(3600), name="eval-a")
    held_b = asyncio.create_task(asyncio.sleep(3600), name="eval-b")
    held_c = asyncio.create_task(asyncio.sleep(3600), name="eval-c")
    cancelled: list[str] = []

    async def fake_cancel(task_id):
        cancelled.append(task_id)
        return {"id": task_id}

    async def snapshot(task_id):
        if task_id == "unattributed":
            raise ServiceError("not_found", 404)
        return {
            "request_json": {
                "scope": "scope-" + task_id,
                "schedule": {"schedule_id": task_id},
            }
        }

    async def schedule(scope, schedule_id, *args, **kwargs):
        return {"actor": "bob" if task_actor(scope) == "b" else "alice"}

    def task_actor(scope):
        return scope.rsplit("-", 1)[1]

    auto.evaluation.repository = SimpleNamespace(snapshot=snapshot, schedule=schedule)
    auto.evaluation.kernel = SimpleNamespace(
        tasks={"a": held_a, "b": held_b, "unattributed": held_c}, cancel=fake_cancel
    )
    try:
        result = await auto.cancel_principal("alice")
        assert result == {"principal_id": "alice", "cancelled": ["a"]}
        assert cancelled == ["a"]
        # bob的评测task仍在执行。
        assert not held_b.done()
    finally:
        for task in (held_a, held_b, held_c):
            task.cancel()
        await asyncio.gather(held_a, held_b, held_c, return_exceptions=True)
