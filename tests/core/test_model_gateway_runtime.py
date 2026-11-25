"""每会话自定义模型：创建冻结、真实wire、配置更新后冻结、跨用户与漂移拒绝。"""

import asyncio

import pytest
from fixtures.http_provider_server import HTTPFixture

from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import ServiceError, fingerprint, parse_json
from krim_agent.model_gateway.contracts import GatewaySelection
from krim_agent.model_gateway.runtime import GatewayRuntime
from krim_agent.model_gateway.selection import SessionModelUpdate, set_selection
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage import sessions as session_sql

_LEVELS = {"high": {"effort": "high", "thinking": None}}
_SELECTION = {"gateway_id": "gw", "model_id": "m1", "reasoning_level": "high"}


def _model(model_id: str) -> dict:
    return {
        "id": model_id,
        "display_name": model_id,
        "allow_images": False,
        "max_output_tokens": 512,
        "reasoning_levels": _LEVELS,
    }


def _gateway(models=None, *, base_url: str, gateway_id: str = "gw") -> dict:
    return {
        "gateway_id": gateway_id,
        "name": "Fixture",
        "base_url": base_url,
        "wire_api": "chat_completions",
        "models": [_model("m1")] if models is None else models,
    }


def _update(
    *, expected: int, key: str, base_url: str, gateways=None, default=None
) -> dict:
    return {
        "expected_revision": expected,
        "request_key": key,
        "gateways": [_gateway(base_url=base_url)] if gateways is None else gateways,
        "default_selection": default,
    }


async def _open_app(
    tmp_path, fixture, *, name="gateway-runtime", settings=None, http_kwargs=None
):
    return await open_application(
        tmp_path / f"{name}.sqlite",
        settings=settings,
        http_config=OpenAICompatibleConfig(
            base_url=fixture.url, model="base-model", **(http_kwargs or {})
        ),
    )


async def _create(app, key, *, selection=None):
    return await app.sessions.create(
        LOCAL, "local", SessionCreate(request_key=key, model_selection=selection)
    )


async def _submit(app, session, key, revision):
    turn = await app.sessions.submit(
        LOCAL, "local", session.id,
        TurnRequest(request_key=key, text="你好", expected_revision=revision),
    )
    return await app.sessions.wait(LOCAL, "local", session.id, turn.id)


async def _frozen_target(app, session_id):
    async def read(conn):
        return (await session_sql.session_row(conn, session_id))["target"]

    return await app.store.work_read(read)


def _marker(frozen):
    return frozen["provider"]["config"]["model_gateway"]


def test_session_create_hash_compatibility():
    legacy = {
        "request_key": "k",
        "title": "新对话",
        "target_key": "target-http",
        "context_policy": None,
    }
    body = SessionCreate(request_key="k")
    document = body.model_dump(mode="json")
    document.pop("project_id", None)  # 与storage.create_session保持同一形状
    assert "model_selection" not in document
    assert fingerprint(document) == fingerprint(legacy)
    chosen = SessionCreate(request_key="k", model_selection=dict(_SELECTION))
    explicit = chosen.model_dump(mode="json")
    explicit.pop("project_id", None)
    assert explicit["model_selection"] == _SELECTION
    assert fingerprint(explicit) != fingerprint(legacy)


async def test_create_without_gateway_keeps_host_target_and_rejects_selection(tmp_path):
    async with HTTPFixture() as fixture:
        async with await _open_app(tmp_path, fixture) as app:
            session = await _create(app, "s1")
            frozen = await _frozen_target(app, session.id)
            assert "model_gateway" not in frozen["provider"]["config"]
            view = await _submit(app, session, "t1", 0)
            assert view.status == "completed"
            assert fixture.requests[0]["body"]["model"] == "base-model"
            with pytest.raises(ServiceError) as error:
                await _create(app, "s2", selection=dict(_SELECTION))
            assert error.value.code == "provider_not_configured"


async def test_explicit_selection_freezes_custom_model_and_reaches_wire(tmp_path):
    async with HTTPFixture() as fixture:
        async with await _open_app(tmp_path, fixture) as app:
            await app.model_gateways.update(
                "local", _update(expected=0, key="cfg-1", base_url=fixture.url)
            )
            session = await _create(app, "s1", selection=dict(_SELECTION))
            marker = _marker(await _frozen_target(app, session.id))
            assert marker == {"principal_id": "local", "revision": 1,
                              "selection": dict(_SELECTION)}
            view = await _submit(app, session, "t1", 0)
            assert view.status == "completed"
            body = fixture.requests[0]["body"]
            assert body["model"] == "m1"
            assert body["reasoning_effort"] == "high"
            assert body["max_tokens"] == 512
            # 无选择的会话继续走宿主目标；两绑定互不影响。
            base_session = await _create(app, "s2")
            await _submit(app, base_session, "t2", 0)
            assert fixture.requests[-1]["body"]["model"] == "base-model"


async def test_default_selection_applies_without_explicit_choice(tmp_path):
    async with HTTPFixture() as fixture:
        async with await _open_app(tmp_path, fixture) as app:
            await app.model_gateways.update(
                "local",
                _update(expected=0, key="cfg-1", base_url=fixture.url,
                        default=dict(_SELECTION)),
            )
            session = await _create(app, "s1")
            marker = _marker(await _frozen_target(app, session.id))
            assert marker["selection"] == dict(_SELECTION)
            view = await _submit(app, session, "t1", 0)
            assert view.status == "completed"
            assert fixture.requests[0]["body"]["model"] == "m1"


async def test_configuration_update_keeps_frozen_session_on_old_revision(tmp_path):
    async with HTTPFixture() as fixture:
        async with await _open_app(tmp_path, fixture) as app:
            await app.model_gateways.update(
                "local", _update(expected=0, key="cfg-1", base_url=fixture.url)
            )
            session = await _create(app, "s1", selection=dict(_SELECTION))
            await _submit(app, session, "t1", 0)
            await app.model_gateways.update(
                "local",
                _update(expected=1, key="cfg-2", base_url=fixture.url,
                        gateways=[_gateway([_model("m2")], base_url=fixture.url)]),
            )
            # 旧会话按冻结revision重建：模型已从当前目录移除仍可继续。
            view = await _submit(app, session, "t2", 1)
            assert view.status == "completed"
            assert fixture.requests[-1]["body"]["model"] == "m1"
            frozen = await _frozen_target(app, session.id)
            assert _marker(frozen)["revision"] == 1
            assert "model_gateway" in frozen["provider"]["config"]
            # 新会话用已移除的模型：按当前目录拒绝。
            with pytest.raises(ServiceError) as error:
                await _create(app, "s2", selection=dict(_SELECTION))
            assert error.value.code == "provider_config"


async def test_cross_principal_selection_and_owner_mismatch_rejected(tmp_path):
    own = {"gateway_id": "own", "model_id": "m1", "reasoning_level": "high"}
    async with HTTPFixture() as fixture:
        async with await _open_app(tmp_path, fixture) as app:
            await _create(app, "s0")  # local无网关文档
            with pytest.raises(ServiceError) as error:
                await _create(app, "s1", selection=dict(_SELECTION))
            assert error.value.code == "provider_not_configured"
            await app.model_gateways.update(
                "other-user", _update(expected=0, key="cfg-x", base_url=fixture.url)
            )
            # local仍无文档：他人网关不可见，等同未配置。
            with pytest.raises(ServiceError) as error:
                await _create(app, "s2", selection=dict(_SELECTION))
            assert error.value.code == "provider_not_configured"
            # local有自己的目录后再选他人gateway_id：按当前主体目录解析拒绝。
            await app.model_gateways.update(
                "local",
                _update(
                    expected=0, key="cfg-1", base_url=fixture.url,
                    gateways=[_gateway(base_url=fixture.url, gateway_id="own")],
                ),
            )
            with pytest.raises(ServiceError) as error:
                await _create(app, "s4", selection=dict(_SELECTION))
            assert error.value.code == "provider_config"
            # 历史重建的owner校验：标记主体与会话owner不一致即拒绝。
            chosen = await _create(app, "s3", selection=dict(own))
            custom = await _frozen_target(app, chosen.id)
            runtime = app.service.model_gateway
            assert isinstance(runtime, GatewayRuntime)
            with pytest.raises(ServiceError) as error:
                await runtime.for_target("target-http", custom, owner_id="other-user")
            assert error.value.code == "permission_denied"


async def test_unsupported_reasoning_level_rejected(tmp_path):
    async with HTTPFixture() as fixture:
        async with await _open_app(tmp_path, fixture) as app:
            await app.model_gateways.update(
                "local", _update(expected=0, key="cfg-1", base_url=fixture.url)
            )
            with pytest.raises(ServiceError) as error:
                await _create(app, "s1", selection={
                    "gateway_id": "gw", "model_id": "m1", "reasoning_level": "instant",
                })
            assert error.value.code == "provider_config"


async def test_descriptor_drift_rejects_rebuild(tmp_path):
    async with HTTPFixture() as fixture:
        async with await _open_app(tmp_path, fixture) as app:
            await app.model_gateways.update(
                "local", _update(expected=0, key="cfg-1", base_url=fixture.url)
            )
            session = await _create(app, "s1", selection=dict(_SELECTION))
            frozen = await _frozen_target(app, session.id)
            runtime = app.service.model_gateway
            drifted = {**frozen, "tools": []}
            with pytest.raises(ServiceError) as error:
                await runtime.for_target("target-http", drifted, owner_id="local")
            assert error.value.code == "target_binding_changed"
            # 一致目标仍按descriptor指纹命中缓存，provider数量不因漂移增长。
            binding = await runtime.for_target("target-http", frozen, owner_id="local")
            assert fingerprint(parse_json(binding.descriptor_json)) == fingerprint(frozen)
            assert len(runtime._providers) == 1


async def test_reopen_rebuilds_frozen_binding_for_old_session(tmp_path):
    async with HTTPFixture() as fixture:
        app = await _open_app(tmp_path, fixture)
        async with app:
            await app.model_gateways.update(
                "local", _update(expected=0, key="cfg-1", base_url=fixture.url)
            )
            session = await _create(app, "s1", selection=dict(_SELECTION))
            await _submit(app, session, "t1", 0)
        # 全新GatewayRuntime（缓存为空）按冻结revision重建：descriptor须逐字节一致。
        async with await _open_app(tmp_path, fixture) as reopened:
            runtime = reopened.service.model_gateway
            assert not runtime._providers
            view = await _submit(reopened, session, "t2", 1)
            assert view.status == "completed"
            assert fixture.requests[-1]["body"]["model"] == "m1"
            frozen = await _frozen_target(reopened, session.id)
            binding = await runtime.for_target("target-http", frozen, owner_id="local")
            assert fingerprint(parse_json(binding.descriptor_json)) == fingerprint(frozen)


async def test_custom_config_inherits_host_transport_caps(tmp_path):
    async with HTTPFixture() as fixture:
        async with await _open_app(
            tmp_path,
            fixture,
            name="gateway-runtime-caps",
            settings=Settings(max_tools_per_step=2),
            http_kwargs={"connect_timeout_seconds": 4.0, "max_request_bytes": 8192},
        ) as app:
            await app.model_gateways.update(
                "local", _update(expected=0, key="cfg-1", base_url=fixture.url)
            )
            session = await _create(app, "s1", selection=dict(_SELECTION))
            frozen = await _frozen_target(app, session.id)
            binding = await app.service.model_gateway.for_target(
                "target-http", frozen, owner_id="local"
            )
            http = parse_json(binding.descriptor_json)["limits"]["http"]
            # 宿主传输/安全上限继承（更严者生效），不可被custom配置绕过：
            assert http["max_tool_calls"] == 2
            assert http["max_request_bytes"] == 8192
            assert http["connect_timeout_seconds"] == 4.0
            # 模型语义字段按网关声明，不继承base模型配置。
            assert http["max_output_tokens"] == 512
            assert "allow_images" not in http


async def test_set_selection_rejects_busy_session_and_switches_when_idle(tmp_path):
    async with HTTPFixture() as fixture:
        async with await _open_app(tmp_path, fixture) as app:
            await app.model_gateways.update(
                "local",
                _update(
                    expected=0, key="cfg-1", base_url=fixture.url,
                    gateways=[_gateway([_model("m1"), _model("m2")], base_url=fixture.url)],
                ),
            )
            session = await _create(app, "s1", selection=dict(_SELECTION))
            view = await _submit(app, session, "t1", 0)
            assert view.status == "completed"
            assert fixture.requests[0]["body"]["model"] == "m1"
            # 会话忙（running）时拒绝切换模型：请求到达fixture即处于running。
            running = await app.sessions.submit(
                LOCAL, "local", session.id,
                TurnRequest(request_key="t2", text="FIXTURE_HOLD", expected_revision=1),
            )
            async with asyncio.timeout(5):
                await fixture.entered.wait()
            update = SessionModelUpdate(
                request_key="sel-1", expected_revision=2,
                model_selection=GatewaySelection(gateway_id="gw", model_id="m2"),
            )
            with pytest.raises(ServiceError) as error:
                await set_selection(app, LOCAL, "local", session.id, update)
            assert error.value.code == "session_busy"
            # 当前轮结束后空闲切换生效；后续turn走新模型，且descriptor带新标记。
            fixture.release.set()
            finished = await app.sessions.wait(LOCAL, "local", session.id, running.id)
            assert finished.status == "completed"
            switched = await set_selection(app, LOCAL, "local", session.id, update)
            assert switched["model"] == "m2" and switched["revision"] == 3
            marker = _marker(await _frozen_target(app, session.id))
            assert marker["selection"] == {
                "gateway_id": "gw", "model_id": "m2", "reasoning_level": None,
            }
            third = await _submit(app, session, "t3", 3)
            assert third.status == "completed"
            assert fixture.requests[-1]["body"]["model"] == "m2"


async def test_close_releases_custom_providers(tmp_path):
    async with HTTPFixture() as fixture:
        app = await _open_app(tmp_path, fixture)
        async with app:
            await app.model_gateways.update(
                "local", _update(expected=0, key="cfg-1", base_url=fixture.url)
            )
            session = await _create(app, "s1", selection=dict(_SELECTION))
            runtime = app.service.model_gateway
            providers = tuple(runtime._providers)
            assert providers and all(not provider.closed for provider in providers)
            view = await _submit(app, session, "t1", 0)
            assert view.status == "completed"
        assert runtime.closed
        assert not runtime._providers
        assert all(provider.closed for provider in providers)
