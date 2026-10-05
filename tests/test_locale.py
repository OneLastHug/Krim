import asyncio
import re
from pathlib import Path

import httpx
import pytest
from alembic import command
from pydantic import ValidationError
from sqlalchemy import inspect, text

from krim_agent.api import create_app
from krim_agent.app import LocalApp
from krim_agent.app_sdk import RemoteHostHandle
from krim_agent.client import KrimClient
from krim_agent.locale import LocaleChange, normalize_locale, resolve_locale
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.migration import migration_config
from krim_agent.protocol.domain import ChatRequest, TopicCreate
from krim_agent.ui_locale import locale_resources


@pytest.mark.parametrize(
    ("raw", "normalized", "effective"),
    [
        ("en-us", "en-US", "en-US"),
        ("zh_Hant_TW", "zh-Hant-TW", "zh-CN"),
        ("en-GB", "en-GB", "en-US"),
        ("fr-FR", "fr-FR", "zh-CN"),
        ("en", "en", "en-US"),
        ("zh", "zh", "zh-CN"),
        ("i-klingon", "tlh", "zh-CN"),
        ("en-US-u-ca-gregory", "en-US-u-ca-gregory", "en-US"),
        ("zh-CN-x-local", "zh-CN-x-local", "zh-CN"),
        ("x-local", "x-local", "zh-CN"),
        ("en-US-a-local-u-ca-gregory-x-test", "en-US-a-local-u-ca-gregory-x-test", "en-US"),
    ],
)
def test_normalization_and_fallback_are_valid_and_deterministic(raw, normalized, effective):
    assert normalize_locale(raw) == normalized
    result = resolve_locale(raw)
    assert result.requested == normalized and result.effective == effective
    assert result == resolve_locale(raw)
    assert result.fallback_chain[0] == normalized
    assert result.fallback_chain.count(effective) == 1
    assert len(result.fallback_chain) == len(set(result.fallback_chain))
    for candidate in result.fallback_chain:
        assert normalize_locale(candidate) == candidate


@pytest.mark.parametrize(
    "raw", ["", "  ", "x", "en-u", "en-a", "en-US-x", "en--US", "123", "en-123", "<script>"]
)
def test_invalid_tags_are_rejected_before_topic_creation(raw):
    with pytest.raises(ValueError):
        normalize_locale(raw)
    with pytest.raises(ValidationError):
        LocaleChange(locale=raw)
    with pytest.raises(ValidationError):
        TopicCreate(locale=raw)


def test_resources_and_default_preserve_placeholders_and_all_declared_ui_keys():
    assert resolve_locale(None).effective == "zh-CN"
    assert TopicCreate().locale == "zh-CN"
    chinese = locale_resources("zh-CN")
    english = locale_resources("en-GB")
    assert chinese.strings.keys() == english.strings.keys()
    for key, value in english.strings.items():
        assert value and chinese.strings[key] == key
        assert re.findall(r"\{([a-z_]+)\}", key) == re.findall(r"\{([a-z_]+)\}", value)
    assert english.strings["发送"] == "Send"
    declared = re.findall(
        r'data-i18n(?:-(?:placeholder|title|aria-label))?="([^"]+)"',
        Path("frontend/index.html").read_text(),
    )
    assert declared and not set(declared).difference(chinese.strings)


async def test_api_topic_locale_resources_validation_and_missing_topic(api):
    assert (await api.get("/v1/locales")).json()["effective"] == "zh-CN"
    assert (await api.get("/v1/topics")).json()["topics"] == []
    topic = (await api.post("/v1/topics", json={"title": "用户标题", "locale": "en-gb"})).json()
    assert topic["locale"] == "en-US" and topic["title"] == "用户标题"
    bundle = (await api.get("/v1/locales", params={"locale": "en-GB"})).json()
    assert bundle["effective"] == "en-US" and bundle["strings"]["发送"] == "Send"
    updated = await api.patch(f"/v1/topics/{topic['id']}/locale", json={"locale": "fr-FR"})
    assert updated.json() == {
        "requested": "fr-FR",
        "effective": "zh-CN",
        "fallback_chain": ["fr-FR", "fr", "zh-CN"],
    }
    tree = (await api.get(f"/v1/topics/{topic['id']}/tree")).json()
    assert tree["topic"]["locale"] == "zh-CN"
    assert (await api.post("/v1/topics", json={"locale": "en-u"})).status_code == 422
    assert (await api.get("/v1/locales", params={"locale": "en-u"})).status_code == 422
    assert (
        await api.patch(f"/v1/topics/{topic['id']}/locale", json={"locale": "en-u"})
    ).status_code == 422
    assert (
        await api.patch("/v1/topics/missing/locale", json={"locale": "en-US"})
    ).status_code == 404
    assert len((await api.get("/v1/topics")).json()["topics"]) == 1


async def test_locale_does_not_change_model_content_or_idempotent_chat(runtime):
    topic = await runtime.repository.create_topic("用户标题", "en-GB")
    request = ChatRequest(text="/echo 用户原文", idempotency_key="same-input")
    run = await runtime.start(topic.id, request)
    task = runtime.production_task(run.id)
    assert task is not None
    await asyncio.wait_for(asyncio.shield(task), 5)
    before = await runtime.repository.run_messages(run.id)
    await runtime.repository.set_topic_locale(topic.id, "zh-CN")
    again = await runtime.start(topic.id, request)
    assert again.id == run.id
    assert await runtime.repository.run_messages(run.id) == before
    assert before[0].content == "/echo 用户原文"
    assert "用户原文" in before[-1].content


async def test_local_and_remote_python_handles_use_authoritative_topic_locale(runtime):
    local = LocalApp(runtime)
    app = create_app(runtime=runtime)
    try:
        async with app.router.lifespan_context(app):
            topic = await local.handle.create_topic("宿主用户标题", locale="en-gb")
            assert topic.locale == "en-US"
            result = await local.handle.set_topic_locale(topic.id, "zh_Hant_TW")
            assert result.requested == "zh-Hant-TW" and result.effective == "zh-CN"
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
            ) as api:
                sid = (await api.post("/v1/host/sessions")).json()["id"]
                async with RemoteHostHandle(
                    "http://127.0.0.1", sid, transport=httpx.ASGITransport(app=app)
                ) as host:
                    assert (await host.set_topic_locale(topic.id, "en-US")).effective == "en-US"
            assert (await local.handle.tree(topic.id)).topic.locale == "en-US"
    finally:
        await local.close()


async def test_real_tcp_python_client_creation_change_and_read(server_url):
    def scenario():
        with KrimClient(server_url) as client:
            topic = client.create_topic("TCP 用户标题", locale="en-gb")
            assert topic.locale == "en-US"
            changed = client.set_topic_locale(topic.id, "fr-FR")
            assert changed.effective == "zh-CN"
            assert client.tree(topic.id).topic.locale == "zh-CN"
            assert client.topics()[0].title == "TCP 用户标题"

    await asyncio.to_thread(scenario)


async def test_locale_reopen_and_guarded_downgrade(tmp_path):
    url = f"sqlite+aiosqlite:///{tmp_path / 'locale.sqlite'}"
    database = Database(url)
    await database.initialize()
    topic = await KrimRepository(database).create_topic("用户原文", "en-GB")
    await database.close()
    reopened = Database(url)
    await reopened.initialize()
    repository = KrimRepository(reopened)
    try:
        assert (await repository.get_topic(topic.id)).locale == "en-US"
        with pytest.raises(RuntimeError, match="Locale"):
            async with reopened.engine.begin() as connection:
                await connection.run_sync(
                    lambda sync: command.downgrade(migration_config(sync), "0010_prompt_context")
                )
        assert (await repository.get_topic(topic.id)).locale == "en-US"
        async with reopened.engine.connect() as connection:
            assert (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar() == "0019_trace_spans"
        await repository.set_topic_locale(topic.id, "zh-CN")
        async with reopened.engine.begin() as connection:
            await connection.run_sync(
                lambda sync: command.downgrade(migration_config(sync), "0010_prompt_context")
            )
            columns = await connection.run_sync(lambda sync: inspect(sync).get_columns("topics"))
            assert "locale" not in {column["name"] for column in columns}
        await reopened.initialize()
        restored = await repository.get_topic(topic.id)
        assert restored.title == "用户原文" and restored.locale == "zh-CN"
    finally:
        await reopened.close()
