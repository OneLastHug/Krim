import asyncio
import hashlib
import json
import shutil
import sys
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr, ValidationError

import krim_agent.tokenizer as tokenizer_module
from krim_agent.agents import AgentDefinition, AgentError, AgentManager
from krim_agent.api import create_app
from krim_agent.app import LocalApp
from krim_agent.application.context import PreCompactDecision
from krim_agent.application.workflow import WorkflowServices
from krim_agent.client import KrimClient
from krim_agent.config import Settings
from krim_agent.context_engine import (
    ContextCompactRequest,
    ContextEngine,
    ContextError,
    ContextOptions,
    ContextRunConfig,
)
from krim_agent.prompt_context import MentionInput, MentionOccurrence
from krim_agent.protocol.domain import (
    ChatRequest,
    EditRequest,
    MessageView,
    RegenerateRequest,
    ServiceError,
)
from krim_agent.providers import (
    ModelText,
    ModelToolCall,
    OpenAICompatibleConfig,
    OpenAICompatibleProvider,
)
from krim_agent.runtime import ChatRuntime
from krim_agent.sdk import ContextBudgetPart, PythonAI, StreamStart
from krim_agent.skills import SkillBatch, SkillDefinition
from krim_agent.tokenizer import MANIFEST_SHA256, RANKS_SHA256, TokenizerError, TokenizerSpec
from krim_agent.tools import ToolContext, ToolRegistry, tool
from krim_agent.workflow import WorkflowEngine, WorkflowGraph, WorkflowNode


def message(index, role, text, details=None):
    return MessageView(
        id=f"m{index}",
        topic_id="topic",
        run_id="run",
        sequence=index,
        role=role,
        content=text,
        details=details or {},
        created_at=datetime(2026, 10, 5, tzinfo=UTC),
    )


@pytest.mark.parametrize(
    "name,text,tokens",
    [
        ("cl100k_base", "Hello, world!", [9906, 11, 1917, 0]),
        ("o200k_base", "Hello, world!", [13225, 11, 2375, 0]),
        ("cl100k_base", "你好世界😀", [57668, 53901, 3574, 244, 98220, 76460, 222]),
        ("o200k_base", "你好世界😀", [177519, 28428, 84083]),
        ("cl100k_base", "<|endoftext|>", [27, 91, 8862, 728, 428, 91, 29]),
        ("o200k_base", "<|endoftext|>", [27, 91, 419, 1440, 919, 91, 29]),
        ("cl100k_base", 'Café\nprint("😀")', [34, 2642, 978, 198, 1374, 446, 76460, 222, 909]),
        ("o200k_base", 'Café\nprint("😀")', [34, 103112, 198, 1598, 568, 84083, 1405]),
    ],
)
def test_fixed_bpe_vectors_and_special_spelling_is_ordinary_text(name, text, tokens):
    spec = TokenizerSpec(name)
    assert spec._encoding.encode_ordinary(text) == tokens
    assert spec.text_tokens(text) == len(tokens)


def test_resources_are_fixed_and_loading_never_downloads(monkeypatch, tmp_path):
    def forbidden(*_args, **_kwargs):
        pytest.fail("离线 tokenizer 尝试下载数据")

    monkeypatch.setattr("requests.get", forbidden)
    monkeypatch.setattr("tiktoken.load.read_file", forbidden)
    monkeypatch.setenv("TIKTOKEN_CACHE_DIR", str(tmp_path / "unused-cache"))
    TokenizerSpec._load.cache_clear()
    resources = Path(tokenizer_module.__file__).parent / "tokenizer_data"
    assert hashlib.sha256((resources / "manifest.json").read_bytes()).hexdigest() == MANIFEST_SHA256
    for name, digest in RANKS_SHA256.items():
        assert hashlib.sha256((resources / f"{name}.tiktoken").read_bytes()).hexdigest() == digest
        assert TokenizerSpec(name).text_tokens("你好世界😀") == (7 if name == "cl100k_base" else 3)
    assert not (tmp_path / "unused-cache").exists()


@pytest.mark.parametrize("name", ["cl100k_base", "o200k_base"])
@pytest.mark.parametrize("damage", ["missing", "ranks", "manifest"])
def test_missing_or_modified_resources_fail_without_fallback(monkeypatch, tmp_path, name, damage):
    resources = Path(tokenizer_module.__file__).parent / "tokenizer_data"
    shutil.copy(resources / "manifest.json", tmp_path / "manifest.json")
    if damage != "missing":
        shutil.copy(resources / f"{name}.tiktoken", tmp_path / f"{name}.tiktoken")
    if damage == "ranks":
        (tmp_path / f"{name}.tiktoken").write_bytes(b"bad-ranks")
    elif damage == "manifest":
        (tmp_path / "manifest.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(tokenizer_module, "files", lambda _package: tmp_path)
    TokenizerSpec._load.cache_clear()
    try:
        with pytest.raises(TokenizerError) as error:
            TokenizerSpec(name)
        assert error.value.code == "tokenizer_unavailable"
    finally:
        TokenizerSpec._load.cache_clear()


def test_approx_compatibility_and_complete_message_projection():
    messages = [
        message(1, "user", "你好世界😀"),
        message(
            2, "assistant", "", {"tool_calls": [{"id": "call", "arguments": {"x": "😀" * 100}}]}
        ),
        message(3, "tool", "结果", {"call_id": "call"}),
    ]
    from krim_agent.context_engine.engine import _chars

    assert TokenizerSpec("approx-v1").count(messages) == (_chars(messages) + 3) // 4
    for name in ("cl100k_base", "o200k_base"):
        spec = TokenizerSpec(name)
        assert spec.count(messages) > spec.count([messages[0], messages[2]]) + 90
        assert spec.count([messages[2]]) > spec.text_tokens("结果")
    with pytest.raises(FrozenInstanceError):
        TokenizerSpec("approx-v1").name = "cl100k_base"


@pytest.mark.parametrize(
    "changes",
    [
        {"context_tokenizer": "approx-v1"},
        {"context_reserved_output_tokens": 0},
        {"context_reserved_output_tokens": 220},
        {"provider_max_output_tokens": 33},
        {"context_tokenizer": "unknown"},
        {"context_reserved_output_tokens": -1},
    ],
)
def test_invalid_model_budget_configuration_is_rejected(changes):
    values = {
        "context_tokenizer": "cl100k_base",
        "model_context_window_tokens": 256,
        "context_reserved_output_tokens": 32,
        **changes,
    }
    with pytest.raises(ValidationError):
        Settings.model_validate(values)


def test_environment_configuration_and_frozen_public_snapshot(monkeypatch):
    for name, value in {
        "CONTEXT_TOKENIZER": "o200k_base",
        "CONTEXT_RESERVED_OUTPUT_TOKENS": "32",
        "MODEL_CONTEXT_WINDOW_TOKENS": "256",
        "MODEL_FRAMING_RESERVE_TOKENS": "16",
        "API_KEY": "private-key",
    }.items():
        monkeypatch.setenv(f"KRIM_AGENT_{name}", value)
    config = ContextRunConfig.from_settings(Settings.from_env())
    assert config.tokenizer == "o200k_base" and config.reserved_output_tokens == 32
    assert "private-key" not in config.model_dump_json()
    with pytest.raises(ValidationError):
        config.tokenizer = "cl100k_base"
    with pytest.raises(ValidationError):
        ContextRunConfig(max_tokens=31)


async def test_missing_encoding_after_configuration_change_leaves_no_queued_run(
    repository, monkeypatch, tmp_path
):
    provider = Recorder()
    runtime = ChatRuntime(repository, provider, ToolRegistry(), Settings())
    topic = await repository.create_topic("missing-encoding")
    runtime.settings.context_tokenizer = "cl100k_base"
    try:
        with monkeypatch.context() as changed:
            changed.setattr(tokenizer_module, "files", lambda _package: tmp_path)
            TokenizerSpec._load.cache_clear()
            with pytest.raises(ServiceError) as error:
                await runtime.start(topic.id, ChatRequest(text="hi", idempotency_key="missing"))
            assert error.value.code == "tokenizer_unavailable" and error.value.status == 503
            assert provider.messages == [] and await repository.list_messages(topic.id) == []
            await repository.assert_topic_idle(topic.id)
            with pytest.raises(ServiceError) as compact_error:
                await runtime.compact(topic.id, ContextCompactRequest())
            assert compact_error.value.code == "tokenizer_unavailable"
            assert not runtime.persistent_context.is_busy(topic.id)
        TokenizerSpec._load.cache_clear()
        run = await runtime.start(topic.id, ChatRequest(text="hi", idempotency_key="missing"))
        assert (await finish(runtime, run)).status == "completed"
    finally:
        TokenizerSpec._load.cache_clear()
        await runtime.close()


def test_output_reserve_and_bpe_keep_tool_groups_complete():
    spec = TokenizerSpec("cl100k_base")
    messages = [
        message(1, "user", "旧问题"),
        message(
            2, "assistant", "", {"tool_calls": [{"id": "call", "name": "lookup", "arguments": {}}]}
        ),
        message(3, "tool", "旧结果" * 100, {"call_id": "call"}),
        message(4, "assistant", "旧答案"),
        message(5, "user", "你好世界😀"),
    ]
    limit = spec.count([messages[-1]]) + 10
    result = ContextEngine(tokenizer=spec).prepare(
        messages,
        ContextOptions(max_tokens=limit, reserved_output_tokens=10),
    )
    assert [item.id for item in result.messages] == ["m5"]
    assert result.tokenizer == "cl100k_base" and result.reserved_output_tokens == 10
    assert result.estimated_tokens + result.reserved_output_tokens == limit
    with pytest.raises(ContextError, match="最新完整"):
        ContextEngine(tokenizer=spec).prepare(
            [messages[-1]],
            ContextOptions(max_tokens=limit - 1, reserved_output_tokens=10),
        )


class Recorder:
    name = "local-budget-recorder"

    def __init__(self):
        self.messages = []

    async def stream(self, messages, _tools):
        self.messages.append(messages)
        yield ModelText(text="ok")


def budget_settings(**changes):
    return Settings.model_validate(
        {
            "context_tokenizer": "cl100k_base",
            "context_reserved_output_tokens": 16,
            "model_context_window_tokens": 256,
            "model_framing_reserve_tokens": 8,
            **changes,
        }
    )


async def finish(runtime, run):
    await asyncio.wait_for(asyncio.shield(runtime.production_task(run.id)), 10)
    return await runtime.repository.get_run(run.id)


@pytest.mark.parametrize("policy", ["never", "auto"])
async def test_over_window_rejects_before_provider_and_persists_stable_error(repository, policy):
    provider = Recorder()
    runtime = ChatRuntime(
        repository, provider, ToolRegistry(), budget_settings(context_policy=policy)
    )
    try:
        topic = await repository.create_topic("over-window")
        run = await runtime.start(topic.id, ChatRequest(text="😀" * 300, idempotency_key="limit"))
        saved = await finish(runtime, run)
        assert saved.status == "failed" and saved.error_code == "model_input_budget"
        assert provider.messages == []
        events = await repository.list_events(run.id)
        assert events[0].data["context_config"]["tokenizer"] == "cl100k_base"
        assert events[-1].data["code"] == "model_input_budget"
        if policy == "never":
            assert events[-2].data["within_limit"] is False
    finally:
        await runtime.close()


@pytest.mark.parametrize("short_by", [0, 1])
async def test_exact_model_window_boundary_with_unicode(repository, short_by):
    provider = Recorder()
    text = "你好世界😀"
    input_tokens = TokenizerSpec("cl100k_base").count([message(1, "user", text)])
    runtime = ChatRuntime(
        repository,
        provider,
        ToolRegistry(),
        budget_settings(
            context_reserved_output_tokens=32,
            model_context_window_tokens=input_tokens + 40 - short_by,
        ),
    )
    try:
        topic = await repository.create_topic("exact-window")
        run = await runtime.start(topic.id, ChatRequest(text=text, idempotency_key="edge"))
        saved = await finish(runtime, run)
        assert saved.status == ("completed" if short_by == 0 else "failed")
        assert len(provider.messages) == (1 if short_by == 0 else 0)
        if short_by == 0:
            budget = (await repository.list_events(run.id))[1].data
            assert budget["required_tokens"] == budget["window_tokens"]
        else:
            assert saved.error_code == "model_input_budget"
    finally:
        await runtime.close()


async def test_deferred_schema_is_budgeted_only_when_activated(repository):
    effects = []

    @tool(
        name="large_lookup",
        description="budget_marker " + "复杂说明" * 600,
        risk="read",
        should_defer=True,
    )
    async def large_lookup() -> str:
        effects.append("executed")
        return "unexpected"

    class Search(Recorder):
        async def stream(self, messages, tools):
            self.messages.append(messages)
            assert "large_lookup" not in [item.name for item in tools]
            yield ModelToolCall(
                id="search", name="tool_search", arguments={"query": "budget_marker"}
            )

    provider, tools = Search(), ToolRegistry()
    tools.register(large_lookup)
    runtime = ChatRuntime(
        repository, provider, tools, budget_settings(model_context_window_tokens=1024)
    )
    try:
        topic = await repository.create_topic("deferred-budget")
        run = await runtime.start(topic.id, ChatRequest(text="search", idempotency_key="search"))
        saved = await finish(runtime, run)
        assert saved.error_code == "model_input_budget" and len(provider.messages) == 1
        assert effects == []
        events = await repository.list_events(run.id)
        assert any(
            isinstance(item.data, dict) and item.data.get("type") == "tool_discovery"
            for item in events
        )
    finally:
        await runtime.close()


async def test_tool_schema_consumes_window_before_any_execution(repository):
    effects = []

    @tool(name="large_schema", description="语义描述" * 300, risk="read")
    async def large_schema(value: str = "x") -> str:
        effects.append(value)
        return value

    tools = ToolRegistry()
    tools.register(large_schema)
    provider = Recorder()
    runtime = ChatRuntime(repository, provider, tools, budget_settings())
    try:
        topic = await repository.create_topic("schema")
        run = await runtime.start(topic.id, ChatRequest(text="hi", idempotency_key="schema"))
        assert (await finish(runtime, run)).error_code == "model_input_budget"
        assert provider.messages == [] and effects == []
    finally:
        await runtime.close()


async def test_precompact_cannot_disable_hard_window(repository):
    provider = Recorder()
    runtime = ChatRuntime(repository, provider, ToolRegistry(), budget_settings())
    runtime.persistent_context.pre_compact = lambda _event: PreCompactDecision(
        options=ContextOptions(policy="never")
    )
    try:
        topic = await repository.create_topic("hook")
        run = await runtime.start(topic.id, ChatRequest(text="😀" * 300, idempotency_key="hook"))
        assert (await finish(runtime, run)).error_code == "model_input_budget"
        assert provider.messages == []
        assert (await repository.list_events(run.id))[-2].data["within_limit"] is False
    finally:
        await runtime.close()


@pytest.mark.parametrize("cap", [None, 64])
async def test_explicit_provider_must_bound_output_by_reserved_tokens(repository, cap):
    calls = []

    def request_seen(request):
        calls.append(request)
        raise AssertionError("无效输出预算不应请求 Provider")

    provider = OpenAICompatibleProvider(
        OpenAICompatibleConfig(
            base_url="http://127.0.0.1/v1", model="local", max_output_tokens=cap
        ),
        transport=httpx.MockTransport(request_seen),
    )
    runtime = ChatRuntime(repository, provider, ToolRegistry(), budget_settings())
    try:
        topic = await repository.create_topic("output-cap")
        run = await runtime.start(topic.id, ChatRequest(text="hi", idempotency_key="cap"))
        assert (await finish(runtime, run)).error_code == "model_output_budget"
        assert calls == []
    finally:
        await runtime.close()
        await provider.close()


async def test_precompact_preserves_output_reserve_in_confirmed_summary(repository):
    provider = Recorder()
    runtime = ChatRuntime(
        repository, provider, ToolRegistry(), budget_settings(model_context_window_tokens=1024)
    )
    try:
        topic = await repository.create_topic("hook-reserve")
        ai = PythonAI(runtime)
        for index in range(3):
            await ai.generate_text(str(index), topic_id=topic.id)
        runtime.persistent_context.pre_compact = lambda _event: PreCompactDecision(
            options=ContextOptions(
                strategy="annotated_compact", max_messages=3, reserved_output_tokens=0
            )
        )
        result = await ai.compact(
            topic.id, ContextCompactRequest(max_messages=3, reserved_output_tokens=24, confirm=True)
        )
        assert (
            result.persisted
            and result.reserved_output_tokens == result.options.reserved_output_tokens == 24
        )
    finally:
        await runtime.close()


@pytest.mark.parametrize("source", ["mention", "skill"])
async def test_reference_and_skill_bodies_are_included_in_hard_window(repository, source):
    provider = Recorder()
    runtime = ChatRuntime(
        repository, provider, ToolRegistry(), budget_settings(model_context_window_tokens=1024)
    )
    try:
        topic = await repository.create_topic("data-budget")
        prompt, mentions = "请看 @文档", []
        if source == "mention":
            ref = await repository.add_mention(
                topic.id, MentionInput(label="文档", content="😀" * 1000)
            )
            mentions = [MentionOccurrence(mention_id=ref.id, start=3, end=6)]
        else:
            await repository.inject_skills(
                topic.id,
                SkillBatch(
                    skills=[
                        SkillDefinition(
                            name="review",
                            description="预算测试",
                            content="😀" * 1000,
                            loading="always",
                        )
                    ]
                ),
            )
        run = await runtime.start(
            topic.id, ChatRequest(text=prompt, mentions=mentions, idempotency_key="body")
        )
        assert (await finish(runtime, run)).error_code == "model_input_budget"
        assert provider.messages == []
    finally:
        await runtime.close()


async def test_bpe_auto_truncation_and_persistent_summary_recheck(repository):
    provider = Recorder()
    runtime = ChatRuntime(
        repository, provider, ToolRegistry(), budget_settings(model_context_window_tokens=80)
    )
    try:
        ai = PythonAI(runtime)
        topic = await repository.create_topic("auto-window")
        for index in range(5):
            result = await ai.generate_text(f"第{index}轮😀", topic_id=topic.id)
            assert result.run.status == "completed"
        assert len(provider.messages[-1]) < 9
        assert provider.messages[-1][-1].content == "第4轮😀"
        runtime.persistent_context.summarizer = lambda _messages: "摘要"
        preview = await ai.compact(topic.id, ContextCompactRequest(max_messages=3, max_tokens=80))
        confirmed = await ai.compact(
            topic.id,
            ContextCompactRequest(
                max_messages=3,
                max_tokens=80,
                confirm=True,
                expected_branch_key=preview.branch_key,
            ),
        )
        assert confirmed.persisted and confirmed.tokenizer == "cl100k_base"
        next_result = await ai.generate_text("下一轮", topic_id=topic.id)
        assert next_result.run.status == "completed"
        assert any(item.details.get("context_summary") for item in provider.messages[-1])
        assert next_result.token_usage is None
    finally:
        await runtime.close()


async def test_sdk_budget_snapshot_survives_midrun_config_change_and_replay(repository):
    entered, release = asyncio.Event(), asyncio.Event()

    class TwoSteps(Recorder):
        async def stream(self, messages, tools):
            self.messages.append(messages)
            if len(self.messages) == 1:
                entered.set()
                await release.wait()
                yield ModelToolCall(id="one", name="echo", arguments={"value": "ok"})
            else:
                yield ModelText(text="done")

    @tool(name="echo", risk="read")
    async def echo(value: str) -> str:
        return value

    tools = ToolRegistry()
    tools.register(echo)
    config = budget_settings(model_context_window_tokens=512, api_key=SecretStr("private-key"))
    provider = TwoSteps()
    runtime = ChatRuntime(repository, provider, tools, config)
    try:
        ai = PythonAI(runtime)
        stream = await ai.stream_text("hi", idempotency_key="snapshot")
        await asyncio.wait_for(entered.wait(), 5)
        config.context_tokenizer = "o200k_base"
        config.model_context_window_tokens = 32
        config.context_reserved_output_tokens = 8
        config.model_framing_reserve_tokens = 0
        release.set()
        result = await stream.collect()
        assert result.run.status == "completed" and result.token_usage is None
        parts = [part async for part in stream.full_stream()]
        budget = [part for part in parts if isinstance(part, ContextBudgetPart)]
        assert len(budget) == 2 and all(
            item.encoding == "cl100k_base" and item.window_tokens == 512 for item in budget
        )
        assert (
            isinstance(parts[0], StreamStart)
            and parts[0].context_config["tokenizer"] == "cl100k_base"
        )
        assert "private-key" not in json.dumps([item.model_dump(mode="json") for item in parts])
        retry = await ai.stream_text("hi", topic_id=result.run.topic_id, idempotency_key="snapshot")
        assert [part async for part in retry.full_stream()] == parts
        assert len(provider.messages) == 2
        reader_provider = Recorder()
        reader = ChatRuntime(repository, reader_provider, ToolRegistry(), Settings())
        try:
            replay = await PythonAI(reader).stream_text(
                "hi",
                topic_id=result.run.topic_id,
                idempotency_key="snapshot",
            )
            assert [part async for part in replay.full_stream()] == parts
            assert reader_provider.messages == []
        finally:
            await reader.close()
        rejected = await ai.generate_text("new", topic_id=result.run.topic_id)
        assert rejected.code == "model_input_budget" and len(provider.messages) == 2
    finally:
        release.set()
        await runtime.close()


async def test_edit_and_regenerate_take_new_context_snapshots(repository):
    provider = Recorder()
    settings = budget_settings()
    runtime = ChatRuntime(repository, provider, ToolRegistry(), settings)
    try:
        topic = await repository.create_topic("new-snapshots")
        run = await runtime.start(topic.id, ChatRequest(text="first", idempotency_key="first"))
        await finish(runtime, run)
        settings.context_tokenizer = "o200k_base"
        edited = await runtime.edit(
            topic.id, run.input_message_id, EditRequest(text="edit", idempotency_key="edit")
        )
        await finish(runtime, edited)
        assistant = (await repository.run_messages(edited.id))[-1]
        regenerated = await runtime.regenerate(
            topic.id, assistant.id, RegenerateRequest(idempotency_key="regen")
        )
        await finish(runtime, regenerated)
        snapshots = [
            (await repository.list_events(item.id))[0].data["context_config"]["tokenizer"]
            for item in (run, edited, regenerated)
        ]
        assert snapshots == ["cl100k_base", "o200k_base", "o200k_base"]
    finally:
        await runtime.close()


async def test_manual_budget_is_preserved_by_http_client_cli_and_host(
    runtime, repository, server_url
):
    runtime.settings.context_tokenizer = "o200k_base"
    runtime.settings.context_reserved_output_tokens = 12
    topic = await repository.create_topic("manual-budget")
    for index in range(3):
        run = await runtime.start(
            topic.id, ChatRequest(text=f"第 {index} 轮", idempotency_key=str(index))
        )
        await finish(runtime, run)

    def client_preview():
        with KrimClient(server_url) as client:
            return client.compact(
                topic.id,
                ContextOptions(
                    strategy="annotated_compact",
                    max_messages=3,
                    max_tokens=4096,
                    reserved_output_tokens=20,
                ),
            )

    preview = await asyncio.to_thread(client_preview)
    assert preview.tokenizer == "o200k_base" and preview.options.reserved_output_tokens == 20
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "krim_agent",
        "compact",
        topic.id,
        "--url",
        server_url,
        "--max-messages",
        "3",
        "--max-tokens",
        "4096",
        "--reserved-output-tokens",
        "10",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
    assert process.returncode == 0, stderr.decode()
    value = json.loads(stdout)
    assert value["tokenizer"] == "o200k_base" and value["options"]["reserved_output_tokens"] == 12
    app = LocalApp(runtime)
    try:
        host = await app.handle.compact(
            topic.id, ContextCompactRequest(max_messages=3, reserved_output_tokens=22)
        )
        assert host.reserved_output_tokens == 22 and host.tokenizer == "o200k_base"
    finally:
        await app.close()


async def test_default_agent_inherits_host_budget_and_rejects_without_provider(repository):
    provider = Recorder()
    runtime = ChatRuntime(repository, provider, ToolRegistry(), budget_settings())
    app = create_app(runtime=runtime)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1",
        ) as client,
    ):
        agent = (await client.post("/v1/agents", json={"name": "default"})).json()
        response = await client.post(
            f"/v1/agents/{agent['agent_id']}/execute", json={"prompt": "😀" * 300}
        )
        assert (
            response.status_code == 200
            and response.json()["result"]["code"] == "model_input_budget"
        )
        assert provider.messages == []


async def test_subagent_intersects_parent_budget_and_binds_definition_fingerprint(repository):
    parent = ContextRunConfig.from_settings(budget_settings())
    child = ContextRunConfig.from_settings(
        budget_settings(model_context_window_tokens=1024, context_reserved_output_tokens=8)
    )
    provider, tools = Recorder(), ToolRegistry()
    definition = AgentDefinition(name="child", provider=provider, tools=tools, context_config=child)
    manager = AgentManager(repository)
    try:
        handle = await manager.spawn(
            definition,
            parent_context=ToolContext(tools, context_config=parent.model_dump(mode="json")),
        )
        assert handle.runtime.settings.model_context_window_tokens == 256
        assert handle.runtime.settings.context_reserved_output_tokens == 16
        result = await handle.execute("😀" * 300)
        assert result.code == "model_input_budget" and provider.messages == []
        assert manager.definition_hash(definition) != manager.definition_hash(
            replace(definition, context_config=parent)
        )
        default = replace(definition, context_config=None)
        assert manager.definition_hash(default) == manager.definition_hash(
            replace(default, context_config=ContextRunConfig())
        )
        with pytest.raises(AgentError) as error:
            await manager.spawn(
                replace(
                    definition, context_config=child.model_copy(update={"tokenizer": "o200k_base"})
                ),
                parent_context=ToolContext(tools, context_config=parent.model_dump(mode="json")),
            )
        assert error.value.code == "agent_context_budget"
    finally:
        await manager.close()


def test_agent_token_budget_with_different_encoding_is_rejected():
    parent = ContextRunConfig(max_tokens=128, tokenizer="approx-v1")
    child = ContextRunConfig(max_tokens=128, tokenizer="cl100k_base")
    with pytest.raises(ValueError, match="tokenizer"):
        child.restricted_by(parent)
    inherited = ContextRunConfig().restricted_by(child)
    assert inherited.tokenizer == "cl100k_base" and inherited.max_tokens == 128


async def test_workflow_model_node_uses_same_hard_budget():
    provider = Recorder()
    async with await PythonAI.create(
        provider=provider, tools=ToolRegistry(), settings=budget_settings()
    ) as ai:
        graph = WorkflowGraph(nodes=[WorkflowNode(id="model", kind="agent")])
        services = WorkflowServices(models={"default": ai})
        result = await WorkflowEngine().execute(graph, "😀" * 300, services.bindings(graph)).wait()
        assert result.status == "failed" and result.code == "model_input_budget"
        assert provider.messages == []
