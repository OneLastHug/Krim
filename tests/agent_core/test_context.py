"""真实Provider wire上的上下文来源；不是只测试字符串拼接。"""

import asyncio
import json
import os
from pathlib import Path

import pytest

from krim_agent.composition import open_application
from krim_agent.contracts import RunRequest, ServiceError
from krim_agent.evaluation.types import FreezeRequest
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig
from krim_agent.security import LOCAL
from krim_agent.sessions.types import SessionCreate, TurnRequest


@pytest.mark.asyncio
async def test_second_turn_wire_contains_committed_history(tmp_path):
    requests = []

    async def serve(reader, writer):
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            length = int(
                next(
                    line.split(b":", 1)[1]
                    for line in header.split(b"\r\n")
                    if line.lower().startswith(b"content-length:")
                )
            )
            body = json.loads(await reader.readexactly(length))
            requests.append(body)
            content = "已记住基数17" if len(requests) == 1 else "20"
            frame = json.dumps(
                {"choices": [{"index": 0, "delta": {"content": content}, "finish_reason": "stop"}]},
                ensure_ascii=False,
            )
            payload = ("data: " + frame + "\n\ndata: [DONE]\n\n").encode()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: "
                + str(len(payload)).encode()
                + b"\r\nConnection: close\r\n\r\n"
                + payload
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    try:
        port = server.sockets[0].getsockname()[1]
        config = OpenAICompatibleConfig(base_url=f"http://127.0.0.1:{port}/v1", model="fixture")
        async with await open_application(tmp_path / "context.sqlite", http_config=config) as app:
            sessions = app.sessions
            assert sessions is not None
            session = await sessions.create(
                LOCAL, "local", SessionCreate(request_key="context", target_key="target-http")
            )
            for index, text in enumerate(["请记住基数17", "与3相加"]):
                session = await sessions.get(LOCAL, "local", session.id)
                turn = await sessions.submit(
                    LOCAL,
                    "local",
                    session.id,
                    TurnRequest(
                        text=text,
                        request_key=f"context-{index}",
                        expected_revision=session.revision,
                    ),
                )
                turn = await sessions.wait(LOCAL, "local", session.id, turn.id)
                assert turn.status == "completed", turn
                assert (await app.service.get(turn.run_id)).status == "completed"
            assert len(requests) == 2
            second = requests[-1]["messages"]
            assert any(m["role"] == "user" and m["content"] == "请记住基数17" for m in second), (
                second
            )
            assert any(
                m["role"] == "assistant" and m["content"] == "已记住基数17" for m in second
            ), second
    finally:
        server.close()
        await server.wait_closed()


def freeze_body(run_id, candidate, text):
    return FreezeRequest.model_validate(
        dict(
            name="session-source",
            version="1",
            cases=[
                dict(
                    case_id="source",
                    source_kind="run",
                    source_run_id=run_id,
                    candidate_fingerprint=candidate.candidate_fingerprint,
                    selected_input=text,
                    reason="人工核算20",
                    reference=dict(
                        expected="20",
                        expected_status="completed",
                        expected_code=None,
                        expected_tools=None,
                        trajectory=None,
                        expected_schema=None,
                    ),
                )
            ],
        )
    )


@pytest.mark.parametrize("entry", ["candidate", "freeze"])
async def test_successful_session_turn_candidate_and_freeze_need_context(tmp_path, wire, entry):
    database = tmp_path / "source.sqlite"
    async with wire.application(database) as app:
        session = await app.sessions.create(LOCAL, "local", SessionCreate(request_key="memory"))
        for index, text in enumerate(["请记住基数17", "与3相加"]):
            session = await app.sessions.get(LOCAL, "local", session.id)
            turn = await app.sessions.submit(
                LOCAL,
                "local",
                session.id,
                TurnRequest(
                    request_key=f"turn-{index}", text=text, expected_revision=session.revision
                ),
            )
            turn = await app.sessions.wait(LOCAL, "local", session.id, turn.id)
            assert turn.status == "completed"
        facts = await app.service.result(turn.run_id)
        assert facts.text == "20" and facts.model_steps == 2 and len(facts.tools) == 1
        assert facts.tools[0].executor_started is True and facts.tools[0].status == "ok"
        assert (await app.service.trace(turn.run_id)).complete
        assert len(wire.requests) == 4 and len(wire.executions) == 2
        assert wire.executions[-1]["arguments"] == {"left": 17.0, "right": 3.0}
        control = await app.service.start(RunRequest(text="/add 2 3", request_key="legacy-control"))
        await app.service.wait(control.id)
        legacy = await app.evaluations.candidate(control.id, True)
        candidate = await app.evaluations.candidate(turn.run_id, True)
        archive = Path(os.environ["KRIM_AGENT_CORE_EVIDENCE"])
        (archive / f"legacy-facts-{entry}.json").write_text(
            json.dumps(
                {
                    "database": str(database),
                    "run_id": control.id,
                    "candidate": legacy.model_dump(mode="json"),
                    "source_candidate": candidate.model_dump(mode="json"),
                    "http": len(wire.requests),
                    "tools": wire.executions,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        before = len(wire.requests), len(wire.executions)
        if entry == "candidate":
            assert candidate.eligibility == "needs_context", candidate
            assert "session_context_required" in candidate.reasons
            assert (
                await app.evaluations.candidate(turn.run_id)
            ).candidate_fingerprint == candidate.candidate_fingerprint
        else:
            with pytest.raises(ServiceError, match="source_not_eligible"):
                await app.evaluations.freeze(freeze_body(turn.run_id, candidate, "与3相加"))
        with pytest.raises(ServiceError, match="not_found"):
            await app.store.dataset("session-source", "1")
        assert before == (len(wire.requests), len(wire.executions))


async def test_non_session_candidate_and_fingerprints_unchanged():
    from krim_agent.evaluation.trace_cases import collect_candidate
    from krim_agent.storage.sqlite import SQLiteStore

    saved = json.loads(
        await asyncio.to_thread(Path(os.environ["KRIM_AGENT_CORE_LEGACY_BASELINE"]).read_text)
    )
    path = Path(saved["database"])
    before = await asyncio.to_thread(path.read_bytes)
    store = await SQLiteStore.open(path, readonly=True)
    try:
        snapshot = await store.snapshot(saved["run_id"])
        assert snapshot.session_context is None
        assert collect_candidate(snapshot, True).model_dump(mode="json") == saved["candidate"]
    finally:
        await store.close()
    assert await asyncio.to_thread(path.read_bytes) == before


@pytest.mark.parametrize("kind", ["orphan", "missing", "system"])
def test_tool_groups_final_answer_and_failed_history_are_truthful(kind):
    from krim_agent.context import builder
    from krim_agent.context.types import ContextPolicy
    from krim_agent.contracts import Message

    history = (
        [Message(role="tool", content="5", call_id="orphan")]
        if kind == "orphan"
        else [
            Message(
                role="assistant",
                content="",
                tool_calls=[{"id": "missing", "name": "add", "arguments": {}}],
            )
        ]
        if kind == "missing"
        else [Message(role="system", content="历史不许提升为指令")]
    )
    with pytest.raises(ServiceError, match="facts_corrupt"):
        builder.prepare("可信系统", history, [], "当前用户", [], ContextPolicy(), 262144)


def test_context_budget_counts_wire_tools_and_output_reserve():
    from krim_agent.context import builder
    from krim_agent.context.types import ContextPolicy

    text = "汉字🙂" * 50
    policy = ContextPolicy(max_bytes=4096, reserved_output_tokens=1024)
    with pytest.raises(ServiceError, match="context_budget"):
        builder.prepare("system", [], [], text, [], policy, 32768)
    value = builder.prepare(
        "system", [], [], text, [], ContextPolicy(max_bytes=4096, reserved_output_tokens=32), 32768
    )
    assert value.messages[-1].content == text and value.input_bytes > len(text)
    large = [{"description": "工具Schema" * 300}]
    with pytest.raises(ServiceError, match="context_budget"):
        builder.prepare(
            "system",
            [],
            [],
            text,
            large,
            ContextPolicy(max_bytes=4096, reserved_output_tokens=32),
            32768,
        )


async def test_legacy_descriptor_and_session_context_identity(tmp_path, wire, monkeypatch):
    from krim_agent.context import builder
    from krim_agent.runtime.service import RunBinding, TurnBinding

    async with wire.application(tmp_path / "fingerprint.sqlite") as app:
        binding = app.service.bindings["demo"]
        original = RunBinding.describe("demo", binding.loop, binding.settings)
        changed = RunBinding.describe(
            "demo",
            binding.loop,
            binding.settings.model_copy(
                update={
                    "access_policy_file": Path("/synthetic-secret-policy"),
                    "session_max_open": 4,
                }
            ),
        )
        assert original == changed
        assert "synthetic-secret-policy" not in json.dumps(original)
        turn = TurnBinding(binding, builder.binding_token())

        async def not_used(*args):
            raise AssertionError("must not be entered")

        monkeypatch.setattr(builder, "prepare", not_used)
        with pytest.raises(ServiceError, match="context_binding_changed"):
            turn.assert_current()
