import asyncio

import pytest

from krim_agent.composition import open_application
from krim_agent.contracts import ApprovalReply, RunRequest, parse_json
from krim_agent.providers.types import ModelText, ModelToolCall


class NoteModel:
    mode = "note-result-fixture"

    def __init__(self, key):
        self.key = key
        self.observed_results = []
        self.calls = 0

    async def stream(self, messages, tools):
        self.calls += 1
        if messages[-1].role == "tool":
            self.observed_results.append(parse_json(messages[-1].content))
            yield ModelText(text="已读取查询结果，可以继续处理。")
        else:
            yield ModelToolCall(id="note-call", name="lookup_note", arguments={"key": self.key})

    async def close(self):
        pass


@pytest.mark.parametrize(
    ("key", "expected"),
    [("context", {"found": False, "key": "context"}),
     ("welcome", "这是需要本次批准的合成演示资料。")],
)
async def test_model_receives_note_hit_or_miss_and_run_continues(tmp_path, key, expected):
    provider = NoteModel(key)
    async with await open_application(tmp_path / "note-runtime.sqlite", provider=provider) as app:
        run = await app.service.start(RunRequest(text="查询示例笔记", request_key="note-runtime"))
        async with asyncio.timeout(3):
            while True:
                pending = await app.service.approvals(run.id)
                if pending:
                    break
                await asyncio.sleep(0.005)
        request = pending[0]
        reply = ApprovalReply(
            request_key="approve-note",
            call_id=request["call_id"],
            arguments_hash=request["arguments_hash"],
            tool_hash=request["tool_hash"],
            allow=True,
        )
        await app.service.reply_approval(run.id, request["request_id"], reply)
        await app.service.wait(run.id)
        result = await app.service.result(run.id)
        assert result.run.status == "completed" and result.run.code is None
        assert result.text == "已读取查询结果，可以继续处理。"
        assert provider.calls == 2 and provider.observed_results == [expected]
        assert len(result.tools) == 1
        observation = result.tools[0]
        assert observation.status == "ok" and observation.code is None
        assert observation.executor_started is True and observation.result_available is True
        assert observation.value == expected
