"""从独立 site-packages 离线验证提案、重开、原摘要确认与 SDK 部件。"""

import asyncio
import json
import socket
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import krim_agent
from krim_agent.config import Settings
from krim_agent.context_engine import ContextCompactRequest, ContextProposalDecision
from krim_agent.protocol.domain import ChatRequest
from krim_agent.providers import ModelText
from krim_agent.sdk import ContextProposalPart, PythonAI
from krim_agent.tools import ToolRegistry

assert "site-packages" in Path(krim_agent.__file__).resolve().parts
assert Path(krim_agent.__file__).resolve().is_relative_to(Path(sys.prefix))


def forbidden(*_args, **_kwargs):
    raise AssertionError("独立提案验证禁止联网")


socket.socket.connect = forbidden


class Provider:
    name = "wheel-proposal"

    def __init__(self):
        self.calls = 0

    async def stream(self, _messages, _tools):
        self.calls += 1
        yield ModelText(text="确认后的离线回答")


async def main():
    with TemporaryDirectory(prefix="krim-proposal-wheel-") as directory:
        settings = Settings(
            database_url=f"sqlite+aiosqlite:///{Path(directory) / 'proposal.sqlite'}",
            context_strategy="annotated_compact",
            context_max_messages=3,
            context_compact_mode="confirmation",
        )
        provider = Provider()
        async with await PythonAI.create(
            settings=settings, provider=provider, tools=ToolRegistry()
        ) as ai:
            topic = await ai.runtime.repository.create_topic("独立提案")
            for index in range(4):
                run, _ = await ai.runtime.repository.begin_run(
                    topic.id, ChatRequest(text=f"历史{index}", idempotency_key=str(index))
                )
                await ai.runtime.repository.add_message(run, "assistant", "历史回答")
                await ai.runtime.repository.finish(run.id, "completed", None, [])
            preview = await ai.compact(topic.id, ContextCompactRequest(max_messages=3))
            assert preview.status == "proposed" and provider.calls == 0
        async with await PythonAI.create(
            settings=settings, provider=provider, tools=ToolRegistry()
        ) as ai:
            pending = await ai.context_proposals(topic.id, pending_only=True)
            assert pending[0].id == preview.proposal_id
            decision = await ai.decide_context_proposal(
                topic.id, pending[0].id, ContextProposalDecision(action="confirm", revision=0)
            )
            assert decision.result.summary == preview.summary
            stream = await ai.stream_text("新的消息", topic_id=topic.id, idempotency_key="one")
            async with asyncio.timeout(10):
                pending = []
                for _ in range(500):
                    pending = await ai.context_proposals(topic.id, pending_only=True)
                    if pending:
                        break
                    await asyncio.sleep(0.02)
            assert provider.calls == 0
            await ai.decide_context_proposal(
                topic.id, pending[0].id, ContextProposalDecision(action="confirm", revision=0)
            )
            assert (await stream.collect()).run.status == "completed"
            parts = [part async for part in stream.full_stream()]
            assert [part.phase for part in parts if isinstance(part, ContextProposalPart)] == [
                "pending",
                "resolved",
            ]
            replay = await ai.stream_text("新的消息", topic_id=topic.id, idempotency_key="one")
            assert (await replay.collect()).run.id == stream.run.id and provider.calls == 1
            print(
                json.dumps(
                    {
                        "package": krim_agent.__file__,
                        "proposal_reopened": True,
                        "provider_calls": provider.calls,
                        "replay_calls": 0,
                    },
                    ensure_ascii=False,
                )
            )


asyncio.run(main())
