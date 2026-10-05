"""真实独立 Python 进程预览/读取/确认提案；仅使用传入临时 SQLite。"""

import asyncio
import json
import sys
from uuid import uuid4

from krim_agent.application.context import PersistentContext
from krim_agent.context_engine import ContextEngine, ContextOptions, ContextProposalDecision
from krim_agent.persistence import Database, KrimRepository
from krim_agent.protocol.domain import ChatRequest


async def main():
    database = Database(sys.argv[1])
    await database.initialize()
    repository = KrimRepository(database)
    service = PersistentContext(repository, ContextEngine(), lambda _messages: "真实进程保存的摘要")
    try:
        if sys.argv[2] == "preview":
            topic = await repository.create_topic("真实进程")
            for index in range(4):
                run, _ = await repository.begin_run(
                    topic.id, ChatRequest(text=f"历史{index}", idempotency_key=str(uuid4()))
                )
                await repository.add_message(run, "assistant", "进程回答")
                await repository.finish(run.id, "completed", None, [])
            result = await service.compact(
                topic.id,
                ContextOptions(strategy="annotated_compact", max_messages=3),
                config_hash="process-config",
            )
            print(
                json.dumps(
                    {
                        "topic_id": topic.id,
                        "proposal_id": result.proposal_id,
                        "summary": result.result.summary,
                    },
                    ensure_ascii=False,
                )
            )
        else:
            topic_id, proposal_id = sys.argv[3:5]
            service.summarizer = lambda _messages: (_ for _ in ()).throw(
                AssertionError("确认不能重新生成摘要")
            )
            result = await service.decide(
                topic_id,
                proposal_id,
                ContextProposalDecision(action="confirm", revision=0),
                config_hash="process-config",
            )
            print(
                json.dumps(
                    {
                        "status": result.status,
                        "revision": result.revision,
                        "summary": result.result.summary,
                    },
                    ensure_ascii=False,
                )
            )
    finally:
        await database.close()


asyncio.run(main())
