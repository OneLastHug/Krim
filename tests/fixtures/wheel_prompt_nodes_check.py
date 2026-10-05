"""独立 wheel Prompt 节点/Unicode/Mention/Skill/PythonAI 快照验证。"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import krim_agent
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.prompt_context import MentionInput, MentionOccurrence
from krim_agent.prompt_nodes import PromptMentionNode, PromptSkillNode, PromptTextNode
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.sdk import PythonAI
from krim_agent.skills import SkillBatch, SkillDefinition
from krim_agent.tools.builtins import builtin_registry


async def main() -> None:
    assert "/site-packages/" in krim_agent.__file__, krim_agent.__file__
    with TemporaryDirectory(prefix="krim-wheel-prompt-nodes-") as folder:
        database = Database(f"sqlite+aiosqlite:///{Path(folder) / 'nodes.sqlite'}")
        await database.initialize()
        repository = KrimRepository(database)
        runtime = ChatRuntime(repository, FakeProvider(delay=0), builtin_registry(), Settings())
        try:
            topic = await repository.create_topic("wheel nodes")
            mention = await repository.add_mention(
                topic.id, MentionInput(label="doc", content="明确内容")
            )
            await repository.inject_skills(
                topic.id,
                SkillBatch(
                    skills=[SkillDefinition(name="review", description="review", content="body")]
                ),
            )
            nodes = [
                PromptTextNode(text="😀 "),
                PromptMentionNode(mention_id=mention.id, label="doc"),
                PromptSkillNode(name="review"),
            ]
            result = await PythonAI(runtime).generate_text(
                "😀 @doc",
                topic_id=topic.id,
                idempotency_key="nodes",
                mentions=[MentionOccurrence(mention_id=mention.id, start=2, end=6)],
                skill_names=["review"],
                nodes=nodes,
            )
            assert result.run.status == "completed"
            message = (await repository.list_messages(topic.id))[0]
            assert message.details["prompt_nodes"] == [
                node.model_dump(mode="json") for node in nodes
            ]
            print(
                json.dumps(
                    {
                        "import": "site-packages",
                        "types": [node.type for node in nodes],
                        "status": result.run.status,
                        "unicode": message.content,
                    },
                    ensure_ascii=False,
                )
            )
        finally:
            await runtime.close()
            await database.close()


asyncio.run(main())
