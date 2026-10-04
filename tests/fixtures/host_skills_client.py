"""真实浏览器 Host 技能注入、查询和资源读取；仅测试 Key 与文本。"""

import asyncio
import json
import os
import sys

from krim_agent.app_sdk import RemoteHostHandle
from krim_agent.skills import SkillBatch, SkillDefinition, SkillResource


async def main() -> None:
    async with RemoteHostHandle(sys.argv[1], sys.argv[2], os.environ["KRIM_AGENT_API_KEY"]) as host:
        state = await host.state()
        assert state.topic_id is not None
        batch = SkillBatch(
            skills=[
                SkillDefinition(
                    name="host-review",
                    description="Python 宿主注入的检查技能",
                    content="Python 技能正文",
                    resources=[
                        SkillResource(path="references/source.md", content="Python 技能资源")
                    ],
                )
            ]
        )
        await host.inject_skills(state.topic_id, batch)
        catalog = await host.skills(state.topic_id)
        body = await host.read_skill(state.topic_id, "host-review")
        resource = await host.read_skill(state.topic_id, "host-review", "references/source.md")
        assert body.content == "Python 技能正文" and resource.content == "Python 技能资源"
        print(
            json.dumps(
                {"topic_id": state.topic_id, "name": "host-review", "count": len(catalog.skills)},
                ensure_ascii=False,
            )
        )


asyncio.run(main())
