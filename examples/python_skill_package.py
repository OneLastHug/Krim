"""本地ZIP技能包→PythonAI→真实read_skill工具→原Run快照演练。"""

import asyncio
import base64
import io
import zipfile

from krim_agent.providers import FakeProvider
from krim_agent.sdk import PythonAI, SkillPackageRequest


async def main() -> None:
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "SKILL.md",
            "---\nname: review\ndescription: 检查输入\nlicense: MIT\n---\n"
            "先检查参数与权限，再给出验证步骤。",
        )
        archive.writestr("references/guide.md", "输入校验、权限、取消和恢复。")
    request = SkillPackageRequest(archive_base64=base64.b64encode(data.getvalue()).decode())
    async with await PythonAI.create(provider=FakeProvider(delay=0)) as ai:
        topic = await ai.runtime.repository.create_topic("Skills包演练")
        catalog = await ai.import_skill_archive(topic.id, request)
        result = await ai.generate_text("/skill review references/guide.md", topic_id=topic.id)
        assert "输入校验" in result.text
        snapshot = await ai.runtime.repository.run_skill_snapshot(result.run.id)
        assert snapshot.skills[0].license == "MIT"
        assert snapshot.skills[0].metadata["krim-package-format"] == "agent-skills-v1"
        assert (await ai.import_skill_archive(topic.id, request)) == catalog
        print({"skill": catalog.skills[0].name, "run": result.run.id, "source": "zip"})


asyncio.run(main())
