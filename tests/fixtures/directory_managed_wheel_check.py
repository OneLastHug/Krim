"""项目外wheel验证AgentSession目录工具沿同一0036事实完成创建/撤销。"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import krim_agent
from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.config import Settings
from krim_agent.sdk import AgentSession, PythonAI
from krim_agent.workspace_directory import DirectoryChangeAction, DirectoryChangeSpec


async def main() -> None:
    assert "/site-packages/" in str(krim_agent.__file__)
    with TemporaryDirectory(prefix="krim-directory-managed-wheel-", dir="/data/tmp/root") as folder:
        root = Path(folder) / "workspace"
        root.mkdir()
        database = Path(folder) / "state.sqlite"
        async with await PythonAI.create(
            settings=Settings(database_url=f"sqlite+aiosqlite:///{database}")
        ) as ai:

            async def policy(definition, _context):
                return "ask" if definition.risk == "write" else "allow"

            ai.runtime.tool_policy = policy
            service = ai.agents()
            service.register_workspace("files", root, version="1", writable=True)
            await service.profile_put(
                AgentProfileUpdate(
                    expected_revision=0,
                    config=AgentProfile(
                        name="directory-wheel",
                        description="directory wheel",
                        workspace_key="files",
                        tool_names=[
                            "directory_preview",
                            "directory_change",
                            "directory_reconcile",
                            "directory_finalize",
                        ],
                    ),
                )
            )
            view = await service.spawn("directory-wheel")
            agent = AgentSession(service, view.agent_id)
            proposal = await agent.preview_directory_change(
                DirectoryChangeSpec(path="empty", operation="create", mode=0o750),
                request_key="preview",
            )
            applied = await agent.change_directory(
                DirectoryChangeAction(
                    proposal_id=proposal.proposal_id,
                    action="apply",
                    expected_revision=proposal.revision,
                    request_key="apply",
                ),
                confirmed=True,
            )
            assert applied.status == "done" and (root / "empty").is_dir()
            current = await service.manager.directory_changes.store.get(
                "session:" + agent.session_id, proposal.proposal_id
            )
            undone = await agent.change_directory(
                DirectoryChangeAction(
                    proposal_id=proposal.proposal_id,
                    action="undo",
                    expected_revision=current.view.revision,
                    request_key="undo",
                ),
                confirmed=True,
            )
            assert undone.status == "done" and not (root / "empty").exists()
            await agent.finalize_directory_change(undone.operation_id, confirmed=True)
            print(json.dumps({"package": krim_agent.__file__, "created": True, "undone": True}))


asyncio.run(main())
