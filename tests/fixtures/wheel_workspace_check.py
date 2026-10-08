"""项目外wheel：工作区SDK/持久恢复/公开静态资源与API一致性。"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx

import krim_agent
from krim_agent.api import create_app
from krim_agent.config import Settings
from krim_agent.providers import FakeProvider
from krim_agent.sdk import PythonAI


class Counting(FakeProvider):
    def __init__(self):
        super().__init__(delay=0)
        self.calls = 0

    async def stream(self, messages, tools):
        self.calls += 1
        async for event in super().stream(messages, tools):
            yield event


async def main():
    assert "/site-packages/" in krim_agent.__file__, krim_agent.__file__
    with TemporaryDirectory(prefix="krim-workspace-wheel-") as directory:
        root = Path(directory) / "files"
        root.mkdir()
        (root / "file.txt").write_text("wheel needle\n", encoding="utf-8")
        settings = Settings(database_url=f"sqlite+aiosqlite:///{Path(directory) / 'agents.sqlite'}")
        async with await PythonAI.create(
            settings=settings,
            provider=Counting(),
            workspace_root=root,
        ) as warm:
            agent = await warm.agent(persistent=True)
            file = await agent.read_file("file.txt")
            assert file.content == "wheel needle\n"
            assert (await agent.workspace()).key == "local"
            await agent.execute("/echo saved", idempotency_key="saved")
            identifier, run_id = agent.agent_id, (await agent.view()).current_run_id
        provider = Counting()
        async with await PythonAI.create(
            settings=settings,
            provider=provider,
            workspace_root=root,
        ) as cold:
            assert await cold.agents().restore_persistent() == 1
            restored = cold.agent_session(identifier)
            assert (await restored.view()).current_run_id == run_id
            assert (await restored.read_file("file.txt")).sha256 == file.sha256
            assert (await restored.list_files()).entries[0].path == "file.txt"
            assert (await restored.search_files("needle")).matches[0].line == 1
            assert provider.calls == 0
            await restored.destroy()
            app = create_app(runtime=cold.runtime, workspace_root=root)
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
                ) as api,
            ):
                script = await api.get("/ui/workspace-ui.js")
                assert script.status_code == 200 and "textContent" in script.text
                view = (await api.post("/v1/agents", json={"name": "default"})).json()
                response = await api.get(
                    f"/v1/agents/{view['agent_id']}/workspace/read", params={"path": "file.txt"}
                )
                assert response.status_code == 200 and response.json()["sha256"] == file.sha256
            print(
                json.dumps(
                    {
                        "package": krim_agent.__file__,
                        "restored": True,
                        "cold_model_calls": provider.calls,
                        "api_sdk_same_sha256": True,
                        "workspace_ui": True,
                    }
                )
            )


asyncio.run(main())
