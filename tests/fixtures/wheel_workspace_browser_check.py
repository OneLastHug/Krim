"""项目外wheel：浏览器模块依赖、访问边界与同一工作区修改接口。"""

import asyncio
import json
import re
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from pydantic import SecretStr

import krim_agent
from krim_agent.api import create_app
from krim_agent.config import Settings


async def main():
    assert "/site-packages/" in krim_agent.__file__
    with TemporaryDirectory(prefix="krim-browser-wheel-") as directory:
        root = Path(directory)
        app = create_app(
            Settings(
                database_url=f"sqlite+aiosqlite:///{root / 'test.sqlite'}",
                api_key=SecretStr("wheel-key"),
                interactive_tools=True,
            ),
            workspace_root=root,
            workspace_writable=True,
        )
        headers = {"Authorization": "Bearer wheel-key"}
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
            ) as api,
        ):
            pending = ["agents-ui.js"]
            checked = set()
            while pending:
                name = pending.pop()
                if name in checked:
                    continue
                response = await api.get(f"/ui/{name}")
                assert response.status_code == 200
                assert "script-src 'self'" in response.headers["content-security-policy"]
                checked.add(name)
                pending.extend(re.findall(r'from "\./([^"/]+\.js)"', response.text))
            assert "workspace-changes-ui.js" in checked
            locale = (await api.get("/v1/locales?locale=en-US", headers=headers)).json()
            assert locale["strings"]["查看操作回执"] == "View operation receipt"
            assert (await api.get("/v1/agents")).status_code == 401
            agent = (await api.post("/v1/agents", headers=headers, json={"name": "default"})).json()
            base = f"/v1/agents/{agent['agent_id']}/workspace"
            proposed = await api.post(
                f"{base}/changes/preview",
                headers=headers,
                json={
                    "spec": {
                        "path": "browser.txt",
                        "operation": "write",
                        "expected_sha256": None,
                        "content": "browser\n",
                    },
                    "request_key": "preview",
                },
            )
            assert proposed.status_code == 200
            proposal = proposed.json()
            applied = await api.post(
                f"{base}/changes?confirmed=true",
                headers=headers,
                json={
                    "proposal_id": proposal["proposal_id"],
                    "action": "apply",
                    "expected_revision": proposal["revision"],
                    "request_key": "apply",
                },
            )
            assert applied.status_code == 200 and applied.json()["status"] == "done"
            file = await api.get(f"{base}/read?path=browser.txt", headers=headers)
            assert file.json()["content"] == "browser\n"
            assert (await api.get(f"{base}/changes", headers=headers)).json()[0][
                "status"
            ] == "applied"
            print(
                json.dumps(
                    {
                        "package": krim_agent.__file__,
                        "assets": sorted(checked),
                        "file_result": "done",
                        "locale": "en-US",
                    }
                )
            )


asyncio.run(main())
