"""项目外wheel核对命令UI完整静态依赖、鉴权与真实隔离命令事实。"""

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
from krim_agent.execution import LinuxSandboxRunner


async def main() -> None:
    assert "/site-packages/" in str(krim_agent.__file__)
    with TemporaryDirectory(prefix="krim-command-ui-check-") as directory:
        root = Path(directory)
        files = root / "workspace"
        files.mkdir()
        app = create_app(
            Settings(
                database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}",
                api_key=SecretStr("wheel-command-ui-key"),
                interactive_tools=True,
            ),
            workspace_root=files,
            workspace_writable=True,
            execution_runner=LinuxSandboxRunner(),
            execution_owned=True,
        )
        headers = {"Authorization": "Bearer wheel-command-ui-key"}
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
            ) as api,
        ):
            html = (await api.get("/ui/")).text
            assert 'id="agent-config-execution"' in html
            assert 'id="agent-execution-options"' in html
            pending = ["app.js"]
            assets: set[str] = set()
            while pending:
                name = pending.pop()
                if name in assets:
                    continue
                response = await api.get(f"/ui/{name}")
                assert response.status_code == 200
                assert "script-src 'self'" in response.headers["content-security-policy"]
                assert response.headers["x-content-type-options"] == "nosniff"
                assets.add(name)
                pending.extend(re.findall(r'from "\./([^"/]+\.js)"', response.text))
            assert {"agents-ui.js", "commands-ui.js", "workspace-changes-ui.js"} <= assets
            script = (await api.get("/ui/commands-ui.js")).text
            assert "requestJsonNullable" in script and "sessionStorage" in script
            assert "textContent" in script and "innerHTML" not in script
            locale = (await api.get("/v1/locales?locale=en-US", headers=headers)).json()
            assert locale["strings"]["执行受控命令"] == "Execute controlled command"
            assert locale["strings"]["执行资源（留空不授权）"] == (
                "Execution resource (leave blank for no grant)"
            )
            assert (await api.post("/_test/workspace", headers=headers, json={})).status_code == 404
            tools = [
                "command_execute",
                "command_get",
                "command_history",
                "command_inspect",
                "command_read",
            ]
            saved = await api.put(
                "/v1/agents/profiles/ui-check",
                headers=headers,
                json={
                    "expected_revision": 0,
                    "config": {
                        "name": "ui-check",
                        "description": "命令界面安装验证",
                        "workspace_key": "local",
                        "execution_key": "local",
                        "tool_names": tools,
                    },
                },
            )
            assert saved.status_code == 200, saved.text
            spawned = await api.post("/v1/agents", headers=headers, json={"name": "ui-check"})
            assert spawned.status_code == 201
            base = f"/v1/agents/{spawned.json()['agent_id']}/commands"
            assert (await api.get(base)).status_code == 401
            accepted = await api.post(
                base + "?confirmed=true",
                headers=headers,
                json={
                    "request_key": "wheel-ui-once",
                    "command": {"argv": ["/usr/bin/python3", "-I", "-c", "print('wheel UI')"]},
                },
            )
            assert accepted.status_code == 202, accepted.text
            command_id = accepted.json()["command_id"]
            result = (await api.post(f"{base}/{command_id}/wait", headers=headers)).json()
            assert result["outcome"] == "completed" and result["processes_reaped"]
            original = (
                await api.get(base + "/by-key?request_key=wheel-ui-once", headers=headers)
            ).json()
            assert original["command_id"] == command_id
            history = (await api.get(base, headers=headers)).json()
            assert len(history["entries"]) == 1
            output = (await api.get(f"{base}/{command_id}/read", headers=headers)).json()
            assert output["text"] == "wheel UI\n"
            print(
                json.dumps(
                    {
                        "package": krim_agent.__file__,
                        "assets": sorted(assets),
                        "command": result["outcome"],
                    }
                )
            )


asyncio.run(main())
