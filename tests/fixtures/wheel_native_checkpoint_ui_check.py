"""项目外wheel：统一检查点的静态依赖、翻译与实际文件恢复入口。"""

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


async def main() -> None:
    assert "/site-packages/" in str(krim_agent.__file__)
    with TemporaryDirectory(prefix="krim-checkpoint-ui-wheel-") as directory:
        root = Path(directory)
        (root / "file.txt").write_text("A\n")
        app = create_app(
            Settings(
                database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}",
                api_key=SecretStr("wheel-checkpoint-key"),
                interactive_tools=True,
            ),
            workspace_root=root,
            workspace_writable=True,
        )
        headers = {"Authorization": "Bearer wheel-checkpoint-key"}
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
            ) as api,
        ):
            html = await api.get("/ui/")
            assert html.status_code == 200 and 'id="checkpoint-source"' in html.text
            assert 'id="native-restore-detail"' in html.text
            checked: set[str] = set()
            pending = ["app.js"]
            while pending:
                name = pending.pop()
                if name in checked:
                    continue
                response = await api.get(f"/ui/{name}")
                assert response.status_code == 200
                assert "script-src 'self'" in response.headers["content-security-policy"]
                checked.add(name)
                pending.extend(re.findall(r'from "\./([^"/]+\.js)"', response.text))
            assert {
                "checkpoint-ui.js",
                "native-checkpoint-ui.js",
                "workspace-changes-ui.js",
            } <= checked
            script = (await api.get("/ui/native-checkpoint-ui.js")).text
            assert "native-restores" in script and "requestJsonNullable" in script
            assert "textContent" in script and "innerHTML" not in script
            locale = (await api.get("/v1/locales?locale=en-US", headers=headers)).json()
            assert locale["strings"]["执行已审阅的恢复"] == "Execute reviewed restore"
            assert locale["strings"]["未确认请求键"] == "Unconfirmed request key"
            assert (
                await api.post("/_test/native-checkpoint", headers=headers, json={})
            ).status_code == 404
            agent = (await api.post("/v1/agents", headers=headers, json={"name": "default"})).json()
            base = f"/v1/agents/{agent['agent_id']}/workspace"
            assert (await api.get(f"{base}/native-checkpoints")).status_code == 401
            accepted = await api.post(
                f"{base}/native-checkpoints",
                headers=headers,
                json={
                    "paths": ["file.txt", "missing.txt"],
                    "request_key": "capture",
                },
            )
            assert accepted.status_code == 202
            checkpoint = (
                await api.post(
                    f"{base}/native-checkpoints/{accepted.json()['checkpoint_id']}/wait",
                    headers=headers,
                )
            ).json()
            assert checkpoint["status"] == "available"
            (root / "file.txt").write_text("B\n")
            (root / "missing.txt").write_text("added")
            accepted = await api.post(
                f"{base}/native-restores/preview",
                headers=headers,
                json={
                    "checkpoint_id": checkpoint["checkpoint_id"],
                    "request_key": "preview",
                },
            )
            assert accepted.status_code == 202
            plan_id = accepted.json()["plan_id"]
            plan = (
                await api.post(f"{base}/native-restores/{plan_id}/wait", headers=headers)
            ).json()
            assert plan["phase"] == "ready" and plan["baseline_checkpoint_id"]
            assert "-B" in plan["items"][0]["proposal"]["preview"]["diff"]
            request = {
                "plan_id": plan_id,
                "expected_revision": plan["revision"],
                "request_key": "execute",
            }
            assert (
                await api.post(
                    f"{base}/native-restores/{plan_id}/execute?confirmed=true",
                    headers=headers,
                    json=request,
                )
            ).status_code == 202
            done = (
                await api.post(f"{base}/native-restores/{plan_id}/wait", headers=headers)
            ).json()
            assert done["status"] == "done", done
            assert (root / "file.txt").read_text() == "A\n" and not (root / "missing.txt").exists()
            proposal = done["items"][0]["proposal"]
            undone = await api.post(
                f"{base}/changes?confirmed=true",
                headers=headers,
                json={
                    "proposal_id": proposal["proposal_id"],
                    "action": "undo",
                    "expected_revision": proposal["revision"],
                    "request_key": "undo",
                },
            )
            assert undone.json()["status"] == "done" and (root / "file.txt").read_text() == "B\n"
            print(
                json.dumps(
                    {
                        "package": krim_agent.__file__,
                        "assets": sorted(checked),
                        "capture": "available",
                        "restore": "done",
                        "undo": "done",
                    }
                )
            )


asyncio.run(main())
