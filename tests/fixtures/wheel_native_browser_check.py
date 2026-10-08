"""独立wheel验证原生浏览器依赖、鉴权、批次历史和实际文件闭环。"""

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
    assert "/site-packages/" in str(krim_agent.__file__)
    with TemporaryDirectory(prefix="krim-native-browser-wheel-") as directory:
        root = Path(directory)
        app = create_app(
            Settings(
                database_url=f"sqlite+aiosqlite:///{root / 'test.sqlite'}",
                api_key=SecretStr("wheel-test-key"),
                interactive_tools=True,
            ),
            workspace_root=root,
            workspace_writable=True,
        )
        headers = {"Authorization": "Bearer wheel-test-key"}
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
            ) as api,
        ):
            html = await api.get("/ui/")
            assert html.status_code == 200 and 'id="change-source"' in html.text
            assert "script-src 'self'" in html.headers["content-security-policy"]
            pending, checked = ["app.js"], set()
            while pending:
                name = pending.pop()
                if name in checked:
                    continue
                response = await api.get(f"/ui/{name}")
                assert response.status_code == 200
                checked.add(name)
                pending.extend(re.findall(r'from "\./([^"/]+\.js)"', response.text))
            assert {"change-list-ui.js", "workspace-changes-ui.js", "api.js"} <= checked
            scripts = (await api.get("/ui/api.js")).text
            assert "requestJsonNullable" in scripts and "ApiError" in scripts
            native = (await api.get("/ui/change-list-ui.js")).text
            assert "native-batches/history" in native and "changeList/sync" in native
            locale = (await api.get("/v1/locales?locale=en-US", headers=headers)).json()
            assert locale["strings"]["保留本页"] == "Keep this page"
            assert locale["strings"]["连续后继保留证明"] == "Successor chain keep proof"
            agent = (await api.post("/v1/agents", headers=headers, json={"name": "default"})).json()
            base = f"/v1/agents/{agent['agent_id']}/workspace"
            assert (await api.get(f"{base}/native-batches/history")).status_code == 401
            assert (await api.get(f"{base}/native-batches/history", headers=headers)).json()[
                "total"
            ] == 0
            proposal = (
                await api.post(
                    f"{base}/changes/preview",
                    headers=headers,
                    json={
                        "spec": {
                            "path": "native.txt",
                            "operation": "write",
                            "expected_sha256": None,
                            "content": "installed file\n",
                        },
                        "request_key": "preview",
                    },
                )
            ).json()
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
            for action in ("keep", "undo"):
                page = (await api.get(f"{base}/native-changes", headers=headers)).json()
                assert page["total"] == 1
                item = page["items"][0]
                response = await api.post(
                    f"{base}/native-batches?confirmed=true",
                    headers=headers,
                    json={
                        "action": action,
                        "snapshot_revision": page["snapshot_revision"],
                        "targets": [
                            {
                                "proposal_id": item["proposal_id"],
                                "expected_revision": item["revision"],
                            }
                        ],
                        "request_key": f"原键/轮次?{action}",
                    },
                )
                assert response.status_code == 202
                batch_id = response.json()["batch_id"]
                done = await api.post(
                    f"{base}/native-batches/{batch_id}/wait?timeout_seconds=10", headers=headers
                )
                assert done.status_code == 200 and done.json()["status"] == "done"
                history = (
                    await api.get(f"{base}/native-batches/history?limit=1", headers=headers)
                ).json()
                assert history["items"][0]["batch_id"] == batch_id
                assert history["items"][0]["done_count"] == 1
                operation = done.json()["items"][0]["operation"]
                proof = (
                    await api.get(
                        f"{base}/operations/{operation['operation_id']}/proof", headers=headers
                    )
                ).json()
                assert proof["status"] == "done"
                if action == "keep":
                    assert proof["kind"] == "exact_after"
                    assert (root / "native.txt").read_text() == "installed file\n"
                else:
                    assert history["total"] == 2 and history["next_cursor"]
                    older = await api.get(
                        f"{base}/native-batches/history",
                        params={"limit": 1, "cursor": history["next_cursor"]},
                        headers=headers,
                    )
                    assert older.json()["items"][0]["action"] == "keep"
                    assert not (root / "native.txt").exists()
                assert str(root) not in json.dumps(history)
                assert "token" not in json.dumps(history) and "receipt" not in json.dumps(history)
            print(
                json.dumps(
                    {"package": krim_agent.__file__, "assets": sorted(checked), "batches": 2}
                )
            )


asyncio.run(main())
