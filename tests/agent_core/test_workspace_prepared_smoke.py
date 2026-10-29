from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import sys
from pathlib import Path

if __name__ == "__main__":
    project_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(project_root / "src"))
    sys.path.insert(0, str(project_root / "tests/agent_core"))

import httpx
import pytest
from fixtures.workspace_server import WorkspaceWire

from krim_agent.api.server import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.providers.openai_compatible import (
    OpenAICompatibleConfig,
    OpenAICompatibleProvider,
)
from krim_agent.security import AccessPolicy, Credential, Membership


class PreparedInputWire(WorkspaceWire):
    """本机HTTP夹具只提议运行已上传的unittest文件。"""

    def script(self, body: dict[str, object]) -> list[bytes]:
        messages = body["messages"]
        current = max(i for i, item in enumerate(messages) if item["role"] == "user")
        tools = [item for item in messages[current + 1 :] if item["role"] == "tool"]
        if not tools:
            return self.call(
                "command_run",
                {
                    "expected_revision": 1,
                    "program": "python",
                    "argv": ["-m", "unittest", "discover", "-s", ".", "-p", "test_*.py"],
                    "cwd": ".",
                },
                0,
            )
        return self.answer("本机夹具已收到受控命令结果")


@pytest.mark.asyncio
async def test_p5_linux_workspace_runner_api_smoke_under_qualified_cgroup(
    tmp_path: Path,
) -> None:
    required = ("KRIM_P5_RUN_ROOT", "KRIM_P5_SANDBOX_UNIT")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        pytest.skip(
            "需要先用 scripts/dev_sandbox.py start 建立 BASE-QUALIFIED profile，"
            "再通过 scripts/dev_sandbox.py exec 提供 " + ", ".join(missing)
        )
    run_root = Path(os.environ["KRIM_P5_RUN_ROOT"])
    unit = os.environ["KRIM_P5_SANDBOX_UNIT"]
    profile_path = run_root / "workspace-profile.json"
    result_path = run_root / "evidence/preparation/result.json"
    def load_json(path: Path) -> object:
        return json.loads(path.read_text(encoding="utf-8"))

    result = await asyncio.to_thread(load_json, result_path)
    profile, base_record = await asyncio.gather(
        asyncio.to_thread(load_json, profile_path),
        asyncio.to_thread(load_json, Path(result["base"])),
    )
    membership = await asyncio.to_thread(Path("/proc/self/cgroup").read_text)
    membership = membership.strip()
    assert result["status"] == "BASE-QUALIFIED"
    assert result["workspace_profile"] == str(profile_path)
    assert base_record["status"] == "BASE-QUALIFIED"
    assert profile["execution"]["cgroup_root"] == result["cgroup_root"]
    assert profile["profile"]["environment"]["rootfs_manifest_sha256"] == (
        result["rootfs_manifest_sha256"]
    )
    assert membership == f"0::/system.slice/{unit}/app"
    runner_evidence = Path(profile["execution"]["evidence"])
    previous_job_names = await asyncio.to_thread(
        lambda: {path.name for path in runner_evidence.glob("job-*")}
    )

    token = "p5-prepared-smoke-local-only"
    access = AccessPolicy(
        credentials=(
            Credential(
                principal_id="p5-smoke",
                token=token,
                memberships=(Membership(workspace_id="local", role="admin", approve=True),),
            ),
        )
    )
    wire = PreparedInputWire()
    await wire.__aenter__()
    provider = OpenAICompatibleProvider(
        OpenAICompatibleConfig(base_url=wire.url, model="fixture", max_tool_calls=1)
    )
    settings = Settings(
        database=tmp_path / "workspace.sqlite",
        workspace_profile_file=profile_path,
        run_timeout=60,
    )
    app = None
    outcome: dict[str, object] = {}
    runner = None
    try:
        app = await open_application(
            settings.database, settings=settings, provider=provider, access_policy=access
        )
        runner = app.runner
        assert runner is not None
        api = create_app(application=app)
        async with api.router.lifespan_context(api):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api),
                base_url="http://localhost",
                headers={"Authorization": "Bearer " + token},
            ) as client:
                capabilities = await client.get("/v3/workspace/capabilities")
                assert capabilities.status_code == 200 and capabilities.json()["available"]

                created = await client.post(
                    "/v2/workspaces/local/sessions",
                    json={
                        "request_key": "p5-smoke-session",
                        "title": "P5 prepared workspace smoke",
                        "target_key": "target-workspace",
                    },
                )
                assert created.status_code == 201, created.text
                session = created.json()
                base = f"/v3/workspaces/local/sessions/{session['id']}"

                seed = b'{"answer":42}\n'
                unittest = (
                    b"import json, unittest\n"
                    b"from pathlib import Path\n\n"
                    b"class UploadedInputTest(unittest.TestCase):\n"
                    b"    def test_seed_and_write_result(self):\n"
                    b"        value = json.loads(Path('seed.json').read_text())\n"
                    b"        self.assertEqual(value, {'answer': 42})\n"
                    b"        Path('answer.json').write_text(json.dumps(value))\n"
                )
                files = [
                    ("seed.json", seed),
                    ("test_uploaded_input.py", unittest),
                ]
                uploaded = await client.post(
                    base + "/inputs",
                    json={
                        "schema_version": 1,
                        "request_key": "p5-smoke-input",
                        "expected_head_revision": 0,
                        "files": [
                            {
                                "path": name,
                                "encoding": "base64",
                                "data": base64.b64encode(content).decode("ascii"),
                                "sha256": hashlib.sha256(content).hexdigest(),
                            }
                            for name, content in files
                        ],
                    },
                )
                assert uploaded.status_code == 200, uploaded.text
                input_revision = uploaded.json()
                current_session = await client.get(
                    f"/v2/workspaces/local/sessions/{session['id']}"
                )
                assert current_session.status_code == 200
                turn_response = await client.post(
                    base + "/turns",
                    json={
                        "schema_version": 1,
                        "request_key": "p5-smoke-turn",
                        "text": "D4_TASK run the uploaded unittest suite",
                        "expected_revision": current_session.json()["revision"],
                        "input": {
                            "mode": "revision",
                            "revision_id": input_revision["revision_id"],
                        },
                        "completion": {
                            "profile": "python-unittest/1",
                            "required_paths": [
                                "seed.json",
                                "test_uploaded_input.py",
                                "answer.json",
                            ],
                            "conditions": [
                                {
                                    "id": "answer",
                                    "kind": "json_equal",
                                    "path": "answer.json",
                                    "pointer": "/answer",
                                    "expected": 42,
                                    "symbols": [],
                                }
                            ],
                        },
                    },
                )
                assert turn_response.status_code == 202, turn_response.text
                turn = turn_response.json()
                turn_id = turn["id"]
                turn_base = f"/v2/workspaces/local/sessions/{session['id']}/turns/{turn_id}"
                approved: set[str] = set()
                async with asyncio.timeout(60):
                    while True:
                        current = await client.get(turn_base)
                        assert current.status_code == 200, current.text
                        state = current.json()
                        if state["status"] not in {"queued", "running"}:
                            break
                        pending = await client.get(turn_base + "/approvals")
                        assert pending.status_code == 200, pending.text
                        for request in pending.json():
                            request_id = request["request_id"]
                            if request_id in approved:
                                continue
                            reply = await client.post(
                                turn_base + "/approvals/" + request_id,
                                json={
                                    "request_key": "p5-smoke-" + request["call_id"],
                                    "call_id": request["call_id"],
                                    "arguments_hash": request["arguments_hash"],
                                    "tool_hash": request["tool_hash"],
                                    "allow": True,
                                },
                            )
                            assert reply.status_code == 200, reply.text
                            approved.add(request_id)
                        await asyncio.sleep(0.02)
                assert state["status"] == "completed", state

                work_response = await client.get(base + "/work")
                assert work_response.status_code == 200, work_response.text
                work = work_response.json()
                assert work["verification"]["state"] == "passed", work
                commands = [effect for effect in work["effects"] if effect["kind"] == "command"]
                assert len(commands) == 1
                command = commands[0]
                assert command["exit_code"] == 0 and command["cleanup"] == "clean"
                revision_id = work["verification"]["revision_id"]
                artifact_response = await client.get(base + "/artifacts/" + revision_id)
                assert artifact_response.status_code == 200, artifact_response.text
                artifact = artifact_response.json()
                assert artifact["source_kind"] == "effect"
                assert artifact["producer_run_id"] == state["run_id"]
                assert artifact["producer_call_id"] == command["call_id"]
                chunk = await client.get(
                    base + f"/artifacts/{revision_id}/files/answer.json?offset=0&length=16384"
                )
                assert chunk.status_code == 200, chunk.text
                assert json.loads(base64.b64decode(chunk.json()["data"])) == {"answer": 42}

                before_rejection = await client.get(base + "/work")
                rejected = await client.post(
                    base + "/inputs",
                    json={
                        "schema_version": 1,
                        "request_key": "p5-smoke-traversal",
                        "expected_head_revision": artifact["revision"],
                        "files": [
                            {
                                "path": "../escape.txt",
                                "encoding": "base64",
                                "data": base64.b64encode(b"escape").decode("ascii"),
                                "sha256": hashlib.sha256(b"escape").hexdigest(),
                            }
                        ],
                    },
                )
                assert rejected.status_code == 409, rejected.text
                assert rejected.json()["error"]["code"] == "artifact_invalid"
                after_rejection = await client.get(base + "/work")
                assert after_rejection.json() == before_rejection.json()
                profile_bytes = await asyncio.to_thread(profile_path.read_bytes)
                outcome = {
                    "status": "P5-LINUX-WORKSPACE-SMOKE-PASSED",
                    "verification_scope": "single Krim-Ubuntu-24.04 Workspace API/Runner run",
                    "unit": unit,
                    "run_root": str(run_root),
                    "workspace_profile": str(profile_path),
                    "profile_sha256": hashlib.sha256(profile_bytes).hexdigest(),
                    "cgroup_membership": membership,
                    "input_revision": input_revision,
                    "turn": state,
                    "approved_requests": sorted(approved),
                    "verification": work["verification"],
                    "command_effect": command,
                    "artifact": artifact,
                    "answer_json": {"answer": 42},
                    "provider_calls": provider.calls,
                    "local_http_fixture_requests": len(wire.requests),
                    "boundary_rejection": {
                        "status": rejected.status_code,
                        "code": rejected.json()["error"]["code"],
                        "work_unchanged": True,
                    },
                }
    finally:
        if app is not None:
            await app.close()
        await provider.close()
        await wire.__aexit__(None, None, None)

    assert app is not None and runner is not None
    new_jobs = await asyncio.to_thread(
        lambda: sorted(
            path for path in runner_evidence.glob("job-*") if path.name not in previous_job_names
        )
    )
    resources = [path / "resources.jsonl" for path in new_jobs]
    assert len(resources) == 1
    event_text = await asyncio.to_thread(resources[0].read_text)
    events = [json.loads(line) for line in event_text.splitlines()]
    event_kinds = [event["kind"] for event in events]
    required_events = {
        "payload_exit",
        "pidfd_closed",
        "populated_zero",
        "leaf_removed",
        "cleanup_complete",
    }
    assert required_events <= set(event_kinds)
    assert runner.closed and not runner.jobs and not runner._resources
    assert app.store.closed
    assert not list(runner.profile.cgroup_root.glob("krim-d1-*"))
    assert not list((runner.profile.material / "tickets").glob("*.json"))
    outcome["cleanup"] = {
        "runner_closed": runner.closed,
        "jobs": len(runner.jobs),
        "resources": len(runner._resources),
        "store_closed": app.store.closed,
        "cgroup_probe_events": event_kinds,
        "cgroup_leaf_removed": True,
        "pidfd_closed": True,
        "tickets_remaining": 0,
    }
    evidence = run_root / "evidence/workspace-smoke" / (
        "result-" + tmp_path.parent.name + "-" + tmp_path.name + ".json"
    )

    def save_evidence() -> None:
        evidence.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with evidence.open("x", encoding="utf-8") as stream:
            json.dump(outcome, stream, ensure_ascii=False, sort_keys=True, indent=2)
            stream.write("\n")
        evidence.chmod(0o600)

    await asyncio.to_thread(save_evidence)


async def serve_workspace_ui() -> None:
    """在已资格unit的app子组运行本机Fixture驱动的Workspace UI/API。"""
    required = ("KRIM_P5_RUN_ROOT", "KRIM_P5_SANDBOX_UNIT")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError("必须经 dev_sandbox.py exec 提供 " + ", ".join(missing))
    run_root = Path(os.environ["KRIM_P5_RUN_ROOT"])
    unit = os.environ["KRIM_P5_SANDBOX_UNIT"]
    profile_path = run_root / "workspace-profile.json"
    result = json.loads((run_root / "evidence/preparation/result.json").read_text())
    profile = json.loads(profile_path.read_text())
    if result["status"] != "BASE-QUALIFIED" or result["workspace_profile"] != str(profile_path):
        raise RuntimeError("当前run没有匹配的BASE-QUALIFIED profile")
    if profile["execution"]["cgroup_root"] != result["cgroup_root"]:
        raise RuntimeError("Workspace profile与资格unit cgroup不匹配")
    membership = (await asyncio.to_thread(Path("/proc/self/cgroup").read_text)).strip()
    if membership != f"0::/system.slice/{unit}/app":
        raise RuntimeError("UI/API进程不在指定unit的app子组")

    import uvicorn

    port = int(os.environ.get("KRIM_P5_UI_PORT", "8877"))
    database = Path(
        os.environ.get("KRIM_P5_UI_DATABASE", str(run_root / "workspace-ui.sqlite"))
    )
    token = os.environ.get("KRIM_P5_UI_TOKEN", "p5-prepared-smoke-local-only")
    access = AccessPolicy(
        credentials=(
            Credential(
                principal_id="p5-smoke",
                token=token,
                memberships=(Membership(workspace_id="local", role="admin", approve=True),),
            ),
        )
    )
    wire = PreparedInputWire()
    await wire.__aenter__()
    provider = OpenAICompatibleProvider(
        OpenAICompatibleConfig(base_url=wire.url, model="fixture", max_tool_calls=1)
    )
    settings = Settings(database=database, workspace_profile_file=profile_path, run_timeout=60)
    app = None
    try:
        app = await open_application(
            database, settings=settings, provider=provider, access_policy=access
        )
        server = uvicorn.Server(
            uvicorn.Config(
                create_app(application=app),
                host="127.0.0.1",
                port=port,
                access_log=False,
                log_level="warning",
            )
        )
        print(
            f"P5 Linux Workspace UI/API ready at http://127.0.0.1:{port}/ui/ "
            f"unit={unit} profile={profile_path}",
            flush=True,
        )
        await server.serve()
    finally:
        if app is not None:
            await app.close()
        await provider.close()
        await wire.__aexit__(None, None, None)


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] != "serve":
        raise SystemExit("usage: test_workspace_prepared_smoke.py serve")
    asyncio.run(serve_workspace_ui())


if __name__ == "__main__":
    main()
