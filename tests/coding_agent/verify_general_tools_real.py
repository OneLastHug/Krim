"""Opt-in general tools through real Broker/XFS/runsc and production API.

The model is a deterministic loopback HTTP fixture and MCP uses the official
local SDK. This verifies infrastructure and protocol integration, not external
model quality. No installation, registry pull, commit or deployment occurs.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import shutil
import sys
import tempfile
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import select

from krim_agent.api.server import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.context.compaction import PROMPT
from krim_agent.development.configuration import DevelopmentConfiguration
from krim_agent.mcp import MCPServerConfig
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig
from krim_agent.sandbox.configuration import SandboxConfiguration
from krim_agent.storage.models import compactions
from krim_agent.tools.runtime import ToolRuntimeConfig
from tests.agent_core.fixtures.session_server import WireFixture
from tests.agent_core.test_account_api import initialize
from tests.coding_agent.test_general_session_tools_flow import MCP_SERVER_SCRIPT
from tests.coding_agent.test_view_image import PNG
from tests.coding_agent.verify_development_broker import (
    APPROVED_IMAGE,
    APPROVED_STORAGE_ROOT,
    Paths,
    broker_lifecycle,
    cleanup_containers,
    quota_preflight,
    require_platform,
)
from tests.coding_agent.verify_sqlite_terminal import RecordingDocker


class GeneralWire(WireFixture):
    def __init__(self) -> None:
        super().__init__()
        self.index = 0
        self.summaries = 0
        self.images = 0
        self.process_id: str | None = None
        self.goal_completed = False
        self.source = (
            "import unittest\n"
            "class Arithmetic(unittest.TestCase):\n"
            "    def test_sum(self): self.assertEqual(2 + 3, 5)\n"
            "if __name__ == '__main__': unittest.main()\n"
            + "# retained project content beyond one MiB\n"
            * 28000
        )
        self.steps = [
            ("enter_plan_mode", {"reason": "Inspect and confirm the real integration task"}),
            (
                "plan_update",
                {
                    "request_key": "plan-start",
                    "expected_revision": 0,
                    "steps": [
                        {
                            "id": "verify",
                            "text": "Verify actual project tools",
                            "status": "in_progress",
                        }
                    ],
                },
            ),
            (
                "goal_update",
                {
                    "request_key": "goal-start",
                    "expected_revision": 0,
                    "action": "set",
                    "objective": "Complete the requested integration",
                    "completion_criteria": ["Real file tests and image delivery"],
                },
            ),
            (
                "ask_user",
                {
                    "questions": [
                        {
                            "id": "proceed",
                            "prompt": "Continue the verification?",
                            "mode": "text",
                            "options": [],
                            "allow_text": True,
                        }
                    ]
                },
            ),
            (
                "exit_plan_mode",
                {"plan": "Write the requested test file, run it, and inspect results."},
            ),
            (
                "memory_write",
                {
                    "scope": "project",
                    "body": "Acceptance uses unittest in app.py",
                    "source_kind": "user",
                    "memory_kind": "note",
                },
            ),
            ("tool_search", {"names": ["docs__doc_read"]}),
            ("read_mcp_resource", {"server": "host:docs", "uri": "doc://brief"}),
            ("docs__doc_read", {"value": "integration evidence"}),
            ("write", {"path": "app.py", "content": self.source}),
            (
                "shell",
                {
                    "command": "python3 app.py && python3 -c "
                    + repr(
                        "import base64;from pathlib import Path;Path('proof.png').write_bytes("
                        + "base64.b64decode('"
                        + base64.b64encode(PNG).decode()
                        + "'))"
                    ),
                    "wait_seconds": 20,
                    "timeout_seconds": None,
                },
            ),
            ("read_output", {}),
            ("view_image", {"path": "proof.png"}),
            ("compact_context", {"reason": "verified real project outputs"}),
            (
                "plan_update",
                {
                    "request_key": "plan-done",
                    "expected_revision": 1,
                    "steps": [
                        {
                            "id": "verify",
                            "text": "Verify actual project tools",
                            "status": "completed",
                        }
                    ],
                },
            ),
        ]

    @staticmethod
    def stream(delta: dict[str, Any], finish: str) -> bytes:
        frames = []
        for value, reason in ((delta, None), ({}, finish)):
            frames.append(
                "data: "
                + json.dumps(
                    {
                        "id": "real-flow",
                        "object": "chat.completion.chunk",
                        "created": 0,
                        "model": "fixture",
                        "choices": [{"index": 0, "delta": value, "finish_reason": reason}],
                    }
                )
                + "\n\n"
            )
        return ("".join(frames) + "data: [DONE]\n\n").encode()

    def response(self, body: dict[str, Any], index: int) -> bytes:
        messages = body["messages"]
        if any(
            isinstance(message.get("content"), list)
            and any(part.get("type") == "image_url" for part in message["content"])
            for message in messages
        ):
            self.images += 1
        if messages[0].get("content", "").startswith(PROMPT):
            self.summaries += 1
            return self.stream(
                {
                    "content": "User confirmed. Real project file tests passed; "
                    "plan and goal remain in durable state."
                },
                "stop",
            )
        last = messages[-1]
        if last["role"] == "tool":
            value = json.loads(last["content"])
            if isinstance(value, dict) and "value" in value and "executor_started" in value:
                value = value["value"]
            if isinstance(value, dict) and "process_id" in value and "state" in value:
                self.process_id = value["process_id"]
                if value["state"] in {"accepted", "running"}:
                    return self.tool("process_status", {"process_id": self.process_id}, index)
                if value.get("exit_code") is not None:
                    assert value["exit_code"] == 0, value
            if self.index == 10:
                assert isinstance(value, dict) and value.get("ok") is True, value
            if self.index == 12:
                assert "OK" in value.get("text", ""), value
        if self.index < len(self.steps):
            name, arguments = self.steps[self.index]
            self.index += 1
            if name == "read_output":
                assert self.process_id is not None
                arguments = {"process_id": self.process_id, "cursor": 0}
            return self.tool(name, arguments, index)
        text = "\n".join(str(item.get("content", "")) for item in messages)
        if "内部续跑（宿主自动触发）" in text and not self.goal_completed:
            self.goal_completed = True
            return self.tool(
                "goal_update",
                {
                    "request_key": "goal-finished",
                    "expected_revision": 1,
                    "action": "complete",
                    "completion_criteria": ["Real file tests and image delivery"],
                    "evidence_refs": [{"kind": "test", "ref": "command:python3-app.py"}],
                },
                index,
            )
        return self.stream({"content": "Real integration phase completed."}, "stop")

    def tool(self, name: str, arguments: dict[str, Any], index: int) -> bytes:
        return self.stream(
            {
                "tool_calls": [
                    {
                        "index": 0,
                        "id": "real-" + str(index),
                        "type": "function",
                        "function": {"name": name, "arguments": json.dumps(arguments)},
                    }
                ]
            },
            "tool_calls",
        )


@asynccontextmanager
async def local_mcp(root: Path):
    script = root / "mcp_fixture.py"
    script.write_text(
        MCP_SERVER_SCRIPT.replace('port=int(os.environ["KRIM_FLOW_MCP_PORT"])', "port=0").replace(
            'server.run(transport="streamable-http")', 'server.run(transport="stdio")'
        )
    )
    yield MCPServerConfig(
        name="docs",
        transport="stdio",
        command=sys.executable,
        args=(str(script),),
        selected_tools=("doc_read",),
        readonly_tools=frozenset({"doc_read"}),
        timeout_seconds=60,
    )


async def client_for(app: Any, token: str):
    api = create_app(application=app)
    api.state.application = app
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api),
        base_url="http://127.0.0.1",
        timeout=120,
        headers={"Authorization": "Bearer " + token},
    )
    return client


async def request(client: httpx.AsyncClient, method: str, route: str, body=None):
    response = await client.request(method, route, json=body)
    assert response.status_code < 400, (method, route, response.status_code, response.text)
    return response.json()


async def main(output: Path) -> None:
    require_platform(APPROVED_STORAGE_ROOT)
    await asyncio.to_thread(output.mkdir, mode=0o700, parents=True, exist_ok=False)
    root = Path(tempfile.mkdtemp(prefix="general-real-", dir=APPROVED_STORAGE_ROOT))
    root_identity = await asyncio.to_thread(root.stat)
    state = root / "broker-state"
    state.mkdir(mode=0o700)
    paths = Paths(
        root,
        root_identity.st_dev,
        root_identity.st_ino,
        state,
        root / "private",
        root / "broker.sock",
        root / "app.sqlite",
        root / "environments",
    )
    wire = await GeneralWire().start()
    docker = RecordingDocker(output / "created-containers.json")
    app = client = handle = None
    owner = project_id = environment_id = None
    workers = []
    report: dict[str, Any] = {
        "model": "deterministic loopback HTTP",
        "broker": "real",
        "storage": "XFS prjquota",
        "runtime": "runsc",
        "steps": [],
    }
    try:
        async with broker_lifecycle(paths):
            token = await initialize(paths.database)
            sandbox_file = root / "sandbox.json"
            sandbox_file.write_text(
                SandboxConfiguration(
                    broker_socket=paths.socket,
                    broker_owner_uid=0,
                ).model_dump_json()
            )
            sandbox_file.chmod(0o600)
            configuration = DevelopmentConfiguration(
                image_id=APPROVED_IMAGE,
                broker_environments_root=paths.environments,
                private_state_root=paths.private_state,
                model_images=True,
            )
            settings = Settings(
                database=paths.database,
                provider="http",
                http_mode="agent",
                sandbox_config_file=sandbox_file,
                max_request_bytes=8_000_000,
                context_max_bytes=8_000_000,
            )
            provider = OpenAICompatibleConfig(
                base_url=f"http://127.0.0.1:{wire.port}/v1",
                model="fixture",
                allow_images=True,
                max_request_bytes=8_000_000,
                max_output_tokens=8192,
                connect_timeout_seconds=10,
                read_timeout_seconds=90,
            )
            async with local_mcp(root) as mcp:
                tools = ToolRuntimeConfig(mcp_servers=(mcp,), web_result_root=root / "web")

                async def open_app():
                    nonlocal app, client
                    app = await open_application(
                        paths.database,
                        settings=settings,
                        http_config=provider,
                        development_configuration=configuration,
                        tool_config=tools,
                    )
                    app.development.environments.runner = docker
                    if app.development._worker_process is not None:
                        workers.append(app.development._worker_process)
                    client = await client_for(app, token)

                await open_app()
                me = await request(client, "GET", "/v2/me")
                owner = me["principal_id"]
                workspace = me["workspaces"][0]["workspace_id"]
                project = await request(
                    client,
                    "POST",
                    "/v2/me/sandbox/projects",
                    {
                        "request_key": "project",
                        "title": "General tools real acceptance",
                        "workspace_id": workspace,
                    },
                )
                project_id, environment_id = project["project_id"], project["environment_id"]
                report["quota_preflight"] = await asyncio.to_thread(
                    quota_preflight,
                    paths,
                    environment_id,
                )
                handle_body = await request(
                    client,
                    "POST",
                    f"/v2/me/development/projects/{project_id}/prepare",
                    {"request_key": "prepare"},
                )
                from krim_agent.development.contracts import EnvironmentHandle

                handle = EnvironmentHandle.model_validate(handle_body)
                await request(
                    client,
                    "PUT",
                    f"/v2/me/development/projects/{project_id}/grant",
                    {
                        "expected_revision": 0,
                        "expires_at": time.time() + 3600,
                        "requires_approval": True,
                    },
                )
                await request(
                    client,
                    "PUT",
                    "/v2/me/memory-preference",
                    {"request_key": "memory-on", "expected_revision": 0, "enabled": True},
                )
                base = f"/v2/workspaces/{workspace}/sessions"
                session = await request(
                    client,
                    "POST",
                    base,
                    {
                        "request_key": "session",
                        "title": "Real general flow",
                        "target_key": "target-development",
                    },
                )
                route = base + "/" + session["id"]
                binding = await request(
                    client,
                    "PUT",
                    f"/v2/me/sandbox/sessions/{session['id']}/project",
                    {
                        "request_key": "bind",
                        "project_id": project_id,
                        "expected_revision": 0,
                        "expected_session_revision": 0,
                    },
                )
                await request(
                    client,
                    "PUT",
                    route + "/extensions",
                    {
                        "request_key": "session-memory",
                        "expected_revision": 0,
                        "memory_enabled": True,
                        "skills": [],
                    },
                )
                current_session = await request(client, "GET", route)
                await request(
                    client,
                    "POST",
                    route + "/turns",
                    {
                        "request_key": "task",
                        "expected_revision": current_session["revision"],
                        "text": "Verify this project with plan and confirmation, write app.py, run "
                        "tests, inspect an image, save this project's unittest convention "
                        "in memory, and continue until all criteria are met.",
                    },
                )
                report["steps"].append(
                    {
                        "step": "prepared",
                        "resource_policy": "unlimited",
                        "session_revision": binding["session_revision"],
                    }
                )
                restarted = False
                answered = set()
                approved = set()
                async with asyncio.timeout(360):
                    while True:
                        interaction = await request(client, "GET", route + "/interactions")
                        for pending in interaction["pending"]:
                            if pending["id"] in answered:
                                continue
                            if not restarted and pending["kind"] == "user_input":
                                parked = await app.service.wait(pending["run_id"])
                                assert parked.status == "awaiting", parked
                                await client.aclose()
                                await app.close()
                                app = client = None
                                await open_app()
                                restarted = True
                                report["steps"].append({"step": "reopen-with-durable-question"})
                            body = {
                                "request_key": "answer-" + pending["id"],
                                "expected_revision": pending["revision"],
                            }
                            if pending["kind"] == "plan_confirmation":
                                body.update(confirmed=True, answers=[])
                            else:
                                body.update(
                                    confirmed=None,
                                    answers=[
                                        {
                                            "question_id": "proceed",
                                            "option_ids": [],
                                            "text": "Proceed with the requested real verification.",
                                        }
                                    ],
                                )
                            await request(
                                client,
                                "POST",
                                route + "/interactions/" + pending["id"] + "/answer",
                                body,
                            )
                            answered.add(pending["id"])
                        history = await request(client, "GET", route + "/turns?limit=50")
                        for item in history:
                            if item["run_id"] is None:
                                continue
                            pending_approvals = await request(
                                client, "GET", route + "/turns/" + item["id"] + "/approvals"
                            )
                            for approval in pending_approvals:
                                if approval["request_id"] in approved:
                                    continue
                                await request(
                                    client,
                                    "POST",
                                    route
                                    + "/turns/"
                                    + item["id"]
                                    + "/approvals/"
                                    + approval["request_id"],
                                    {
                                        "request_key": "approve-" + approval["request_id"],
                                        "allow": True,
                                        **{
                                            key: approval[key]
                                            for key in ("call_id", "arguments_hash", "tool_hash")
                                        },
                                    },
                                )
                                approved.add(approval["request_id"])
                            assert item["status"] not in {"failed", "cancelled", "interrupted"}, (
                                item
                            )
                        goal = await request(client, "GET", route + "/goal")
                        if goal["status"] == "completed" and all(
                            item["status"] == "completed" for item in history
                        ):
                            break
                        await asyncio.sleep(0.1)
                tool_results = []
                for turn in history:
                    snapshot = await app.store.snapshot(turn["run_id"])
                    tool_results.extend(
                        event.data for event in snapshot.events if event.kind == "tool_end"
                    )
                assert all(item["status"] == "ok" for item in tool_results), tool_results
                verified_tools = [item["name"] for item in tool_results]
                assert {name for name, _ in wire.steps} <= set(verified_tools)
                assert verified_tools.count("ask_user") == 1
                report["verified_tools"] = verified_tools
                quota = await app.sandbox.broker.call("usage", environment_id=environment_id)
                assert quota["usage"]["byte_limit"] == quota["usage"]["inode_limit"] == 0
                report["kernel_quota"] = quota
                content = paths.environments / environment_id / "projects" / project_id / "content"
                assert (content / "app.py").read_text() == wire.source
                assert len(wire.source.encode()) > 1024**2
                assert (content / "proof.png").read_bytes() == PNG
                assert wire.images > 0 and restarted
                assert len(answered) == 2 and len(history) == 2
                plan = await request(client, "GET", route + "/plan")
                assert plan["counts"]["completed"] == 1
                memories = await request(
                    client, "GET", f"/v2/workspaces/{workspace}/projects/{project_id}/memories"
                )
                assert len(memories["items"]) == 1
                report["steps"].append(
                    {
                        "step": "full-task-and-goal-continuation",
                        "turns": len(history),
                        "answers": len(answered),
                        "approvals": len(approved),
                        "file_bytes": len(wire.source.encode()),
                        "image_requests": wire.images,
                        "memory_records": len(memories["items"]),
                        "goal": goal["status"],
                    }
                )
                for index in range(3):
                    if index == 2:
                        await request(
                            client,
                            "POST",
                            route + "/context/compactions",
                            {"request_key": "manual-compact", "reason": "real acceptance"},
                        )
                    current_session = await request(client, "GET", route)
                    follow = await request(
                        client,
                        "POST",
                        route + "/turns",
                        {
                            "request_key": "follow-" + str(index),
                            "text": "Verify retained state.",
                            "expected_revision": current_session["revision"],
                        },
                    )
                    async with asyncio.timeout(120):
                        while True:
                            saved = await request(client, "GET", route + "/turns/" + follow["id"])
                            if saved["status"] not in {"queued", "running"}:
                                break
                            await asyncio.sleep(0.1)
                    assert saved["status"] == "completed", saved
                assert wire.summaries > 0
                async with app.store.engine.connect() as connection:
                    count = len((await connection.execute(select(compactions))).all())
                assert count > 0
                report["steps"].append(
                    {
                        "step": "manual-compaction-and-followups",
                        "summary_requests": wire.summaries,
                        "persisted_compactions": count,
                    }
                )
                await request(client, "POST", f"/v2/me/development/projects/{project_id}/stop")
                report["exit"] = 0
    except Exception as error:
        report.update(exit=1, error_type=type(error).__name__, error=str(error))
        if app is not None:
            try:
                async with app.store.engine.connect() as connection:
                    from krim_agent.storage.models import runs, turns

                    report["runs"] = [
                        dict(row)
                        for row in (
                            await connection.execute(select(runs.c.id, runs.c.status, runs.c.code))
                        ).mappings()
                    ]
                    report["turns"] = [
                        dict(row)
                        for row in (
                            await connection.execute(select(turns.c.status, turns.c.code))
                        ).mappings()
                    ]
                report["tool_results"] = []
                for run in report["runs"]:
                    snapshot = await app.store.snapshot(run["id"])
                    report["tool_results"].extend(
                        {key: event.data.get(key) for key in ("name", "status", "code")}
                        | {"value": str(event.data.get("value"))[:2000]}
                        for event in snapshot.events
                        if event.kind == "tool_end"
                    )
            except Exception:
                pass
        raise
    finally:
        if client is not None:
            await client.aclose()
        if app is not None:
            if app.development is not None and handle is not None:
                try:
                    await app.development.environments.stop(handle)
                except Exception as error:
                    report.setdefault("cleanup_errors", []).append(str(error))
            await app.close()
        for process in workers:
            try:
                if process.returncode is None:
                    process.terminate()
                    await asyncio.wait_for(process.wait(), 10)
                report.setdefault("supervisor_exit_codes", []).append(process.returncode)
            except Exception as error:
                report.setdefault("cleanup_errors", []).append(str(error))
        outcomes, blockers = await cleanup_containers(
            docker,
            image=APPROVED_IMAGE,
            owner_id=owner,
            project_id=project_id,
            environment_id="dev-" + project_id if project_id else None,
            content=(
                paths.environments / environment_id / "projects" / project_id / "content"
                if environment_id and project_id
                else None
            ),
        )
        report["container_cleanup"] = outcomes
        report["cleanup_blockers"] = blockers
        await wire.close()
        report["model_requests"] = len(wire.requests)
        report["summary_requests"] = wire.summaries
        report["fixture_errors"] = wire.errors
        # These are this script's private resources, identified before creation.
        current = await asyncio.to_thread(root.lstat)
        linked = await asyncio.to_thread(root.is_symlink)
        if (
            not blockers
            and not report.get("cleanup_errors")
            and (current.st_dev, current.st_ino) == (root_identity.st_dev, root_identity.st_ino)
            and not linked
        ):
            await asyncio.to_thread(shutil.rmtree, root)
            report["temporary_root_removed"] = True
        else:
            report["retained_root"] = str(root)
        if blockers or report.get("cleanup_errors") or wire.errors:
            report["exit"] = 1
        (output / "result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2))
        print(json.dumps(report, ensure_ascii=False, indent=2))
    if report["exit"] != 0:
        raise RuntimeError("acceptance cleanup or protocol fixture failed; inspect result.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--run-real", action="store_true")
    args = parser.parse_args()
    if not args.run_real:
        parser.error("--run-real explicitly opts into isolated host resources")
    asyncio.run(main(args.evidence))
