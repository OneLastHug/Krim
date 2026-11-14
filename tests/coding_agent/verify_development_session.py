"""Opt-in SDK/session/API plus real Docker terminal integration.

Model responses are deterministic local inputs, not paid gateway calls. Project
files use the existing ProjectFiles implementation in a new fixture directory;
quota provision replies are a fixture, so this is not the final website acceptance.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
import time
from pathlib import Path

import httpx

from krim_agent.api.server import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import ApprovalReply
from krim_agent.development.configuration import DevelopmentConfiguration
from krim_agent.providers.types import ModelText, ModelToolCall
from krim_agent.sandbox.configuration import SandboxConfiguration
from tests.agent_core.test_account_api import initialize
from tests.coding_agent.test_development_entrypoint import ProjectBroker
from tests.coding_agent.verify_sqlite_terminal import RecordingDocker


class RealFileBroker(ProjectBroker):
    async def call(self, action, **values):
        result = await super().call(action, **values)
        if action == "provision_project":
            content = (
                self.root / values["environment_id"] / "projects" / values["project_id"] / "content"
            )
            os.chown(content, 65532, 65532)
        return result


class SessionScript:
    def __init__(self):
        self.calls = 0

    async def stream(self, messages, tools):
        self.calls += 1
        last = messages[-1]
        if last.role != "tool":
            yield ModelToolCall(
                id="shell-" + str(self.calls),
                name="dev_shell",
                arguments={
                    "command": "printf 'sdk-real-write\\n' >> proof.txt",
                    "wait_seconds": 2.0,
                },
            )
        elif self.calls % 3 == 2:
            yield ModelToolCall(
                id="read-" + str(self.calls), name="dev_read", arguments={"path": "proof.txt"}
            )
        else:
            yield ModelText(text="File written and read through the project tools.")

    async def close(self):
        pass


def directories(root: Path) -> Path:
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    project_root = Path(tempfile.mkdtemp(prefix="coding-agent-sdk-", dir="/srv/krim-sandbox"))
    environments = project_root / "environments"
    environments.mkdir(mode=0o700)
    return environments


async def main(root: Path, image: str) -> None:
    environments = await asyncio.to_thread(directories, root)
    database = root / "application.sqlite"
    token = await initialize(database)
    sandbox_config = root / "sandbox.json"
    sandbox_config.write_text(
        SandboxConfiguration(broker_socket=root / "fixture.sock").model_dump_json()
    )
    sandbox_config.chmod(0o600)
    configuration = DevelopmentConfiguration(
        image_id=image, broker_environments_root=environments, private_state_root=root / "private"
    )
    provider = SessionScript()
    app = await open_application(
        database,
        settings=Settings(database=database, sandbox_config_file=sandbox_config),
        provider=provider,
        development_configuration=configuration,
    )
    assert app.development is not None and app.sandbox is not None
    development = app.development
    broker = RealFileBroker(environments)
    app.sandbox.broker = broker
    docker = RecordingDocker(root / "created-container-ids.json")
    development.environments.runner = docker
    observations = []
    owner = None
    project_id = None

    def record(label, **values):
        observations.append({"step": label, **values})
        (root / "results.json").write_text(
            json.dumps(
                {
                    "image": image,
                    "observations": observations,
                    "model": "local scripted ModelProvider via real ai.Agent",
                    "broker": "real ProjectFiles; quota provisioning fixture",
                    "full_project_acceptance": False,
                },
                indent=2,
            )
        )
        print(json.dumps(observations[-1]), flush=True)

    api = create_app(application=app)
    try:
        async with (
            api.router.lifespan_context(api),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api),
                base_url="http://127.0.0.1",
                headers={"Authorization": "Bearer " + token},
                timeout=60,
            ) as client,
        ):
            me = (await client.get("/v2/me")).json()
            owner = me["principal_id"]
            workspace = me["workspaces"][0]["workspace_id"]
            project_response = await client.post(
                "/v2/me/sandbox/projects",
                json={
                    "request_key": "sdk-project",
                    "workspace_id": workspace,
                    "title": "SDK integration",
                },
            )
            assert project_response.status_code == 201, project_response.text
            project = project_response.json()
            project_id = project["project_id"]
            prepared = await client.post(
                f"/v2/me/development/projects/{project_id}/prepare", json={"request_key": "prepare"}
            )
            assert prepared.status_code == 200, prepared.text
            record("project-prepared", state=prepared.json()["state"])
            for index, approval in enumerate([False, True]):
                grant = await client.put(
                    f"/v2/me/development/projects/{project_id}/grant",
                    json={
                        "expected_revision": index,
                        "expires_at": time.time() + 600,
                        "requires_approval": approval,
                    },
                )
                assert grant.status_code == 200, grant.text
                session_response = await client.post(
                    f"/v2/workspaces/{workspace}/sessions",
                    json={
                        "request_key": "session-" + str(index),
                        "target_key": "target-development",
                    },
                )
                assert session_response.status_code == 201, session_response.text
                session = session_response.json()
                bound = await client.put(
                    f"/v2/me/sandbox/sessions/{session['id']}/project",
                    json={
                        "request_key": "bind-" + str(index),
                        "expected_revision": 0,
                        "expected_session_revision": 0,
                        "project_id": project_id,
                    },
                )
                assert bound.status_code == 200, bound.text
                turn_response = await client.post(
                    f"/v2/workspaces/{workspace}/sessions/{session['id']}/turns",
                    json={
                        "request_key": "task-" + str(index),
                        "text": "Write and read the project proof",
                        "expected_revision": bound.json()["session_revision"],
                    },
                )
                assert turn_response.status_code == 202, turn_response.text
                actor = app.identities.authenticate("Bearer " + token)
                turn = turn_response.json()
                async with asyncio.timeout(60):
                    while True:
                        saved_turn = await app.sessions.turn(
                            actor, workspace, session["id"], turn["id"]
                        )
                        if saved_turn.run_id:
                            run = await app.service.get(saved_turn.run_id)
                            if run.status == "awaiting":
                                assert approval
                                request = (await app.service.approvals(run.id))[0]
                                await app.service.reply_approval(
                                    run.id,
                                    request["request_id"],
                                    ApprovalReply(
                                        request_key="approval-" + str(index),
                                        call_id=request["call_id"],
                                        tool_hash=request["tool_hash"],
                                        arguments_hash=request["arguments_hash"],
                                        allow=True,
                                    ),
                                )
                            elif run.status in {"completed", "failed", "aborted"}:
                                break
                        await asyncio.sleep(0.05)
                snapshot = await app.store.snapshot(run.id)
                (root / f"run-{index}.json").write_text(
                    json.dumps(
                        {
                            "status": run.status,
                            "code": run.code,
                            "events": [event.model_dump(mode="json") for event in snapshot.events],
                        },
                        indent=2,
                    )
                )
                assert run.status == "completed", run.code
                content = (
                    environments / project["environment_id"] / "projects" / project_id / "content"
                )
                assert (content / "proof.txt").read_text().splitlines() == ["sdk-real-write"] * (
                    index + 1
                )
                record("session-task", approval=approval, run_status=run.status, effects=index + 1)
            assert provider.calls == 6
            stopped = await client.post(f"/v2/me/development/projects/{project_id}/stop")
            assert stopped.status_code == 200, stopped.text
            record("environment-stop", state=stopped.json()["state"])
    finally:
        await app.close()
        process = development._worker_process
        if process is not None and process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), 5)
            except TimeoutError:
                process.kill()
                await process.wait()
        cleanup = []
        for cid in docker.created:
            inspected = await docker.run(("inspect", cid))
            if inspected.code != 0:
                cleanup.append({"id": cid, "absent": True})
                continue
            value = json.loads(inspected.stdout)[0]
            labels = value["Config"]["Labels"]
            if (
                value["Id"] != cid
                or value["Image"] != image
                or labels.get("io.krim.development.owner") != owner
                or labels.get("io.krim.development.project") != project_id
            ):
                cleanup.append({"id": cid, "binding_mismatch": True})
                continue
            reply = await docker.run(("rm", "--force", cid))
            cleanup.append({"id": cid, "exit_code": reply.code})
        broker.files.close()
        record("cleanup", containers=cleanup)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--image", required=True)
    args = parser.parse_args()
    asyncio.run(main(args.evidence, args.image))
