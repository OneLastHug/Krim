"""Opt-in real Broker/XFS/runsc/SDK delivery with synthetic project Secrets."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import shlex
import time
from pathlib import Path

from krim_agent.account_mcp.secrets import MasterKeyring
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.development.configuration import DevelopmentConfiguration
from krim_agent.providers.types import ModelText, ModelToolCall
from krim_agent.sandbox.configuration import SandboxConfiguration
from krim_agent.sandbox.quota import QuotaPolicy
from krim_agent.sessions.types import SessionCreate, TurnRequest
from tests.agent_core.test_account_api import initialize
from tests.coding_agent.verify_development_broker import (
    APPROVED_IMAGE,
    APPROVED_STORAGE_ROOT,
    approved_limits,
    broker_lifecycle,
    cleanup_containers,
    clear_quota,
    new_paths,
    quota_preflight,
    remove_test_root,
    require_empty_quota,
    require_platform,
    save_json,
)
from tests.coding_agent.verify_sqlite_terminal import RecordingDocker

SYNTHETIC = "synthetic-process-fixture-" + "4" * 24
CONSUMER = (
    "import hashlib,os,time,pathlib; "
    "v=os.read(int(os.environ['KRIM_SECRET_FD_0']),8193); "
    "assert v not in pathlib.Path('/proc/self/cmdline').read_bytes(); "
    "assert v not in pathlib.Path('/proc/self/environ').read_bytes(); "
    "pathlib.Path('/workspace/secret-proof').write_text(hashlib.sha256(v).hexdigest()); "
    "print(v.decode(),flush=True); time.sleep(120)"
)


class SecretModel:
    def __init__(self):
        self.calls = 0
        self.started = asyncio.Event()
        self.finish = asyncio.Event()

    async def stream(self, messages, tools):
        self.calls += 1
        if self.calls == 1:
            yield ModelToolCall(
                id="secret-command", name="shell",
                arguments={
                    "command": "exec /usr/bin/python3 -I -c " + shlex.quote(CONSUMER),
                    "secret_refs": ["fixture-token"], "timeout_seconds": 120,
                    "wait_seconds": 1.0,
                },
            )
        else:
            self.started.set()
            await self.finish.wait()
            yield ModelText(text="Synthetic Secret lifecycle verified.")

    async def close(self):
        self.finish.set()


async def verify(evidence: Path) -> dict:
    storage = require_platform(APPROVED_STORAGE_ROOT)
    await asyncio.to_thread(evidence.mkdir, mode=0o700, parents=False, exist_ok=False)
    paths = new_paths(storage)
    model = SecretModel()
    app = None
    docker = RecordingDocker(evidence / "created-container-ids.json")
    actor = project = handle = None
    content = None
    quota_created = False
    result = {"status": "running", "external_model_requests": 0, "application": "infrastructure"}
    errors = []
    try:
        async with broker_lifecycle(paths) as broker:
            try:
                token = await initialize(paths.database)
                keyring = paths.root / "fixture-vault.json"
                MasterKeyring.create(b"p" * 32).save(keyring)
                sandbox_path = paths.root / "sandbox.json"
                sandbox_path.write_text(SandboxConfiguration(
                    broker_socket=paths.socket, broker_owner_uid=0,
                    quotas=QuotaPolicy(defaults=approved_limits()),
                ).model_dump_json())
                sandbox_path.chmod(0o600)
                app = await open_application(
                    paths.database,
                    settings=Settings(
                        database=paths.database, sandbox_config_file=sandbox_path,
                        personal_mcp_key_file=keyring, max_request_bytes=262144,
                    ),
                    provider=model,
                    development_configuration=DevelopmentConfiguration(
                        image_id=APPROVED_IMAGE, broker_environments_root=paths.environments,
                        private_state_root=paths.private_state, memory_bytes=128 * 1024**2,
                        pids_limit=64,
                    ),
                )
                development = app.development
                assert development is not None and app.sandbox is not None
                assert app.identities is not None and app.sessions is not None
                development.environments.runner = docker
                actor = app.identities.authenticate("Bearer " + str(token))
                workspace = actor.memberships[0].workspace_id
                project = await app.sandbox.create_project(actor, {
                    "workspace_id": workspace, "title": "Synthetic Secret verification",
                }, request_key="secret-project")
                require_empty_quota(quota_preflight(paths, project["environment_id"]))
                content = (paths.environments / project["environment_id"] / "projects"
                           / project["project_id"] / "content")
                handle = await development.prepare_project(
                    actor, project["project_id"], request_key="prepare",
                )
                quota_created = True
                quota = await broker.call("usage", environment_id=project["environment_id"])
                usage = quota["usage"]
                assert usage["byte_limit"] == 32 * 1024**2 and usage["inode_limit"] == 1000
                await development.grant_project(
                    actor, project["project_id"], expected_revision=0,
                    expires_at=time.time() + 180, requires_approval=False,
                    actions=("shell.execute", "secret.deliver"),
                )
                await development.configure_project_secret(
                    actor, project["project_id"], "fixture-token", value=SYNTHETIC,
                    expected_revision=0, expires_at=time.time() + 180,
                )
                session = await app.sessions.create(actor, workspace, SessionCreate(
                    request_key="secret-session", target_key="target-development",
                ))
                from krim_agent.sandbox.bindings import bind_project
                from krim_agent.sandbox.models import BindProjectBody

                bound = await bind_project(app.sandbox, actor, session.id, BindProjectBody(
                    expected_revision=0, expected_session_revision=0,
                    project_id=project["project_id"],
                ), request_key="bind")
                turn = await app.sessions.submit(actor, workspace, session.id, TurnRequest(
                    text="Run the authorized synthetic Secret command.", request_key="secret-turn",
                    expected_revision=bound["session_revision"],
                ))
                async with asyncio.timeout(45):
                    while not model.started.is_set():  # noqa: ASYNC110
                        current_turn = await app.sessions.turn(
                            actor, workspace, session.id, turn.id,
                        )
                        if current_turn.run_id is not None:
                            current_run = await app.service.get(current_turn.run_id)
                            if current_run.status in {"failed", "aborted", "completed"}:
                                raise RuntimeError(
                                    "run ended before consumer: " + str(current_run.code)
                                )
                        await asyncio.sleep(.1)
                saved = await app.sessions.turn(actor, workspace, session.id, turn.id)
                snapshot = await app.store.snapshot(saved.run_id)
                process_event = next(e for e in snapshot.events if e.kind == "tool_end")
                assert process_event.data["status"] == "ok", process_event.data.get("code")
                process_id = process_event.data["value"]["process_id"]
                process = await development.repositories.processes.get(process_id)
                assert process.handle.state == "running"
                async with asyncio.timeout(10):
                    while not (content / "secret-proof").exists():  # noqa: ASYNC110
                        await asyncio.sleep(.05)
                assert (content / "secret-proof").read_text() == hashlib.sha256(
                    SYNTHETIC.encode(),
                ).hexdigest()
                output = await development.terminal.read_output(process.handle, 0)
                assert output.text == "" and SYNTHETIC not in output.model_dump_json()
                assert SYNTHETIC not in json.dumps([
                    e.model_dump(mode="json") for e in snapshot.events
                ])
                from krim_agent.development.contracts import OperationIdentity

                checkpoint, _ = await app.store.read_checkpoint(saved.run_id)
                entry = next(iter(checkpoint["entries"].values()))
                identity = OperationIdentity.model_validate(entry["identity"])
                receipt = await development.repositories.operations.lookup(identity)
                assert receipt.state == "completed" and receipt.result_ref == identity.call_id
                # Current process and native container are the actual live launch evidence.
                inspected = await docker.run(("inspect", process.execution_id))
                assert inspected.code == 0
                container = json.loads(inspected.stdout)[0]
                assert container["HostConfig"]["Runtime"] == "runsc"
                assert container["HostConfig"]["NetworkMode"] == "none"
                assert container["HostConfig"]["ReadonlyRootfs"] is True
                assert container["Config"]["User"] == "65532:65532"
                await development.configure_project_secret(
                    actor, project["project_id"], "fixture-token", value="replacement-fixture",
                    expected_revision=1, expires_at=time.time() + 180,
                )
                stopped = await development.repositories.processes.get(process_id)
                assert stopped.handle.state in {"cancelled", "exited", "failed"}
                assert (await docker.run(("inspect", process.execution_id))).code != 0
                model.finish.set()
                completed = await app.service.wait(saved.run_id)
                assert completed.status == "completed", completed.code
                result.update(status="passed", quota=usage, process_state=stopped.handle.state,
                              proof_sha256=(content / "secret-proof").read_text(),
                              run_id=saved.run_id, process_id=process_id,
                              log_payload_bytes=len(output.text.encode()),
                              native_runtime="runsc", network_mode="none")
            finally:
                model.finish.set()
                if app is not None:
                    if handle is not None:
                        try:
                            with app.development.host_cleanup():
                                await app.development.environments.stop(handle)
                        except Exception as error:
                            errors.append(type(error).__name__ + ":environment_stop")
                    try:
                        await app.close()
                    except Exception as error:
                        errors.append(type(error).__name__ + ":app_close")
                    worker = app.development._worker_process
                    if worker is not None and worker.returncode is None:
                        worker.terminate()
                        try:
                            await asyncio.wait_for(worker.wait(), 5)
                        except TimeoutError:
                            worker.kill()
                            await worker.wait()
                outcomes, blockers = await cleanup_containers(
                    docker, image=APPROVED_IMAGE,
                    owner_id=actor.principal_id if actor else None,
                    project_id=project["project_id"] if project else None,
                    environment_id=handle.environment_id if handle else None,
                    content=content,
                )
                result["cleanup_containers"] = outcomes
                errors.extend(blockers)
                if project is not None and quota_created and not errors:
                    result["quota_cleared"] = clear_quota(paths, project["environment_id"])
    except Exception as error:
        result.update(status="failed", error_type=type(error).__name__, error=str(error))
    finally:
        result["cleanup_errors"] = errors
        if not errors and result["status"] == "passed":
            remove_test_root(paths, storage)
        else:
            result["retained_root"] = str(paths.root)
        save_json(evidence, "result.json", result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--start-broker", action="store_true", required=True)
    args = parser.parse_args()
    outcome = asyncio.run(verify(args.evidence))
    print(json.dumps(outcome, indent=2))
    raise SystemExit(0 if outcome["status"] == "passed" and not outcome["cleanup_errors"] else 1)
