"""Opt-in SQLite authority + real runsc downloads; project provisioning is a fixture."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import tempfile
import time
from pathlib import Path

from sqlalchemy import update

from krim_agent.accounts.service import IdentityService
from krim_agent.contracts import ServiceError
from krim_agent.development.configuration import DevelopmentConfiguration
from krim_agent.development.service import DevelopmentService
from krim_agent.project_access.service import command_intent
from krim_agent.sandbox.configuration import SandboxConfiguration
from krim_agent.sandbox.service import SandboxService
from krim_agent.storage.sandbox_models import sandbox_projects
from tests.coding_agent.test_development_storage import prepare
from tests.coding_agent.verify_sqlite_terminal import RecordingDocker


def directories(root: Path):
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    project_root = Path(tempfile.mkdtemp(prefix="coding-download-", dir="/srv/krim-sandbox"))
    content = project_root / "content"
    content.mkdir(mode=0o700)
    os.chown(content, 65532, 65532)
    return project_root, content, project_root.stat()


async def main(root: Path, image: str) -> None:
    if os.geteuid() != 0:
        raise ValueError("trusted Docker host operator required")
    project_root, content, original = await asyncio.to_thread(directories, root)
    store = identities = sandbox = service = None
    docker = RecordingDocker(root / "created-container-ids.json")
    template = None
    observations = []
    success = False

    def record(step, **values):
        observations.append({"step": step, **values})
        (root / "results.json").write_text(json.dumps({
            "image": image, "passed": success, "observations": observations,
            "authorization": "real SQLite accounts/project grants/original CommandRequest",
            "project_provisioning": "registered content fixture; not Broker/XFS prepare",
            "application_acceptance": False,
        }, indent=2))
        print(json.dumps(observations[-1]), flush=True)

    async def completed(process):
        assert service is not None
        async with asyncio.timeout(150):
            while process.state in {"accepted", "running"}:
                await asyncio.sleep(0.1)
                process = await service.terminal.inspect(process)
        return process

    try:
        store, _, _, template = await prepare(
            root / "application.sqlite", image_id=image, creating=True, content_path=content,
        )
        identities = await IdentityService.open(store)
        actor = identities.resolve(template.authorization.actor_id)
        sandbox = SandboxService(store, identities, SandboxConfiguration())
        async with store.engine.begin() as conn:
            await conn.execute(update(sandbox_projects).where(
                sandbox_projects.c.project_id == template.environment.project_id,
            ).values(status="ready"))
        configuration = DevelopmentConfiguration(
            image_id=image, broker_environments_root=project_root,
            private_state_root=root / "private",
        )
        await asyncio.to_thread(DevelopmentService.prepare_private, configuration)
        service = DevelopmentService(store, identities, sandbox, configuration)
        service.environments.runner = docker
        service.downloads.runner = docker
        await service.start_supervisor()
        environment = await service.environments.start(template.environment)
        template = template.model_copy(update={"environment": environment})
        project = await sandbox.project(actor, environment.project_id)
        await service.grant_project(actor, environment.project_id, expected_revision=0,
                                    expires_at=time.time() + 600, requires_approval=False)
        policy = await service.configure_download_policy(
            actor, environment.project_id, "npm-packages", expected_revision=0,
            hosts=("registry.npmjs.org",), expires_at=time.time() + 600,
        )
        env_record = await service.repositories.environments.resolve(environment)
        assert env_record.network_id is None and env_record.network_policy_ref is None
        record("offline-environment", state=environment.state)

        def request(operation, command, *, wait=0.0):
            value = template.model_copy(update={
                "operation_id": operation, "command": command,
                "timeout_seconds": 120.0, "wait_seconds": wait,
                "authorization": template.authorization.model_copy(update={
                    "network_policy_ref": policy.policy_id,
                }),
            })
            intent = command_intent(value, workspace_id=project["workspace_id"])
            return value.model_copy(update={"authorization": value.authorization.model_copy(
                update={"arguments_digest": intent.digest(value.authorization)},
            )})

        installation = request(
            "authorized-install",
            "npm install --no-save --package-lock=false esbuild@0.25.12 && "
            "node -e \"const e=require('esbuild');"
            "if(e.version!=='0.25.12')process.exit(21);"
            "const r=e.transformSync('const n: number = 7',{loader:'ts'});"
            "if(!r.code.includes('7'))process.exit(22);"
            "console.log('sqlite-install-transform-ok')\"",
        )
        installed = await completed(await service.terminal.submit(installation))
        output = await service.terminal.read_output(installed, 0)
        (root / "install-output.txt").write_text(output.text)
        assert installed.state == "exited" and installed.exit_code == 0
        assert "sqlite-install-transform-ok" in output.text
        duplicate = await service.terminal.submit(installation)
        assert duplicate.process_id == installed.process_id
        record("authorized-install-and-replay", state=installed.state,
               exit_code=installed.exit_code, same_process=True)

        revocation = request(
            "revocation-download",
            "curl --fail --silent --show-error --max-time 30 -o /dev/null "
            "https://registry.npmjs.org/esbuild/0.25.12 && printf download-ready; "
            "while [ ! -f /workspace/continue-download ]; do sleep 0.1; done; "
            "if curl --fail --silent --max-time 5 -o /dev/null "
            "https://registry.npmjs.org/esbuild/0.25.12; then exit 42; "
            "else printf download-denied; fi",
        )
        active = await service.terminal.submit(revocation)
        async with asyncio.timeout(40):
            while True:
                output = await service.terminal.read_output(active, 0)
                if "download-ready" in output.text:
                    break
                active = await service.terminal.inspect(active)
                if active.state not in {"accepted", "running"}:
                    raise AssertionError("download command exited before readiness")
                await asyncio.sleep(0.1)
        row = await service.repositories.processes.get(active.process_id)
        inspected = json.loads((await docker.run(("inspect", row.execution_id))).stdout)[0]
        assert inspected["HostConfig"]["NetworkMode"] == "none"
        assert not inspected["HostConfig"]["PortBindings"]
        assert inspected["Config"]["User"] == "65532:65532"
        await service.configure_download_policy(
            actor, environment.project_id, policy.policy_id, expected_revision=1,
            hosts=policy.hosts, expires_at=policy.expires_at, revoked=True,
        )
        await asyncio.to_thread((content / "continue-download").touch)
        active = await completed(active)
        output = await service.terminal.read_output(active, 0)
        assert active.exit_code == 0 and "download-denied" in output.text
        record("committed-revocation-closes-downloads", exit_code=active.exit_code,
               network="none", published_ports=False, uid=65532)
        try:
            await service.terminal.submit(request("after-revoke", "printf forbidden"))
        except ServiceError as error:
            assert error.code == "permission_denied"
        else:
            raise AssertionError("revoked policy accepted a new command")
        record("revoked-command-denied")
        success = True
    finally:
        cleanup = []
        if service is not None:
            await service.close()
        for cid in docker.created:
            result = await docker.run(("inspect", cid))
            if result.code != 0:
                cleanup.append({"id": cid, "absent": True})
                continue
            value = json.loads(result.stdout)[0]
            labels = value["Config"].get("Labels", {})
            assert template is not None
            assert value["Id"] == cid and value["Image"] == image
            assert labels.get("io.krim.development.owner") == template.environment.owner_id
            assert labels.get("io.krim.development.project") == template.environment.project_id
            reply = await docker.run(("rm", "--force", cid))
            assert reply.code == 0
            assert (await docker.run(("inspect", cid))).code != 0
            cleanup.append({"id": cid, "removed": True})
        worker = service._worker_process if service is not None else None
        if worker is not None and worker.returncode is None:
            worker.terminate()
            try:
                await asyncio.wait_for(worker.wait(), 5)
            except TimeoutError:
                worker.kill()
                await worker.wait()
        if sandbox is not None:
            await sandbox.close()
        if identities is not None:
            await identities.close()
        if store is not None:
            await store.close()
        current = await asyncio.to_thread(project_root.lstat)
        assert (current.st_dev, current.st_ino) == (original.st_dev, original.st_ino)
        await asyncio.to_thread(shutil.rmtree, project_root)
        record("cleanup", containers=cleanup, project_removed=True,
               supervisor_exit=worker.returncode if worker is not None else None)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--image", required=True)
    args = parser.parse_args()
    asyncio.run(main(args.evidence, args.image))
