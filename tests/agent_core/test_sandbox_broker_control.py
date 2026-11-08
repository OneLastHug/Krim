"""Broker服务接入的合成测试；真实XFS实验单独记录。"""

import base64
import json
import os

import pytest
from test_sandbox_control import managed

from krim_agent.contracts import ServiceError
from krim_agent.sandbox.broker_client import BrokerClient


class FakeBroker:
    def __init__(self):
        self.calls = []
        self.fail_once = False

    async def call(self, action, **arguments):
        self.calls.append((action, arguments))
        if self.fail_once:
            self.fail_once = False
            raise ServiceError("sandbox_backend_unavailable", 503)
        if action == "usage":
            return {
                "usage": {
                    "environment_bytes": 4096,
                    "environment_inodes": 3,
                    "byte_limit": 5 * 1024**3,
                    "inode_limit": 100000,
                }
            }
        if action == "list":
            return {"path": arguments["path"], "entries": [{"name": "hello.txt", "type": "file"}]}
        if action == "read":
            return {
                "path": arguments["path"],
                "data": base64.b64encode(b"hello").decode(),
                "size": 5,
                "eof": True,
            }
        return {"status": "ready", "environment_id": arguments["environment_id"]}


async def test_project_provision_is_durable_and_files_use_broker(tmp_path):
    async with managed(tmp_path / "broker.sqlite") as (_, _, service, admin, member, *_):
        broker = FakeBroker()
        service.broker = broker
        project = await service.create_project(
            member,
            {
                "workspace_id": member.memberships[0].workspace_id,
                "title": "目录项目",
            },
            request_key="create",
        )
        result = await service.provision(
            member, project_id=project["project_id"], request_key="prepare"
        )
        assert result["status"] == "ready"
        assert result["runtime_available"] is False
        assert [a for a, _ in broker.calls] == ["provision_environment", "provision_project"]
        assert (
            await service.provision(member, project_id=project["project_id"], request_key="prepare")
            == result
        )
        assert len(broker.calls) == 2
        current = await service.environment(member)
        assert current["usage"]["environment_bytes"] == 4096
        assert current["enforcement"] == "xfs_project_quota"
        listing = await service.directory(member, project["project_id"])
        assert listing["entries"][0]["name"] == "hello.txt"
        chunk = await service.read_file(member, project["project_id"], path="hello.txt")
        assert base64.b64decode(chunk["data"]) == b"hello"
        with pytest.raises(ServiceError, match="not_found"):
            await service.read_file(admin, project["project_id"], path="hello.txt")
        with pytest.raises(ServiceError, match="key_conflict"):
            await service.provision(member, request_key="prepare")


async def test_provision_failure_keeps_original_intent_for_safe_preparation(tmp_path):
    async with managed(tmp_path / "retry.sqlite") as (_, _, service, _, member, *_):
        broker = FakeBroker()
        broker.fail_once = True
        service.broker = broker
        with pytest.raises(ServiceError, match="sandbox_backend_unavailable"):
            await service.provision(member, request_key="prepare")
        operation = await service.operation(member, "prepare")
        assert operation["status"] == "pending"
        env_id = operation["result"]["environment_id"]
        assert (await service.environment(member))["status"] == "provisioning"
        ready = await service.provision(member, request_key="prepare")
        assert ready["environment_id"] == env_id
        assert (await service.operation(member, "prepare"))["status"] == "completed"


@pytest.fixture
def short_socket_directory():
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory(prefix="krim-sock-", dir="/tmp") as root:
        yield Path(root)


async def test_broker_client_checks_socket_owner_and_protocol(short_socket_directory):
    import asyncio

    path = short_socket_directory / "broker.sock"

    async def handle(reader, writer):
        request = json.loads(await reader.readline())
        assert request == {"action": "health", "arguments": {}}
        writer.write(b'{"result":{"storage_available":true}}\n')
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_unix_server(handle, path=path)
    path.chmod(0o600)
    try:
        async with server:
            result = await BrokerClient(path, owner_uid=os.getuid()).call("health")
            assert result["storage_available"] is True
            with pytest.raises(ServiceError, match="sandbox_backend_unavailable"):
                await BrokerClient(path, owner_uid=os.getuid() + 1).call("health")
            path.chmod(0o666)
            with pytest.raises(ServiceError, match="sandbox_backend_unavailable"):
                await BrokerClient(path, owner_uid=os.getuid()).call("health")
    finally:
        path.unlink()


async def test_file_result_is_not_returned_after_authorization_changes(tmp_path):
    async with managed(tmp_path / "changed.sqlite") as (_, identities, service, admin, member, *_):
        broker = FakeBroker()
        service.broker = broker
        project = await service.create_project(
            member,
            {"workspace_id": member.memberships[0].workspace_id, "title": "撤权测试"},
            request_key="create",
        )
        await service.provision(member, project_id=project["project_id"], request_key="prepare")
        original = broker.call

        async def changed(action, **arguments):
            response = await original(action, **arguments)
            if action == "read":
                current = await identities.account(admin, member.principal_id)
                await identities.update_account(
                    admin,
                    member.principal_id,
                    {"expected_revision": current.revision, "status": "disabled"},
                    request_key="disable-while-reading",
                )
            return response

        broker.call = changed
        with pytest.raises(ServiceError, match="unauthorized"):
            await service.read_file(member, project["project_id"], path="hello.txt")


async def test_http_provision_and_reads_use_the_selected_broker(tmp_path):
    import httpx
    from test_account_api import initialize

    from krim_agent.api.server import create_app
    from krim_agent.composition import open_application
    from krim_agent.config import Settings

    database = tmp_path / "http.sqlite"
    token = await initialize(database)
    configuration = tmp_path / "sandbox.json"
    configuration.write_text("{}")
    configuration.chmod(0o600)
    async with await open_application(
        database, settings=Settings(database=database, sandbox_config_file=configuration)
    ) as app:
        app.sandbox.broker = FakeBroker()
        api = create_app(application=app)
        async with (
            api.router.lifespan_context(api),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api),
                base_url="http://127.0.0.1",
                headers={"Authorization": "Bearer " + token},
            ) as client,
        ):
            me = (await client.get("/v2/me")).json()
            created = await client.post(
                "/v2/me/sandbox/projects",
                json={
                    "request_key": "project",
                    "workspace_id": me["workspaces"][0]["workspace_id"],
                    "title": "HTTP目录测试",
                },
            )
            assert created.status_code == 201, created.text
            project_id = created.json()["project_id"]
            prepared = await client.post(
                f"/v2/me/sandbox/projects/{project_id}/provision", json={"request_key": "prepare"}
            )
            assert prepared.status_code == 200, prepared.text
            assert prepared.json()["status"] == "ready"
            listing = await client.get(f"/v2/me/sandbox/projects/{project_id}/files")
            assert listing.status_code == 200, listing.text
            assert listing.json()["entries"][0]["name"] == "hello.txt"
            chunk = await client.get(
                f"/v2/me/sandbox/projects/{project_id}/file", params={"path": "hello.txt"}
            )
            assert chunk.status_code == 200 and base64.b64decode(chunk.json()["data"]) == b"hello"
            no_command = await client.post(
                f"/v2/me/sandbox/projects/{project_id}/exec", json={"command": "must-not-run"}
            )
            assert no_command.status_code == 404


async def test_pending_provision_cannot_apply_an_obsolete_global_policy(tmp_path):
    from krim_agent.sandbox.configuration import SandboxConfiguration
    from krim_agent.sandbox.service import SandboxService

    async with managed(tmp_path / "policy.sqlite") as (store, identities, service, _, member, *_):
        broker = FakeBroker()
        broker.fail_once = True
        service.broker = broker
        with pytest.raises(ServiceError, match="sandbox_backend_unavailable"):
            await service.provision(member, request_key="prepare")
        replacement = SandboxService(
            store,
            identities,
            SandboxConfiguration.model_validate(
                {"quotas": {"defaults": {"environment_bytes": 2 * 1024**3}}}
            ),
        )
        replacement.broker = broker
        try:
            with pytest.raises(ServiceError, match="revision_conflict"):
                await replacement.provision(member, request_key="prepare")
            assert len(broker.calls) == 1
        finally:
            await replacement.close()


async def test_unknown_writer_blocks_new_writes_but_not_reading_saved_files(tmp_path):
    from sqlalchemy import update

    from krim_agent.storage.sandbox_models import sandbox_projects

    async with managed(tmp_path / "blocked-read.sqlite") as (store, _, service, _, member, *_):
        broker = FakeBroker()
        service.broker = broker
        project = await service.create_project(
            member,
            {"workspace_id": member.memberships[0].workspace_id, "title": "保留成果"},
            request_key="project",
        )
        await service.provision(member, project_id=project["project_id"], request_key="prepare")
        await store._write(
            lambda conn: conn.execute(
                update(sandbox_projects)
                .where(sandbox_projects.c.project_id == project["project_id"])
                .values(status="blocked")
            )
        )
        chunk = await service.read_file(member, project["project_id"], path="hello.txt")
        assert base64.b64decode(chunk["data"]) == b"hello"
        assert chunk["may_change"] is True
        with pytest.raises(ServiceError, match="workspace_busy"):
            await service.provision(
                member, project_id=project["project_id"], request_key="new-prepare"
            )
