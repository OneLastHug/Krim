"""REST/TCP Client/CLI消费同一混合Worker账本，严格拒绝内部凭据和重复参数。"""

import asyncio
import json
import os
import socket
import sys
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
import uvicorn
from test_mixed_workspace_batch_fences import attempt_counts
from test_native_workspace_public_api import assert_public, native_api

from krim_agent.client import ClientError, KrimClient
from krim_agent.workspace_change import FileChangeSpec, WorkspacePreviewRequest
from krim_agent.workspace_directory import DirectoryChangeSpec, DirectoryPreviewRequest
from krim_agent.workspace_mixed_batch import MixedBatchRequest, MixedBatchTarget

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="需要Linux条件事务")


async def request_for(api, agent):
    base = f"/v1/agents/{agent}/workspace"
    file = await api.post(
        f"{base}/changes/preview",
        json={
            "spec": {
                "path": "file.txt",
                "operation": "write",
                "content": "data",
                "expected_sha256": None,
            },
            "request_key": "file",
        },
    )
    directory = await api.post(
        f"{base}/directory-changes/preview",
        json={
            "spec": {"path": "empty", "operation": "create", "mode": 0o750},
            "request_key": "directory",
        },
    )
    assert file.status_code == directory.status_code == 200, (file.text, directory.text)
    return {
        "action": "apply",
        "request_key": "审阅/../?confirmed=false#%",
        "targets": [
            {"source": "file", "proposal_id": file.json()["proposal_id"], "expected_revision": 0},
            {
                "source": "directory",
                "proposal_id": directory.json()["proposal_id"],
                "expected_revision": 0,
            },
        ],
    }


async def test_rest_same_fact_by_key_wait_cancel_and_original_retries(runtime, tmp_path):
    async with native_api(runtime, tmp_path) as (_app, api, agent):
        request = await request_for(api, agent)
        base = f"/v1/agents/{agent}/workspace/mixed-batches"
        started = await api.post(base, params={"confirmed": "true"}, json=request)
        assert started.status_code == 202, started.text
        identifier = started.json()["batch_id"]
        done_response = await api.post(f"{base}/{identifier}/wait")
        assert done_response.status_code == 200 and done_response.json()["status"] == "done", (
            done_response.text
        )
        done = done_response.json()
        assert_public(done)
        assert [item["source"] for item in done["items"]] == ["file", "directory"]
        for response in (
            await api.get(f"{base}/{identifier}"),
            await api.get(f"{base}/by-key", params={"request_key": request["request_key"]}),
            await api.post(base, params={"confirmed": "true"}, json=request),
            await api.post(f"{base}/{identifier}/cancel"),
        ):
            assert response.status_code in {200, 202} and response.json() == done
        assert (await api.get(f"{base}/by-key", params={"request_key": "missing"})).json() is None
        assert (await api.post(base, json={**request, "action": "undo"})).status_code == 409
        assert await attempt_counts(runtime.repository.database) == (1, 1)
        assert (tmp_path / "file.txt").read_text() == "data" and (tmp_path / "empty").is_dir()
        other = (await api.post("/v1/agents", json={"name": "default"})).json()["agent_id"]
        foreign = f"/v1/agents/{other}/workspace/mixed-batches"
        assert (await api.get(f"{foreign}/{identifier}")).status_code == 404
        assert (
            await api.get(f"{foreign}/by-key", params={"request_key": request["request_key"]})
        ).json() is None
        assert (await api.post(foreign, json=request)).status_code == 404


@pytest.mark.parametrize("extra", ["token", "owner_id", "lease_expires_at", "fence", "root"])
async def test_public_body_rejects_internal_fields_before_acceptance(runtime, tmp_path, extra):
    async with native_api(runtime, tmp_path) as (_app, api, agent):
        request = await request_for(api, agent)
        base = f"/v1/agents/{agent}/workspace/mixed-batches"
        response = await api.post(base, json={**request, extra: "forged"})
        assert response.status_code == 422
        assert (
            await api.get(f"{base}/by-key", params={"request_key": request["request_key"]})
        ).json() is None
        assert await attempt_counts(runtime.repository.database) == (0, 0)


async def test_public_query_duplicates_unsafe_keys_and_wait_budget_are_rejected(runtime, tmp_path):
    async with native_api(runtime, tmp_path) as (app, api, agent):
        request = await request_for(api, agent)
        base = f"/v1/agents/{agent}/workspace/mixed-batches"
        for params in (
            [("confirmed", "true"), ("confirmed", "false")],
            {"owner_id": "evil"},
            {"root": "/tmp"},
        ):
            response = await api.post(base, params=params, json=request)
            assert response.status_code == 422
        for params in (
            [("request_key", "one"), ("request_key", "two")],
            {"request_key": "bad\x7f"},
            {"request_key": "ok", "token": "evil"},
        ):
            assert (await api.get(f"{base}/by-key", params=params)).status_code == 422
        for timeout in ("nan", "inf", "-1", "301"):
            assert (
                await api.post(f"{base}/missing/wait", params={"timeout_seconds": timeout})
            ).status_code == 422
        assert (await api.get(f"{base}/missing", params={"confirmed": "true"})).status_code == 422
        assert (
            await api.post(f"{base}/missing/cancel", params={"owner_id": "forged"})
        ).status_code == 422
        schemas = app.openapi()["components"]["schemas"]
        assert "MixedBatchFence" not in schemas
        assert not {"token", "owner_id", "fence", "root"}.intersection(
            schemas["MixedBatchRequest"]["properties"]
        )
        assert await attempt_counts(runtime.repository.database) == (0, 0)


async def cli(url, *args):
    env = {key: value for key, value in os.environ.items() if not key.startswith("KRIM_AGENT_")}
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "krim_agent",
        "workspace",
        *map(str, args),
        "--url",
        url,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), 20)
        return process.returncode, stdout.decode(), stderr.decode()
    except BaseException:
        if process.returncode is None:
            process.kill()
        await process.communicate()
        raise


async def test_tcp_client_and_real_cli_apply_keep_undo_and_original_key(runtime, tmp_path):
    async with native_api(runtime, tmp_path) as (app, _api, _agent):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(128)
        listener.setblocking(False)
        url = f"http://127.0.0.1:{listener.getsockname()[1]}"
        server = uvicorn.Server(uvicorn.Config(app, lifespan="off", log_level="error"))
        task = asyncio.create_task(server.serve(sockets=[listener]))
        try:
            async with asyncio.timeout(10):
                while not server.started:
                    if task.done():
                        await task
                    await asyncio.sleep(0.01)

            def prepare():
                with KrimClient(url) as client:
                    agent = client.agent_spawn("default").agent_id
                    file = client.agent_workspace_preview(
                        agent,
                        WorkspacePreviewRequest(
                            spec=FileChangeSpec(
                                path="file.txt",
                                operation="write",
                                content="data",
                                expected_sha256=None,
                            ),
                            request_key="file",
                        ),
                    )
                    directory = client.agent_directory_preview(
                        agent,
                        DirectoryPreviewRequest(
                            spec=DirectoryChangeSpec(path="empty", operation="create", mode=0o750),
                            request_key="dir",
                        ),
                    )
                    request = MixedBatchRequest(
                        action="apply",
                        request_key="tcp",
                        targets=[
                            MixedBatchTarget(
                                source="file", proposal_id=file.proposal_id, expected_revision=0
                            ),
                            MixedBatchTarget(
                                source="directory",
                                proposal_id=directory.proposal_id,
                                expected_revision=0,
                            ),
                        ],
                    )
                    accepted = client.agent_workspace_start_mixed_batch(
                        agent, request, confirmed=True
                    )
                    done = client.agent_workspace_wait_mixed_batch(agent, accepted.batch_id)
                    assert done.status == "done"
                    assert client.agent_workspace_mixed_batch(agent, done.batch_id) == done
                    assert client.agent_workspace_mixed_batch_by_key(agent, "tcp") == done
                    return agent, done

            agent, applied = await asyncio.to_thread(prepare)
            code, output, errors = await cli(
                url, "mixed-batch", agent, "--batch-id", applied.batch_id
            )
            assert code == 0 and json.loads(output)["batch_id"] == applied.batch_id, errors
            code, output, errors = await cli(
                url, "mixed-batch-by-key", agent, "--request-key", "tcp"
            )
            assert code == 0 and json.loads(output)["status"] == "done", errors
            for action in ("keep", "undo"):

                def revisions():
                    with KrimClient(url) as client:
                        file = client.agent_workspace_proposal(agent, applied.items[0].proposal_id)
                        directory = client.agent_directory_query(
                            agent, kind="proposal", identifier=applied.items[1].proposal_id
                        ).proposal
                        return [file.revision, directory.revision]

                current = await asyncio.to_thread(revisions)
                request = MixedBatchRequest(
                    action=action,
                    request_key=action,
                    targets=[
                        MixedBatchTarget(
                            source=item.source,
                            proposal_id=item.proposal_id,
                            expected_revision=revision,
                        )
                        for item, revision in zip(applied.items, current, strict=True)
                    ],
                )
                path = (
                    Path(".cache/project-completion/p04-mixed-worker") / f"request-{uuid4()}.json"
                )
                path.write_text(request.model_dump_json())
                code, output, errors = await cli(
                    url, "start-mixed-batch", agent, "--request", path, "--confirm"
                )
                assert code == 0, errors
                identifier = json.loads(output)["batch_id"]
                code, output, errors = await cli(url, "wait-mixed-batch", agent, identifier)
                assert code == 0 and json.loads(output)["status"] == "done", errors
            assert not (tmp_path / "file.txt").exists() and not (tmp_path / "empty").exists()
            assert await attempt_counts(runtime.repository.database) == (3, 3)
        finally:
            server.should_exit = True
            await asyncio.wait_for(task, 15)
            listener.close()


@pytest.mark.parametrize("timeout", [float("nan"), float("inf"), -1, 301])
def test_client_invalid_wait_does_not_make_http_call(timeout):
    calls = []
    transport = httpx.MockTransport(lambda request: calls.append(request) or httpx.Response(500))
    with KrimClient("http://127.0.0.1", transport=transport) as client:
        with pytest.raises(ClientError) as rejected:
            client.agent_workspace_wait_mixed_batch("agent", "batch", timeout_seconds=timeout)
        assert rejected.value.code == "workspace_mixed_batch_timeout" and not calls
