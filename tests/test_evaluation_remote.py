"""真实 HTTP、同步/异步客户端、鉴权/目录与进度断开合约。"""

import asyncio

import pytest

from krim_agent.client import KrimClient
from krim_agent.evaluation import EvalCase, EvalDataset
from krim_agent.evaluation.jobs import EvalRemoteStartRequest
from krim_agent.evaluation.remote import AsyncEvalClient
from krim_agent.providers import ModelText


@pytest.mark.parametrize("target", ["text", "agent"])
async def test_real_tcp_sync_and_async_sdk_share_job_facts(server_url, target):
    request = EvalRemoteStartRequest(
        target=target,
        dataset=EvalDataset(
            name="remote",
            cases=[
                EvalCase(
                    id="add",
                    input="/add 2 3",
                    expected='[fake] 工具结果：{"sum": 5.0}',
                    metadata={"expected_tools": ["add"]},
                )
            ],
        ),
        scorers=["exact-match", "tool-calls"],
        request_key="remote-" + target,
    )
    async with AsyncEvalClient(server_url) as client:
        view = await client.start(request)
        assert (await client.wait(view.id)).status == "completed"
        report = await client.report(view.id)
        assert report.summary.passed == 1
        rows = await client.items(view.id, limit=1)
        assert rows.items[0].output.tools[0].arguments == {"left": 2.0, "right": 3.0}
        events = [event async for event in client.events(view.id)]
        assert [event.seq for event in events] == list(range(len(events)))
        assert events[-1].data["status"] == "completed"
        assert (await client.start(request)).id == view.id
    with KrimClient(server_url) as client:
        value = await asyncio.to_thread(client.evaluations.report, view.id)
        assert value == report
        view_again = await asyncio.to_thread(client.evaluations.get, view.id)
        assert view_again.event_count == len(events)
        await asyncio.to_thread(client.evaluations.close)
        assert await asyncio.to_thread(client.topics) is not None


async def test_invalid_catalog_and_body_cannot_inject_code(api):
    valid = {"dataset": {"name": "x", "cases": [{"id": "x", "input": "x"}]}}
    for changes in (
        {"scorers": ["__import__"]},
        {"target": "workflow"},
        {"provider_base_url": "https://invalid.example"},
        {"dataset": {"name": "x", "cases": [{"id": "x", "input": "x", "script": "print(1)"}]}},
    ):
        response = await api.post("/v1/evals", json={**valid, **changes})
        assert response.status_code == 422
    response = await api.post(
        "/v1/evals", json={**valid, "target": "agent", "agent_name": "missing"}
    )
    assert response.status_code == 404


async def test_dataset_upload_has_explicit_limit_without_expanding_chat_body(api):
    payload = {
        "dataset": {
            "name": "large",
            "cases": [{"id": str(i), "input": "x" * 1000} for i in range(80)],
        },
        "scorers": ["success"],
    }
    response = await api.post("/v1/evals", json=payload)
    assert response.status_code == 202
    await api.post("/v1/evals/" + response.json()["id"] + "/cancel")
    too_large = await api.post("/v1/topics", content=b"x" * 65537)
    assert too_large.status_code == 413


async def test_remote_cancel_saves_partial_and_invalid_cursor_does_not_create_event(api, runtime):
    entered = asyncio.Event()

    class Paused:
        name = "remote-pause"

        async def stream(self, _messages, _tools):
            yield ModelText(text="partial")
            entered.set()
            await asyncio.Event().wait()

    runtime.provider = Paused()
    response = await api.post(
        "/v1/evals",
        json={
            "dataset": {"name": "pause", "cases": [{"id": "one", "input": "x"}]},
            "scorers": ["success"],
        },
    )
    job_id = response.json()["id"]
    await asyncio.wait_for(entered.wait(), 5)
    stop = await api.post(f"/v1/evals/{job_id}/cancel")
    assert stop.json()["status"] == "cancelled"
    partial = await api.get(f"/v1/evals/{job_id}/partial-report")
    assert partial.json()["complete"] is False
    item = (await api.get(f"/v1/evals/{job_id}/items")).json()["items"][0]
    assert item["output"]["value"] == "partial" and item["output"]["status"] == "aborted"
    count = (await api.get(f"/v1/evals/{job_id}")).json()["event_count"]
    assert (await api.get(f"/v1/evals/{job_id}/stream?from_seq={count + 1}")).status_code == 416
    assert (await api.get(f"/v1/evals/{job_id}")).json()["event_count"] == count
    stream = await api.get(f"/v1/evals/{job_id}/stream")
    assert "event: eval-event" in stream.text
