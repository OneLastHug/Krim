"""Model-facing background job controls reuse session authorization and approvals."""

import asyncio
import base64
import json

from test_sandbox_command import CommandBroker, approval, command_app, finished


def response(tool_name=None, arguments=None, call_id="task-control"):
    delta = (
        {"content": "后台任务结果已记录"}
        if tool_name is None
        else {
            "tool_calls": [{
                "index": 0,
                "id": call_id,
                "type": "function",
                "function": {"name": tool_name, "arguments": json.dumps(arguments)},
            }]
        }
    )
    frames = []
    for value, finish in (
        (delta, None),
        ({}, "stop" if tool_name is None else "tool_calls"),
    ):
        frames.append("data: " + json.dumps({
            "choices": [{"index": 0, "delta": value, "finish_reason": finish}]
        }) + "\n\n")
    return ("".join(frames) + "data: [DONE]\n\n").encode()


class LiveLogBroker(CommandBroker):
    def __init__(self):
        super().__init__()
        self.waiting = True
        self.log_calls = []

    async def call(self, action, **arguments):
        if action == "command_logs":
            self.log_calls.append(arguments)
            self.waiting = False
            return {"stdout": base64.b64encode(b"job progress").decode(), "stderr": ""}
        return await super().call(action, **arguments)


async def test_model_can_wait_read_live_logs_and_observe_finished_task(
    tmp_path, wire, monkeypatch
):
    broker = LiveLogBroker()
    async with command_app(
        tmp_path, wire, monkeypatch, real_broker=broker,
        tool_name="sandbox_task_start", bind_session=True,
    ) as (app, _broker, _actor, _project, run_id, request):
        steps = [
            ("sandbox_task_wait", {"seconds": 1}),
            ("sandbox_task_log", {"max_bytes": 8}),
            ("sandbox_task_status", {}),
        ]
        handle = None
        index = 0

        def continue_model(body, _request_index):
            nonlocal handle, index
            if handle is None:
                value = json.loads(body["messages"][-1]["content"])
                handle = value["operation_id"]
            if index == len(steps):
                return response()
            name, arguments = steps[index]
            index += 1
            return response(name, {"operation_id": handle, **arguments}, f"task-control-{index}")

        monkeypatch.setattr(wire, "response", continue_model)
        await app.service.reply_approval(run_id, request["request_id"], approval(request))
        view = await finished(app, run_id)
        assert view.status == "completed"
        ends = [
            event.data for event in await app.service.events(run_id) if event.kind == "tool_end"
        ]
        assert [item["name"] for item in ends] == ["sandbox_task_start", *[s[0] for s in steps]]
        assert ends[0]["value"]["status"] == "accepted"
        assert ends[1]["value"]["status"] == "running"
        assert ends[2]["value"]["status"] == "running"
        assert base64.b64decode(ends[2]["value"]["stdout"]) == b"progress"
        assert ends[3]["value"]["status"] == "finished"
        assert ends[3]["value"]["verification"] == "not_evaluated"
        assert len(broker.starts) == 1
        assert len(broker.log_calls) == 1
        assert all(poll["renew"] is False for poll in broker.statuses)


async def test_model_cancel_requires_separate_approval_and_stops_original_job(
    tmp_path, wire, monkeypatch
):
    broker = CommandBroker()
    broker.waiting = True
    async with command_app(
        tmp_path, wire, monkeypatch, real_broker=broker,
        tool_name="sandbox_task_start", bind_session=True,
    ) as (app, _broker, actor, project, run_id, request):
        control_sent = False

        def continue_model(body, _request_index):
            nonlocal control_sent
            if control_sent:
                return response()
            control_sent = True
            handle = json.loads(body["messages"][-1]["content"])["operation_id"]
            return response(
                "sandbox_task_cancel",
                {"operation_id": handle, "expected_generation": 0},
            )

        monkeypatch.setattr(wire, "response", continue_model)
        await app.service.reply_approval(run_id, request["request_id"], approval(request))
        async with asyncio.timeout(30):
            while True:
                requests = await app.service.approvals(run_id)
                cancellation = next(
                    (item for item in requests if item["name"] == "sandbox_task_cancel"), None
                )
                if (
                    cancellation is not None
                    and (await app.service.get(run_id)).status == "awaiting"
                ):
                    break
                await asyncio.sleep(0.02)
        assert broker.cancelled is False
        assert cancellation["risk"] == "write"
        reply = approval(cancellation).model_copy(update={"request_key": "approve-task-cancel"})
        await app.service.reply_approval(run_id, cancellation["request_id"], reply)
        assert (await finished(app, run_id)).status == "completed"
        assert broker.cancelled is True
        assert len(broker.starts) == 1
        assert all(poll["renew"] is False for poll in broker.statuses)
        assert (await app.sandbox.project(actor, project["project_id"]))["status"] == "ready"
        events = await app.service.events(run_id)
        assert any(event.kind == "sandbox_task_cancelled" for event in events)
