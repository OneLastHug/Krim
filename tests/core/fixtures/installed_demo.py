"""在真正仓库外以独立解释器 -I 验收；不注入源码 sys.path。"""

import asyncio
import hashlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import krim_agent
from krim_agent.composition import open_application
from krim_agent.contracts import ApprovalReply
from krim_agent.sdk import RunSDK

root = Path(sys.argv[1])
root.mkdir(parents=True, exist_ok=True)
module = Path(krim_agent.__file__).resolve()
assert "site-packages" in module.parts, module
assert not str(Path.cwd()).startswith("/workspace/Krim-agent-core")
assert not any(str(x).startswith("/workspace/Krim-agent-core/src") for x in sys.path)
for name in [
    "krim_agent.evaluation.recovery",
    "krim_agent.evaluation.judge",
    "krim_agent.evaluation.conversation",
    "krim_agent.agents",
    "krim_agent.protocol",
]:
    assert importlib.util.find_spec(name) is None, name


async def main():
    database = root / "sdk.sqlite"
    async with await open_application(database) as app:
        sdk = RunSDK(app.service)
        run = await sdk.start("/add 2 3", "installed-sdk")
        await sdk.wait(run.id)
        facts = await sdk.result(run.id)
        trace = await app.service.trace(run.id)
        assert facts.text == "5" and facts.model_steps == 2 and len(facts.tools) == 1
        assert app.provider.calls == 2 and trace.complete
        await sdk.result(run.id)
        await app.service.events(run.id)
        await app.service.trace(run.id)
        assert app.provider.calls == 2
        result = {
            "module": str(module),
            "executable": sys.executable,
            "cwd": str(Path.cwd()),
            "run": facts.model_dump(mode="json"),
            "trace": trace.model_dump(mode="json"),
            "provider_calls": app.provider.calls,
        }
        approvals = []
        for allow in (True, False):
            note = await sdk.start("/note welcome", "installed-sdk-approval-" + str(allow))
            async with asyncio.timeout(3):
                while True:
                    requests = await app.service.approvals(note.id)
                    if requests:
                        break
                    await asyncio.sleep(0.01)
            request = requests[0]
            reply = ApprovalReply(
                request_key="sdk-reply-" + str(allow),
                allow=allow,
                **{key: request[key] for key in ("call_id", "arguments_hash", "tool_hash")},
            )
            await app.service.reply_approval(note.id, request["request_id"], reply)
            await sdk.wait(note.id)
            again = await app.service.reply_approval(note.id, request["request_id"], reply)
            facts = await sdk.result(note.id)
            assert again["replayed"] is True
            assert facts.run.status == ("completed" if allow else "failed")
            assert facts.tools[0].executor_started is allow
            assert sum(event.kind == "approval_reply" for event in facts.events) == 1
            approvals.append(facts.model_dump(mode="json"))
        app.provider.entered.clear()
        slow = await sdk.start("/slow", "installed-sdk-cancel")
        await app.provider.entered.wait()
        assert app.provider.active == 1
        assert (await sdk.cancel(slow.id)).status == "aborted"
        assert app.provider.active == 0
        result.update(approvals=approvals, cancelled_run=slow.id)
    await asyncio.gather(app.close(), app.close())
    assert app.provider.closed and app.store.closed
    assert all(task.done() for task in app.service.tasks.values())
    assert app.provider.streams_closed == app.provider.calls
    result["cleanup"] = {
        "provider_closed": app.provider.closed,
        "database_closed": app.store.closed,
        "tasks_done": all(task.done() for task in app.service.tasks.values()),
        "active": app.provider.active,
        "provider_calls": app.provider.calls,
        "streams_closed": app.provider.streams_closed,
    }
    return result, database, run.id


result, database, run_id = asyncio.run(main())
before = hashlib.sha256(database.read_bytes()).hexdigest()
cold = subprocess.run(
    [
        sys.executable,
        "-I",
        "-B",
        "-m",
        "krim_agent",
        "result",
        "--database",
        str(database),
        "--id",
        run_id,
    ],
    capture_output=True,
    text=True,
    check=True,
)
assert json.loads(cold.stdout)["run"]["id"] == run_id
assert hashlib.sha256(database.read_bytes()).hexdigest() == before
result["cold_cli_zero_writes"] = True
(root / "installed-demo.json").write_text(json.dumps(result, ensure_ascii=False, indent=2))
print(
    json.dumps(
        {
            "module": str(module),
            "run_id": run_id,
            "provider_calls": 2,
            "cold_cli_zero_writes": True,
        }
    )
)
