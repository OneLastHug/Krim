"""D4消费者夹具：复用真实TCP Provider、Store和Runner的既有装配。"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import socket
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast


def _fixture_imports() -> Any:
    if __name__ == "__main__":
        # 子进程只导入C/source；源码注入不冒充安装包资格。
        source = Path(os.environ["KRIM_D4_ROOT"]) / "source"
        sys.path.insert(0, str(source / "src"))
        sys.path.insert(0, str(source / "tests/agent_core"))
    from test_tool_pipeline import application_case, http_suite

    return application_case, http_suite


application_case, http_suite = _fixture_imports()
HTTPFixture = cast(Any, http_suite)().HTTPFixture
frame, choice = cast(Any, http_suite)().frame, cast(Any, http_suite)().choice


class WorkspaceWire:
    """真实TCP协议夹具；仅产生模型提议，不伪造Runner/Artifact/核验结果。"""

    def __init__(self, mode: str = "pass-next-turn") -> None:
        self._wire = HTTPFixture()
        self._wire.script = self.script
        self.case = mode
        self.summaries: list[Any] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._wire, name)

    async def __aenter__(self) -> WorkspaceWire:
        await self._wire.__aenter__()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self._wire.__aexit__(*args)

    def script(self, body: dict[str, Any]) -> list[bytes]:
        messages = body["messages"]
        if "tools" not in body:
            pairs = dict(re.findall(r"SENTINEL_([0-9]+)=([0-9]+)", json.dumps(messages)))
            answer = ";".join("SENTINEL_" + k + "=" + pairs[k] for k in sorted(pairs))
            self.summaries.append({"request": body, "answer": answer})
            return self.answer(answer or "完整历史已概括")
        current = max(i for i, item in enumerate(messages) if item["role"] == "user")
        text = messages[current]["content"]
        tools = [item for item in messages[current + 1 :] if item["role"] == "tool"]
        if text.startswith("SENTINEL_"):
            return self.answer(text.split(" ", 1)[0])
        if text.startswith("D4_NEXT "):
            if tools:
                return self.answer("已按原revision读取真实文件")
            return self.call(
                "artifact_read",
                {
                    "revision_id": text.split()[1],
                    "path": "answer.json",
                    "offset": 0,
                    "length": 2048,
                },
                0,
            )
        if not text.startswith("D4_TASK"):
            return self.answer("没有授权执行要求")
        if len(tools) == 0:
            return self.call(
                "workspace_write",
                {
                    "expected_revision": 0,
                    "path": "calc.py",
                    "expected_file_hash": None,
                    "offset": 0,
                    "delete_length": 0,
                    "replacement": "def add(left, right):\n    return left + right\n",
                },
                0,
            )
        if len(tools) == 1:
            source = (
                "import unittest, json\nfrom calc import add\n"
                "class ActualTest(unittest.TestCase):\n"
                "    def test_answer(self):\n"
                "        self.assertEqual(add(20, 22), 42)\n"
                "        with open('answer.json', 'w') as output:\n"
                "            json.dump({'answer': 42}, output)\n"
            )
            if self.case == "zero-tests":
                source = "# 合成零发现负例，不含真实秘密\nVALUE = 42\n"
            elif self.case == "nonzero-exit":
                source = source.replace("add(20, 22), 42", "add(20, 22), -1")
            return self.call(
                "workspace_write",
                {
                    "expected_revision": 1,
                    "path": "test_calc.py",
                    "expected_file_hash": None,
                    "offset": 0,
                    "delete_length": 0,
                    "replacement": source,
                },
                1,
            )
        if len(tools) == 2:
            return self.call(
                "command_run",
                {
                    "expected_revision": 2,
                    "program": "python",
                    "argv": ["-m", "unittest", "discover", "-s", ".", "-p", "test_*.py"],
                },
                2,
            )
        return self.answer("模型结束仅为提议，实际完成由Verifier决定")

    @staticmethod
    def answer(text: str) -> list[bytes]:
        return [
            frame(choice({"content": text})),
            frame(choice(finish="stop")),
            frame(
                {
                    "choices": [],
                    "usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5},
                }
            ),
            frame("[DONE]"),
        ]

    @staticmethod
    def call(name: str, arguments: dict[str, Any], step: int) -> list[bytes]:
        return [
            frame(
                choice(
                    {
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": "d4-call-" + str(step),
                                "type": "function",
                                "function": {"name": name, "arguments": json.dumps(arguments)},
                            }
                        ]
                    }
                )
            ),
            frame(choice(finish="tool_calls")),
            frame("[DONE]"),
        ]


async def workspace_application(data_case: Any, tmp_path: Path, **kwargs: Any) -> Any:
    """不复制执行/存储实现；每次材料、DB和资源都由本轮根拥有。"""
    configuration = kwargs.pop("configuration", None)
    if configuration is None:
        value = await cast(Any, application_case)(data_case, tmp_path, **kwargs)
    else:
        from test_session_read_sides import Connections

        from krim_agent.composition import open_application
        from krim_agent.providers.openai_compatible import (
            OpenAICompatibleConfig,
            OpenAICompatibleProvider,
        )

        await data_case[0].close()
        notes = data_case[1]
        provider = kwargs.pop("provider", None)
        wire = kwargs.pop("wire", None)
        if provider is None:
            wire = await (wire or HTTPFixture()).__aenter__()

            class ClosingWire:
                async def close(self) -> None:
                    await wire.__aexit__(None, None, None)

            notes["owned"].append(ClosingWire())
            provider = OpenAICompatibleProvider(
                OpenAICompatibleConfig(
                    base_url=wire.url,
                    model="fixture",
                    max_tool_calls=configuration.profile.max_tools_per_step,
                )
            )
            notes["owned"].append(provider)
            notes.setdefault("observed_providers", []).append(provider)
        database = Path(kwargs.pop("database", notes["database"]))
        assert database.is_relative_to(Path(os.environ["KRIM_D4_ROOT"]))
        app = await open_application(
            database, provider=provider, workspace_profile=configuration, **kwargs
        )
        notes["owned"].append(app)
        notes.setdefault("runners", []).append(app.runner)
        notes.setdefault("store_probes", []).append((app.store, cast(Any, Connections)(app.store)))
        value = app, wire, provider
    from krim_agent.api import workspace as api
    from krim_agent.sdk import workspace as sdk
    from krim_agent.workspace import verification

    root = Path(os.environ["KRIM_D4_ROOT"]) / "source/src"

    def read_consumers() -> Any:
        consumers: dict[str, Any] = {}
        for module in [api, sdk, verification]:
            assert module.__file__ is not None
            path = Path(module.__file__)
            assert path.is_relative_to(root)
            consumers[module.__name__] = {
                "path": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        return consumers

    data_case[1]["d4_consumers"] = await asyncio.to_thread(read_consumers)
    return value


@asynccontextmanager
async def serving(application: Any) -> Any:
    """有界真实API TCP服务器；不新建Application或资源owner。"""
    import uvicorn

    from krim_agent.api.server import create_app

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(
            create_app(application=application),
            lifespan="on",
            access_log=False,
            log_config=None,
            ws="none",
        )
    )
    task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        async with asyncio.timeout(5):
            while not server.started:
                if task.done():
                    await task
                    raise AssertionError("API未启动")
                await asyncio.sleep(0.01)
        yield "http://127.0.0.1:" + str(port)
    finally:
        server.should_exit = True
        await task
        listener.close()


async def command_consumer(
    app: Any,
    notes: dict[str, Any],
    key: str,
    *,
    ambiguous: Any = False,
) -> Any:
    """定向真实command消费者；实际Store关联/审批/Runner，不假造Receipt或revision。"""
    import base64
    import time
    import uuid

    from krim_agent.contracts import encoded
    from krim_agent.security import LOCAL
    from krim_agent.sessions.types import SessionCreate
    from krim_agent.tools.core import Invocation
    from krim_agent.tools.executor import ToolContext
    from krim_agent.workspace.types import (
        Completion,
        ExecutionAccess,
        InputFile,
        InputSnapshotRequest,
        WorkspaceTurnRequest,
    )

    async with app.service._admission:
        session = await app.sessions.create(
            LOCAL, "local", SessionCreate(request_key=key, target_key="target-workspace")
        )
        access = ExecutionAccess(LOCAL, "local", session.id)
        initial = await app.artifacts.head(access)
        source = (
            "import unittest\nclass Actual(unittest.TestCase):\n"
            "    def test_ok(self):\n        self.assertEqual(20+22,42)\n"
        )
        if ambiguous:
            source += "        print('Ran 2 tests in 0.001s')\n"
        content = source.encode()
        seeded = await app.artifacts.input(
            access,
            InputSnapshotRequest(
                request_key=key + ".input",
                expected_head_revision=0,
                files=(
                    InputFile(
                        path="test_actual.py",
                        data=base64.b64encode(content).decode(),
                        sha256=hashlib.sha256(content).hexdigest(),
                    ),
                ),
            ),
        )
        turn = await app.sessions.submit(
            LOCAL,
            "local",
            session.id,
            WorkspaceTurnRequest(
                request_key=key + ".turn",
                text="真实命令故障接缝",
                expected_revision=0,
                completion=Completion(),
            ),
        )
        request, _ = await app.store.turn_request(turn.id)
        binding = app.service.bindings["target-workspace"]
        run, fresh = await app.store.begin_turn_run(
            turn.id, request, json.loads(binding.descriptor_json), binding.settings
        )
        assert fresh
        await app.store.set_running(run.id)
    notes.setdefault("manual_runs", []).append((app.store, run.id))
    frame_value = await app.work.prepare(run.id)
    deadline = asyncio.get_running_loop().time() + max(
        0, run.created_at + frame_value.conditions["runtime"]["run_timeout_seconds"] - time.time()
    )
    scope = app.work.scope(frame_value, binding.assert_current, deadline)
    spec = binding.loop.executor.registry.get("command_run")
    invocation = Invocation(
        run.id,
        str(uuid.uuid4()),
        "command-consumer",
        0,
        "command_run",
        encoded(
            {
                "expected_revision": seeded.revision,
                "program": "python",
                "argv": ["-m", "unittest", "discover", "-s", ".", "-p", "test_*.py"],
                "cwd": ".",
            }
        ),
        spec.fingerprint,
    )
    context = ToolContext(
        lambda kind, data: app.store.append(run.id, kind, data),
        app.service._approve,
        90,
        binding.assert_current,
        scope,
    )
    return run, access, seeded, initial, invocation, context, binding.loop.executor


async def crash_worker(database: Path, marker: Path, configuration_path: Path) -> None:
    from krim_agent.composition import open_application
    from krim_agent.contracts import ApprovalReply
    from krim_agent.execution.linux import SandboxRunner
    from krim_agent.providers.openai_compatible import (
        OpenAICompatibleConfig,
        OpenAICompatibleProvider,
    )
    from krim_agent.security import LOCAL
    from krim_agent.sessions.types import SessionCreate
    from krim_agent.workspace.targets import WorkspaceConfiguration
    from krim_agent.workspace.types import Completion, WorkspaceTurnRequest

    original = SandboxRunner.__init__
    holder: dict[str, Any] = {}
    marked_once = False

    def init(runner: Any, *args: Any, **kwargs: Any) -> Any:
        hook = kwargs.get("hook")

        async def marked(phase: Any) -> Any:
            nonlocal marked_once
            if hook:
                await hook(phase)
            if phase == "payload_output" and not marked_once:
                marked_once = True
                app = holder["app"]
                current = holder["turn"]
                value = await app.sessions.turn(LOCAL, "local", holder["session"].id, current.id)
                temporary = marker.with_suffix(".temporary")
                temporary.write_text(
                    json.dumps(
                        {
                            "pid": os.getpid(),
                            "run_id": value.run_id,
                            "turn_id": value.id,
                            "session_id": value.session_id,
                            "request": holder["request"].model_dump(mode="json"),
                            "provider_calls": holder["provider"].calls,
                            "material": str(runner.profile.material),
                            "phase": phase,
                        }
                    )
                )
                os.replace(temporary, marker)
                await asyncio.Event().wait()

        kwargs["hook"] = marked
        original(runner, *args, **kwargs)

    runner_class: Any = SandboxRunner
    runner_class.__init__ = init
    wire = await WorkspaceWire().__aenter__()
    provider = OpenAICompatibleProvider(
        OpenAICompatibleConfig(base_url=wire.url, model="fixture", max_tool_calls=4)
    )
    app = await open_application(
        database,
        provider=provider,
        workspace_profile=WorkspaceConfiguration.load(configuration_path),
    )
    holder.update(app=app, provider=provider)
    real_append = app.store.append

    async def append(run_id: Any, kind: Any, data: Any, **kwargs: Any) -> Any:
        value = await real_append(run_id, kind, data, **kwargs)
        if kind == "approval_request":
            await app.service.reply_approval(
                run_id,
                data["request_id"],
                ApprovalReply(
                    request_key="worker-" + data["call_id"],
                    call_id=data["call_id"],
                    arguments_hash=data["arguments_hash"],
                    tool_hash=data["tool_hash"],
                    allow=True,
                ),
            )
        return value

    mutable_store: Any = app.store
    mutable_store.append = append
    try:
        assert app.sessions is not None
        session = await app.sessions.create(
            LOCAL, "local", SessionCreate(request_key="restart", target_key="target-workspace")
        )
        request = WorkspaceTurnRequest(
            request_key="restart-work", text="D4_TASK", expected_revision=0, completion=Completion()
        )
        turn = await app.sessions.submit(LOCAL, "local", session.id, request)
        holder.update(session=session, request=request, turn=turn)
        await app.sessions.wait(LOCAL, "local", session.id, turn.id)
    finally:
        await app.close()
        await provider.close()
        await wire.__aexit__(None, None, None)


if __name__ == "__main__":
    assert sys.argv[1] == "crash"
    asyncio.run(crash_worker(Path(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4])))
