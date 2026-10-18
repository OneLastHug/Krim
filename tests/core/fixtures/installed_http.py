"""真正项目外-I安装验收：SDK/API/CLI共享内核，本机真实流及自有进程清理。"""

import argparse
import asyncio
import json
import os
import signal
import socket
import sys
from contextlib import nullcontext
from dataclasses import replace
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import uvicorn

import krim_agent
from krim_agent import composition
from krim_agent.api import create_app
from krim_agent.client import AsyncRunClient
from krim_agent.config import Settings
from krim_agent.contracts import RunRequest
from krim_agent.evaluation.types import EvalRequest, FreezeRequest, rule
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig
from krim_agent.sdk import RunSDK
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.tools.builtins import builtin_registry
from krim_agent.tools.registry import ToolRegistry


def request(dataset, key):
    return EvalRequest.model_validate(
        {
            "request_key": key,
            "dataset": {k: getattr(dataset, k) for k in ["name", "version", "fingerprint"]},
            "target_key": "target-http",
            "rules": [rule(n).model_dump(mode="json") for n in ["status_code", "exact_match"]],
            "execution": {
                "repetitions": 1,
                "concurrency": 1,
                "item_timeout_seconds": 12,
                "scorer_timeout_seconds": 1,
                "batch_timeout_seconds": 600,
            },
        }
    )


async def freeze(app, name="installed", text="/add 2 3"):
    return await app.evaluations.freeze(
        FreezeRequest.model_validate(
            {
                "name": name,
                "version": "1",
                "cases": [
                    {
                        "case_id": "one",
                        "source_kind": "manual",
                        "input": text,
                        "reference": {
                            "expected": "5",
                            "expected_status": "completed",
                            "expected_code": None,
                            "expected_tools": None,
                            "trajectory": None,
                            "expected_schema": None,
                        },
                        "reason": "人工核算的合成参考",
                    }
                ],
            }
        )
    )


async def main(args):
    root = Path(args.evidence)
    await asyncio.to_thread(root.mkdir, parents=True, exist_ok=True)
    assert sys.flags.isolated and "site-packages" in Path(krim_agent.__file__).parts
    assert not any(str(p).endswith("/source/src") for p in sys.path)
    source_spec = spec_from_file_location(
        "fixture_only", Path(__file__).with_name("http_provider_server.py")
    )
    module = module_from_spec(source_spec)
    source_spec.loader.exec_module(module)
    commands = []
    children = []
    tools = []
    cleanup = {}

    def tracked(spec):
        async def execute(arguments):
            tools.append(spec.name)
            return await spec.executor(arguments)

        return replace(spec, executor=execute)

    registry = ToolRegistry(tuple(tracked(spec) for spec, _ in builtin_registry()._frozen))
    composition.builtin_registry = lambda: registry

    async def cli(arguments, env, label, interrupt=False, fixture=None):
        argv = [sys.executable, "-I", "-B", "-m", "krim_agent", *arguments]
        child_env = dict(os.environ, **env)
        child_env.pop("KRIM_GUIDE_SOURCE", None)
        child_env.pop("PYTHONPATH", None)
        before = len(fixture.requests) if fixture is not None else 0
        process = await asyncio.create_subprocess_exec(
            *argv,
            cwd=root,
            env=child_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        children.append(process)
        try:
            if interrupt:
                async with asyncio.timeout(5):
                    while len(fixture.requests) == before:
                        assert process.returncode is None
                        await asyncio.sleep(0.005)
                process.send_signal(signal.SIGINT)
            out, err = await asyncio.wait_for(process.communicate(), 15)
            (root / (label + ".stdout")).write_bytes(out)
            (root / (label + ".stderr")).write_bytes(err)
            commands.append(
                {
                    "argv": argv,
                    "cwd": str(root),
                    "pid": process.pid,
                    "exit_code": process.returncode,
                    "label": label,
                }
            )
            return process.returncode, json.loads(out)
        finally:
            if process.returncode is None:
                process.terminate()
                await process.wait()

    class Server(uvicorn.Server):
        def capture_signals(self):
            return nullcontext()

    async with module.HTTPFixture(expected_key="SYNTHETIC_INSTALLED_OUTBOUND") as fixture:
        config = OpenAICompatibleConfig(
            base_url=fixture.url, model="fixture", api_key="SYNTHETIC_INSTALLED_OUTBOUND"
        )
        app = await composition.open_application(
            root / "http.sqlite",
            settings=Settings(database=root / "http.sqlite", api_key="SYNTHETIC_INSTALLED_API"),
            http_config=config,
        )
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(16)
        listener.setblocking(False)
        port = listener.getsockname()[1]
        server = Server(
            uvicorn.Config(create_app(application=app), log_level="warning", access_log=False)
        )
        server_task = asyncio.create_task(server.serve(sockets=[listener]))
        try:
            sdk = RunSDK(app.service)
            view = await sdk.start("/add 2 3", "installed-http-sdk", target="target-http")
            await sdk.wait(view.id)
            facts = await sdk.result(view.id)
            assert (
                facts.text == "5"
                and facts.model_steps == 2
                and tools == ["add"]
                and len(fixture.requests) == 2
            )
            dataset = await freeze(app)
            evaluation = await app.evaluations.start(request(dataset, "installed-http-eval"))
            await app.evaluations.wait(evaluation.id)
            report = await app.evaluations.report(evaluation.id)
            assert (
                report["items"][0]["quality"] == "passed"
                and len(fixture.requests) == 4
                and tools == ["add"] * 2
            )
            async with asyncio.timeout(3):
                while not server.started:
                    if server_task.done():
                        await server_task
                    await asyncio.sleep(0.005)
            base = f"http://127.0.0.1:{port}"
            client = AsyncRunClient(base, "SYNTHETIC_INSTALLED_API")
            try:
                body = RunRequest(
                    text="/add 2 3", request_key="installed-api-http", target="target-http"
                )
                api_view = await client.start(body)
                await client.wait(api_view.id)
                assert (await client.start(body)).id == api_view.id and (
                    await client.result(api_view.id)
                ).text == "5"
                env = {
                    "KRIM_AGENT_API_KEY": "SYNTHETIC_INSTALLED_API",
                    "KRIM_AGENT_HTTP_BASE_URL": "https://u:p@invalid",
                }
                rc, remote = await cli(
                    [
                        "run",
                        "--url",
                        base,
                        "--text",
                        "/add 2 3",
                        "--key",
                        "installed-cli-remote",
                        "--target",
                        "target-http",
                    ],
                    env,
                    "remote-run",
                )
                assert (
                    rc == 0
                    and remote["text"] == "5"
                    and len(fixture.requests) == 8
                    and tools == ["add"] * 4
                )
                held = await freeze(app, "held", "/echo FIXTURE_HOLD")
                fixture.entered.clear()
                rc, remote_eval = await cli(
                    [
                        "eval",
                        "start",
                        "--url",
                        base,
                        "--name",
                        held.name,
                        "--revision",
                        held.version,
                        "--fingerprint",
                        held.fingerprint,
                        "--target",
                        "target-http",
                        "--key",
                        "remote-held",
                    ],
                    env,
                    "remote-eval",
                )
                assert rc == 0
                await asyncio.wait_for(fixture.entered.wait(), 3)
                assert len(fixture.requests) == 9
                assert (
                    await client.eval_get(remote_eval["id"])
                ).status == "running" and not app.evaluations.tasks[remote_eval["id"]].done()
                assert (await client.eval_cancel(remote_eval["id"])).status == "aborted"
            finally:
                await client.close()
            local_env = {
                "KRIM_AGENT_HTTP_BASE_URL": fixture.url,
                "KRIM_AGENT_HTTP_MODEL": "fixture",
                "KRIM_AGENT_HTTP_API_KEY": "SYNTHETIC_INSTALLED_OUTBOUND",
                "KRIM_AGENT_API_KEY": "",
            }
            rc, local = await cli(
                [
                    "run",
                    "--database",
                    str(root / "local.sqlite"),
                    "--text",
                    "/add 2 3",
                    "--key",
                    "local-http",
                    "--target",
                    "target-http",
                ],
                local_env,
                "local-run",
            )
            assert (
                rc == 0 and local["text"] == "5" and local["tools"][0]["executor_started"] is True
            )
            for interrupted in [False, True]:
                database = root / (
                    "local-eval-interrupt.sqlite" if interrupted else "local-eval.sqlite"
                )
                async with await composition.open_application(database) as setup:
                    data = await freeze(
                        setup, "local", "/echo FIXTURE_HOLD" if interrupted else "/add 2 3"
                    )
                rc, result = await cli(
                    [
                        "eval",
                        "start",
                        "--database",
                        str(database),
                        "--name",
                        data.name,
                        "--revision",
                        data.version,
                        "--fingerprint",
                        data.fingerprint,
                        "--target",
                        "target-http",
                        "--key",
                        "local-eval",
                    ],
                    local_env,
                    "local-eval-interrupt" if interrupted else "local-eval",
                    interrupt=interrupted,
                    fixture=fixture,
                )
                assert rc == (130 if interrupted else 0)
                if interrupted:
                    reader = await SQLiteStore.open(database, readonly=True)
                    try:
                        assert (await reader.eval_by_key("local-eval"))["status"] == "aborted"
                    finally:
                        await reader.close()
                else:
                    assert result["status"] == "completed" and result["complete"]
            rc, interrupted_run = await cli(
                [
                    "run",
                    "--database",
                    str(root / "run-interrupt.sqlite"),
                    "--text",
                    "/echo FIXTURE_HOLD",
                    "--key",
                    "interrupt-run",
                    "--target",
                    "target-http",
                ],
                local_env,
                "local-run-interrupt",
                interrupt=True,
                fixture=fixture,
            )
            assert rc == 130 and interrupted_run["error"]["code"] == "cancelled"
            # 当前writer仍拥有http.sqlite；冷读CLI必须不创建第二writer/触发恢复或读取坏HTTP配置。
            before = len(fixture.requests)
            rc, cold = await cli(
                ["get", "--database", str(root / "http.sqlite"), "--id", view.id],
                {"KRIM_AGENT_HTTP_BASE_URL": "https://u:p@invalid"},
                "cold-get",
            )
            assert rc == 0 and cold["id"] == view.id and len(fixture.requests) == before
            (root / "observed.json").write_text(
                json.dumps(
                    {
                        "module": krim_agent.__file__,
                        "executable": sys.executable,
                        "cwd": str(Path.cwd()),
                        "sdk": facts.model_dump(mode="json"),
                        "api_run_id": api_view.id,
                        "report": report,
                        "provider_requests": fixture.requests,
                        "main_tool_entries": tools,
                        "commands": commands,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
        finally:
            server.should_exit = True
            await asyncio.wait_for(server_task, 5)
            listener.close()
            await app.close()
            for child in children:
                if child.returncode is None:
                    child.terminate()
                    await child.wait()
            cleanup = {
                "server_task_done": server_task.done(),
                "run_tasks_done": all(t.done() for t in app.service.tasks.values()),
                "eval_tasks_done": all(t.done() for t in app.evaluations.tasks.values()),
                "scorers_done": not app.evaluations.current,
                "database_closed": app.store.closed,
                "providers_closed": all(p.closed for p in app.owned_providers),
                "children_exited": all(p.returncode is not None for p in children),
                "api_port": port,
                "backend_port": fixture.port,
            }
            (root / "cleanup.json").write_text(json.dumps(cleanup, indent=2))
    for port in [cleanup["api_port"], cleanup["backend_port"]]:
        probe = socket.socket()
        probe.settimeout(0.2)
        try:
            assert probe.connect_ex(("127.0.0.1", port)) != 0
        finally:
            probe.close()
    cleanup.update(backend_active=fixture.active, ports_closed=True)
    (root / "cleanup.json").write_text(json.dumps(cleanup, indent=2))
    assert fixture.active == 0 and all(
        v is True for k, v in cleanup.items() if k.endswith(("_done", "_closed", "_exited"))
    )
    print(
        json.dumps(
            {"module": krim_agent.__file__, "requests": len(fixture.requests), "cleanup": cleanup}
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", required=True)
    asyncio.run(main(parser.parse_args()))
