"""Run with -I against a pip --target installation, never the checkout package."""

import argparse
import asyncio
import hashlib
import importlib.metadata
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--installed-root", required=True, type=Path)
    args = parser.parse_args()
    installed = args.installed_root.resolve(strict=True)
    sys.path.insert(0, str(installed))

    import httpx

    import krim_agent
    from krim_agent.api.server import create_app
    from krim_agent.composition import open_application
    from krim_agent.config import Settings
    from krim_agent.contracts import RunRequest, ServiceError
    from krim_agent.evaluation.domain import CaseSpec, DatasetRef, MetricSpec, TaskSpec
    from krim_agent.evaluation.product_types import ProductDatasetSpec
    from krim_agent.providers.openai_compatible import (
        OpenAICompatibleConfig,
        OpenAICompatibleProvider,
    )
    from krim_agent.sdk.evaluations import EvaluationSDK
    from krim_agent.security import LOCAL
    from krim_agent.sessions.types import SessionCreate, TurnRequest

    assert Path(krim_agent.__file__).resolve().is_relative_to(installed)
    retired = (
        "providers.fake", "providers.eval_script", "tools.builtins",
        "workspace.operations", "workspace.targets", "tools.workspace",
    )
    for name in retired:
        assert importlib.util.find_spec("krim_agent." + name) is None, name
    distribution = importlib.metadata.distribution("krim-agent")
    entries = {entry.name: entry.value for entry in distribution.entry_points}
    assert entries["krim"] == "krim_agent.cli:main"
    assert entries["krim-eval"] == "krim_agent.evaluation.cli:main"
    console = installed / "bin" / "krim"
    assert console.is_file(), "Linux pip installation must generate the console entry point"
    cli = subprocess.run(
        [sys.executable, str(console), "--version"],
        env={**os.environ, "PYTHONPATH": str(installed)},
        capture_output=True, text=True, timeout=30,
    )
    assert cli.returncode == 0, cli.stderr
    assert distribution.version in cli.stdout

    web = installed / "krim_agent" / "web"
    for filename, field in (("script-manifest.json", "scripts"), ("font-manifest.json", "fonts")):
        manifest = json.loads((web / filename).read_text(encoding="utf-8"))
        for item in manifest[field]:
            data = (web / item["path"]).read_bytes()
            assert len(data) == item["size_bytes"]
            assert hashlib.sha256(data).hexdigest() == item["sha256"]

    requests = []

    class StreamBody(httpx.AsyncByteStream):
        def __init__(self, chunks):
            self.chunks = chunks

        async def __aiter__(self):
            for chunk in self.chunks:
                yield chunk

        async def aclose(self):
            pass

    async def respond(request):
        assert request.url.host == "127.0.0.1"
        assert request.url.path == "/v1/chat/completions"
        body = json.loads(request.content)
        assert body["model"] == "installed-fixture"
        requests.append(body)
        chunks = []
        for delta, finish in (({"content": "installed-ok"}, None), ({}, "stop")):
            value = {
                "id": "installed-smoke", "object": "chat.completion.chunk", "created": 1,
                "model": "installed-fixture",
                "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
            }
            chunks.append("data: " + json.dumps(value) + "\n\n")
        return httpx.Response(
            200, headers={"Content-Type": "text/event-stream"},
            stream=StreamBody([chunk.encode() for chunk in chunks] + [b"data: [DONE]\n\n"]),
        )

    async def verify(root):
        async with await open_application(root / "management.sqlite") as application:
            assert not application.service.bindings
            try:
                await application.service.start(RunRequest(text="no model", request_key="missing"))
            except ServiceError as error:
                assert error.code == "provider_not_configured"
            else:
                raise AssertionError("unconfigured package created a run")
            app = create_app(application=application)
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
                ) as client:
                    health = await client.get("/healthz")
                    assert health.status_code == 200
                    assert health.json()["run_ready"] is False
                    assert health.json()["provider"] == "unconfigured"
                    assets = await client.get("/ui/assets/app.js")
                    assert assets.status_code == 200 and assets.content

        original_init = httpx.AsyncClient.__init__

        def init(client, *positional, **keywords):
            keywords.setdefault("transport", httpx.MockTransport(respond))
            original_init(client, *positional, **keywords)

        httpx.AsyncClient.__init__ = init
        provider = None
        try:
            provider = OpenAICompatibleProvider(OpenAICompatibleConfig(
                base_url="http://127.0.0.1:9/v1", model="installed-fixture",
            ))
            async with await open_application(
                root / "session.sqlite", provider=provider,
                settings=Settings(product_eval_enabled=True),
            ) as application:
                session = await application.sessions.create(
                    LOCAL, "local", SessionCreate(request_key="installed-session"),
                )
                turn = await application.sessions.submit(
                    LOCAL, "local", session.id,
                    TurnRequest(text="installation smoke", request_key="turn",
                                expected_revision=session.revision),
                )
                finished = await application.sessions.wait(LOCAL, "local", session.id, turn.id)
                assert finished.status == "completed", finished
                snapshot = await application.store.snapshot(finished.run_id)
                assert any("installed-ok" in json.dumps(event.data) for event in snapshot.events)
                assert application.evaluation_product is not None
                evaluation = EvaluationSDK(application.evaluation_product)
                dataset = await evaluation.freeze_dataset(ProductDatasetSpec(
                    name="installed-eval", version="v1", cases=[CaseSpec(
                        case_id="plain", instruction="Say exactly installed-ok.", files={},
                        reference={"expected": "installed-ok"},
                    )],
                ))
                metrics = [MetricSpec.model_validate(row)
                           for row in await evaluation.metrics("krim-session", mode="manual")]
                task = await evaluation.start_task(TaskSpec(
                    request_key="installed-eval", target_key="krim-session", metrics=metrics,
                    dataset=DatasetRef(name=dataset["name"], version=dataset["version"],
                                       fingerprint=dataset["fingerprint"]),
                ))
                completed = await application.product_evaluations.kernel.wait(task["id"])
                assert completed["status"] == "completed", completed["status"]
                item = completed["items"][0]
                assert item["quality"] == "passed"
                evidence = item["output"]["target_result"]
                assert evidence["schema_version"] == "krim-session/1"
                assert evidence["session_id"] != session.id
                assert evidence["run_id"] and evidence["turn_id"]
                assert evidence["text"] == "installed-ok"
                observed = await application.sessions.get(LOCAL, "local", evidence["session_id"])
                assert observed.status == evidence["session_status"] == "closed"
                assert item["output"]["exit_code"] is None
                await evaluation.report_page(task["id"])
        finally:
            if provider is not None:
                await provider.close()
            httpx.AsyncClient.__init__ = original_init
        assert len(requests) == 2
        assert all("installation smoke" not in json.dumps(message)
                   for message in requests[1]["messages"])

    with tempfile.TemporaryDirectory(prefix="krim-installed-convergence-") as temporary:
        asyncio.run(verify(Path(temporary)))
    loaded = [
        Path(module.__file__).resolve()
        for name, module in sys.modules.items()
        if name.startswith("krim_agent") and getattr(module, "__file__", None)
    ]
    assert loaded and all(path.is_relative_to(installed) for path in loaded)
    print(json.dumps({
        "version": distribution.version, "installed_modules": len(loaded),
        "retired_modules_absent": len(retired), "model_requests": len(requests),
        "console_entrypoint": "passed", "management_api": "passed",
        "session_model_boundary": "passed", "product_evaluation_session_chain": "passed",
        "packaged_assets": "passed",
    }))


if __name__ == "__main__":
    main()
