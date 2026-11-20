from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import func, select

from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import ServiceError
from krim_agent.evaluation.domain import CaseSpec, DatasetRef, MetricSpec, TaskSpec
from krim_agent.evaluation.product_types import ProductDatasetSpec
from krim_agent.evaluation.types import DatasetRef as LegacyDatasetRef
from krim_agent.evaluation.types import EvalRequest, rule
from krim_agent.providers.openai_compatible import (
    OpenAICompatibleConfig,
    OpenAICompatibleProvider,
)
from krim_agent.sdk.evaluations import EvaluationSDK
from krim_agent.security import LOCAL, PrincipalContext
from krim_agent.storage.models import sessions as session_rows
from tests.support.builtins import builtin_registry, demo_policy


class SSE(httpx.AsyncByteStream):
    def __init__(self, events: list[dict[str, Any] | str]) -> None:
        self.events = events

    async def __aiter__(self):
        for event in self.events:
            raw = event if isinstance(event, str) else json.dumps(event, separators=(",", ":"))
            yield f"data: {raw}\n\n".encode()

    async def aclose(self) -> None:
        return None


class GatedSSE(httpx.AsyncByteStream):
    def __init__(self, entered, release) -> None:
        self.entered, self.release = entered, release
        self.closed = False

    async def __aiter__(self):
        self.entered.set()
        await self.release.wait()
        event = {
            "choices": [
                {"index": 0, "delta": {"content": "hello"}, "finish_reason": "stop"}
            ]
        }
        yield b"data: " + json.dumps(event, separators=(",", ":")).encode() + b"\n\n"
        yield b"data: [DONE]\n\n"

    async def aclose(self) -> None:
        self.closed = True


def fixture_provider() -> tuple[OpenAICompatibleProvider, list[dict[str, Any]]]:
    requests: list[dict[str, Any]] = []
    responses = [
        [
            {
                "choices": [
                    {
                        "index": 0,
                        "delta": {
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": "echo-call-1",
                                    "type": "function",
                                    "function": {
                                        "name": "echo",
                                        "arguments": '{"text":"hello"}',
                                    },
                                }
                            ]
                        },
                        "finish_reason": None,
                    }
                ]
            },
            {"choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]},
            "[DONE]",
        ],
        [
            {"choices": [{"index": 0, "delta": {"content": "hello"}, "finish_reason": None}]},
            {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
            "[DONE]",
        ],
    ]

    async def respond(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        index = len(requests) - 1
        if index >= len(responses):
            raise AssertionError("evaluation retried a completed model turn")
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=SSE(responses[index]),
        )

    provider = OpenAICompatibleProvider(
        OpenAICompatibleConfig(base_url="https://model.example/v1", model="fixture")
    )
    provider._client._transport = httpx.MockTransport(respond)
    provider._sealed = provider.binding_token()
    return provider, requests


async def count_eval_sessions(app: Any) -> int:
    async with app.store.lock, app.store.engine.begin() as conn:
        return int(
            await conn.scalar(
                select(func.count()).select_from(session_rows).where(
                    session_rows.c.create_key.like("eval.%")
                )
            )
        )


@pytest.mark.asyncio
async def test_product_case_uses_authorized_session_chain_and_preserves_real_facts(
    tmp_path: Path, monkeypatch
):
    provider, requests = fixture_provider()
    app = await open_application(
        tmp_path / "runs.sqlite",
        settings=Settings(product_eval_enabled=True),
        provider=provider,
        registry=builtin_registry(),
        policy=demo_policy,
    )
    try:
        assert app.evaluation_product is not None
        sdk = EvaluationSDK(app.evaluation_product)
        targets = await sdk.targets()
        assert {row["key"] for row in targets} == {"krim-session", "reconcile-reference"}
        assert next(row for row in targets if row["key"] == "reconcile-reference")["kind"] == (
            "non-model-reference"
        )

        dataset = await sdk.freeze_dataset(
            ProductDatasetSpec(
                name="plain-session",
                version="v1",
                cases=[
                    CaseSpec(
                        case_id="hello",
                        instruction="Say hello exactly.",
                        files={},
                        reference={"expected": "hello"},
                    )
                ],
            )
        )
        metrics = [
            MetricSpec.model_validate(row)
            for row in await sdk.metrics("krim-session", mode="manual")
        ]
        request = TaskSpec(
            request_key="plain-session-run",
            dataset=DatasetRef(
                name=dataset["name"],
                version=dataset["version"],
                fingerprint=dataset["fingerprint"],
            ),
            target_key="krim-session",
            metrics=metrics,
        )
        original_submit = app.sessions.submit
        submit_return_lost = False

        async def lose_first_submit_return(*args, **kwargs):
            nonlocal submit_return_lost
            turn = await original_submit(*args, **kwargs)
            if not submit_return_lost:
                submit_return_lost = True
                raise OSError("SESSION_SUBMIT_RETURN_LOST")
            return turn

        monkeypatch.setattr(app.sessions, "submit", lose_first_submit_return)
        task = await sdk.start_task(request)
        completed = await app.product_evaluations.kernel.wait(task["id"])
        assert completed["status"] == "completed", completed
        item = completed["items"][0]
        output = item["output"]
        result = output["target_result"]
        assert item["quality"] == "passed"
        assert result["schema_version"] == "krim-session/1"
        assert result["text"] == "hello"
        assert result["session_id"] and result["turn_id"] and result["run_id"]
        assert result["status"] == result["turn_status"] == "completed"
        assert result["session_status"] == "closed"
        assert result["session_record_retained"] is True
        assert result["host_runtime"] == {"ownership": "borrowed", "retained": True}
        assert output["exit_code"] is None and output["process"] is None
        assert output["cleanup"] == "closed"
        assert output["artifacts"] == {}

        session = await app.sessions.get(LOCAL, "local", result["session_id"])
        turn = await app.sessions.turn(LOCAL, "local", session.id, result["turn_id"])
        assert session.status == "closed"
        assert turn.run_id == result["run_id"] and turn.status == "completed"
        snapshot = await app.store.snapshot(turn.run_id)
        assert snapshot.session_context is not None
        assert snapshot.session_context.session_id == session.id
        assert snapshot.session_context.turn_id == turn.id
        from krim_agent.evaluation.adapters.krim_source import KrimSourceReader
        from krim_agent.evaluation.product import EvaluationProduct

        scope = EvaluationProduct._scope("local")
        source = await KrimSourceReader(
            app.store, app.sessions, LOCAL, workspace_id="local", scope=scope
        ).read(scope, {"run_id": turn.run_id})
        assert source["target"]["key"] == "krim-session"
        assert any(event.kind == "tool_start" for event in snapshot.events)
        assert any(event.kind == "tool_end" for event in snapshot.events)
        assert result["tools"] is not None
        assert any(
            row["kind"] == "tool_end" and row["receipt"].get("name") == "echo"
            for row in result["tools"]
        )
        assert submit_return_lost is True
        assert len(requests) == 2

        # Idempotent task replay reads the same result and sends no second model request.
        replay = await sdk.start_task(request)
        assert replay["id"] == task["id"]
        assert len(requests) == 2

        # Unauthorized callers cannot launch a case or create a session.
        before_sessions = await count_eval_sessions(app)
        denied_sdk = EvaluationSDK(
            app.evaluation_product,
            principal=PrincipalContext("outsider", 1, ()),
            workspace="local",
        )
        with pytest.raises(ServiceError) as denied:
            await denied_sdk.start_task(request.model_copy(update={"request_key": "unauthorized"}))
        assert denied.value.code == "not_found"
        assert await count_eval_sessions(app) == before_sessions

        # A revoked/revoking principal is rechecked at session admission.
        app.sessions._revoked.add(LOCAL.principal_id)
        revoked_task = await sdk.start_task(
            request.model_copy(update={"request_key": "revoked-before-admission"})
        )
        revoked_result = await app.product_evaluations.kernel.wait(revoked_task["id"])
        assert revoked_result["status"] == "failed"
        assert revoked_result["code"] == "permission_denied"
        assert await count_eval_sessions(app) == before_sessions
        app.sessions._revoked.discard(LOCAL.principal_id)

        # Old script targets are retired, not silently mapped to paid execution.
        with pytest.raises(ServiceError) as retired:
            await sdk.start_task(request.model_copy(
                update={"request_key": "old-script-retry", "target_key": "krim-script-v2"}
            ))
        assert retired.value.code == "target_retired" and retired.value.status == 410
        retired_dataset = await sdk.freeze_dataset(
            ProductDatasetSpec(
                name="script-verifier-retired",
                version="v1",
                cases=[
                    CaseSpec(
                        case_id="old-script",
                        instruction="old arithmetic case",
                        files={},
                        verifier="krim-script/1",
                        reference={"expected": "5"},
                    )
                ],
            )
        )
        with pytest.raises(ServiceError) as retired_verifier:
            await sdk.start_task(TaskSpec(
                request_key="script-verifier-retry",
                dataset=DatasetRef(
                    name=retired_dataset["name"], version=retired_dataset["version"],
                    fingerprint=retired_dataset["fingerprint"],
                ),
                target_key="krim-session",
                metrics=metrics,
            ))
        assert retired_verifier.value.code == "target_retired"
        assert retired_verifier.value.status == 410
        assert app.evaluations.targets() == []
        legacy = EvalRequest(
            request_key="retired-legacy-eval",
            dataset=LegacyDatasetRef(
                name=dataset["name"], version=dataset["version"],
                fingerprint=dataset["fingerprint"],
            ),
            target_key="target-v2",
            rules=[rule("status_code")],
            execution={
                "repetitions": 1,
                "concurrency": 1,
                "item_timeout_seconds": 12,
                "scorer_timeout_seconds": 1,
                "batch_timeout_seconds": 600,
            },
        )
        with pytest.raises(ServiceError) as legacy_retired:
            await app.evaluations.start(legacy)
        assert legacy_retired.value.code == "execution_retired"
        assert legacy_retired.value.status == 410
        assert len(requests) == 2
    finally:
        app.sessions._revoked.discard(LOCAL.principal_id)
        await app.close()


@pytest.mark.asyncio
async def test_cancel_uses_session_control_and_joins_run_cleanup(tmp_path: Path):
    entered = asyncio.Event()
    release = asyncio.Event()
    streams: list[GatedSSE] = []
    provider_requests = []

    async def respond(request: httpx.Request) -> httpx.Response:
        provider_requests.append(json.loads(request.content))
        stream = GatedSSE(entered, release)
        streams.append(stream)
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream)

    provider = OpenAICompatibleProvider(
        OpenAICompatibleConfig(base_url="https://model.example/v1", model="fixture")
    )
    provider._client._transport = httpx.MockTransport(respond)
    provider._sealed = provider.binding_token()
    app = await open_application(
        tmp_path / "cancel.sqlite",
        settings=Settings(product_eval_enabled=True),
        provider=provider,
        registry=builtin_registry(),
        policy=demo_policy,
    )
    try:
        sdk = EvaluationSDK(app.evaluation_product)
        dataset = await sdk.freeze_dataset(
            ProductDatasetSpec(
                name="cancel-session", version="v1",
                cases=[CaseSpec(case_id="hello", instruction="Say hello", files={})],
            )
        )
        metrics = [
            MetricSpec.model_validate(row)
            for row in await sdk.metrics("krim-session", mode="manual")
        ]
        task = await sdk.start_task(TaskSpec(
            request_key="cancel-through-session-control",
            dataset=DatasetRef(name=dataset["name"], version=dataset["version"],
                               fingerprint=dataset["fingerprint"]),
            target_key="krim-session", metrics=metrics,
        ))
        await asyncio.wait_for(entered.wait(), 5)
        adapter = app.product_evaluations.repository.registry["krim-session"]
        for _ in range(100):
            if adapter.jobs:
                break
            await asyncio.sleep(0.01)
        assert adapter.jobs
        turn_id, record = next(iter(adapter.jobs.items()))
        run_id = (await app.sessions.turn(LOCAL, "local", record["session_id"], turn_id)).run_id
        assert run_id is not None

        result = await app.product_evaluations.kernel.cancel(task["id"])
        assert result["status"] == "aborted"
        turn = await app.sessions.turn(LOCAL, "local", record["session_id"], turn_id)
        session = await app.sessions.get(LOCAL, "local", record["session_id"])
        assert turn.status not in {"queued", "running"}
        assert session.status == "closed"
        assert (await app.store.snapshot(run_id)).run.status not in {"queued", "running"}
        assert adapter.observations[turn_id]["cleanup"] == "closed"
        assert len(provider_requests) == 1
        assert streams and streams[0].closed
    finally:
        release.set()
        await app.close()


@pytest.mark.asyncio
async def test_missing_model_does_not_register_or_execute_krim_session(tmp_path: Path):
    async with await open_application(
        tmp_path / "no-model.sqlite", settings=Settings(product_eval_enabled=True)
    ) as app:
        assert app.evaluation_product is not None
        sdk = EvaluationSDK(app.evaluation_product)
        assert [row["key"] for row in await sdk.targets()] == ["reconcile-reference"]
        dataset = await sdk.freeze_dataset(
            ProductDatasetSpec(
                name="no-model",
                version="v1",
                cases=[CaseSpec(case_id="plain", instruction="answer", files={})],
            )
        )
        metrics = [
            MetricSpec.model_validate(row)
            for row in await sdk.metrics("reconcile-reference", mode="manual")
        ]
        with pytest.raises(ServiceError) as unavailable:
            await sdk.start_task(TaskSpec(
                request_key="no-model-case",
                dataset=DatasetRef(name=dataset["name"], version=dataset["version"],
                                   fingerprint=dataset["fingerprint"]),
                target_key="krim-session",
                metrics=metrics,
            ))
        assert unavailable.value.code == "provider_not_configured"
        assert unavailable.value.status == 503
        assert await sdk.list_tasks() == []
