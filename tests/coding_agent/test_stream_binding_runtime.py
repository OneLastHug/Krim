"""Text-stream fences through both real run loops and SQLite persistence."""

import asyncio
from collections import Counter

import pytest

from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.context import builder
from krim_agent.contracts import RunRequest, ServiceError
from krim_agent.providers.types import ModelProgress, ModelText
from krim_agent.runtime.service import RunBinding, TurnBinding
from krim_agent.tools.core import ToolSpec
from krim_agent.tools.executor import ToolExecutor
from tests.coding_agent.test_sdk_run_service import application as development_application
from tests.support.builtins import builtin_registry


class BufferedText:
    def __init__(self, chunks=32, *, hold=False):
        self.chunks = chunks
        self.hold = hold
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.closed = 0

    async def stream(self, messages, tools):
        try:
            for _ in range(self.chunks):
                yield ModelProgress(phase="text")
                yield ModelText(text="正文。\n")
            self.entered.set()
            if self.hold:
                await self.release.wait()
        finally:
            self.closed += 1

    async def close(self):
        self.release.set()


def observe_binding(monkeypatch):
    counts = Counter()
    full = RunBinding.assert_current
    light = RunBinding.assert_runtime_current

    def checked(binding):
        counts["full"] += 1
        return full(binding)

    def runtime_checked(binding):
        counts["light"] += 1
        return light(binding)

    monkeypatch.setattr(RunBinding, "assert_current", checked)
    monkeypatch.setattr(RunBinding, "assert_runtime_current", runtime_checked)
    return counts


@pytest.mark.parametrize("target", ["ordinary", "development"])
async def test_run_loops_use_light_fences_and_preserve_durable_text(
    tmp_path, monkeypatch, target,
):
    counts = observe_binding(monkeypatch)
    provider = BufferedText()
    if target == "ordinary":
        app = await open_application(
            tmp_path / "ordinary.sqlite", provider=provider, registry=builtin_registry(),
            settings=Settings(history_compaction=False),
        )
        try:
            run = await app.service.start(RunRequest(text="write", request_key="stream"))
            counts.clear()
            result = await asyncio.wait_for(app.service.wait(run.id), 15)
            snapshot = await app.store.snapshot(run.id)
        finally:
            await app.close()
    else:
        from tests.coding_agent import test_sdk_run_service

        monkeypatch.setattr(test_sdk_run_service, "HostScript", lambda: provider)
        store, service, harness, _, run, cleanup = await development_application(
            tmp_path / "development.sqlite", approval=False,
        )
        try:
            counts.clear()
            result = await asyncio.wait_for(service.wait(run.id), 15)
            snapshot = await store.snapshot(run.id)
            assert harness.counts["write"] == 0
            assert cleanup == [run.id]
        finally:
            await service.close()
            await store.close()
    assert result.status == "completed", result.code
    text = [event.data["text"] for event in snapshot.events if event.kind == "text"]
    assert text == ["正文。\n"] * provider.chunks
    assert [event.seq for event in snapshot.events] == list(range(len(snapshot.events)))
    assert sum(event.kind == "model_end" for event in snapshot.events) == 1
    assert sum(event.kind == "terminal" for event in snapshot.events) == 1
    assert counts["light"] >= 5 * provider.chunks
    assert counts["full"] < provider.chunks
    assert provider.closed == 1


async def test_real_run_cancel_keeps_partial_text_and_closes_provider(tmp_path):
    provider = BufferedText(8, hold=True)
    async with await open_application(
        tmp_path / "cancel.sqlite", provider=provider, registry=builtin_registry(),
        settings=Settings(history_compaction=False),
    ) as app:
        run = await app.service.start(RunRequest(text="write", request_key="cancel"))
        await asyncio.wait_for(provider.entered.wait(), 10)
        result = await asyncio.wait_for(app.service.cancel(run.id), 5)
        snapshot = await app.store.snapshot(run.id)
        assert (result.status, result.code) == ("aborted", "cancelled")
        assert "".join(e.data["text"] for e in snapshot.events if e.kind == "text") == (
            "正文。\n" * 8
        )
        assert sum(e.kind == "terminal" for e in snapshot.events) == 1
        assert not any(e.kind == "model_end" for e in snapshot.events)
        assert provider.closed == 1


async def test_turn_runtime_check_covers_per_turn_executor_and_context(tmp_path, monkeypatch):
    async with await open_application(
        tmp_path / "turn-binding.sqlite", provider=BufferedText(), registry=builtin_registry(),
    ) as app:
        base = app.service.bindings["target-http"]
        executor = ToolExecutor(builtin_registry(), None)
        binding = TurnBinding(base, builder.binding_token(), executor=executor)
        binding.assert_runtime_current()
        executor.registry._items = {}
        with pytest.raises(ServiceError, match="tool_binding_changed"):
            binding.assert_runtime_current()
        context_binding = TurnBinding(base, builder.binding_token())
        monkeypatch.setattr(builder, "prepare", lambda *args: None)
        with pytest.raises(ServiceError, match="context_binding_changed"):
            context_binding.assert_runtime_current()


async def test_binding_light_check_avoids_schema_but_full_check_detects_content(
    tmp_path, monkeypatch,
):
    async with await open_application(
        tmp_path / "binding.sqlite", provider=BufferedText(), registry=builtin_registry(),
    ) as app:
        binding = app.service.bindings["target-http"]
        descriptor = ToolSpec.descriptor
        built = []

        def observed(spec):
            built.append(spec.name)
            return descriptor(spec)

        monkeypatch.setattr(ToolSpec, "descriptor", observed)
        for _ in range(100):
            binding.assert_runtime_current()
        assert built == []
        spec = binding.loop.executor.registry._frozen[0][0]
        object.__setattr__(spec, "description", spec.description + " changed")
        binding.assert_runtime_current()
        with pytest.raises(ServiceError, match="tool_binding_changed"):
            binding.assert_current()
        assert built
