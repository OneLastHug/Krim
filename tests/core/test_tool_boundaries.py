import asyncio
import json
import sqlite3
import time
from types import SimpleNamespace

import pytest
from agent_helpers import EventProvider, pending, registry_for

from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import ApprovalReply, RunRequest, ServiceError
from krim_agent.providers.types import ModelText, ModelToolCall
from krim_agent.tools.output import OutputContract


class ToolResultProvider(EventProvider):
    """Complete after receiving the real tool observation, without repeating its ID."""

    def __init__(self, events):
        super().__init__(events)
        self.observations = []

    async def stream(self, messages, tools):
        observations = [message for message in messages if message.role == "tool"]
        if not observations:
            async for event in super().stream(messages, tools):
                yield event
            return
        self.calls += 1
        self.observations.extend(json.loads(message.content) for message in observations)
        yield ModelText(text="Tool result received.")


async def parked_request(app, run_id):
    request = await pending(app.service, run_id)
    assert (await app.service.wait(run_id)).status == "awaiting"
    assert await app.store.read_checkpoint(run_id) is not None
    return request


class FalseyDeny:
    def __bool__(self):
        raise AssertionError("合法策略不能被判断真假")

    async def __call__(self, invocation):
        return "deny"


@pytest.mark.asyncio
async def test_falsey_policy_denies_without_executor_or_truthiness(tmp_path):
    calls = []

    async def execute(args):
        calls.append(args)
        return "actual"

    provider = ToolResultProvider(
        [ModelToolCall(id="denied", name="echo", arguments={"text": "x"})]
    )
    async with await open_application(
        tmp_path / "deny.db", policy=FalseyDeny(), provider=provider, registry=registry_for(execute)
    ) as app:
        run = await app.service.start(RunRequest(text="request", request_key="deny"))
        await app.service.wait(run.id)
        result = await app.service.result(run.id)
        assert result.run.status == "completed" and result.run.code is None
        assert result.tools[0].status == "denied"
        assert result.tools[0].executor_started is False
        assert result.tools[0].code == "permission_denied"
        assert calls == [] and provider.calls == 2
        assert provider.observations == [
            {
                "call_id": result.tools[0].call_id,
                "name": "echo",
                "code": "permission_denied",
                "status": "denied",
            }
        ]


@pytest.mark.asyncio
async def test_post_failure_preserves_actual_result(tmp_path):
    async def broken_post(invocation, outcome):
        raise RuntimeError("SYNTHETIC_SECRET")

    async with await open_application(tmp_path / "post.db", post=broken_post) as app:
        run = await app.service.start(RunRequest(text="/add 2 3", request_key="post"))
        await app.service.wait(run.id)
        result = await app.service.result(run.id)
        assert result.run.status == "failed"
        assert result.tools[0].executor_started is True
        assert result.tools[0].result_available is True
        assert result.tools[0].value == {"sum": 5.0}
        assert result.tools[0].code == "post_hook_error"
        assert app.provider.calls == 1


def make_reply(request, allow=True, key="reply-1"):
    return ApprovalReply(
        request_key=key,
        allow=allow,
        **{k: request[k] for k in ("call_id", "arguments_hash", "tool_hash")},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode,code",
    [
        ("no-policy", "permission_denied"),
        ("input", "invalid_input"),
        ("unknown", "unknown_tool"),
        ("pre-forge", "pre_hook_error"),
        ("output", "invalid_output"),
        ("post-forge", "post_hook_error"),
    ],
)
async def test_tool_chain_rejects_and_never_invents_success(tmp_path, mode, code):
    calls = []

    async def execute(args):
        calls.append(args)
        return {"wrong": "shape"} if mode == "output" else "actual"

    async def pre(invocation):
        return {"fake": "success"}

    async def post(invocation, result):
        result.value = "forged"

    provider = ToolResultProvider(
        [
            ModelToolCall(
                id="c",
                name="absent" if mode == "unknown" else "echo",
                arguments={"text": False} if mode == "input" else {"text": "ok"},
            )
        ]
    )
    kwargs = {"provider": provider, "registry": registry_for(execute)}
    if mode == "no-policy":
        kwargs["policy"] = None
    if mode == "pre-forge":
        kwargs["pre"] = pre
    if mode == "post-forge":
        kwargs["post"] = post
    async with await open_application(tmp_path / (mode + ".db"), **kwargs) as app:
        run = await app.service.start(RunRequest(text="request", request_key=mode))
        await app.service.wait(run.id)
        facts = await app.service.result(run.id)
        if mode == "no-policy":
            assert facts.run.status == "completed" and facts.run.code is None
            assert facts.tools[0].status == "denied" and facts.tools[0].code == code
            assert facts.tools[0].executor_started is False
            assert provider.observations == [
                {
                    "call_id": facts.tools[0].call_id,
                    "name": "echo",
                    "code": code,
                    "status": "denied",
                }
            ]
        else:
            assert facts.run.status == "failed" and facts.run.code == code
        assert len(calls) == (1 if mode in {"output", "post-forge"} else 0)
        assert provider.calls == (2 if mode == "no-policy" else 1) and provider.active == 0
        if mode == "post-forge":
            assert facts.tools[0].value == "actual"


@pytest.mark.asyncio
async def test_repeated_call_id_after_soft_denial_is_rejected(tmp_path):
    calls = []

    async def execute(args):
        calls.append(args)
        return "actual"

    # Deliberately retain the repeating fixture for the separate duplicate-ID guard.
    provider = EventProvider([ModelToolCall(id="repeated", name="echo", arguments={"text": "x"})])
    async with await open_application(
        tmp_path / "repeat-denied.db",
        provider=provider,
        registry=registry_for(execute),
        policy=None,
    ) as app:
        run = await app.service.start(RunRequest(text="request", request_key="repeat"))
        await app.service.wait(run.id)
        facts = await app.service.result(run.id)
        assert facts.run.status == "failed" and facts.run.code == "duplicate_tool_call"
        assert calls == [] and provider.calls == 2 and provider.active == 0
        assert len(facts.tools) == 1
        assert facts.tools[0].status == "denied" and facts.tools[0].code == "permission_denied"
        assert facts.tools[0].executor_started is False


@pytest.mark.parametrize(
    "schema", [{"$ref": "https://example.invalid/secret"}, {"type": "not-a-type"}]
)
def test_output_schema_never_fetches_remote_or_accepts_invalid_schema(schema):
    with pytest.raises(ValueError):
        OutputContract(schema)


def test_registry_rejects_write_and_duplicate_names():
    from dataclasses import replace

    from krim_agent.tools.builtins import builtin_registry

    spec = builtin_registry()._frozen[0][0]
    with pytest.raises(ValueError):
        replace(spec, risk="write")
    with pytest.raises(ValueError):
        __import__("krim_agent.tools.registry", fromlist=["ToolRegistry"]).ToolRegistry(
            (spec, spec)
        )


@pytest.mark.asyncio
async def test_frozen_definition_change_during_intent_wait_refuses_executor(tmp_path):
    calls = []

    async def execute(args):
        calls.append(1)
        return "actual"

    registry = registry_for(execute)
    async with await open_application(tmp_path / "freeze.db", registry=registry) as app:
        original = app.store.append

        async def replace_definition(run_id, kind, data, **kwargs):
            result = await original(run_id, kind, data, **kwargs)
            if kind == "tool_intent":
                registry._items = {}
            return result

        app.store.append = replace_definition
        run = await app.service.start(RunRequest(text="/echo ok", request_key="freeze"))
        await app.service.wait(run.id)
        assert (await app.service.get(run.id)).code == "tool_binding_changed"
        assert calls == []


@pytest.mark.asyncio
async def test_approval_bound_once_and_conflicting_or_cross_run_rejected(tmp_path):
    calls = []

    async def execute(args):
        calls.append(1)
        return "real"

    async def ask(invocation):
        return "ask"

    async with await open_application(
        tmp_path / "approval.db", registry=registry_for(execute), policy=ask
    ) as app:
        run = await app.service.start(RunRequest(text="/echo ok", request_key="approval"))
        request = await pending(app.service, run.id)
        wrong = make_reply(request).model_copy(update={"call_id": "different"})
        with pytest.raises(ServiceError):
            await app.service.reply_approval(run.id, request["request_id"], wrong)
        assert calls == []
        reply = make_reply(request)
        await app.service.reply_approval(run.id, request["request_id"], reply)
        await app.service.wait(run.id)
        assert (await app.service.reply_approval(run.id, request["request_id"], reply))[
            "replayed"
        ] is True
        with pytest.raises(ServiceError):
            await app.service.reply_approval(
                run.id, request["request_id"], make_reply(request, False)
            )
        other = await app.service.start(RunRequest(text="/slow", request_key="other"))
        with pytest.raises(ServiceError):
            await app.service.reply_approval(other.id, request["request_id"], reply)
        await app.service.cancel(other.id)
        assert calls == [1]


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["deny", "expire", "cancel"])
async def test_denied_expired_cancelled_approval_has_zero_executor_calls(
    tmp_path, monkeypatch, action
):
    calls = []

    async def execute(args):
        calls.append(1)
        return "actual"

    async def ask(invocation):
        return "ask"

    provider = ToolResultProvider(
        [ModelToolCall(id="approval", name="echo", arguments={"text": "ok"})]
    )
    async with await open_application(
        tmp_path / (action + ".db"), registry=registry_for(execute), policy=ask, provider=provider
    ) as app:
        run = await app.service.start(RunRequest(text="request", request_key=action))
        request = await parked_request(app, run.id)
        if action == "expire":
            # Advance only the approval admission clock, not execution-budget time.
            monkeypatch.setattr(
                "krim_agent.runtime.service.time",
                SimpleNamespace(time=lambda: request["expires_at"] + 1, monotonic=time.monotonic),
            )
            assert await app.service.approvals(run.id) == []
            with pytest.raises(ServiceError, match="approval_expired"):
                await app.service.reply_approval(run.id, request["request_id"], make_reply(request))
        elif action == "deny":
            await app.service.reply_approval(
                run.id, request["request_id"], make_reply(request, False)
            )
        else:
            cancelled = await app.service.cancel(run.id)
            assert cancelled.status == "aborted" and cancelled.code == "cancelled"
        await app.service.wait(run.id)
        if action != "deny":
            with pytest.raises(ServiceError, match="approval_unavailable"):
                await app.service.reply_approval(run.id, request["request_id"], make_reply(request))
        facts = await app.service.result(run.id)
        assert calls == [] and facts.tools[0].executor_started is False
        assert (facts.run.status, facts.run.code) == {
            "deny": ("completed", None),
            "expire": ("failed", "approval_expired"),
            "cancel": ("aborted", "cancelled"),
        }[action]
        assert sum(event.kind == "terminal" for event in facts.events) == 1
        assert sum(event.kind == "approval_reply" for event in facts.events) == (action == "deny")
        if action == "deny":
            assert facts.tools[0].status == "denied" and facts.tools[0].code == "approval_denied"
            assert provider.observations == [
                {
                    "call_id": facts.tools[0].call_id,
                    "name": "echo",
                    "code": "approval_denied",
                    "status": "denied",
                }
            ]
        else:
            assert provider.calls == 1


@pytest.mark.asyncio
async def test_schema_tamper_is_a_binding_error_before_new_run(tmp_path):
    async def execute(args):
        raise AssertionError("不得调用")

    registry = registry_for(execute)
    async with await open_application(tmp_path / "schema-tamper.db", registry=registry) as app:
        registry._frozen[0][0].output_schema["$ref"] = "https://example.invalid/never-fetch"
        with pytest.raises(ServiceError, match="tool_binding_changed"):
            await app.service.start(RunRequest(text="/echo x", request_key="tamper"))
        assert await app.store.by_key("tamper") is None


@pytest.mark.asyncio
async def test_post_cannot_mutate_nested_original_result(tmp_path):
    async def post(invocation, result):
        result.value["sum"] = -99

    async with await open_application(tmp_path / "nested-post.db", post=post) as app:
        run = await app.service.start(RunRequest(text="/add 2 3", request_key="nested"))
        await app.service.wait(run.id)
        result = await app.service.result(run.id)
        assert result.run.code == "post_hook_error" and result.tools[0].value == {"sum": 5.0}
        assert result.tools[0].executor_started and result.tools[0].result_available


@pytest.mark.asyncio
@pytest.mark.parametrize("allow", [True, False])
async def test_approval_commit_caller_cancellation_delivers_original_decision_once(tmp_path, allow):
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def execute(args):
        calls.append(args.text)
        return "actual"

    async def ask(invocation):
        return "ask"

    database = tmp_path / ("approval-commit-" + str(allow) + ".db")
    async with await open_application(
        database,
        settings=Settings(approval_timeout=3),
        registry=registry_for(execute),
        policy=ask,
    ) as app:
        run = await app.service.start(RunRequest(text="/echo bound", request_key="commit"))
        request = await parked_request(app, run.id)
        request_id = request["request_id"]
        original = app.store._event

        async def hold_transaction(conn, run_id, kind, *args, **kwargs):
            event = await original(conn, run_id, kind, *args, **kwargs)
            if kind == "approval_reply":
                entered.set()
                await release.wait()
            return event

        app.store._event = hold_transaction
        reply = make_reply(request, allow)
        caller = asyncio.create_task(app.service.reply_approval(run.id, request_id, reply))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            with sqlite3.connect("file:" + str(database) + "?mode=ro", uri=True) as reader:
                assert (
                    reader.execute(
                        "SELECT count(*) FROM run_events WHERE kind='approval_reply'"
                    ).fetchone()[0]
                    == 0
                )
            caller.cancel()
            await asyncio.sleep(0)
            caller.cancel()
            assert not caller.done() and calls == []
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await caller
            replay = await app.service.reply_approval(run.id, request_id, reply)
            assert replay["replayed"] is True and replay["allow"] is allow
            await asyncio.wait_for(app.service.wait(run.id), 2)
            facts = await app.service.result(run.id)
            assert facts.run.status == "completed" and facts.run.code is None
            assert facts.tools[0].executor_started is allow
            assert facts.tools[0].status == ("ok" if allow else "denied")
            assert facts.tools[0].code == (None if allow else "approval_denied")
            assert calls == (["bound"] if allow else [])
            decisions = [event for event in facts.events if event.kind == "approval_reply"]
            assert len(decisions) == 1 and decisions[0].data["reply"] == reply.model_dump(
                mode="json"
            )
            assert sum(event.kind == "terminal" for event in facts.events) == 1
            for conflicting in [
                make_reply(request, not allow),
                make_reply(request, allow, "other-key"),
            ]:
                with pytest.raises(ServiceError, match="approval_conflict"):
                    await app.service.reply_approval(run.id, request_id, conflicting)
            assert calls == (["bound"] if allow else [])
        finally:
            release.set()
            await asyncio.gather(caller, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("stop_first", [False, True], ids=["commit-first", "stop-first"])
async def test_approval_commit_racing_public_cancel_cannot_revive_run(
    tmp_path, monkeypatch, stop_first
):
    entered, release, stopped = (asyncio.Event() for _ in range(3))
    calls = []

    async def execute(args):
        calls.append(args.text)
        return "actual"

    async def ask(invocation):
        return "ask"

    async with await open_application(
        tmp_path / ("cancel-commit-" + str(stop_first) + ".db"),
        registry=registry_for(execute),
        policy=ask,
    ) as app:
        run = await app.service.start(RunRequest(text="/echo bound", request_key="race"))
        request = await parked_request(app, run.id)
        request_id = request["request_id"]
        original = app.store.append
        original_stop = app.store.request_stop

        async def held_append(run_id, kind, data, **kwargs):
            if kind == "approval_reply" and stop_first:
                # The reply passed admission, but stop commits before its event.
                entered.set()
                await release.wait()
            event = await original(run_id, kind, data, **kwargs)
            if kind == "approval_reply" and not stop_first:
                # Decision is durable; pause before the service can resume it.
                entered.set()
                await release.wait()
            return event

        async def observed_stop(run_id):
            await original_stop(run_id)
            stopped.set()

        monkeypatch.setattr(app.store, "append", held_append)
        monkeypatch.setattr(app.store, "request_stop", observed_stop)
        reply = make_reply(request)
        caller = asyncio.create_task(app.service.reply_approval(run.id, request_id, reply))
        cancellation = None
        try:
            await asyncio.wait_for(entered.wait(), 2)
            cancellation = asyncio.create_task(app.service.cancel(run.id))
            await asyncio.wait_for(stopped.wait(), 2)
            result = await asyncio.wait_for(cancellation, 2)
            assert result.status == "aborted" and result.code == "cancelled"
            release.set()
            outcome = (await asyncio.wait_for(asyncio.gather(caller, return_exceptions=True), 2))[0]
            if stop_first:
                assert isinstance(outcome, ServiceError) and outcome.code == "run_closed"
                with pytest.raises(ServiceError, match="approval_unavailable"):
                    await app.service.reply_approval(run.id, request_id, reply)
            else:
                assert outcome["replayed"] is False
                assert (await app.service.reply_approval(run.id, request_id, reply))["replayed"]
            assert (await app.service.wait(run.id)).status == "aborted"
            assert await app.service.approvals(run.id) == []
            assert calls == [] and app.provider.calls == 1
            facts = await app.service.result(run.id)
            assert sum(e.kind == "terminal" for e in facts.events) == 1
            assert sum(e.kind == "approval_reply" for e in facts.events) == (not stop_first)
        finally:
            release.set()
            await asyncio.gather(
                caller, *([cancellation] if cancellation else []), return_exceptions=True
            )


@pytest.mark.asyncio
async def test_decision_admitted_before_expiry_survives_delayed_commit(tmp_path, monkeypatch):
    """Expiry gates new decisions, not an already admitted decision's commit/replay."""
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def execute(args):
        calls.append(args.text)
        return "actual"

    async def ask(invocation):
        return "ask"

    async with await open_application(
        tmp_path / "expiry-commit.db",
        registry=registry_for(execute),
        policy=ask,
        settings=Settings(approval_timeout=30),
    ) as app:
        run = await app.service.start(RunRequest(text="/echo bound", request_key="expiry-commit"))
        request = await parked_request(app, run.id)
        original = app.store._event

        async def held_event(conn, run_id, kind, *args, **kwargs):
            event = await original(conn, run_id, kind, *args, **kwargs)
            if kind == "approval_reply":
                entered.set()
                await release.wait()
            return event

        monkeypatch.setattr(app.store, "_event", held_event)
        reply = make_reply(request)
        caller = asyncio.create_task(
            app.service.reply_approval(run.id, request["request_id"], reply)
        )
        try:
            await asyncio.wait_for(entered.wait(), 2)
            monkeypatch.setattr(
                "krim_agent.runtime.service.time",
                SimpleNamespace(time=lambda: request["expires_at"] + 1, monotonic=time.monotonic),
            )
            assert calls == []
            release.set()
            assert (await asyncio.wait_for(caller, 2))["replayed"] is False
            assert (await app.service.wait(run.id)).status == "completed"
            assert (await app.service.reply_approval(run.id, request["request_id"], reply))[
                "replayed"
            ]
            facts = await app.service.result(run.id)
            assert calls == ["bound"]
            assert sum(e.kind == "approval_reply" for e in facts.events) == 1
            assert sum(e.kind == "terminal" for e in facts.events) == 1
        finally:
            release.set()
            await asyncio.gather(caller, return_exceptions=True)
