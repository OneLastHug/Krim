import asyncio

import pytest

from krim_agent.budgets import TokenPrice, UsageBudgetConfig
from krim_agent.persistence.budgets import UsageBudgetStore
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.usage import TokenUsage


async def pending_run(repository, key="one"):
    topic = await repository.create_topic("budget ledger")
    run, _ = await repository.begin_run(topic.id, ChatRequest(text="test", idempotency_key=key))
    return run


def test_price_uses_integer_micros_and_rounds_up_without_float():
    price = TokenPrice(currency="USD", input_micros_per_million=1, output_micros_per_million=2)
    assert price.cost(0, 0) == 0
    assert price.cost(1, 1) == 1
    assert price.cost(1_000_000, 1_000_000) == 3
    with pytest.raises(ValueError):
        UsageBudgetConfig(max_cost_micros=1)


async def test_reservation_settlement_and_idempotency_across_scopes(repository):
    store = UsageBudgetStore(repository.database)
    root = await store.bind("runtime", "root", UsageBudgetConfig(max_total_tokens=100))
    child = await store.bind("agent", "child", UsageBudgetConfig(max_total_tokens=60), root)
    run = await pending_run(repository)
    reserved = await store.reserve(run.id, 0, child, 20, 10, "body")
    assert (await store.account(root[0])).held_tokens == 30
    assert (await store.account(child[-1])).held_tokens == 30
    assert (await store.reserve(run.id, 0, child, 20, 10, "body")).request_id == reserved.request_id
    await store.transition(reserved.request_id, "dispatched")
    actual = TokenUsage(input_tokens=15, output_tokens=5, total_tokens=20)
    settled = await store.settle(reserved.request_id, actual)
    assert not settled.overdrawn and settled.state == "settled"
    assert (await store.account(root[0])).spent_tokens == 20
    assert (await store.account(child[-1])).held_tokens == 0
    await store.settle(reserved.request_id, actual)
    assert (await store.account(root[0])).spent_tokens == 20
    with pytest.raises(ServiceError) as error:
        await store.reserve(run.id, 0, child, 21, 10, "changed")
    assert error.value.code == "budget_request_conflict"


async def test_unknown_reservation_blocks_new_request_until_receipt_resolution(repository):
    store = UsageBudgetStore(repository.database)
    ids = await store.bind("agent", "unknown", UsageBudgetConfig(max_total_tokens=100))
    run = await pending_run(repository)
    reserved = await store.reserve(run.id, 0, ids, 20, 10, "one")
    await store.transition(reserved.request_id, "dispatched")
    uncertain = await store.transition(reserved.request_id, "uncertain")
    assert (await store.account(ids[0])).uncertain_requests == 1
    with pytest.raises(ServiceError) as error:
        await store.reserve(run.id, 1, ids, 1, 1, "two")
    assert error.value.code == "budget_usage_unknown"
    with pytest.raises(ServiceError):
        await store.settle(
            reserved.request_id,
            TokenUsage(input_tokens=10, output_tokens=5, total_tokens=15),
            receipt="receipt",
            revision=0,
        )
    result = await store.settle(
        reserved.request_id,
        TokenUsage(input_tokens=10, output_tokens=5, total_tokens=15),
        receipt="receipt",
        revision=uncertain.revision,
    )
    assert result.source == "host_resolution"
    assert (await store.account(ids[0])).held_tokens == 0
    assert (await store.reserve(run.id, 1, ids, 1, 1, "two")).state == "reserved"


async def test_all_parent_accounts_reserve_atomically_and_reject_partial_debit(repository):
    store = UsageBudgetStore(repository.database)
    first = await store.bind("runtime", "parent-a", UsageBudgetConfig(max_total_tokens=100))
    second = await store.bind("runtime", "parent-b", UsageBudgetConfig(max_total_tokens=10))
    run = await pending_run(repository)
    with pytest.raises(ServiceError) as error:
        await store.reserve(run.id, 0, (*first, *second), 10, 1, "too much")
    assert error.value.code == "budget_tokens_exceeded"
    assert (await store.account(first[0])).held_tokens == 0
    assert await store.requests(run.id) == []


async def test_same_available_balance_cannot_be_reserved_twice(repository):
    store = UsageBudgetStore(repository.database)
    ids = await store.bind("agent", "race", UsageBudgetConfig(max_total_tokens=10))
    one, two = await pending_run(repository, "one"), await pending_run(repository, "two")
    results = await asyncio.gather(
        store.reserve(one.id, 0, ids, 5, 5, "one"),
        store.reserve(two.id, 0, ids, 5, 5, "two"),
        return_exceptions=True,
    )
    assert sum(isinstance(value, ServiceError) for value in results) == 1
    assert (await store.account(ids[0])).held_tokens == 10


async def test_cost_reservation_snapshot_and_actual_overrun_are_retained(repository):
    store = UsageBudgetStore(repository.database)
    config = UsageBudgetConfig(
        max_total_tokens=100,
        max_cost_micros=10,
        price=TokenPrice(
            currency="CNY", input_micros_per_million=1_000_000, output_micros_per_million=2_000_000
        ),
    )
    ids = await store.bind("agent", "priced", config)
    run = await pending_run(repository)
    reservation = await store.reserve(run.id, 0, ids, 4, 3, "priced")
    assert reservation.allocations[0].reserved_cost_micros == 10
    with pytest.raises(ServiceError) as error:
        await store.reserve(run.id, 1, ids, 1, 1, "extra")
    assert error.value.code == "budget_cost_exceeded"
    await store.transition(reservation.request_id, "dispatched")
    settled = await store.settle(
        reservation.request_id, TokenUsage(input_tokens=6, output_tokens=3, total_tokens=9)
    )
    assert settled.overdrawn and settled.allocations[0].charged_cost_micros == 12
    account = await store.account(ids[0])
    assert account.spent_tokens == 9 and account.spent_cost_micros == 12 and account.overdrawn


class MeteredProvider:
    name = "budget-model"

    def __init__(self, *, report=True, gate=None):
        self.calls = 0
        self.report, self.gate = report, gate

    async def stream(self, _messages, _tools):
        from krim_agent.providers import ModelText, ModelUsage

        self.calls += 1
        yield ModelText(text="预算回答")
        if self.gate is not None:
            await self.gate.wait()
        if self.report:
            yield ModelUsage(usage=TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7))


def runtime_settings(limit=1000):
    from krim_agent.config import Settings

    return Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        context_tokenizer="cl100k_base",
        context_reserved_output_tokens=16,
        usage_budget=UsageBudgetConfig(max_total_tokens=limit),
    )


async def test_runtime_reserves_before_provider_and_settles_reported_usage():
    from krim_agent.sdk import PythonAI, UsageBudgetPart
    from krim_agent.tools import ToolRegistry

    provider = MeteredProvider()
    async with await PythonAI.create(
        provider=provider, tools=ToolRegistry(), settings=runtime_settings()
    ) as ai:
        stream = await ai.stream_text("test", idempotency_key="one")
        result = await stream.collect()
        assert result.run.status == "completed" and result.token_usage.total_tokens == 7
        parts = [part async for part in stream.full_stream()]
        states = [part.request.state for part in parts if isinstance(part, UsageBudgetPart)]
        assert states == ["reserved", "settled"]
        account = (await ai.runtime.budget_store.accounts())[0]
        assert account.spent_tokens == 7 and account.held_tokens == 0
        replay = await ai.generate_text("test", topic_id=result.run.topic_id, idempotency_key="one")
        assert replay.run.id == result.run.id and provider.calls == 1
        assert (await ai.runtime.budget_store.accounts())[0].spent_tokens == 7


async def test_runtime_budget_rejection_makes_zero_provider_calls():
    from krim_agent.sdk import PythonAI
    from krim_agent.tools import ToolRegistry

    provider = MeteredProvider()
    async with await PythonAI.create(
        provider=provider, tools=ToolRegistry(), settings=runtime_settings(1)
    ) as ai:
        result = await ai.generate_text("reject")
        assert result.code == "budget_tokens_exceeded" and provider.calls == 0
        assert await ai.runtime.budget_store.requests(result.run.id) == []


async def test_missing_usage_fails_closed_and_keeps_reserved_budget():
    from krim_agent.sdk import PythonAI
    from krim_agent.tools import ToolRegistry

    provider = MeteredProvider(report=False)
    async with await PythonAI.create(
        provider=provider, tools=ToolRegistry(), settings=runtime_settings()
    ) as ai:
        result = await ai.generate_text("unknown")
        assert result.code == "budget_usage_unknown" and provider.calls == 1
        account = (await ai.runtime.budget_store.accounts())[0]
        assert account.held_tokens > 0 and account.uncertain_requests == 1
        blocked = await ai.generate_text("next")
        assert blocked.code == "budget_usage_unknown" and provider.calls == 1


async def test_agent_budget_survives_multiple_turns_and_uses_own_scope(repository):
    from krim_agent.agents import AgentDefinition, AgentManager
    from krim_agent.context_engine import ContextRunConfig
    from krim_agent.tools import ToolRegistry

    config = ContextRunConfig.from_settings(runtime_settings())
    manager = AgentManager(repository)
    provider = MeteredProvider()
    try:
        handle = await manager.spawn(
            AgentDefinition(
                name="limited",
                provider=provider,
                tools=ToolRegistry(),
                context_config=config,
                usage_budget=UsageBudgetConfig(max_total_tokens=100),
            )
        )
        first = await handle.execute("short", "first")
        assert first.status == "completed"
        first_account = (await handle.runtime.budget_store.accounts())[0]
        assert first_account.owner_kind == "agent" and first_account.owner_id == handle.agent_id
        assert first_account.spent_tokens == 7
        second = await handle.resume("this second question is longer than first", "second")
        assert second.code == "budget_tokens_exceeded" and provider.calls == 1
        assert (
            await handle.runtime.budget_store.account(first_account.account_id)
        ).spent_tokens == 7
    finally:
        await manager.close()


async def test_workflow_requests_share_persisted_scope_and_budget():
    from krim_agent.application.workflow import WorkflowServices
    from krim_agent.sdk import PythonAI
    from krim_agent.tools import ToolRegistry
    from krim_agent.workflow import WorkflowEngine, WorkflowGraph, WorkflowNode

    provider = MeteredProvider()
    async with await PythonAI.create(
        provider=provider, tools=ToolRegistry(), settings=runtime_settings()
    ) as ai:
        services = WorkflowServices(
            models={"default": ai}, usage_budget=UsageBudgetConfig(max_total_tokens=1)
        )
        graph = WorkflowGraph(nodes=[WorkflowNode(id="model", kind="agent")])
        result = (
            await WorkflowEngine()
            .execute(graph, "workflow budget", services.bindings(graph))
            .wait()
        )
        assert result.code == "budget_tokens_exceeded" and provider.calls == 0
        accounts = await ai.runtime.budget_store.accounts()
        assert any(
            account.owner_kind == "workflow" and account.owner_id == result.run_id
            for account in accounts
        )


async def test_two_database_instances_cannot_both_take_the_same_balance(repository):
    from krim_agent.persistence import Database

    first = UsageBudgetStore(repository.database)
    account_ids = await first.bind(
        "runtime", "process-shared", UsageBudgetConfig(max_total_tokens=10)
    )
    one, two = await pending_run(repository, "one-db"), await pending_run(repository, "two-db")
    other_database = Database(str(repository.database.engine.url))
    await other_database.initialize()
    try:
        other = UsageBudgetStore(other_database)
        results = await asyncio.gather(
            first.reserve(one.id, 0, account_ids, 5, 5, "one-db"),
            other.reserve(two.id, 0, account_ids, 5, 5, "two-db"),
            return_exceptions=True,
        )
        assert sum(isinstance(value, ServiceError) for value in results) == 1
        assert (await first.account(account_ids[0])).held_tokens == 10
    finally:
        await other_database.close()


async def test_database_recovery_retains_interrupted_reservation_and_blocks(repository):
    store = UsageBudgetStore(repository.database)
    ids = await store.bind("runtime", "recover", UsageBudgetConfig(max_total_tokens=100))
    run = await pending_run(repository)
    reserved = await store.reserve(run.id, 0, ids, 20, 10, "recover")
    await store.transition(reserved.request_id, "dispatched")
    await repository.recover_interrupted()
    await store.recover()
    recovered = await store.request(reserved.request_id)
    assert recovered.state == "uncertain" and (await store.account(ids[0])).held_tokens == 30
    with pytest.raises(ServiceError) as error:
        await store.reserve(run.id, 1, ids, 1, 1, "retry")
    assert error.value.code == "budget_usage_unknown"


async def test_budget_definition_cannot_change_to_reset_spending(repository):
    store = UsageBudgetStore(repository.database)
    ids = await store.bind("agent", "stable", UsageBudgetConfig(max_total_tokens=100))
    run = await pending_run(repository)
    reserved = await store.reserve(run.id, 0, ids, 5, 5, "stable")
    await store.transition(reserved.request_id, "dispatched")
    await store.settle(
        reserved.request_id, TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7)
    )
    with pytest.raises(ServiceError) as error:
        await store.bind("agent", "stable", UsageBudgetConfig(max_total_tokens=200))
    assert error.value.code == "budget_definition_changed"
    assert (await store.account(ids[0])).spent_tokens == 7


async def test_runtime_receipt_resolution_allows_new_call_without_rewriting_usage():
    from krim_agent.sdk import PythonAI
    from krim_agent.tools import ToolRegistry

    provider = MeteredProvider(report=False)
    async with await PythonAI.create(
        provider=provider, tools=ToolRegistry(), settings=runtime_settings()
    ) as ai:
        failed = await ai.generate_text("missing report")
        requests = await ai.runtime.budget_store.requests(failed.run.id)
        request = requests[0]
        await ai.runtime.budget_store.settle(
            request.request_id,
            TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7),
            receipt="host checked",
            revision=request.revision,
        )
        assert (await ai.runtime.repository.usage_view(failed.run.id)).token_usage is None
        provider.report = True
        again = await ai.generate_text("after verified resolution")
        assert again.run.status == "completed" and provider.calls == 2
        account = (await ai.runtime.budget_store.accounts())[0]
        assert account.spent_tokens == 14 and account.uncertain_requests == 0


async def test_cancel_after_reservation_commit_releases_before_provider():
    from krim_agent.sdk import PythonAI
    from krim_agent.tools import ToolRegistry

    provider = MeteredProvider()
    entered, release = asyncio.Event(), asyncio.Event()
    async with await PythonAI.create(
        provider=provider, tools=ToolRegistry(), settings=runtime_settings()
    ) as ai:
        original = ai.runtime.budget_store.reserve

        async def delayed(*args, **kwargs):
            row = await original(*args, **kwargs)
            entered.set()
            await release.wait()
            return row

        ai.runtime.budget_store.reserve = delayed
        stream = await ai.stream_text("cancel before dispatch")
        await asyncio.wait_for(entered.wait(), 5)
        stop = asyncio.create_task(stream.abort())
        await asyncio.sleep(0.02)
        assert not stop.done()
        release.set()
        await stop
        row = (await ai.runtime.budget_store.requests(stream.run.id))[0]
        assert row.state == "released" and provider.calls == 0
        assert (await ai.runtime.budget_store.accounts())[0].held_tokens == 0


async def test_cancel_during_provider_keeps_unknown_reservation():
    from krim_agent.sdk import PythonAI, TextDelta
    from krim_agent.tools import ToolRegistry

    provider = MeteredProvider(gate=asyncio.Event())
    async with await PythonAI.create(
        provider=provider, tools=ToolRegistry(), settings=runtime_settings()
    ) as ai:
        stream = await ai.stream_text("cancel dispatched")
        subscription = stream.full_stream()
        async for part in subscription:
            if isinstance(part, TextDelta):
                break
        await subscription.aclose()
        await stream.abort()
        row = (await ai.runtime.budget_store.requests(stream.run.id))[0]
        assert row.state == "uncertain" and provider.calls == 1
        account = (await ai.runtime.budget_store.accounts())[0]
        assert account.held_tokens > 0 and account.uncertain_requests == 1


async def test_settlement_storage_failure_blocks_next_provider_call():
    from krim_agent.sdk import PythonAI
    from krim_agent.tools import ToolRegistry

    provider = MeteredProvider()
    async with await PythonAI.create(
        provider=provider, tools=ToolRegistry(), settings=runtime_settings()
    ) as ai:

        async def refuse(*_args, **_kwargs):
            raise OSError("测试结算存储故障")

        ai.runtime.budget_store.settle = refuse
        result = await ai.generate_text("storage failure")
        assert result.run.status == "failed" and provider.calls == 1
        row = (await ai.runtime.budget_store.requests(result.run.id))[0]
        assert row.state == "uncertain"
        next_result = await ai.generate_text("must not dispatch again")
        assert next_result.code == "budget_usage_unknown" and provider.calls == 1


async def test_token_report_overrun_is_saved_and_blocks_continuation():
    from krim_agent.providers import ModelText, ModelUsage
    from krim_agent.sdk import PythonAI
    from krim_agent.tools import ToolRegistry

    class Large(MeteredProvider):
        async def stream(self, _messages, _tools):
            self.calls += 1
            yield ModelText(text="report overrun")
            yield ModelUsage(
                usage=TokenUsage(input_tokens=500, output_tokens=100, total_tokens=600)
            )

    provider = Large()
    async with await PythonAI.create(
        provider=provider, tools=ToolRegistry(), settings=runtime_settings()
    ) as ai:
        result = await ai.generate_text("one")
        assert result.code == "budget_report_overrun"
        account = (await ai.runtime.budget_store.accounts())[0]
        assert account.spent_tokens == 600 and account.overdrawn
        rejected = await ai.generate_text("two")
        assert rejected.code == "budget_account_overdrawn" and provider.calls == 1


async def test_parent_budget_is_charged_by_delegated_agent(repository):
    from krim_agent.agents import AgentDefinition, AgentManager
    from krim_agent.context_engine import ContextRunConfig
    from krim_agent.tools import ToolContext, ToolRegistry

    store = UsageBudgetStore(repository.database)
    root = await store.bind(
        "runtime", "delegation-parent", UsageBudgetConfig(max_total_tokens=1000)
    )
    settings = runtime_settings()
    context = ContextRunConfig.from_settings(settings)
    tools = ToolRegistry()
    manager = AgentManager(repository)
    try:
        handle = await manager.spawn(
            AgentDefinition(name="delegated", provider=MeteredProvider(), tools=tools),
            parent_context=ToolContext(
                tools, context_config=context.model_dump(mode="json"), budget_account_ids=root
            ),
        )
        result = await handle.execute("child", "child")
        assert result.status == "completed"
        assert (await store.account(root[0])).spent_tokens == 7
        requests = await store.requests(result.run_id)
        assert [allocation.account_id for allocation in requests[0].allocations] == list(root)
    finally:
        await manager.close()


async def test_two_real_python_processes_cannot_oversubscribe_budget(repository):
    import sys

    store = UsageBudgetStore(repository.database)
    accounts = await store.bind(
        "runtime", "real-process-race", UsageBudgetConfig(max_total_tokens=10)
    )
    one, two = await pending_run(repository, "p-one"), await pending_run(repository, "p-two")
    processes = []
    try:
        for run in (one, two):
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "tests/fixtures/budget_reserve_worker.py",
                repository.database.engine.url.database,
                run.id,
                accounts[0],
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            assert await asyncio.wait_for(process.stdout.readline(), 15) == b"READY\n"
            processes.append(process)
        for process in processes:
            process.stdin.write(b"GO\n")
            await process.stdin.drain()
        results = []
        for process in processes:
            stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
            assert process.returncode == 0, stderr.decode()
            results.append(__import__("json").loads(stdout))
        assert sum(value.get("state") == "reserved" for value in results) == 1
        assert sum(value.get("code") == "budget_tokens_exceeded" for value in results) == 1
        assert (await store.account(accounts[0])).held_tokens == 10
    finally:
        for process in processes:
            if process.returncode is None:
                process.terminate()
                await process.wait()


async def test_persistent_agent_keeps_spending_across_real_process_restore(tmp_path):
    import json
    import sys

    path = tmp_path / "persistent-budget.sqlite"

    async def worker(mode):
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "tests/fixtures/persistent_budget_worker.py",
            str(path),
            mode,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), 20)
        assert process.returncode == 0, stderr.decode()
        return json.loads(stdout)

    first = await worker("first")
    assert (
        first["status"] == "completed"
        and first["spent_tokens"] == 7
        and first["provider_calls"] == 1
    )
    second = await worker("second")
    assert second["agent_id"] == first["agent_id"]
    assert (
        second["code"] == "budget_tokens_exceeded"
        and second["spent_tokens"] == 7
        and second["provider_calls"] == 0
    )


async def test_http_client_and_cli_read_and_resolve_budget(repository, server_url):
    import json
    import sys

    from krim_agent.budgets import BudgetResolveRequest
    from krim_agent.client import KrimClient
    from krim_agent.sdk import PythonAI
    from krim_agent.tools import ToolRegistry

    settings = runtime_settings()
    settings.database_url = str(repository.database.engine.url)
    provider = MeteredProvider(report=False)
    async with await PythonAI.create(
        provider=provider, tools=ToolRegistry(), settings=settings
    ) as ai:
        result = await ai.generate_text("http unknown budget")
        request = (await ai.runtime.budget_store.requests(result.run.id))[0]

        def inspect_and_resolve():
            with KrimClient(server_url) as client:
                accounts = client.budget_accounts()
                before = client.budget_request(request.request_id)
                assert accounts[0].uncertain_requests == 1 and before.state == "uncertain"
                return client.budget_resolve(
                    request.request_id,
                    BudgetResolveRequest(
                        revision=before.revision,
                        receipt="trusted test receipt",
                        usage=TokenUsage(input_tokens=5, output_tokens=2, total_tokens=7),
                    ),
                )

        settled = await asyncio.to_thread(inspect_and_resolve)
        assert settled.state == "settled" and settled.source == "host_resolution"
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "krim_agent",
            "budget",
            "run",
            result.run.id,
            "--url",
            server_url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
        assert process.returncode == 0, stderr.decode()
        values = json.loads(stdout)
        assert values[0]["state"] == "settled" and values[0]["receipt"] == "trusted test receipt"
        assert (await ai.runtime.repository.usage_view(result.run.id)).token_usage is None


async def test_budgets_migration_refuses_to_discard_accounts(repository):
    from alembic import command
    from sqlalchemy import text

    from krim_agent.persistence.migration import migration_config

    store = UsageBudgetStore(repository.database)
    await store.bind("runtime", "protected", UsageBudgetConfig(max_total_tokens=100))
    with pytest.raises(RuntimeError, match="累计预算账本"):
        async with repository.database.engine.begin() as connection:
            await connection.run_sync(
                lambda conn: command.downgrade(migration_config(conn), "0016_agent_handles")
            )
    async with repository.database.engine.connect() as connection:
        assert (
            await connection.execute(text("SELECT version_num FROM alembic_version"))
        ).scalar() == "0041_workspace_tree_plans"
    assert (await store.accounts())[0].owner_id == "protected"


async def test_nested_workflow_models_share_root_budget():
    from krim_agent.application.workflow import WorkflowServices
    from krim_agent.sdk import PythonAI
    from krim_agent.tools import ToolRegistry
    from krim_agent.workflow import WorkflowEngine, WorkflowGraph, WorkflowNode

    nested = WorkflowGraph(nodes=[WorkflowNode(id="model", kind="agent")])
    graph = WorkflowGraph(
        nodes=[
            WorkflowNode(
                id="fanout", kind="fanout", config={"graph": nested.model_dump(mode="json")}
            )
        ]
    )
    provider = MeteredProvider()
    async with await PythonAI.create(
        provider=provider, tools=ToolRegistry(), settings=runtime_settings()
    ) as ai:
        services = WorkflowServices(
            models={"default": ai}, usage_budget=UsageBudgetConfig(max_total_tokens=100)
        )
        result = (
            await WorkflowEngine(max_concurrency=1)
            .execute(
                graph,
                ["one", "another long item with insufficient reservation"],
                services.bindings(graph),
            )
            .wait()
        )
        assert result.status == "failed" and provider.calls == 1
        accounts = [
            account
            for account in await ai.runtime.budget_store.accounts()
            if account.owner_kind == "workflow"
        ]
        assert (
            len(accounts) == 1
            and accounts[0].owner_id == result.run_id
            and accounts[0].spent_tokens == 7
        )
