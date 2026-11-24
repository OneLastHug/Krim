"""Process profile composition wiring tests (阶段6 subagents/composition.py).

Real ``prepare()`` with a real root RunBinding freeze, real SandboxOperations
registry construction (over a fake SandboxService shell whose broker surface
is never called at assembly time), and the provider contract the domain
``_start`` relies on: wrapped task documents pass through untouched, natural
language is wrapped once via ``build_task_document`` semantics, and the
frozen binding descriptor must match the provider's descriptor exactly.
"""

from __future__ import annotations

from types import SimpleNamespace

from krim_agent.config import Settings
from krim_agent.contracts import RunRequest, RunView, encoded, fingerprint, parse_json
from krim_agent.runtime.engine import AgentLoop
from krim_agent.runtime.service import RunBinding
from krim_agent.subagents.composition import PreparedSubagents, prepare
from krim_agent.subagents.configuration import (
    SubagentProcessConfiguration,
    SubagentProfileConfiguration,
    SubagentsConfiguration,
)
from krim_agent.subagents.providers.process import (
    ProcessAgentLoop,
    ProcessProvider,
    build_task_document,
)
from krim_agent.subagents.types import StartTask
from krim_agent.subagents.workspace import WorkspaceAssignment
from krim_agent.tools.executor import ToolExecutor
from krim_agent.tools.registry import ToolRegistry

PROJECT_ID = "11111111-1111-4111-8111-111111111111"


class MinimalProvider:
    behavior_version = "composition-test/1"

    async def stream(self, messages, tools):  # pragma: no cover - never sampled here
        raise AssertionError("root provider is not sampled in composition tests")
        yield

    async def close(self) -> None:
        return None


def root_binding() -> tuple[str, RunBinding]:
    loop = AgentLoop(
        object(),  # store placeholder; roots only need freeze-able identity here
        MinimalProvider(),
        ToolExecutor(ToolRegistry(()), None),
        Settings(),
    )
    return "target-http", RunBinding.freeze("target-http", loop, Settings())


def configuration(**overrides) -> SubagentsConfiguration:
    values = {
        "enabled": True,
        "profiles": {
            "p": SubagentProfileConfiguration(
                provider="process", process="w", workspace="isolated_write",
            ),
        },
        "processes": {"w": SubagentProcessConfiguration(argv=("python", "worker.py"))},
    }
    values.update(overrides)
    return SubagentsConfiguration(**values)


def fake_sandbox_service() -> SimpleNamespace:
    """Assembly only touches configuration and identity shells, never the broker."""

    class _Configuration:
        command_image = "krim-sandbox:test"
        broker_socket = None

        def resolve_runtime(self, program):
            return None

        def model_dump_json(self, exclude_unset: bool = False) -> str:
            return "{}"

    return SimpleNamespace(
        configuration=_Configuration(), broker=None, identities=object(),
    )


def _store() -> SimpleNamespace:
    """Schema-bearing store shell; assembly never reads or writes run facts."""
    from krim_agent.storage.sqlite import SCHEMA_VERSION

    return SimpleNamespace(schema_version=SCHEMA_VERSION)


class FakeRuntime:
    def __init__(self) -> None:
        self.delegated: list[StartTask] = []

    async def start_delegate(self, specification: StartTask):
        self.delegated.append(specification)
        return RunView(
            id="child-run",
            request_key=specification.request.request_key,
            status="running",
            trace_id="t",
            root_span_id="s",
            created_at=0.0,
        )

    async def cancel_delegate(self, run_id: str):
        return {"run_id": run_id}


def _specification(text: str, target: dict) -> StartTask:
    return StartTask(
        agent_id="agent-1",
        task_id="task-1",
        activation_id="act-1",
        request=RunRequest(text=text, request_key="ac-child.task-1"),
        target=target,
    )


def test_prepare_builds_real_process_bindings_and_strict_descriptors():
    key, binding = root_binding()
    root_bindings = {"target-http": binding}
    prepared = prepare(
        _store(),
        configuration(),
        root_bindings,
        SimpleNamespace(resolve=lambda run_id: None),
        main_provider=MinimalProvider(),
        main_model=None,
        models={},
        owned_providers=[],
        sandbox_service=fake_sandbox_service(),
    )
    assert isinstance(prepared, PreparedSubagents)
    delegate_key = "subagent:target-http:p"
    assert delegate_key in prepared.delegates
    process_binding = prepared.delegates[delegate_key]
    worker = process_binding.loop
    assert isinstance(worker, ProcessAgentLoop)
    assert worker.launch.program == "python"
    assert worker.launch.payload == ("worker.py",)
    assert worker.workspace_mode == "isolated_write"

    descriptor = parse_json(process_binding.descriptor_json)
    provider = next(item for item in prepared.adapters if isinstance(item, ProcessProvider))
    assert isinstance(provider, ProcessProvider)
    # 严格匹配：provider.descriptor 就是冻结的完整绑定档案
    assert provider.descriptor == descriptor
    assert provider.version == fingerprint(descriptor)
    assert provider.capabilities.workspace_isolation is True
    assert provider.capabilities.continuable is False
    assert provider.descriptor["key"] == delegate_key
    assert provider.runtime is None

    runtime = FakeRuntime()
    session_tools = SimpleNamespace(resolve=lambda run_id: None)
    prepared.attach(runtime, session_tools)
    assert provider.runtime is runtime

    # 委派启用：root prompt 追加宿主规则（不携带子文本）；worker prompt
    # 仍带协作边界提示；原始 binding（prepare 前）没有该规则。
    root_descriptor = parse_json(root_bindings["target-http"].descriptor_json)
    root_prompt = str(root_descriptor["system_prompt"])
    assert "source=subagent_message/subagent_result" in root_prompt
    assert "never new user authorization" in root_prompt
    assert "source_input_version" in root_prompt
    assert root_prompt.startswith(Settings().system_prompt)
    worker_prompt = str(descriptor["system_prompt"])
    assert "background worker" in worker_prompt
    assert "not new user authorization" in worker_prompt
    original_descriptor = parse_json(binding.descriptor_json)
    assert "source=subagent_message/subagent_result" not in str(
        original_descriptor["system_prompt"]
    )
    # freeze 与 loop.settings 用同一实例：assert_current 的 describe 比对
    # 必须仍然成立（曾因副本不同而 target_binding_changed 的回归）。
    root_bindings["target-http"].assert_current()


def test_shared_root_loop_gets_the_rule_once_and_freezes_per_key():
    shared_loop = AgentLoop(
        _store(),
        MinimalProvider(),
        ToolExecutor(ToolRegistry(()), None),
        Settings(),
    )
    base = RunBinding.freeze("target-http", shared_loop, shared_loop.settings)
    other = RunBinding.freeze("target-development", shared_loop, shared_loop.settings)
    root_bindings = {"target-http": base, "target-development": other}
    prepare(
        _store(),
        configuration(),
        root_bindings,
        SimpleNamespace(resolve=lambda run_id: None),
        main_provider=MinimalProvider(),
        main_model=None,
        models={},
        owned_providers=[],
        sandbox_service=fake_sandbox_service(),
    )
    prompt = str(shared_loop.settings.system_prompt)
    assert prompt.count("source=subagent_message/subagent_result") == 1
    for key in ("target-http", "target-development"):
        # prepare 就地把每个根 key 重冻结为 loop.settings 实例（共享 loop
        # 只追加一次宿主规则），assert_current 的 describe 比对都成立
        assert root_bindings[key].settings is shared_loop.settings
        root_bindings[key].assert_current()


def test_prepare_refuses_process_profiles_without_sandbox_or_process_entry():
    key, binding = root_binding()
    for broken in (
        configuration(),
        configuration(**{
            "profiles": {
                "p": SubagentProfileConfiguration(
                    provider="process", process="missing", workspace="isolated_write",
                ),
            },
        }),
    ):
        try:
            prepare(
                _store(),
                broken,
                {"target-http": binding},
                SimpleNamespace(resolve=lambda run_id: None),
                main_provider=MinimalProvider(),
                main_model=None,
                models={},
                owned_providers=[],
                sandbox_service=None,
            )
        except Exception as error:
            assert getattr(error, "code", None) == "provider_not_configured"
        else:
            raise AssertionError("process profiles without sandbox must fail at assembly")


async def test_provider_passes_wrapped_documents_through_and_wraps_plain_text_once():
    key, binding = root_binding()
    prepared = prepare(
        _store(),
        configuration(),
        {"target-http": binding},
        SimpleNamespace(resolve=lambda run_id: None),
        main_provider=MinimalProvider(),
        main_model=None,
        models={},
        owned_providers=[],
        sandbox_service=fake_sandbox_service(),
    )
    provider = next(item for item in prepared.adapters if isinstance(item, ProcessProvider))
    assert isinstance(provider, ProcessProvider)
    descriptor = provider.descriptor
    runtime = FakeRuntime()
    provider.runtime = runtime

    assignment = WorkspaceAssignment(
        mode="isolated_write",
        agent_id="agent-1",
        source_project_id=PROJECT_ID,
        workspace_project_id=PROJECT_ID,
        directory=".krim/subagent-workspaces/agent-1",
        baseline_fingerprint="0" * 64,
        input_version=1,
    )

    async def prepare_input(_specification: StartTask) -> WorkspaceAssignment:
        return assignment

    provider._prepare_input = prepare_input  # type: ignore[attr-defined]

    # 域层已包装好的文档：原样透传，不重复包 JSON
    wrapped = build_task_document("fix the bug", assignment)
    await provider.start(_specification(wrapped, dict(descriptor)))
    delegated = runtime.delegated[-1]
    assert delegated.request.text == wrapped
    assert delegated.request.request_key == "ac-child.task-1"

    # 自然语言 + 钩子：包装一次，携带 assignment
    runtime.delegated.clear()
    await provider.start(_specification("fix the bug", dict(descriptor)))
    delegated = runtime.delegated[-1]
    assert delegated.request.text == wrapped
    assert parse_json(delegated.request.text)["workspace"]["directory"] == \
        ".krim/subagent-workspaces/agent-1"

    # 外来 target：严格匹配失败即拒
    try:
        await provider.start(_specification(wrapped, {"key": "subagent:target-http:other"}))
    except Exception as error:
        assert getattr(error, "code", None) == "target_binding_changed"
    else:
        raise AssertionError("foreign descriptor must be rejected")
    assert len(runtime.delegated) == 1  # 外来 target 未产生任何委托


async def test_provider_refuses_isolated_mode_without_assignment_and_shared_mismatch():
    key, binding = root_binding()
    prepared = prepare(
        _store(),
        configuration(),
        {"target-http": binding},
        SimpleNamespace(resolve=lambda run_id: None),
        main_provider=MinimalProvider(),
        main_model=None,
        models={},
        owned_providers=[],
        sandbox_service=fake_sandbox_service(),
    )
    provider = next(item for item in prepared.adapters if isinstance(item, ProcessProvider))
    assert isinstance(provider, ProcessProvider)
    provider.runtime = FakeRuntime()

    # isolated profile 且域准入没接 prepare_input：未带 assignment 的自然
    # 语言任务在登记 run 前被拒，不会产出无隔离声明的一次性运行。
    try:
        await provider.start(_specification("fix the bug", dict(provider.descriptor)))
    except Exception as error:
        assert getattr(error, "code", None) == "invalid_input"
    else:
        raise AssertionError("isolated profile without assignment must be refused")

    shared_claim = encoded({
        "task": "t",
        "workspace": WorkspaceAssignment(
            mode="shared_read_only",
            agent_id="agent-1",
            source_project_id=PROJECT_ID,
            workspace_project_id=PROJECT_ID,
            baseline_fingerprint="0" * 64,
            input_version=1,
        ).model_dump(mode="json"),
    })
    try:
        await provider.start(_specification(shared_claim, dict(provider.descriptor)))
    except Exception as error:
        assert getattr(error, "code", None) == "invalid_input"
    else:
        raise AssertionError("isolated profile must not accept a shared claim")
