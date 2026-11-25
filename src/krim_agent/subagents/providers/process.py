"""One-shot process provider over the existing sandbox executor (plan §5.1).

The provider declares a non-continuable, leaf-depth backend whose execution is
a host-preconfigured ``argv`` run inside the current sandbox (``SandboxCommand``:
``python``/``node`` only, at most 32 argv items and 8192 bytes, array passing
with no shell anywhere). The task document reaches the process as one single
argv element or as a controlled file under the managed sandbox namespace —
never interpolated into a shell.

Unsupported capabilities are rejected before creation. ``start`` registers the
child run with the RunService through ``start_delegate``; process runs are
never driven outside the service, so events, approvals, cancellation and the
durable parking flow keep working unchanged. The concrete
``ExecutorSandboxPort`` adapter drives the existing sandbox task/file tools
through the run's own ToolExecutor admission chain — policy, approvals and the
SandboxOperations scope stay in force; it adds no host shortcut and no second
container system. Cancellation goes through ``sandbox_task_cancel`` and kills
the real process group, not just a coroutine.

Current capability boundaries (documented, not worked around): background
sandbox commands have a fixed watchdog of ``wall_seconds`` (at most 60), so a
one-shot process is a bounded execution, not a resident worker; send, fork and
cold resume are refused with the subagent error codes.
"""

from __future__ import annotations

import asyncio
import base64
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast

from pydantic import JsonValue, ValidationError

from krim_agent.config import Settings
from krim_agent.context import builder, compaction
from krim_agent.contracts import (
    RunRequest,
    RunView,
    ServiceError,
    encoded,
    fingerprint,
    parse_json,
)
from krim_agent.runtime import model_step
from krim_agent.runtime.contracts import ExecutionBudget, RunStore, settled
from krim_agent.runtime.engine import AgentLoop
from krim_agent.runtime.sdk_execution import Parked, SDKExecutionPolicy
from krim_agent.sandbox.command import SandboxCommand
from krim_agent.subagents.errors import SubagentError
from krim_agent.subagents.providers.base import ProviderCapabilities
from krim_agent.subagents.types import StartTask
from krim_agent.subagents.workspace import (
    MANAGEMENT_ROOT,
    WorkspaceAssignment,
    WorkspaceBaseline,
    WorkspaceIsolation,
    WorkspaceLimits,
    WorkspaceMode,
    validate_assignment,
)
from krim_agent.tools.core import Invocation
from krim_agent.tools.executor import Admission, ToolContext, ToolExecutor

if TYPE_CHECKING:
    from krim_agent.runtime.service import RunService
    from krim_agent.sandbox.operations import SandboxOperations

PROCESS_PROGRAMS = frozenset({"python", "node"})
MAX_COMMAND_ITEMS = 32
MAX_COMMAND_BYTES = 8192
MAX_WALL_SECONDS = 60
TASK_INPUT_ROOT = f"{MANAGEMENT_ROOT}/subagent-tasks"
MAX_PATCH_EVENT_BYTES = 32_768
_HEX = frozenset("0123456789abcdef")

# ProcessAgentLoop 以宿主身份调用的内部沙箱工具集合。稳定内部 call id 形如
# ``process-<run_id>-<label>``（见 ``internal_process_call_id``），模型调用永远
# 不会携带这种 host call id。组合层的 session guard 用 ``is_internal_process_invocation``
# 识别这些调用后，仍按既有 manual/read 权限与 interactions 判定，不绕过权限；
# process 只运行就必须执行命令，不能套用 SDK 的只读工具过滤。
INTERNAL_PROCESS_TOOLS = frozenset({
    "sandbox_project",
    "sandbox_task_start",
    "sandbox_task_wait",
    "sandbox_task_log",
    "sandbox_task_cancel",
    "sandbox_read",
    "sandbox_list",
    "sandbox_write",
    "sandbox_mkdir",
    "sandbox_remove",
})


def internal_process_call_id(run_id: str, label: str) -> str:
    """Stable host call id shared by the adapter and the guard predicate."""
    return f"process-{run_id}-{label}"


def is_internal_process_invocation(invocation: Invocation) -> bool:
    """True when the invocation has the shape ProcessAgentLoop generates.

    Checks the stable host call id (bound to its own run id) and membership in
    ``INTERNAL_PROCESS_TOOLS``. Main composition combines this with its own
    "registered run / no model provider" registry check; the predicate alone
    grants nothing to model-issued calls, which carry provider call ids.
    """
    prefix = f"process-{invocation.run_id}-"
    return (
        isinstance(invocation.name, str)
        and invocation.name in INTERNAL_PROCESS_TOOLS
        and isinstance(invocation.call_id, str)
        and invocation.call_id.startswith(prefix)
        and len(invocation.call_id) > len(prefix)
    )

# 沙箱内 bootstrap（进程 provider 的实际副本初始化；argv 数组传递，无 shell）：
# 优先受控 git worktree（--detach，主 checkout 不可被切分支），失败退回有界
# 拷贝；两种模式都把用户未提交改动复制进去（含二进制），并回报 JSON 清单。
PREPARE_WORKSPACE_SCRIPT = """\
import json, os, shutil, subprocess, sys
target = sys.argv[1]
limits = json.loads(sys.argv[2])
max_files = int(limits.get("max_files", 400))
max_file_bytes = int(limits.get("max_file_bytes", 1048576))
skip = {".git", ".krim", "node_modules", ".venv"}
head = None
try:
    raw = open(".git/HEAD").read().strip()
    if raw.startswith("ref:"):
        raw = open(".git/" + raw[4:].strip()).read().strip()
    if len(raw) == 40 and all(c in "0123456789abcdef" for c in raw):
        head = raw
except OSError:
    head = None
mode = "copy"
marker = target + ".prepared.json"
if os.path.lexists(target):
    if os.path.islink(target) or not os.path.isdir(target) or os.path.islink(marker):
        raise ValueError("invalid workspace directory")
    if not os.path.exists(marker) and isinstance(limits.get("prior_prepared"), dict):
        with open(marker, "x") as destination:
            json.dump({**limits["prior_prepared"], "directory": target, "version": 1}, destination)
    with open(marker) as source:
        prepared = json.load(source)
    if (prepared.get("directory") != target or prepared.get("version") != 1
            or prepared.get("mode") not in {"copy", "worktree"}
            or prepared.get("truncated") is not False):
        raise ValueError("workspace preparation not verified")
    print(json.dumps({**prepared, "copied": 0, "reused": True}))
    sys.exit(0)
if head:
    done = subprocess.run(
        ["git", "worktree", "add", "--detach", target, head],
        capture_output=True, text=True, timeout=40,
    )
    if done.returncode == 0:
        mode = "worktree"
if mode == "copy":
    os.makedirs(target, exist_ok=True)
copied = 0
skipped = 0
truncated = False
for root, dirs, files in os.walk("."):
    dirs[:] = [d for d in dirs if d not in skip]
    for name in files:
        src = os.path.join(root, name)
        rel = os.path.relpath(src, ".")
        if copied >= max_files:
            truncated = True
            break
        dst = os.path.join(target, rel)
        try:
            if os.path.getsize(src) > max_file_bytes:
                skipped += 1
                continue
            parent = os.path.dirname(dst)
            if parent:
                os.makedirs(parent, exist_ok=True)
            shutil.copyfile(src, dst)
            copied += 1
        except OSError:
            skipped += 1
    if truncated:
        break
manifest = {
    "mode": mode, "head": head, "copied": copied,
    "skipped": skipped, "truncated": truncated,
}
if not truncated and skipped == 0:
    with open(marker + ".tmp", "x") as destination:
        json.dump({**manifest, "directory": target, "version": 1}, destination)
    os.replace(marker + ".tmp", marker)
print(json.dumps(manifest))
"""

# 工作目录约束：worker 进程以隔离目录为 cwd 启动（plan §9.2）。包装脚本
# 与目标目录一起作为 argv 元素传入沙箱，绝不经过 shell。cwd 约束不是安全
# 边界——脚本仍可主动写 ../ 路径；真正的边界仍是沙箱文件权限与审批。
PYTHON_CWD_WRAPPER = (
    "import os,sys\n"
    "os.chdir(sys.argv[1])\n"
    "os.execv(sys.executable, [sys.executable, '-I', *sys.argv[2:]])\n"
)
NODE_CWD_WRAPPER = (
    "const cp=require('node:child_process');\n"
    "process.chdir(process.argv[1]);\n"
    "const done=cp.spawnSync(process.execPath,process.argv.slice(2),"
    "{stdio:'inherit',shell:false});\n"
    "process.exit(typeof done.status==='number'?done.status:1);\n"
)


@dataclass(frozen=True)
class ProcessLaunch:
    """A host-validated launch plan: program, fixed payload, bounded wall.

    One argv slot is reserved for the task document (json or controlled file
    path), which is why the payload is capped two items below the sandbox's
    own argv limit.
    """

    program: str
    payload: tuple[str, ...]
    wall_seconds: int = 20


def validate_process_launch(argv: Any, *, wall_seconds: int = 20) -> ProcessLaunch:
    """Validate a configured ``argv`` against the actual SandboxCommand
    capability and reject before creation (plan §5.1); never guess."""
    if (
        not isinstance(argv, (tuple, list))
        or not argv
        or any(not isinstance(item, str) or not item for item in argv)
    ):
        raise ValueError("process argv must be a non-empty sequence of strings")
    program, payload = argv[0], tuple(argv[1:])
    if program not in PROCESS_PROGRAMS:
        raise ValueError(f"process program must be one of {sorted(PROCESS_PROGRAMS)}")
    if not payload:
        raise ValueError("process payload must carry at least one entry argument")
    if len(payload) > MAX_COMMAND_ITEMS - 2:
        raise ValueError("process payload leaves no argv room for the task document")
    if any("\0" in item for item in payload):
        raise ValueError("process payload must not contain NUL bytes")
    if sum(len(item.encode("utf-8")) for item in payload) + 2 > MAX_COMMAND_BYTES:
        raise ValueError("process payload leaves no byte room for the task document")
    if type(wall_seconds) is not int or not 1 <= wall_seconds <= MAX_WALL_SECONDS:
        raise ValueError(f"wall_seconds must be within 1..{MAX_WALL_SECONDS}")
    if program == "node":
        entry = payload[0]
        if entry in {"-e", "--eval"}:
            if len(payload) < 2:
                raise ValueError("node eval must be followed by a script argument")
        elif (
            entry.startswith(("-", "/"))
            or "\\" in entry
            or any(segment == ".." for segment in entry.split("/"))
        ):
            raise ValueError("node entry must be eval or an in-project script path")
    try:
        # The probe is the authoritative check against the real model, not a
        # copy of its limits.
        SandboxCommand(
            program=cast(Literal["python", "node"], program),
            argv=[*payload, "task", "task-input.json"],
            expected_generation=0,
            wall_seconds=wall_seconds,
        )
    except ValueError as error:
        raise ValueError(f"process argv does not fit SandboxCommand: {error}") from None
    return ProcessLaunch(program=program, payload=payload, wall_seconds=wall_seconds)


def parse_task_document(text: str) -> tuple[dict[str, JsonValue], WorkspaceAssignment | None]:
    """Split the delegated input into the delivered document and its
    validated workspace claim; malformed documents are rejected up front."""
    try:
        document = parse_json(text)
    except ValueError:
        raise ServiceError("invalid_input", 422) from None
    if not isinstance(document, dict) or not isinstance(document.get("task"), str):
        raise ServiceError("invalid_input", 422)
    raw = document.get("workspace")
    assignment: WorkspaceAssignment | None = None
    if raw is not None:
        try:
            assignment = WorkspaceAssignment.model_validate(raw)
        except (ValidationError, ValueError):
            raise ServiceError("invalid_input", 422) from None
    return document, assignment


def build_task_document(
    task: str, assignment: WorkspaceAssignment | None = None
) -> str:
    """Wrap the natural-language spawn payload into the delivered document.

    The domain layer builds ``StartTask.request.text`` with this function; the
    process receives the same JSON as one argv element or as the controlled
    task-input file. ``task`` must be non-empty; the workspace claim, when
    present, is validated here so malformed assignments never reach a run.
    """
    document: dict[str, JsonValue] = {"task": task}
    if assignment is not None:
        if assignment.mode == "shared_read_only" and assignment.directory is not None:
            raise ServiceError("invalid_input", 422)
        if assignment.mode == "isolated_write" and not assignment.directory:
            raise ServiceError("invalid_input", 422)
        document["workspace"] = cast(
            dict[str, JsonValue], assignment.model_dump(mode="json")
        )
    return encoded(document)


def validate_task_document(
    text: str, *, workspace_mode: WorkspaceMode
) -> tuple[dict[str, JsonValue], WorkspaceAssignment | None]:
    document, assignment = parse_task_document(text)
    validate_assignment(assignment, expected_mode=workspace_mode)
    return document, assignment


class SandboxExecutionPort(Protocol):
    """The authorized sandbox surface one process activation runs against.

    The host injects it; the concrete adapter (``ExecutorSandboxPort``) maps
    every method onto the existing sandbox tool API — background tasks via
    ``sandbox_task_start/wait/cancel/log`` with their persistent receipts, and
    file operations via the sandbox file tools — executed through the run's
    own ToolExecutor so approvals and the workspace scope stay authoritative.
    An ``ask`` decision surfaces as ``SandboxApprovalRequired`` with the
    pending, not-yet-executed invocation.
    """

    def tool_hash(self, tool: str) -> str: ...

    async def project_state(self) -> dict[str, Any]: ...

    async def start(self, command: SandboxCommand, *, label: str = "task-start") -> dict[
        str, Any
    ]: ...

    async def wait(
        self, project_id: str, operation_id: str, *, seconds: int = 5
    ) -> dict[str, Any]: ...

    async def cancel(
        self, project_id: str, operation_id: str, *, expected_generation: int
    ) -> dict[str, Any]: ...

    async def logs(
        self, project_id: str, operation_id: str, *, max_bytes: int = 8192
    ) -> dict[str, Any]: ...

    async def read(
        self, project_id: str, path: str, *, offset: int = 0, limit: int = 16384
    ) -> dict[str, Any]: ...

    async def entries(self, project_id: str, path: str, *, offset: int = 0) -> dict[str, Any]: ...

    async def ensure_directory(
        self, project_id: str, path: str, *, label: str | None = None
    ) -> dict[str, Any]: ...

    async def make_directory(self, project_id: str, path: str) -> dict[str, Any]: ...

    async def write(
        self,
        project_id: str,
        path: str,
        content: str,
        *,
        expected_sha256: str | None = None,
        label: str | None = None,
    ) -> dict[str, Any]: ...

    async def remove(
        self, project_id: str, path: str, *, expected_sha256: str, label: str | None = None
    ) -> dict[str, Any]: ...

    async def git_head(self, project_id: str) -> str | None: ...

    async def project_generation(self) -> int: ...

    async def prepare_workspace(
        self, project_id: str, directory: str, *, limits: dict[str, int]
    ) -> dict[str, Any]: ...


class SandboxApprovalRequired(Exception):
    """A sandbox write decision needs the durable approval pipeline.

    ``invocation`` identifies the pending call, which has NOT executed; the
    loop parks the run with the standard checkpoint + Parked flow and the
    existing ``reply_approval`` path resumes it.
    """

    def __init__(self, invocation: Invocation, admission: Admission) -> None:
        self.invocation = invocation
        self.admission = admission
        super().__init__(invocation.call_id)


class ExecutorSandboxPort:
    """Concrete ``SandboxExecutionPort`` over the run's existing sandbox tools.

    Every call is one admission + execution of a registered sandbox tool
    through the provided ``ToolExecutor``/``ToolContext`` — exactly the path
    any model-issued sandbox call takes, including ``ask`` parking decisions.
    Read limits follow the tool models (``sandbox_read`` pages at 16 KiB,
    ``sandbox_list`` at 100 entries); mutations track the project generation
    from receipts so per-file CAS preconditions stay exact, and every
    mutation call id is unique per invocation (their durable operation keys
    fingerprint the call id).
    """

    def __init__(
        self,
        executor: ToolExecutor,
        context: ToolContext,
        run_id: str,
        *,
        approved: Mapping[str, bool] | None = None,
    ) -> None:
        self._executor = executor
        self._context = context
        self._run_id = run_id
        self._approved = dict(approved) if approved else {}
        self._steps: dict[str, int] = {}
        self._calls = 0
        self._generation: dict[str, int] = {}

    def tool_hash(self, tool: str) -> str:
        return self._executor.registry.get(tool).fingerprint

    def _step(self, label: str) -> int:
        if label not in self._steps:
            self._steps[label] = len(self._steps)
        return self._steps[label]

    async def invoke(
        self, label: str, tool: str, arguments: dict[str, JsonValue]
    ) -> dict[str, Any]:
        spec = self._executor.registry.get(tool)
        step = self._step(label)
        call_id = internal_process_call_id(self._run_id, label)
        invocation = Invocation(
            run_id=self._run_id,
            call_id=call_id,
            span_id=f"process-{call_id}",
            step_index=step,
            name=tool,
            arguments_json=encoded(arguments),
            tool_hash=spec.fingerprint,
        )
        admission = await self._executor.admit(invocation, self._context, timeout_seconds=None)
        if admission.decision == "ask" and not admission.approved:
            decision = self._approved.get(call_id)
            if decision is not True:
                if decision is False:
                    raise ServiceError("approval_denied")
                raise SandboxApprovalRequired(invocation, admission)
            admission = replace(admission, approved=True)
        observation = await self._executor.execute(invocation, self._context, admission)
        if observation.status != "ok":
            raise ServiceError(observation.code or "tool_error")
        value = observation.value
        return cast(dict[str, Any], value if isinstance(value, dict) else {})

    def _mutation_label(self, prefix: str, label: str | None) -> str:
        if label is not None:
            return label
        self._calls += 1
        return f"{prefix}-{self._calls}"

    async def project_state(self) -> dict[str, Any]:
        return await self.invoke("project-state", "sandbox_project", {})

    async def project_generation(self) -> int:
        state = await self.project_state()
        project = state.get("project")
        generation = project.get("generation") if isinstance(project, dict) else None
        if type(generation) is not int or generation < 0:
            raise ServiceError("workspace_contract_required")
        project_id = project.get("project_id") if isinstance(project, dict) else None
        if isinstance(project_id, str):
            self._generation.setdefault(project_id, generation)
        return generation

    async def _generation_of(self, project_id: str) -> int:
        known = self._generation.get(project_id)
        if known is not None:
            return known
        return await self.project_generation()

    def _bump(self, project_id: str, receipt: dict[str, Any]) -> None:
        value = receipt.get("generation")
        if type(value) is int:
            self._generation[project_id] = value

    async def start(
        self, command: SandboxCommand, *, label: str = "task-start"
    ) -> dict[str, Any]:
        if command.project_id is not None:
            self._generation.setdefault(command.project_id, command.expected_generation)
        return await self.invoke(label, "sandbox_task_start", command.model_dump(mode="json"))

    async def wait(
        self, project_id: str, operation_id: str, *, seconds: int = 5
    ) -> dict[str, Any]:
        self._calls += 1
        return await self.invoke(f"task-wait-{self._calls}", "sandbox_task_wait", {
            "project_id": project_id,
            "operation_id": operation_id,
            "seconds": max(1, min(10, seconds)),
        })

    async def cancel(
        self, project_id: str, operation_id: str, *, expected_generation: int
    ) -> dict[str, Any]:
        return await self.invoke("task-cancel", "sandbox_task_cancel", {
            "project_id": project_id,
            "operation_id": operation_id,
            "expected_generation": expected_generation,
        })

    async def logs(
        self, project_id: str, operation_id: str, *, max_bytes: int = 8192
    ) -> dict[str, Any]:
        self._calls += 1
        return await self.invoke(f"task-log-{self._calls}", "sandbox_task_log", {
            "project_id": project_id,
            "operation_id": operation_id,
            "max_bytes": max(1, min(8192, max_bytes)),
        })

    async def read(
        self, project_id: str, path: str, *, offset: int = 0, limit: int = 16384
    ) -> dict[str, Any]:
        self._calls += 1
        return await self.invoke(f"read-{self._calls}", "sandbox_read", {
            "project_id": project_id,
            "path": path,
            "offset": offset,
            "limit": max(1, min(16384, limit)),
        })

    async def entries(self, project_id: str, path: str, *, offset: int = 0) -> dict[str, Any]:
        self._calls += 1
        return await self.invoke(f"list-{self._calls}", "sandbox_list", {
            "project_id": project_id,
            "path": path,
            "offset": offset,
            "limit": 100,
        })

    async def ensure_directory(
        self, project_id: str, path: str, *, label: str | None = None
    ) -> dict[str, Any]:
        receipt = await self.invoke(
            self._mutation_label("mkdir", label), "sandbox_mkdir", {
                "project_id": project_id,
                "path": path,
                "expected_generation": await self._generation_of(project_id),
            },
        )
        self._bump(project_id, receipt)
        return receipt

    async def make_directory(self, project_id: str, path: str) -> dict[str, Any]:
        """``WorkspacePort.make_directory`` 形状；无标签（整合/清理路径）。"""
        return await self.ensure_directory(project_id, path)

    async def write(
        self,
        project_id: str,
        path: str,
        content: str,
        *,
        expected_sha256: str | None = None,
        label: str | None = None,
    ) -> dict[str, Any]:
        receipt = await self.invoke(
            self._mutation_label("write", label), "sandbox_write", {
                "project_id": project_id,
                "path": path,
                "expected_generation": await self._generation_of(project_id),
                "expected_sha256": expected_sha256,
                "content": content,
            },
        )
        self._bump(project_id, receipt)
        return receipt

    async def remove(
        self, project_id: str, path: str, *, expected_sha256: str, label: str | None = None
    ) -> dict[str, Any]:
        receipt = await self.invoke(
            self._mutation_label("remove", label), "sandbox_remove", {
                "project_id": project_id,
                "path": path,
                "expected_generation": await self._generation_of(project_id),
                "expected_sha256": expected_sha256,
            },
        )
        self._bump(project_id, receipt)
        return receipt

    async def git_head(self, project_id: str) -> str | None:
        """Best-effort HEAD read. The sandbox file surface deliberately blocks
        ``.git``, so through this port the answer is normally ``None``; the
        working-tree manifest remains the effective baseline either way."""
        try:
            head = await self.read(project_id, ".git/HEAD", limit=256)
        except ServiceError:
            return None
        try:
            raw = base64.b64decode(str(head.get("data", "")), validate=True).decode("utf-8").strip()
        except (ValueError, UnicodeDecodeError):
            return None
        if len(raw) == 40 and all(character in _HEX for character in raw):
            return raw
        if raw.startswith("ref:"):
            reference = raw[4:].strip()
            if not reference or reference.startswith("/") or ".." in reference.split("/"):
                return None
            try:
                ref = await self.read(project_id, f".git/{reference}", limit=256)
                commit = base64.b64decode(str(ref.get("data", "")), validate=True)
                commit = commit.decode("utf-8").strip()
            except (ServiceError, ValueError, UnicodeDecodeError):
                return None
            if len(commit) == 40 and all(character in _HEX for character in commit):
                return commit
        return None

    async def prepare_workspace(
        self, project_id: str, directory: str, *, limits: dict[str, int]
    ) -> dict[str, Any]:
        """Materialize the isolated copy with one approved in-sandbox command.

        The bootstrap runs inside the container (it may read ``.git`` there —
        the control-surface prohibition does not bind in-container processes):
        git worktree first (detached, so the main checkout cannot be switched),
        plain bounded copy as fallback, with the user's uncommitted changes
        copied on top in both modes. It reports its manifest as JSON on
        stdout; hard infrastructure failures raise, an in-container fallback
        is reported in the result instead of being silent.
        """
        generation = await self._generation_of(project_id)
        try:
            command = SandboxCommand(
                project_id=project_id,
                expected_generation=generation,
                program="python",
                argv=["-c", PREPARE_WORKSPACE_SCRIPT, directory, encoded(limits)],
                wall_seconds=50,
            )
        except ValueError:
            raise ServiceError("invalid_input", 422) from None
        receipt = await self.invoke("workspace-prepare", "sandbox_task_start",
                                    command.model_dump(mode="json"))
        operation_id = str(receipt.get("operation_id", ""))
        if not operation_id:
            raise ServiceError("facts_corrupt")
        while True:
            state = await self.wait(project_id, operation_id, seconds=5)
            if state.get("status") in {"finished", "unknown"}:
                break
        logs = await self.logs(project_id, operation_id, max_bytes=8192)
        try:
            stdout = base64.b64decode(str(logs.get("stdout", "")), validate=True).decode(
                "utf-8", "replace"
            )
        except ValueError:
            stdout = ""
        result: dict[str, Any] | None = None
        for line in reversed(stdout.splitlines()):
            candidate = line.strip()
            if not candidate.startswith("{"):
                continue
            try:
                parsed = parse_json(candidate)
            except ValueError:
                continue
            if isinstance(parsed, dict) and parsed.get("mode") in {"worktree", "copy"}:
                result = cast(dict[str, Any], parsed)
                break
        if state.get("status") == "unknown":
            raise ServiceError("sandbox_cleanup_required")
        if state.get("exit_code") != 0 or result is None:
            raise ServiceError("execution_error")
        return {
            **result,
            "operation_id": operation_id,
            "exit_code": state.get("exit_code"),
        }


class ProcessProvider:
    """Route to a pre-registered ``ProcessAgentLoop`` binding (sdk.py 契约).

    ``descriptor`` is the complete frozen descriptor of the ``RunBinding``
    composition froze for ``subagent:<profile>`` — not a summary of it — so
    ``start_delegate``'s fingerprint checks stay exact. ``runtime`` may be
    ``None`` at construction (before ``PreparedSubagents.attach``); the host
    assigns ``provider.runtime`` afterwards, mirroring ``SdkSessionProvider``.
    One instance per configured process profile; removing the registry entry
    only blocks new starts — accepted runs keep their binding.
    """

    def __init__(
        self,
        runtime: RunService | None,
        descriptor: dict[str, JsonValue],
        *,
        launch: ProcessLaunch,
        workspace_mode: WorkspaceMode = "shared_read_only",
        prepare_input: Callable[[StartTask], Awaitable[WorkspaceAssignment | None]] | None = None,
    ) -> None:
        if not isinstance(descriptor.get("key"), str) or "context" in descriptor:
            raise ValueError("process provider requires a frozen binding descriptor")
        if not isinstance(launch, ProcessLaunch):
            raise ValueError("process provider requires a validated ProcessLaunch")
        self.runtime = runtime
        self.launch = launch
        self.workspace_mode: WorkspaceMode = workspace_mode
        self._prepare_input = prepare_input
        self.capabilities = ProviderCapabilities(
            continuable=False,
            fork=False,
            structured_output=False,
            images=False,
            workspace_isolation=workspace_mode == "isolated_write",
            depth_policy="leaf",
        )
        self._descriptor_json = encoded(descriptor)
        self.version = fingerprint(descriptor)

    @property
    def descriptor(self) -> dict[str, JsonValue]:
        # Do not publish a mutable reference to the provider's frozen binding.
        return cast(dict[str, JsonValue], parse_json(self._descriptor_json))

    async def start(self, specification: StartTask) -> RunView:
        if encoded(specification.target) != self._descriptor_json:
            raise ServiceError("target_binding_changed")
        if self.workspace_mode != "isolated_write":
            # 现有沙箱命令没有只读挂载：shared_read_only process 会在同一
            # 可写项目上运行 shell，等于是可写主工作区。提前拒绝，不伪装
            # read-only；正式可用的 process profile 必须 isolated_write。
            raise SubagentError(
                "subagent_capability",
                accepted=False,
                action="Configure the process profile with workspace=isolated_write.",
            )
        if self.runtime is None:
            raise ServiceError("closed", 503)
        # 域层 _start 传来的 request.text 是自然语言任务（非 JSON 文档）：
        # provider 在此包装成投递文档。workspace assignment 由域准入经
        # prepare_input 钩子准备（主会话把它接到 WorkspaceIsolation.assign
        # 的结果上）；已包装过的文档（域 payload['text']）逐字节透传，
        # 绝不重复包 JSON。
        original = specification.request.text
        parsed_assignment: WorkspaceAssignment | None
        try:
            _document, parsed_assignment = parse_task_document(original)
        except ServiceError:
            parsed_assignment = None
            parsed = False
        else:
            parsed = True
        if parsed:
            validate_assignment(parsed_assignment, expected_mode=self.workspace_mode)
            text = original
        else:
            assignment: WorkspaceAssignment | None = None
            if self._prepare_input is not None:
                assignment = await self._prepare_input(specification)
            validate_assignment(assignment, expected_mode=self.workspace_mode)
            document: dict[str, JsonValue] = {"task": original}
            if assignment is not None:
                document["workspace"] = cast(
                    dict[str, JsonValue], assignment.model_dump(mode="json")
                )
            text = encoded(document)
        request = RunRequest(
            text=text,
            request_key=specification.request.request_key,
            target=specification.request.target,
        )
        return await self.runtime.start_delegate(
            cast(StartTask, specification.model_copy(update={"request": request}))
        )

    async def interrupt(self, run_id: str) -> Any:
        # The domain service owns the tree/task checks before cancelling; the
        # run's own finalization plus ``cancel_run_tasks`` kill the process.
        if self.runtime is None:
            raise ServiceError("closed", 503)
        return await self.runtime.cancel_delegate(run_id)

    async def dispose(self, activation_id: str) -> None:
        if not activation_id:
            raise ValueError("missing activation identity")

    def refusal(self, action: Literal["send", "fork", "resume"]) -> SubagentError:
        """The typed one-shot refusal for the domain service to raise."""
        codes: dict[str, str] = {
            "send": "subagent_not_continuable",
            "fork": "subagent_capability",
            "resume": "subagent_not_recoverable",
        }
        return SubagentError(codes[action], accepted=False, action="reject")


class ProcessBackend:
    """Marker provider for one-shot process runs; no model sampling occurs."""

    behavior_version = "process/1"

    async def stream(self, messages: Any, tools: Any) -> AsyncIterator[Any]:
        raise ServiceError("execution_error")
        yield  # pragma: no cover - one-shot activations never sample

    async def close(self) -> None:
        return None


def _delivery_path(assignment: WorkspaceAssignment | None) -> str | None:
    if assignment is not None and assignment.mode == "isolated_write":
        return f"{TASK_INPUT_ROOT}/{assignment.agent_id}/task-input.json"
    return None


def _scoped_argv(
    program: str, payload: tuple[str, ...], last: str, directory: str | None
) -> tuple[str, ...]:
    """Worker argv with the isolated directory as its working directory.

    The wrapper and directory ride along as plain argv elements (never a
    shell). This constrains where the worker starts, not what it may touch —
    that boundary stays with the sandbox and the approval pipeline.
    """
    if directory is None:
        return (*payload, last)
    if program == "python":
        return ("-c", PYTHON_CWD_WRAPPER, directory, *payload, last)
    return ("-e", NODE_CWD_WRAPPER, directory, *payload, last)


def _cancel_confirmed(outcome: Any) -> bool:
    """Cancel is clean only on a confirmed finished state.

    ``unknown`` (or any non-finished state) means the worker may still be
    running and the slot must be retained as cleanup debt. When the port
    returns an actual receipt, its ``cleanup`` value is authoritative.
    """
    if not isinstance(outcome, dict) or outcome.get("status") != "finished":
        return False
    receipt = outcome.get("receipt")
    if isinstance(receipt, dict) and "cleanup" in receipt:
        return receipt.get("cleanup") == "clean"
    return True


class ProcessAgentLoop(AgentLoop):
    """Run a host-preconfigured one-shot process as a registered production run.

    The loop performs no model sampling. It drives the sandbox task tools
    through the run's ToolExecutor, waits for the broker receipt, separates
    result (``process_finished`` with exit code and output) from cleanup
    failure (``process_cleanup_failed``), and cancels the real process group
    through ``sandbox_task_cancel`` on the way out. An ``ask`` decision parks
    the run with the durable approval checkpoint that ``RunService._drive``
    already resumes, so the existing run/approval mechanism is reused as is.
    """

    def __init__(
        self,
        store: RunStore,
        provider: Any,
        executor: ToolExecutor,
        settings: Settings,
        *,
        sandbox: SandboxOperations | None,
        launch: ProcessLaunch,
        workspace_mode: WorkspaceMode = "shared_read_only",
        workspace_limits: WorkspaceLimits | None = None,
        port: Callable[..., SandboxExecutionPort] | None = None,
        process_slot: Callable[[str], AbstractAsyncContextManager[None]] | None = None,
        baseline_source: Callable[[str], Awaitable[WorkspaceBaseline | None]] | None = None,
    ) -> None:
        super().__init__(store, provider, executor, settings, sandbox=sandbox)
        if sandbox is None:
            raise ServiceError("workspace_contract_required")
        if not isinstance(launch, ProcessLaunch):
            raise ValueError("process loop requires a validated ProcessLaunch")
        self.launch = launch
        self.workspace_mode: WorkspaceMode = workspace_mode
        self._workspace_limits = workspace_limits or WorkspaceLimits()
        self._port_source = port
        # 工具进程并发槽（domain.process_slot，RequestLimiter 之上的
        # asyncctx）：实际 TaskStart 到确认结束/真实清理之间持有；审批
        # 停靠发生在 start 的 admit 内，槽随 async with 释放，不占额度。
        self._process_slot = process_slot
        # 补丁收集所需基线的来源（domain task.input['baseline']）；运行期
        # 也可解析 self.subagents 上的同名方法。缺失时只发诊断事件，不造补丁。
        self._baseline_source = baseline_source

    def _build_port(
        self,
        executor: ToolExecutor,
        context: ToolContext,
        run_id: str,
        approved: Mapping[str, bool],
    ) -> SandboxExecutionPort:
        if self._port_source is not None:
            return self._port_source(executor, context, run_id, approved)
        return ExecutorSandboxPort(executor, context, run_id, approved=approved)

    async def _retain_process_debt(
        self,
        run_id: str,
        operation_id: str,
        lease: Any,
        slot: AbstractAsyncContextManager[None] | None,
    ) -> bool:
        """清理债登记：优先 ProcessLease.retain_debt（持久记录+释放置为
        no-op），否则 domain.remember_process_debt 保留 CM 引用供 reconcile
        确认后 close。都没有（域未接线）返回 False——调用方按现状释放，
        但 run 终态仍是未确认失败，不掩盖清理债。"""
        retain = getattr(lease, "retain_debt", None)
        if retain is not None:
            await retain(run_id, operation_id)
            return True
        remember = getattr(getattr(self, "subagents", None), "remember_process_debt", None)
        if remember is not None and slot is not None:
            await remember(run_id, operation_id, slot)
            return True
        return False

    async def _collect_workspace_patch(
        self,
        run_id: str,
        assignment: WorkspaceAssignment,
        port: SandboxExecutionPort,
        emit: Callable[[str, dict[str, JsonValue]], Awaitable[Any]],
        check: Callable[[], None],
    ) -> None:
        """收集隔离副本相对基线的补丁，作为独立的 workspace_patch 事件。

        只读产出，不整合主目录（root merge 工具另接）。基线由 domain
        经 baseline_source 提供（task.input['baseline']）；缺失或收集失败
        如实发 workspace_patch_unavailable 诊断事件，不造补丁、不掩盖。
        补丁文本超过事件预算时只保留指纹与变更元数据（diffs_included=false），
        完整补丁可由 root 用同一 WorkspaceIsolation.collect 路径按指纹重取。
        """
        baseline_source = self._baseline_source or getattr(
            getattr(self, "subagents", None), "workspace_baseline", None,
        )
        if baseline_source is None:
            # 未接线（domain 尚未提供 task.input['baseline']）：不造假补丁、
            # 也不刷诊断事件；domain.on_settled 只会拿到既有流程事件。
            return
        baseline: WorkspaceBaseline | None = None
        try:
            baseline = await baseline_source(run_id)
        except ServiceError as error:
            await emit("workspace_patch_unavailable", {
                "directory": assignment.directory, "code": error.code,
            })
            return
        if baseline is None:
            await emit("workspace_patch_unavailable", {
                "directory": assignment.directory, "code": "baseline_missing",
            })
            return
        try:
            patch = await WorkspaceIsolation(
                cast(Any, port), limits=self._workspace_limits,
            ).collect(assignment, baseline)
        except ServiceError as error:
            await emit("workspace_patch_unavailable", {
                "directory": assignment.directory, "code": error.code,
            })
            return
        check()
        changes: list[JsonValue] = [
            {
                "path": change.path,
                "kind": change.kind,
                "base_sha256": change.base_sha256,
                "result_sha256": change.result_sha256,
                "size": change.size,
                "binary": change.binary,
                "diff_truncated": change.diff_truncated,
                **({"diff": change.diff} if change.diff is not None else {}),
            }
            for change in patch.changes
        ]
        diffs_included = True
        if len(encoded(changes).encode("utf-8")) > MAX_PATCH_EVENT_BYTES:
            # 大输出改指纹引用：完整补丁留在隔离目录，按 patch_fingerprint
            # 由同一 collect 路径重取。
            diffs_included = False
            changes = [
                {key: value for key, value in cast(dict[str, JsonValue], change).items()
                 if key != "diff"}
                for change in changes
            ]
        await emit("workspace_patch", {
            "patch_fingerprint": patch.fingerprint,
            "baseline_fingerprint": patch.baseline_fingerprint,
            "source_project_id": patch.source_project_id,
            "directory": patch.directory,
            "mode": patch.mode,
            "input_version": patch.input_version,
            "truncated": patch.truncated,
            "diffs_included": diffs_included,
            "changes": cast(JsonValue, changes),
        })

    async def run(
        self,
        run_id: str,
        approval_request: Any,
        assert_target_current: Callable[[], None] = lambda: None,
        execution_budget: ExecutionBudget | None = None,
        resume: Any = None,
        executor: ToolExecutor | None = None,
        run_tools: Any | None = None,
        session_scope: Any | None = None,
        startup_gate: Callable[[], asyncio.Lock | None] | None = None,
        stream_check: Callable[[], None] | None = None,
    ) -> None:
        # Process workers do not consume model text deltas. Accept the shared
        # run interface while retaining complete checks around their effects.
        def check() -> None:
            assert_target_current()
            if self._d3_seal != (
                compaction.binding_token(), model_step.binding_token(), builder.binding_token()
            ):
                raise ServiceError("target_binding_changed")

        check()
        snapshot = await self.store.snapshot(run_id)
        check()
        if self.workspace_mode != "isolated_write":
            # 无只读挂载就没有安全的 shared_read_only shell：运行前拒绝，
            # 不把可写主工作区当成隔离副本（与 provider.start 的 preflight 一致）。
            raise ServiceError("subagent_capability")
        document, assignment = parse_task_document(snapshot.request.text)
        if assignment is None or not assignment.directory:
            # 隔离 profile 收不到 workspace claim：域准入缺口，运行前拒绝，
            # 不把主工作区当成隔离副本。
            raise ServiceError("subagent_capability")
        validate_assignment(assignment, expected_mode=self.workspace_mode)
        completed: frozenset[str] = frozenset()
        replies: Mapping[str, bool] = {}
        if resume is not None:
            checkpoint = resume.checkpoint
            state = checkpoint.get("process") if isinstance(checkpoint, dict) else None
            if (
                fingerprint(checkpoint) != resume.fingerprint
                or checkpoint.get("format") != "sdk-host-v2"
                or checkpoint.get("run_id") != run_id
                or not isinstance(state, dict)
            ):
                raise ServiceError("facts_corrupt")
            completed = frozenset(str(item) for item in state.get("completed", ()))
            replies = resume.replies
        if execution_budget is None:
            timeout = self.settings.run_timeout
            deadline = (
                None if timeout is None
                else asyncio.get_running_loop().time()
                + max(0, snapshot.run.created_at + timeout - time.time())
            )
            execution_budget = ExecutionBudget(deadline)
        assert self.sandbox is not None
        scope = await self.sandbox.scope(run_id, check, execution_budget)
        policy = SDKExecutionPolicy.from_settings(self.settings)

        async def emit(kind: str, data: dict[str, JsonValue]) -> Any:
            event = await self.store.append(run_id, kind, data)
            check()
            return event

        async def reject(_invocation: Any, _timeout: Any) -> bool:
            raise ServiceError("approval_unavailable")

        active_executor = executor or self.executor
        context = ToolContext(
            emit, reject, policy.tool_timeout, check, scope, policy.approval_timeout,
        )
        port = self._build_port(active_executor, context, run_id, replies)
        project_id = scope.project_id
        if not project_id:
            raise ServiceError("workspace_contract_required")
        task_json = encoded(document)
        directory = assignment.directory if assignment is not None else None
        file_path: str | None = None
        scoped = _scoped_argv(self.launch.program, self.launch.payload, task_json, directory)
        if (
            len(scoped) > MAX_COMMAND_ITEMS
            or sum(len(item.encode("utf-8")) for item in scoped) > MAX_COMMAND_BYTES
        ):
            file_path = _delivery_path(assignment)
            if file_path is None:
                raise ServiceError("subagent_capability")
            scoped = _scoped_argv(
                self.launch.program, self.launch.payload, file_path, directory,
            )
        operation_id: str | None = None
        finished = False
        generation = await port.project_generation()
        labels: set[str] = set(completed)
        try:
            # 隔离副本初始化走运行内审批路径（一条沙箱内 bootstrap 命令：
            # git worktree 优先、有界拷贝兜底、含未提交改动），不在 spawn
            # 工具里绕过 registry 直接改沙箱（plan §9.2）。
            if directory is not None and "workspace-prepare" not in labels:
                prepared = await port.prepare_workspace(
                    project_id, directory, limits=self._workspace_limits.copy_bounds(),
                )
                labels.add("workspace-prepare")
                if prepared.get("truncated") is True:
                    # 副本不完整就不能宣称隔离成功：在 worker 启动前明确拒绝
                    # （spawn 期基线截断同样应由域准入拒绝）。
                    raise ServiceError("subagent_capability")
                await emit("workspace_prepared", {
                    "directory": directory,
                    "mode": prepared.get("mode"),
                    "head": prepared.get("head"),
                    "copied": prepared.get("copied"),
                    "skipped": prepared.get("skipped"),
                    "truncated": prepared.get("truncated"),
                    "operation_id": prepared.get("operation_id"),
                })
            if file_path is not None:
                input_directory = file_path.rsplit("/", 1)[0]
                if "prepare-dir" not in labels:
                    await port.ensure_directory(
                        project_id, input_directory, label="prepare-dir",
                    )
                    labels.add("prepare-dir")
                if "prepare-input" not in labels:
                    await port.write(
                        project_id, file_path, task_json, label="prepare-input",
                    )
                    labels.add("prepare-input")
            argv = list(scoped)
            if "task-start" in labels:
                raise ServiceError("facts_corrupt")
            # bootstrap 与受控文件投递都会推进项目代：TaskStart 前刷新，
            # 避免按过期 expected_generation 被拒。
            generation = await port.project_generation()
            try:
                command = SandboxCommand(
                    project_id=project_id,
                    expected_generation=generation,
                    program=cast(Literal["python", "node"], self.launch.program),
                    argv=argv,
                    wall_seconds=self.launch.wall_seconds,
                )
            except ValueError:
                raise ServiceError("invalid_input", 422) from None
            # 工具进程槽（domain.process_slot，RequestLimiter(max_tool_processes)
            # 之上；测试可经构造参数注入）：实际 TaskStart 到确认结束之间持有；
            # 审批停靠在槽内抛出、外层 finally 先于 park 释放，等待审批不占
            # 额度；失败路径先等真实 port.cancel/清理完成再归还；执行未确认
            # （unknown）或取消失败时按 cleanup debt 保留槽（ProcessLease
            # .retain_debt 或 domain.remember_process_debt），由 domain
            # reconcile 确认干净后归还，不在本 loop 提前释放。
            slot_factory = self._process_slot or getattr(
                getattr(self, "subagents", None), "process_slot", None,
            )
            slot = None if slot_factory is None else slot_factory(run_id)
            lease: Any = None
            slot_entered = False
            debt_retained = False
            state: dict[str, Any] = {}
            if slot is not None:
                lease = await slot.__aenter__()
                slot_entered = True
            try:
                operation = await port.start(command)
                operation_id = str(operation.get("operation_id", ""))
                if not operation_id:
                    raise ServiceError("facts_corrupt")
                await emit("process_started", {
                    "operation_id": operation_id,
                    "project_id": project_id,
                    "program": self.launch.program,
                    "delivery": "file" if file_path is not None else "argv",
                    "wall_seconds": self.launch.wall_seconds,
                })
                while True:
                    state = await port.wait(project_id, operation_id, seconds=5)
                    check()
                    if state.get("status") in {"finished", "unknown"}:
                        break
                    await asyncio.sleep(0.2)
                finished = True
                logs = await port.logs(project_id, operation_id, max_bytes=8192)
                await emit("process_finished", {
                    "operation_id": operation_id,
                    "status": state.get("status"),
                    "exit_code": state.get("exit_code"),
                    "code": state.get("code"),
                    "output_complete": state.get("output_complete"),
                    "verification": state.get("verification"),
                    "generation": state.get("generation"),
                    "stdout": logs.get("stdout"),
                    "stderr": logs.get("stderr"),
                })
                if state.get("status") == "unknown":
                    raise ServiceError("execution_unknown")
                exit_code = state.get("exit_code")
                if type(exit_code) is not int or exit_code != 0:
                    raise ServiceError("execution_error")
                await self._collect_workspace_patch(
                    run_id, assignment, port, emit, check,
                )
            except BaseException:
                cancelled_clean = False
                if operation_id is not None and not finished:
                    try:
                        outcome = await settled(port.cancel(
                            project_id, operation_id, expected_generation=generation,
                        ))
                        cancelled_clean = _cancel_confirmed(outcome)
                    except Exception:
                        cancelled_clean = False
                # 只有真实 operation 才可能产生清理债：审批停靠在 port.start
                # 内部抛出时 operation_id 为空，根本没有进程可清理，槽随外层
                # Parked 立即释放，不进 debt。
                unconfirmed = operation_id is not None and (
                    finished and state.get("status") == "unknown"
                    or not finished and not cancelled_clean
                )
                if unconfirmed and slot_entered:
                    debt_retained = await self._retain_process_debt(
                        run_id, operation_id, lease, slot,
                    )
                raise
            finally:
                # ask/batch approval在port.start内抛出时，真实operation尚未
                # 创建；该路径释放slot后才外层Parked，不占工具并发额度。
                if slot is not None and slot_entered and not debt_retained:
                    await slot.__aexit__(None, None, None)
        except SandboxApprovalRequired as pending:
            data = await approval_request(pending.invocation, execution_budget.remaining)
            await self.store.append(run_id, "approval_request", data)
            checkpoint = {
                "format": "sdk-host-v2",
                "run_id": run_id,
                "response_complete": True,
                "process": {
                    "completed": sorted(labels),
                    "awaiting": pending.invocation.call_id,
                },
                "budget_remaining": execution_budget.remaining,
            }
            await self.store.write_checkpoint(run_id, checkpoint, fingerprint(checkpoint))
            raise Parked() from None
        except BaseException:
            if operation_id is not None and not finished and slot is None:
                try:
                    await settled(port.cancel(
                        project_id, operation_id, expected_generation=generation,
                    ))
                except Exception:
                    pass
            raise
