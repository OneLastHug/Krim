"""二进制输入仍受原工具权限、真实身份、容量与冻结回程约束。"""

import base64
import hashlib
import os
import shutil
import sys

import pytest
from test_agent_commands import NAMES, request, setup

from krim_agent.execution import SandboxLimits
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import CommandRead
from krim_agent.tools import ShortCircuit
from krim_agent.workspace import Workspace, WorkspaceFile
from krim_agent.workspace_writer import WorkspaceWriter

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要真实Linux命令与文件事务",
)


@pytest.mark.parametrize("payload", [b"\0\xff" * 6500, b"\xff\xfe\x80" * 4300])
async def test_readonly_binary_input_is_full_private_bytes_with_bounded_summary(
    runtime, tmp_path, payload
):
    target = tmp_path / "input.bin"
    target.write_bytes(payload)
    target.chmod(0o640)
    service, agent, runner = await setup(runtime, tmp_path)
    observed = []

    async def inspect(definition, value, context):
        if definition.name == "workspace_read" and context.command_capture is not None:
            observed.append(dict(value))
        return value

    service._handle(agent.agent_id).runtime.tools.add_global_post(inspect)
    intent = request(
        "data=open('input.bin','rb').read();print(len(data),data[-3:].hex());"
        "open('copy.bin','wb').write(data)",
        paths=["input.bin"],
    )
    try:
        result = await agent.execute_command(intent)
        assert result.outcome == "completed" and runner.calls == 1
        assert result.stdout_text == f"{len(payload)} {payload[-3:].hex()}\n"
        assert target.read_bytes() == payload
        assert observed == [
            WorkspaceFile(
                path="input.bin",
                sha256=hashlib.sha256(payload).hexdigest(),
                byte_size=len(payload),
                start_line=1,
                lines=0,
                content=(
                    f"binary bytes={len(payload)} sha256={hashlib.sha256(payload).hexdigest()}\n"
                ),
                truncated=True,
            ).model_dump(mode="json")
        ]
        saved = bytearray()
        for offset in range(0, len(payload), 4096):
            part = await agent.read_command(
                CommandRead(
                    command_id=result.command_id,
                    source="artifact",
                    path="copy.bin",
                    offset=offset,
                    limit=4096,
                )
            )
            saved.extend(base64.b64decode(part.data_base64))
        assert bytes(saved) == payload
        inputs = await service.manager.commands.store.files(
            "session:" + agent.session_id, result.command_id, "input"
        )
        assert inputs["input.bin"].data == payload and inputs["input.bin"].mode == 0o640
        with pytest.raises(ServiceError) as text_only:
            await agent.read_file("input.bin")
        assert text_only.value.code in {"workspace_binary", "workspace_encoding"}
        target.write_bytes(b"external")
        assert (await agent.execute_command(intent)).command_id == result.command_id
        assert runner.calls == 1 and len(observed) == 1 and target.read_bytes() == b"external"
    finally:
        await service.close()


@pytest.mark.parametrize(
    "fault", ["deny", "short", "path", "range", "content", "sha256", "size", "missing"]
)
async def test_binary_input_cannot_bypass_policy_capture_or_hook_consistency(
    runtime, tmp_path, fault
):
    payload = b"\0\xff\x01"
    (tmp_path / "input.bin").write_bytes(payload)
    (tmp_path / "other.bin").write_bytes(b"\0other")

    async def policy(definition, _context):
        return "deny" if fault == "deny" and definition.name == "workspace_read" else "allow"

    service, agent, runner = await setup(
        runtime, tmp_path, policy=policy, names=NAMES[1:] if fault == "missing" else None
    )
    registry = service._handle(agent.agent_id).runtime.tools

    async def pre(definition, context):
        if definition.name == "workspace_read":
            if fault == "short":
                return ShortCircuit(
                    WorkspaceFile(
                        path="input.bin",
                        sha256=hashlib.sha256(payload).hexdigest(),
                        byte_size=len(payload),
                        start_line=1,
                        lines=0,
                        content="binary bytes=3",
                        truncated=True,
                    ).model_dump(mode="json")
                )
            if fault == "path":
                context.arguments["path"] = "other.bin"
            if fault == "range":
                context.arguments["start_line"] = 2

    async def post(definition, value, _context):
        if definition.name == "workspace_read":
            if fault == "content":
                value["content"] = "forged binary summary"
            if fault == "sha256":
                value["sha256"] = "0" * 64
            if fault == "size":
                value["byte_size"] = 0
        return value

    registry.add_global_pre(pre)
    registry.add_global_post(post)
    try:
        with pytest.raises(ServiceError) as rejected:
            await agent.execute_command(request(paths=["input.bin"]))
        assert rejected.value.code in {
            "permission_denied",
            "tool_not_found",
            "execution_input_target",
            "execution_input_not_captured",
        }
        assert runner.calls == 0
        assert (
            await service.manager.commands.store.by_key("session:" + agent.session_id, "one")
            is None
        )
        assert (tmp_path / "input.bin").read_bytes() == payload
    finally:
        await service.close()


@pytest.mark.parametrize("limit", ["file", "total"])
async def test_binary_inputs_obey_execution_file_and_aggregate_limits(runtime, tmp_path, limit):
    (tmp_path / "a.bin").write_bytes(b"\0" * (1025 if limit == "file" else 600))
    (tmp_path / "b.bin").write_bytes(b"\xff" * 600)
    service, agent, runner = await setup(
        runtime, tmp_path, limits=SandboxLimits(file_bytes=1024, artifact_bytes=1024)
    )
    try:
        with pytest.raises(ServiceError) as rejected:
            await agent.execute_command(
                request(paths=["a.bin"] if limit == "file" else ["a.bin", "b.bin"])
            )
        assert rejected.value.code == "execution_input_limit" and runner.calls == 0
    finally:
        await service.close()


@pytest.mark.parametrize(
    "kind", ["capacity", "symlink", "hardlink", "directory", "fifo", "sensitive"]
)
def test_private_binary_capture_keeps_workspace_boundaries(tmp_path, kind):
    target = tmp_path / "input.bin"
    target.write_bytes(b"\0\xff")
    path = "input.bin"
    expected = {
        "capacity": "workspace_file_limit",
        "symlink": "workspace_access",
        "hardlink": "workspace_link",
        "directory": "workspace_file_type",
        "fifo": "workspace_file_type",
        "sensitive": "workspace_sensitive",
    }[kind]
    if kind == "symlink":
        (tmp_path / "link.bin").symlink_to(target)
        path = "link.bin"
    elif kind == "hardlink":
        os.link(target, tmp_path / "hard.bin")
    elif kind == "directory":
        (tmp_path / "directory").mkdir()
        path = "directory"
    elif kind == "fifo":
        os.mkfifo(tmp_path / "fifo")
        path = "fifo"
    elif kind == "sensitive":
        path = ".env"
        (tmp_path / path).write_bytes(b"\0secret")
    with Workspace(tmp_path, max_file_bytes=1 if kind == "capacity" else 64) as workspace:
        with pytest.raises(ServiceError) as rejected:
            WorkspaceWriter(workspace).capture_file(path, require_writable=False, allow_binary=True)
        assert rejected.value.code == expected


@pytest.mark.parametrize("change", ["bytes", "identity", "mode", "parent"])
async def test_binary_input_changed_after_capture_never_dispatches(runtime, tmp_path, change):
    parent = tmp_path / "source"
    parent.mkdir()
    target = parent / "input.bin"
    target.write_bytes(b"\0\xff")
    (tmp_path / "second.bin").write_bytes(b"\0second")
    service, agent, runner = await setup(runtime, tmp_path)

    async def mutate(definition, value, context):
        if definition.name == "workspace_read" and context.arguments["path"] == "second.bin":
            if change == "bytes":
                target.write_bytes(b"\0external")
            elif change == "identity":
                target.rename(parent / "saved.bin")
                target.write_bytes(b"\0\xff")
            elif change == "mode":
                target.chmod(0o600)
            else:
                parent.rename(tmp_path / "saved")
                parent.mkdir()
                target.write_bytes(b"\0\xff")
        return value

    service._handle(agent.agent_id).runtime.tools.add_global_post(mutate)
    try:
        with pytest.raises(ServiceError) as rejected:
            await agent.execute_command(request(paths=["source/input.bin", "second.bin"]))
        assert rejected.value.code in {"workspace_checkpoint_changed", "workspace_parent_changed"}
        assert runner.calls == 0
    finally:
        await service.close()
