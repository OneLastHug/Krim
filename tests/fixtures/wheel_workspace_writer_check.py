"""项目外安装包验证：宿主文件事务、工厂写能力与旧只读目录一致。"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import krim_agent
from krim_agent.cli import build_parser
from krim_agent.sdk import PythonAI
from krim_agent.workspace_change import FileChangeSpec, TextReplacement
from krim_agent.workspace_writer import WorkspaceWriter


async def main():
    assert "/site-packages/" in krim_agent.__file__
    with TemporaryDirectory(prefix="krim-write-wheel-") as directory:
        root = Path(directory)
        (root / "file.txt").write_text("old\n", encoding="utf-8")
        async with await PythonAI.create(workspace_root=root, workspace_writable=True) as ai:
            binding = ai.agents().resources.workspace("local")
            assert binding.workspace.writable
            writer = WorkspaceWriter(binding.workspace)
            prepared = writer.prepare(
                FileChangeSpec(
                    path="file.txt",
                    operation="replace",
                    expected_sha256=binding.workspace.read("file.txt").sha256,
                    replacements=[TextReplacement(old_text="old", new_text="new")],
                )
            )
            effect = writer.apply(prepared)
            assert effect.receipt.status == "done" and (root / "file.txt").read_text() == "new\n"
            assert writer.inspect_recovery(effect.recovery)[1].data == b"old\n"
            writer.finalize(prepared, effect.recovery)
            assert not await asyncio.to_thread(lambda: list(root.glob("._krim-tx-*")))
            agent = await ai.agent()
            assert (await agent.workspace()).writable
            assert (await agent.read_file("file.txt")).sha256 == effect.receipt.after_sha256
            assert {
                tool.name
                for tool in ai.runtime.tools.definitions
                if tool.name.startswith("workspace_")
            } == {"workspace_read", "workspace_list", "workspace_search"}
        args = build_parser().parse_args(["serve", "--workspace", str(root), "--workspace-write"])
        assert args.workspace_write
        print(
            json.dumps(
                {
                    "package": krim_agent.__file__,
                    "write_verified": True,
                    "old_snapshot_verified": True,
                    "cleanup_verified": True,
                    "factory_writable": True,
                    "model_write_tools": False,
                }
            )
        )


asyncio.run(main())
