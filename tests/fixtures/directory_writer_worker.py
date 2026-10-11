"""真实目录事务进程骤停/冷核实；当前低层JSON意图仅用于测试。"""

import json
import os
import sys
from dataclasses import asdict
from pathlib import Path

import krim_agent
import krim_agent.workspace_directory as module
from krim_agent.workspace import Workspace
from krim_agent.workspace_directory import (
    DirectoryChangeSpec,
    DirectoryRecovery,
    DirectorySnapshot,
    PreparedDirectoryChange,
    WorkspaceDirectoryWriter,
)


def main():
    folder, mode, action = sys.argv[1:]
    root = Path(folder)
    files = root / "files"
    files.mkdir(exist_ok=True)
    state_file = root / "intent.json"
    with Workspace(files, writable=True) as workspace:
        writer = WorkspaceDirectoryWriter(workspace)
        if mode in {"before", "after"}:
            if action == "delete":
                (files / "empty").mkdir()
                (files / "empty").chmod(0o750)
                snapshot = writer.capture("empty")
                spec = DirectoryChangeSpec(
                    path="empty", operation="delete", expected_fingerprint=snapshot.fingerprint
                )
            else:
                spec = DirectoryChangeSpec(path="empty", operation="create", mode=0o750)
            plan = writer.prepare(spec)

            def record(reference):
                state_file.write_text(
                    json.dumps({"plan": asdict(plan), "reference": asdict(reference)})
                )
                with state_file.open("rb") as saved:
                    os.fsync(saved.fileno())

            original = module.rename_at

            def crash(fd, source, target, **kwargs):
                if mode == "after":
                    original(fd, source, target, **kwargs)
                    os.fsync(fd)
                os._exit(23)

            module.rename_at = crash
            writer.apply(plan, record_intent=record)
            raise AssertionError("测试进程应在原系统调用边界退出")
        state = json.loads(state_file.read_text())
        raw_plan = state["plan"]
        raw_reference = state["reference"]
        before = DirectorySnapshot(**raw_plan["before"]) if raw_plan["before"] is not None else None
        plan = PreparedDirectoryChange(
            raw_plan["spec_json"],
            raw_plan["workspace_fingerprint"],
            tuple(raw_plan["parent_identity"]),
            before,
            raw_plan["token"],
        )
        reference = DirectoryRecovery(
            raw_reference["workspace_fingerprint"],
            raw_reference["path"],
            raw_reference["stage_name"],
            tuple(raw_reference["parent_identity"]),
            DirectorySnapshot(**raw_reference["expected"]),
        )
        result = writer.verify(plan, reference)
        if result.receipt.status == "done":
            writer.finalize(plan, reference)
        elif result.receipt.status == "rejected":
            writer.finalize(plan, reference, rejected=True)
        print(
            json.dumps(
                {
                    "package": krim_agent.__file__,
                    "status": result.receipt.status,
                    "target_exists": (files / "empty").exists(),
                    "stage_exists": (files / reference.stage_name).exists(),
                }
            )
        )


main()
