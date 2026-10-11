"""项目外wheel验证依赖顺序、原mode、JSON守卫和无文件副作用。"""

import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from pydantic import ValidationError

import krim_agent
from krim_agent.execution import (
    DirectoryDependencyEntry,
    DirectoryDependencyPlan,
    build_directory_dependency_plan,
)

assert "/site-packages/" in str(krim_agent.__file__)
with TemporaryDirectory(prefix="krim-directory-dependency-", dir="/data/tmp/root") as folder:
    workspace = Path(folder)
    original = b"\x00\xffuntouched"
    (workspace / "before.bin").write_bytes(original)
    entries = [
        DirectoryDependencyEntry(path="tree", kind="directory", mode=0o750),
        DirectoryDependencyEntry(path="tree/sub", kind="directory", mode=0o700),
        DirectoryDependencyEntry(path="tree/sub/payload.bin", kind="file"),
    ]
    create = build_directory_dependency_plan(reversed(entries))
    delete = build_directory_dependency_plan(entries, operation="delete")
    assert [item.path for item in create.steps] == [item.path for item in entries]
    assert [item.path for item in delete.steps] == [item.path for item in reversed(entries)]
    assert create.steps[0].mode == 0o750 and create.steps[1].mode == 0o700
    assert DirectoryDependencyPlan.model_validate_json(create.model_dump_json()) == create
    forged = create.model_dump(mode="json")
    forged["steps"][-1]["depends_on"] = []
    try:
        DirectoryDependencyPlan.model_validate_json(json.dumps(forged))
    except ValidationError:
        pass
    else:
        raise AssertionError("missing dependency accepted")
    wide = [
        DirectoryDependencyEntry(path=f"tree/file-{index:02}", kind="file") for index in range(40)
    ]
    wide_plan = build_directory_dependency_plan([entries[0], *wide], operation="delete")
    assert len(wide_plan.steps[-1].depends_on) == 40
    assert sorted(item.name for item in workspace.iterdir()) == ["before.bin"]
    assert (
        hashlib.sha256((workspace / "before.bin").read_bytes()).digest()
        == hashlib.sha256(original).digest()
    )
    print(
        json.dumps(
            {
                "package": krim_agent.__file__,
                "create_delete_order": True,
                "mode_retained": True,
                "json_guard": True,
                "wide_directory": True,
                "zero_file_effects": True,
            }
        )
    )
