"""完整目录成果的父闭包、确定性拓扑顺序与容量边界。"""

import json

import pytest
from pydantic import ValidationError

from krim_agent.execution.directory_dependencies import (
    DirectoryDependencyEntry,
    DirectoryDependencyPlan,
    build_directory_dependency_plan,
)
from krim_agent.protocol.domain import ServiceError


def entry(path: str, kind: str, mode: int | None = None) -> DirectoryDependencyEntry:
    return DirectoryDependencyEntry(path=path, kind=kind, mode=mode)


def test_create_plan_is_parent_first_and_deterministic():
    entries = [
        entry("tree/src/main.py", "file"),
        entry("tree", "directory", 0o750),
        entry("tree/src", "directory", 0o750),
        entry("tree/README.md", "file"),
    ]
    first = build_directory_dependency_plan(entries)
    second = build_directory_dependency_plan(reversed(entries))
    assert first == second
    assert [step.path for step in first.steps] == [
        "tree",
        "tree/src",
        "tree/README.md",
        "tree/src/main.py",
    ]
    assert first.steps[-1].depends_on == ["tree/src"]


def test_delete_plan_is_child_first():
    plan = build_directory_dependency_plan(
        [
            entry("tree", "directory", 0o750),
            entry("tree/src", "directory", 0o750),
            entry("tree/src/main.py", "file"),
        ],
        operation="delete",
    )
    assert [step.path for step in plan.steps] == [
        "tree/src/main.py",
        "tree/src",
        "tree",
    ]
    assert plan.steps[-1].depends_on == ["tree/src"]


@pytest.mark.parametrize(
    "entries,code",
    [
        ([entry("tree/src/file.py", "file")], "execution_directory_dependency_parent"),
        (
            [entry("tree", "directory", 0o750), entry("tree", "file")],
            "execution_directory_dependency_conflict",
        ),
    ],
)
def test_plan_rejects_incomplete_or_conflicting_manifest(entries, code):
    with pytest.raises(ServiceError) as refused:
        build_directory_dependency_plan(entries)
    assert refused.value.code == code


def test_entry_rejects_noncanonical_path_and_wrong_mode():
    with pytest.raises(ValidationError):
        entry("tree//file", "file")
    with pytest.raises(ValidationError):
        entry("tree", "directory")
    with pytest.raises(ValidationError):
        entry("tree/file", "file", 0o644)


def test_plan_rejects_capacity_before_materializing_a_large_tree():
    entries = [entry("tree", "directory", 0o750)] + [
        entry(f"tree/file-{index}.txt", "file") for index in range(3)
    ]
    with pytest.raises(ServiceError) as refused:
        build_directory_dependency_plan(entries, max_entries=3)
    assert refused.value.code == "execution_directory_dependency_limit"


def test_capacity_stops_reading_iterator_at_limit_plus_one():
    observed = 0

    def source():
        nonlocal observed
        for index in range(10):
            observed += 1
            yield entry(f"file-{index}", "file")

    with pytest.raises(ServiceError) as refused:
        build_directory_dependency_plan(source(), max_entries=3)
    assert refused.value.code == "execution_directory_dependency_limit"
    assert observed == 4


def test_delete_plan_retains_every_child_in_a_wide_directory():
    names = [f"tree/file-{index:02}.txt" for index in range(40)]
    plan = build_directory_dependency_plan(
        [entry("tree", "directory", 0o750), *(entry(path, "file") for path in names)],
        operation="delete",
    )
    assert plan.steps[-1].path == "tree" and plan.steps[-1].depends_on == names


def test_plan_preserves_directory_modes_through_json_roundtrip():
    plan = build_directory_dependency_plan([entry("tree", "directory", 0o750)])
    value = plan.model_dump(mode="json")
    assert value["steps"][0]["mode"] == 0o750
    assert DirectoryDependencyPlan.model_validate_json(plan.model_dump_json()) == plan


def test_mutated_entry_is_revalidated_at_build():
    item = entry("file.txt", "file")
    item.path = "../escape"
    with pytest.raises(ValidationError):
        build_directory_dependency_plan([item])


@pytest.mark.parametrize("operation", ["create", "delete"])
def test_json_plan_cannot_drop_mandatory_dependencies(operation):
    plan = build_directory_dependency_plan(
        [entry("tree", "directory", 0o750), entry("tree/file", "file")],
        operation=operation,
    )
    payload = plan.model_dump(mode="json")
    next(item for item in payload["steps"] if item["depends_on"])["depends_on"] = []
    with pytest.raises(ValidationError):
        DirectoryDependencyPlan.model_validate_json(json.dumps(payload))


@pytest.mark.parametrize("field", ["path", "depth", "mode", "cycle", "duplicate"])
def test_json_plan_rejects_corrupt_path_depth_mode_and_edges(field):
    payload = build_directory_dependency_plan(
        [entry("tree", "directory", 0o750), entry("tree/file", "file")]
    ).model_dump(mode="json")
    child = payload["steps"][1]
    if field == "path":
        child["path"] = "../escape"
    elif field == "depth":
        child["depth"] = 3
    elif field == "mode":
        child["mode"] = 0o644
    elif field == "cycle":
        payload["steps"][0]["depends_on"] = ["tree/file"]
    else:
        child["depends_on"] = ["tree", "tree"]
    with pytest.raises(ValidationError):
        DirectoryDependencyPlan.model_validate_json(json.dumps(payload))
