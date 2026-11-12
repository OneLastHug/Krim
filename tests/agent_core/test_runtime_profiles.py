"""C1规格封闭性：严格类型、资源边界、fail-closed解析与现runner参数等价。"""

import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from krim_agent.contracts import ServiceError
from krim_agent.sandbox.runner import GATE, OUTPUT_LIMIT
from krim_agent.sandbox.runtime_profiles import (
    BENIGN_PAYLOADS,
    NODE_BENIGN_PAYLOADS,
    PYTHON_BENIGN_PAYLOADS,
    BenignPayload,
    RuntimeCatalog,
    RuntimeProfile,
    RuntimeProfileEntry,
    TmpfsMount,
)

MANIFEST = Path(__file__).resolve().parents[2] / "runtime_images" / "runtime-manifest.json"
DEV_IMAGE = "sha256:d163eadaea1f779baca092829e2c6969e7514bf284880f8637d8204320ff505b"
ANY_ID = "sha256:" + "a" * 64
PYTHON_PROFILE = {"program": "python", "interpreter": "/usr/bin/python3.12"}
ENTRY_NAMES = {
    "python-normal-job-dev",
    "python-dependency-install",
    "python-connector",
    "node-normal-job",
    "node-dependency-install",
    "node-connector",
}


def catalog() -> RuntimeCatalog:
    return RuntimeCatalog.load(MANIFEST)


def python_profile(**overrides: object) -> RuntimeProfile:
    return RuntimeProfile.model_validate({**PYTHON_PROFILE, **overrides})


def dev_entry(image: str | None, scope: str | None = "dev_only") -> RuntimeProfileEntry:
    body: dict[str, object] = {
        "name": "python-dev-entry",
        "profile": PYTHON_PROFILE,
        "availability": "dev_verified",
        "scope": scope,
        "image": image,
    }
    return RuntimeProfileEntry.model_validate(body)


def not_built_entry(**overrides: object) -> RuntimeProfileEntry:
    body: dict[str, object] = {
        "name": "node-design-entry",
        "profile": {"program": "node", "interpreter": "/usr/local/bin/node"},
        "availability": "not_built",
        "scope": "release",
        "image": None,
    }
    return RuntimeProfileEntry.model_validate({**body, **overrides})


def test_manifest_entries_are_fail_closed_by_construction():
    cat = catalog()
    assert {entry.name for entry in cat.profiles} == ENTRY_NAMES
    dev = cat.entry("python", "normal_job")
    assert dev.availability == "dev_verified" and dev.scope == "dev_only"
    assert dev.image == DEV_IMAGE
    for entry in cat.profiles:
        assert entry.profile.network == "none"
        assert entry.profile.dropped_capabilities == "ALL"
        assert entry.profile.no_new_privileges is True
        assert entry.profile.read_only_root is True
        assert entry.profile.run_as_uid == entry.profile.run_as_gid == 65532
        if entry.availability == "not_built":
            assert entry.image is None
            with pytest.raises(ServiceError):
                cat.resolve(entry.profile.program, entry.profile.purpose)
            with pytest.raises(ServiceError):
                cat.resolve(entry.profile.program, entry.profile.purpose, allow_dev=True)


def test_python_dev_profile_matches_current_runner_contract():
    resolved = catalog().resolve("python", "normal_job", allow_dev=True)
    assert resolved.image == DEV_IMAGE and resolved.scope == "dev_only"
    profile = resolved.profile
    assert profile.interpreter == "/usr/bin/python3.12"
    assert profile.cpu_cores == 1.0
    assert profile.memory_bytes == profile.memory_swap_bytes == 2**30
    assert profile.pids_limit == 512
    assert profile.nproc_limit == 128
    assert profile.output_limit_bytes == OUTPUT_LIMIT == 32768
    assert profile.tmpfs == (TmpfsMount(destination="/tmp", size_bytes=134217728),)
    assert profile.argv_bytes_limit == 8192
    assert profile.safe_flags == ("-I",)
    # 与RunscRunner容器入口逐项一致；监管门后的用户argv等价于[sys.executable,'-I',*argv]。
    assert profile.gate_entry(GATE) == ["/usr/bin/python3.12", "-I", "-u", "-c", GATE]
    assert profile.payload_argv(["-c", "print(1)"]) == [
        "/usr/bin/python3.12",
        "-I",
        "-c",
        "print(1)",
    ]
    again = catalog().resolve("python", "normal_job", allow_dev=True)
    assert again.fingerprint == resolved.fingerprint
    assert len(resolved.fingerprint) == 64
    assert all(character in "0123456789abcdef" for character in resolved.fingerprint)


def test_resolve_refuses_unknown_not_built_dev_and_wall_overflow():
    cat = catalog()
    with pytest.raises(ServiceError):
        cat.resolve("shell", "normal_job")
    with pytest.raises(ServiceError):
        cat.resolve("python", "eval")
    with pytest.raises(ServiceError):
        cat.resolve("python", "normal_job")
    with pytest.raises(ServiceError):
        cat.resolve("node", "normal_job", allow_dev=True)
    with pytest.raises(ServiceError):
        cat.resolve("python", "dependency_install", allow_dev=True)
    with pytest.raises(ServiceError):
        cat.resolve("python", "normal_job", allow_dev=True, wall_seconds=61)
    assert cat.resolve("python", "normal_job", allow_dev=True, wall_seconds=60).program == "python"


def test_image_reference_accepts_only_id_or_official_digest():
    dev_entry(ANY_ID)
    dev_entry(f"docker.io/library/python@sha256:{'b' * 64}", scope="release")
    for image, scope in [
        ("docker.io/library/python:3.12@sha256:" + "b" * 64, "release"),
        ("python:latest", "release"),
        ("sha256:" + "g" * 64, "dev_only"),
        ("sha256:" + "a" * 63, "dev_only"),
        ("registry.example.com/mirror/python@sha256:" + "b" * 64, "release"),
        (f"docker.io/library/node@sha256:{'b' * 64}", "release"),
    ]:
        with pytest.raises(ValidationError):
            dev_entry(image, scope=scope)
    with pytest.raises(ValidationError):
        dev_entry(None)
    with pytest.raises(ValidationError):
        dev_entry(ANY_ID, scope=None)
    with pytest.raises(ValidationError):
        dev_entry(ANY_ID, scope="release")
    with pytest.raises(ValidationError):
        not_built_entry(image=ANY_ID, scope="dev_only")


def test_strict_types_reject_bools_strings_and_extra_fields():
    for patch in [
        {"wall_seconds": True},
        {"max_wall_seconds": False},
        {"cpu_cores": "1"},
        {"pids_limit": True},
        {"memory_bytes": 1073741824.0},
        {"network": "bridge"},
        {"dropped_capabilities": "ALL,SYS_ADMIN"},
        {"no_new_privileges": False},
        {"host_path": "/"},
        {"env": {"PATH": "/tmp"}},
        {"mount": "/etc"},
        {"network_mode": "host"},
        {"privileged": True},
    ]:
        with pytest.raises(ValidationError):
            python_profile(**patch)
    profile = python_profile()
    with pytest.raises(ValidationError):
        profile.cpu_cores = 2.0  # type: ignore[misc]
    with pytest.raises(ServiceError):
        profile.validate_argv([1])
    with pytest.raises(ValidationError):
        TmpfsMount(destination="/tmp", size_bytes=134217728, mode=9999)


def test_argv_bounds_and_full_array_generation():
    profile = python_profile()
    with pytest.raises(ServiceError):
        profile.validate_argv([])
    with pytest.raises(ServiceError):
        profile.validate_argv(["a"] * 33)
    with pytest.raises(ServiceError):
        profile.validate_argv(["a" * 8193])
    with pytest.raises(ServiceError):
        profile.validate_argv(["a" * 4096, "b" * 4097])
    with pytest.raises(ServiceError):
        profile.validate_argv(["x\0"])
    assert profile.validate_argv(["a" * 256] * 32) == ["a" * 256] * 32
    assert profile.payload_argv(["-c", "1"]) == ["/usr/bin/python3.12", "-I", "-c", "1"]


def test_wall_and_tool_timeout_coordination():
    assert python_profile(wall_seconds=60, tool_timeout_seconds=75).max_wall_seconds == 60
    with pytest.raises(ValidationError):
        python_profile(wall_seconds=60, tool_timeout_seconds=74)
    with pytest.raises(ValidationError):
        python_profile(wall_seconds=61)
    with pytest.raises(ValidationError):
        python_profile(wall_seconds=31, max_wall_seconds=30)
    assert python_profile(wall_seconds=30, max_wall_seconds=30).max_wall_seconds == 30
    with pytest.raises(ValidationError):
        python_profile(tool_timeout_seconds=181)
    with pytest.raises(ValidationError):
        python_profile(supervisor_grace_seconds=0)


def test_resource_and_path_boundaries():
    for patch in [
        {"memory_bytes": 2**34, "memory_swap_bytes": 2**34},
        {"memory_swap_bytes": 2**30 + 1},
        {"pids_limit": 513},
        {"pids_limit": 15},
        {"nproc_limit": 129},
        {"cpu_cores": 0},
        {"cpu_cores": 4.5},
        {"output_limit_bytes": 32769},
        {"run_as_uid": 0},
        {"run_as_gid": 0},
        {"interpreter": "/usr/bin/node"},
        {"interpreter": "python3.12"},
        {"interpreter": "/usr/bin/../bin/python3.12"},
        {"interpreter": "/usr/bin/python3.12 "},
        {"workdir": "workspace"},
        {"workdir": "/"},
        {"tmpfs": ({"destination": "/var/tmp", "size_bytes": 134217728},)},
        {"tmpfs": ({"destination": "/tmp", "size_bytes": 536870913},)},
        {"dependency_dirs": ("/opt/krim-deps",)},
        {"dependency_dirs": ("/workspace",)},
        {"dependency_dirs": ("/tmp/deps",)},
        {"dependency_dirs": ("relative/path",)},
        {"dependency_dirs": ("/workspace/../outside",)},
    ]:
        with pytest.raises(ValidationError):
            python_profile(**patch)
    assert python_profile(output_limit_bytes=32768).output_limit_bytes == 32768
    assert python_profile(dependency_dirs=("/workspace/.krim-deps",)).dependency_dirs == (
        "/workspace/.krim-deps",
    )
    for destination in ["tmp", "/tmp/", "/tmp/../x", "/tmp//y", "/"]:
        with pytest.raises(ValidationError):
            TmpfsMount(destination=destination, size_bytes=134217728)


def test_node_profile_generates_safe_flags_and_rejects_untrusted_gate():
    node = catalog().profile("node", "normal_job")
    assert node.safe_flags == (
        "--disallow-code-generation-from-strings",
        "--max-old-space-size=512",
    )
    assert node.payload_argv(["--eval", "1+1"]) == [
        "/usr/local/bin/node",
        "--disallow-code-generation-from-strings",
        "--max-old-space-size=512",
        "--eval",
        "1+1",
    ]
    with pytest.raises(ServiceError):
        node.gate_entry("trusted-gate")
    with pytest.raises(ServiceError):
        node.validate_argv(["--eval"])
    with pytest.raises(ServiceError):
        node.validate_argv(["/etc/passwd"])
    with pytest.raises(ServiceError):
        node.validate_argv(["../outside.js"])


def test_benign_payloads_pass_profile_argv_bounds():
    cat = catalog()
    assert {payload.program for payload in BENIGN_PAYLOADS} == {"python", "node"}
    for payload in BENIGN_PAYLOADS:
        profile = cat.profile(payload.program, "normal_job")
        assert profile.validate_argv(payload.argv) == list(payload.argv)
    assert len(NODE_BENIGN_PAYLOADS) == 3 and len(PYTHON_BENIGN_PAYLOADS) == 4


def test_python_benign_payloads_execute_locally_without_workspace_fixture():
    if sys.platform != "linux" or not Path("/tmp").is_dir():
        pytest.skip("需要在linux宿主上运行python良性负载")
    for payload in PYTHON_BENIGN_PAYLOADS:
        if payload.workspace_fixture is not None:
            continue
        result = subprocess.run(
            [sys.executable, "-I", *payload.argv], capture_output=True, text=True, timeout=30
        )
        assert result.returncode == 0, (payload.name, result.stderr)
        assert payload.expect_stdout in result.stdout, (payload.name, result.stdout)


def json_duplicate_purpose() -> str:
    template = (
        '{"name":"ENTRY","profile":{"program":"python",'
        '"interpreter":"/usr/bin/python3.12"},"availability":"not_built"}'
    )
    first = template.replace("ENTRY", "first-dup")
    second = template.replace("ENTRY", "second-dup")
    return f'{{"schema_version":1,"profiles":[{first},{second}]}}'


def test_catalog_resolve_image_requires_registered_digest_and_scope():
    cat = catalog()
    assert cat.resolve_image(
        DEV_IMAGE, "python", allow_dev=True
    ).name == "python-normal-job-dev"
    with pytest.raises(ServiceError, match="sandbox_backend_unavailable"):
        cat.resolve_image("sha256:" + "a" * 64, "python", allow_dev=True)
    with pytest.raises(ServiceError, match="sandbox_backend_unavailable"):
        cat.resolve_image(DEV_IMAGE, "python")
    with pytest.raises(ServiceError, match="sandbox_backend_unavailable"):
        cat.resolve("node", "normal_job", allow_dev=True)


def test_malformed_manifest_and_catalog_fail_closed(tmp_path: Path):
    with pytest.raises(ServiceError):
        RuntimeCatalog.load(tmp_path / "missing.json")
    for content in [
        "{not json",
        '{"schema_version":2,"profiles":[]}',
        '{"schema_version":1,"extra":true,"profiles":[]}',
        '{"schema_version":1,"profiles":[]}',
        json_duplicate_purpose(),
    ]:
        path = tmp_path / "manifest.json"
        path.write_text(content, encoding="utf-8")
        with pytest.raises(ServiceError):
            RuntimeCatalog.load(path)
    with pytest.raises(ValidationError):
        RuntimeCatalog.model_validate(
            {
                "schema_version": 1,
                "profiles": [
                    {
                        "name": "dup-a",
                        "profile": PYTHON_PROFILE,
                        "availability": "not_built",
                    },
                    {
                        "name": "dup-b",
                        "profile": PYTHON_PROFILE,
                        "availability": "not_built",
                    },
                ],
            }
        )


def test_benign_payload_model_rejects_bad_definitions():
    BenignPayload(name="ok-payload", program="python", argv=["-c", "1"], expect_stdout="1")
    base = {
        "name": "ok-payload",
        "program": "python",
        "argv": ["-c", "1"],
        "expect_stdout": "1",
    }
    for patch in [
        {"name": "UPPER"},
        {"argv": []},
        {"argv": ["x\0"]},
        {"expect_stdout": ""},
        {"workspace_fixture": "relative"},
        {"extra": True},
    ]:
        with pytest.raises(ValidationError):
            BenignPayload.model_validate({**base, **patch})
