"""RunscRunner与注入规格的接线：创建参数、入口、cgroup核对全部同步profile。

替身docker按创建参数如实应答inspect；监管门流程用本机子进程模拟
KRIM_GATE_READY/KRIM_PAYLOAD_START握手协议，cgroup文件经假Path注入。
"""

from __future__ import annotations

import base64
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.sandbox import runner as runner_module
from krim_agent.sandbox.runner import GATE, OUTPUT_LIMIT, RunscRunner
from krim_agent.sandbox.runtime_profiles import (
    LEGACY_PYTHON_PROFILE,
    TMPFS_FLAGS,
    ResolvedRuntime,
    RuntimeCatalog,
    RuntimeProfile,
    TmpfsMount,
)

MANIFEST = Path(__file__).resolve().parents[2] / "runtime_images" / "runtime-manifest.json"
DEV_IMAGE = "sha256:d163eadaea1f779baca092829e2c6969e7514bf284880f8637d8204320ff505b"
IDENTITY = "sha256:" + "b" * 64
FAKE_PID = 4242

# 与监管门相同的输出协议；两段输出间留间隔，使输出超限触发在started之后。
MINI_GATE = (
    "import sys,time\n"
    "w=sys.stdout.buffer\n"
    "w.write(b'KRIM_GATE_READY\\n');w.flush()\n"
    "if sys.stdin.buffer.readline(8)!=b'go\\n':sys.exit(125)\n"
    "w.write(b'KRIM_PAYLOAD_START\\n');w.flush()\n"
    "time.sleep(0.1)\n"
    "w.write(sys.argv[1].encode()*int(sys.argv[2]));w.flush()\n"
    "sys.exit(0)\n"
)
SILENT_GATE = "import time\ntime.sleep(30)\n"


def catalog() -> RuntimeCatalog:
    return RuntimeCatalog.load(MANIFEST)


def profile_with(**overrides: object) -> RuntimeProfile:
    body: dict[str, Any] = LEGACY_PYTHON_PROFILE.model_dump(mode="json")
    body.update(overrides)
    return RuntimeProfile.model_validate(body)


def runtime_for(profile: RuntimeProfile, image: str = DEV_IMAGE) -> ResolvedRuntime:
    return ResolvedRuntime(
        name="test-profile",
        program=profile.program,
        purpose="normal_job",
        scope="dev_only",
        image=image,
        profile=profile,
    )


def node_runtime() -> ResolvedRuntime:
    return ResolvedRuntime(
        name="test-node",
        program="node",
        purpose="normal_job",
        scope="dev_only",
        image=DEV_IMAGE,
        profile=RuntimeProfile(program="node", interpreter="/usr/local/bin/node"),
    )


def observe(_name: str, _data: dict[str, Any]) -> None:
    """阶段回调占位。"""


class FakeDocker:
    """替身docker CLI：容器记录由create参数如实生成，inspect按其应答。"""

    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.create_args: list[str] | None = None
        self.containers: dict[str, dict[str, Any]] = {}
        self.running = False

    def __call__(self, *arguments: str) -> Any:
        self.calls.append(arguments)
        if arguments[:2] == ("image", "inspect"):
            return [{"Id": IDENTITY}]
        if arguments[0] == "info":
            return {"Runtimes": {"runsc": {}}, "CgroupVersion": "2"}
        if arguments[0] == "inspect":
            record = self.containers.get(arguments[1])
            if record is None:
                return [
                    {
                        "Id": IDENTITY,
                        "Image": IDENTITY,
                        "State": {"Running": False, "Pid": 0, "ExitCode": 0},
                        "Config": {"Labels": {}, "User": ""},
                        "HostConfig": {},
                        "Mounts": [],
                    }
                ]
            value = dict(record)
            value["State"] = {
                "Running": self.running,
                "Pid": FAKE_PID if self.running else 0,
                "ExitCode": 0,
            }
            return [value]
        raise AssertionError(arguments)

    def register(self, args: list[str]) -> None:
        self.create_args = list(args)
        name = args[args.index("--name") + 1]
        labels: dict[str, str] = {}
        user = ""
        network = ""
        cap_drop: list[str] = []
        security: list[str] = []
        mounts: list[dict[str, str]] = []
        readonly = False
        index = 0
        while index < len(args):
            token = args[index]
            # runner对--network/--user/--cap-drop/--security-opt均发单token"="形式；
            # 双token分支仅为替身自身的健壮性保留。
            if token == "--label":
                key, _, value = args[index + 1].partition("=")
                labels[key] = value
                index += 2
            elif token.startswith("--user="):
                user = token.partition("=")[2]
                index += 1
            elif token.startswith("--network="):
                network = token.partition("=")[2]
                index += 1
            elif token.startswith("--cap-drop="):
                cap_drop.append(token.partition("=")[2])
                index += 1
            elif token.startswith("--security-opt="):
                security.append(token.partition("=")[2])
                index += 1
            elif token == "--read-only":
                readonly = True
                index += 1
            elif token == "--mount":
                pieces = dict(piece.partition("=")[::2] for piece in args[index + 1].split(","))
                mounts.append({"Source": pieces["src"], "Destination": pieces["dst"]})
                index += 2
            else:
                index += 1
        self.containers[name] = {
            "Id": IDENTITY,
            "Image": IDENTITY,
            "Config": {"Labels": labels, "User": user},
            "HostConfig": {
                "Runtime": "runsc",
                "NetworkMode": network,
                "ReadonlyRootfs": readonly,
                "Privileged": False,
                "PidMode": "",
                "CapDrop": cap_drop,
                "SecurityOpt": security,
            },
            "Mounts": mounts,
        }


class FakeRun:
    """替身subprocess.run；create_returncode=1用于捕获create参数且不进入start。"""

    def __init__(self, fake: FakeDocker, create_returncode: int = 0) -> None:
        self.fake = fake
        self.create_returncode = create_returncode

    def __call__(self, args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[bytes]:
        assert args[0] == "/usr/bin/docker" and args[1] != "start"
        if args[1] == "create":
            self.fake.create_args = list(args)  # 参数捕获与成败无关
            if self.create_returncode == 0:
                self.fake.register(args)
            return subprocess.CompletedProcess(args, self.create_returncode, b"", b"")
        if args[1] in {"stop", "kill", "rm"}:
            self.fake.running = False
            return subprocess.CompletedProcess(args, 0, b"", b"")
        raise AssertionError(args)


class GateStart:
    """docker start替身：改启本机子进程，按监管门协议输出。"""

    def __init__(self, fake: FakeDocker, script: str, payload: list[str]) -> None:
        self.fake = fake
        self.script = script
        self.payload = payload
        self.real = subprocess.Popen

    def __call__(self, args: list[str], **kwargs: Any) -> subprocess.Popen[bytes]:
        assert args[:2] == ["/usr/bin/docker", "start"]
        self.fake.running = True
        return self.real([sys.executable, "-I", "-c", self.script, *self.payload], **kwargs)


@pytest.fixture
def fake_docker(monkeypatch: pytest.MonkeyPatch) -> FakeDocker:
    fake = FakeDocker()
    monkeypatch.setattr(RunscRunner, "docker", fake)
    monkeypatch.setattr(runner_module.subprocess, "run", FakeRun(fake))
    return fake


@pytest.fixture
def capture_only(fake_docker: FakeDocker, monkeypatch: pytest.MonkeyPatch) -> FakeDocker:
    # create失败即可完整捕获create参数，且流程不进入start。
    monkeypatch.setattr(runner_module.subprocess, "run", FakeRun(fake_docker, create_returncode=1))
    return fake_docker


def make_runner(runtime: ResolvedRuntime | None = None) -> RunscRunner:
    return RunscRunner(DEV_IMAGE, str(uuid.uuid4()), runtime)


def install_cgroup_fs(monkeypatch: pytest.MonkeyPatch, pid: int, values: dict[str, str]) -> None:
    group = "/sys/fs/cgroup/krim/jobs/x"
    files = {
        f"/proc/{pid}/cgroup": "0::/krim/jobs/x\n",
        group + "/cpu.max": values["cpu.max"],
        group + "/memory.max": values["memory.max"],
        group + "/memory.swap.max": values["memory.swap.max"],
        group + "/pids.max": values["pids.max"],
    }

    class FakePath:
        def __init__(self, raw: str) -> None:
            self.raw = raw

        def __truediv__(self, other: str) -> FakePath:
            return FakePath(self.raw + "/" + other)

        def read_text(self) -> str:
            if self.raw not in files:
                raise FileNotFoundError(self.raw)
            return files[self.raw]

    monkeypatch.setattr(runner_module, "Path", FakePath)


def legacy_values() -> dict[str, str]:
    """1 CPU、1GiB内存、禁swap、512 pids；即LEGACY_PYTHON_PROFILE的期望核对值。"""
    return {
        "cpu.max": "100000 100000",
        "memory.max": "1073741824",
        "memory.swap.max": "0",
        "pids.max": "512",
    }


def test_legacy_constructor_keeps_historical_create_parameters(
    capture_only: FakeDocker, tmp_path: Path
):
    content = tmp_path / "content"
    content.mkdir()
    runner = make_runner()
    operation = str(uuid.uuid4())
    receipt = runner.run(
        operation, content, ["-c", "print(1)"], 5, threading.Event(), observe
    )
    assert receipt["code"] == "sandbox_setup" and receipt["cleanup"] == "clean"
    assert receipt["executor_started"] is False
    # 与profile化之前RunscRunner的固定参数逐项等价：安全边界与资源上限原样保留；
    # --memory/--memory-swap/--tmpfs size由单位写法改为等值字节数。
    assert capture_only.create_args == [
        "/usr/bin/docker",
        "create",
        "--name",
        runner.name(operation),
        "--interactive",
        "--label",
        "krim.installation=" + runner.installation_id,
        "--label",
        "krim.operation=" + operation,
        "--runtime=runsc",
        "--network=none",
        "--read-only",
        "--cap-drop=ALL",
        "--security-opt=no-new-privileges",
        "--user=65532:65532",
        "--memory=1073741824",
        "--memory-swap=1073741824",
        "--cpus=1",
        "--pids-limit=512",
        "--ulimit",
        "nproc=128:128",
        "--log-driver=none",
        "--tmpfs=/tmp:rw,nosuid,nodev,noexec,size=134217728,mode=1777",
        "--env",
        "KRIM_JOB_WALL=5",
        "--workdir=/workspace",
        "--mount",
        "type=bind,src=" + str(content) + ",dst=/workspace",
        DEV_IMAGE,
        "/usr/bin/python3.12",
        "-I",
        "-u",
        "-c",
        GATE,
        "-c",
        "print(1)",
    ]


def test_manifest_runtime_reproduces_legacy_parameters(capture_only: FakeDocker, tmp_path: Path):
    content = tmp_path / "content"
    content.mkdir()
    resolved = catalog().resolve("python", "normal_job", allow_dev=True)
    assert resolved.profile.output_limit_bytes == OUTPUT_LIMIT == 32768
    runner = make_runner(resolved)
    assert runner.runtime is resolved and runner.profile is resolved.profile
    runner.run(str(uuid.uuid4()), content, ["-c", "print(1)"], 5, threading.Event(), observe)
    args = capture_only.create_args
    assert args is not None
    # 注入manifest规格与历史默认生成完全相同的创建参数与入口。
    assert "--user=65532:65532" in args
    assert "--memory=1073741824" in args and "--memory-swap=1073741824" in args
    assert "--cpus=1" in args and "--pids-limit=512" in args
    assert args[args.index("--ulimit") + 1] == "nproc=128:128"
    assert "--tmpfs=/tmp:rw,nosuid,nodev,noexec,size=134217728,mode=1777" in args
    assert args[args.index(DEV_IMAGE) + 1 :] == [
        "/usr/bin/python3.12",
        "-I",
        "-u",
        "-c",
        GATE,
        "-c",
        "print(1)",
    ]


def test_create_parameters_are_sourced_from_injected_profile(
    capture_only: FakeDocker, tmp_path: Path
):
    content = tmp_path / "content"
    content.mkdir()
    profile = profile_with(
        cpu_cores=2.0,
        memory_bytes=536870912,
        memory_swap_bytes=536870912,
        pids_limit=256,
        nproc_limit=64,
        run_as_uid=65533,
        run_as_gid=65533,
        wall_seconds=10,
        max_wall_seconds=30,
        tmpfs=(TmpfsMount(destination="/tmp", size_bytes=67108864),),
    )
    runner = make_runner(runtime_for(profile))
    receipt = runner.run(
        str(uuid.uuid4()), content, ["-c", "print(1)"], 10, threading.Event(), observe
    )
    assert receipt["code"] == "sandbox_setup" and receipt["cleanup"] == "clean"
    args = capture_only.create_args
    assert args is not None
    assert "--cpus=2" in args
    assert "--memory=536870912" in args and "--memory-swap=536870912" in args
    assert "--pids-limit=256" in args
    assert args[args.index("--ulimit") + 1] == "nproc=64:64"
    assert "--user=65533:65533" in args
    assert "--tmpfs=/tmp:rw," + ",".join(TMPFS_FLAGS) + ",size=67108864,mode=1777" in args
    assert args[args.index(DEV_IMAGE) + 1 : -2] == ["/usr/bin/python3.12", "-I", "-u", "-c", GATE]


def test_wall_and_argv_bounds_follow_profile(capture_only: FakeDocker, tmp_path: Path):
    content = tmp_path / "content"
    content.mkdir()
    runner = make_runner(catalog().resolve("python", "normal_job", allow_dev=True))
    with pytest.raises(ServiceError, match="invalid_input"):
        runner.run(str(uuid.uuid4()), content, ["-c", "1"], 61, threading.Event(), observe)
    restricted = make_runner(
        runtime_for(profile_with(wall_seconds=20, max_wall_seconds=30, tool_timeout_seconds=45))
    )
    with pytest.raises(ServiceError, match="invalid_input"):
        restricted.run(str(uuid.uuid4()), content, ["-c", "1"], 31, threading.Event(), observe)
    with pytest.raises(ServiceError, match="invalid_input"):
        restricted.run(str(uuid.uuid4()), content, ["a"] * 33, 5, threading.Event(), observe)
    with pytest.raises(ServiceError, match="invalid_input"):
        restricted.run(str(uuid.uuid4()), content, ["x\0"], 5, threading.Event(), observe)
    assert capture_only.create_args is None  # 全部在校验阶段拒绝，未触达docker create


def test_runtime_image_binding_and_legacy_image_validation(fake_docker: FakeDocker):
    with pytest.raises(ServiceError, match="sandbox_config_invalid"):
        make_runner(runtime_for(profile_with(), image="sha256:" + "a" * 64))
    with pytest.raises(ServiceError, match="sandbox_config_invalid"):
        RunscRunner("python:latest", str(uuid.uuid4()))
    with pytest.raises(ServiceError, match="sandbox_config_invalid"):
        RunscRunner("docker.io/library/python:3.12@sha256:" + "a" * 64, str(uuid.uuid4()))
    assert fake_docker.calls == []  # 镜像校验先于任何docker调用


def test_node_runtime_builds_a_fixed_gate_without_shell_controls(
    capture_only: FakeDocker, tmp_path: Path
):
    content = tmp_path / "content"
    content.mkdir()
    runner = make_runner(node_runtime())
    runner.run(
        str(uuid.uuid4()), content, ["--eval", "console.log(1)"], 5,
        threading.Event(), observe,
    )
    args = capture_only.create_args
    assert args is not None
    image_index = args.index(DEV_IMAGE)
    gate = args[image_index + 3]
    assert args[image_index + 1 : image_index + 5] == [
        "/usr/local/bin/node", "-e", gate, "--"
    ]
    assert "--disallow-code-generation-from-strings" in gate
    assert "--max-old-space-size=512" in gate
    assert "KRIM_GATE_READY\\n" in gate
    assert "KRIM_PAYLOAD_START\\n" in gate
    assert "shell:false" in gate
    assert args[-2:] == ["--eval", "console.log(1)"]
    assert "--network=none" in args and "--runtime=runsc" in args


def test_cgroup_verification_follows_injected_profile(
    fake_docker: FakeDocker, monkeypatch: pytest.MonkeyPatch
):
    profile = profile_with(
        cpu_cores=2.0,
        memory_bytes=536870912,
        memory_swap_bytes=536870912,
        pids_limit=256,
    )
    runner = make_runner(runtime_for(profile))
    install_cgroup_fs(
        monkeypatch,
        FAKE_PID,
        {
            "cpu.max": "200000 100000",
            "memory.max": "536870912",
            "memory.swap.max": "0",
            "pids.max": "256",
        },
    )
    assert runner._cgroup({"State": {"Pid": FAKE_PID}}) == {
        "cpu.max": "200000 100000",
        "memory.max": "536870912",
        "memory.swap.max": "0",
        "pids.max": "256",
    }
    for broken in [
        {**legacy_values()},  # 1 CPU比例≠cpu_cores=2
        {**legacy_values(), "cpu.max": "200000 100000", "pids.max": "256"},  # memory漂移
        {**legacy_values(), "cpu.max": "200000 100000", "memory.max": "536870912"},  # pids漂移
        {**legacy_values(), "cpu.max": "200000 100000", "memory.max": "536870912",
         "pids.max": "256", "memory.swap.max": "268435456"},  # swap未禁用
        {**legacy_values(), "memory.max": "536870912", "pids.max": "256",
         "cpu.max": "max 100000"},  # 配额缺失
        {**legacy_values(), "memory.max": "536870912", "pids.max": "256",
         "cpu.max": "max"},  # 残缺格式同样fail-closed
        {**legacy_values(), "memory.max": "536870912", "pids.max": "256",
         "cpu.max": "100000"},  # 缺period段
    ]:
        install_cgroup_fs(monkeypatch, FAKE_PID, broken)
        with pytest.raises(ServiceError, match="sandbox_limit"):
            runner._cgroup({"State": {"Pid": FAKE_PID}})


def test_cgroup_verification_keeps_legacy_defaults(
    fake_docker: FakeDocker, monkeypatch: pytest.MonkeyPatch
):
    runner = make_runner()  # 两参构造：核对值必须仍是1 CPU/1GiB/禁swap/512 pids
    install_cgroup_fs(monkeypatch, FAKE_PID, legacy_values())
    actual = runner._cgroup({"State": {"Pid": FAKE_PID}})
    assert actual == legacy_values()
    install_cgroup_fs(monkeypatch, FAKE_PID, {**legacy_values(), "pids.max": "256"})
    with pytest.raises(ServiceError, match="sandbox_limit"):
        runner._cgroup({"State": {"Pid": FAKE_PID}})
    for pid in [0, -1, "5", True]:
        with pytest.raises(ServiceError, match="sandbox_setup"):
            runner._cgroup({"State": {"Pid": pid}})


def test_runtime_fingerprint_binds_profile_beyond_image(fake_docker: FakeDocker):
    from krim_agent.contracts import fingerprint as digest

    legacy_digest = digest({"profile": LEGACY_PYTHON_PROFILE.model_dump(mode="json")})
    legacy = make_runner()
    assert legacy.runtime_fingerprint == legacy_digest
    assert make_runner().runtime_fingerprint == legacy_digest  # 与构造次数无关
    resolved = catalog().resolve("python", "normal_job", allow_dev=True)
    injected = make_runner(resolved)
    assert injected.runtime_fingerprint == resolved.fingerprint
    assert injected.runtime_fingerprint != legacy_digest
    # 同一镜像、不同资源规格必须产生不同指纹：请求hash据此绑定profile。
    retuned = make_runner(
        runtime_for(profile_with(cpu_cores=2.0, wall_seconds=10, max_wall_seconds=30))
    )
    assert retuned.runtime_fingerprint != injected.runtime_fingerprint
    assert retuned.image == injected.image  # 仅identity无法区分该差异


@pytest.mark.skipif(sys.platform != "linux", reason="管道select仅linux可测")
def test_gate_handshake_cgroup_and_output_collection(
    fake_docker: FakeDocker, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    content = tmp_path / "content"
    content.mkdir()
    install_cgroup_fs(monkeypatch, FAKE_PID, legacy_values())
    monkeypatch.setattr(
        runner_module.subprocess, "Popen", GateStart(fake_docker, MINI_GATE, ["x", "5"])
    )
    runner = make_runner(catalog().resolve("python", "normal_job", allow_dev=True))
    phases: list[tuple[str, dict[str, Any]]] = []

    def record(name: str, data: dict[str, Any]) -> None:
        phases.append((name, data))

    operation = str(uuid.uuid4())
    receipt = runner.run(operation, content, ["-c", "print('x')"], 5, threading.Event(), record)
    assert [name for name, _ in phases] == ["creating", "created", "gate_verified", "started"]
    assert dict(phases)["gate_verified"]["cgroup"] == legacy_values()
    assert receipt["executor_started"] is True
    assert receipt["exit_code"] == 0 and receipt["code"] is None
    assert receipt["output_complete"] is True and receipt["cleanup"] == "clean"
    assert base64.b64decode(receipt["stdout"]) == b"x" * 5
    assert receipt["image"] == IDENTITY and receipt["container"] == runner.name(operation)


@pytest.mark.skipif(sys.platform != "linux", reason="管道select仅linux可测")
def test_output_limit_follows_injected_profile(
    fake_docker: FakeDocker, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    content = tmp_path / "content"
    content.mkdir()
    install_cgroup_fs(monkeypatch, FAKE_PID, legacy_values())
    monkeypatch.setattr(
        runner_module.subprocess, "Popen", GateStart(fake_docker, MINI_GATE, ["x", "5000"])
    )
    runner = make_runner(runtime_for(profile_with(output_limit_bytes=4096)))
    receipt = runner.run(str(uuid.uuid4()), content, ["-c", "1"], 5, threading.Event(), observe)
    assert receipt["code"] == "output_limit" and receipt["output_complete"] is False
    assert receipt["executor_started"] is True and receipt["cleanup"] == "clean"
    assert len(base64.b64decode(receipt["stdout"])) == 4096


@pytest.mark.skipif(sys.platform != "linux", reason="管道select仅linux可测")
def test_deadline_uses_profile_supervisor_grace(
    fake_docker: FakeDocker, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    content = tmp_path / "content"
    content.mkdir()
    monkeypatch.setattr(runner_module.subprocess, "Popen", GateStart(fake_docker, SILENT_GATE, []))
    # grace=1：runner侧deadline=wall+grace=2秒；历史固定min(wall+15,75)需16秒。
    profile = profile_with(
        wall_seconds=1, max_wall_seconds=1, supervisor_grace_seconds=1, tool_timeout_seconds=30
    )
    runner = make_runner(runtime_for(profile))
    started_at = time.monotonic()
    receipt = runner.run(str(uuid.uuid4()), content, ["-c", "1"], 1, threading.Event(), observe)
    elapsed = time.monotonic() - started_at
    assert receipt["code"] == "run_timeout" and receipt["executor_started"] is False
    assert receipt["exit_code"] is None and receipt["cleanup"] == "clean"
    assert elapsed < 8


def test_cancelled_before_create_never_touches_docker(fake_docker: FakeDocker, tmp_path: Path):
    content = tmp_path / "content"
    content.mkdir()
    runner = make_runner(catalog().resolve("python", "normal_job", allow_dev=True))
    cancel = threading.Event()
    cancel.set()
    receipt = runner.run(str(uuid.uuid4()), content, ["-c", "1"], 5, cancel, observe)
    assert receipt["code"] == "cancelled" and receipt["cleanup"] == "clean"
    assert fake_docker.create_args is None
