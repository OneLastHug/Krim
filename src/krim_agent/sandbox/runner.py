"""固定 runsc 容器执行；先握手核对宿主限制，再放行用户程序。

docker创建参数、容器入口与cgroup核对值一律由注入的ResolvedRuntime规格生成；
两参构造保持历史python默认参数（LEGACY_PYTHON_PROFILE，逐值等于profile化
之前的固定参数）。node监管门尚未实现：注入node runtime在构造时即拒绝，
不编造镜像、不静默联网构建。
"""

from __future__ import annotations

import base64
import json
import os
import re
import selectors
import subprocess
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.sandbox.runtime_profiles import (
    LEGACY_PYTHON_PROFILE,
    TMPFS_FLAGS,
    ResolvedRuntime,
)
from krim_agent.sandbox.xfs import internal_id, private_directory

IMAGE = re.compile(r"(?:sha256:[0-9a-f]{64}|[A-Za-z0-9._/:-]+@sha256:[0-9a-f]{64})\Z")
# LEGACY_PYTHON_PROFILE的输出上限；运行时以profile.output_limit_bytes为准。
OUTPUT_LIMIT = 32768
GATE = r"""
import os,selectors,signal,subprocess,sys,time
newline=bytes([10])
os.write(1,b'KRIM_GATE_READY'+newline)
if sys.stdin.buffer.readline(8)!=b'go'+newline:sys.exit(125)
child=subprocess.Popen([sys.executable,'-I',*sys.argv[1:]],stdin=subprocess.DEVNULL,
                       stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                       env={'PATH':'/usr/bin:/bin','HOME':'/tmp','PYTHONNOUSERSITE':'1'},
                       start_new_session=True)
os.write(1,b'KRIM_PAYLOAD_START'+newline)
def stop(signum,frame):
    try:os.killpg(child.pid,signal.SIGTERM)
    except ProcessLookupError:pass
signal.signal(signal.SIGTERM,stop)
signal.signal(signal.SIGINT,stop)
deadline=time.monotonic()+float(os.environ['KRIM_JOB_WALL'])
with selectors.DefaultSelector() as channels:
    channels.register(child.stdout,selectors.EVENT_READ,1)
    channels.register(child.stderr,selectors.EVENT_READ,2)
    while channels.get_map():
        if time.monotonic()>=deadline:
            try:os.killpg(child.pid,signal.SIGKILL)
            except ProcessLookupError:pass
            child.wait();sys.exit(124)
        for event,_ in channels.select(.1):
            body=os.read(event.fd,8192)
            if body:os.write(event.data,body)
            else:channels.unregister(event.fileobj)
code=child.wait()
sys.exit(code if code>=0 else 128-code)
"""


class RunscRunner:
    def __init__(
        self, image: str, installation_id: str, runtime: ResolvedRuntime | None = None
    ) -> None:
        if not IMAGE.fullmatch(image):
            raise ServiceError("sandbox_config_invalid", 422)
        if runtime is not None:
            if runtime.image != image:
                raise ServiceError("sandbox_config_invalid", 422)
            if runtime.profile.program != "python":
                # node监管门未接线：拒绝注入，等待专用node门实现后开放。
                raise ServiceError("sandbox_backend_unavailable", 503)
        self.runtime = runtime
        self.profile = runtime.profile if runtime is not None else LEGACY_PYTHON_PROFILE
        # 请求指纹绑定运行规格而非仅镜像：注入runtime用其fingerprint（镜像+规格），
        # 两参构造用LEGACY完整profile摘要。identity保持原有"本机镜像Id核实"含义。
        self.runtime_fingerprint = (
            runtime.fingerprint
            if runtime is not None
            else fingerprint({"profile": LEGACY_PYTHON_PROFILE.model_dump(mode="json")})
        )
        self.image = image
        self.installation_id = internal_id(installation_id)
        self.environment = {"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "HOME": "/nonexistent"}
        self.identity = self.docker("image", "inspect", image)[0]["Id"]
        info = self.docker("info", "--format", "{{json .}}")
        if "runsc" not in info["Runtimes"] or info["CgroupVersion"] != "2":
            raise ServiceError("sandbox_backend_unavailable", 503)

    @staticmethod
    def docker(*arguments: str) -> Any:
        env = {"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "HOME": "/nonexistent"}
        try:
            result = subprocess.run(
                ["/usr/bin/docker", *arguments], env=env, capture_output=True, text=True, timeout=12
            )
            if result.returncode:
                raise ServiceError("sandbox_backend_unavailable", 503)
            return json.loads(result.stdout) if result.stdout.strip() else None
        except (OSError, ValueError, subprocess.TimeoutExpired):
            raise ServiceError("sandbox_backend_unavailable", 503) from None

    def name(self, operation_id: str) -> str:
        internal_id(operation_id)
        return (
            "krim-job-"
            + self.installation_id.replace("-", "")[:12]
            + "-"
            + operation_id.replace("-", "")
        )

    def inspect(self, name: str, operation_id: str) -> dict[str, Any]:
        value: dict[str, Any] = self.docker("inspect", name)[0]
        labels = value["Config"]["Labels"]
        if (
            labels.get("krim.installation") != self.installation_id
            or labels.get("krim.operation") != operation_id
            or value["Image"] != self.identity
        ):
            raise ServiceError("target_binding_changed")
        return value

    def stop(self, operation_id: str) -> dict[str, Any]:
        name = self.name(operation_id)
        value = self.inspect(name, operation_id)
        if value["State"]["Running"]:
            result = subprocess.run(
                ["/usr/bin/docker", "stop", "--time", "2", name],
                env=self.environment,
                capture_output=True,
                timeout=8,
            )
            if result.returncode:
                subprocess.run(
                    ["/usr/bin/docker", "kill", name],
                    env=self.environment,
                    capture_output=True,
                    timeout=5,
                )
        value = self.inspect(name, operation_id)
        if value["State"]["Running"] or value["State"]["Pid"] != 0:
            raise ServiceError("sandbox_cleanup_failed")
        return value

    def remove(self, operation_id: str) -> None:
        value = self.inspect(self.name(operation_id), operation_id)
        if value["State"]["Running"] or value["State"]["Pid"] != 0:
            raise ServiceError("sandbox_cleanup_required")
        result = subprocess.run(
            ["/usr/bin/docker", "rm", self.name(operation_id)],
            env=self.environment,
            capture_output=True,
            timeout=8,
        )
        if result.returncode:
            raise ServiceError("sandbox_cleanup_failed")

    def _cgroup(self, value: dict[str, Any]) -> dict[str, str]:
        pid = value["State"]["Pid"]
        if type(pid) is not int or pid <= 0:
            raise ServiceError("sandbox_setup")
        lines = Path(f"/proc/{pid}/cgroup").read_text().splitlines()
        group = next(line[3:] for line in lines if line.startswith("0::"))
        root = Path("/sys/fs/cgroup") / group.lstrip("/")
        actual = {
            key: (root / key).read_text().strip()
            for key in ("cpu.max", "memory.max", "memory.swap.max", "pids.max")
        }
        quota_period = actual["cpu.max"].split()
        profile = self.profile
        # memory_swap_bytes==memory_bytes由模型强制，因此swap上限恒为0。
        # cpu.max无配额（"max ..."/格式缺失）一律按限额漂移处理，失败即关闭。
        if (
            len(quota_period) != 2
            or quota_period[0] == "max"
            or abs(int(quota_period[0]) / int(quota_period[1]) - profile.cpu_cores) > 1e-9
            or actual["memory.max"] != str(profile.memory_bytes)
            or actual["memory.swap.max"] != str(profile.memory_swap_bytes - profile.memory_bytes)
            or actual["pids.max"] != str(profile.pids_limit)
        ):
            raise ServiceError("sandbox_limit")
        return actual

    def _create_args(
        self, name: str, operation_id: str, content: Path, argv: list[str], wall: int
    ) -> list[str]:
        """创建参数全部由profile生成；LEGACY_PYTHON_PROFILE逐值等于历史固定参数。

        read_only_root/no_new_privileges为Literal[True]、network为Literal["none"]、
        dropped_capabilities为Literal["ALL"]，安全边界不可经profile放宽。
        """
        profile = self.profile
        args = [
            "/usr/bin/docker",
            "create",
            "--name",
            name,
            "--interactive",
            "--label",
            "krim.installation=" + self.installation_id,
            "--label",
            "krim.operation=" + operation_id,
            "--runtime=runsc",
            "--network=" + profile.network,
            "--read-only",
            "--cap-drop=" + profile.dropped_capabilities,
            "--security-opt=no-new-privileges",
            "--user=" + f"{profile.run_as_uid}:{profile.run_as_gid}",
            "--memory=" + str(profile.memory_bytes),
            "--memory-swap=" + str(profile.memory_swap_bytes),
            "--cpus=" + format(profile.cpu_cores, "g"),
            "--pids-limit=" + str(profile.pids_limit),
            "--ulimit",
            f"nproc={profile.nproc_limit}:{profile.nproc_limit}",
            "--log-driver=none",
        ]
        tmpfs_flags = ",".join(("rw", *TMPFS_FLAGS))
        for mount in profile.tmpfs:
            args.append(
                f"--tmpfs={mount.destination}:{tmpfs_flags},size={mount.size_bytes},"
                f"mode={format(mount.mode, 'o')}"
            )
        args += [
            "--env",
            "KRIM_JOB_WALL=" + str(wall),
            "--workdir=" + profile.workdir,
            "--mount",
            "type=bind,src=" + str(content) + ",dst=" + profile.workdir,
            self.image,
            *profile.gate_entry(GATE),
            *argv,
        ]
        return args

    def run(
        self,
        operation_id: str,
        content: Path,
        argv: list[str],
        wall: int,
        cancel: threading.Event,
        phase: Callable[[str, dict[str, Any]], None],
    ) -> dict[str, Any]:
        private_directory(content)
        profile = self.profile
        if type(wall) is not int or not 1 <= wall <= profile.max_wall_seconds:
            raise ServiceError("invalid_input", 422)
        argv = profile.validate_argv(argv)
        name = self.name(operation_id)
        phase("creating", {"container": name, "image": self.identity})
        args = self._create_args(name, operation_id, content, argv, wall)
        created = False
        start_requested = False
        attach: subprocess.Popen[bytes] | None = None
        stdout, stderr = bytearray(), bytearray()
        observed: dict[str, Any] = {"started": False, "code": None}
        clean = False
        exit_code: int | None = None
        try:
            if cancel.is_set():
                return {
                    "executor_started": False,
                    "exit_code": None,
                    "stdout": "",
                    "stderr": "",
                    "output_complete": True,
                    "cleanup": "clean",
                    "code": "cancelled",
                }
            # 写入creating回执后即视为创建可能发生；失败必须按确定名字核实。
            created = True
            result = subprocess.run(args, env=self.environment, capture_output=True, timeout=15)
            if result.returncode:
                raise ServiceError("sandbox_setup")
            value = self.inspect(name, operation_id)
            host = value["HostConfig"]
            if (
                host["Runtime"] != "runsc"
                or host["NetworkMode"] != "none"
                or not host["ReadonlyRootfs"]
                or value["Config"]["User"] != f"{profile.run_as_uid}:{profile.run_as_gid}"
                or host["Privileged"]
                or host["PidMode"] == "host"
                or "ALL" not in host["CapDrop"]
                or not any(s.startswith("no-new-privileges") for s in host["SecurityOpt"])
            ):
                raise ServiceError("target_binding_changed")
            mounts = value["Mounts"]
            if (
                len(mounts) != 1
                or mounts[0]["Source"] != str(content)
                or mounts[0]["Destination"] != profile.workdir
            ):
                raise ServiceError("target_binding_changed")
            phase("created", {"container": name})
            start_requested = True
            attach = subprocess.Popen(
                ["/usr/bin/docker", "start", "--attach", "--interactive", name],
                env=self.environment,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self._collect(attach, operation_id, wall, cancel, phase, stdout, stderr, observed)
        except (
            ServiceError,
            OSError,
            ValueError,
            StopIteration,
            subprocess.TimeoutExpired,
        ) as error:
            observed["code"] = (
                error.code if isinstance(error, ServiceError) else "execution_unknown"
            )
        finally:
            try:
                if created:
                    state = self.stop(operation_id)["State"]
                    exit_code = state["ExitCode"] if observed["started"] else None
                clean = True
            except (ServiceError, OSError, subprocess.TimeoutExpired):
                if not start_requested:
                    clean = True  # 未发start，可能残留的创建记录不能运行用户代码。
                else:
                    observed["code"] = "sandbox_cleanup_failed"
            if attach is not None:
                if attach.poll() is None:
                    attach.kill()
                attach.communicate(timeout=5)
        return {
            "executor_started": observed["started"],
            "exit_code": exit_code,
            "stdout": base64.b64encode(stdout).decode(),
            "stderr": base64.b64encode(stderr).decode(),
            "output_complete": observed["code"] != "output_limit",
            "cleanup": "clean" if clean else "unknown",
            "code": observed["code"],
            "container": name,
            "image": self.identity,
        }

    def _collect(
        self,
        attach: subprocess.Popen[bytes],
        operation_id: str,
        wall: int,
        cancel: threading.Event,
        phase: Callable[[str, dict[str, Any]], None],
        stdout: bytearray,
        stderr: bytearray,
        observed: dict[str, Any],
    ) -> None:
        assert attach.stdout is not None and attach.stderr is not None and attach.stdin is not None
        pending = bytearray()
        ready = False
        deadline = time.monotonic() + wall + self.profile.supervisor_grace_seconds
        with selectors.DefaultSelector() as streams:
            streams.register(attach.stdout, selectors.EVENT_READ, "out")
            streams.register(attach.stderr, selectors.EVENT_READ, "err")
            while streams.get_map():
                if cancel.is_set():
                    observed["code"] = "cancelled"
                    break
                if time.monotonic() >= deadline:
                    observed["code"] = "run_timeout"
                    break
                for event, _ in streams.select(0.1):
                    chunk = os.read(event.fd, 8192)
                    if not chunk:
                        streams.unregister(event.fileobj)
                        continue
                    target = stdout if event.data == "out" else stderr
                    if event.data == "out" and not observed["started"]:
                        pending.extend(chunk)
                        while b"\n" in pending and not observed["started"]:
                            line, _, rest = pending.partition(b"\n")
                            pending[:] = rest
                            if not ready and line == b"KRIM_GATE_READY":
                                actual = self._cgroup(
                                    self.inspect(self.name(operation_id), operation_id)
                                )
                                phase("gate_verified", {"cgroup": actual})
                                if cancel.is_set():
                                    observed["code"] = "cancelled"
                                    return
                                attach.stdin.write(b"go\n")
                                attach.stdin.flush()
                                ready = True
                                deadline = time.monotonic() + wall
                            elif ready and line == b"KRIM_PAYLOAD_START":
                                observed["started"] = True
                                phase("started", {"executor_started": True})
                                target.extend(pending)
                                pending.clear()
                            else:
                                raise ServiceError("sandbox_setup")
                        if len(pending) > 256:
                            raise ServiceError("sandbox_setup")
                    else:
                        remaining = self.profile.output_limit_bytes - len(stdout) - len(stderr)
                        target.extend(chunk[: max(0, remaining)])
                        if len(chunk) > remaining:
                            observed["code"] = "output_limit"
                            return
        if not observed["started"] and observed["code"] is None:
            observed["code"] = "sandbox_setup"
