#!/usr/bin/env python3
"""验证已安装的 runsc；只使用指定镜像，不安装或拉取，不改全局配置。"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

IMAGE = re.compile(r"(?:sha256:[0-9a-f]{64}|[A-Za-z0-9._/:-]+@sha256:[0-9a-f]{64})\Z")


class Probe:
    def __init__(self, image: str) -> None:
        self.image = image
        self.names: list[str] = []
        self.deadline = time.monotonic() + 170
        self.checks: dict[str, Any] = {}

    def docker(self, *args: str) -> str:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("验证总时限已到")
        result = subprocess.run(
            ["docker", *args], capture_output=True, text=True, timeout=min(30, remaining)
        )
        if result.returncode:
            raise RuntimeError(f"docker {args[0]}: {result.stderr[-2000:]} {result.stdout[-2000:]}")
        return result.stdout.strip()

    def inspect(self, name: str) -> Any:
        return json.loads(self.docker("inspect", name))[0]

    def create(self, code: str, directory: Path | None = None, *, memory_mib: int = 128) -> str:
        name = "krim-probe-" + uuid.uuid4().hex
        self.names.append(name)
        args = [
            "create",
            "--name",
            name,
            "--label",
            "krim.environment-probe=true",
            "--runtime=runsc",
            "--network=none",
            "--read-only",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--user=65532:65532",
            "--tmpfs=/tmp:rw,nosuid,nodev,noexec,size=16m,mode=1777",
            f"--memory={memory_mib}m",
            f"--memory-swap={memory_mib}m",
            "--cpus=0.5",
            "--pids-limit=512",
            "--ulimit",
            "nproc=32:32",
        ]
        if directory is not None:
            args += ["--mount", f"type=bind,src={directory},dst=/workspace"]
        self.docker(*args, self.image, "/usr/bin/python3.12", "-I", "-c", code)
        config = self.inspect(name)["HostConfig"]
        required = {
            "Runtime": "runsc",
            "NetworkMode": "none",
            "ReadonlyRootfs": True,
            "Memory": memory_mib * 1024**2,
            "MemorySwap": memory_mib * 1024**2,
            "NanoCpus": 500_000_000,
            "PidsLimit": 512,
        }
        for key, value in required.items():
            if config[key] != value:
                raise RuntimeError(f"配置不符: {key}")
        if "ALL" not in config["CapDrop"] or "no-new-privileges" not in str(config["SecurityOpt"]):
            raise RuntimeError("缺少 capability/权限限制")
        return name

    def run(self, code: str, directory: Path | None = None, *, memory_mib: int = 128) -> Any:
        name = self.create(code, directory, memory_mib=memory_mib)
        try:
            output = self.docker("start", "--attach", name)
        except RuntimeError as error:
            state = self.inspect(name)["State"]
            raise RuntimeError(f"{error}; actual_state={state}") from error
        state = self.inspect(name)["State"]
        if state["Running"] or state["ExitCode"] != 0:
            raise RuntimeError("测试程序未正常结束: " + str(state))
        return json.loads(output)

    def cleanup(self) -> list[str]:
        errors = []
        for name in self.names:
            try:
                result = subprocess.run(
                    ["docker", "rm", "-f", name],
                    capture_output=True,
                    text=True,
                    timeout=max(0.1, min(10, self.deadline + 10 - time.monotonic())),
                )
                if result.returncode and "No such container" not in result.stderr:
                    errors.append(name + ": " + result.stderr[-500:])
            except (OSError, subprocess.TimeoutExpired) as error:
                errors.append(name + ": " + str(error))
        return errors

    def exercise(self, storage: Path) -> None:
        a, b = storage / "account-a", storage / "account-b"
        if not a.is_dir() or not b.is_dir() or a.is_symlink() or b.is_symlink():
            raise ValueError("缺少独立测试目录，拒绝创建或替换")
        key = "probe-" + uuid.uuid4().hex
        code = r"""
import errno,json,os,pathlib,socket
assert os.getuid()==65532
try:
    pathlib.Path('/workspace/rootfs-write-test').write_text('blocked')
except OSError as e:
    assert e.errno==errno.EROFS
else:
    raise AssertionError('rootfs writable')
tmp=os.statvfs('/tmp')
assert tmp.f_blocks*tmp.f_frsize<=16*1024*1024
assert not pathlib.Path('/var/run/docker.sock').exists()
assert [name for _,name in socket.if_nameindex()]==['lo']
s=socket.socket();s.settimeout(.25)
try:
    s.connect(('1.1.1.1',443))
except OSError:
    pass
else:
    raise AssertionError('external network reachable')
finally:
    s.close()
result={'uid':os.getuid(),'read_only_root':True,'external_network':False}
result['tmpfs_limit_bytes']=tmp.f_blocks*tmp.f_frsize
print(json.dumps(result))
"""
        self.checks["isolation"] = self.run(code)
        try:
            self.run(
                f"import pathlib,json; pathlib.Path('/workspace/{key}').write_text('A'); "
                "print(json.dumps({'written':True}))",
                a,
            )
            result = self.run(
                f"import pathlib,json; assert pathlib.Path('/workspace/{key}').read_text()=='A'; "
                "print(json.dumps({'survived_container_recreation':True}))",
                a,
            )
            self.checks["persistent_directory"] = result
            self.checks["separate_directory"] = self.run(
                f"import pathlib,json; assert not pathlib.Path('/workspace/{key}').exists(); "
                "print(json.dumps({'account_a_file_visible_in_b':False}))",
                b,
            )
        finally:
            (a / key).unlink(missing_ok=True)
        self.process_limit()
        self.stop_and_cgroup()

    def process_limit(self) -> None:
        code = r"""
import errno,json,resource,subprocess,sys
children=[]; limited=False
try:
    assert resource.getrlimit(resource.RLIMIT_NPROC)==(32,32)
    for i in range(40):
        try:
            command=[sys.executable,'-I','-c','import time;time.sleep(10)']
            children.append(subprocess.Popen(command))
        except OSError as e:
            assert e.errno==errno.EAGAIN
            limited=True;break
    assert limited, 'Application task limit did not stop 40 children'
    print(json.dumps({'application_limit_enforced':limited,'created_children':len(children)}))
finally:
    for child in children: child.terminate()
    for child in children:
        try: child.wait(timeout=2)
        except subprocess.TimeoutExpired: child.kill();child.wait(timeout=2)
"""
        self.checks["application_process_limit"] = self.run(code, memory_mib=512)

    def stop_and_cgroup(self) -> None:
        name = self.create("import time; time.sleep(90)")
        self.docker("start", name)
        state = self.inspect(name)["State"]
        if not state["Running"]:
            raise RuntimeError("测试容器未运行")
        membership = Path(f"/proc/{state['Pid']}/cgroup").read_text()
        path = next(row[3:] for row in membership.splitlines() if row.startswith("0::"))
        cgroup = Path("/sys/fs/cgroup") / path.lstrip("/")
        actual = {
            field: (cgroup / field).read_text().strip()
            for field in ("cpu.max", "memory.max", "memory.swap.max", "pids.max")
        }
        if actual["memory.max"] != str(128 * 1024**2) or actual["memory.swap.max"] != "0":
            raise RuntimeError("实际内存/swap限额不符: " + str(actual))
        if actual["pids.max"] != "512":
            raise RuntimeError("实际宿主任务数限额不符: " + str(actual))
        quota, period = actual["cpu.max"].split()
        if quota == "max" or int(quota) / int(period) != 0.5:
            raise RuntimeError("实际CPU限额不符: " + str(actual))
        self.checks["host_cgroup"] = actual
        self.docker("stop", "--time", "2", name)
        stopped = self.inspect(name)["State"]
        if stopped["Running"] or stopped["Pid"] != 0:
            raise RuntimeError("容器停止未完成")
        self.checks["stop"] = {"running": stopped["Running"], "pid": stopped["Pid"]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--storage", type=Path, default=Path("/srv/krim-sandbox"))
    parser.add_argument("--exercise", action="store_true")
    args = parser.parse_args()
    if not IMAGE.fullmatch(args.image):
        parser.error("镜像必须为固定 repo@sha256 或本地 sha256 image ID")
    probe = Probe(args.image)
    report: dict[str, Any] = {"image": args.image, "passed": False, "checks": probe.checks}
    try:
        info = json.loads(probe.docker("info", "--format", "{{json .}}"))
        if "runsc" not in info["Runtimes"] or info["CgroupVersion"] != "2":
            raise RuntimeError("缺少runsc或cgroup v2")
        probe.docker("image", "inspect", args.image)
        report["preflight"] = {
            "docker_root": info["DockerRootDir"],
            "cgroup_version": info["CgroupVersion"],
        }
        if args.exercise:
            probe.exercise(args.storage)
        report["passed"] = True
    except (
        OSError,
        ValueError,
        RuntimeError,
        TimeoutError,
        StopIteration,
        subprocess.TimeoutExpired,
    ) as error:
        report["error"] = str(error)
    finally:
        cleanup = probe.cleanup()
        report["cleanup_errors"] = cleanup
        if cleanup:
            report["passed"] = False
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
