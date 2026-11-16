"""Small, fixed Docker/runsc control surface; no command/job management here."""
from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from typing import Any, Protocol

from krim_agent.project_access.process_bootstrap import secret_wait_bootstrap

from .bootstrap import CWD_EXEC
from .model import EnvironmentError, RuntimeSpec

LABEL = "io.krim.development."
USER = "65532:65532"
HOME = "/home/krim"


@dataclass(frozen=True)
class Reply:
    code: int
    stdout: str
    stderr: str = ""


class Runner(Protocol):
    prefix: tuple[str, ...]

    async def run(self, args: tuple[str, ...]) -> Reply: ...


class DockerCLI:
    """Local Unix socket only; ignore caller Docker contexts and proxy variables."""

    prefix: tuple[str, ...] = ("/usr/bin/docker", "--host=unix:///var/run/docker.sock")

    async def run(self, args: tuple[str, ...]) -> Reply:
        # Include process creation in the cancellation fence: cancelling during
        # spawn can otherwise orphan a CLI that already submitted a mutation.
        task = asyncio.create_task(self._run(args))
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
            except BaseException:
                break
        if cancelled:
            if not task.cancelled():
                task.exception()
            raise asyncio.CancelledError
        return task.result()

    async def _run(self, args: tuple[str, ...]) -> Reply:
        process = await asyncio.create_subprocess_exec(
            *self.prefix, *args, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={"PATH": "/usr/bin:/bin", "HOME": "/nonexistent", "LC_ALL": "C"},
        )
        async def finish() -> tuple[bytes, bytes]:
            size = 0

            async def read(stream: asyncio.StreamReader | None) -> bytes:
                nonlocal size
                assert stream is not None
                chunks = []
                while chunk := await stream.read(65536):
                    size += len(chunk)
                    if size > 4 * 1024 * 1024:
                        raise EnvironmentError("daemon_reply_too_large")
                    chunks.append(chunk)
                return b"".join(chunks)

            readers = [asyncio.create_task(read(process.stdout)),
                       asyncio.create_task(read(process.stderr))]
            try:
                async with asyncio.timeout(45):
                    stdout, stderr = await asyncio.gather(*readers)
                    await process.wait()
                    return stdout, stderr
            except BaseException:
                if process.returncode is None:
                    process.kill()
                await process.wait()
                raise
            finally:
                for reader in readers:
                    if not reader.done():
                        reader.cancel()
                await asyncio.gather(*readers, return_exceptions=True)

        stdout, stderr = await finish()
        return Reply(process.returncode or 0, stdout.decode("utf-8", "strict"),
                     stderr.decode("utf-8", "replace"))


def tmpfs(record: RuntimeSpec) -> dict[str, str]:
    return {"/tmp": f"rw,nosuid,nodev,size={record.limits.tmp_bytes},mode=1777",
            HOME: (f"rw,nosuid,nodev,size={record.limits.home_bytes},"
                   "uid=65532,gid=65532,mode=700")}


def labels(record: RuntimeSpec) -> dict[str, str]:
    h = record.handle
    result = {LABEL + "owner": h.owner_id, LABEL + "project": h.project_id,
              LABEL + "environment": h.environment_id, LABEL + "generation": str(h.generation),
              LABEL + "fingerprint": record.fingerprint,
              LABEL + "kind": "execution" if record.request is not None else "environment"}
    if record.request is not None:
        result[LABEL + "process"] = str(record.process_id)
        result[LABEL + "operation"] = record.request.operation_id
    return result


def command(record: RuntimeSpec) -> tuple[str, list[str]]:
    if record.request is None:
        return "/bin/sleep", ["infinity"]
    network_wait = ""
    if record.network.policy_ref is not None:
        from krim_agent.dev_network.shim import WAIT_CODE
        network_wait = WAIT_CODE
    if record.request.authorization.secret_refs:
        assert record.process_id is not None
        bootstrap = secret_wait_bootstrap(
            record.request, record.process_id, CWD_EXEC, record.request.timeout_seconds
        )
        args = ["-I", "-c", network_wait + bootstrap,
                record.request.cwd, record.request.command]
        # Keep the original request timeout as PID 1. The reader then applies
        # a second, tighter deadline for the remaining authorization lifetime.
        if record.request.timeout_seconds is None:
            return "/usr/bin/python3", args
        return "/usr/bin/timeout", ["--foreground", "--signal=KILL",
                                   f"{record.request.timeout_seconds:.9f}s",
                                   "/usr/bin/python3", *args]
    bootstrap = network_wait + CWD_EXEC
    args = ["-I", "-c", bootstrap, record.request.cwd, record.request.command]
    if record.request.timeout_seconds is None:
        return "/usr/bin/python3", args
    # Existing coreutils timeout is PID 1, so its exit tears down the whole
    # execution namespace even for descendants that escaped the shell group.
    # Foreground preserves PTY input; container teardown supplies tree cleanup.
    return "/usr/bin/timeout", ["--foreground", "--signal=KILL",
                               f"{record.request.timeout_seconds:.9f}s",
                               "/usr/bin/python3", *args]


def create_args(record: RuntimeSpec) -> tuple[str, ...]:
    limits = record.limits
    entrypoint, cmd = command(record)
    args = ["create", "--name", record.name, "--pull=never", "--runtime=runsc",
            "--user=" + USER, "--cap-drop=ALL", "--security-opt=no-new-privileges",
            "--read-only", "--restart=no", "--init=false", "--log-driver=none",
            f"--memory={limits.memory_bytes}", f"--memory-swap={limits.memory_bytes}",
            f"--cpus={limits.nano_cpus / 1_000_000_000:.9f}", f"--pids-limit={limits.pids}",
            f"--shm-size={limits.shm_bytes}", "--ulimit=nofile=1024:4096",
            "--network=none",
            "--mount", (f"type=bind,source={record.mount.path},target=/workspace,"
                        "bind-propagation=rprivate,bind-recursive=disabled"),
            "--env=HOME=" + HOME, "--env=npm_config_cache=/workspace/.cache/npm",
            "--env=PIP_CACHE_DIR=/workspace/.cache/pip", "--workdir=/workspace",
            "--entrypoint=" + entrypoint, "--no-healthcheck"]
    for key, value in network_env(record).items():
        args += ["--env=" + key + "=" + value]
    if record.request is not None:
        args += ["--interactive"]
        if record.request.pty:
            args += ["--tty"]
    for key, value in tmpfs(record).items():
        args += ["--tmpfs", f"{key}:{value}"]
    for key, value in labels(record).items():
        args += ["--label", f"{key}={value}"]
    return tuple([*args, record.image_id, *cmd])


async def checked(runner: Runner, args: tuple[str, ...]) -> str:
    reply = await runner.run(args)
    if reply.code:
        raise EnvironmentError("docker_" + args[0] + "_failed")
    return reply.stdout.strip()


async def inspect(runner: Runner, record: RuntimeSpec) -> dict[str, Any] | None:
    target = record.container_id or record.name
    reply = await runner.run(("container", "inspect", target))
    if reply.code:
        if ("No such container:" in reply.stderr or "No such object:" in reply.stderr):
            return None
        raise EnvironmentError("docker_inspect_failed")
    try:
        entries = json.loads(reply.stdout)
        if not isinstance(entries, list) or len(entries) != 1 or not isinstance(entries[0], dict):
            raise ValueError
        return entries[0]
    except (ValueError, TypeError):
        raise EnvironmentError("docker_inspect_invalid") from None


async def check_image(runner: Runner, record: RuntimeSpec) -> None:
    reply = await checked(runner, ("image", "inspect", record.image_id))
    try:
        image, = json.loads(reply)
        if image["Id"] != record.image_id or image["Config"].get("Volumes"):
            raise ValueError
    except (ValueError, KeyError, TypeError):
        raise EnvironmentError("image_identity_drift") from None


async def check_network(runner: Runner, record: RuntimeSpec) -> None:
    del runner
    if record.network.network_id is not None:
        raise EnvironmentError("uncontrolled_network_forbidden")


def network_env(record: RuntimeSpec) -> dict[str, str]:
    if record.network.policy_ref is None:
        return {}
    from krim_agent.dev_network.shim import CA_PATH, PROXY_PORT
    proxy = f"http://127.0.0.1:{PROXY_PORT}"
    return {"HTTPS_PROXY": proxy, "https_proxy": proxy,
            "HTTP_PROXY": proxy, "http_proxy": proxy,
            "NO_PROXY": "", "no_proxy": "",
            "NODE_EXTRA_CA_CERTS": CA_PATH, "SSL_CERT_FILE": CA_PATH,
            "REQUESTS_CA_BUNDLE": CA_PATH, "CURL_CA_BUNDLE": CA_PATH,
            "PIP_CERT": CA_PATH, "npm_config_cafile": CA_PATH,
            "npm_config_proxy": proxy, "npm_config_https_proxy": proxy,
            "npm_config_audit": "false", "npm_config_fund": "false"}


async def stop_verified(runner: Runner, record: RuntimeSpec, value: dict[str, Any]) -> str:
    state = verify(record, value)
    cid = str(value["Id"])
    if value["State"].get("Running") or value["State"].get("Paused"):
        if value["State"].get("Paused"):
            await checked(runner, ("unpause", cid))
        await checked(runner, ("stop", "--time=1", cid))
        current = await inspect(runner, record)
        if current is None:
            return "unknown"
        state = verify(record, current)
        if state != "stopped":
            raise EnvironmentError("execution_stop_incomplete")
    return state


def verify(record: RuntimeSpec, value: dict[str, Any]) -> str:
    """Reject drift before every reuse, including recovered/crash-left containers."""
    try:
        config, host = value["Config"], value["HostConfig"]
        entrypoint, cmd = command(record)
        cid = value["Id"]
        if (not isinstance(cid, str) or not re.fullmatch(r"[0-9a-f]{64}", cid)
                or (record.container_id is not None and record.container_id != cid)
                or value["Name"] != "/" + record.name or value["Image"] != record.image_id
                or config["Image"] != record.image_id
                or any(config.get("Labels", {}).get(k) != v for k, v in labels(record).items())
                or config["User"] != USER or config["WorkingDir"] != "/workspace"
                or config.get("Entrypoint") != [entrypoint] or config.get("Cmd") != cmd
                or value["Path"] != entrypoint or value["Args"] != cmd
                or config.get("Healthcheck") not in (None, {"Test": ["NONE"]})
                or config.get("Volumes")
                or bool(config.get("OpenStdin")) != (record.request is not None)
                or bool(config.get("Tty")) != (record.request is not None and record.request.pty)
                or host["Runtime"] != "runsc" or not host["ReadonlyRootfs"]
                or host.get("Privileged") or host.get("CapAdd")
                or set(host.get("CapDrop") or ()) != {"ALL"}
                or set(host.get("SecurityOpt") or ()) != {"no-new-privileges"}
                or host.get("PidMode") or host.get("IpcMode") not in {"private", ""}
                or host.get("UTSMode") or host.get("UsernsMode")
                or host.get("CgroupnsMode") == "host"
                or host.get("Devices") or host.get("DeviceRequests")
                or host.get("DeviceCgroupRules")
                or host.get("Binds") or host.get("VolumesFrom") or host.get("Links")
                or host.get("PublishAllPorts") or host.get("PortBindings")
                or host.get("ExtraHosts") or host.get("Dns") or host.get("DnsSearch")
                or host.get("DnsOptions") or host.get("Sysctls")
                or host.get("RestartPolicy", {}).get("Name") != "no"
                or host.get("LogConfig", {}).get("Type") != "none"
                or host.get("NetworkMode") != "none"
                or host.get("Memory") != record.limits.memory_bytes
                or host.get("MemorySwap") != record.limits.memory_bytes
                or host.get("NanoCpus") != record.limits.nano_cpus
                or host.get("PidsLimit") != record.limits.pids
                or host.get("ShmSize") != record.limits.shm_bytes
                or host.get("Ulimits") != [{"Name": "nofile", "Soft": 1024, "Hard": 4096}]
                or host.get("OomKillDisable") or host.get("Init")
                or host.get("Tmpfs") != tmpfs(record)):
            raise ValueError
        environment = dict(item.split("=", 1) for item in config["Env"])
        if (environment.get("HOME") != HOME
                or environment.get("npm_config_cache") != "/workspace/.cache/npm"
                or environment.get("PIP_CACHE_DIR") != "/workspace/.cache/pip"
                or any(environment.get(k) != v for k, v in network_env(record).items())):
            raise ValueError
        # No image volumes, second project, host socket, recursive submount or port publish.
        mounts = value["Mounts"]
        if len(mounts) != 1:
            raise ValueError
        mount = mounts[0]
        if (mount["Type"] != "bind" or mount["Source"] != str(record.mount.path)
                or mount["Destination"] != "/workspace" or mount["RW"] is not True
                or mount["Propagation"] != "rprivate"):
            raise ValueError
        spec, = host["Mounts"]
        if (spec["Type"] != "bind" or spec["Source"] != str(record.mount.path)
                or spec["Target"] != "/workspace" or spec.get("ReadOnly", False)
                or spec.get("BindOptions", {}).get("Propagation") != "rprivate"
                or spec.get("BindOptions", {}).get("NonRecursive") is not True):
            raise ValueError
        networks = value["NetworkSettings"]["Networks"]
        if len(networks) != 1:
            raise ValueError
        if record.network.network_id is None:
            if set(networks) != {"none"}:
                raise ValueError
        elif next(iter(networks.values()))["NetworkID"] != record.network.network_id:
            raise ValueError
        state = value["State"]
        if state.get("Paused") or state.get("Restarting") or state.get("Dead"):
            return "unknown"
        if state["Running"]:
            return "ready"
        if state["Status"] in {"created", "exited"}:
            return "stopped"
        return "unknown"
    except (ValueError, KeyError, TypeError, AttributeError):
        raise EnvironmentError("container_binding_drift") from None
