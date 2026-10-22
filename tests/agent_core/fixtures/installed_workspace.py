"""FINAL有限安装验收/同包服务driver；产品只从当前隔离安装环境导入。"""

import argparse
import asyncio
import base64
import csv
import hashlib
import importlib.metadata
import io
import json
import os
import re
import signal
import sqlite3
import stat
import sys
import time
import zipfile
from collections.abc import Callable
from contextlib import closing
from dataclasses import asdict
from pathlib import Path
from typing import Any


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    for name in ["database", "evidence", "materials", "root", "wheel", "contract"]:
        parser.add_argument("--" + name, type=Path)
    parser.add_argument("--port", type=int)
    parser.add_argument("--qualify-consumer", action="store_true", default=None)
    args = parser.parse_args()
    options = [word.split("=", 1)[0] for word in sys.argv[1:] if word.startswith("--")]
    if len(options) != len(set(options)):
        parser.error("duplicate option")
    fields = {key for key, value in vars(args).items() if value is not None}
    if fields not in ({"database", "port", "evidence"},
                      {"evidence", "materials", "root", "wheel", "contract"},
                      {"qualify_consumer", "evidence"}):
        parser.error("必须完整选择资格或服务接口，不可混用")
    if fields == {"evidence", "materials", "root", "wheel", "contract"}:
        parser.error("child禁止重跑旧安装验收")
    return args


def raw(path: Path) -> bytes:
    for parent in reversed(path.parents):
        if not stat.S_ISDIR(parent.lstat().st_mode):
            raise ValueError("nonregular parent")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("nonregular input")
        with os.fdopen(os.dup(fd), "rb") as stream:
            return stream.read()
    finally:
        os.close(fd)


def sha(path: Path) -> str:
    return hashlib.sha256(raw(path)).hexdigest()


def write(path: Path, value: Any) -> None:
    with path.open("x") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)


def context() -> tuple[Path, dict[str, Any]]:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in items:
            if key in value:
                raise ValueError("duplicate JSON key")
            value[key] = item
        return value

    root = Path(os.environ["KRIM_M2_FINAL_ROOT"])
    path = Path(os.environ["KRIM_M2_FINAL_GRANT"])
    if root != Path(__file__).resolve().parents[4] or path != root / "release/directory01/grant.json":
        raise ValueError("installation_identity: fixed child context")
    grant = json.loads(raw(path), object_pairs_hook=pairs)
    if sha(path) != raw(path.with_name("grant.sha256")).decode().strip():
        raise ValueError("grant seal")
    if str(root) != grant["acceptance_root"] or grant["source"] != str(root / "source"):
        raise ValueError("root binding")
    for key in grant["sealed_refs"]:
        if sha(Path(grant[key + "_file"])) != grant[key + "_sha256"]:
            raise ValueError("sealed input " + key)
    approval = json.loads(raw(Path(grant["approval_file"])), object_pairs_hook=pairs)
    if approval["verdict"] != "APPROVE" or approval["reviewed_revision"] != (
        grant["reviewed_revision"]
    ):
        raise ValueError("plan approval")
    receipt = str(root / "evidence/final/final-directory01-consumer/CONSUMER-IDENTITY.json")
    if grant["consumer_identity_receipt"] != receipt or grant[
        "installation_identity_receipt"
    ] != receipt:
        raise ValueError("installation_identity: receipt aliases")
    return root, grant


def identity(root: Path, grant: dict[str, Any], wheel: Path, manifest: str,
             command: str) -> dict[str, Any]:
    """先用标准库核验安装bytes，再允许导入产品；不猜site或扫描wheel。"""
    expected = grant["package_set"]
    if sys.flags.isolated != 1 or Path(sys.executable) != root / "installed-env/bin/python":
        raise ValueError("installed interpreter")
    if Path(sys.executable).resolve() != Path(grant["read_only_interpreter"]).resolve():
        raise ValueError("interpreter material")
    if any(str(root / "source") in value for value in sys.path):
        raise ValueError("source injection")
    matches = [value for value in importlib.metadata.distributions()
        if re.sub(r"[-_.]+", "-", value.metadata.get("Name", "")).lower() == "krim-agent"]
    if len(matches) != 1:
        raise ValueError("ambiguous installed package")
    dist = matches[0]
    if dist.version != "0.4.0a1":
        raise ValueError("installed version")
    info = list(dist.files or [])
    metadata_names = [v for v in info if str(v).endswith(".dist-info/METADATA")]
    record_names = [v for v in info if str(v).endswith(".dist-info/RECORD")]
    if len(metadata_names) != 1 or len(record_names) != 1:
        raise ValueError("ambiguous distribution")
    metadata = Path(str(dist.locate_file(metadata_names[0])))
    record = Path(str(dist.locate_file(record_names[0])))
    site = Path(str(dist.locate_file("krim_agent"))).parent
    if not site.is_relative_to(root / "installed-env"):
        raise ValueError("site outside environment")
    actual_site: set[str] = set()

    def visit(path: Path) -> None:
        info = path.lstat()
        if stat.S_ISDIR(info.st_mode):
            for item in path.iterdir():
                visit(item)
        elif stat.S_ISREG(info.st_mode):
            actual_site.add(str(path.relative_to(site)))
        else:
            raise ValueError("nonregular installed package")

    visit(site / "krim_agent")
    if actual_site != {n.removeprefix("src/") for n in expected}:
        raise ValueError("site exact75")
    entries = {row[0]: row[1:] for row in csv.reader(io.StringIO(raw(record).decode()))}
    hashes: dict[str, Any] = {}
    with zipfile.ZipFile(wheel) as archive:
        product = [n for n in archive.namelist() if n.startswith("krim_agent/")]
        if sorted(product) != sorted(n.removeprefix("src/") for n in expected):
            raise ValueError("wheel exact75")
        for name in expected:
            member = name.removeprefix("src/")
            src_hash = sha(root / "source" / name)
            wheel_bytes = archive.read(member)
            wheel_hash = hashlib.sha256(wheel_bytes).hexdigest()
            site_hash = sha(Path(str(dist.locate_file(member))))
            if src_hash != wheel_hash or site_hash != wheel_hash:
                raise ValueError("source wheel site " + name)
            digest = base64.urlsafe_b64encode(bytes.fromhex(site_hash)).decode().rstrip("=")
            if entries.get(member) != ["sha256=" + digest, str(len(wheel_bytes))]:
                raise ValueError("RECORD " + name)
            hashes[name] = {"source_sha256": src_hash, "wheel_sha256": wheel_hash,
                            "site_sha256": site_hash}
    return {"kind": "M2_FINAL_INSTALLED_IDENTITY_V1", "grant_sha256":
        sha(Path(os.environ["KRIM_M2_FINAL_GRANT"])), "acceptance_root": str(root),
        "wheel": {"path": str(wheel), "sha256": sha(wheel)}, "version": dist.version,
        "package_files": hashes, "interpreter": {"path": sys.executable,
        "realpath": str(Path(sys.executable).resolve()),
        "sha256": sha(Path(sys.executable).resolve())}, "site_root": str(site),
        "site_files": {n: value["site_sha256"] for n, value in hashes.items()},
        "metadata": {"path": str(metadata), "sha256": sha(metadata)},
        "record": {"path": str(record), "sha256": sha(record)},
        "source_manifest_sha256": manifest, "validation_command_sha256": command}


def installed_identity(root: Path, grant: dict[str, Any]) -> dict[str, Any]:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in items:
            if key in value:
                raise ValueError("duplicate JSON key")
            value[key] = item
        return value

    def load(path: Path) -> dict[str, Any]:
        value: dict[str, Any] = json.loads(raw(path), object_pairs_hook=pairs)
        if not isinstance(value, dict):
            raise ValueError("identity object")
        return value

    try:
        schema = load(Path(grant["consumer_identity_contract_file"]))
        receipt_path = root / "evidence/final/final-directory01-consumer/CONSUMER-IDENTITY.json"
        if not (str(receipt_path) == grant["consumer_identity_receipt"] ==
                grant["installation_identity_receipt"] == schema["receipt"]):
            raise ValueError("receipt aliases")
        receipt = load(receipt_path)
        if set(receipt) != set(schema["required_fields"]):
            raise ValueError("consumer fields")
        if (receipt["kind"], receipt["scope"]) != (schema["kind"], schema["scope"]) or (
            type(receipt["schema_version"]) is not int or receipt["schema_version"] != 1
        ):
            raise ValueError("consumer kind")
        for name in ("installation", "consumer", "materials", "qualification"):
            if set(receipt[name]) != set(schema[name + "_fields"]):
                raise ValueError("consumer fields: " + name)
        if set(receipt["inherited"]) != set(schema["inherited_keys"]):
            raise ValueError("inherited fields")
        command_path = Path(schema["validation_command"])
        result_path = Path(schema["validation_result"])
        command = load(command_path)
        result = load(result_path)
        complete = load(Path(schema["completion_seal"]))
        if complete != {
            "kind": "CONSUMER_COMMAND_COMPLETED_V1", "consumer_sha256": sha(receipt_path),
            "command_sha256": sha(command_path), "result_sha256": sha(result_path), "exit_code": 0
        }:
            raise ValueError("consumer seal")
        if type(result["exit_code"]) is not int or result["exit_code"] != 0 or (
            command["label"] != schema["qualification_label"] or
            result["product_argv"] != command["product_argv"] or
            result["source_manifest_sha256"] != command["source_manifest_sha256"] or
            result["label"] != command["label"]
        ):
            raise ValueError("consumer result")
        actual = consumer_facts(root, grant)
        for name in ("driver", "e2e"):
            if actual["consumer"][name] != receipt["consumer"][name]:
                raise ValueError("consumer bytes changed: " + name)
        if actual["installation"] != receipt["installation"]:
            raise ValueError("installation bytes changed")
        if actual != receipt:
            raise ValueError("consumer facts changed")
        return receipt
    except (OSError, ValueError, KeyError, StopIteration, TypeError,
            importlib.metadata.PackageNotFoundError, zipfile.BadZipFile) as error:
        raise ValueError("installation_identity: " + str(error)) from None


def consumer_facts(root: Path, grant: dict[str, Any]) -> dict[str, Any]:
    """只读真实安装与消费者事实；不导入产品、不写文件或开启业务资源。"""
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in items:
            if key in value:
                raise ValueError("duplicate JSON key")
            value[key] = item
        return value

    def load(path: Path) -> Any:
        return json.loads(raw(path), object_pairs_hook=pairs)

    def record(path: Path) -> dict[str, Any]:
        data = raw(path)
        info = path.lstat()
        if info.st_uid != 1001 or info.st_nlink != 1:
            raise ValueError("input ownership: " + str(path))
        return {"sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data),
                "source_mode": stat.S_IMODE(info.st_mode)}

    schema = load(Path(grant["consumer_identity_contract_file"]))
    folder = root / "evidence/final/final-directory01-consumer"
    command_path = folder / "command.json"
    command = load(command_path)
    manifest_path = folder / "source-manifest.json"
    driver = root / "source/tests/agent_core/fixtures/installed_workspace.py"
    if Path(__file__).resolve() != driver or command["product_argv"] != [
        str(root / "installed-env/bin/python"), "-I", "-B", str(driver),
        "--qualify-consumer", "--evidence", str(folder)
    ] or (command["kind"], command["label"], command["cwd"]) != (
        "consumer-qualify", schema["qualification_label"], str(folder)
    ):
        raise ValueError("consumer command")
    if command["source_manifest"] != str(manifest_path) or (
        command["source_manifest_sha256"] != sha(manifest_path)
    ):
        raise ValueError("consumer source manifest")
    frozen = load(manifest_path)
    if set(frozen) != set(load(Path(grant["source_manifest_file"]))):
        raise ValueError("source file set")
    for name, expected in frozen.items():
        if record(root / "source" / name) != expected:
            raise ValueError("consumer source changed: " + name)

    inherited = grant["inherited_installation"]
    if set(inherited) != set(schema["inherited_keys"]) or inherited != load(
        Path(grant["inherited_installation_file"])
    ):
        raise ValueError("inherited index")
    for name, value in inherited.items():
        expected = {"sha256": value["sha256"], "bytes": value["bytes"],
                    "source_mode": value["mode"]}
        if record(Path(value["original_path"])) != expected or record(
            Path(value["copy_path"])
        ) != expected:
            raise ValueError("inherited bytes: " + name)
    old = {name: load(Path(inherited[name]["copy_path"])) for name in (
        "grant", "identity_contract", "identity_receipt", "command", "result",
        "validation_closed", "package_manifest", "source_manifest"
    )}
    receipt = old["identity_receipt"]
    if len(receipt) != 13 or set(receipt) != set(old["identity_contract"]["required_fields"]):
        raise ValueError("inherited identity schema")
    if receipt["kind"] != "M2_FINAL_INSTALLED_IDENTITY_V1" or (
        receipt["grant_sha256"] != inherited["grant"]["sha256"] or
        receipt["acceptance_root"] != old["grant"]["acceptance_root"] or
        receipt["validation_command_sha256"] != inherited["command"]["sha256"] or
        receipt["source_manifest_sha256"] != inherited["source_manifest"]["sha256"] or
        old["result"]["exit_code"] != 0 or old["command"]["label"] !=
        old["identity_contract"]["installed_label"] or
        old["result"]["product_argv"] != old["command"]["product_argv"] or
        old["validation_closed"]["success"] is not True or
        old["validation_closed"]["FD_equal"] is not True or
        old["validation_closed"]["module_consumers"] != receipt["site_files"]
    ):
        raise ValueError("inherited installation result")
    old_argv = old["command"]["product_argv"]
    if old_argv[old_argv.index("--wheel") + 1] != inherited["wheel"]["original_path"] or (
        receipt["wheel"]["sha256"] != inherited["wheel"]["sha256"] or
        old["command"]["source_manifest_sha256"] != inherited["source_manifest"]["sha256"]
    ):
        raise ValueError("inherited command binding")
    if grant["package_set"] != old["package_manifest"] or len(set(grant["package_set"])) != 75:
        raise ValueError("package exact75")

    ready = load(Path(grant["ready_file"]))
    expected_python = ready["interpreter"]
    if sys.flags.isolated != 1 or sys.prefix != str(root / "installed-env") or (
        sys.base_prefix != expected_python["base_prefix"] or
        sys.path != expected_python["sys_path"] or
        sys.executable != expected_python["executable"] or
        "krim_agent" in sys.modules
    ):
        raise ValueError("isolated installation context")
    copy = load(Path(grant["copy_manifest_file"]))
    directories = load(Path(grant["directory_manifest_file"]))
    observed: set[str] = set()

    def visit_environment(path: Path) -> None:
        name = str(path.relative_to(root))
        observed.add(name)
        info = path.lstat()
        if stat.S_ISDIR(info.st_mode):
            if name not in directories or stat.S_IMODE(info.st_mode) != directories[name]["mode"]:
                raise ValueError("environment directory: " + name)
            for child_path in path.iterdir():
                visit_environment(child_path)
        elif stat.S_ISLNK(info.st_mode):
            item = copy.get(name, {})
            if item.get("type") != "symlink" or os.readlink(path) != item["target"] or (
                stat.S_IMODE(info.st_mode) != item["mode"]
            ):
                raise ValueError("environment link: " + name)
        elif stat.S_ISREG(info.st_mode):
            item = copy.get(name, {})
            if record(path) != {key: item.get(key) for key in ("sha256", "bytes", "source_mode")}:
                raise ValueError("environment bytes: " + name)
        else:
            raise ValueError("nonregular environment: " + name)

    visit_environment(root / "installed-env")
    expected_environment = {n for n in copy if n.startswith("installed-env/")} | {
        n for n in directories if n == "installed-env" or n.startswith("installed-env/")
    }
    if observed != expected_environment:
        raise ValueError("environment file set")
    for name, item in ready["startup_files"].items():
        if sha(Path(name)) != item["sha256"]:
            raise ValueError("startup bytes")

    wheel = Path(inherited["wheel"]["copy_path"])
    installed = identity(root, grant, wheel, sha(manifest_path), sha(command_path))
    hashes = installed["package_files"]
    site = Path(installed["site_root"])
    with zipfile.ZipFile(wheel) as archive:
        for name, value in hashes.items():
            if value != receipt["package_files"][name]:
                raise ValueError("inherited package bytes: " + name)
            member = name.removeprefix("src/")
            value.update(source_mode=stat.S_IMODE((root / "source" / name).lstat().st_mode),
                site_mode=stat.S_IMODE((site / member).lstat().st_mode),
                wheel_mode=stat.S_IMODE(archive.getinfo(member).external_attr >> 16),
                bytes=len(archive.read(member)))
    interpreter = installed["interpreter"] | {
        "prefix": sys.prefix, "base_prefix": sys.base_prefix,
        "isolated": sys.flags.isolated, "sys_path": list(sys.path)
    }
    installation = {key: installed[key] for key in (
        "version", "wheel", "site_root", "package_files", "site_files", "metadata", "record"
    )} | {"interpreter": interpreter}
    font_path = Path(grant["font_qualification_file"])
    font = load(font_path)
    browser_path = Path(grant["browser_executable"])
    if font["status"] != "MATERIALS_ONLY_READY" or font["product_executed"] is not False or (
        font["coverage"]["missing_codepoints"] != [] or
        not all(font[key] is True for key in ("page_closed", "context_closed", "browser_closed")) or
        font["browser"]["sha256"] != sha(browser_path) or
        font["browser"]["version"] != ready["browser_version"] or
        font["browser"]["copy_manifest_sha256"] != grant["copy_manifest_sha256"]
    ):
        raise ValueError("font/browser material binding")
    consumer: dict[str, Any] = {key: {"path": str(root / "source" / name),
                      "sha256": sha(root / "source" / name),
                      "mode": stat.S_IMODE((root / "source" / name).lstat().st_mode)}
                for key, name in {
                    "driver": "tests/agent_core/fixtures/installed_workspace.py",
                    "e2e": "frontend/e2e/agent-workspace.spec.ts"
                }.items()}
    consumer.update(source_manifest={"path": str(manifest_path), "sha256": sha(manifest_path)},
        playwright_config={"path": str(root / "source/frontend/playwright.config.ts"),
            "sha256": sha(root / "source/frontend/playwright.config.ts")},
        copy_manifest_sha256=sha(Path(grant["copy_manifest_file"])))
    return {"kind": schema["kind"], "schema_version": 1, "scope": "consumer-only",
        "grant_sha256": sha(root / "release/directory01/grant.json"), "acceptance_root": str(root),
        "inherited": inherited, "installation": installation, "consumer": consumer,
        "materials": {"font_qualification": {"path": str(font_path), "sha256": sha(font_path)},
            "browser": {"path": str(browser_path), "sha256": sha(browser_path),
                "version": font["browser"]["version"],
                "closure_manifest_sha256": sha(Path(grant["copy_manifest_file"]))}},
        "qualification": {"label": schema["qualification_label"],
            "command": {"path": str(command_path), "sha256": sha(command_path)},
            "source_manifest_sha256": sha(manifest_path), "observed_at": command["started_at"]}}


class ModelWire:
    """有限SSE本机协议夹具；只产模型提议，所有业务effect来自真实安装产品。"""

    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.requests: list[Any] = []
        self.tasks: set[asyncio.Task[Any]] = set()
        self.writers: set[Any] = set()
        self.closed = 0
        self.server: Any = None

    async def start(self) -> None:
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        self.url = f"http://127.0.0.1:{self.port}/v1"

    @staticmethod
    def frame(delta: Any = None, finish: str | None = None) -> bytes:
        return ("data: " + json.dumps({"choices": [{"index": 0, "delta": delta or {},
            "finish_reason": finish}]}, ensure_ascii=False) + "\n\n").encode()

    def answer(self, value: str) -> list[bytes]:
        return [self.frame({"content": value}), self.frame(finish="stop"),
            b'data: {"choices":[],"usage":{"prompt_tokens":3,'
            b'"completion_tokens":2,"total_tokens":5}}\n\n',
            b"data: [DONE]\n\n"]

    def call(self, name: str, arguments: Any, index: int) -> list[bytes]:
        return [self.frame({"tool_calls": [{"index": 0, "id": "final-call-" + str(index),
            "type": "function", "function": {"name": name,
                "arguments": json.dumps(arguments)}}]}), self.frame(finish="tool_calls"),
            b"data: [DONE]\n\n"]

    def script(self, body: Any) -> list[bytes]:
        messages = body["messages"]
        if "tools" not in body:
            if self.mode == "summary-fail":
                return [b"data: {invalid\n\n"]
            pairs = dict(re.findall(r"SENTINEL_([0-9]+)=([0-9]+)", json.dumps(messages)))
            return self.answer(";".join("SENTINEL_" + k + "=" + pairs[k] for k in sorted(pairs)))
        current = max(i for i, value in enumerate(messages) if value["role"] == "user")
        text = messages[current]["content"]
        tools = [value for value in messages[current + 1:] if value["role"] == "tool"]
        if text.startswith("SENTINEL_"):
            return self.answer(text.split(" ", 1)[0])
        if text.startswith("D4_NEXT "):
            if tools:
                return self.answer("按原revision读取完成")
            return self.call("artifact_read", {"revision_id": text.split()[1],
                "path": "answer.json", "offset": 0, "length": 2048}, 0)
        if not (text.startswith("D4_TASK") or "创建calc.py" in text):
            return self.answer("没有授权执行请求")
        if not tools:
            return self.call("workspace_write", {"expected_revision": 0, "path": "calc.py",
                "expected_file_hash": None, "offset": 0, "delete_length": 0,
                "replacement": "def add(left, right):\n    return left + right\n"}, 0)
        if len(tools) == 1:
            source = ("import unittest, json, time\nfrom calc import add\n"
                "class Actual(unittest.TestCase):\n    def test_answer(self):\n"
                "        self.assertEqual(add(20,22),42)\n"
                "        with open('answer.json','w') as output:\n"
                "            json.dump({'answer': "
                + ("41" if self.mode == "verification-fail" else "42")
                + "},output)\n")
            if self.mode == "command-stop":
                source += "        print('FINAL_RUNNING',flush=True)\n        time.sleep(15)\n"
            if self.mode == "artifact-unknown":
                source += "        open('.env.production','w').write('synthetic-not-a-secret')\n"
            return self.call("workspace_write", {"expected_revision": 1, "path": "test_calc.py",
                "expected_file_hash": None, "offset": 0, "delete_length": 0,
                "replacement": source}, 1)
        if len(tools) == 2:
            return self.call("command_run", {"expected_revision": 2, "program": "python",
                "argv": ["-m", "unittest", "discover", "-s", ".", "-p", "test_*.py"]}, 2)
        return self.answer("模型结束是提议，完成由真实Verifier核验")

    async def handle(self, reader: Any, writer: Any) -> None:
        task = asyncio.current_task()
        assert task is not None
        self.tasks.add(task)
        self.writers.add(writer)
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            if len(header) > 65536:
                raise ValueError("fixture header")
            fields = dict(line.split(":", 1) for line in header.decode().split("\r\n")[1:]
                if ":" in line)
            size = int(next(v for k, v in fields.items() if k.lower() == "content-length"))
            if not 0 <= size <= 1048576:
                raise ValueError("fixture body")
            body = json.loads(await reader.readexactly(size))
            self.requests.append(body)
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                b"Connection: close\r\n\r\n")
            for chunk in self.script(body):
                writer.write(chunk)
                await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            self.closed += 1
            self.tasks.discard(task)
            self.writers.discard(writer)

    async def close(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        for writer in tuple(self.writers):
            writer.close()
        await asyncio.gather(*tuple(self.tasks), return_exceptions=True)
        assert not self.tasks and not self.writers


def execution_configuration(root: Path, grant: dict[str, Any], folder: Path) -> Path:
    from krim_agent.execution.types import Profile
    from krim_agent.workspace.targets import environment
    from krim_agent.workspace.types import WorkspaceProfile

    base = root / "evidence/prep-base/BASE-QUALIFIED.json"
    material = json.loads(raw(base))
    if sha(base) != grant["base_sha256"]:
        raise ValueError("base binding")
    execution = Profile(rootfs=Path(material["rootfs"]["path"]), base=base,
        cgroup_root=Path(grant["cgroup_root"]),
        material=root / "materials/d1-os" / (folder.relative_to(root / "evidence/final").parts[0]
            + "-" + folder.name),
        evidence=root / "evidence/d1-os" / (folder.relative_to(root / "evidence/final").parts[0]
            + "-" + folder.name))
    profile = WorkspaceProfile(max_tools_per_step=4, environment=environment(execution))
    path = folder / "trusted-profile.json"
    write(path, {"profile": profile.model_dump(mode="json"),
        "execution": {k: str(v) if isinstance(v, Path) else v
            for k, v in asdict(execution).items()},
        "artifact_root": str(folder / "objects")})
    path.chmod(0o600)
    return path


class Owner:
    def __init__(self, folder: Path, mode: str) -> None:
        self.folder, self.mode = folder, mode
        self.wire = ModelWire(mode)
        self.app: Any = None
        self.provider: Any = None
        self.server: Any = None
        self.server_task: Any = None
        self.listener: Any = None
        self.pool: list[Any] = []
        self.pending: set[int] = set()
        self.http: list[Any] = []
        self.before_fd: list[str] = []
        self.database = folder / "app.sqlite"
        self.closed = False
        self.session: Any = None
        self.other: Any = None

    async def start(self, root: Path, grant: dict[str, Any], *, port: int = 0,
                    seed: bool = False, database: Path | None = None) -> None:
        import socket

        import uvicorn
        from sqlalchemy import event

        from krim_agent.api.server import create_app
        from krim_agent.composition import open_application
        from krim_agent.config import Settings
        from krim_agent.providers.openai_compatible import (
            OpenAICompatibleConfig,
            OpenAICompatibleProvider,
        )

        self.folder.mkdir(exist_ok=False)
        self.before_fd = sorted(os.listdir("/proc/self/fd"))
        if database is not None:
            self.database = database
        await self.wire.start()
        config = None if self.mode == "unavailable" else execution_configuration(
            root, grant, self.folder)
        policy = self.folder / "access.json"
        write(policy, {"credentials": [{"principal_id": name, "token": token,
            "legacy": name == "final", "memberships": [{"workspace_id": "local",
            "role": "admin", "approve": True}]} for name, token in
            [("final", "final-api-token"), ("peer", "final-peer-token")]]})
        policy.chmod(0o600)
        settings = Settings(database=self.database,
            workspace_profile_file=config, access_policy_file=policy)
        self.provider = OpenAICompatibleProvider(OpenAICompatibleConfig(
            base_url=self.wire.url, model="fixture", max_tool_calls=4 if config else 1))
        self.app = await open_application(settings.database, settings=settings,
            provider=self.provider)
        if config is not None:
            workspace = self.app.service.bindings["target-workspace"]
            assert workspace.loop.provider is self.app.service.bindings["target-http"].loop.provider
            assert workspace.loop.provider.config.max_tool_calls == 4
        def checkout(conn: Any, record: Any, proxy: Any) -> None:
            assert id(conn) not in self.pending
            self.pending.add(id(conn))
            self.pool.append({"kind": "checkout", "connection": id(conn)})

        def reset(conn: Any, record: Any, state: Any) -> None:
            self.pool.append({"kind": "reset", "connection": id(conn),
                "asyncio_safe": state.asyncio_safe})

        def checkin(conn: Any, record: Any) -> None:
            self.pending.remove(id(conn))
            self.pool.append({"kind": "checkin", "connection": id(conn)})

        def close(conn: Any, record: Any) -> None:
            self.pool.append({"kind": "close", "connection": id(conn)})

        handlers: list[tuple[str, Callable[..., None]]] = [
            ("checkout", checkout), ("reset", reset), ("checkin", checkin), ("close", close)]
        for kind, handler in handlers:
            event.listen(self.app.store.engine.sync_engine, kind, handler)
        api = create_app(application=self.app)

        @api.middleware("http")
        async def observe(request: Any, call_next: Any) -> Any:
            self.http.append({"method": request.method, "path": request.url.path})
            return await call_next(request)

        self.api = api
        self.session: Any = None
        self.other: Any = None
        if self.mode != "unavailable":
            from krim_agent.context.types import ContextPolicy
            from krim_agent.security import Membership, PrincipalContext
            from krim_agent.sessions.types import SessionCreate
            from krim_agent.workspace.types import Completion, WorkspaceTurnRequest

            actor = PrincipalContext("final", 1,
                (Membership(workspace_id="local", role="admin"),), True)
            self.session = await self.app.sessions.create(actor, "local", SessionCreate(
                request_key="primary", title="FINAL工作A", target_key="target-workspace",
                context_policy=ContextPolicy(max_bytes=18000, max_messages=32,
                    reserved_output_tokens=512)))
            self.other = await self.app.sessions.create(actor, "local", SessionCreate(
                request_key="other", title="FINAL工作B", target_key="target-workspace"))
            if seed:
                # 摘要失败只作用于当前任务；历史仍由同一真实Provider/Store产生。
                mode = self.wire.mode
                self.wire.mode = "normal"
                try:
                    for index in range(8):
                        session = await self.app.sessions.get(actor, "local", self.session.id)
                        turn = await self.app.sessions.submit(actor, "local", self.session.id,
                            WorkspaceTurnRequest(request_key="history-" + str(index),
                                text=f"SENTINEL_{index}={index + 11} " + "x" * 1500,
                                expected_revision=session.revision, completion=Completion()))
                        await self.app.sessions.wait(actor, "local", self.session.id, turn.id)
                finally:
                    self.wire.mode = mode
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", port))
        self.port = self.listener.getsockname()[1]
        self.server = uvicorn.Server(uvicorn.Config(api, log_config=None, access_log=False,
            lifespan="on", ws="none"))
        self.server_task = asyncio.create_task(self.server.serve(sockets=[self.listener]))
        async with asyncio.timeout(5):
            while not self.server.started:
                if self.server_task.done():
                    await self.server_task
                    raise ValueError("server startup")
                await asyncio.sleep(0.01)

    def state(self) -> dict[str, Any]:
        return {"mode": self.mode, "database": str(self.database),
            "url": f"http://127.0.0.1:{getattr(self, 'port', None)}",
            "session_id": self.session.id if self.session is not None else None,
            "other_session_id": self.other.id if self.other is not None else None,
            "provider_calls": self.app.provider.calls,
            "streams_closed": self.app.provider.streams_closed,
            "requests": self.wire.requests, "http": self.http,
            "active_runs": sum(not t.done() for t in self.app.service.tasks.values())}

    async def close(self) -> None:
        if self.closed:
            return
        errors: list[Any] = []
        if self.server is not None:
            self.server.should_exit = True
        if self.server_task is not None:
            try:
                await self.server_task
            except BaseException as error:
                errors.append(repr(error))
        if self.listener is not None:
            self.listener.close()
        snapshot: dict[str, Any] = {"mode": self.mode, "database": str(self.database)}
        try:
            if self.app is not None:
                snapshot.update(self.state())
        except BaseException as error:
            errors.append("observe-before: " + repr(error))
        if self.app is not None:
            try:
                await self.app.close()
            except BaseException as error:
                errors.append(repr(error))
        if self.provider is not None:
            try:
                await self.provider.close()
            except BaseException as error:
                errors.append(repr(error))
        try:
            await self.wire.close()
        except BaseException as error:
            errors.append(repr(error))
        if self.app is not None:
            from krim_agent.storage.sqlite import SQLiteStore

            try:
                runner = self.app.runner
                snapshot.update({"store_closed": self.app.store.closed,
                    "provider_closed": self.app.provider.closed, "pool": self.pool,
                    "provider_calls": self.app.provider.calls,
                    "streams_closed": self.app.provider.streams_closed,
                    "provider_active": self.app.provider.active,
                    "model_connections_closed": self.wire.closed,
                    "model_tasks_pending": len(self.wire.tasks),
                    "pending_connections": sorted(self.pending),
                    "unsafe_gc": sum(e["kind"] == "reset" and not e["asyncio_safe"]
                        for e in self.pool),
                    "session_dispatcher_done": self.app.sessions._dispatcher.done(),
                    "application_close_task_done": self.app._close_task.done(),
                    "run_service_close_task_done": self.app.service._close_task.done(),
                    "runner": {"closed": runner.closed, "jobs": len(runner.jobs),
                        "resources": len(runner._resources), "owner_closed": runner._owner.closed,
                        "material": str(runner.profile.material),
                        "evidence": str(runner.profile.evidence)} if runner is not None else None,
                    "all_producers_joined": all(t.done() for t in self.app.service.tasks.values())})
            except BaseException as error:
                errors.append("observe-after: " + repr(error))
            try:
                second = await SQLiteStore.open(self.database)
                await second.close()
                snapshot["second_writer_closed"] = second.closed
            except BaseException as error:
                errors.append(repr(error))
            try:
                backup = self.folder / "consistent-readonly.sqlite"
                with closing(sqlite3.connect(self.database.as_uri() + "?mode=ro",
                    uri=True)) as reader, closing(sqlite3.connect(backup)) as writer:
                    reader.backup(writer)
                snapshot["backup"] = {"path": str(backup), "sha256": sha(backup)}
            except BaseException as error:
                errors.append("backup: " + repr(error))
        snapshot["errors"] = errors
        snapshot["FD_before"] = self.before_fd
        snapshot["FD_after"] = sorted(os.listdir("/proc/self/fd"))
        snapshot["FD_equal"] = snapshot["FD_before"] == snapshot["FD_after"]
        write(self.folder / "closed.json", snapshot)
        if errors or self.pending or snapshot.get("unsafe_gc", 0):
            raise ValueError("owner cleanup failed " + repr(errors))
        self.closed = True


async def serve(root: Path, grant: dict[str, Any], args: Any, receipt: Any,
                start_cwd: str) -> None:
    from fastapi import Request

    import krim_agent

    assert Path(krim_agent.__file__).is_relative_to(Path(receipt["installation"]["site_root"]))
    before = sorted(os.listdir("/proc/self/fd"))
    tasks_before = asyncio.all_tasks()
    entries: list[dict[str, Any]] = []
    preparations: dict[str, dict[str, Any]] = {}
    stopping = asyncio.Event()
    closing_run = False
    registry = args.evidence / "preparations"
    registry.mkdir()
    scenarios: list[dict[str, Any]] = []
    for viewport in ("1280x900", "390x844"):
        prefix = ["agent-workspace.spec.ts", viewport]
        cases: list[tuple[str, str, bool, list[str]]] = [
            ("workspace_unavailable_is_truthful_and_zero_effect", "unavailable", False, [])]
        cases.extend((f"confirmed_inputs_{v}_refresh_remount_same_identity", "inputs", False, [])
                     for v in ("no-turn", "old-turn"))
        cases.extend((f"real_accepted_input_{b}_late_{s}", "inputs", False, [])
                     for b in ("post", "get") for s in ("session-return", "credential", "remount"))
        cases.append(("long_history_summary_approval_real_unittest_artifact_verification_next_turn",
                      "normal", True, ["real long history fixture"]))
        for mode in ("summary-fail", "approval-denied", "command-stop",
                     "verification-fail", "artifact-unknown"):
            cases.append((f"real_{mode}_preserves_partial_truth",
                "normal" if mode == "approval-denied" else mode, mode == "summary-fail", [mode]))
        for title, mode, seed, groups in cases:
            scenarios.append({"node_key": prefix + groups + [title], "mode": mode, "seed": seed})

    def describe(entry: dict[str, Any]) -> dict[str, Any]:
        return {key: entry.get(key) for key in (
            "owner_id", "node_key", "prepare_id", "mode", "seed", "phase", "start_entered",
            "handed_off", "start_error", "close_error", "started_at", "ready_at", "closed_at"
        )} | {"database": str(entry["owner"].database),
              "database_created": entry["owner"].database.exists(),
              "start_task_done": entry["start_task"].done(),
              "close_task_done": entry["close_task"] is not None and entry["close_task"].done()}

    def register(owner: Owner, node_key: list[str], prepare_id: str,
                 **kwargs: Any) -> dict[str, Any]:
        entry: dict[str, Any] = {"owner_id": len(entries), "owner": owner, "node_key": node_key,
            "prepare_id": prepare_id, "mode": owner.mode, "seed": kwargs.get("seed", False),
            "phase": "registered", "start_entered": False, "handed_off": False,
            "start_error": None, "close_error": None, "close_task": None,
            "started_at": time.time(), "ready_at": None, "closed_at": None}
        entries.append(entry)
        preparations[prepare_id] = entry

        async def launch() -> None:
            entry["start_entered"] = True
            entry["phase"] = "starting"
            try:
                await owner.start(root, grant, **kwargs)
                if entry["owner_id"]:
                    @owner.api.get("/__fixture/state")
                    async def state() -> Any:
                        return owner.state()

                entry["state"] = owner.state() | {"owner_id": entry["owner_id"],
                    "node_key": node_key, "prepare_id": prepare_id}
                entry["phase"] = "ready"
                entry["ready_at"] = time.time()
                write(registry / (prepare_id + ".ready.json"), describe(entry))
            except BaseException as error:
                entry["start_error"] = repr(error)
                entry["phase"] = "failed"
                raise

        entry["start_task"] = asyncio.create_task(launch(), name="prepare-" + prepare_id)
        write(registry / (prepare_id + ".registered.json"), describe(entry))
        return entry

    async def close_actual(entry: dict[str, Any]) -> dict[str, Any]:
        entry["phase"] = "closing"
        start_task = entry["start_task"]
        if not start_task.done():
            start_task.cancel()
        await asyncio.gather(start_task, return_exceptions=True)
        try:
            if entry["start_entered"]:
                owner = entry["owner"]
                await owner.close()
                value: dict[str, Any] = json.loads(raw(owner.folder / "closed.json"))
            else:
                value = {"resource_state": "not_started", "start_task_joined": True}
            entry["phase"] = "closed"
            entry["closed_at"] = time.time()
            write(registry / (entry["prepare_id"] + ".closed.json"), describe(entry))
            return value
        except BaseException as error:
            entry["phase"] = "cleanup_unknown"
            entry["close_error"] = repr(error)
            write(registry / (entry["prepare_id"] + ".close-error.json"), describe(entry))
            raise

    async def close_entry(entry: dict[str, Any]) -> dict[str, Any]:
        if entry["close_task"] is None:
            entry["close_task"] = asyncio.create_task(close_actual(entry),
                name="close-" + entry["prepare_id"])
        task = entry["close_task"]
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
        value: dict[str, Any] = task.result()
        if cancelled:
            raise asyncio.CancelledError
        return value

    primary = Owner(args.evidence / "primary", "normal")
    primary_entry = register(primary, ["primary"], args.evidence.name + "-primary",
        port=args.port, database=args.database)
    waiter: asyncio.Task[bool] | None = None
    try:
        await primary_entry["start_task"]

        @primary.api.post("/__fixture/new")
        async def prepare(request: Request) -> Any:
            if closing_run:
                raise ValueError("fixture closing")
            body = await request.json()
            if set(body) != {"mode", "seed", "node_key", "prepare_id"} or (
                type(body["seed"]) is not bool
            ):
                raise ValueError("finite fixture scenario")
            ordinal = next((i for i, item in enumerate(scenarios) if item == {
                key: body[key] for key in ("node_key", "mode", "seed")
            }), None)
            if ordinal is None or body["prepare_id"] != args.evidence.name + "-" + str(ordinal):
                raise ValueError("finite preparation identity")
            entry = preparations.get(body["prepare_id"])
            if entry is None:
                owner = Owner(args.evidence / ("owner-" + str(len(entries))), body["mode"])
                entry = register(owner, body["node_key"], body["prepare_id"], seed=body["seed"])
            try:
                await asyncio.shield(entry["start_task"])
                if entry["phase"] != "ready":
                    raise ValueError("preparation no longer ready")
                entry["handed_off"] = True
                return entry["state"]
            except BaseException:
                await close_entry(entry)
                raise

        @primary.api.post("/__fixture/close")
        async def close_owner(request: Request) -> Any:
            value = await request.json()
            if set(value) != {"owner_id"} or type(value["owner_id"]) is not int or not (
                1 <= value["owner_id"] < len(entries)
            ):
                raise ValueError("finite owner identity")
            return await close_entry(entries[value["owner_id"]])

        @primary.api.post("/__fixture/shutdown")
        async def shutdown() -> Any:
            nonlocal closing_run
            closing_run = True
            stopping.set()
            return {"stopping": True}

        write(args.evidence / "SERVER-READY.json", {"start_cwd": start_cwd,
            "runtime_cwd": str(Path.cwd()), "identity_receipt_sha256":
            sha(Path(grant["installation_identity_receipt"])), "module": krim_agent.__file__,
            "sys_path": sys.path, "isolated": sys.flags.isolated,
            "interpreter": sys.executable, "pid": os.getpid(), "port": args.port,
            "consumer_receipt_path": grant["consumer_identity_receipt"],
            "consumer_receipt_sha256": sha(Path(grant["consumer_identity_receipt"])),
            "consumer_receipt_kind": receipt["kind"],
            "grant_sha256": receipt["grant_sha256"]})
        waiter = asyncio.create_task(stopping.wait(), name="fixture-stop-waiter")
        await asyncio.wait([waiter, primary.server_task], return_when=asyncio.FIRST_COMPLETED)
    finally:
        closing_run = True
        errors: list[str] = []
        if waiter is not None:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
        for entry in reversed(entries[1:]):
            try:
                await close_entry(entry)
            except BaseException as error:
                errors.append(repr(error))
        if primary.server is not None:
            primary.server.should_exit = True
        try:
            await close_entry(primary_entry)
        except BaseException as error:
            errors.append(repr(error))
        pending = [t for t in asyncio.all_tasks() if t not in tasks_before and not t.done()]
        if pending:
            try:
                async with asyncio.timeout(5):
                    await asyncio.gather(*pending)
            except BaseException as error:
                errors.append(repr(error))
        after = sorted(os.listdir("/proc/self/fd"))
        write(args.evidence / "SERVER-CLOSED.json", {"owners": len(entries), "errors": errors,
            "FD_before": before, "FD_after": after, "FD_equal": before == after,
            "all_tasks_joined": all(t.done() for t in pending) and all(
                e["start_task"].done() and e["close_task"] is not None and
                e["close_task"].done() for e in entries),
            "entries": [describe(e) for e in entries], "pid": os.getpid()})
        if errors or before != after:
            raise ValueError("serve cleanup failed")


def child_environment() -> dict[str, str]:
    return {name: os.environ[name] for name in (
        "PATH", "HOME", "TMPDIR", "LANG", "LD_LIBRARY_PATH", "PYTHONDONTWRITEBYTECODE",
        "KRIM_M2_FINAL_ROOT", "KRIM_M2_FINAL_GRANT"
    ) if name in os.environ}


async def child(argv: list[str], cwd: Path, evidence: Path, name: str,
                env: dict[str, str] | None = None) -> dict[str, Any]:
    """有限已创建子进程：原PID/pidfd归属、真实wait和完整双流。"""
    started = time.time()
    env = child_environment() if env is None else env
    process = await asyncio.create_subprocess_exec(*argv, cwd=cwd, env=env,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    pidfd = os.pidfd_open(process.pid)
    try:
        stdout, stderr = await process.communicate()
    finally:
        if process.returncode is None:
            signal.pidfd_send_signal(pidfd, signal.SIGTERM)
            await process.wait()
        os.close(pidfd)
    (evidence / (name + ".stdout")).write_bytes(stdout)
    (evidence / (name + ".stderr")).write_bytes(stderr)
    result = {"argv": argv, "cwd": str(cwd), "env": env,
        "pid": process.pid, "waited_pid": process.pid, "rc": process.returncode,
        "pidfd_closed": True, "start": started, "end": time.time()}
    write(evidence / (name + ".command.json"), result)
    return result


async def rollback(root: Path, args: Any) -> None:
    """真实0.3安装包产生schema3事实；仅在新根升级、拒旧及还原备份。"""
    import shutil

    from krim_agent.runtime.facts import project_facts
    from krim_agent.storage.sqlite import SQLiteStore

    database = args.evidence / "legacy.sqlite"
    seed_path = args.evidence / "legacy-seed.json"
    generator = args.evidence / "legacy-generator.py"
    # 这是有限API消费者，不复制Store/事务/迁移逻辑，不注入M1源码。
    generator.write_text('''import asyncio,json,sys
from pathlib import Path
from krim_agent.composition import open_application
from krim_agent.contracts import RunRequest
from krim_agent.observability import project_trace
from krim_agent.runtime.facts import project_facts
from krim_agent.sdk import SessionSDK
from krim_agent.sessions.types import SessionCreate,TurnRequest
from krim_agent.evaluation.types import FreezeRequest,EvalRequest,rule
import krim_agent
async def main():
 assert krim_agent.__version__=="0.3.0a1"
 async with await open_application(Path(sys.argv[1])) as app:
  assert app.store.schema_version==3
  run=await app.service.start(RunRequest(text="/add 2 3",request_key="legacy-run"))
  await app.service.wait(run.id)
  snapshot=await app.store.snapshot(run.id)
  candidate=await app.evaluations.candidate(run.id,True)
  reference={"expected":"5","expected_status":"completed","expected_code":None,
   "expected_tools":None,"trajectory":None,"expected_schema":None}
  dataset=await app.evaluations.freeze(FreezeRequest.model_validate({"name":"legacy",
   "version":"1","cases":[{"case_id":"one","source_kind":"run","source_run_id":run.id,
   "candidate_fingerprint":candidate.candidate_fingerprint,"selected_input":"/add 2 3",
   "reason":"合成回滚核对","reference":reference}]}))
  ev=await app.evaluations.start(EvalRequest.model_validate({"request_key":"legacy-eval",
   "dataset":{k:getattr(dataset,k) for k in ["name","version","fingerprint"]},
   "target_key":"target-v2","rules":[rule("status_code").model_dump(mode="json"),
   rule("exact_match").model_dump(mode="json")],"execution":{"repetitions":1,
   "concurrency":1,"item_timeout_seconds":12,"scorer_timeout_seconds":1,
   "batch_timeout_seconds":600}}))
  await app.evaluations.wait(ev.id)
  report=await app.evaluations.report(ev.id)
  sdk=SessionSDK(app.sessions)
  session=await sdk.create(SessionCreate(request_key="legacy-session"))
  turn=await sdk.submit(session.id,TurnRequest(request_key="legacy-turn",text="/echo old",
   expected_revision=0))
  turn=await sdk.wait(session.id,turn.id)
  result={"run_id":run.id,"session_id":session.id,"turn_id":turn.id,"eval_id":ev.id,
   "module":krim_agent.__file__,"version":krim_agent.__version__,
   "provider_calls":app.provider.calls,
   "facts":project_facts(snapshot).model_dump(mode="json"),
   "trace":project_trace(snapshot).model_dump(mode="json"),"report":report,
   "dataset_fingerprint":dataset.fingerprint}
 Path(sys.argv[2]).write_text(json.dumps(result,ensure_ascii=False))
asyncio.run(main())
''')
    old_python = root / "rollback-env/bin/python"
    result = await child([str(old_python), "-I", "-B", str(generator), str(database),
        str(seed_path)], args.evidence, args.evidence, "legacy-generate")
    assert result["rc"] == 0
    seed = json.loads(raw(seed_path))
    backup = args.evidence / "schema3-before-upgrade.sqlite"
    with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as reader:
        with closing(sqlite3.connect(backup)) as writer:
            reader.backup(writer)
    store = await SQLiteStore.open(database)
    try:
        assert store.schema_version == 4
        snapshot = await store.snapshot(seed["run_id"])
        assert project_facts(snapshot).model_dump(mode="json") == seed["facts"]
        from krim_agent.observability import project_trace

        assert project_trace(snapshot).model_dump(mode="json") == seed["trace"]
        assert (await store.eval_snapshot(seed["eval_id"]))["report"] == seed["report"]
    finally:
        await store.close()
    schema4 = args.evidence / "schema4-preserved.sqlite"
    shutil.copyfile(database, schema4)
    reject = args.evidence / "old-reject.py"
    reject.write_text('''import asyncio,sys
from pathlib import Path
from krim_agent.storage.sqlite import SQLiteStore
from krim_agent.contracts import ServiceError
import sqlite3
from contextlib import closing
async def main():
 with closing(sqlite3.connect(Path(sys.argv[1]).as_uri()+"?mode=ro",uri=True)) as conn:
  assert conn.execute("PRAGMA user_version").fetchone()[0]==4
 try:
  s=await SQLiteStore.open(Path(sys.argv[1]),readonly=True)
 except ServiceError as error:
  assert error.code=="database_invalid"
  print("old schema3 capability explicitly rejects actual schema4",error.code);return
 await s.close();raise AssertionError("old package accepted schema4")
asyncio.run(main())
''')
    result = await child([str(old_python), "-I", "-B", str(reject), str(database)],
        args.evidence, args.evidence, "old-schema4-reject")
    assert result["rc"] == 0
    shutil.copyfile(backup, database)
    reader_code = args.evidence / "old-restored.py"
    reader_code.write_text('''import asyncio,json,sys
from pathlib import Path
from krim_agent.composition import open_application
from krim_agent.contracts import RunRequest
from krim_agent.runtime.facts import project_facts
from krim_agent.observability import project_trace
async def main():
 seed=json.loads(Path(sys.argv[2]).read_text())
 async with await open_application(Path(sys.argv[1])) as app:
  assert app.store.schema_version==3
  snap=await app.store.snapshot(seed["run_id"])
  assert project_facts(snap).model_dump(mode="json")==seed["facts"]
  assert project_trace(snap).model_dump(mode="json")==seed["trace"]
  assert await app.evaluations.report(seed["eval_id"])==seed["report"]
  found=await app.service.start(RunRequest(text="/add 2 3",request_key="legacy-run"))
  assert found.id==seed["run_id"] and app.provider.calls==0
 print("schema3 restored; original key/Facts/Trace/report; zero replay")
asyncio.run(main())
''')
    result = await child([str(old_python), "-I", "-B", str(reader_code), str(database),
        str(seed_path)], args.evidence, args.evidence, "old-restored-zero-replay")
    assert result["rc"] == 0
    write(args.evidence / "ROLLBACK.json", {"schema3_backup_sha256": sha(backup),
        "preserved_schema4_sha256": sha(schema4), "legacy_seed_sha256": sha(seed_path),
        "restored_zero_replay": True, "only_new_root": True})


async def cli_checks(root: Path, grant: dict[str, Any], args: Any, owner: Owner) -> None:
    import socket

    import httpx

    from krim_agent.storage.sqlite import SQLiteStore

    folder = args.evidence / "cli"
    folder.mkdir()
    profile = execution_configuration(root, grant, folder)
    env = child_environment()
    env.update(KRIM_AGENT_PROVIDER="http", KRIM_AGENT_RUN_TIMEOUT="180",
        KRIM_AGENT_MAX_ACTIVE_RUNS="1", KRIM_AGENT_WORKSPACE_PROFILE_FILE=str(profile),
        KRIM_AGENT_ACCESS_POLICY_FILE=str(owner.folder / "access.json"),
        KRIM_AGENT_HTTP_BASE_URL=owner.wire.url, KRIM_AGENT_HTTP_MODEL="gpt-6.1-sol",
        KRIM_AGENT_HTTP_REASONING_EFFORT="xhigh", KRIM_AGENT_HTTP_API_KEY="synthetic-local-only",
        KRIM_AGENT_HTTP_READ_TIMEOUT_SECONDS="60")
    executable = str(root / "installed-env/bin/krim")
    before = len(owner.wire.requests)
    conflict = env | {"KRIM_AGENT_HTTP_MODEL": "explicit-conflicting-model"}
    rejected = folder / "rejected.sqlite"
    probe = await child([executable, "serve", "--database", str(rejected), "--port", "8875"],
        args.evidence, args.evidence, "cli-explicit-conflict", conflict)
    assert probe["rc"] != 0 and not rejected.exists() and len(owner.wire.requests) == before
    selected = socket.socket()
    selected.bind(("127.0.0.1", 0))
    port = selected.getsockname()[1]
    selected.close()
    argv = [executable, "serve", "--database", str(folder / "app.sqlite"), "--port", str(port)]
    process = await asyncio.create_subprocess_exec(*argv, cwd=args.evidence, env=env,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    pidfd = os.pidfd_open(process.pid)
    client = httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}",
        headers={"Authorization": "Bearer final-api-token"}, trust_env=False)
    record: dict[str, Any] = {"argv": argv, "env": env, "pid": process.pid,
        "provider_calls_before": before}
    try:
        async with asyncio.timeout(10):
            while True:
                if process.returncode is not None:
                    raise ValueError("CLI exited before ready")
                try:
                    health = await client.get("/healthz")
                    if health.status_code == 200:
                        break
                except httpx.ConnectError:
                    pass
                await asyncio.sleep(0.02)
        assert (await client.get("/openapi.json")).json()["info"]["version"] == "0.4.0a1"
        capability = await client.get("/v3/workspace/capabilities")
        assert capability.status_code == 200
        session = (await client.post("/v2/workspaces/local/sessions", json={
            "request_key": "cli-work", "target_key": "target-workspace"})).json()
        base = f"/v3/workspaces/local/sessions/{session['id']}"
        response = await client.post(base + "/turns", json={"schema_version": 1,
            "request_key": "cli-turn", "expected_revision": 0, "text": "D4_TASK CLI",
            "input": {"mode": "head_at_dispatch", "revision_id": None},
            "completion": {"profile": "python-unittest/1", "required_paths":
                ["calc.py", "answer.json"], "conditions": []}})
        assert response.status_code == 202, response.text
        turn = response.json()
        turn_base = f"/v2/workspaces/local/sessions/{session['id']}/turns/{turn['id']}"
        async with asyncio.timeout(30):
            while True:
                approvals = await client.get(turn_base + "/approvals")
                assert approvals.status_code == 200
                for item in approvals.json():
                    reply = await client.post(turn_base + "/approvals/" + item["request_id"],
                        json={"request_key": "cli-" + item["call_id"],
                        "call_id": item["call_id"], "arguments_hash": item["arguments_hash"],
                        "tool_hash": item["tool_hash"], "allow": True})
                    assert reply.status_code == 200, reply.text
                value = (await client.get(turn_base)).json()
                if value["status"] not in {"queued", "running"}:
                    break
                await asyncio.sleep(0.02)
        view = (await client.get(base + "/turns/by-key/cli-turn")).json()
        assert value["status"] == "completed" and view["verification"]["state"] == "passed"
        assert len(owner.wire.requests) == before + 4
        assert all(v["model"] == "gpt-6.1-sol" for v in owner.wire.requests[before:])
        record.update(work=view, actual_HTTP_requests=owner.wire.requests[before:],
            health=health.json(), capabilities=capability.json(), run_id=value["run_id"])
    finally:
        await client.aclose()
        if process.returncode is None:
            signal.pidfd_send_signal(pidfd, signal.SIGINT)
        stdout, stderr = await process.communicate()
        os.close(pidfd)
        (args.evidence / "cli-serve.stdout").write_bytes(stdout)
        (args.evidence / "cli-serve.stderr").write_bytes(stderr)
        record.update(rc=process.returncode, waited_pid=process.pid, pidfd_closed=True)
        write(args.evidence / "CLI-SERVE.json", record)
    assert process.returncode in {0, 130}
    reader = await SQLiteStore.open(folder / "app.sqlite", readonly=True)
    try:
        snapshot = await reader.snapshot(record["run_id"])
        current_profile = snapshot.target.get("workspace_profile")
        assert isinstance(current_profile, dict) and current_profile["max_tools_per_step"] == 4
    finally:
        await reader.close()
    second = await SQLiteStore.open(folder / "app.sqlite")
    await second.close()
    check = socket.socket()
    assert check.connect_ex(("127.0.0.1", port)) != 0
    check.close()
    write(args.evidence / "CLI-CLOSED.json", {"second_writer_closed": second.closed,
        "port_closed": True, "same_pid_waited": process.pid, "source_injection": False,
        "implicit_http_cap_projected_to": 4, "contradiction_before_DB_and_HTTP": True})


async def validate_installation(root: Path, grant: dict[str, Any], args: Any) -> None:
    from krim_agent.client import AsyncRunClient
    from krim_agent.contracts import ApprovalReply
    from krim_agent.sdk import WorkspaceSDK
    from krim_agent.security import Membership, PrincipalContext

    command_path = args.evidence / "command.json"
    command = json.loads(raw(command_path))
    receipt = identity(root, grant, args.wheel,
        sha(Path(command["source_manifest"])), sha(command_path))
    assert command["product_argv"][command["product_argv"].index("--wheel") + 1] == str(args.wheel)
    owner = Owner(args.evidence / "validation-owner", "normal")
    actor = PrincipalContext("final", 1, (Membership(workspace_id="local", role="admin"),), True)
    before_fd = sorted(os.listdir("/proc/self/fd"))
    succeeded = False
    try:
        await owner.start(root, grant)
        client = AsyncRunClient(f"http://127.0.0.1:{owner.port}", "final-api-token")
        approving = True

        async def approve() -> None:
            consumed: set[str] = set()
            while approving:
                for run_id in list(owner.app.service.tasks):
                    for item in await owner.app.service.approvals(run_id):
                        key = item["request_id"]
                        if key in consumed:
                            continue
                        consumed.add(key)
                        await client.request("POST", f"/v1/runs/{run_id}/approvals/{key}",
                            ApprovalReply(request_key="allow-" + key,
                            call_id=item["call_id"], arguments_hash=item["arguments_hash"],
                            tool_hash=item["tool_hash"], allow=True).model_dump(mode="json"))
                await asyncio.sleep(0.01)

        approval_task = asyncio.create_task(approve())
        try:
            env = child_environment()
            env["KRIM_AGENT_API_KEY"] = "final-api-token"
            result = await child([sys.executable, "-I", "-B",
                str(root / "source/examples/agent_core_workspace.py"), "--url",
                f"http://127.0.0.1:{owner.port}", "--key", "installed-workspace-example"],
                args.evidence, args.evidence, "workspace-example", env)
            assert result["rc"] == 0
            from krim_agent.sdk import SessionSDK

            session = await SessionSDK(client).by_key("installed-workspace-example.session")
            work = WorkspaceSDK(client)
            found = await work.by_key(session.id, "installed-workspace-example")
            finished = await owner.app.sessions.wait(actor, "local", session.id, found.turn_id)
            final = await work.by_key(session.id, "installed-workspace-example")
            assert finished.status == "completed" and final.verification is not None
            assert final.verification.state == "passed"
            calls = owner.app.provider.calls
            recovered = await work.by_key(session.id, "installed-workspace-example")
            assert recovered.turn_id == found.turn_id
            assert owner.app.provider.calls == calls
            write(args.evidence / "WORKSPACE-EXAMPLE.json", final.model_dump(mode="json"))
        finally:
            approving = False
            await approval_task
            await client.close()
        result = await child([sys.executable, "-I", "-B",
            str(root / "source/examples/agent_core_chat.py"), "--database",
            str(args.evidence / "chat-example.sqlite")], args.evidence, args.evidence,
            "chat-example")
        assert result["rc"] == 0
        await cli_checks(root, grant, args, owner)
        await rollback(root, args)
        succeeded = True
    finally:
        await owner.close()
    after_fd = sorted(os.listdir("/proc/self/fd"))
    if not succeeded or before_fd != after_fd:
        raise ValueError("installation validation/FD cleanup")
    write(args.evidence / "VALIDATION-CLOSED.json", {"success": True,
        "FD_before": before_fd, "FD_after": after_fd, "FD_equal": True,
        "module_consumers": receipt["site_files"], "sys_path": sys.path,
        "runtime_cwd": str(Path.cwd()), "interpreter": sys.executable})
    write(Path(grant["installation_identity_receipt"]), receipt)


def main() -> None:
    args = arguments()
    root, grant = context()
    evidence = args.evidence
    if not evidence.is_absolute() or evidence.parent != root / "evidence/final" or (
        ".." in evidence.parts
    ):
        raise ValueError("evidence scope")
    for parent in [*reversed(evidence.parents), evidence]:
        if not stat.S_ISDIR(parent.lstat().st_mode):
            raise ValueError("nonregular evidence directory")
    if args.qualify_consumer:
        if evidence.name != "final-directory01-consumer" or Path.cwd() != evidence:
            raise ValueError("fixed consumer output")
        write(Path(grant["consumer_identity_receipt"]), consumer_facts(root, grant))
        return
    if args.database is not None:
        if evidence.name not in ("final-recovery01-first", "final-recovery01-browser"):
            raise ValueError("fixed browser output")
        receipt = installed_identity(root, grant)
        if args.database != evidence / "browser.sqlite" or args.port != 8876:
            raise ValueError("serve fixed arguments")
        start_cwd = str(Path.cwd())
        os.chdir(evidence)
        asyncio.run(serve(root, grant, args, receipt, start_cwd))
    else:
        raise ValueError("child installation mode forbidden")


if __name__ == "__main__":
    main()
