"""Launch the unmodified production CLI for an opt-in, real-model chat check.

Only host setup lives here: isolated managed identity, Broker, empty project,
short-lived Shell grant and in-memory provider credentials. No model answers,
project source, tool calls or test outcomes are supplied by this launcher.
Run this server through preview_start, not a background shell.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import socket
import struct
import sys
import time
from pathlib import Path

import httpx
from pydantic import SecretStr

from krim_agent.accounts.service import IdentityService
from krim_agent.development.configuration import DevelopmentConfiguration
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig
from krim_agent.sandbox.configuration import SandboxConfiguration
from krim_agent.sandbox.quota import QuotaPolicy
from krim_agent.security import AccessPolicy, Credential, Membership
from krim_agent.storage.sqlite import SQLiteStore
from tests.coding_agent.verify_development_broker import (
    APPROVED_IMAGE,
    APPROVED_STORAGE_ROOT,
    Paths,
    approved_limits,
    broker_lifecycle,
    new_paths,
    require_platform,
    save_json,
)

# Host supplies a disposable access token; never embed an account credential here.
TEST_PRINCIPAL = "shell-chat-acceptance"
TEST_WORKSPACE = "shell-chat-acceptance"


def test_access_token() -> str:
    token = os.environ.get("KRIM_TEST_ACCESS_TOKEN", "")
    if not 32 <= len(token) <= 256 or any(not 33 <= ord(char) <= 126 for char in token):
        raise RuntimeError("KRIM_TEST_ACCESS_TOKEN must contain a disposable access token")
    return token


async def receive_provider(directory: Path) -> OpenAICompatibleConfig:
    path = directory / "credential.sock"
    future = asyncio.get_running_loop().create_future()

    async def accept(reader, writer):
        try:
            peer = writer.get_extra_info("socket").getsockopt(
                socket.SOL_SOCKET,
                socket.SO_PEERCRED,
                struct.calcsize("3i"),
            )
            _, uid, _ = struct.unpack("3i", peer)
            if uid != os.geteuid() or future.done():
                return
            raw = await asyncio.wait_for(reader.readline(), 5)
            config = OpenAICompatibleConfig.model_validate_json(raw)
            if not config.api_key.get_secret_value():
                raise ValueError("missing provider credential")
            future.set_result(config)
            writer.write(b"accepted\n")
            await writer.drain()
        except Exception:
            if not future.done():
                future.set_exception(RuntimeError("credential handoff failed"))
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_unix_server(accept, path, limit=8192)
    path.chmod(0o600)
    try:
        async with server:
            return await asyncio.wait_for(future, 120)
    finally:
        path.unlink(missing_ok=True)


async def initialize_identity(database: Path) -> None:
    store = await SQLiteStore.open(database)
    try:
        identities = await IdentityService.open(store)
        await identities.import_policy(
            AccessPolicy(
                credentials=(
                    Credential(
                        principal_id=TEST_PRINCIPAL,
                        token=SecretStr(test_access_token()),
                        memberships=(Membership(workspace_id=TEST_WORKSPACE, role="admin"),),
                    ),
                )
            ),
            TEST_PRINCIPAL,
            request_key="shell-chat-bootstrap",
            display_names={TEST_PRINCIPAL: "Shell 实测临时账号"},
        )
    finally:
        await store.close()


async def prepare_project(client: httpx.AsyncClient, directory: Path) -> None:
    async def call(method, path, body=None):
        response = await client.request(method, path, json=body)
        if response.status_code >= 400:
            raise RuntimeError(f"setup {path}: {response.status_code}")
        return response.json()

    actor = await call("GET", "/v2/me")
    tools = await call("GET", "/v2/targets/target-development/tools")
    assert "target-development" in actor["target_keys"]
    assert "shell" in [tool["name"] for tool in tools]
    project = await call(
        "POST",
        "/v2/me/sandbox/projects",
        {
            "request_key": "shell-chat-project",
            "workspace_id": TEST_WORKSPACE,
            "title": "LeetCode Hot 100 Shell 实测",
        },
    )
    project_id = project["project_id"]
    environment = await call(
        "POST",
        f"/v2/me/development/projects/{project_id}/prepare",
        {
            "request_key": "shell-chat-prepare",
        },
    )
    previous = (
        json.loads((directory / "ready.json").read_text())
        if (directory / "ready.json").exists()
        else None
    )
    grant = await call(
        "PUT",
        f"/v2/me/development/projects/{project_id}/grant",
        {
            "expected_revision": previous["grant"]["revision"] if previous else 0,
            "expires_at": time.time() + 3600,
            "requires_approval": False,
            "actions": ["shell.execute"],
        },
    )
    save_json(
        directory,
        "ready.json",
        {
            "target_keys": actor["target_keys"],
            "tools": [tool["name"] for tool in tools],
            "project": project,
            "environment": environment,
            "grant": grant,
            "workspace_id": TEST_WORKSPACE,
            "provider": "http",
            "http_mode": "agent",
            "project_source_preseeded": False,
            "chat_turns_submitted_by_launcher": 0,
        },
    )
    print("READY: production /ui/; target-development; shell; empty project", flush=True)


async def run(args) -> None:
    test_access_token()  # Validate before creating any host resources.
    storage = require_platform(APPROVED_STORAGE_ROOT)
    directory = storage / args.name
    directory.mkdir(mode=0o700, exist_ok=args.resume)
    if args.resume:
        saved = json.loads((directory / "host.json").read_text())
        root = Path(saved["root"])
        info = await asyncio.to_thread(root.lstat)
        if (
            root.parent != storage
            or await asyncio.to_thread(root.is_symlink)
            or (info.st_dev, info.st_ino)
            != (
                saved["root_device"],
                saved["root_inode"],
            )
        ):
            raise RuntimeError("test root binding changed")
        paths = Paths(
            root,
            info.st_dev,
            info.st_ino,
            root / "broker-state",
            root / "development-private",
            root / "broker.sock",
            root / "application.sqlite",
            root / "environments",
        )
    else:
        paths = new_paths(storage)
    save_json(
        directory,
        "host.json",
        {
            "root": paths.root,
            "root_device": paths.root_device,
            "root_inode": paths.root_inode,
            "database": paths.database,
            "image": APPROVED_IMAGE,
            "port": args.port,
        },
    )
    provider = await receive_provider(directory)
    save_json(
        directory,
        "provider-bounds.json",
        provider.model_dump(
            mode="json",
            exclude={"api_key", "base_url"},
        ),
    )
    await initialize_identity(paths.database)
    sandbox_path = paths.root / "sandbox.json"
    sandbox_path.write_text(
        SandboxConfiguration(
            broker_socket=paths.socket,
            broker_owner_uid=0,
            quotas=QuotaPolicy(defaults=approved_limits()),
        ).model_dump_json()
    )
    sandbox_path.chmod(0o600)
    development_path = paths.root / "development.json"
    development_path.write_text(
        DevelopmentConfiguration(
            image_id=APPROVED_IMAGE,
            broker_environments_root=paths.environments,
            private_state_root=paths.private_state,
            memory_bytes=1024**3,
            pids_limit=128,
            max_steps=32,
            max_tool_calls=64,
        ).model_dump_json()
    )
    development_path.chmod(0o600)
    env = {key: value for key, value in os.environ.items() if not key.startswith("KRIM_AGENT_")}
    env.update(
        {
            "KRIM_AGENT_PROVIDER": "http",
            "KRIM_AGENT_HTTP_MODE": "agent",
            "KRIM_AGENT_MAX_ACTIVE_RUNS": "1",
            "KRIM_AGENT_RUN_TIMEOUT": "300",
            "KRIM_AGENT_MODEL_TIMEOUT": "90",
            "KRIM_AGENT_CONTEXT_RESERVED_OUTPUT_TOKENS": "4096",
            "KRIM_AGENT_HTTP_BASE_URL": provider.base_url,
            "KRIM_AGENT_HTTP_MODEL": provider.model,
            "KRIM_AGENT_HTTP_WIRE_API": provider.wire_api,
            "KRIM_AGENT_HTTP_API_KEY": provider.api_key.get_secret_value(),
            "KRIM_AGENT_HTTP_MAX_OUTPUT_TOKENS": str(provider.max_output_tokens),
            "KRIM_AGENT_HTTP_CONNECT_TIMEOUT_SECONDS": str(provider.connect_timeout_seconds),
            "KRIM_AGENT_HTTP_READ_TIMEOUT_SECONDS": str(provider.read_timeout_seconds),
            "KRIM_AGENT_HTTP_MAX_REQUEST_BYTES": str(provider.max_request_bytes),
            "KRIM_AGENT_HTTP_MAX_RESPONSE_BYTES": str(provider.max_response_bytes),
            "KRIM_AGENT_HTTP_MAX_EVENT_BYTES": str(provider.max_event_bytes),
            "KRIM_AGENT_HTTP_MAX_EVENTS": str(provider.max_events),
            "KRIM_AGENT_HTTP_MAX_ARGUMENTS_BYTES": str(provider.max_arguments_bytes),
            "KRIM_AGENT_HTTP_MAX_TOOL_CALLS": str(provider.max_tool_calls),
            "KRIM_AGENT_SANDBOX_CONFIG_FILE": str(sandbox_path),
            "KRIM_AGENT_DEVELOPMENT_CONFIG_FILE": str(development_path),
        }
    )
    event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, event.set)
    child = None
    async with broker_lifecycle(paths):
        try:
            child = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "krim_agent",
                "serve",
                "--database",
                str(paths.database),
                "--port",
                str(args.port),
                env=env,
            )
            env.pop("KRIM_AGENT_HTTP_API_KEY")
            async with httpx.AsyncClient(
                base_url=f"http://127.0.0.1:{args.port}",
                headers={"Authorization": "Bearer " + test_access_token()},
                timeout=60,
            ) as client:
                async with asyncio.timeout(60):
                    while True:
                        if child.returncode is not None:
                            raise RuntimeError("production CLI exited before readiness")
                        try:
                            response = await client.get("/healthz")
                            if response.status_code == 200:
                                break
                        except httpx.TransportError:
                            pass
                        await asyncio.sleep(0.2)
                await prepare_project(client, directory)
                stopped = asyncio.create_task(event.wait())
                exited = asyncio.create_task(child.wait())
                await asyncio.wait([stopped, exited], return_when=asyncio.FIRST_COMPLETED)
                stopped.cancel()
                await asyncio.gather(stopped, return_exceptions=True)
        finally:
            if child is not None and child.returncode is None:
                child.terminate()
                try:
                    await asyncio.wait_for(child.wait(), 15)
                except TimeoutError:
                    child.kill()
                    await child.wait()
            # Keep this run's fresh database and project files for inspection.
            # Stop its exact environment through the API before preview_stop.
            print("STOPPED: production CLI and Broker; test state retained", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--port", type=int, default=5204)
    parser.add_argument("--resume", action="store_true")
    arguments = parser.parse_args()
    if not arguments.name.startswith("chat-shell-") or "/" in arguments.name:
        parser.error("name must be a single chat-shell- directory name")
    asyncio.run(run(arguments))
