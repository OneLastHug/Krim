"""Opt-in real model + Web + remote MCP acceptance through production session APIs.

Default use is --check-config (no network/containers). --preflight explicitly
permits service calls; --run-real additionally creates isolated Broker/runsc/XFS
resources. Never install, pull an image, commit, push, or access a user database.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import secrets
import shlex
import shutil
import stat
import tempfile
import time
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

from sqlalchemy import select

from krim_agent.account_mcp.secrets import MasterKeyring
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.context.compaction import PROMPT
from krim_agent.dev_environment import docker
from krim_agent.dev_environment.model import RuntimeSpec
from krim_agent.development.configuration import DevelopmentConfiguration
from krim_agent.development.contracts import (
    AuthorizationRef,
    CommandRequest,
    EnvironmentHandle,
    RunOwnership,
)
from krim_agent.providers.openai_compatible import OpenAICompatibleProvider
from krim_agent.providers.types import ModelUsage
from krim_agent.sandbox.configuration import SandboxConfiguration
from krim_agent.storage.models import compactions
from tests.agent_core.test_account_api import initialize
from tests.coding_agent.external_acceptance import (
    AcceptanceError,
    failure,
    grade_evidence,
    load_services,
    preflight,
)
from tests.coding_agent.external_challenge import (
    INPUT_CSV,
    TASK_CONTRACT,
    grade_csv_output,
    make_challenge,
)
from tests.coding_agent.verify_development_broker import (
    APPROVED_IMAGE,
    APPROVED_STORAGE_ROOT,
    Paths,
    broker_lifecycle,
    cleanup_containers,
    quota_preflight,
    require_platform,
)
from tests.coding_agent.verify_general_tools_real import client_for, request
from tests.coding_agent.verify_sqlite_terminal import RecordingDocker


class ObservedProvider(OpenAICompatibleProvider):
    """Count request-local images/usage without logging prompts or wire payloads."""

    def __init__(self, config, observed):
        super().__init__(config)
        self.observed = observed

    async def stream_with_budget(self, messages, tools, budget):
        self.observed["requests"] += 1
        self.observed["image_requests"] += int(any(getattr(m, "images", None) for m in messages))
        self.observed["summary_requests"] += int(
            bool(messages) and messages[0].content.startswith(PROMPT)
        )
        async for event in super().stream_with_budget(messages, tools, budget):
            if isinstance(event, ModelUsage) and event.final:
                self.observed["usage"].append(event.usage.model_dump(mode="json"))
            yield event


class Host:
    def __init__(self, services, paths, output, token, report):
        self.services, self.paths, self.output = services, paths, output
        self.token, self.report = token, report
        self.app = self.client = self.handle = None
        self.project = None
        self.providers, self.workers = [], []
        self.docker = RecordingDocker(output / "created-containers.json")
        self.observed = {"requests": 0, "image_requests": 0, "summary_requests": 0, "usage": []}

    async def open(self):
        services, paths = self.services, self.paths
        sandbox_file = paths.root / "sandbox.json"
        if not sandbox_file.exists():
            sandbox_file.write_text(
                SandboxConfiguration(
                    broker_socket=paths.socket,
                    broker_owner_uid=0,
                ).model_dump_json()
            )
            sandbox_file.chmod(0o600)
            MasterKeyring.create(secrets.token_bytes(32)).save(paths.root / "mcp-key.json")
        settings = Settings(
            database=paths.database,
            provider="http",
            http_mode="agent",
            sandbox_config_file=sandbox_file,
            personal_mcp_key_file=paths.root / "mcp-key.json",
            max_request_bytes=services.provider.max_request_bytes,
            context_max_bytes=services.provider.max_request_bytes,
        )
        configuration = DevelopmentConfiguration(
            image_id=APPROVED_IMAGE,
            broker_environments_root=paths.environments,
            private_state_root=paths.private_state,
            model_images=True,
            max_steps=services.scenario.max_steps,
            max_tool_calls=services.scenario.max_tool_calls,
        )
        provider = ObservedProvider(services.provider, self.observed)
        self.providers.append(provider)
        tools = replace(
            services.tools,
            web_result_root=paths.root / "web",
            mcp_servers=(services.mcp,) if services.scenario.mcp_scope == "host" else (),
        )
        self.app = await open_application(
            paths.database,
            settings=settings,
            provider=provider,
            development_configuration=configuration,
            tool_config=tools,
        )
        self.app.development.environments.runner = self.docker
        if self.app.development._worker_process is not None:
            self.workers.append(self.app.development._worker_process)
        self.client = await client_for(self.app, self.token)

    async def reopen(self, run_id):
        parked = await self.app.service.wait(run_id)
        if parked.status != "awaiting":
            raise AcceptanceError("question was not durably parked")
        await self.client.aclose()
        await self.app.close()
        await self.providers[-1].close()
        self.app = self.client = None
        await self.open()

    async def setup(self):
        await self.open()
        me = await request(self.client, "GET", "/v2/me")
        self.owner = me["principal_id"]
        self.workspace = me["workspaces"][0]["workspace_id"]
        self.project = await request(
            self.client,
            "POST",
            "/v2/me/sandbox/projects",
            {
                "request_key": "project",
                "title": "External services acceptance",
                "workspace_id": self.workspace,
            },
        )
        project_id = self.project["project_id"]
        self.report["quota_preflight"] = await asyncio.to_thread(
            quota_preflight, self.paths, self.project["environment_id"]
        )
        value = await request(
            self.client,
            "POST",
            f"/v2/me/development/projects/{project_id}/prepare",
            {"request_key": "prepare"},
        )
        self.handle = EnvironmentHandle.model_validate(value)
        self.content = (
            self.paths.environments
            / self.project["environment_id"]
            / "projects"
            / project_id
            / "content"
        )
        await request(
            self.client,
            "PUT",
            f"/v2/me/development/projects/{project_id}/grant",
            {
                "expected_revision": 0,
                "expires_at": time.time() + self.services.scenario.timeout_seconds + 300,
                "requires_approval": True,
            },
        )
        base = f"/v2/workspaces/{self.workspace}/sessions"
        session = await request(
            self.client,
            "POST",
            base,
            {
                "request_key": "session",
                "title": "Real external task",
                "target_key": "target-development",
            },
        )
        self.route = base + "/" + session["id"]
        self.session_id = session["id"]
        await request(
            self.client,
            "PUT",
            f"/v2/me/sandbox/sessions/{session['id']}/project",
            {
                "request_key": "bind",
                "project_id": project_id,
                "expected_revision": 0,
                "expected_session_revision": 0,
            },
        )
        await request(
            self.client,
            "PUT",
            "/v2/me/memory-preference",
            {"request_key": "enable-memory", "expected_revision": 0, "enabled": True},
        )
        await request(
            self.client,
            "PUT",
            self.route + "/extensions",
            {
                "request_key": "session-memory",
                "expected_revision": 0,
                "memory_enabled": True,
                "skills": [],
            },
        )
        if self.services.scenario.mcp_scope == "account":
            mcp = self.services.mcp
            self.connection = await request(
                self.client,
                "POST",
                "/v2/me/mcp-connections",
                {
                    "request_key": "mcp",
                    "name": mcp.name,
                    "url": mcp.url,
                    "allowed_hosts": sorted(mcp.allowed_hosts),
                    "selected_tools": list(mcp.selected_tools),
                    "readonly_tools": sorted(mcp.readonly_tools),
                    "credentials": {
                        key: value.get_secret_value() for key, value in mcp.headers.items()
                    },
                    "timeout_seconds": mcp.timeout_seconds,
                },
            )
            await request(
                self.client,
                "PUT",
                self.route + "/mcp",
                {
                    "request_key": "select-mcp",
                    "expected_revision": 0,
                    "connections": [
                        {
                            "connection_id": self.connection["id"],
                            "revision": self.connection["revision"],
                        }
                    ],
                },
            )
        self.content = (
            self.paths.environments
            / self.project["environment_id"]
            / "projects"
            / project_id
            / "content"
        )

    async def submit(self, text, key):
        session = await request(self.client, "GET", self.route)
        return await request(
            self.client,
            "POST",
            self.route + "/turns",
            {"request_key": key, "expected_revision": session["revision"], "text": text},
        )

    async def close(self):
        errors = self.report.setdefault("cleanup_errors", [])
        if self.client is not None:
            try:
                await self.client.aclose()
            except Exception as error:
                errors.append(failure(error))
        if self.app is not None:
            # Stop dispatch and join producers before stopping the project mount.
            # Application.close repeats these idempotent closes after environment
            # cleanup; the store must remain open for verified container stopping.
            for service in (self.app.sessions, self.app.service):
                try:
                    await service.close()
                except Exception as error:
                    errors.append(failure(error))
            try:
                if self.handle is not None:
                    with self.app.development.host_cleanup():
                        await self.app.development.environments.stop(self.handle)
            except Exception as error:
                errors.append(failure(error))
            try:
                await self.app.close()
            except Exception as error:
                errors.append(failure(error))
        for provider in self.providers:
            try:
                await provider.close()
            except Exception as error:
                errors.append(failure(error))
        for process in self.workers:
            try:
                if process.returncode is None:
                    process.terminate()
                    try:
                        await asyncio.wait_for(process.wait(), 10)
                    except TimeoutError:
                        process.kill()
                        await process.wait()
                self.report.setdefault("supervisor_exit_codes", []).append(process.returncode)
            except Exception as error:
                errors.append(failure(error))
        project = self.project or {}
        outcomes, blockers = await cleanup_containers(
            self.docker,
            image=APPROVED_IMAGE,
            owner_id=getattr(self, "owner", None),
            project_id=project.get("project_id"),
            environment_id="dev-" + project["project_id"] if project else None,
            content=getattr(self, "content", None),
        )
        # Cleanup helper messages can include local paths, so do not persist their text.
        self.report["container_cleanup"] = outcomes
        self.report["cleanup_blocker_count"] = len(blockers)
        self.report["model"] = self.observed


@asynccontextmanager
async def isolated_host(services, output, report):
    root = Path(tempfile.mkdtemp(prefix="external-real-", dir=APPROVED_STORAGE_ROOT))
    identity = await asyncio.to_thread(root.lstat)
    (root / "broker-state").mkdir(mode=0o700)
    paths = Paths(
        root,
        identity.st_dev,
        identity.st_ino,
        root / "broker-state",
        root / "private",
        root / "broker.sock",
        root / "app.sqlite",
        root / "environments",
    )
    stack, host = AsyncExitStack(), None
    try:
        await stack.enter_async_context(broker_lifecycle(paths))
        token = await initialize(paths.database)
        host = Host(services, paths, output, token, report)
        await host.setup()
        yield host
    finally:
        try:
            if host is not None:
                await host.close()
        except Exception as error:
            report.setdefault("cleanup_errors", []).append(failure(error))
        try:
            await stack.aclose()
        except Exception as error:
            report.setdefault("cleanup_errors", []).append(failure(error))
        current = await asyncio.to_thread(root.lstat)
        linked = await asyncio.to_thread(root.is_symlink)
        if (
            not report.get("cleanup_errors")
            and not report.get("cleanup_blocker_count")
            and not linked
            and (current.st_dev, current.st_ino) == (identity.st_dev, identity.st_ino)
        ):
            await asyncio.to_thread(shutil.rmtree, root)
            report["temporary_root_removed"] = True
        else:
            report["retained_root"] = str(root)


def task_prompt(services, public_tool):
    scenario = services.scenario
    source = ("host:" if scenario.mcp_scope == "host" else "account:") + services.mcp.name
    resource = (
        f"读取MCP资源 {source} 的 {scenario.mcp_resource_uri} 并在sources.json注明。"
        if scenario.mcp_resource_uri is not None
        else "本轮不要求MCP资源能力。"
    )
    return (
        "在这个一次性测试项目完成CSV校验CLI。只用Python标准库，不安装依赖，不提交或推送。"
        "先enter_plan_mode并创建计划、设置Goal；用ask_user提出一个mode=text的问题确认标准库约定，"
        "然后exit_plan_mode等待确认。宿主会回答并逐次批准本项目的write/edit/shell。"
        f"\n用tool_search发现web_search、web_fetch及{public_tool}。"
        f"真实搜索：{scenario.web_query}；读取并引用{scenario.web_url}。"
        f"远端MCP任务：{scenario.mcp_task}；必须实际调用{public_tool}，不能只列目录。"
        f"本轮只读调用参数参考：{json.dumps(scenario.mcp_arguments, ensure_ascii=False)}。"
        f"{resource}\n用view_image看项目的challenge.png，不能假设图片顺序。"
        f"\n{TASK_CONTRACT}\n样例输入是input.csv。实现validator.py，写测试并运行，"
        "读实际read_output和退出码；失败要修复。将查阅资料落实到实现或测试，"
        '写sources.json，格式为{"web":["实际引用URL"],"mcp":['
        '{"server":"实际来源","tool":"远端工具名","note":"实际参考内容摘要"}]}。'
        "将本项目使用Python标准库csv/unittest的约定存入project记忆。"
        "第一轮报告实现进度，Goal保持active，宿主将顺序续跑；内部续跑核实测试和计划后再完成Goal。"
        "使用compact_context请求整理上下文，继续时不要重放已执行的写入。"
    )


def answer_body(pending):
    body = {"request_key": "answer-" + pending["id"], "expected_revision": pending["revision"]}
    if pending["kind"] == "plan_confirmation":
        return body | {"confirmed": True, "answers": []}
    answers = []
    for question in pending["payload"]["questions"]:
        if question["mode"] != "text":
            raise AcceptanceError("harness only answers the requested text confirmation")
        answers.append(
            {
                "question_id": question["id"],
                "option_ids": [],
                "text": "确认只使用Python标准库；继续本项目验收，不安装、不提交、不推送。",
            }
        )
    return body | {"confirmed": None, "answers": answers}


async def drive(host):
    answered, approved = set(), set()
    restarted = False
    async with asyncio.timeout(host.services.scenario.timeout_seconds):
        while True:
            pending = (await request(host.client, "GET", host.route + "/interactions"))["pending"]
            for item in pending:
                if item["id"] in answered:
                    continue
                if item["kind"] == "user_input" and not restarted:
                    await host.reopen(item["run_id"])
                    restarted = True
                await request(
                    host.client,
                    "POST",
                    host.route + "/interactions/" + item["id"] + "/answer",
                    answer_body(item),
                )
                answered.add(item["id"])
            history = await request(host.client, "GET", host.route + "/turns?limit=50")
            for turn in history:
                if turn["status"] in {"failed", "cancelled", "interrupted"}:
                    raise AcceptanceError("a model-driven turn failed")
                if turn["run_id"] is None:
                    continue
                approvals = await request(
                    host.client, "GET", host.route + "/turns/" + turn["id"] + "/approvals"
                )
                for item in approvals:
                    if item["request_id"] in approved:
                        continue
                    args = item["arguments"]
                    allow = (
                        item["name"] in {"write", "edit", "shell", "memory_write"}
                        and args.get("lifecycle", "run") == "run"
                        and not args.get("secret_refs")
                        and not args.get("download_policy")
                        and (item["name"] != "memory_write" or args.get("scope") == "project")
                    )
                    await request(
                        host.client,
                        "POST",
                        host.route + "/turns/" + turn["id"] + "/approvals/" + item["request_id"],
                        {
                            "request_key": "approve-" + item["request_id"],
                            "allow": allow,
                            **{k: item[k] for k in ("call_id", "arguments_hash", "tool_hash")},
                        },
                    )
                    approved.add(item["request_id"])
            goal = await request(host.client, "GET", host.route + "/goal")
            if (
                history
                and goal["status"] == "completed"
                and all(item["status"] == "completed" for item in history)
            ):
                return history, restarted, len(answered), len(approved)
            if goal["status"] in {"blocked", "cancelled", "paused"}:
                raise AcceptanceError("goal did not complete")
            await asyncio.sleep(0.2)


def read_artifact(content, name):
    path = content / name
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 4 * 1024**2:
            raise AcceptanceError("artifact must be a regular bounded test file")
        with os.fdopen(os.dup(descriptor), "rb") as stream:
            return stream.read()
    finally:
        os.close(descriptor)


def grade_holdout(raw, order):
    # Validate the original envelope first; never let normalization hide duplicate
    # keys/lines or wrong types. The project receives reversed data, not the sample.
    grade_csv_output(raw, order=order)
    value = json.loads(raw)
    if [row["line"] for row in value["rows"]] != list(range(2, 8)):
        raise AcceptanceError("grader rows are incomplete or out of order")
    normalized = {"rows": [dict(row, line=9 - row["line"]) for row in reversed(value["rows"])]}
    return grade_csv_output(json.dumps(normalized).encode(), order=order)


async def grade_program(host, order):
    # Run generated code ONLY in runsc. Never import/evaluate the project on the host.
    filename = ".grader-" + uuid4().hex + ".csv"
    rows = INPUT_CSV.splitlines(keepends=True)
    holdout = rows[0] + b"".join(reversed(rows[1:]))
    await asyncio.to_thread((host.content / filename).write_bytes, holdout)
    record = await host.app.development.repositories.environments.resolve(host.handle)
    ownership = RunOwnership(
        owner_id=host.owner, session_id=host.session_id, run_id="grade-" + uuid4().hex
    )
    command = CommandRequest(
        operation_id="grade-" + uuid4().hex,
        environment=record.environment,
        ownership=ownership,
        authorization=AuthorizationRef(
            actor_id=host.owner,
            actor_generation=1,
            project_id=host.project["project_id"],
            arguments_digest="0" * 64,
        ),
        command="python3 -I validator.py " + shlex.quote(filename),
        timeout_seconds=30,
    )
    spec = RuntimeSpec(record, command, "grade-" + uuid4().hex)
    created = await docker.checked(host.docker, docker.create_args(spec))
    spec = replace(spec, execution_id=created)
    inspected = await docker.inspect(host.docker, spec)
    if inspected is None or docker.verify(spec, inspected) != "stopped":
        raise AcceptanceError("grader container binding could not be verified")
    reply = await host.docker.run(("start", "--attach", created))
    if reply.code != 0:
        raise AcceptanceError("independent container execution failed")
    result = grade_holdout(reply.stdout.encode(), order)
    if not result["passed"]:
        raise AcceptanceError("independent CSV oracle rejected output")
    return result


async def account_checks(host):
    """Local ownership/revocation API checks; never revoke an upstream credential."""
    if host.services.scenario.mcp_scope != "account":
        return {"status": "not_requested"}
    created = await request(
        host.client,
        "POST",
        "/v2/admin/accounts",
        {"request_key": "other-account", "display_name": "Acceptance observer"},
    )
    other_id = created.get("account", created)["principal_id"]
    key = await request(
        host.client,
        "POST",
        f"/v2/admin/accounts/{other_id}/key/issue",
        {"request_key": "other-account-key"},
    )
    headers = {"Authorization": "Bearer " + key["token"]}
    listed = await host.client.get("/v2/me/mcp-connections", headers=headers)
    foreign_session = await host.client.get(host.route, headers=headers)
    if (
        listed.status_code != 200
        or listed.json() != []
        or foreign_session.status_code not in {403, 404}
    ):
        raise AcceptanceError("account MCP or session ownership isolation failed")
    connection = host.connection
    await request(
        host.client,
        "DELETE",
        "/v2/me/mcp-connections/" + connection["id"],
        {"request_key": "delete-test-connection", "expected_revision": connection["revision"]},
    )
    selection = await request(host.client, "GET", host.route + "/mcp")
    response = await host.client.put(
        host.route + "/mcp",
        json={
            "request_key": "reject-deleted-connection",
            "expected_revision": selection["revision"],
            "connections": [
                {"connection_id": connection["id"], "revision": connection["revision"]}
            ],
        },
    )
    if selection["connections"] or response.status_code != 404:
        raise AcceptanceError("deleted account MCP connection was still selectable")
    return {
        "status": "passed",
        "other_account_connections": 0,
        "foreign_session_denied": True,
        "deleted_connection_reselection_denied": True,
    }


async def run_joint(services, output, report):
    challenge = make_challenge()
    async with isolated_host(services, output, report) as host:
        # Only pixels and public sample data enter the project, never the oracle.
        for name, raw in (("challenge.png", challenge.image), ("input.csv", INPUT_CSV)):
            await asyncio.to_thread((host.content / name).write_bytes, raw)
        public_tool = report["preflight"]["mcp"]["public_name"]
        await host.submit(task_prompt(services, public_tool), "task")
        history, restarted, answers, approvals = await drive(host)
        # Keep the production keep_turns=2 policy: create a third completed turn
        # before asking to compact a historical prefix, rather than weakening it.
        await host.submit("不修改文件，简要确认当前计划和Goal完成状态。", "retain-state")
        await drive(host)
        # Explicit host request tests the same manual compaction used by the UI.
        await request(
            host.client,
            "POST",
            host.route + "/context/compactions",
            {"request_key": "manual-compact", "reason": "external acceptance follow-up"},
        )
        await host.submit(
            "保留已完成状态，不修改文件。确认整理后仍能读取计划、Goal与项目约定。", "followup"
        )
        history, _, _, _ = await drive(host)
        events = []
        for turn in history:
            snapshot = await host.app.store.snapshot(turn["run_id"])
            events.extend(snapshot.events)
        async with host.app.store.engine.connect() as conn:
            count = len((await conn.execute(select(compactions))).all())
        plan = await request(host.client, "GET", host.route + "/plan")
        goal = await request(host.client, "GET", host.route + "/goal")
        checks = grade_evidence(
            events,
            services,
            restarted=restarted,
            compactions=count,
            goal=goal,
            plan=plan,
            mcp_tool_name=public_tool,
        )
        checks["image_delivered"] = host.observed["image_requests"] > 0
        checks["goal_continuation"] = any(
            t["request_key"].startswith("goal-continue:") for t in history
        )
        raw_sources = await asyncio.to_thread(read_artifact, host.content, "sources.json")
        sources = json.loads(raw_sources)
        checks["web_citation"] = services.scenario.web_url in sources.get("web", [])
        checks["mcp_citation"] = any(
            item.get("tool") == services.scenario.mcp_tool and item.get("note")
            for item in sources.get("mcp", [])
        )
        memories = await request(
            host.client,
            "GET",
            f"/v2/workspaces/{host.workspace}/projects/" + host.project["project_id"] + "/memories",
        )
        checks["project_memory"] = bool(memories["items"])
        report["checks"] = checks
        report["turns"] = len(history)
        report["interaction_answers"], report["approvals"] = answers, approvals
        report["artifact_hashes"] = {
            name: hashlib.sha256(
                await asyncio.to_thread(read_artifact, host.content, name)
            ).hexdigest()
            for name in ("validator.py", "sources.json")
        }
        report["program"] = await grade_program(host, challenge.order)
        report["account_checks"] = await account_checks(host)
        if not all(checks.values()):
            raise AcceptanceError("recorded task evidence is incomplete")
        report["passed"] = True


async def execute(services, output, *, full):
    if full:
        require_platform(APPROVED_STORAGE_ROOT)  # Before paid requests or evidence creation.
    if not output.is_absolute() or any(p.is_symlink() for p in (output, *output.parents)):
        raise AcceptanceError("evidence needs a new absolute non-symlink directory")
    await asyncio.to_thread(output.mkdir, mode=0o700, parents=True, exist_ok=False)
    report = {
        "mode": "joint" if full else "preflight",
        "passed": False,
        "configuration": services.summary(),
        "mcp_resource_coverage": "requested"
        if services.scenario.mcp_resource_uri
        else "not_requested",
    }
    started = time.monotonic()
    try:
        async with asyncio.timeout(services.scenario.timeout_seconds):
            report["preflight"] = await preflight(services)
            if full:
                await run_joint(services, output, report)
            else:
                report["passed"] = True
    except Exception as error:
        report["error"] = failure(error)
        report["passed"] = False
    finally:
        if report.get("cleanup_errors") or report.get("cleanup_blocker_count"):
            report["passed"] = False
        report["seconds"] = round(time.monotonic() - started, 2)
        await asyncio.to_thread(
            (output / "result.json").write_text, json.dumps(report, ensure_ascii=False, indent=2)
        )
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--evidence", type=Path)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check-config", action="store_true")
    mode.add_argument("--preflight", action="store_true")
    mode.add_argument("--run-real", action="store_true")
    args = parser.parse_args()
    try:
        services = load_services(args.config)
        if args.check_config:
            print(json.dumps(services.summary(), ensure_ascii=False, indent=2))
            return
        if args.evidence is None:
            raise AcceptanceError("--evidence is required for service calls")
        report = asyncio.run(execute(services, args.evidence, full=args.run_real))
        print(json.dumps(report, ensure_ascii=False, indent=2))
        if not report["passed"]:
            raise SystemExit(1)
    except Exception as error:
        print(json.dumps({"passed": False, "error": failure(error)}))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
