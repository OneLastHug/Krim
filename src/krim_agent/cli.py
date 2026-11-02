"""Krim Agent CLI：写端显式装配，冷读取只打开只读库。"""

import argparse
import asyncio
import json
import os
import signal
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast
from urllib.parse import quote

from krim_agent import __version__
from krim_agent.client import AsyncRunClient
from krim_agent.composition import Application, open_application
from krim_agent.config import Settings
from krim_agent.contracts import TERMINAL, RunRequest, ServiceError, parse_json
from krim_agent.evaluation.regression import gate_reports, make_report, validate_report
from krim_agent.evaluation.service import eval_view
from krim_agent.evaluation.trace_cases import collect_candidate
from krim_agent.evaluation.types import EvalRequest, FreezeRequest, RuleName, rule
from krim_agent.observability import project_trace
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig
from krim_agent.runtime.facts import project_facts
from krim_agent.sdk.sessions import SessionSDK
from krim_agent.sessions.types import ControlRequest, SessionCreate, TurnRequest
from krim_agent.storage.sqlite import SQLiteStore


def freeze_request(args: argparse.Namespace) -> FreezeRequest:
    case: dict[str, Any] = {
        "case_id": args.case_id,
        "reason": args.reason,
        "reference": {
            "expected": parse_json(args.expected_json),
            "expected_status": args.status,
            "expected_code": args.code,
            "expected_tools": None,
            "trajectory": None,
            "expected_schema": None,
        },
    }
    if args.source_run is not None:
        if args.candidate_fingerprint is None:
            raise ServiceError("invalid_input", 422)
        case.update(
            source_kind="run",
            source_run_id=args.source_run,
            candidate_fingerprint=args.candidate_fingerprint,
            selected_input=args.input,
        )
    else:
        if args.candidate_fingerprint is not None:
            raise ServiceError("invalid_input", 422)
        case.update(source_kind="manual", input=args.input)
    return FreezeRequest.model_validate(
        {"name": args.name, "version": args.revision, "cases": [case]}
    )


def eval_request(args: argparse.Namespace) -> EvalRequest:
    return EvalRequest.model_validate(
        {
            "request_key": args.key,
            "dataset": {
                "name": args.name,
                "version": args.revision,
                "fingerprint": args.fingerprint,
            },
            "target_key": args.target,
            "rules": [
                rule(cast(RuleName, n)).model_dump(mode="json") for n in args.rules.split(",")
            ],
            "execution": {
                "repetitions": args.repetitions,
                "concurrency": 1,
                "item_timeout_seconds": 12,
                "scorer_timeout_seconds": 1,
                "batch_timeout_seconds": 600,
            },
        }
    )


@asynccontextmanager
async def owned_eval(
    path: Path,
    http_config: OpenAICompatibleConfig | None = None,
    *,
    settings: Settings | None = None,
) -> AsyncIterator[Application]:
    task = asyncio.current_task()
    assert task is not None
    loop = asyncio.get_running_loop()
    previous = {}
    for signum in [signal.SIGINT, signal.SIGTERM]:
        previous[signum] = signal.getsignal(signum)
        loop.add_signal_handler(signum, task.cancel)
    try:
        opened = (
            await open_application(path, settings=settings, http_config=http_config)
            if http_config is not None or settings is not None
            else await open_application(path)
        )
        async with opened as application:
            yield application
    finally:
        for signum, handler in previous.items():
            loop.remove_signal_handler(signum)
            signal.signal(signum, handler)


async def execute_m2(args: argparse.Namespace) -> Any:
    operation = getattr(args, "operation", "")
    if not args.url and args.command == "eval" and operation in {"wait", "cancel"}:
        raise ServiceError("readonly")
    frozen = freeze_request(args) if args.command == "dataset" and operation == "freeze" else None
    requested = eval_request(args) if args.command == "eval" and operation == "start" else None
    if args.url:
        client = AsyncRunClient(args.url, os.environ.get("KRIM_AGENT_API_KEY", ""))
        try:
            if args.command == "candidate":
                return (await client.candidate(args.id, args.include_input)).model_dump(mode="json")
            if args.command == "dataset":
                if frozen is not None:
                    return (await client.freeze(frozen)).model_dump(mode="json")
                return await client.request(
                    "GET",
                    f"/v1/datasets/{quote(args.name, safe='')}/{quote(args.revision, safe='')}",
                )
            if args.command == "report":
                return await client.report(args.id)
            if args.command == "compare":
                return await client.compare(args.baseline, args.candidate)
            if requested is not None:
                return (await client.eval_start(requested)).model_dump(mode="json")
            if operation == "by-key":
                return (await client.eval_by_key(args.key)).model_dump(mode="json")
            if operation == "get":
                return (await client.eval_get(args.id)).model_dump(mode="json")
            if operation == "wait":
                return (await client.eval_wait(args.id)).model_dump(mode="json")
            return (await client.eval_cancel(args.id)).model_dump(mode="json")
        finally:
            await client.close()
    if frozen is not None:
        async with await open_application(Path(args.database)) as app:
            assert app.evaluations is not None
            return (await app.evaluations.freeze(frozen)).model_dump(mode="json")
    if requested is not None:
        http_config = (
            OpenAICompatibleConfig.from_env() if requested.target_key == "target-http" else None
        )
        settings = Settings.from_env() if requested.target_key == "target-http" else None
        async with owned_eval(
            Path(args.database), http_config=http_config, settings=settings
        ) as app:
            service = app.evaluations
            assert service is not None
            view = await service.start(requested)
            await service.wait(view.id)
            return await service.report(view.id)
    store = await SQLiteStore.open(Path(args.database), readonly=True)
    try:
        if args.command == "candidate":
            return collect_candidate(await store.snapshot(args.id), args.include_input).model_dump(
                mode="json"
            )
        if args.command == "dataset":
            return (await store.dataset(args.name, args.revision)).model_dump(mode="json")
        if args.command == "compare":
            before, after = (
                await store.eval_snapshot(args.baseline),
                await store.eval_snapshot(args.candidate),
            )
            return gate_reports(
                validate_report(before["report"] or make_report(before)),
                validate_report(after["report"] or make_report(after)),
            )
        if args.command == "report":
            snapshot = await store.eval_snapshot(args.id)
            return validate_report(snapshot["report"] or make_report(snapshot))
        found_snapshot = (
            await store.eval_by_key(args.key)
            if operation == "by-key"
            else await store.eval_snapshot(args.id)
        )
        if found_snapshot is None:
            raise ServiceError("not_found", 404)
        return eval_view(found_snapshot).model_dump(mode="json")
    finally:
        await store.close()


async def execute_sessions(args: argparse.Namespace) -> Any:
    operation = args.operation
    if not args.url and operation in {"close", "resume", "cancel", "wait"}:
        raise ServiceError("readonly")

    async def perform(sdk: SessionSDK) -> Any:
        if args.command == "session":
            if operation == "create":
                return (
                    await sdk.create(
                        SessionCreate(
                            request_key=args.key, title=args.title, target_key=args.target
                        )
                    )
                ).model_dump(mode="json")
            if operation == "list":
                return [v.model_dump(mode="json") for v in await sdk.list()]
            if operation == "by-key":
                return (await sdk.by_key(args.key)).model_dump(mode="json")
            if operation == "get":
                return (await sdk.get(args.id)).model_dump(mode="json")
            return (
                await sdk.control(
                    args.id,
                    operation,
                    ControlRequest(request_key=args.key, expected_revision=args.revision),
                )
            ).model_dump(mode="json")
        if operation == "start":
            session = await sdk.get(args.session)
            if not args.url and session.dispatch_paused:
                raise ServiceError("invalid_input", 422)
            value = await sdk.submit(
                args.session,
                TurnRequest(request_key=args.key, text=args.text, expected_revision=args.revision),
            )
            if not args.url:
                value = await sdk.wait(args.session, value.id)
            return value.model_dump(mode="json")
        if operation == "by-key":
            return (await sdk.turn_by_key(args.session, args.key)).model_dump(mode="json")
        if operation == "history":
            return [v.model_dump(mode="json") for v in await sdk.history(args.session)]
        if operation == "result":
            return (await sdk.result(args.session, args.id)).model_dump(mode="json")
        if operation == "trace":
            return await sdk.trace(args.session, args.id)
        if operation == "cancel":
            return (
                await sdk.control(
                    args.session,
                    "cancel_turn",
                    ControlRequest(request_key=args.key, expected_revision=args.revision),
                    args.id,
                )
            ).model_dump(mode="json")
        return (
            await (
                sdk.wait(args.session, args.id)
                if operation == "wait"
                else sdk.turn(args.session, args.id)
            )
        ).model_dump(mode="json")

    if args.url:
        client = AsyncRunClient(args.url, os.environ.get("KRIM_AGENT_API_KEY", ""))
        try:
            return await perform(SessionSDK(client, workspace=args.workspace))
        finally:
            await client.close()
    writing = operation in {"create", "start"}
    if writing:
        settings = Settings.from_env()
        target = getattr(args, "target", "demo")
        if operation == "start" and settings.provider != "http":
            source = await SQLiteStore.open(Path(args.database), readonly=True)
            try:
                target = (
                    await SessionSDK(source, workspace=args.workspace).get(args.session)
                ).target_key
            finally:
                await source.close()
        http = (
            OpenAICompatibleConfig.from_env()
            if settings.provider == "http" or target == "target-http"
            else None
        )
        if target == "target-http" and http is None:
            raise ServiceError("provider_config", 422)
        async with owned_eval(Path(args.database), http_config=http, settings=settings) as app:
            assert app.sessions is not None
            return await perform(SessionSDK(app.sessions, workspace=args.workspace))
    store = await SQLiteStore.open(Path(args.database), readonly=True)
    try:
        return await perform(SessionSDK(store, workspace=args.workspace))
    finally:
        await store.close()


async def execute_workspace(args: argparse.Namespace) -> Any:
    from krim_agent.sdk.workspace import WorkspaceSDK
    from krim_agent.workspace.types import InputSnapshotRequest, WorkspaceTurnRequest

    operation = args.operation
    if not args.url and operation in {"start", "input"}:
        # 独立本机CLI只有冷读权；开库/读请求/Provider之前拒绝写组合。
        raise ServiceError("readonly")
    body = None
    if operation in {"start", "input"}:
        import stat
        path = Path(args.request)
        info = await asyncio.to_thread(path.lstat)
        if not stat.S_ISREG(info.st_mode) or info.st_size > 65536:
            raise ServiceError("invalid_input", 422)
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            raw = os.read(fd, 65537)
        finally:
            os.close(fd)
        try:
            body = (WorkspaceTurnRequest.model_validate_json(raw) if operation == "start"
                    else InputSnapshotRequest.model_validate_json(raw))
        except ValueError:
            raise ServiceError("invalid_input", 422) from None

    async def perform(sdk: WorkspaceSDK) -> Any:
        value: Any
        if operation == "start":
            assert isinstance(body, WorkspaceTurnRequest)
            return (await sdk.start(args.session, body)).model_dump(mode="json")
        if operation == "input":
            assert isinstance(body, InputSnapshotRequest)
            return (await sdk.input(args.session, body)).model_dump(mode="json")
        if operation == "artifact":
            value = (await sdk.chunk(args.session, args.revision, args.path,
                offset=args.offset, length=args.length) if args.path else
                await sdk.artifact(args.session, args.revision))
        elif operation == "verify":
            value = await sdk.verify(args.session, args.turn)
        elif args.input_key:
            value = await sdk.input_by_key(args.session, args.input_key)
        elif args.key:
            value = await sdk.by_key(args.session, args.key)
        else:
            value = await sdk.get(args.session, args.turn)
        return value.model_dump(mode="json") if value is not None else None

    if args.url:
        client = AsyncRunClient(args.url, os.environ.get("KRIM_AGENT_API_KEY", ""))
        try:
            return await perform(WorkspaceSDK(client, workspace=args.workspace))
        finally:
            await client.close()
    store = await SQLiteStore.open(Path(args.database), readonly=True)
    try:
        return await perform(WorkspaceSDK(store, workspace=args.workspace,
            artifact_root=Path(args.artifact_root) if args.artifact_root else None))
    finally:
        await store.close()


async def execute(args: argparse.Namespace) -> Any:
    if args.command == "workspace":
        return await execute_workspace(args)
    if args.command in {"session", "turn"}:
        return await execute_sessions(args)
    if args.command in {"candidate", "dataset", "eval", "report", "compare"}:
        return await execute_m2(args)
    if args.url:
        client = AsyncRunClient(args.url, os.environ.get("KRIM_AGENT_API_KEY", ""))
        try:
            if args.command == "run":
                run = await client.start(
                    RunRequest(
                        text=args.text, request_key=args.key, target=getattr(args, "target", "demo")
                    )
                )
                await client.wait(run.id)
                return (await client.result(run.id)).model_dump(mode="json")
            if args.command == "cancel":
                return await client.request("POST", "/v1/runs/" + args.id + "/cancel")
            suffix = "" if args.command == "get" else "/" + args.command
            return await client.request("GET", "/v1/runs/" + args.id + suffix)
        finally:
            await client.close()
    if args.command == "run":
        settings = Settings.from_env()
        http_config = (
            OpenAICompatibleConfig.from_env()
            if getattr(args, "target", "demo") == "target-http" or settings.provider == "http"
            else None
        )
        owner = (
            owned_eval(Path(args.database), http_config=http_config, settings=settings)
            if getattr(args, "target", "demo") == "target-http" or settings.provider == "http"
            else await open_application(Path(args.database), settings=settings)
        )
        async with owner as app:
            run = await app.service.start(
                RunRequest(
                    text=args.text, request_key=args.key, target=getattr(args, "target", "demo")
                )
            )
            await app.service.wait(run.id)
            return (await app.service.result(run.id)).model_dump(mode="json")
    if args.command == "cancel":
        raise ServiceError("readonly")
    store = await SQLiteStore.open(Path(args.database), readonly=True)
    try:
        snapshot = await store.snapshot(args.id)
        if args.command == "result" and snapshot.run.status not in TERMINAL:
            raise ServiceError("not_terminal")
        if args.command == "trace":
            return project_trace(snapshot).model_dump(mode="json")
        if args.command == "events":
            return [event.model_dump(mode="json") for event in snapshot.events]
        return (
            snapshot.run.model_dump(mode="json")
            if args.command == "get"
            else project_facts(snapshot).model_dump(mode="json")
        )
    finally:
        await store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Krim Agent")
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="command", required=True)
    serve = sub.add_parser("serve")
    serve.add_argument("--database", required=True)
    serve.add_argument("--port", type=int, default=8765)
    serve.add_argument("--tool-config", type=Path)
    for command in ("run", "get", "result", "trace", "events", "cancel"):
        child = sub.add_parser(command)
        child.add_argument("--database", default=".data/krim-agent.db")
        child.add_argument("--url")
        if command == "run":
            child.add_argument("--text", required=True)
            child.add_argument("--key", required=True)
            child.add_argument(
                "--target",
                default="demo",
                choices=["demo", "target-v1", "target-v2", "target-bad", "target-http"],
            )
        else:
            child.add_argument("--id", required=True)

    def mode(child: argparse.ArgumentParser) -> None:
        group = child.add_mutually_exclusive_group(required=True)
        group.add_argument("--url")
        group.add_argument("--database")

    for command, operations in (
        ("session", ["create", "list", "get", "by-key", "close", "resume"]),
        ("turn", ["start", "get", "by-key", "history", "wait", "cancel", "result", "trace"]),
    ):
        commands = sub.add_parser(command).add_subparsers(dest="operation", required=True)
        for operation in operations:
            child = commands.add_parser(operation)
            mode(child)
            child.add_argument("--workspace", default="local")
            if command == "turn":
                child.add_argument("--session", required=True)
            if operation in {"create", "start", "by-key", "close", "resume", "cancel"}:
                child.add_argument("--key", required=True)
            if operation in {"start", "close", "resume", "cancel"}:
                child.add_argument("--revision", required=True, type=int)
            if operation in {"get", "wait", "close", "resume", "cancel", "result", "trace"}:
                child.add_argument("--id", required=True)
            if operation == "create":
                child.add_argument("--title", default="新对话")
                child.add_argument(
                    "--target",
                    default="demo",
                    choices=["demo", "target-v1", "target-v2", "target-bad", "target-http"],
                )
            if operation == "start":
                child.add_argument("--text", required=True)

    candidate = sub.add_parser("candidate")
    mode(candidate)
    candidate.add_argument("--id", required=True)
    candidate.add_argument("--include-input", action="store_true")
    dataset = sub.add_parser("dataset").add_subparsers(dest="operation", required=True)
    for operation in ["freeze", "get"]:
        child = dataset.add_parser(operation)
        mode(child)
        child.add_argument("--name", required=True)
        child.add_argument("--revision", required=True)
        if operation == "freeze":
            child.add_argument("--case-id", default="case-1")
            child.add_argument("--input", required=True)
            child.add_argument("--source-run")
            child.add_argument("--candidate-fingerprint")
            child.add_argument("--expected-json", required=True)
            child.add_argument("--status", required=True, choices=["completed", "failed"])
            child.add_argument("--code")
            child.add_argument("--reason", required=True)
    evaluation = sub.add_parser("eval").add_subparsers(dest="operation", required=True)
    for operation in ["start", "get", "by-key", "wait", "cancel"]:
        child = evaluation.add_parser(operation)
        mode(child)
        if operation == "start":
            for field in ["name", "revision", "fingerprint", "target", "key"]:
                child.add_argument("--" + field, required=True)
            child.add_argument("--rules", default="status_code,exact_match")
            child.add_argument("--repetitions", type=int, default=1)
        elif operation == "by-key":
            child.add_argument("--key", required=True)
        else:
            child.add_argument("--id", required=True)
    report = sub.add_parser("report")
    mode(report)
    report.add_argument("--id", required=True)
    compare = sub.add_parser("compare")
    mode(compare)
    compare.add_argument("--baseline", required=True)
    compare.add_argument("--candidate", required=True)
    workspace = sub.add_parser("workspace")
    workops = workspace.add_subparsers(dest="operation", required=True)
    for workop in ("start", "input", "get", "artifact", "verify"):
        child = workops.add_parser(workop)
        mode(child)
        child.add_argument("--workspace", default="local")
        child.add_argument("--session", required=True)
        child.add_argument("--artifact-root")
        if workop in {"start", "input"}:
            child.add_argument("--request", required=True)
        elif workop == "artifact":
            child.add_argument("--revision", required=True)
            child.add_argument("--path")
            child.add_argument("--offset", type=int, default=0)
            child.add_argument("--length", type=int, default=16384)
        else:
            child.add_argument("--turn")
            child.add_argument("--key")
            child.add_argument("--input-key")
    args = parser.parse_args()
    try:
        if args.command == "serve":
            import uvicorn

            from krim_agent.api import create_app

            changes: dict[str, Any] = {"database": Path(args.database)}
            if args.tool_config is not None:
                changes["tool_config_file"] = args.tool_config
            settings = Settings.from_env().model_copy(update=changes)
            uvicorn.run(
                create_app(settings, http_config=OpenAICompatibleConfig.from_env()),
                host="127.0.0.1",
                port=args.port,
                access_log=False,
                log_level="warning",
            )
            return
        print(json.dumps(asyncio.run(execute(args)), ensure_ascii=False, allow_nan=False))
    except ServiceError as error:
        print(json.dumps({"error": {"code": error.code}}, ensure_ascii=False))
        raise SystemExit(1) from None
    except (ValueError, KeyError):
        print(json.dumps({"error": {"code": "invalid_input"}}))
        raise SystemExit(1) from None
    except (asyncio.CancelledError, KeyboardInterrupt):
        print(json.dumps({"error": {"code": "cancelled"}}))
        raise SystemExit(130) from None
