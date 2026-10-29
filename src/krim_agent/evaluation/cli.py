"""普通本地CLI；run等待自然终态，读取不启动adapter。"""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
from importlib.resources import files
from pathlib import Path
from typing import Any

from . import domain
from .app import open_eval
from .domain import DatasetRef, DatasetSpec, TaskSpec
from .reports import gate_reports


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="krim-eval")
    root.add_argument("--root", type=Path, required=True)
    commands = root.add_subparsers(dest="command", required=True)
    dataset = commands.add_parser("dataset").add_subparsers(dest="operation", required=True)
    freeze = dataset.add_parser("freeze")
    freeze.add_argument("file", type=Path)
    task = commands.add_parser("task").add_subparsers(dest="operation", required=True)
    run = task.add_parser("run")
    run.add_argument("name")
    run.add_argument("version")
    run.add_argument("fingerprint")
    run.add_argument("--key", required=True)
    run.add_argument("--target", default="reconcile-reference")
    run.add_argument("--profile-file", type=Path)
    for name in ["get", "by-key"]:
        read = task.add_parser(name)
        read.add_argument("id")
        if name == "get":
            projection = read.add_mutually_exclusive_group()
            projection.add_argument("--cases", action="store_true")
            projection.add_argument("--trials", action="store_true")
            read.add_argument("--case-id")
            read.add_argument("--repetition", type=int)
            read.add_argument("--cursor")
            read.add_argument("--limit", type=int)
            read.add_argument("--scope", default="local")
    report = commands.add_parser("report")
    report.add_argument("id")
    report.add_argument("--pages", action="store_true")
    report.add_argument("--cursor")
    report.add_argument("--limit", type=int)
    report.add_argument("--scope", default="local")
    compare = commands.add_parser("compare")
    compare.add_argument("baseline")
    compare.add_argument("candidate")
    compare.add_argument("--pages", action="store_true")
    compare.add_argument("--scope", default="local")
    example = commands.add_parser("example")
    example.add_argument("name", choices=["reconciliation"])
    example.add_argument("--key", required=True)
    commands.add_parser("targets")
    continuous = commands.add_parser("continuous").add_subparsers(dest="area", required=True)
    observe = continuous.add_parser("observe").add_subparsers(dest="operation", required=True)
    imp = observe.add_parser("import")
    imp.add_argument("file", type=Path)
    imp.add_argument("--key", required=True)
    candidates = continuous.add_parser("candidate").add_subparsers(dest="operation", required=True)
    candidates.add_parser("get").add_argument("id")
    confirm = candidates.add_parser("confirm")
    confirm.add_argument("file", type=Path)
    confirm.add_argument("--key", required=True)
    data = continuous.add_parser("dataset").add_subparsers(dest="operation", required=True)
    freeze_continuous = data.add_parser("freeze")
    freeze_continuous.add_argument("file", type=Path)
    freeze_continuous.add_argument("--key", required=True)
    continuous_task = continuous.add_parser("task").add_subparsers(dest="operation", required=True)
    continuous_task.add_parser("submit").add_argument("file", type=Path)
    continuous_compare = continuous.add_parser("compare")
    continuous_compare.add_argument("file", type=Path)
    continuous_compare.add_argument("--pages", action="store_true")
    schedules = continuous.add_parser("schedule").add_subparsers(dest="operation", required=True)
    schedules.add_parser("create").add_argument("file", type=Path)
    for name in ["get", "tick"]:
        schedules.add_parser(name).add_argument("id")
    disable = schedules.add_parser("disable")
    disable.add_argument("id")
    disable.add_argument("--revision", required=True, type=int)
    disable.add_argument("--key", required=True)
    return root


async def execute(args: argparse.Namespace) -> dict[str, Any]:
    root = args.root.absolute()
    if args.command == "continuous":
        import os
        import stat

        from krim_agent.contracts import ServiceError

        from .continuous_reports import compare as continuous_compare
        from .continuous_types import (
            ComparisonPolicy,
            ConfirmationRequest,
            ContinuousRequest,
            FreezeConfirmed,
            ImportRequest,
            ScheduleSpec,
            window_request,
        )

        def input_value() -> dict[str, Any]:
            path = args.file.absolute()
            if not path.is_relative_to(root) or ".." in path.parts:
                raise ServiceError("invalid_input", 422)
            for parent in [path, *path.parents]:
                if parent.is_symlink():
                    raise ServiceError("invalid_input", 422)
            known = path.lstat()
            if not stat.S_ISREG(known.st_mode) or known.st_nlink != 1 or known.st_size > 1048576:
                raise ServiceError("invalid_input", 422)
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                st = os.fstat(fd)
                if (
                    not stat.S_ISREG(st.st_mode)
                    or st.st_nlink != 1
                    or st.st_size > 1048576
                    or (st.st_dev, st.st_ino) != (known.st_dev, known.st_ino)
                ):
                    raise ServiceError("invalid_input", 422)
                raw = os.read(fd, 1048577)
            finally:
                os.close(fd)
            value = domain.neutral_parse(raw.decode())
            if not isinstance(value, dict):
                raise ServiceError("invalid_input", 422)
            if hasattr(args, "key"):
                if value.get("request_key", args.key) != args.key:
                    raise ServiceError("key_conflict")
                value["request_key"] = args.key
            return value

        database = root / "eval.sqlite"
        operation = getattr(args, "operation", None)
        payload = input_value() if hasattr(args, "file") else None
        continuous_request = (
            ContinuousRequest.model_validate(payload) if args.area == "task" else None
        )
        if continuous_request is not None and continuous_request.scope != "local":
            raise ServiceError("not_found", 404)
        readonly = args.area == "compare" or operation == "get"
        execution = continuous_request is not None and continuous_request.mode == "execute"
        if continuous_request is not None and database.exists():
            async with await open_eval(database, readonly=True, execution_enabled=False) as reader:
                old = await reader.repository.by_key(continuous_request.model_dump(mode="json"))
                if old is not None:
                    if old["request_hash"] != domain.neutral_fingerprint(
                        continuous_request.model_dump(mode="json")
                    ):
                        raise ServiceError("key_conflict")
                    return await reader.repository.report(old["id"])
        if args.area == "schedule" and operation == "tick":
            async with await open_eval(database, readonly=True, execution_enabled=False) as reader:
                row = await reader.repository.schedule("local", args.id)
                import time

                index = int((time.time() - row["spec"]["epoch"]) // row["spec"]["interval_seconds"])
                if 0 <= index < row["spec"]["max_windows"]:
                    window = window_request(row, index)
                    old = await reader.repository.by_key(window.model_dump(mode="json"))
                    if old is not None:
                        if old["request_hash"] != domain.neutral_fingerprint(
                            window.model_dump(mode="json")
                        ):
                            raise ServiceError("key_conflict")
                        return await reader.repository.report(old["id"])
                execution = row["spec"]["template"]["mode"] == "execute"
        if args.area == "schedule" and operation == "create":
            schedule_spec = ScheduleSpec.model_validate(payload)
            execution = schedule_spec.template.mode == "execute"
        async with await open_eval(database, readonly=readonly, execution_enabled=execution) as app:
            flow = app.continuous
            if args.area == "observe":
                return {
                    "observations": await flow.import_sources(ImportRequest.model_validate(payload))
                }
            if args.area == "candidate":
                return (
                    await app.repository.candidate("local", args.id)
                    if operation == "get"
                    else await flow.confirm_case(ConfirmationRequest.model_validate(payload))
                )
            if args.area == "dataset":
                return await flow.freeze_dataset(FreezeConfirmed.model_validate(payload))
            if args.area == "task":
                assert continuous_request is not None
                started = await flow.submit(continuous_request)
                await app.kernel.wait(started["id"])
                return await app.repository.report(started["id"])
            if args.area == "compare":
                policy = ComparisonPolicy.model_validate(payload)
                if getattr(args, "pages", False):
                    return await app.repository.compare_paged("local", policy)
                return continuous_compare(
                    policy,
                    await app.repository.report(policy.baseline_id),
                    await app.repository.report(policy.candidate_id),
                )
            if operation == "create":
                return await flow.create_schedule(ScheduleSpec.model_validate(payload))
            if operation == "get":
                return await app.repository.schedule("local", args.id)
            if operation == "disable":
                return await flow.disable(args.id, args.revision, args.key)
            view = await flow.tick(args.id)
            await app.kernel.wait(view["id"])
            return await app.repository.report(view["id"])
    if args.command == "targets":
        return {
            "targets": [
                {"key": "reconcile-reference", "kind": "non-model-reference"},
                {"key": "codex-cli", "status": "requires_M0_profile"},
                {"key": "claude-code", "status": "requires_M0_profile"},
                {"key": "krim-script-v2", "status": "library_host_required"},
            ]
        }
    profiles = []
    target = getattr(args, "target", "reconcile-reference")
    if target != "reconcile-reference":
        from .adapters.profiles import load_profile

        if target not in {"codex-cli", "claude-code"} or args.profile_file is None:
            raise ValueError("native_not_ready")
        profile = load_profile(args.profile_file, target)
        profile.binding.assert_store(root / "eval.sqlite")
        profiles = [profile]
    database = root / "eval.sqlite"
    if (
        args.command in {"report", "compare"}
        or args.command == "task"
        and args.operation
        in {
            "get",
            "by-key",
        }
    ):
        async with await open_eval(database, readonly=True) as app:
            if args.command in {"report", "compare"} or (
                args.command == "task" and args.operation == "get"
            ):
                # 复用现有scope授权入口；所有正文/subject读取必须在它之后。
                identities = (
                    [args.baseline, args.candidate] if args.command == "compare" else [args.id]
                )
                from sqlalchemy import select

                from krim_agent.contracts import ServiceError

                from .repository import tasks as task_rows

                async with app.repository.lock, app.repository.engine.begin() as conn:
                    await conn.exec_driver_sql("BEGIN")
                    for identity in dict.fromkeys(identities):
                        allowed_scope = (
                            await conn.execute(
                                select(task_rows.c.scope).where(task_rows.c.id == identity)
                            )
                        ).scalar_one_or_none()
                        if allowed_scope != args.scope:
                            raise ServiceError("not_found", 404)
            if args.command == "report":
                if getattr(args, "pages", False):
                    page = await app.repository.report_page(
                        args.scope,
                        args.id,
                        cursor=args.cursor,
                        limit=16 if args.limit is None else args.limit,
                    )
                    return {
                        "rows": page.rows,
                        "next_cursor": page.next_cursor,
                        "binding": page.binding,
                        "total_count": page.total_count,
                    }
                if (
                    getattr(args, "cursor", None) is not None
                    or getattr(args, "limit", None) is not None
                ):
                    raise ValueError("page_projection_required")
                return await app.repository.report(args.id)
            if args.command == "compare":
                if getattr(args, "pages", False):
                    from .continuous_types import ComparisonPolicy

                    baseline = await app.repository.report(args.baseline)
                    candidate = await app.repository.report(args.candidate)
                    if baseline.get("subject_fingerprint") != candidate.get("subject_fingerprint"):
                        return {
                            "status": "invalid",
                            "policy": "continuous-regression/1",
                            "reasons": [{"code": "unapproved_subject"}],
                        }
                    policy = ComparisonPolicy(
                        baseline_id=args.baseline,
                        candidate_id=args.candidate,
                        baseline_hash=baseline["report_hash"],
                        candidate_hash=candidate["report_hash"],
                        allowed_subjects=[baseline["subject_fingerprint"]],
                    )
                    return await app.repository.compare_paged(args.scope, policy)
                return dict(
                    gate_reports(
                        await app.repository.report(args.baseline),
                        await app.repository.report(args.candidate),
                    )
                )
            if args.operation == "by-key":
                result = await app.repository.by_key({"scope": "local", "request_key": args.id})
                if result is None:
                    raise ValueError("not_found")
                return result
            if getattr(args, "case_id", None) is not None:
                if (
                    args.repetition is None
                    or args.cursor is not None
                    or args.limit is not None
                    or args.cases
                    or args.trials
                ):
                    raise ValueError("single_trial_arguments")
                return await app.repository.read_trial(
                    args.scope, args.id, args.case_id, args.repetition
                )
            if getattr(args, "repetition", None) is not None:
                raise ValueError("case_id_required")
            if getattr(args, "cases", False) or getattr(args, "trials", False):
                method = app.repository.case_page if args.cases else app.repository.trial_page
                page = await method(
                    args.scope,
                    args.id,
                    cursor=args.cursor,
                    limit=16 if args.limit is None else args.limit,
                )
                return {
                    "rows": page.rows,
                    "next_cursor": page.next_cursor,
                    "binding": page.binding,
                    "total_count": page.total_count,
                }
            if (
                getattr(args, "cursor", None) is not None
                or getattr(args, "limit", None) is not None
            ):
                raise ValueError("page_projection_required")
            return await app.repository.snapshot(args.id)
    async with await open_eval(
        database,
        workspace=root / "workspaces",
        native_profiles=profiles,
    ) as app:
        if args.command == "dataset":
            dataset = DatasetSpec.model_validate_json(args.file.read_bytes())
            return (await app.repository.freeze(dataset)).ref().model_dump(mode="json")
        if args.command == "example":
            dataset = DatasetSpec.model_validate_json(
                files("krim_agent.evaluation.examples")
                .joinpath("reconciliation.v1.json")
                .read_bytes()
            )
            dataset_ref = (await app.repository.freeze(dataset)).ref()
        else:
            dataset_ref = DatasetRef(
                name=args.name,
                version=args.version,
                fingerprint=args.fingerprint,
            )
        request = TaskSpec(
            dataset=dataset_ref,
            request_key=args.key,
            target_key=target,
            execution=domain.Budget(
                item_timeout_seconds=180,
                batch_timeout_seconds=240,
            )
            if profiles
            else domain.Budget(),
            metrics=app.repository.metric_registry.specs("native" if profiles else "reference"),
        )
        started = await app.run(request)
        result = await app.kernel.wait(started["id"])
        return await app.repository.report(result["id"])


async def owned(args: argparse.Namespace) -> dict[str, Any]:
    task = asyncio.current_task()
    assert task is not None
    loop = asyncio.get_running_loop()
    previous = {signum: signal.getsignal(signum) for signum in [signal.SIGINT, signal.SIGTERM]}
    for signum in previous:
        loop.add_signal_handler(signum, task.cancel)
    try:
        return await execute(args)
    finally:
        for signum, handler in previous.items():
            loop.remove_signal_handler(signum)
            signal.signal(signum, handler)


def main() -> None:
    args = parser().parse_args()
    try:
        result = asyncio.run(owned(args))
    except (Exception, asyncio.CancelledError) as error:
        print(
            json.dumps(
                {
                    "error": type(error).__name__,
                    "code": getattr(
                        error,
                        "code",
                        str(error),
                    ),
                }
            )
        )
        raise SystemExit(1) from None
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
