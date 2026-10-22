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
        task.add_parser(name).add_argument("id")
    commands.add_parser("report").add_argument("id")
    compare = commands.add_parser("compare")
    compare.add_argument("baseline")
    compare.add_argument("candidate")
    example = commands.add_parser("example")
    example.add_argument("name", choices=["reconciliation"])
    example.add_argument("--key", required=True)
    commands.add_parser("targets")
    return root


async def execute(args: argparse.Namespace) -> dict[str, Any]:
    root = args.root.absolute()
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
            if args.command == "report":
                return await app.repository.report(args.id)
            if args.command == "compare":
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
