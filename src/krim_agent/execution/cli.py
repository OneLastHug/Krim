"""命令管理CLI：请求文件有界，退出码区分接受、完成、未完成和未知。"""

import argparse
from pathlib import Path

from krim_agent.client import KrimClient

from .agent_models import AgentCommandRequest, CommandSummary
from .directory_publication import CommandDirectoryProposalRequest
from .publication import CommandProposalRequest


def _request(path: str | None, parser: argparse.ArgumentParser) -> bytes:
    if not path:
        parser.error("命令动作需要--request本地JSON文件")
    assert path is not None
    with Path(path).open("rb") as stream:
        raw = stream.read(65537)
    if len(raw) > 65536:
        parser.error("命令请求不能超过64KiB")
    return raw


def result_code(value: CommandSummary, *, cancelled: bool = False) -> int:
    if value.phase == "interrupted" or value.outcome == "uncertain":
        return 3
    if value.phase != "finished":
        return 2
    if not value.processes_reaped:
        return 3
    if cancelled or value.outcome == "completed":
        return 0
    return 1


def execute(client: KrimClient, args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    if args.action == "start":
        accepted = client.agent_command_start(
            args.agent_id,
            AgentCommandRequest.model_validate_json(_request(args.request, parser)),
            confirmed=args.confirm,
        )
        if not args.wait:
            print(accepted.model_dump_json(indent=2))
            return 0
        result = client.agent_command_wait(
            args.agent_id,
            accepted.command_id,
            timeout_seconds=args.timeout_seconds,
            confirmed=args.confirm,
        )
        print(result.model_dump_json(indent=2))
        return result_code(result)
    if args.action == "history":
        page = client.agent_command_history(
            args.agent_id,
            after=args.after,
            limit=args.limit if args.limit is not None else 32,
            confirmed=args.confirm,
        )
        print(page.model_dump_json(indent=2))
        return 0
    if args.action == "directory-propose":
        directory_request = CommandDirectoryProposalRequest.model_validate_json(
            _request(args.request, parser)
        )
        if args.value is not None and args.value != directory_request.command_id:
            parser.error("命令标识与目录提案请求不一致")
        directory_proposal = client.agent_command_directory_propose(
            args.agent_id, directory_request, confirmed=args.confirm
        )
        print(directory_proposal.model_dump_json(indent=2))
        return 0
    if args.action == "directory-changes":
        if not args.value:
            parser.error("命令目录changes需要原command_id")
        directory_page = client.agent_command_directory_changes(
            args.agent_id,
            args.value,
            after=args.after,
            limit=args.limit if args.limit is not None else 32,
            confirmed=args.confirm,
        )
        print(directory_page.model_dump_json(indent=2))
        return 0
    if args.action == "propose":
        request = CommandProposalRequest.model_validate_json(_request(args.request, parser))
        if args.value is not None and args.value != request.command_id:
            parser.error("命令标识与提案请求不一致")
        proposal = client.agent_command_propose(args.agent_id, request, confirmed=args.confirm)
        print(proposal.model_dump_json(indent=2))
        return 0
    if not args.value:
        parser.error("命令动作需要原command_id；by-key需要原request_key")
    if args.action == "by-key":
        found = client.agent_command_by_key(args.agent_id, args.value, confirmed=args.confirm)
        print("null" if found is None else found.model_dump_json(indent=2))
        return 2 if found is None else result_code(found)
    if args.action == "resources":
        print(
            client.agent_command_resources(
                args.agent_id, args.value, confirmed=args.confirm
            ).model_dump_json(indent=2)
        )
        return 0
    if args.action == "inspect":
        print(
            client.agent_command_inspect(
                args.agent_id, args.value, confirmed=args.confirm
            ).model_dump_json(indent=2)
        )
        return 0
    if args.action == "read":
        print(
            client.agent_command_read(
                args.agent_id,
                args.value,
                source=args.source,
                path=args.path,
                offset=args.offset,
                limit=args.limit if args.limit is not None else 2048,
                confirmed=args.confirm,
            ).model_dump_json(indent=2)
        )
        return 0
    if args.action == "changes":
        print(
            client.agent_command_changes(
                args.agent_id,
                args.value,
                after=args.after,
                limit=args.limit if args.limit is not None else 32,
                include_unchanged=args.include_unchanged,
                confirmed=args.confirm,
            ).model_dump_json(indent=2)
        )
        return 0
    if args.action == "wait":
        result = client.agent_command_wait(
            args.agent_id, args.value, timeout_seconds=args.timeout_seconds, confirmed=args.confirm
        )
    elif args.action == "cancel":
        result = client.agent_command_cancel(args.agent_id, args.value, confirmed=args.confirm)
    else:
        result = client.agent_command(args.agent_id, args.value, confirmed=args.confirm)
    print(result.model_dump_json(indent=2))
    return result_code(result, cancelled=args.action == "cancel")
