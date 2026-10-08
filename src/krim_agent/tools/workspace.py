"""受控只读工具从可信ToolContext取得绑定，模型只传相对路径。"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from functools import cache

from pydantic import BaseModel, Field

from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.workspace import Workspace, WorkspaceFile, WorkspaceListing, WorkspaceSearch

from .core import ToolContext, ToolDefinition, ToolError


class ReadFileInput(StrictModel):
    path: str = Field(min_length=1, max_length=512)
    start_line: int = Field(default=1, ge=1)
    max_lines: int = Field(default=200, ge=1, le=1000)


class ListFilesInput(StrictModel):
    path: str = Field(default=".", max_length=512)
    limit: int = Field(default=200, ge=1, le=1000)


class SearchFilesInput(StrictModel):
    query: str = Field(min_length=1, max_length=200)
    path: str = Field(default=".", max_length=512)
    limit: int = Field(default=100, ge=1, le=100)


async def workspace_io[Result](
    operation: Callable[..., Result], *args: object, **kwargs: object
) -> Result:
    # to_thread取消不能终止OS读取；等待有界读取收束再传播取消，避免资源先关闭。
    task = asyncio.create_task(asyncio.to_thread(operation, *args, **kwargs))
    cancelled = False
    while True:
        try:
            result = await asyncio.shield(task)
            break
        except asyncio.CancelledError:
            if task.cancelled():
                raise
            cancelled = True
        except Exception:
            if cancelled:
                raise asyncio.CancelledError from None
            raise
    if cancelled:
        raise asyncio.CancelledError
    return result


def _workspace(context: ToolContext) -> Workspace:
    if context.workspace is None:
        raise ToolError("workspace_unbound", "当前会话没有工作区授权")
    try:
        return context.workspace.require()
    except ServiceError as error:
        raise ToolError(error.code, str(error)) from error


@cache
def workspace_tools() -> tuple[ToolDefinition, ...]:
    async def read(arguments: BaseModel, context: ToolContext) -> dict[str, object]:
        request = ReadFileInput.model_validate(arguments.model_dump())
        try:
            result = await workspace_io(
                _workspace(context).read,
                request.path,
                start_line=request.start_line,
                max_lines=request.max_lines,
            )
            _workspace(context)
            return result.model_dump(mode="json")
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error

    async def listing(arguments: BaseModel, context: ToolContext) -> dict[str, object]:
        request = ListFilesInput.model_validate(arguments.model_dump())
        try:
            result = await workspace_io(_workspace(context).list, request.path, limit=request.limit)
            _workspace(context)
            return result.model_dump(mode="json")
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error

    async def search(arguments: BaseModel, context: ToolContext) -> dict[str, object]:
        request = SearchFilesInput.model_validate(arguments.model_dump())
        try:
            result = await workspace_io(
                _workspace(context).search,
                request.query,
                path=request.path,
                limit=request.limit,
            )
            _workspace(context)
            return result.model_dump(mode="json")
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error

    return (
        ToolDefinition(
            "workspace_read",
            "读取已授权工作区的UTF-8文件片段与SHA-256",
            ReadFileInput,
            read,
            "read",
            output_schema=WorkspaceFile,
        ),
        ToolDefinition(
            "workspace_list",
            "有界列出已授权工作区目录，不跟随链接",
            ListFilesInput,
            listing,
            "read",
            output_schema=WorkspaceListing,
        ),
        ToolDefinition(
            "workspace_search",
            "在已授权工作区有界检索完整UTF-8文本",
            SearchFilesInput,
            search,
            "read",
            output_schema=WorkspaceSearch,
        ),
    )
