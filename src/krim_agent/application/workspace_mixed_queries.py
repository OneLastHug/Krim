"""模型混合批次只读消费；最终回程只认可本次原Backend记录。"""

from __future__ import annotations

import json
from typing import Any

from pydantic import JsonValue

from krim_agent.persistence.workspace_mixed_batches import MixedBatchStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.tools.core import ToolContext, ToolError
from krim_agent.workspace_mixed_batch import MixedBatchQuery, MixedBatchQueryResult

from .workspace_changes import WorkspaceChangeService
from .workspace_directories import DirectoryChangeService


class MixedBatchQueryService:
    def __init__(
        self, changes: WorkspaceChangeService, directories: DirectoryChangeService
    ) -> None:
        self.changes = changes
        self.store = MixedBatchStore(changes.store, directories.store)

    async def tool_query(
        self, payload: dict[str, Any], context: ToolContext
    ) -> dict[str, JsonValue]:
        try:
            request = MixedBatchQuery.model_validate(payload)
            scope, _binding = await self.changes._guard(context, writing=False)
            batch = None
            history = None
            if request.kind == "batch":
                assert request.batch_id is not None
                batch = await self.store.get(scope, request.batch_id)
            elif request.kind == "by_key":
                assert request.request_key is not None
                batch = await self.store.by_key(scope, request.request_key)
            else:
                history = await self.store.history(
                    scope, cursor=request.cursor, limit=request.limit
                )
            await self.changes._guard(context, writing=False)
            value = MixedBatchQueryResult(
                kind=request.kind, batch=batch, history=history
            ).model_dump(mode="json")
            context._mixed_query_response = json.dumps(value, sort_keys=True)
            return value
        except ServiceError as error:
            raise ToolError(error.code, str(error)) from error

    async def validate_query(self, value: Any, context: ToolContext) -> None:
        try:
            parsed = MixedBatchQueryResult.model_validate_json(json.dumps(value)).model_dump(
                mode="json"
            )
            if (
                context._mixed_query_response is None
                or json.dumps(parsed, sort_keys=True) != context._mixed_query_response
            ):
                raise ValueError()
        except (TypeError, ValueError):
            raise ToolError(
                "workspace_mixed_batch_query_unverified",
                "混合查询回程未关联本次原账本",
            ) from None
