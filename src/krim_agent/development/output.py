"""Project a command receipt together with the existing bounded first log page."""

from collections.abc import Awaitable, Callable
from typing import cast

from pydantic import JsonValue

from krim_agent.contracts import ServiceError
from krim_agent.development.contracts import OutputPage, ProcessHandle


async def shell_receipt(
    handle: ProcessHandle,
    read_output: Callable[[ProcessHandle, int], Awaitable[OutputPage]],
) -> dict[str, JsonValue]:
    result = cast(dict[str, JsonValue], handle.model_dump(mode="json"))
    try:
        page = await read_output(handle, 0)
        if page.process_id != handle.process_id or page.cursor != 0:
            raise ValueError("output binding changed")
    except Exception as error:
        # Log availability is not command success. Preserve the durable process
        # state and never include exception strings (which may contain secrets).
        result["output"] = None
        result["output_error"] = {
            "code": error.code if isinstance(error, ServiceError) else "output_unavailable",
            "cursor": 0,
            "next_action": "read_output",
        }
    else:
        result["output"] = page.model_dump(mode="json")
    return result
