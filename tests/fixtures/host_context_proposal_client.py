"""真实 Python Host 经 HTTP/SSE 与同源 iframe 查询/确认压缩提案。"""

import asyncio
import json
import os
import sys

from krim_agent.app_sdk import RemoteHostHandle


async def main():
    async with RemoteHostHandle(sys.argv[1], sys.argv[2], os.environ["KRIM_AGENT_API_KEY"]) as host:
        topic_id, proposal_id = sys.argv[3:5]
        listed = await host.emit("context/proposals", {"topic_id": topic_id, "pending_only": True})
        assert listed.ok and isinstance(listed.data, list) and len(listed.data) == 1
        original = await host.emit(
            "context/proposal", {"topic_id": topic_id, "proposal_id": proposal_id}
        )
        assert original.ok and isinstance(original.data, dict)
        assert original.data["status"] == "proposed"
        decided = await host.emit(
            "context/decision",
            {
                "topic_id": topic_id,
                "proposal_id": proposal_id,
                "action": "confirm",
                "revision": original.data["revision"],
            },
        )
        assert decided.ok and isinstance(decided.data, dict)
        assert decided.data["status"] == "completed"
        assert decided.data["result"] == original.data["result"]
        print(
            json.dumps(
                {"topic_id": topic_id, "proposal_id": proposal_id, "status": "completed"},
                ensure_ascii=False,
            )
        )


asyncio.run(main())
