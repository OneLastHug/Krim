"""安装包贯通例子；默认离线脚本，--url连接可信应用服务。"""

import argparse
import asyncio
import json
import os
import uuid
from pathlib import Path

from krim_agent.client import AsyncRunClient
from krim_agent.composition import open_application
from krim_agent.sdk import SessionSDK
from krim_agent.sessions.types import ControlRequest, SessionCreate, TurnRequest


async def conversation(sdk, texts):
    session = await sdk.create(SessionCreate(request_key=uuid.uuid4().hex, title="贯通示例"))
    results = []
    for index, text in enumerate(texts):
        current = await sdk.get(session.id)
        turn = await sdk.submit(
            session.id,
            TurnRequest(request_key=f"turn-{index}", text=text, expected_revision=current.revision),
        )
        turn = await sdk.wait(session.id, turn.id)
        assert turn.status == "completed", turn
        facts = await sdk.result(session.id, turn.id)
        assert facts.model_steps == 2 and len(facts.tools) == 1
        assert facts.tools[0].executor_started and facts.tools[0].result_available
        results.append({"turn_id": turn.id, "run_id": turn.run_id, "text": facts.text})
    assert len(await sdk.history(session.id)) == 2
    current = await sdk.get(session.id)
    await sdk.control(
        session.id,
        "close",
        ControlRequest(request_key="close-example", expected_revision=current.revision),
    )
    return {
        "session_id": session.id,
        "turns": results,
        "closed": (await sdk.get(session.id)).status == "closed",
    }


async def main(args):
    if args.url:
        client = AsyncRunClient(args.url, os.environ.get("KRIM_AGENT_API_KEY", ""))
        try:
            result = await conversation(
                SessionSDK(client, workspace=args.workspace), ["请记住基数17", "与3相加"]
            )
            assert result["turns"][-1]["text"] == "20"
            return result | {"mode": "controlled-http-application", "provider_calls": None}
        finally:
            await client.close()
    async with await open_application(Path(args.database)) as app:
        result = await conversation(SessionSDK(app.sessions), ["/echo hello", "/add 2 3"])
        assert result["turns"][-1]["text"] == "5"
        return result | {"mode": "offline-script", "provider_calls": app.provider.calls}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Krim Agent多轮贯通示例")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--database")
    target.add_argument("--url")
    parser.add_argument("--workspace", default="local")
    print(json.dumps(asyncio.run(main(parser.parse_args())), ensure_ascii=False, indent=2))
