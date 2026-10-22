"""安装包工作区示例；需可信长驻owner已配置workspace，审批由操作者显式完成。

从实际安装包运行：python agent_core_workspace.py --url http://127.0.0.1:8765 --key stable-work-001。
本源码示例不自动审批、重POST、安装依赖或把source注入解释器。
"""

import argparse
import asyncio
import os

from krim_agent.client import AsyncRunClient
from krim_agent.sdk import SessionSDK, WorkspaceSDK
from krim_agent.sessions.types import SessionCreate
from krim_agent.workspace.types import Completion, CompletionCondition, WorkspaceTurnRequest


async def main(url: str, key: str) -> None:
    client = AsyncRunClient(url, os.environ.get("KRIM_AGENT_API_KEY", ""))
    try:
        sessions = SessionSDK(client)
        work = WorkspaceSDK(client)
        session = await sessions.create(
            SessionCreate(
                request_key=key + ".session", target_key="target-workspace", title="受核验的小项目"
            )
        )
        request = WorkspaceTurnRequest(
            request_key=key,
            expected_revision=session.revision,
            text="创建calc.py和unittest测试；实现add(20,22)=42并运行固定unittest发现命令。",
            completion=Completion(
                required_paths=("calc.py", "test_calc.py"),
                conditions=(
                    CompletionCondition(
                        id="add", kind="ast_symbols", path="calc.py", symbols=("add",)
                    ),
                ),
            ),
        )
        # 保存完整request及原key；未知回程只能work.by_key，不自动再start。
        turn = await work.start(session.id, request)
        print({"session_id": session.id, "turn_id": turn.id, "request_key": request.request_key})
        print("在原Turn审批入口核对每个参数后明确批准；模型done不会代替业务核验。")
    finally:
        # 这里只关闭本示例拥有的HTTP读者；不取消服务端Run或关闭服务端owner。
        await client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--key", required=True)
    arguments = parser.parse_args()
    asyncio.run(main(arguments.url, arguments.key))
