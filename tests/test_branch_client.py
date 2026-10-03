import asyncio
import json
import sys

import httpx

from krim_agent.client import KrimClient


async def cli(url, *arguments):
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "krim_agent",
        *arguments,
        "--url",
        url,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
    assert process.returncode == 0, stderr.decode()
    return stdout.decode()


async def test_client_and_cli_keep_inactive_run_reply_and_expose_branch_commands(server_url):
    def client_scenario():
        with KrimClient(server_url) as client:
            topic = client.create_topic("客户端分支")
            original = client.send(topic.id, "/echo 原分支", "original")
            assert list(client.events(original.id))[-1].data["status"] == "completed"
            edited = client.edit(topic.id, original.input_message_id, "/echo 新分支", "edit")
            list(client.events(edited.id))
            assert "新分支" in client.messages(topic.id)[-1].content
            regenerated = client.regenerate(topic.id, original.input_message_id, "regenerate")
            list(client.events(regenerated.id))
            assert regenerated.input_message_id == original.input_message_id
            client.select_branch(topic.id, edited.input_message_id)
            assert "新分支" in client.messages(topic.id)[-1].content
            assert "原分支" in client.run_messages(original.id)[-1].content
            assert len(client.tree(topic.id).messages) == 11
            return topic.id, original.input_message_id

    topic_id, source_id = await asyncio.to_thread(client_scenario)
    # 原运行已完成但不在活动分支；CLI 幂等重试仍必须展示原运行的持久回复。
    retry = await cli(
        server_url, "chat", "/echo 原分支", "--topic", topic_id, "--idempotency-key", "original"
    )
    assert "原分支" in retry and "新分支" not in retry
    tree = json.loads(await cli(server_url, "tree", topic_id))
    assert len(tree["messages"]) == 11 and len(tree["active_path"]) == 4
    branch = json.loads(await cli(server_url, "branch", topic_id, source_id))
    assert branch["active_branches"]["__root__"] == source_id
    edited = json.loads(
        await cli(
            server_url, "edit", topic_id, source_id, "CLI 编辑", "--idempotency-key", "cli-edit"
        )
    )
    assert edited["action"] == "edit"
    async with httpx.AsyncClient(base_url=server_url) as client:
        await client.get(f"/v1/runs/{edited['id']}/events")
    regenerated = json.loads(
        await cli(
            server_url,
            "regenerate",
            topic_id,
            edited["input_message_id"],
            "--idempotency-key",
            "cli-regen",
        )
    )
    assert regenerated["action"] == "regenerate"
    async with httpx.AsyncClient(base_url=server_url) as client:
        response = await client.get(f"/v1/runs/{regenerated['id']}/events")
        assert '"status":"completed"' in response.text
