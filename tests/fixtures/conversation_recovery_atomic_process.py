"""恢复分支SQL提交前/后真正强杀；另进程按原键区分未落盘与已提交。"""

import asyncio
import json
import os
import sys
from pathlib import Path

from sqlalchemy import event
from sqlalchemy.orm import Session

import krim_agent
from krim_agent.config import Settings
from krim_agent.persistence.conversation_recovery import (
    ConversationRecoveryScope,
    ConversationRecoveryStore,
)
from krim_agent.persistence.database import MessageRow
from krim_agent.persistence.tree import MessageTree
from krim_agent.protocol.domain import MessageTreeView
from krim_agent.providers import FakeProvider
from krim_agent.sdk import PythonAI

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_conversation_recovery import abandoned, wanted  # noqa: E402


class Never(FakeProvider):
    calls = 0

    async def stream(self, messages, tools):
        self.calls += 1
        raise AssertionError("恢复持久读回不能调用模型")
        yield


def save(path, value):
    with path.open("w") as output:
        json.dump(value, output)
        output.flush()
        os.fsync(output.fileno())


async def main():
    phase, point, folder = sys.argv[1:4]
    root = Path(folder)
    provider = Never(delay=0)
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=provider,
    ) as ai:
        if phase == "produce":
            topic, run, scope, store = await abandoned(ai.runtime.repository)
            preview = await store.preview(scope)
            request = wanted(preview, "atomic-recovery")
            before = await ai.runtime.repository.message_tree(topic.id)
            metadata = {
                "topic_id": topic.id,
                "run_id": run.id,
                "before": before.model_dump(mode="json"),
                "request": request.model_dump(mode="json"),
                "recovery_id": store.message_id(scope, request.request_key),
            }
            save(root / "metadata.json", metadata)

            def signal():
                print(
                    json.dumps(
                        {
                            "pid": os.getpid(),
                            "point": point,
                            "recovery_id": metadata["recovery_id"],
                            "package": str(krim_agent.__file__),
                        }
                    ),
                    flush=True,
                )
                sys.stdin.readline()
                raise AssertionError("须由父进程真正SIGKILL")

            if point == "before":

                def held(sql, _context):
                    if any(
                        isinstance(row, MessageRow)
                        and row.role == "system"
                        and row.details.get("kind") == "conversation_recovery"
                        for row in sql.new
                    ):
                        signal()

                event.listen(Session, "after_flush", held)
            result = await store.apply(scope, request, confirmed=True)
            assert result.recovery_message_id == metadata["recovery_id"]
            signal()
        metadata = json.loads((root / "metadata.json").read_text())
        scope = ConversationRecoveryScope(metadata["topic_id"])
        store = ConversationRecoveryStore(ai.runtime.repository)
        before = MessageTreeView.model_validate_json(json.dumps(metadata["before"]))
        current = await ai.runtime.repository.message_tree(scope.topic_id)
        record = await store.by_key(scope, "atomic-recovery")
        if point == "before":
            assert record is None and current == before
            preview = await store.preview(scope)
            assert preview.available
            record = await store.apply(scope, wanted(preview, "atomic-recovery"), confirmed=True)
        else:
            assert record is not None and record.recovery_message_id == metadata["recovery_id"]
            after = await ai.runtime.repository.message_tree(scope.topic_id)
            assert await store.apply(scope, record.request, confirmed=True) == record
            assert await ai.runtime.repository.message_tree(scope.topic_id) == after
        final = await ai.runtime.repository.message_tree(scope.topic_id)
        assert [
            message for message in final.messages if message.id != record.recovery_message_id
        ] == before.messages
        assert len(final.messages) == len(before.messages) + 1
        MessageTree(final.messages).require_complete_tools(
            await ai.runtime.repository.list_messages(scope.topic_id)
        )
        assert provider.calls == 0
        print(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "point": point,
                    "recovery_id": record.recovery_message_id,
                    "original_messages": len(before.messages),
                    "final_messages": len(final.messages),
                    "provider_calls": provider.calls,
                    "atomic_rollback_observed": point == "before",
                    "original_key_readback": True,
                    "package": str(krim_agent.__file__),
                }
            ),
            flush=True,
        )


asyncio.run(main())
