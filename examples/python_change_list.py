"""离线 Python 宿主 Change List：真实文件撤销与持久保留决定。"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from krim_agent.app import LocalApp
from krim_agent.change_list import (
    ChangeExecution,
    ChangeInvoke,
    ChangeItemInput,
    ChangeItemResult,
    ChangeReceipt,
    ChangeSync,
)
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry


async def main() -> None:
    with TemporaryDirectory(prefix="krim-change-example-") as folder:
        root = Path(folder)
        files = {name: root / f"{name}.txt" for name in ("one", "two")}
        for path in files.values():
            await asyncio.to_thread(path.write_text, "新值", encoding="utf-8")
        database = Database(f"sqlite+aiosqlite:///{root / 'changes.sqlite'}")
        await database.initialize()
        runtime = ChatRuntime(
            KrimRepository(database), FakeProvider(delay=0), builtin_registry(), Settings()
        )
        local = LocalApp(runtime)

        async def apply(execution: ChangeExecution) -> ChangeReceipt:
            # 宿主只处理自己登记的资源；metadata 不能授予任意路径读取或执行权限。
            for item in execution.items:
                path = files[item.id]
                if execution.action.startswith("undo"):
                    await asyncio.to_thread(
                        path.write_text, item.metadata["before"], encoding="utf-8"
                    )
                else:
                    record = json.dumps({"resource": item.id, "decision": "keep"})
                    await asyncio.to_thread((root / f"kept-{item.id}.json").write_text, record)
            return ChangeReceipt(
                **execution.model_dump(exclude={"items"}),
                results=[
                    ChangeItemResult(item_id=item.id, status="done") for item in execution.items
                ],
            )

        local.handle.on_change_invoke(apply)
        try:
            topic = await local.handle.create_topic("真实变更示例")
            view = await local.handle.sync_changes(
                topic.id,
                ChangeSync(
                    revision=0,
                    items=[
                        ChangeItemInput(id=name, title=f"修改 {name}", metadata={"before": "旧值"})
                        for name in files
                    ],
                ),
            )
            undone = await local.handle.invoke_change(
                topic.id,
                ChangeInvoke(
                    revision=view.revision, action="undo", item_id="one", idempotency_key="undo-one"
                ),
            )
            current = await local.handle.change_list(topic.id)
            kept = await local.handle.invoke_change(
                topic.id,
                ChangeInvoke(
                    revision=current.revision,
                    action="keep",
                    item_id="two",
                    idempotency_key="keep-two",
                ),
            )
            assert undone.status == kept.status == "done"
            assert await asyncio.to_thread(files["one"].read_text) == "旧值"
            assert await asyncio.to_thread(files["two"].read_text) == "新值"
            print(
                json.dumps(
                    {
                        "statuses": [
                            item.status for item in (await local.handle.change_list(topic.id)).items
                        ],
                        "undo_file": "旧值",
                        "kept_file": "新值",
                    },
                    ensure_ascii=False,
                )
            )
        finally:
            await local.close()
            await runtime.close()
            await database.close()


asyncio.run(main())
