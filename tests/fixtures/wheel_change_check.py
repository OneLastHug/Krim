"""独立安装包验证 Change List、真实宿主执行、0013 和 UI 资源。"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from sqlalchemy import text

import krim_agent
from krim_agent.api import create_app
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
    assert "/site-packages/" in krim_agent.__file__, krim_agent.__file__
    with TemporaryDirectory(prefix="krim-wheel-change-") as folder:
        root = Path(folder)
        resource = root / "actual.txt"
        await asyncio.to_thread(resource.write_text, "新值")
        database = Database(f"sqlite+aiosqlite:///{root / 'change.sqlite'}")
        runtime = ChatRuntime(
            KrimRepository(database), FakeProvider(delay=0), builtin_registry(), Settings()
        )
        app, local = create_app(runtime=runtime), LocalApp(runtime)

        async def apply(execution: ChangeExecution) -> ChangeReceipt:
            await asyncio.to_thread(resource.write_text, "旧值")
            return ChangeReceipt(
                **execution.model_dump(exclude={"items"}),
                results=[
                    ChangeItemResult(item_id=item.id, status="done") for item in execution.items
                ],
            )

        local.handle.on_change_invoke(apply)
        try:
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
                ) as client,
            ):
                topic = await local.handle.create_topic("wheel 清单")
                view = await local.handle.sync_changes(
                    topic.id,
                    ChangeSync(revision=0, items=[ChangeItemInput(id="one", title="真实资源")]),
                )
                operation = await local.handle.invoke_change(
                    topic.id,
                    ChangeInvoke(
                        revision=view.revision, action="undo", item_id="one", idempotency_key="once"
                    ),
                )
                assert operation.status == "done"
                assert await asyncio.to_thread(resource.read_text) == "旧值"
                assert (await client.get(f"/v1/topics/{topic.id}/change-list")).json()["items"][0][
                    "status"
                ] == "undone"
                assert (await client.get("/ui/change-list-ui.js")).status_code == 200
                assert (
                    "/v1/topics/{topic_id}/change-list/invoke"
                    in (await client.get("/openapi.json")).json()["paths"]
                )
                async with database.engine.connect() as connection:
                    head = (
                        await connection.execute(text("SELECT version_num FROM alembic_version"))
                    ).scalar()
                assert head == "0013_change_lists"
                print(
                    json.dumps(
                        {
                            "import": "site-packages",
                            "head": head,
                            "status": operation.status,
                            "resource": "旧值",
                            "asset": "change-list-ui.js",
                        },
                        ensure_ascii=False,
                    )
                )
        finally:
            await local.close()
            await runtime.close()
            await database.close()


asyncio.run(main())
