"""浏览器 Change List 的可信 Python 宿主；实际修改测试目录中的文件。"""

import asyncio
import json
import os
import signal
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

from krim_agent.app_sdk import RemoteHostHandle
from krim_agent.change_list import ChangeItemInput, ChangeItemResult, ChangeReceipt, ChangeSync


async def main():
    stop = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stop.set)
    with TemporaryDirectory(prefix="krim-browser-changes-") as folder:
        root = Path(folder)
        paths = {name: root / f"{name}.txt" for name in ("one", "two")}
        for path in paths.values():
            path.write_text("新值", encoding="utf-8")
        async with RemoteHostHandle(
            sys.argv[1], sys.argv[2], os.environ["KRIM_AGENT_API_KEY"]
        ) as host:

            async def apply(execution):
                for item in execution.items:
                    assert item.metadata == {
                        "resource": item.id,
                        "before": "旧值",
                        "source": "test",
                    }
                    if execution.action.startswith("undo"):
                        paths[item.id].write_text(item.metadata["before"], encoding="utf-8")
                    else:
                        (root / f"kept-{item.id}.json").write_text(
                            json.dumps({"decision": "keep", "resource": item.id})
                        )
                if sys.argv[4] == "error":
                    raise RuntimeError("private executor failure after file write")
                print(
                    json.dumps(
                        {
                            "event": "applied",
                            "operation_id": execution.operation_id,
                            "action": execution.action,
                        }
                    ),
                    flush=True,
                )
                return ChangeReceipt(
                    **execution.model_dump(exclude={"items"}),
                    results=[
                        ChangeItemResult(item_id=item.id, status="done") for item in execution.items
                    ],
                )

            async def click(event):
                assert event.item.metadata["source"] == "test"
                print(
                    json.dumps({"event": "click", "id": event.item.id, "revision": event.revision}),
                    flush=True,
                )
                return "ignored by notification"

            host.on_change_invoke(apply)
            host.on_change_click(click)

            async def listen():
                async for _event in host.events():
                    pass

            listener = asyncio.create_task(listen())
            try:
                view = await host.sync_changes(
                    sys.argv[3],
                    ChangeSync(
                        revision=0,
                        items=[
                            ChangeItemInput(
                                id=name,
                                title=f"变更 {name} <img id='change-html'>",
                                description="实际 Python 文件资源",
                                metadata={"resource": name, "before": "旧值", "source": "test"},
                            )
                            for name in paths
                        ],
                    ),
                )
                assert (await host.change_list(sys.argv[3])).revision == view.revision
                print(
                    json.dumps({"event": "ready", "directory": folder, "revision": view.revision}),
                    flush=True,
                )
                await stop.wait()
            finally:
                listener.cancel()
                await asyncio.gather(listener, return_exceptions=True)


asyncio.run(main())
