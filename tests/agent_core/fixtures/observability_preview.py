"""本地隔离验收：真实服务生成合成记录，身份仅在进程内，无外部模型。"""
from __future__ import annotations

import asyncio
import sys
from contextlib import asynccontextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

import uvicorn  # noqa: E402

from krim_agent.api.server import create_app  # noqa: E402
from krim_agent.composition import open_application  # noqa: E402
from krim_agent.config import Settings  # noqa: E402
from krim_agent.contracts import ServiceError  # noqa: E402
from krim_agent.evaluation.continuous_types import SourceSelection  # noqa: E402
from krim_agent.evaluation.domain import CaseSpec, DatasetRef, MetricSpec, TaskSpec  # noqa: E402
from krim_agent.evaluation.product_types import ProductDatasetSpec, RunImportRequest  # noqa: E402
from krim_agent.sdk import WorkspaceEvaluationSDK  # noqa: E402
from krim_agent.security import LOCAL  # noqa: E402
from krim_agent.sessions.types import SessionCreate, TurnRequest  # noqa: E402

# Existing WSL distribution is on F:. Avoid DrvFs SQLite locking without
# weakening production locks. Never open a user's real session database here.
EVIDENCE = Path.home() / ".cache" / "krim-observability-qa-c67746"


async def seed(application) -> None:
    sessions = application.sessions
    run_ids = []
    for key, title, texts in (
        ("observability-arithmetic", "观测验收 · 算术与连续对话", (
            "/add 2 3", "比较前一轮的输入来源，保留本轮的实际请求记录。")),
        ("observability-long", "观测验收 · 长文本与工具返回", (
            "/echo 验收工具返回记录",
            "来源引用与长文本 " + "这是保存在隔离验收数据库中的普通对话文本。" * 30)),
        ("observability-failure", "观测验收 · 参数校验失败", ("/add nan 2",)),
    ):
        session = await sessions.create(LOCAL, "local", SessionCreate(
            title=title, request_key=key))
        for index, text in enumerate(texts):
            turn_key = f"{key}-{index}"
            try:
                turn = await sessions.turn_by_key(LOCAL, "local", session.id, turn_key)
            except ServiceError as error:
                if error.code != "not_found":
                    raise
                current = await sessions.get(LOCAL, "local", session.id)
                turn = await sessions.submit(LOCAL, "local", session.id, TurnRequest(
                    text=text, request_key=turn_key, expected_revision=current.revision))
            async with asyncio.timeout(10):
                while True:
                    value = await sessions.turn(LOCAL, "local", session.id, turn.id)
                    if value.status in {"completed", "failed", "aborted", "interrupted"}:
                        break
                    await asyncio.sleep(0.01)
            if value.run_id:
                run_ids.append(value.run_id)
    product = WorkspaceEvaluationSDK(application.evaluation_product)
    await product.import_runs(RunImportRequest(
        request_key="observability-preview-sources",
        sources=[SourceSelection(run_id=run_id) for run_id in run_ids],
    ))
    frozen = await product.freeze_dataset(ProductDatasetSpec(
        name="observability-arithmetic", version="v1", cases=[
            CaseSpec(case_id="arithmetic", instruction="/add 2 3",
                     files={"INPUT.txt": "/add 2 3"}, verifier="krim-script/1",
                     reference={"expected": "5"}),
            CaseSpec(case_id="decimal", instruction="/add 2.5 3.5",
                     files={"INPUT.txt": "/add 2.5 3.5"}, verifier="krim-script/1",
                     reference={"expected": "6"}),
        ],
    ))
    for target in ("krim-script-v1", "krim-script-v2"):
        metrics = [MetricSpec.model_validate(row)
                   for row in await product.metrics(target, mode="manual")]
        task = await product.start_task(TaskSpec(
            request_key="observability-preview-" + target, target_key=target,
            dataset=DatasetRef(name=frozen["name"], version=frozen["version"],
                               fingerprint=frozen["fingerprint"]), metrics=metrics,
        ))
        await application.product_evaluations.kernel.wait(task["id"])


async def main() -> None:
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    settings = Settings(database=EVIDENCE / "browser.sqlite",
                        product_eval_database=EVIDENCE / "browser-evaluations.sqlite",
                        product_eval_enabled=True, identity_mode="legacy")
    async with await open_application(settings.database, settings=settings) as application:
        app = create_app(application=application)
        original_lifespan = app.router.lifespan_context

        @asynccontextmanager
        async def lifespan(active):
            async with original_lifespan(active):
                await seed(application)
                yield

        app.router.lifespan_context = lifespan
        await uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=5188)).serve()


if __name__ == "__main__":
    asyncio.run(main())
