"""生成、工具与完整 Agent 的本地 eval 示例，不调用外部模型。"""

import asyncio
from pathlib import Path

from krim_agent.agents import AgentDefinition, AgentManager
from krim_agent.evaluation import (
    AgentTarget,
    EvalConfig,
    ExactMatchScorer,
    ToolCallsScorer,
    compare_reports,
    evaluate,
    load_dataset,
    rescore,
    save_report,
)
from krim_agent.providers import FakeProvider
from krim_agent.sdk import PythonAI


async def main() -> None:
    # 样本与期望固定；每次尝试使用独立话题，不污染其他样本的上下文。
    data = load_dataset(Path(__file__).with_name("eval-basic.json"))
    scorers = [ExactMatchScorer(), ToolCallsScorer()]
    config = EvalConfig(concurrency=2)
    async with await PythonAI.create(provider=FakeProvider(delay=0)) as ai:
        generated = await ai.evaluate(data, scorers=scorers, config=config, version="demo-v1")
        save_report(generated, ".data/evals/generated.json")
        print("生成评估：", generated.summary.model_dump_json())
        # 离线重评分复用实际输出；模型与工具不会再次执行。
        rescored = await rescore(generated, scorers=scorers)
        print("离线对比：", compare_reports(generated, rescored).model_dump_json())
        # Agent 每样本创建新句柄并销毁，沿用现有权限、预算与真实工具审计。
        manager = AgentManager(ai.runtime.repository, max_concurrent=2)
        try:
            definition = AgentDefinition(
                name="eval_calculator", provider=ai.runtime.provider, tools=ai.runtime.tools
            )
            agents = await evaluate(
                data, AgentTarget(manager, definition), scorers=scorers, config=config
            )
            save_report(agents, ".data/evals/agents.json")
            print("Agent 评估：", agents.summary.model_dump_json())
        finally:
            await manager.close()


if __name__ == "__main__":
    asyncio.run(main())
