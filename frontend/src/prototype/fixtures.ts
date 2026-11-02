export type TurnStatus = "complete" | "streaming" | "stopped";
export interface PrototypeTurn {
  id: string;
  prompt: string;
  answer: string;
  status: TurnStatus;
  files: string[];
}
export interface PrototypeConversation {
  id: string;
  title: string;
  project: string;
  pinned: boolean;
  archived: boolean;
  draft: string;
  turns: PrototypeTurn[];
}
export interface PreviewFile {
  name: string;
  path: string;
  language: string;
  content: string;
}

export const files: PreviewFile[] = [
  {
    name: "timeout.py", path: "src/runtime/timeout.py", language: "Python",
    content: `import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")


async def call_with_timeout(
    operation: Callable[[], Awaitable[T]],
    timeout_seconds: float,
) -> T:
    """把时限绑定到整次操作，而不是单次网络读取。"""
    async with asyncio.timeout(timeout_seconds):
        return await operation()


async def example() -> str:
    async def read() -> str:
        await asyncio.sleep(0.1)
        return "ready"

    return await call_with_timeout(read, 5.0)
`,
  },
  {
    name: "evaluation.md", path: "docs/evaluation.md", language: "Markdown",
    content: `# 评测结果的阅读顺序

先检查运行状态，再看结果和调用过程。

## 三个问题

1. 这次运行是否完整结束？
2. 最终答案是否满足要求？
3. 工具调用是否符合预期？

## 比较条件

使用相同的数据集版本、参数与评分规则。
失败和取消单独保留，不用空字符串代替结果。
`,
  },
  {
    name: "README.md", path: "README.md", language: "Markdown",
    content: `# Krim Agent

一个围绕会话、工具与评测组织的 Agent 工作区。

## 阅读代码

- Session：维护对话与轮次
- Runtime：执行一次运行
- Tools：声明、审批与调用工具
- Evaluation：保存案例并比较结果

接口层负责接收请求，运行层负责推进状态。
`,
  },
];

export const architectureAnswer = `可以把超时处理拆成三个部分：**调用时限、取消传播、结果归属**。它们需要配合，但不应该混在同一个异常处理分支里。

### 先让时限覆盖完整调用

把超时放在工具执行的外层，排队、网络读取和结果解析都计入同一个时限。Python 可以直接使用原生的异步上下文：

\`\`\`python
async def call_with_timeout(operation, timeout_seconds):
    async with asyncio.timeout(timeout_seconds):
        return await operation()
\`\`\`

这里的 \`TimeoutError\` 在上下文退出后处理。不要在工具内部吞掉 \`CancelledError\`，否则用户点击停止后，后台操作可能还会继续。

### 不同结果要分别保留

| 情况 | 运行状态 | 界面反馈 |
| --- | --- | --- |
| 正常返回 | 已完成 | 展示结果与工具摘要 |
| 超过时限 | 失败 | 显示超时原因，允许重试 |
| 用户停止 | 已停止 | 保留已经生成的内容 |
| 回程未确认 | 待核实 | 先查原运行，不重复提交 |

> 超时说明这次等待结束了，不一定说明外部操作没有发生。

因此，带副作用的工具还需要稳定的请求标识。重试前查询原结果，避免把一次操作执行两遍。`;

const evaluationAnswer = `比较两次评测时，我会先对齐**数据集版本、模型参数和评分规则**，再看分数变化。

### 先读失败，不只读均值

- **正确性**：最终答案是否满足案例要求。
- **工具行为**：调用名称、参数和顺序是否符合预期。
- **运行完整性**：取消、超时和未知结果分别统计。

\`\`\`json
{
  "dataset": "tool-calling-v1",
  "checks": ["answer", "tool_arguments", "completion"],
  "keep_failed_cases": true
}
\`\`\`

同一个总分可能掩盖不同问题：一组结果可能是答案差，另一组则是工具执行失败。把失败案例和运行记录放在一起，更容易判断下一步应该改哪里。`;

const readingAnswer = `建议从一条完整请求开始读，而不是按文件夹逐个翻。

\`用户输入 → Session → Runtime → Provider → Tool → 结果\`

把这条路径跑通后，再回头看取消、审批和恢复。你会更容易区分：哪些代码推进业务，哪些代码保证操作不会重复。

### 阅读顺序

1. 找到创建一轮对话的入口。
2. 看 Runtime 如何消费模型事件。
3. 沿着一次工具调用读到执行器。
4. 回到会话，确认结果如何保存。

可以先从右侧的 \`README.md\` 记下模块关系。`;

export function createFixtures(): PrototypeConversation[] {
  const turn = (id: string, prompt: string, answer: string, fileNames: string[]): PrototypeTurn => ({
    id, prompt, answer, files: fileNames, status: "complete",
  });
  return [
    { id: "new", title: "新会话", project: "Krim", pinned: false, archived: false, draft: "", turns: [] },
    { id: "timeouts", title: "工具调用与超时处理", project: "Krim", pinned: true, archived: false, draft: "", turns: [turn("timeout-1", "帮我梳理一下工具调用的超时和取消，怎么做才不会重复执行？", architectureAnswer, ["timeout.py", "README.md"])] },
    { id: "evaluations", title: "评测结果应该怎么看", project: "Krim", pinned: true, archived: false, draft: "", turns: [turn("eval-1", "两次评测的分数差不多，怎么判断具体哪里需要改进？", evaluationAnswer, ["evaluation.md"])] },
    { id: "reading", title: "把 Agent 拆开来看", project: "Krim", pinned: false, archived: false, draft: "", turns: [turn("read-1", "我想从源码理解一个 Agent，先读哪部分比较合适？", readingAnswer, ["README.md"])] },
    { id: "permissions", title: "工具审批的交互细节", project: "Krim", pinned: false, archived: false, draft: "", turns: [turn("permission-1", "工具审批里哪些信息应该直接展示？", "优先展示**要做什么、影响哪个对象、有什么后果**。\n\n原始参数可以展开查看，但不能让用户读完 JSON 才知道自己在批准什么。允许和拒绝应放在同一位置，等待审批时保留当前上下文。", [])] },
    { id: "notes", title: "整理本周的阅读笔记", project: "Notes", pinned: false, archived: false, draft: "", turns: [turn("notes-1", "把阅读笔记按问题归类。", "可以先分成三个主题：\n\n- **系统怎么运行**：会话、状态与事件。\n- **操作怎么控制**：权限、取消与恢复。\n- **结果怎么判断**：评测案例、评分与比较。\n\n每条笔记保留一个具体问题和对应的代码入口，比按阅读日期堆放更容易回顾。", [])] },
  ];
}

export function responseFor(prompt: string): { answer: string; files: string[] } {
  if (/评测|结果|分数/.test(prompt)) return { answer: evaluationAnswer, files: ["evaluation.md"] };
  if (/源码|阅读|理解|架构/.test(prompt)) return { answer: readingAnswer, files: ["README.md"] };
  return { answer: architectureAnswer, files: ["timeout.py"] };
}
