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

export const architectureAnswer = `A tool timeout means the operation did not return before its time limit. It may have been stopped, or it may still be running elsewhere; don’t assume it had no effect. Before retrying, check the operation’s status—especially if repeating it could create duplicate changes.

| Situation | What to do |
| --- | --- |
| Read-only request | Retry with more time |
| Change or submission | Check status before retrying |
| Repeated timeouts | Reduce scope or investigate the bottleneck |

\`\`\`python
try:
    result = run_tool(timeout=30)
except TimeoutError:
    check_status()
    retry_if_safe()
\`\`\`

A longer timeout can help with slow operations, but it won’t fix a persistent bottleneck. For safe retries, use an idempotent operation—one that won’t cause extra effects when repeated.`;

const evaluationAnswer = `比较两次评测时，我会先对齐**数据集版本、模型参数和评分规则**，再看分数变化。

### 先读失败，不只读均值

- **正确性**：最终答案是否满足案例要求。
- **工具行为**：调用名称、参数和顺序是否符合预期。
- **运行完整性**：取消、超时和未知结果分别统计。

\`\`\`json
{
  "dataset": "tool-calling-v1",
  "checks": ["answer", "tool_arguments", "completion"],
  "comparison_dimensions": ["dataset_version", "model_parameters", "scoring_rules", "tool_behavior", "completion_status", "retry_safety"],
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

export const markdownPrecisionRequestId = "req_20261103_7f2a9c0d4e138b6a90c4e51f83d2a7b6f0c19e48a6d2f5b1c7039a4d8e62f0c1b9a5d3e7f6142c8b0a6d9e3f1c5b7a2d8e4f690c1a3b5d7e9f20468c1a5b3d7e9f0124";
const markdownRequestLog = `    GET /v1/runs/trace?request_id="${markdownPrecisionRequestId}" status=504 message="<gateway timeout>" retries=1 & source="api"
\tattempt=2 outcome="confirmed-not-applied" elapsed_ms=281
\tresponse="Retry-After: 2s"; note="same request id"`;
const markdownUnclosedLog = `2026-11-03T10:14:31Z request_id=${markdownPrecisionRequestId} retry_check=status_not_applied
2026-11-03T10:14:33Z request_id=${markdownPrecisionRequestId} retry_start=accepted`;

const markdownPrecisionAnswer = `# 请求超时后的安全重试

先确认超时发生在哪一层，再决定是否重试。**客户端没有收到响应，不代表服务端没有执行**；先用行内代码 \`request_id\` 查原请求，再判断下一步。

> 优先查询原请求状态，再决定是否再次提交。
>
> - 只读查询可以在服务恢复后重试。
>   - 保留原始请求标识，确认结果是否已经返回。
> - 对创建或提交操作，先确认服务端是否完成。
>
>   > 如果接口支持幂等键，重试时复用同一个键，避免重复副作用。

1. 查询原请求的执行状态。
   - 已完成：读取原结果，不重复提交。
   - 未执行：按接口约定发起一次重试。
2. 状态仍未知：检查服务端日志或联系接口所有者，再决定是否重试。

请求追踪记录： [${markdownPrecisionRequestId}](/prototype.html?request_id=${markdownPrecisionRequestId})

| 请求类型 | 先检查什么 | 何时重试 |
| :--- | :---: | ---: |
| 只读查询 | 服务状态 | 恢复后 |
| 创建或提交 | 原请求状态 | 确认未执行后 |
| 状态未知 | 服务端日志 | 结果可确认后 |

<img src="https://example.invalid/tracker.gif" alt="旧版接口返回的图片" onerror="window.__markdownHtmlRan=true">
<script>window.__markdownScriptRan = true</script>

[旧版接口说明](javascript:alert%281%29)

\`\`\`request-log-unregistered
${markdownRequestLog}
\`\`\`

~~~
${markdownUnclosedLog}
`;

export function createFixtures(): PrototypeConversation[] {
  const turn = (id: string, prompt: string, answer: string, fileNames: string[]): PrototypeTurn => ({
    id, prompt, answer, files: fileNames, status: "complete",
  });
  return [
    { id: "new", title: "新会话", project: "Krim", pinned: false, archived: false, draft: "", turns: [] },
    { id: "timeouts", title: "工具调用与超时处理", project: "Krim", pinned: true, archived: false, draft: "", turns: [turn("timeout-1", "Respond in English. Explain tool timeouts in at most 180 words. Use two short paragraphs, a Python code block with 4 lines, and a three-row Markdown table. Do not read, edit, or execute files.", architectureAnswer, [])] },
    { id: "evaluations", title: "评测结果应该怎么看", project: "Krim", pinned: true, archived: false, draft: "", turns: [turn("eval-1", "两次评测的分数差不多，怎么判断具体哪里需要改进？", evaluationAnswer, ["evaluation.md"])] },
    { id: "reading", title: "把 Agent 拆开来看", project: "Krim", pinned: false, archived: false, draft: "", turns: [turn("read-1", "我想从源码理解一个 Agent，先读哪部分比较合适？", readingAnswer, ["README.md"])] },
    { id: "markdown-precision", title: "请求超时后的安全重试", project: "Krim", pinned: false, archived: false, draft: "", turns: [turn("markdown-precision-1", "请求超时后怎样安全重试？", markdownPrecisionAnswer, [])] },
    { id: "permissions", title: "工具审批的交互细节", project: "Krim", pinned: false, archived: false, draft: "", turns: [turn("permission-1", "工具审批里哪些信息应该直接展示？", "优先展示**要做什么、影响哪个对象、有什么后果**。\n\n原始参数可以展开查看，但不能让用户读完 JSON 才知道自己在批准什么。允许和拒绝应放在同一位置，等待审批时保留当前上下文。", [])] },
    { id: "notes", title: "整理本周的阅读笔记", project: "Notes", pinned: false, archived: false, draft: "", turns: [turn("notes-1", "把阅读笔记按问题归类。", "可以先分成三个主题：\n\n- **系统怎么运行**：会话、状态与事件。\n- **操作怎么控制**：权限、取消与恢复。\n- **结果怎么判断**：评测案例、评分与比较。\n\n每条笔记保留一个具体问题和对应的代码入口，比按阅读日期堆放更容易回顾。", [])] },
  ];
}

export function responseFor(prompt: string): { answer: string; files: string[] } {
  if (/评测|结果|分数/.test(prompt)) return { answer: evaluationAnswer, files: ["evaluation.md"] };
  if (/源码|阅读|理解|架构/.test(prompt)) return { answer: readingAnswer, files: ["README.md"] };
  return { answer: architectureAnswer, files: ["timeout.py"] };
}
