import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { appendMarkdownBlocks } from "../src/lib/markdown-blocks";
import { MessageMarkdown } from "../src/components/messages/MessageMarkdown";

const highlightState = vi.hoisted(() => ({ runs: 0 }));

function hasCodeElement(node: unknown): boolean {
  if (node === null || typeof node !== "object") return false;
  const element = node as { tagName?: unknown; children?: unknown };
  if (element.tagName === "code") return true;
  return Array.isArray(element.children) && element.children.some(child => hasCodeElement(child));
}

vi.mock("rehype-highlight", async importOriginal => {
  const actual = await importOriginal<typeof import("rehype-highlight")>();
  const counted = ((options?: Parameters<typeof actual.default>[0]) => {
    const transform = actual.default(options) as unknown as (...args: unknown[]) => unknown;
    return (...args: unknown[]) => {
      if (hasCodeElement(args[0])) highlightState.runs += 1;
      return transform(...args);
    };
  }) as unknown as typeof actual.default;
  return { ...actual, default: counted };
});

let root: Root;
let host: HTMLDivElement;

beforeEach(() => {
  host = document.createElement("div");
  document.body.append(host);
  root = createRoot(host);
  highlightState.runs = 0;
  (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); });

async function render(text: string, copy = vi.fn(async (_text: string) => true), streaming?: boolean) {
  await act(async () => root.render(<MessageMarkdown text={text} onCopy={copy} streaming={streaming} />));
  return copy;
}

async function markdownHtml(text: string, streaming?: boolean): Promise<string> {
  const element = document.createElement("div");
  document.body.append(element);
  const elementRoot = createRoot(element);
  try {
    await act(async () => elementRoot.render(<MessageMarkdown text={text} onCopy={async () => true} streaming={streaming} />));
    return element.querySelector(".message-markdown")!.innerHTML;
  } finally {
    await act(async () => elementRoot.unmount());
    element.remove();
  }
}

test("共享消息保留高亮前代码的缩进、符号和换行", async () => {
  const code = "def example():\n\treturn '<tag> & value'\n";
  const copy = await render("```python\n" + code + "```");
  expect(host.querySelector(".hljs-keyword")).not.toBeNull();
  expect(host.querySelector(".message-code-head > span")?.textContent).toBe("python");
  expect(host.querySelector('.message-code-head [aria-label="复制代码"]')).not.toBeNull();
  await act(async () => host.querySelector<HTMLButtonElement>('[aria-label="复制代码"]')!.click());
  expect(copy).toHaveBeenCalledExactlyOnceWith(code);
  expect(host.querySelector("pre")!.getAttribute("tabindex")).toBe("0");
  expect(host.querySelector("[data-krim-code-text]")).toBeNull();
});

test("未知语言和未闭合代码仍能复制文本", async () => {
  const copy = await render("```not-registered docs/example.txt\n    value: false\n\tresult: null");
  expect(host.querySelector(".message-code-head > span")?.textContent).toBe("docs/example.txt");
  await act(async () => host.querySelector<HTMLButtonElement>('[aria-label="复制代码"]')!.click());
  expect(copy).toHaveBeenCalledExactlyOnceWith("    value: false\n\tresult: null\n");
});

test("模型图片只生成显式链接，不自动加载外部资源", async () => {
  await render('![结果图片](https://example.invalid/result.png)\n\n[危险链接](javascript:alert%281%29)\n\n<script>window.__unsafe = true</script>');
  expect(host.querySelectorAll("img,script")).toHaveLength(0);
  expect(host.querySelector("a")!.getAttribute("href")).toBe("https://example.invalid/result.png");
  expect(host.querySelector("a")!.getAttribute("rel")).toBe("noreferrer");
  expect(host.querySelectorAll("a")).toHaveLength(1);
  expect(host.textContent).toContain("危险链接");
});

test("GFM表格和嵌套列表保留结构", async () => {
  await render("| 名称 | 数量 |\n| --- | ---: |\n| 记录 | 0 |\n\n1. 读取\n   - 核对状态\n\n> 保留原请求。");
  expect(host.querySelectorAll("table td")).toHaveLength(2);
  expect(host.querySelector("ol ul li")!.textContent).toBe("核对状态");
  expect(host.querySelector("blockquote")!.textContent).toContain("保留原请求");
});

test("增量切分复用已完成块并前置跨块引用定义", () => {
  const first = appendMarkdownBlocks(null, "段落甲。\n\n```python\nvalue = 1\n");
  expect(first.whole).toBe(false);
  expect(first.blocks).toEqual([
    { source: "段落甲。", kind: "content" },
    { source: "```python\nvalue = 1\n", kind: "content" },
  ]);
  expect(first.defs).toBe("");
  const second = appendMarkdownBlocks(first, "段落甲。\n\n```python\nvalue = 1\n```\n\n[ref]: https://example.com\n\n见 [ref][]。");
  expect(second.blocks[0]!.source === first.blocks[0]!.source).toBe(true);
  expect(second.blocks).toEqual([
    { source: "段落甲。", kind: "content" },
    { source: "```python\nvalue = 1\n```", kind: "content" },
    { source: "[ref]: https://example.com", kind: "hidden" },
    { source: "见 [ref][]。", kind: "content" },
  ]);
  expect(second.defs).toBe("[ref]: https://example.com");
  const replaced = appendMarkdownBlocks(second, "完全不同");
  expect(replaced.blocks.map(block => block.source)).toEqual(["完全不同"]);
  expect(replaced.stableEnd).toBe(0);
});

test("流式切块与整段渲染的最终 HTML 一致", async () => {
  const doc = [
    "# 报告",
    "",
    "开头段落，含 **加粗**、`行内代码` 与 [ref][] 引用链接。",
    "",
    "| 名称 | 数量 |",
    "| --- | ---: |",
    "| 记录 | 0 |",
    "| 很长 | 9 |",
    "",
    "1. 外层",
    "   - 内层",
    "     1. 更深",
    "",
    "2. 第二项",
    "",
    "> 引用第一行",
    "> 引用第二行",
    "",
    "```python",
    "def add(a, b):",
    "    return a + b",
    "",
    "class Gate:",
    "    pass",
    "```",
    "",
    "~~~text",
    "plain <b>kept</b> raw",
    "~~~",
    "",
    "<div>直接 HTML 文本</div>",
    "",
    "Setext 标题",
    "==========",
    "",
    "---",
    "",
    "1. 列表内引用 [it][]",
    "   - [it]: https://example.com/item",
    "",
    "[ref]: https://example.com/api",
    "",
    "```ts",
    'const value: string = "<tag> & value";',
  ].join("\n");
  const streamingHtml = await markdownHtml(doc, true);
  expect(streamingHtml).toBe(await markdownHtml(doc, false));
  expect(streamingHtml.includes("hljs-keyword")).toBe(true);
});

test("脚注等文档级结构回退整段渲染且输出一致", async () => {
  const doc = "正文含脚注[^1]。\n\n[^1]: 脚注说明";
  const reference = await markdownHtml(doc, false);
  expect(await markdownHtml(doc, true)).toBe(reference);
  expect(reference.length).toBeGreaterThan(0);
});

test("长文渐进追加时与整段渲染逐帧一致、已完成块不再高亮", async () => {
  const deltas = [
    "# 增量报告",
    "\n\n第一段文字。",
    "\n\n引用链接 [ref][] 与 `code`。",
    "\n\n[ref]: https://example.com/a",
    "\n\n| 名称 | 数量 |\n| --- | ---: |",
    "\n| 记录 | 0 |",
    "\n\n1. 外层",
    "\n   - 内层",
    "\n\n2. 宽松列表项",
    "\n\n> 引用行",
    "\n> 续行",
    "\n>\n> 空行后的引用段",
    "\n\n<div>HTML 块文本</div>",
    "\n\n```python",
    "\ndef gate(value):",
    "\n    return value",
    "\n\n```",
    "\n\n结尾段落",
  ];
  const onCopy = vi.fn(async (_text: string) => true);
  let text = "";
  let firstPre: HTMLPreElement | null = null;
  let firstParagraph: HTMLParagraphElement | null = null;
  for (const delta of deltas) {
    text += delta;
    await render(text, onCopy, true);
    expect(host.querySelector(".message-markdown")!.innerHTML).toBe(await markdownHtml(text));
    firstPre ??= host.querySelector("pre");
    firstParagraph ??= host.querySelector("p");
  }
  expect(firstPre).not.toBeNull();
  const runsAfterStream = highlightState.runs;
  for (const extra of ["\n\n补充一段。", "继续补充。"]) {
    text += extra;
    await render(text, onCopy, true);
    expect(highlightState.runs).toBe(runsAfterStream);
    expect(host.querySelector("pre")).toBe(firstPre);
    expect(host.querySelector("p")).toBe(firstParagraph);
  }
  expect(host.querySelector(".message-markdown")!.innerHTML).toBe(await markdownHtml(text));
});

test("流式中代码块闭合后仍保留高亮与原文复制", async () => {
  const code = "def gate(value):\n    return '<tag> & value'\n";
  const copy = await render("```python\n", undefined, true);
  await render("```python\n" + code + "```", copy, true);
  expect(host.querySelector(".hljs-keyword")).not.toBeNull();
  expect(host.querySelector(".message-code-head > span")?.textContent).toBe("python");
  await act(async () => host.querySelector<HTMLButtonElement>('[aria-label="复制代码"]')!.click());
  expect(copy).toHaveBeenCalledExactlyOnceWith(code);
});
