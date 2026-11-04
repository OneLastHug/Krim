import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { MessageMarkdown } from "../src/components/messages/MessageMarkdown";

let root: Root;
let host: HTMLDivElement;

beforeEach(() => {
  host = document.createElement("div");
  document.body.append(host);
  root = createRoot(host);
  (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); });

async function render(text: string, copy = vi.fn(async (_text: string) => true)) {
  await act(async () => root.render(<MessageMarkdown text={text} onCopy={copy} />));
  return copy;
}

test("共享消息保留高亮前代码的缩进、符号和换行", async () => {
  const code = "def example():\n\treturn '<tag> & value'\n";
  const copy = await render("```python\n" + code + "```");
  expect(host.querySelector(".hljs-keyword")).not.toBeNull();
  await act(async () => host.querySelector<HTMLButtonElement>('[aria-label="复制代码"]')!.click());
  expect(copy).toHaveBeenCalledExactlyOnceWith(code);
  expect(host.querySelector("pre")!.getAttribute("tabindex")).toBe("0");
  expect(host.querySelector("[data-krim-code-text]")).toBeNull();
});

test("未知语言和未闭合代码仍能复制文本", async () => {
  const copy = await render("```not-registered\n    value: false\n\tresult: null");
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
