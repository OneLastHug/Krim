import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { toolKind, toolSummary, formatToolValue } from "../src/lib/tool-display";
import { useScrollbars } from "../src/hooks/use-scrollbars";
import { ContextSummary, contextCount } from "../src/components/ContextSummary";
import type { SessionContextState } from "../src/hooks/use-session-context";

let root: Root, host: HTMLDivElement;
beforeEach(() => {
  (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  host = document.createElement("div"); document.body.append(host); root = createRoot(host);
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); vi.useRealTimers(); });
function Scrollers() { useScrollbars(); return <><div data-testid="sidebar"/><div data-testid="transcript"/></>; }

test("滚动条只在对应容器滚动时显示，800ms 后独立隐藏", async () => {
  vi.useFakeTimers();
  await act(async () => root.render(<Scrollers/>));
  const [side, transcript] = [...host.querySelectorAll<HTMLElement>("[data-testid]")];
  side!.dispatchEvent(new Event("scroll")); transcript!.dispatchEvent(new Event("scroll"));
  expect(side!.dataset.scrolling).toBe("true"); expect(transcript!.dataset.scrolling).toBe("true");
  vi.advanceTimersByTime(400); side!.dispatchEvent(new Event("scroll")); vi.advanceTimersByTime(401);
  expect(transcript!.dataset.scrolling).toBeUndefined(); expect(side!.dataset.scrolling).toBe("true");
  vi.advanceTimersByTime(400); expect(side!.dataset.scrolling).toBeUndefined();
});
test("卸载取消滚动条计时并清除活动标记", async () => {
  vi.useFakeTimers(); await act(async () => root.render(<Scrollers/>));
  const side = host.querySelector<HTMLElement>("[data-testid]")!;
  side.dispatchEvent(new Event("scroll")); expect(vi.getTimerCount()).toBe(1);
  await act(async () => root.render(<div/>)); expect(vi.getTimerCount()).toBe(0); expect(side.dataset.scrolling).toBeUndefined();
});
test("工具图标类别和摘要使用实际工具名及参数", () => {
  expect(toolKind("file_read")).toBe("file"); expect(toolKind("shell_exec")).toBe("terminal");
  expect(toolKind("grep")).toBe("search"); expect(toolKind("mcp:service:tool")).toBe("mcp");
  expect(toolSummary({ command: "npm\n  run check" })).toBe("npm run check");
  expect(toolSummary(null)).toBe(""); expect(toolSummary({ path: "a".repeat(250) })).toHaveLength(180);
});
test("常见工具文本格式化；未知结构保留 JSON", () => {
  expect(formatToolValue("line1\nline2")).toBe("line1\nline2");
  expect(formatToolValue({ stdout: "ok", stderr: "warning" })).toBe("ok\nwarning");
  expect(formatToolValue({ content: [{ type: "text", text: "first" }, { type: "text", text: "second" }] })).toBe("first\nsecond");
  expect(formatToolValue({ exit_code: 2 })).toContain('"exit_code": 2');
});
function context(): SessionContextState {
  return { data: null, manifest: null, turnId: "", limit: 900000, used: 0, loading: false, error: "", refresh: vi.fn() };
}
test("首轮前显示空状态，不伪造来源数据或模型用量", async () => {
  await act(async () => root.render(<ContextSummary context={context()}/>));
  expect(host.textContent).toContain("0 / 900k"); expect(host.textContent).toContain("发送首条消息后");
  expect(host.querySelector(".context-composition")).toBeNull();
});
test("未知 token 保持未知；来源构成使用字节且列出未归属余量", async () => {
  const value = context(); value.turnId = "T"; value.used = null;
  value.manifest = { version: 1, counter_version: "approx-utf8/1", input_bytes: 100, estimated_tokens: null, model_context_window: null, reserved_output_tokens: null, input_budget_bytes: null, compaction_changes: null, items_truncated: true, omitted_items: 1, source_refs_truncated: false,
    items: [{ key: "tool", category: "tools", label: "Tools", content_hash: "a".repeat(64), input_bytes: 60, estimated_tokens: null, message_index: null, tool_index: null, call_id: null, provider_call_id: null, source_refs: null, source_refs_truncated: false }] };
  await act(async () => root.render(<ContextSummary context={value}/>));
  expect(host.textContent).toContain("— / 900k"); expect(host.textContent).toContain("工具定义");
  expect(host.textContent).toContain("未归属"); expect(host.textContent).toContain("60.0%"); expect(host.textContent).toContain("40.0%");
  expect(host.textContent).not.toContain("NaN"); expect(host.textContent).not.toContain("Infinity");
  expect(host.querySelector(".context-composition title")?.textContent).toContain("字节");
});
test("上下文读取错误提供重试，详情跳转保留指定轮次", async () => {
  const value = context(), trace = vi.fn(); value.error = "读取失败"; value.turnId = "T";
  await act(async () => root.render(<ContextSummary context={value} onTrace={trace}/>));
  const buttons = [...host.querySelectorAll("button")];
  await act(async () => buttons.find(button => button.textContent === "重试")!.click()); expect(value.refresh).toHaveBeenCalledOnce();
  await act(async () => buttons.find(button => button.textContent === "查看完整上下文")!.click()); expect(trace).toHaveBeenCalledOnce();
});
test("上下文数字格式保留未知值且不丢失百万级容量", () => {
  expect(contextCount(null)).toBe("—"); expect(contextCount(900000)).toBe("900k"); expect(contextCount(0)).toBe("0");
});
