import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { toolKind, toolSummary, formatToolValue } from "../src/lib/tool-display";
import { useScrollbars } from "../src/hooks/use-scrollbars";
import { ContextSummary, contextCount } from "../src/components/ContextSummary";
import type { SessionContextState } from "../src/hooks/use-session-context";
import { ChatHistory } from "../src/components/ChatHistory";
import type { SessionState } from "../src/hooks/use-session";
import type { Turn } from "../src/lib/session-contracts";

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
function actionChat(status: Turn["status"] = "running", overrides: Partial<SessionState> = {}): SessionState {
  const turn: Turn = { id: "turn-1", session_id: "session-1", workspace_id: "workspace-1", owner_id: "owner-1",
    ordinal: 1, request_key: "request-1", text: "用户原始问题", accepted_revision: 1, status,
    run_id: status === "queued" ? null : "run-1", context_fingerprint: null, transcript: null, code: null,
    created_at: 1, started_at: status === "queued" ? null : 2, finished_at: status === "running" || status === "queued" ? null : 3 };
  return { id: "session-1", workspace: "workspace-1", ready: true, history: [turn], currentTurn: turn,
    details: {}, turnEvents: {}, turnFacts: {}, events: [], approvals: [], pendingControls: [],
    session: { status: "open" }, canWrite: true, canApprove: true, controlling: false,
    edit: vi.fn(), control: vi.fn(async () => {}), loadDetails: vi.fn(async () => {}), ...overrides } as unknown as SessionState;
}
test("运行状态、复制、复用与停止合并在同一图标操作组", async () => {
  const chat = actionChat();
  await act(async () => root.render(<ChatHistory chat={chat}/>));
  const row = host.querySelector('.chat-user-actions[aria-label="本轮消息操作"]')!;
  expect([...row.querySelectorAll("button")].map(button => button.getAttribute("aria-label")))
    .toEqual(["复制用户消息", "复用提问", "停止此轮"]);
  expect(host.querySelector(".chat-turn-meta,.chat-waiting")).toBeNull();
  const state = row.querySelector('[role="status"]')!;
  expect(state.getAttribute("aria-label")).toBe("进行中");
  expect(state.querySelector(".sr-only")?.textContent).toBe("进行中");
  expect(state.querySelector("svg")?.classList.contains("motion-safe:animate-spin")).toBe(true);
  expect(host.querySelector(".chat-user-wrap")?.classList.contains("has-turn-state")).toBe(true);
  const stop = row.querySelector<HTMLButtonElement>('[aria-label="停止此轮"]')!;
  expect(stop.textContent?.trim()).toBe(""); expect(stop.querySelector("svg")).not.toBeNull();
  expect(stop.title).toBe("停止此轮");
  await act(async () => stop.click());
  expect(chat.control).toHaveBeenCalledExactlyOnceWith(chat.history[0], "cancel");
});
test.each([
  ["queued", "排队中", "lucide-clock"], ["failed", "失败", "lucide-circle-alert"],
  ["cancelled", "已停止", "lucide-ban"], ["interrupted", "已中断", "lucide-ban"],
] as const)("%s 用状态图标保留语义，不另起文字行", async (status, label, icon) => {
  await act(async () => root.render(<ChatHistory chat={actionChat(status)}/>));
  const row = host.querySelector(".chat-user-actions")!;
  expect(row.querySelector('[role="status"]')?.getAttribute("aria-label")).toBe(label);
  expect(row.querySelector(".chat-turn-status svg")?.classList.contains(icon)).toBe(true);
  expect(row.querySelector('[aria-label="停止此轮"]') !== null).toBe(status === "queued");
  expect(host.querySelector(".chat-turn-meta")).toBeNull();
});
test("已完成轮次不显示状态和停止，保留复制与复用", async () => {
  await act(async () => root.render(<ChatHistory chat={actionChat("completed")}/>));
  const row = host.querySelector(".chat-user-actions")!;
  expect(row.querySelector(".chat-turn-status")).toBeNull();
  expect(row.querySelector('[aria-label="停止此轮"]')).toBeNull();
  expect(row.querySelectorAll("button")).toHaveLength(2);
  expect(host.querySelector(".has-turn-state")).toBeNull();
});
test.each([{ canWrite: false, controlling: false }, { canWrite: true, controlling: true }])(
  "停止图标保留原有权限与请求中禁用条件 %j", async overrides => {
    const chat = actionChat("running", overrides);
    await act(async () => root.render(<ChatHistory chat={chat}/>));
    const stop = host.querySelector<HTMLButtonElement>('.chat-user-actions [aria-label="停止此轮"]')!;
    expect(stop.disabled).toBe(true);
    await act(async () => stop.click());
    expect(chat.control).not.toHaveBeenCalled();
  },
);
test("合并后的复制和复用仍使用该轮原文", async () => {
  const descriptor = Object.getOwnPropertyDescriptor(navigator, "clipboard");
  const writeText = vi.fn(async () => {});
  Object.defineProperty(navigator, "clipboard", { configurable: true, value: { writeText } });
  try {
    const chat = actionChat();
    await act(async () => root.render(<ChatHistory chat={chat}/>));
    await act(async () => host.querySelector<HTMLButtonElement>('.chat-user-actions [aria-label="复制用户消息"]')!.click());
    expect(writeText).toHaveBeenCalledExactlyOnceWith(chat.history[0]!.text);
    await act(async () => host.querySelector<HTMLButtonElement>('.chat-user-actions [aria-label="复用提问"]')!.click());
    expect(chat.edit).toHaveBeenCalledExactlyOnceWith(chat.history[0]!.text);
  } finally {
    if (descriptor) Object.defineProperty(navigator, "clipboard", descriptor);
    else Reflect.deleteProperty(navigator, "clipboard");
  }
});
test("等待审批在同一操作行使用静态盾牌图标", async () => {
  const chat = actionChat("running", { approvals: [{ request_id: "approval-1", run_id: "run-1", call_id: "call-1",
    name: "lookup", arguments: { query: "x" }, arguments_hash: "a".repeat(64), tool_hash: "b".repeat(64), expires_at: null }] });
  await act(async () => root.render(<ChatHistory chat={chat}/>));
  const state = host.querySelector(".chat-user-actions .chat-turn-status")!;
  expect(state.getAttribute("aria-label")).toBe("等待审批");
  expect(state.querySelector(".lucide-shield")).not.toBeNull();
  expect(state.querySelector("svg")?.classList.contains("motion-safe:animate-spin")).toBe(false);
});

function context(): SessionContextState {
  return { data: null, manifest: null, reported: null, requestId: "", turnId: "", limit: 900000, used: 0, loading: false, error: "", refresh: vi.fn() };
}
test("首轮前显示空状态，不伪造来源数据或模型用量", async () => {
  await act(async () => root.render(<ContextSummary context={context()}/>));
  expect(host.textContent).toContain("0 / 900,000"); expect(host.textContent).toContain("发送首条消息后");
  expect(host.querySelector(".context-composition")).toBeNull();
});
test("未知 token 保持未知；来源构成使用字节且列出未归属余量", async () => {
  const value = context(); value.turnId = "T"; value.used = null;
  value.manifest = { version: 1, counter_version: "approx-utf8/1", input_bytes: 100, estimated_tokens: null, model_context_window: null, reserved_output_tokens: null, input_budget_bytes: null, compaction_changes: null, items_truncated: true, omitted_items: 1, source_refs_truncated: false,
    items: [{ key: "tool", category: "tools", label: "Tools", content_hash: "a".repeat(64), input_bytes: 60, estimated_tokens: null, message_index: null, tool_index: null, call_id: null, provider_call_id: null, source_refs: null, source_refs_truncated: false }] };
  await act(async () => root.render(<ContextSummary context={value}/>));
  expect(host.textContent).toContain("— / 900,000"); expect(host.textContent).toContain("工具定义");
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
test("上下文数字显示完整整数，保留未知和零值", () => {
  expect(contextCount(null)).toBe("—"); expect(contextCount(900000)).toBe("900,000"); expect(contextCount(0)).toBe("0");
  expect(contextCount(9123)).toBe("9,123"); expect(contextCount(1000001)).toBe("1,000,001");
});
function countedContext(): SessionContextState {
  return { ...context(), turnId: "turn", requestId: "request", used: 9450,
    manifest: { version: 1, counter_version: "calibrated/heuristic-script/1", input_bytes: 37800,
      estimated_tokens: 9450, model_context_window: 1000000, reserved_output_tokens: 32000,
      input_budget_bytes: null, compaction_changes: null, items_truncated: false, omitted_items: 0, source_refs_truncated: false,
      items: [{ key: "history", category: "history", label: "历史", content_hash: "b".repeat(64), input_bytes: 37800,
        estimated_tokens: 9450, message_index: 0, tool_index: null, call_id: null, provider_call_id: null, source_refs: null, source_refs_truncated: false }] },
    reported: { input: 9123, output: 2173, total: 11296, cached: 4001, cacheWrite: 0, reasoning: 432, final: true },
  };
}
test("上报用量与上次请求预估分开展示，数值不缩写也不加约等号", async () => {
  await act(async () => root.render(<ContextSummary context={countedContext()} compactView/>));
  expect(host.querySelector(".g-context-overview")?.textContent).toContain("9,450 / 900,000");
  expect(host.textContent).toContain("上次请求输入预估 · calibrated/heuristic-script/1");
  const usage = host.querySelector('[aria-label="上次请求上报用量"]');
  expect([...usage!.querySelectorAll("tr")].map(row => [...row.querySelectorAll("td")].slice(0, 2).map(cell => cell.textContent))).toEqual([
    ["输入", "9,123"], ["输出", "2,173"], ["总计", "11,296"], ["缓存读取", "4,001"], ["缓存写入", "0"], ["推理输出", "432"],
  ]);
  expect(usage?.textContent).not.toContain("≈"); expect(usage?.textContent).not.toMatch(/\d(?:\.\d+)?[kKM]\b/);
  expect(host.querySelector(".g-context-overview span")?.getAttribute("title")).toContain("上次请求输入预估");
});
test("没有上报用量时不把上下文预估填到实际输入", async () => {
  const value = countedContext(); value.reported = null;
  await act(async () => root.render(<ContextSummary context={value} compactView/>));
  expect(host.textContent).toContain("上次请求未上报用量");
  expect(host.querySelector('[aria-label="上次请求上报用量"]')).toBeNull();
  expect(host.querySelector(".g-context-overview")?.textContent).toContain("9,450 / 900,000");
});
test("部分上报显示未完成和未知总量，不自行计算或补零", async () => {
  const value = countedContext(); value.reported = { ...value.reported!, total: null, final: false };
  await act(async () => root.render(<ContextSummary context={value} compactView/>));
  const total = [...host.querySelectorAll('[aria-label="上次请求上报用量"] tr')].find(row => row.firstElementChild?.textContent === "总计");
  expect(total?.textContent).toContain("未知");
  expect(total?.textContent).not.toContain("11,296");
  expect(host.textContent).toContain("上次请求上报（未完成）");
});
