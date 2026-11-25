import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { ChatHistory } from "../src/components/ChatHistory";
import type { SessionState } from "../src/hooks/use-session";
import type { Turn } from "../src/lib/session-contracts";

// ChatHistory滚动契约：真实ChatHistory + 合成SessionState。jsdom无布局，
// scrollTop/scrollHeight/clientHeight经原型存根接管、ResizeObserver可手动触发，
// 断言只看滚动行为，不依赖CSS与真实尺寸。
class ResizeObserverStub {
  static instances: ResizeObserverStub[] = [];
  readonly callback: ResizeObserverCallback;
  constructor(callback: ResizeObserverCallback) { this.callback = callback; ResizeObserverStub.instances.push(this); }
  observe(): void {}
  unobserve(): void {}
  disconnect(): void {}
}

interface EventRecord { run_id: string; seq: number; event_id: string; kind: string; data: Record<string, unknown>; at: number }
type Geometry = { scrollTop: number; scrollHeight: number; clientHeight: number };

let geometries = new WeakMap<HTMLElement, Geometry>();
let savedDescriptors: [string, PropertyDescriptor][] = [];
let addedScrollIntoView = false;
let root: Root | null = null;
let host: HTMLDivElement | null = null;

beforeAll(() => {
  if (typeof Element.prototype.scrollIntoView !== "function") {
    Object.defineProperty(Element.prototype, "scrollIntoView", { value: () => {}, configurable: true, writable: true });
    addedScrollIntoView = true;
  }
  for (const key of ["scrollTop", "scrollHeight", "clientHeight"]) {
    const descriptor = Object.getOwnPropertyDescriptor(Element.prototype, key);
    if (!descriptor?.get) continue;
    savedDescriptors.push([key, descriptor]);
    const setter = descriptor.set;
    Object.defineProperty(Element.prototype, key, {
      configurable: true,
      enumerable: descriptor.enumerable,
      get(this: Element) {
        const geometry = geometries.get(this as HTMLElement);
        return geometry ? geometry[key as keyof Geometry] : descriptor.get!.call(this);
      },
      set: setter
        ? function (this: Element, value: number) {
            const geometry = geometries.get(this as HTMLElement);
            if (geometry) geometry.scrollTop = value; else setter.call(this, value);
          }
        : undefined,
    });
  }
});
afterAll(() => {
  for (const [key, descriptor] of savedDescriptors) Object.defineProperty(Element.prototype, key, descriptor);
  if (addedScrollIntoView) Reflect.deleteProperty(Element.prototype, "scrollIntoView");
});

beforeEach(() => {
  (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  geometries = new WeakMap();
  host = document.createElement("div");
  document.body.append(host);
  root = createRoot(host);
  history.replaceState(null, "", "/?view=chat&workspace=ws&session=s1");
  ResizeObserverStub.instances.length = 0;
  vi.stubGlobal("ResizeObserver", ResizeObserverStub);
});
afterEach(async () => {
  const mounted = root;
  if (mounted) await act(async () => mounted.unmount());
  host?.remove();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  root = null; host = null;
});

function turn(id: string, status: Turn["status"]): Turn {
  return { id, session_id: "s1", workspace_id: "ws", owner_id: "owner", ordinal: 1, request_key: `key-${id}`, text: `问题 ${id}`, accepted_revision: 1, status, run_id: status === "queued" ? null : `run-${id}`, context_fingerprint: null, transcript: null, code: null, created_at: 1, started_at: status === "queued" ? null : 2, finished_at: status === "running" || status === "queued" ? null : 3 };
}
function textEvent(seq: number, value: string, runId: string): EventRecord {
  return { run_id: runId, seq, event_id: `e-${runId}-${seq}`, kind: "text", data: { step_index: 0, text: value }, at: 1 };
}
function chatOf(history: Turn[], overrides: { events?: EventRecord[]; turnEvents?: Record<string, EventRecord[]> } = {}): SessionState {
  const currentTurn = history.find(item => item.status === "running" || item.status === "queued") ?? null;
  return { id: "s1", workspace: "ws", ready: true, history, currentTurn, details: {}, turnEvents: overrides.turnEvents ?? {}, turnFacts: {}, events: overrides.events ?? [], approvals: [], pendingControls: [], session: { status: "open" }, canWrite: true, canApprove: true, controlling: false, turnPending: false, edit: vi.fn(), control: vi.fn(async () => {}), loadDetails: vi.fn(async () => {}) } as unknown as SessionState;
}

// 先以空历史挂载拿到transcript再接管几何量，随后带真实历史重挂，
// 保证挂载期的贴底与focusTurn逻辑跑在受控几何上。
async function mountScroll(initial: SessionState, values: { scrollHeight: number; clientHeight: number }) {
  await act(async () => root!.render(<ChatHistory chat={chatOf([])} />));
  const panel = host!.querySelector<HTMLElement>(".chat-transcript")!;
  const geometry: Geometry = { scrollTop: 0, scrollHeight: values.scrollHeight, clientHeight: values.clientHeight };
  geometries.set(panel, geometry);
  const thread = host!.querySelector<HTMLElement>(".chat-thread")!;
  await act(async () => root!.render(<ChatHistory chat={initial} />));
  return { panel, thread, geometry };
}
async function fireResize() {
  await act(async () => { for (const observer of ResizeObserverStub.instances) observer.callback([], observer as unknown as ResizeObserver); });
}
const jumpButton = () => host!.querySelector<HTMLButtonElement>('[aria-label="回到底部"]');

describe("ChatHistory 滚动契约", () => {
  it.each(["running", "queued"] as const)("最新进行中轮次带?turn=挂载即跟随，内容长高后ResizeObserver回调仍贴底（%s）", async status => {
    const live = turn("t-live", status);
    history.replaceState(null, "", "/?view=chat&workspace=ws&session=s1&turn=t-live");
    const { geometry } = await mountScroll(chatOf([turn("t-old", "completed"), live]), { scrollHeight: 1000, clientHeight: 300 });
    expect(geometry.scrollTop).toBe(700); // scrollHeight-clientHeight，提交回执继续跟随
    expect(jumpButton()).toBeNull();
    geometry.scrollHeight = 2200; // 流式内容长高
    await fireResize();
    expect(geometry.scrollTop).toBe(1900); // 贴底 = scrollHeight - clientHeight
    await act(async () => window.dispatchEvent(new Event("krim:focus-turn")));
    expect(geometry.scrollTop).toBe(1900); // 再次广播仍跟随最新进行中轮次
    expect(jumpButton()).toBeNull();
  });

  it("定位已完成旧轮次只平移transcript.scrollTop，不用scrollIntoView牵动外层", async () => {
    history.replaceState(null, "", "/?view=chat&workspace=ws&session=s1&turn=t-old");
    vi.spyOn(Element.prototype, "getBoundingClientRect").mockImplementation(function (this: Element) {
      const rect = this.classList.contains("chat-transcript")
        ? { top: 0, height: 300, bottom: 300, left: 0, right: 0, width: 0 }
        : { top: 1200, height: 200, bottom: 1400, left: 0, right: 0, width: 0 };
      return rect as unknown as DOMRect;
    });
    const scrollIntoView = vi.spyOn(Element.prototype, "scrollIntoView").mockImplementation(() => {});
    const { geometry } = await mountScroll(chatOf([turn("t-old", "completed"), turn("t-live", "running")]), { scrollHeight: 3000, clientHeight: 300 });
    expect(geometry.scrollTop).toBe(3850); // 挂载定位：先贴底2700，再平移1150
    geometry.scrollTop = 1000;
    await act(async () => window.dispatchEvent(new Event("krim:focus-turn")));
    expect(geometry.scrollTop).toBe(2150); // 事件驱动重复定位按当前scrollTop平移同一差值
    expect(scrollIntoView).not.toHaveBeenCalled();
    expect(window.scrollY).toBe(0);
    expect(document.documentElement.scrollTop).toBe(0);
    expect(document.body.scrollTop).toBe(0);
  });

  it("用户上滚后新文字与内容长高不抢位置，回到底部按钮恢复贴底", async () => {
    const live = turn("t-live", "running");
    const events = [textEvent(0, "甲", "run-t-live")];
    const { panel, geometry } = await mountScroll(chatOf([live], { events }), { scrollHeight: 1000, clientHeight: 300 });
    expect(host!.textContent).toContain("甲");
    panel.scrollTop = 100;
    await act(async () => panel.dispatchEvent(new Event("scroll", { bubbles: true })));
    expect(jumpButton()).not.toBeNull();
    await act(async () => root!.render(<ChatHistory chat={chatOf([live], { events: [...events, textEvent(1, "乙", "run-t-live")] })} />));
    geometry.scrollHeight = 1600;
    await fireResize();
    expect(geometry.scrollTop).toBe(100); // 离底后新文字与长高都不抢滚动位置
    expect(host!.textContent).toContain("乙");
    await act(async () => jumpButton()!.click());
    expect(geometry.scrollTop).toBe(1300); // 回到底部恢复贴底
    expect(jumpButton()).toBeNull();
  });

  it("纯文本DOM变更不全树数审批，涉及审批的变更才更新待处理计数", async () => {
    const done = turn("t-done", "completed");
    const { panel, thread } = await mountScroll(chatOf([done], { turnEvents: { "t-done": [textEvent(0, "第一段", "run-t-done")] } }), { scrollHeight: 1000, clientHeight: 300 });
    panel.scrollTop = 100;
    await act(async () => panel.dispatchEvent(new Event("scroll", { bubbles: true }))); // 离底显示悬浮条，计数才可见
    const countCalls = vi.spyOn(thread, "querySelectorAll");
    countCalls.mockClear();
    const node = (className: string, text: string) => { const element = document.createElement("div"); element.className = className; element.textContent = text; return element; };
    await act(async () => host!.querySelector(".chat-answer")!.append(document.createTextNode("纯文本追加")));
    await act(async () => thread.append(node("", "无关节点")));
    expect(countCalls).not.toHaveBeenCalled(); // 文本与无关节点不触发全树统计
    expect(host!.textContent).not.toContain("待处理操作");
    await act(async () => { thread.append(node("chat-approval", "审批一")); await new Promise(resolve => setTimeout(resolve, 0)); });
    expect(countCalls).toHaveBeenCalledTimes(1);
    expect(host!.textContent).toContain("1 项待处理操作");
    await act(async () => { thread.append(node("chat-approval", "审批二")); await new Promise(resolve => setTimeout(resolve, 0)); });
    expect(host!.textContent).toContain("2 项待处理操作");
  });
});
