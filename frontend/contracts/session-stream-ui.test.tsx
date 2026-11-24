import { act, useLayoutEffect, useRef } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ChatHistory } from "../src/components/ChatHistory";
import { useSession, type SessionState } from "../src/hooks/use-session";
import type { Session, Turn } from "../src/lib/session-contracts";

// SSE接入回归：真实useSession + 真实ReadableStream SSE帧 + 合成HTTP，无生产网络。
// 执行策略hook替换为空桩，隔离其额外轮询。断言以行为为准，不依赖固定请求顺序。
vi.mock("../src/hooks/use-execution-policy", () => ({ useExecutionPolicy: () => ({ reload: () => undefined }) }));

const workspace = "local";
const sessionsBase = `/v2/workspaces/${workspace}/sessions`;
const sessionPathOf = (id: string) => `${sessionsBase}/${id}`;
const turnBaseOf = (sessionId: string, turnId: string) => `${sessionPathOf(sessionId)}/turns/${turnId}`;
const streamPathOf = (sessionId: string, turnId: string) => `${turnBaseOf(sessionId, turnId)}/stream`;
const key0 = "synthetic-stream-key";
const actor = { principal_id: "owner", generation: 1, legacy: false, workspaces: [{ workspace_id: workspace, role: "admin", approve: true }] };
const approvalData = { run_id: "run-1", request_id: "req-1", call_id: "call-1", name: "shell", arguments: {}, arguments_hash: "a".repeat(64), tool_hash: "b".repeat(64), expires_at: null };

function session(id: string): Session {
  return { id, workspace_id: workspace, owner_id: actor.principal_id, create_key: `create-${id}`, title: id, target_key: "demo", target_fingerprint: "a".repeat(64), context_policy: { max_bytes: 131072, max_messages: 100, reserved_output_tokens: 100, counter_version: "approx-utf8/1" }, revision: 1, status: "open", dispatch_paused: false, created_at: 1, closed_at: null, archived_at: null, deleted_at: null, lifecycle_revision: 0 };
}
function turn(id: string, sessionId: string, runId: string, status: Turn["status"], text = id, withTranscript = false, ordinal = 1): Turn {
  return { id, session_id: sessionId, workspace_id: workspace, owner_id: actor.principal_id, ordinal, request_key: `key-${id}`, text, accepted_revision: 1, status, run_id: runId, context_fingerprint: null, transcript: withTranscript ? [{ role: "user", content: text, call_id: null, tool_calls: [] }, { role: "assistant", content: `完成：${text}`, call_id: null, tool_calls: [] }] : null, code: null, created_at: 1, started_at: 1, finished_at: status === "running" || status === "queued" ? null : 3 };
}
function event(seq: number, kind: string, data: Record<string, unknown>, runId = "run-1") {
  return { run_id: runId, seq, event_id: `e${seq}`, kind, data, at: 1 };
}
function textEvent(seq: number, value: string, runId = "run-1") {
  return event(seq, "text", { step_index: 0, text: value }, runId);
}
type EventRecord = ReturnType<typeof event>;
const encoder = new TextEncoder();
function dataFrame(raw: EventRecord) {
  return `id: ${raw.seq}\nevent: ${raw.kind}\ndata: ${JSON.stringify(raw)}\n\n`;
}
const heartbeatFrame = ": heartbeat\n\n";
const errorFrame = (code: string) => `event: error\ndata: ${JSON.stringify({ code })}\n\n`;

interface Call { path: string; url: URL; method: string; authorization: string | null; signal: AbortSignal | null }
interface StreamHandle { call: Call; send: (...frames: string[]) => void; end: () => void }
let calls: Call[] = [];
let opened: StreamHandle[] = [];
let route!: (call: Call) => Response | Promise<Response>;

function headerOf(options: RequestInit, name: string): string | null {
  const raw = options.headers;
  if (!raw) return null;
  if (typeof Headers !== "undefined" && raw instanceof Headers) return raw.get(name);
  const entries: [string, string][] = Array.isArray(raw) ? raw as [string, string][] : Object.entries(raw as Record<string, string>);
  return entries.find(([key]) => key.toLowerCase() === name.toLowerCase())?.[1] ?? null;
}
function sse(call: Call): Response {
  let pump!: ReadableStreamDefaultController<Uint8Array>;
  const body = new ReadableStream<Uint8Array>({ start(c) { pump = c; } });
  const handle: StreamHandle = {
    call,
    send(...frames: string[]) { for (const frame of frames) { try { pump.enqueue(encoder.encode(frame)); } catch { /* 连接已中止，入队失败属预期 */ } } },
    end() { try { pump.close(); } catch { /* 已关闭 */ } },
  };
  opened.push(handle);
  return new Response(body, { status: 200, headers: { "Content-Type": "text/event-stream", "Cache-Control": "no-store" } });
}
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });
const deferred = () => { let resolve!: () => void; const promise = new Promise<void>(done => { resolve = done; }); return { promise, resolve }; };
function hangUntilGate(signal: AbortSignal | null, gate: ReturnType<typeof deferred>, payload: () => Response): Promise<Response> {
  const promise = new Promise<Response>((resolve, reject) => {
    const abort = () => reject(new DOMException("The operation was aborted.", "AbortError"));
    if (signal?.aborted) { abort(); return; }
    signal?.addEventListener("abort", abort, { once: true });
    void gate.promise.then(() => {
      signal?.removeEventListener("abort", abort);
      resolve(payload());
    }, abort);
  });
  promise.catch(() => undefined); // 悬挂响应被中止时的拒绝不得成为未处理拒绝
  return promise;
}

let sessionsPayload: Session[];
let turnsPayload: Turn[];
let turnsOtherPayload: Turn[];
let approvalsPayload: Record<string, unknown>[];
let approvalsHold: ReturnType<typeof deferred> | null;
let approvalsReads: number;
let resultReads: number;
let turnsReads: number;
let eventsSource: EventRecord[];
const defaultStream = (call: Call) => call.path.includes("/stream?") ? sse(call) : undefined;
function defaultRoute(call: Call): Response | Promise<Response> | undefined {
  if (call.path === "/v2/me") return json(actor);
  if (call.path === `${sessionsBase}?limit=50`) return json(sessionsPayload);
  if (call.path === sessionPathOf("existing")) return json(session("existing"));
  if (call.path === sessionPathOf("other")) return json(session("other"));
  if (call.path.startsWith(`${sessionPathOf("existing")}/turns?`)) { turnsReads++; return json(turnsPayload); }
  if (call.path.startsWith(`${sessionPathOf("other")}/turns?`)) return json(turnsOtherPayload);
  if (call.path.startsWith(`${turnBaseOf("existing", "t1")}/approvals`)) { approvalsReads++; if (approvalsHold) return hangUntilGate(call.signal, approvalsHold, () => json(approvalsPayload)); return json(approvalsPayload); }
  if (call.path.startsWith(`${turnBaseOf("existing", "t2")}/approvals`)) return json([]);
  if (call.path === `${turnBaseOf("existing", "t1")}/result`) { resultReads++; return json({ run: { id: "run-1" }, tools: [], events: [] }); }
  if (call.path === `${turnBaseOf("existing", "t1")}/trace`) return json({ session_id: "existing", turn_id: "t1", trace: { run_id: "run-1" } });
  if (call.path.startsWith(`${turnBaseOf("existing", "t1")}/events?`)) {
    const from = Number(call.url.searchParams.get("from_seq"));
    return json(eventsSource.filter(item => item.seq >= from));
  }
  return undefined;
}
function compose(streamPlan?: (call: Call) => Response | Promise<Response> | undefined) {
  return (call: Call): Response | Promise<Response> => {
    const answer = defaultRoute(call) ?? (streamPlan ?? defaultStream)(call);
    if (!answer) throw new Error(`未预期的请求：${call.method} ${call.path}`);
    return answer;
  };
}

// jsdom下用可控帧队列替换rAF：测试手动flush，精确断言"多个同步text只一次帧更新"。
let frameQueue: Map<number, () => void>;
let nextFrameId = 1;
const raf = (callback: FrameRequestCallback) => { const id = nextFrameId++; frameQueue.set(id, () => callback(performance.now())); return id; };
const cancelRaf = (id: number) => { frameQueue.delete(id); };
async function flushFrame() {
  // 先排空微任务：刚入队的ReadableStream块需经reader微任务处理后才会调度帧回调。
  await act(async () => { await sleep(0); });
  const pending = [...frameQueue.values()];
  frameQueue.clear();
  if (!pending.length) return;
  await act(async () => { for (const run of pending) run(); });
}

const fence = { current: 0 };
let activeKey = key0;
let chat: SessionState | null = null;
let host: HTMLDivElement | null = null;
let root: Root | null = null;
let unmounted = false;
let eventRefs = 0; // events引用变化次数（帧合并断言用）
function Harness() {
  const value = useSession(activeKey, true, fence, () => undefined);
  chat = value;
  const seen = useRef<unknown>(null);
  useLayoutEffect(() => {
    if (value.events !== seen.current) { seen.current = value.events; eventRefs++; }
  });
  return <ChatHistory chat={value} />;
}

const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
async function beat(ms = 5) { await act(async () => { await sleep(ms); }); }
async function until(check: () => void, budget = 2000, flush = false) {
  let last: unknown;
  const deadline = performance.now() + budget;
  for (;;) {
    if (flush) await flushFrame();
    try { check(); return; } catch (error) { last = error; }
    if (performance.now() >= deadline) throw last;
    await beat(10);
  }
}
async function hold(ms: number) { const deadline = performance.now() + ms; while (performance.now() < deadline) await beat(25); }
async function mount() {
  host = document.createElement("div");
  document.body.append(host);
  root = createRoot(host);
  await act(async () => root!.render(<Harness />));
  await until(() => expect(chat?.ready).toBe(true));
}
const streamCalls = () => calls.filter(call => call.path.includes("/stream?"));
const eventsCalls = () => calls.filter(call => call.path.includes("/events?"));
const seqs = () => (chat?.events ?? []).map(item => item.seq);
const texts = () => (chat?.events ?? []).map(item => (item.data as { text?: string }).text);

let originalWindowRaf: typeof window.requestAnimationFrame;
let originalWindowCaf: typeof window.cancelAnimationFrame;
beforeEach(() => {
  (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  localStorage.clear();
  history.replaceState(null, "", `/?view=chat&workspace=${workspace}&session=existing`);
  calls = []; opened = []; frameQueue = new Map(); nextFrameId = 1; eventRefs = 0;
  sessionsPayload = [session("existing"), session("other")];
  turnsPayload = [turn("t1", "existing", "run-1", "running", "第一问")];
  turnsOtherPayload = [turn("t0", "other", "run-0", "completed", "旧问题", true)];
  approvalsPayload = []; approvalsHold = null; approvalsReads = 0;
  resultReads = 0; turnsReads = 0; eventsSource = [];
  activeKey = key0; chat = null; unmounted = false;
  originalWindowRaf = window.requestAnimationFrame; originalWindowCaf = window.cancelAnimationFrame;
  window.requestAnimationFrame = raf; window.cancelAnimationFrame = cancelRaf;
  vi.stubGlobal("requestAnimationFrame", raf);
  vi.stubGlobal("cancelAnimationFrame", cancelRaf);
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => {
    const url = new URL(String(input), location.origin);
    const call: Call = { path: url.pathname + url.search, url, method: options.method ?? "GET", authorization: headerOf(options, "authorization"), signal: options.signal ?? null };
    calls.push(call);
    return route(call);
  }));
  route = compose();
});
afterEach(async () => {
  approvalsHold?.resolve();
  for (const handle of opened) handle.end();
  if (root && !unmounted) await act(async () => root!.unmount());
  host?.remove();
  window.requestAnimationFrame = originalWindowRaf; window.cancelAnimationFrame = originalWindowCaf;
  vi.unstubAllGlobals();
  chat = null; root = null; host = null;
});

describe("useSession SSE接入", () => {
  it("悬挂的审批不阻塞文字：多个同步text合并为一次帧提交，approval_request触发异步补读", async () => {
    approvalsHold = deferred();
    approvalsPayload = [approvalData];
    await mount();
    await until(() => expect(streamCalls()).toHaveLength(1));
    await until(() => expect(approvalsReads).toBeGreaterThanOrEqual(1)); // 流启动即异步读审批
    expect(chat?.approvals).toEqual([]);
    const stream = opened[0]!;
    const before = eventRefs;
    stream.send(dataFrame(textEvent(0, "甲")));
    await beat();
    stream.send(dataFrame(textEvent(1, "乙")));
    await beat();
    stream.send(dataFrame(textEvent(2, "丙")));
    await beat();
    expect(chat?.events).toEqual([]); // 帧未flush前不提交
    expect(eventRefs - before).toBe(0);
    await flushFrame();
    expect(eventRefs - before).toBe(1); // 三个同步text只一次帧更新
    expect(texts()).toEqual(["甲", "乙", "丙"]);
    expect(host!.textContent).toContain("甲乙丙");
    stream.send(dataFrame(event(3, "approval_request", approvalData)));
    await flushFrame();
    expect(chat?.events).toHaveLength(4);
    expect(approvalsReads).toBe(1); // 审批GET悬挂期间不重复请求，也不阻塞文字
    stream.send(dataFrame(textEvent(4, "丁")));
    await flushFrame();
    expect(host!.textContent).toContain("丁");
    approvalsHold.resolve();
    await until(() => expect(approvalsReads).toBe(2)); // dirty后补读审批
    await until(() => expect(chat?.approvals).toHaveLength(1));
    expect(host!.textContent).toContain("甲乙丙");
  }, 15000);

  it("EOF后按已确认seq重连，事件不重复且全程只GET", async () => {
    eventsSource = [textEvent(0, "甲"), textEvent(1, "乙"), textEvent(2, "丙")];
    await mount();
    await until(() => expect(streamCalls()).toHaveLength(1));
    expect(streamCalls()[0]!.path).toBe(`${streamPathOf("existing", "t1")}?from_seq=0`);
    expect(streamCalls()[0]!.method).toBe("GET");
    expect(streamCalls()[0]!.authorization).toBe(`Bearer ${key0}`);
    const first = opened[0]!;
    first.send(dataFrame(textEvent(0, "甲")), dataFrame(textEvent(1, "乙")), dataFrame(textEvent(2, "丙")));
    await flushFrame();
    expect(seqs()).toEqual([0, 1, 2]);
    first.end(); // EOF不代表终态，按确认cursor重连
    await until(() => expect(streamCalls()).toHaveLength(2), 2500);
    expect(streamCalls()[1]!.path).toBe(`${streamPathOf("existing", "t1")}?from_seq=3`);
    const second = opened[1]!;
    second.send(dataFrame(textEvent(3, "丁")));
    second.end();
    await until(() => expect(streamCalls()).toHaveLength(3), 2500);
    expect(streamCalls()[2]!.path).toBe(`${streamPathOf("existing", "t1")}?from_seq=4`);
    await flushFrame();
    expect(seqs()).toEqual([0, 1, 2, 3]);
    expect(chat!.events.every((item, index) => item.event_id === `e${index}`)).toBe(true);
    expect(calls.some(call => call.method === "POST")).toBe(false);
    expect(streamCalls().every(call => call.method === "GET")).toBe(true);
  }, 15000);

  it("terminal在连接未关闭时收尾：唤醒元数据、结果只读一次、下一轮接入第二条流并保留上一轮", async () => {
    eventsSource = [textEvent(0, "甲"), textEvent(1, "乙")];
    await mount();
    await until(() => expect(streamCalls()).toHaveLength(1));
    const first = opened[0]!;
    first.send(dataFrame(textEvent(0, "甲")), dataFrame(textEvent(1, "乙")));
    await flushFrame();
    expect(seqs()).toEqual([0, 1]);
    turnsPayload = [turn("t1", "existing", "run-1", "completed", "第一问", true), turn("t2", "existing", "run-2", "running", "第二问", false, 2)];
    first.send(dataFrame(event(2, "terminal", { status: "completed" }))); // 底层连接保持打开
    await until(() => expect(resultReads).toBe(1), 2500);
    await until(() => expect(chat?.turnFacts.t1).toMatchObject({ run: { id: "run-1" } }));
    expect(chat?.turnEvents.t1).toHaveLength(3); // 流内接收的甲、乙与terminal帧都在上一轮时间线里
    expect(chat?.turnEvents.t1!.at(-1)!.kind).toBe("terminal");
    await until(() => expect(streamCalls()).toHaveLength(2), 2500);
    expect(streamCalls()[1]!.path).toBe(`${streamPathOf("existing", "t2")}?from_seq=0`);
    expect(first.call.signal?.aborted).toBe(true); // 上一轮连接被取消
    expect(chat?.history.map(item => item.status)).toEqual(["completed", "running"]);
    const rounds = turnsReads;
    await until(() => expect(turnsReads).toBeGreaterThan(rounds), 2600); // ~1500ms元数据刷新
    expect(resultReads).toBe(1); // 最终结果不重复读取
    expect(streamCalls()).toHaveLength(2); // 不新建连接
    expect(chat?.turnEvents.t1).toHaveLength(3); // 上一轮事件保留
    expect(chat?.turnFacts.t1).toMatchObject({ run: { id: "run-1" } });
    expect(chat?.events).toEqual([]); // 第二轮流初始为空
  }, 20000);

  it.each(["select", "apiKey", "unmount"] as const)("切换会话、换密钥或卸载都中止旧流，迟到帧不污染（%s）", async mode => {
    await mount();
    await until(() => expect(streamCalls()).toHaveLength(1));
    const first = opened[0]!;
    first.send(dataFrame(textEvent(0, "原始")));
    await flushFrame();
    expect(seqs()).toEqual([0]);
    if (mode === "select") {
      await act(async () => chat!.select(workspace, "other"));
      await until(() => expect(chat?.session?.id).toBe("other"));
    } else if (mode === "apiKey") {
      activeKey = "rotated-key";
      await act(async () => root!.render(<Harness />));
      await until(() => expect(chat?.ready).toBe(true));
      await until(() => expect(streamCalls()).toHaveLength(2), 2500);
      expect(streamCalls()[1]!.authorization).toBe("Bearer rotated-key");
      expect(streamCalls()[1]!.path).toBe(`${streamPathOf("existing", "t1")}?from_seq=0`);
    } else {
      await act(async () => root!.unmount());
      unmounted = true;
    }
    await until(() => expect(first.call.signal?.aborted).toBe(true));
    first.send(dataFrame(textEvent(1, "晚到")), dataFrame(textEvent(2, "污染")));
    first.end();
    await hold(200);
    await flushFrame();
    if (mode === "unmount") return; // 已卸载，中止断言即可
    expect(chat?.events).toEqual([]); // 迟到旧帧不能进入当前视图
    expect(host!.textContent).not.toContain("晚到");
    expect(calls.some(call => call.method === "POST")).toBe(false);
  }, 15000);

  it.each(["HTTP 401", "HTTP 403", "SSE error unauthorized"] as const)("鉴权失败不重连也不回退（%s）", async mode => {
    if (mode === "SSE error unauthorized") {
      route = compose(call => {
        if (!call.path.includes("/stream?")) return undefined;
        const answer = sse(call);
        opened.at(-1)!.send(errorFrame("unauthorized"));
        opened.at(-1)!.end();
        return answer;
      });
    } else {
      const status = mode === "HTTP 401" ? 401 : 403;
      const code = mode === "HTTP 401" ? "unauthorized" : "permission_denied";
      route = compose(call => call.path.includes("/stream?") ? json({ error: { code } }, status) : undefined);
    }
    await mount();
    await until(() => expect(streamCalls()).toHaveLength(1));
    await until(() => expect(chat?.notice).toBeTruthy(), 2000);
    await hold(900);
    expect(streamCalls()).toHaveLength(1); // 不重连
    expect(eventsCalls()).toHaveLength(0); // 不回退/events轮询
    expect(calls.some(call => call.method === "POST")).toBe(false);
  }, 15000);

  it("心跳与静默不替换events引用，空历史对象保留", async () => {
    await mount();
    await until(() => expect(streamCalls()).toHaveLength(1));
    await flushFrame();
    const empty = chat!.events;
    expect(empty).toEqual([]);
    const before = eventRefs;
    opened[0]!.send(heartbeatFrame, heartbeatFrame, heartbeatFrame);
    await hold(250);
    expect(chat!.events).toBe(empty); // 心跳不产生新引用
    expect(eventRefs - before).toBe(0);
    opened[0]!.send(dataFrame(textEvent(0, "甲")));
    await flushFrame();
    const applied = chat!.events;
    expect(applied).not.toBe(empty);
    opened[0]!.send(heartbeatFrame, heartbeatFrame);
    await hold(250);
    expect(chat!.events).toBe(applied); // 事件存在后心跳同样保留引用
    expect(eventRefs - before).toBe(1);
    expect(host!.textContent).toContain("甲");
  }, 15000);

  it("rAF不执行时文字经~50ms定时器提交一次，随后flush不重复commit", async () => {
    await mount();
    await until(() => expect(streamCalls()).toHaveLength(1));
    const before = eventRefs;
    opened[0]!.send(dataFrame(textEvent(0, "甲")));
    await hold(70); // 帧回调保持未执行，50ms timer先到提交（后台遮挡场景）
    expect(texts()).toEqual(["甲"]);
    expect(eventRefs - before).toBe(1);
    const applied = chat!.events;
    await flushFrame(); // 迟到的帧回调被取消/守卫，不重复提交
    expect(eventRefs - before).toBe(1);
    expect(chat!.events).toBe(applied);
    expect(host!.textContent).toContain("甲");
  }, 15000);

  it.each(["404", "405", "501", "非SSE内容类型"] as const)("流式不支持时回退/events轮询读取文字（%s）", async mode => {
    eventsSource = [textEvent(0, "甲"), textEvent(1, "乙")];
    route = compose(call => {
      if (!call.path.includes("/stream?")) return undefined;
      if (mode === "非SSE内容类型") return new Response("[]", { status: 200, headers: { "Content-Type": "application/json" } });
      return json({ error: { code: "not_found" } }, Number(mode));
    });
    await mount();
    await until(() => expect(streamCalls()).toHaveLength(1));
    await until(() => expect(eventsCalls()).toHaveLength(1), 2500);
    expect(eventsCalls()[0]!.path).toContain("from_seq=0");
    await flushFrame();
    expect(texts()).toEqual(["甲", "乙"]);
    expect(streamCalls()).toHaveLength(1); // 不反复尝试流式
    eventsSource = [...eventsSource, textEvent(2, "丙")];
    await until(() => expect(texts()).toEqual(["甲", "乙", "丙"]), 2500, true); // 轮询读到丙并按帧提交
    expect(calls.some(call => call.method === "POST")).toBe(false);
    const cadence = eventsCalls().length;
    await hold(700);
    expect(eventsCalls().length - cadence).toBeGreaterThanOrEqual(2); // ~300ms轮询
  }, 20000);

  it("回退初次/events读悬挂时补读单飞：同seq不重复GET，事件只收一次", async () => {
    eventsSource = [textEvent(0, "甲"), textEvent(1, "乙")];
    const eventsGate = deferred();
    let zeroReads = 0;
    const base = compose();
    // 流式返回501触发回退；事件路由由本测试接管；其余端点沿用默认路由。
    route = call => {
      if (call.path.includes("/stream?")) return json({ error: { code: "not_found" } }, 501);
      if (!call.path.includes("/events?")) return base(call);
      const from = Number(call.url.searchParams.get("from_seq"));
      if (from === 0) {
        zeroReads++;
        return zeroReads === 1 ? hangUntilGate(call.signal, eventsGate, () => json([textEvent(0, "甲"), textEvent(1, "乙")])) : json(eventsSource.filter(item => item.seq >= from));
      }
      return json(eventsSource.filter(item => item.seq >= from));
    };
    await mount();
    await until(() => expect(streamCalls()).toHaveLength(1));
    await until(() => expect(zeroReads).toBe(1)); // 初次/events读已发出并悬挂
    const roundsBefore = turnsReads;
    await hold(1900); // 覆盖元数据刷新周期（首回退1500ms或300ms节奏均覆盖）
    await until(() => expect(turnsReads).toBeGreaterThan(roundsBefore), 2500); // 悬挂期间确有元数据轮次触发补读
    expect(zeroReads).toBe(1); // 单飞：补读复用同一Promise，不发同seq GET
    eventsGate.resolve();
    await until(() => expect(texts()).toEqual(["甲", "乙"]), 2500, true);
    expect(seqs()).toEqual([0, 1]); // 事件只收一次
    expect(zeroReads).toBe(1);
    eventsSource = [...eventsSource, textEvent(2, "丙")];
    await until(() => expect(texts()).toEqual(["甲", "乙", "丙"]), 2500, true); // 释放后轮询继续推进cursor
    expect(zeroReads).toBe(1);
    expect(calls.some(call => call.method === "POST")).toBe(false);
  }, 20000);

  it.each(["seq跳号", "run身份不符"] as const)("协议破坏报notice并停止（%s）", async mode => {
    await mount();
    await until(() => expect(streamCalls()).toHaveLength(1));
    const first = opened[0]!;
    first.send(dataFrame(textEvent(0, "甲")));
    await flushFrame();
    expect(seqs()).toEqual([0]);
    first.send(mode === "seq跳号" ? dataFrame(textEvent(2, "跳")) : dataFrame(textEvent(1, "冒", "run-other")));
    await until(() => expect(chat?.notice).toBeTruthy(), 2000);
    await hold(700);
    expect(streamCalls()).toHaveLength(1); // 停止，不重连
    expect(eventsCalls()).toHaveLength(0); // 不回退
    expect(seqs()).toEqual([0]); // 越界事件未进入
    expect(host!.textContent).not.toContain(mode === "seq跳号" ? "跳" : "冒");
    expect(calls.some(call => call.method === "POST")).toBe(false);
  }, 15000);
});
