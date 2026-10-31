import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { TraceWorkbench } from "../src/components/TraceWorkbench";
import { parseTraceDetail, parseTracePage, traceDuration, traceSpanRows, type TraceIndexItem } from "../src/lib/trace-contracts";
import type { Actor } from "../src/lib/session-contracts";

let root: Root, host: HTMLDivElement;
let handler: (path: string, options: RequestInit) => Response | Promise<Response>;
const calls: { path: string; method: string; headers: HeadersInit | undefined }[] = [];
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });
const defaultIdentity = (): Actor => ({ principal_id: "operator", generation: 1, legacy: false, workspaces: [{ workspace_id: "local", role: "admin", approve: true }] });
function item(runId: string, sessionId = "S", turnId = "T"): TraceIndexItem {
  return { source_kind: "session_turn", run_id: runId, trace_id: `trace-${runId}`, session_id: sessionId, turn_id: turnId, session_title: `会话 ${sessionId}`, target_key: "target-http", status: "completed", created_at: 1_700_000_000, finished_at: 1_700_000_004 };
}
function page(items: TraceIndexItem[], next_cursor: string | null = null) { return { items, next_cursor }; }
function detail(value: TraceIndexItem, marker = "node-value") {
  return { session_id: value.session_id, turn_id: value.turn_id, trace: { run_id: value.run_id, trace_id: value.trace_id, complete: true, reasons: [], spans: [
    { run_id: value.run_id, trace_id: value.trace_id, span_id: "root", parent_span_id: null, kind: "agent", start: 1_700_000_000, end: 1_700_000_001, status: "completed", attributes: { marker } },
    { run_id: value.run_id, trace_id: value.trace_id, span_id: "child", parent_span_id: "root", kind: "model", start: 1_700_000_000.25, end: 1_700_000_000.75, status: "completed", attributes: { child: true } },
  ] } };
}
function props(overrides: Partial<React.ComponentProps<typeof TraceWorkbench>> = {}): React.ComponentProps<typeof TraceWorkbench> {
  return { apiKey: "", workspace: "local", identity: defaultIdentity(), active: true, ...overrides };
}
beforeEach(() => {
  calls.length = 0;
  history.replaceState({}, "", "/ui/?view=trace&workspace=local");
  handler = path => path.includes("/traces?") ? json(page([])) : json({});
  host = document.createElement("div"); document.body.append(host); root = createRoot(host);
  (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => {
    const path = String(input); calls.push({ path, method: options.method ?? "GET", headers: options.headers });
    return handler(path, options);
  }));
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); vi.unstubAllGlobals(); });
async function mount(value = props()) { await act(async () => root.render(<TraceWorkbench {...value}/>)); }
async function rerender(value: React.ComponentProps<typeof TraceWorkbench>) { await act(async () => root.render(<TraceWorkbench {...value}/>)); }
async function until(check: () => void) {
  let error: unknown;
  for (let index = 0; index < 80; index++) { try { check(); return; } catch (reason) { error = reason; await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); }); } }
  throw error;
}
function button(label: string) {
  const found = [...host.querySelectorAll("button")].find(value => value.getAttribute("aria-label") === label || value.textContent?.trim() === label);
  expect(found, label).toBeTruthy(); return found!;
}
function rowButton(label: string) {
  const found = [...host.querySelectorAll<HTMLButtonElement>(".trace-index-row")].find(value => value.textContent?.includes(label));
  expect(found, label).toBeTruthy(); return found!;
}
function indexCalls() { return calls.filter(call => call.path.includes("/v2/workspaces/") && call.path.includes("/traces?")); }
function actor(principal_id: string, generation: number, workspaces = ["local"]): Actor {
  return { principal_id, generation, legacy: false, workspaces: workspaces.map(workspace_id => ({ workspace_id, role: "admin", approve: true })) };
}
function deferred<T>() { let resolve!: (value: T) => void; const promise = new Promise<T>(done => { resolve = done; }); return { promise, resolve }; }

test("空密钥可用本地身份只读索引与节点详情，返回会话并且零POST", async () => {
  const onSelection = vi.fn(), onSession = vi.fn(), first = item("run-1");
  handler = path => path.includes("/traces?") ? json(page([first])) : path.endsWith("/trace") ? json(detail(first)) : json({});
  await mount(props({ onSelection, onSession }));
  await until(() => expect(host.textContent).toContain("会话 S"));
  expect(indexCalls()).toHaveLength(1);
  expect(indexCalls()[0]!.path).toContain("limit=20");
  expect((indexCalls()[0]!.headers as Record<string, string>).Authorization).toBeUndefined();
  await act(async () => rowButton("会话 S").click());
  await until(() => expect(host.textContent).toContain("node-value"));
  expect(onSelection).toHaveBeenCalledWith("run-1");
  await act(async () => button("返回对话").click());
  expect(onSession).toHaveBeenCalledWith("S");
  expect(calls.every(value => value.method === "GET")).toBe(true);
  expect(calls.every(value => value.path.startsWith("/v2/"))).toBe(true);
});

test("可写工作区的加入数据集入口只调用导航回调", async () => {
  const selected = item("run-to-dataset"), onEvaluate = vi.fn();
  handler = path => path.includes("/traces?") ? json(page([selected])) : path.endsWith("/trace") ? json(detail(selected)) : json({});
  await mount(props({ onEvaluate })); await until(() => expect(host.textContent).toContain("会话 S"));
  await act(async () => rowButton("会话 S").click());
  await until(() => expect(host.textContent).toContain("node-value"));
  await act(async () => button("加入数据集").click());
  expect(onEvaluate).toHaveBeenCalledWith("local", "run-to-dataset");
  expect(calls.every(value => value.method === "GET")).toBe(true);
});

test("当前工作区只读时不显示加入数据集入口", async () => {
  const selected = item("viewer-run"), onEvaluate = vi.fn();
  const identity = { ...defaultIdentity(), workspaces: [{ workspace_id: "local", role: "viewer", approve: false }] };
  handler = path => path.includes("/traces?") ? json(page([selected])) : path.endsWith("/trace") ? json(detail(selected)) : json({});
  await mount(props({ identity, onEvaluate })); await until(() => expect(host.textContent).toContain("会话 S"));
  await act(async () => rowButton("会话 S").click());
  await until(() => expect(host.textContent).toContain("node-value"));
  expect(host.querySelector(".trace-evaluate-action")).toBeNull();
  expect(onEvaluate).not.toHaveBeenCalled();
  expect(calls.every(value => value.method === "GET")).toBe(true);
});

test("索引翻页和状态筛选绑定自己的游标范围", async () => {
  const first = item("page-1", "session-page-1"), second = item("page-2", "session-page-2"), failed = { ...item("failed-1", "session-failed"), status: "failed" as const };
  handler = path => {
    const query = new URL("http://local" + path).searchParams;
    if (query.get("status") === "failed") return json(page([failed]));
    if (query.get("cursor") === "cursor-2") return json(page([second]));
    return json(page([first], "cursor-2"));
  };
  await mount(); await until(() => expect(host.textContent).toContain("会话 session-page-1"));
  await act(async () => button("下一页").click()); await until(() => expect(host.textContent).toContain("会话 session-page-2"));
  expect(indexCalls()[1]!.path).toContain("cursor=cursor-2");
  await act(async () => button("上一页").click()); await until(() => expect(host.textContent).toContain("会话 session-page-1"));
  expect(new URL("http://local" + indexCalls()[2]!.path).searchParams.has("cursor")).toBe(false);
  await act(async () => {
    const select = host.querySelector<HTMLSelectElement>('[aria-label="按状态筛选"]')!;
    select.value = "failed"; select.dispatchEvent(new Event("change", { bubbles: true }));
  });
  await until(() => expect(host.textContent).toContain("会话 session-failed"));
  const filtered = new URL("http://local" + indexCalls()[3]!.path);
  expect(filtered.searchParams.get("status")).toBe("failed"); expect(filtered.searchParams.has("cursor")).toBe(false);
});

test("session和turn深链只请求精准索引页并自动定位详情", async () => {
  const onSelection = vi.fn(), selected = item("deep-run", "deep-session", "deep-turn");
  handler = path => path.includes("/traces?") ? json(page([selected])) : path.endsWith("/trace") ? json(detail(selected)) : json({});
  await mount(props({ sessionId: "deep-session", turnId: "deep-turn", onSelection }));
  await until(() => expect(host.textContent).toContain("node-value"));
  const query = new URL("http://local" + indexCalls()[0]!.path).searchParams;
  expect(query.get("session_id")).toBe("deep-session"); expect(query.get("turn_id")).toBe("deep-turn");
  expect(indexCalls()).toHaveLength(1); expect(onSelection).toHaveBeenCalledWith("deep-run");
  expect(calls.some(value => value.path.includes("/sessions/") && !value.path.endsWith("/trace"))).toBe(false);
});

const scopeChanges: { name: string; change: (value: React.ComponentProps<typeof TraceWorkbench>) => React.ComponentProps<typeof TraceWorkbench> }[] = [
  { name: "key", change: value => ({ ...value, apiKey: "new-key" }) },
  { name: "actor", change: value => ({ ...value, identity: actor("other", 1) }) },
  { name: "generation", change: value => ({ ...value, identity: actor("operator", 2) }) },
  { name: "workspace", change: value => ({ ...value, workspace: "other", identity: actor("operator", 1, ["local", "other"]) }) },
  { name: "active", change: value => ({ ...value, active: false }) },
];
for (const change of scopeChanges) {
  test(`scope变更后丢弃迟到的索引响应：${change.name}`, async () => {
    const late = deferred<Response>(); let count = 0;
    handler = path => {
      if (!path.includes("/traces?")) return json({});
      count++; return count === 1 ? late.promise : json(page([item(`fresh-${change.name}`, `fresh-session-${change.name}`)]));
    };
    const oldProps = props(); await mount(oldProps); await until(() => expect(indexCalls()).toHaveLength(1));
    await rerender(change.change(oldProps));
    if (change.name !== "active") await until(() => expect(host.textContent).toContain(`fresh-session-${change.name}`));
    await act(async () => { late.resolve(json(page([item(`late-${change.name}`, `late-session-${change.name}`)]))); await Promise.resolve(); });
    expect(host.textContent).not.toContain(`late-session-${change.name}`);
  });
}
for (const change of scopeChanges) {
  test(`scope变更后丢弃迟到的详情响应：${change.name}`, async () => {
    const late = deferred<Response>(); let detailCount = 0, listCount = 0;
    const oldItem = item(`old-${change.name}`, "old-session", "old-turn"), freshItem = item(`fresh-${change.name}`, "fresh-session", "fresh-turn");
    handler = path => {
      if (path.includes("/traces?")) return json(page([listCount++ === 0 ? oldItem : freshItem]));
      if (path.endsWith("/trace")) return detailCount++ === 0 ? late.promise : json(detail(freshItem, `fresh-detail-${change.name}`));
      return json({});
    };
    const oldProps = props(); await mount(oldProps); await until(() => expect(host.textContent).toContain("old-session"));
    await act(async () => rowButton("会话 old-session").click());
    await until(() => expect(calls.some(value => value.path.includes("/old-session/turns/old-turn/trace"))).toBe(true));
    await rerender(change.change(oldProps));
    if (change.name !== "active") {
      await until(() => expect(host.textContent).toContain("fresh-session"));
      await act(async () => rowButton("会话 fresh-session").click());
      await until(() => expect(host.textContent).toContain(`fresh-detail-${change.name}`));
    }
    await act(async () => { late.resolve(json(detail(oldItem, `late-detail-${change.name}`))); await Promise.resolve(); });
    expect(host.textContent).not.toContain(`late-detail-${change.name}`);
    if (change.name !== "active") expect(host.textContent).toContain(`fresh-detail-${change.name}`);
  });
}

test("刷新保留当前运行并重读同一轮详情", async () => {
  const current = item("stable-run"); let detailCount = 0;
  handler = path => path.includes("/traces?") ? json(page([current])) : path.endsWith("/trace") ? json(detail(current, `detail-${++detailCount}`)) : json({});
  await mount(); await until(() => expect(host.textContent).toContain("会话 S"));
  await act(async () => rowButton("会话 S").click()); await until(() => expect(host.textContent).toContain("detail-1"));
  await act(async () => button("刷新运行记录").click()); await until(() => expect(host.textContent).toContain("detail-2"));
  expect(host.querySelector('.trace-index-row[aria-pressed="true"]')).toBeTruthy();
  expect(calls.filter(value => value.path.endsWith("/trace"))).toHaveLength(2);
});

test("响应身份不匹配、坏游标和重复ID均拒绝", async () => {
  const selected = item("bad-detail"), mismatch = detail(selected); mismatch.trace.trace_id = "other-trace";
  handler = path => path.includes("/traces?") ? json(page([selected])) : path.endsWith("/trace") ? json(mismatch) : json({});
  await mount(); await until(() => expect(host.textContent).toContain("会话 S"));
  await act(async () => rowButton("会话 S").click()); await until(() => expect(host.textContent).toContain("Trace 身份或内容无效"));
  expect(() => parseTracePage({ items: [item("dup"), item("dup")], next_cursor: null })).toThrow("Trace 索引对象重复");
  expect(() => parseTracePage({ items: [], next_cursor: "x".repeat(4096) })).not.toThrow();
  expect(() => parseTracePage({ items: [], next_cursor: "x".repeat(4097) })).toThrow("Trace 分页响应无效");
  expect(() => parseTracePage({ items: [], next_cursor: "\u0000" })).toThrow("Trace 分页响应无效");
  expect(() => parseTracePage({ items: [{ ...item("external"), source_kind: "external_agent" }], next_cursor: null })).toThrow("Trace 索引对象无效");
  expect(() => parseTraceDetail({ ...detail(selected), turn_id: "different-turn" }, selected)).toThrow("Trace 来源不匹配");
  const wrongRun = detail(selected); wrongRun.trace.run_id = "different-run";
  expect(() => parseTraceDetail(wrongRun, selected)).toThrow("Trace 身份或内容无效");
  const repeatedSpan = detail(selected); repeatedSpan.trace.spans[1]!.span_id = "root";
  expect(() => parseTraceDetail(repeatedSpan, selected)).toThrow("Trace 节点重复");
});

test("span树保留父子关系并显示耗时", () => {
  const rows = traceSpanRows(detail(item("tree")).trace.spans);
  expect(rows.map(row => [row.span.span_id, row.depth])).toEqual([["root", 0], ["child", 1]]);
  expect(traceDuration(1, 1.25)).toBe("250 ms"); expect(traceDuration(2, 1)).toBe("—");
});
