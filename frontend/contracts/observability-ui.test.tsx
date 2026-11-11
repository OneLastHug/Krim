import { act, StrictMode, useState } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import type { Actor } from "../src/lib/session-contracts";
import { ObservabilityModule } from "../src/observability";
import type { ObsPage } from "../src/observability/obs-route";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

let host: HTMLDivElement;
let root: Root;
let page: ObsPage;
const calls: { path: string; method: string; signal?: AbortSignal }[] = [];
const actor: Actor = { principal_id: "operator", generation: 1, legacy: false, workspaces: [{ workspace_id: "team", role: "member", approve: false }] };
const json = (value: unknown) => new Response(JSON.stringify(value), { status: 200, headers: { "Content-Type": "application/json" } });
const usage = { input_tokens: 10, output_tokens: 2, total_tokens: 12, cached_input_tokens: null, cache_write_input_tokens: null, reasoning_output_tokens: null };

function listItem(sessionId: string, turnId: string, runId: string) {
  return {
    source_kind: "session_turn", run_id: runId, trace_id: `trace-${runId}`, session_id: sessionId, turn_id: turnId,
    session_title: `会话 ${sessionId}`, target_key: "demo", status: "completed", created_at: 100, finished_at: 101,
    projection_status: "ok", projection_code: null, duration_ms: 195.13273239135742, duration_source: "mono",
    model_requests: 1, tool_calls: 0, usage, usage_complete: true, usage_steps: 1, steps_missing_usage: 0,
    compaction_passes: 0, manifest_requests: 1, manifest_status: "present", cost: null, cost_status: "unpriced",
  };
}
function detail(sessionId: string, turnId: string, runId: string) {
  return {
    session_id: sessionId,
    turn_id: turnId,
    run: { run_id: runId, trace_id: `trace-${runId}`, request_key: `request-${runId}`, status: "completed", code: null, created_at: 100, finished_at: 101, duration_ms: 1000, duration_source: "wall_clock" },
    trace: { run_id: runId, trace_id: `trace-${runId}`, complete: true, reasons: [], spans: [] },
    configuration: null,
    requests: [{
      id: `span-${runId}`, step_index: 0, purpose: "task", status: "unknown", started_at: 100.125, ended_at: null,
      duration_ms: null, duration_source: "wall_clock", timing: null, tool_calls: 0,
      usage: { usage, usage_reports: 1, usage_final: true },
      manifest: {
        version: 1, counter_version: "approx-utf8/1", input_bytes: 10, estimated_tokens: 3,
        items: [{ key: `message:0:user:${runId}`, category: "history", label: "User message", content_hash: "a".repeat(64), input_bytes: 10, estimated_tokens: 3, message_index: 0, tool_index: null, call_id: null, source_refs: null, source_refs_truncated: false }],
        model_context_window: null, items_truncated: false, omitted_items: 0, source_refs_truncated: false, reserved_output_tokens: null, input_budget_bytes: null,
      },
      manifest_status: "present", cost: null, cost_status: "unpriced",
    }],
    tools: [], compactions: [],
    coverage: { passes: 0, complete_passes: 0, covered_turns: null, history_turns: null, first_covered_turn_id: null, last_covered_turn_id: null, summary_step_index: null, manifest_status: "present", steps_with_usage: 1, steps_missing_usage: 0 },
    usage: { totals: usage, usage_complete: true, steps_with_usage: 1, steps_missing_usage: 0 },
    cost: null, cost_status: "unpriced",
  };
}

function listResponse() {
  return { scope: { version: 1, workspace_id: "team", role: "member", status: null, session_id: null, limit: 20, counted: "returned_page" }, items: [listItem("session-b", "turn-b", "run-b"), listItem("session-a", "turn-a", "run-a")], next_cursor: null };
}
function Harness() {
  const [currentPage, setCurrentPage] = useState<ObsPage>(page);
  return <div className="workbench-page">
    <button type="button" onClick={() => { page = "runs"; setCurrentPage("runs"); }}>Show runs</button>
    <button type="button" onClick={() => { page = "context"; setCurrentPage("context"); }}>Show context</button>
    <ObservabilityModule apiKey="secret" workspace="team" identity={actor} identityLoading={false} theme="bright" page={currentPage} onPageChange={next => { page = next; setCurrentPage(next); }} onNavigate={() => {}}/>
  </div>;
}
function render() {
  return act(async () => root.render(<StrictMode><Harness/></StrictMode>));
}
async function until(check: () => void) {
  let last: unknown;
  for (let index = 0; index < 80; index++) {
    try { check(); return; } catch (error) { last = error; }
    await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); });
  }
  throw last;
}
async function selectSession(value: string) {
  await act(async () => {
    const select = host.querySelector<HTMLSelectElement>('[aria-label="选择会话"]')!;
    Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, "value")!.set!.call(select, value);
    select.dispatchEvent(new Event("change", { bubbles: true }));
  });
}

beforeEach(() => {
  page = "context";
  calls.length = 0;
  history.replaceState(null, "", "/ui/?view=observe&workspace=team");
  host = document.createElement("div");
  document.body.append(host);
  root = createRoot(host);
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => {
    const path = String(input);
    calls.push({ path, method: options.method ?? "GET", signal: options.signal ?? undefined });
    if (path.startsWith("/v2/workspaces/team/observations?")) return json(listResponse());
    const match = path.match(/\/sessions\/([^/]+)\/turns\/([^/]+)\/observations$/);
    if (match) {
      const sid = decodeURIComponent(match[1]!);
      const tid = decodeURIComponent(match[2]!);
      const run = tid === "turn-b" ? "run-b" : tid === "turn-a" ? "run-a" : `run-${tid}`;
      return json(detail(sid, tid, run));
    }
    return json({ error: { code: "not_found" } });
  }));
});
afterEach(async () => {
  await act(async () => root.unmount());
  host.remove();
  vi.unstubAllGlobals();
});

test("默认上下文自动选择有授权的真实轮次，且会话选择器可切换当前有界页中的会话", async () => {
  await render();
  await until(() => expect(host.querySelector('[aria-label="选择模型请求"]')).toBeTruthy());
  expect(host.querySelector<HTMLSelectElement>('[aria-label="选择会话"]')?.value).toBe("session-b");
  expect(calls.some(call => call.path.includes("session_id="))).toBe(false);
  await selectSession("session-a");
  await until(() => expect(calls.some(call => call.path.includes("/sessions/session-a/turns/turn-a/observations"))).toBe(true));
  expect(host.querySelector<HTMLSelectElement>('[aria-label="选择会话"]')?.value).toBe("session-a");
  expect(new URL(location.href).searchParams.get("obs_turn")).toBe("turn-a");
  expect(calls.every(call => call.method === "GET")).toBe(true);
});

test("深链轮次不在当前列表页时仍直接读取授权详情并保留选择", async () => {
  history.replaceState(null, "", "/ui/?view=observe&workspace=team&obs_session=session-old&obs_turn=turn-old");
  await render();
  await until(() => expect(host.querySelector('[aria-label="选择模型请求"]')).toBeTruthy());
  expect(calls.some(call => call.path.includes("/sessions/session-old/turns/turn-old/observations"))).toBe(true);
  expect(host.querySelector<HTMLSelectElement>('[aria-label="选择会话"]')?.value).toBe("session-old");
  expect(new URL(location.href).searchParams.get("obs_turn")).toBe("turn-old");
});

test("离开模块后重挂载，在详情到达使内容变高之后恢复滚动", async () => {
  host.classList.add("workbench-page");
  let top = 0;
  Object.defineProperty(host, "scrollTop", {
    configurable: true,
    get: () => top,
    set: value => { top = host.querySelector('[aria-label="选择模型请求"]') ? value : 0; },
  });
  const mountModule = () => act(async () => root.render(
    <ObservabilityModule apiKey="secret" workspace="team" identity={actor} identityLoading={false}
      theme="bright" page="context" onPageChange={() => {}} onNavigate={() => {}}/>
  ));
  await mountModule();
  await until(() => expect(host.querySelector('[aria-label="选择模型请求"]')).toBeTruthy());
  host.scrollTop = 214;
  await act(async () => host.dispatchEvent(new Event("scroll")));
  await act(async () => root.render(null));
  host.scrollTop = 0;
  await mountModule();
  await until(() => expect(host.querySelector('[aria-label="选择模型请求"]')).toBeTruthy());
  await until(() => expect(host.scrollTop).toBe(214));
});

test("切换页并返回时恢复宿主主内容区滚动位置", async () => {
  await render();
  await until(() => expect(host.querySelector('[aria-label="选择模型请求"]')).toBeTruthy());
  const workbenchPage = host.querySelector<HTMLElement>(".workbench-page")!;
  workbenchPage.scrollTop = 126;
  await act(async () => workbenchPage.dispatchEvent(new Event("scroll")));
  await act(async () => host.querySelector<HTMLButtonElement>("button")!.click());
  await until(() => expect(host.querySelector(".obs-run-row")).toBeTruthy());
  await act(async () => host.querySelector<HTMLButtonElement>(".obs-run-row")!.click());
  await until(() => expect(host.textContent).toContain("执行时间线"));
  await act(async () => host.querySelectorAll<HTMLButtonElement>("button")[1]!.click());
  await until(() => expect(host.querySelector('[aria-label="选择模型请求"]')).toBeTruthy());
  expect(workbenchPage.scrollTop).toBe(126);
});
