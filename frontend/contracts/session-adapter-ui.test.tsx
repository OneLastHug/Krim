import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { useSession, type SessionState } from "../src/hooks/use-session";

let root: Root, container: HTMLDivElement, state: SessionState | null;
let handler: (path: string, options: RequestInit) => Response | Promise<Response> | undefined;
let apiKey = "synthetic-key";
const calls: { path: string; method: string; body: unknown }[] = [];
const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const base = "/v2/workspaces/local/sessions";
const session = (revision = 7) => ({ id: "A", workspace_id: "local", owner_id: "local", create_key: "created", title: "旧名称", target_key: "demo", target_fingerprint: "0".repeat(64), context_policy: { max_bytes: 131072, max_messages: 256, reserved_output_tokens: 2048, counter_version: "approx-utf8/1" }, revision, status: "open", dispatch_paused: false, created_at: 1, closed_at: null });
const turn = (id: string, status: "queued" | "running" | "completed" = "completed") => ({ id, session_id: "A", workspace_id: "local", owner_id: "local", ordinal: 1, request_key: "turn-key-" + id, text: "合成输入", accepted_revision: 1, status, run_id: "run-" + id, context_fingerprint: "1".repeat(64), transcript: null, code: null, created_at: 1, started_at: 2, finished_at: status === "running" ? null : 3 });
const fence = { current: 0 };
function Harness() {
  state = useSession(apiKey, true, fence, () => undefined);
  return <output>{String(state.ready)}</output>;
}
async function until(check: () => void) {
  let error: unknown;
  for (let index = 0; index < 80; index++) {
    try { check(); return; }
    catch (value) { error = value; await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); }); }
  }
  throw error;
}
async function mount() {
  container = document.createElement("div"); document.body.append(container); root = createRoot(container);
  (globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  await act(async () => root.render(<Harness />));
  await until(() => expect(state?.ready).toBe(true));
}

beforeEach(() => {
  localStorage.clear(); history.replaceState({}, "", "/ui/?workspace=local&session=A"); calls.length = 0; state = null; apiKey = "synthetic-key";
  handler = path => {
    if (path === "/v2/me") return json({ principal_id: "local", generation: 1, legacy: false, workspaces: [{ workspace_id: "local", role: "admin", approve: true }] });
    if (path === base + "?limit=50") return json([session()]);
    if (path === base + "/A") return json(session());
    if (path.startsWith(base + "/A/turns?")) return json([]);
    return undefined;
  };
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => {
    const path = String(input), method = options.method ?? "GET", body = options.body ? JSON.parse(String(options.body)) : null;
    calls.push({ path, method, body });
    return handler(path, options) ?? json([]);
  }));
});
afterEach(async () => {
  if (root) await act(async () => root.unmount());
  container?.remove(); vi.unstubAllGlobals(); state = null;
});

test("viewer即使approve=true也不能审批", async () => {
  handler = path => {
    if (path === "/v2/me") return json({ principal_id: "local", generation: 1, legacy: false, workspaces: [{ workspace_id: "local", role: "viewer", approve: true }] });
    if (path === base + "?limit=50") return json([session()]);
    if (path === base + "/A") return json(session());
    if (path.startsWith(base + "/A/turns?")) return json([turn("active", "running")]);
    if (path.endsWith("/events?from_seq=0")) return json([]);
    if (path.endsWith("/approvals")) return json([{ request_id: "approval", run_id: "run-active", call_id: "internal", arguments_hash: "a".repeat(64), tool_hash: "b".repeat(64), allow: true }]);
    return undefined;
  };
  await mount();
  await until(() => expect(state?.approvals).toHaveLength(1));
  expect(state?.canApprove).toBe(false);
  expect(calls.some(call => call.method === "POST")).toBe(false);
});

test("rename未知回程只按原request_key查询controls by-key", async () => {
  let losePost = true;
  const fallback = handler;
  handler = (path, options) => {
    if (path === base + "/A/rename" && options.method === "POST") {
      if (losePost) return Promise.reject(new TypeError("accepted response lost"));
      return json({ session_id: "A", request_key: (JSON.parse(String(options.body)) as { request_key: string }).request_key, status: "completed" });
    }
    if (path.startsWith(base + "/A/controls/by-key/")) return json({ session_id: "A", request_key: decodeURIComponent(path.split("/").at(-1)!), status: "completed" });
    return fallback?.(path, options);
  };
  await mount();
  await act(async () => state!.rename("新名称"));
  const post = calls.find(call => call.path === base + "/A/rename" && call.method === "POST");
  expect(post).toBeTruthy();
  expect(post!.body).toMatchObject({ title: "新名称", expected_revision: 7 });
  const ledger = JSON.parse(localStorage.getItem("krim.agent-core.controls/1")!) as Record<string, { body: { request_key: string } }>;
  const requestKey = Object.values(ledger)[0]!.body.request_key;
  expect(requestKey).toBeTruthy();
  losePost = false;
  await act(async () => state!.recoverControl(Object.values(ledger)[0] as never));
  expect(calls.filter(call => call.path === base + "/A/rename" && call.method === "POST")).toHaveLength(1);
  expect(calls.some(call => call.path === base + "/A/controls/by-key/" + encodeURIComponent(requestKey) && call.method === "GET")).toBe(true);
  expect(localStorage.getItem("krim.agent-core.controls/1")).toBe("{}");
});

test("运行期空facts随终态刷新且不重复请求", async () => {
  let status: "running" | "completed" = "running", resultReads = 0;
  handler = path => {
    if (path === "/v2/me") return json({ principal_id: "local", generation: 1, legacy: false, workspaces: [{ workspace_id: "local", role: "admin", approve: true }] });
    if (path === base + "?limit=50") return json([session()]);
    if (path === base + "/A") return json(session());
    if (path.startsWith(base + "/A/turns?")) return json([turn("active", status)]);
    if (path.endsWith("/events?from_seq=0")) return json([]);
    if (path.endsWith("/approvals")) return json([]);
    if (path.endsWith("/trace")) return json({ session_id: "A", turn_id: "active", trace: { run_id: "run-active" } });
    if (path.endsWith("/result")) { resultReads++; return json({ run: { id: "run-active" }, tools: [], events: [] }); }
    return undefined;
  };
  await mount();
  await until(() => expect(state?.history[0]?.status).toBe("running"));
  await act(async () => state!.loadDetails(state!.history[0]!));
  expect(state?.details.active?.facts).toBeNull();
  expect(state?.details.active?.loadedForStatus).toBe("running");
  status = "completed";
  await until(() => expect(state?.details.active?.facts).toMatchObject({ run: { id: "run-active" } }));
  expect(state?.details.active?.loadedForStatus).toBe("completed");
  expect(state?.details.active?.traceLoaded).toBe(false);
  await act(async () => state!.loadDetails(state!.history[0]!));
  expect(state?.details.active?.traceLoaded).toBe(true);
  expect(resultReads).toBe(1);
});

test("身份generation更新时丢弃迟到的旧历史响应", async () => {
  let generation = 1, turnReads = 0, release!: (response: Response) => void;
  const oldResponse = new Promise<Response>(resolve => { release = resolve; });
  handler = path => {
    if (path === "/v2/me") return json({ principal_id: "local", generation, legacy: false, workspaces: [{ workspace_id: "local", role: "admin", approve: true }] });
    if (path === base + "?limit=50") return json([session()]);
    if (path === base + "/A") return json(session());
    if (path === base + "/A/turns?offset=0&limit=50") {
      turnReads++;
      if (turnReads === 1) return oldResponse;
      return json([turn("new-generation")]);
    }
    return undefined;
  };
  await mount();
  await until(() => expect(turnReads).toBe(1));
  generation = 2;
  await act(async () => state!.refresh());
  await until(() => expect(turnReads).toBeGreaterThanOrEqual(2));
  await until(() => expect(state?.history.map(item => item.id)).toEqual(["new-generation"]));
  await act(async () => { release(json([turn("late-generation-1")])); await Promise.resolve(); });
  expect(state?.history.map(item => item.id)).toEqual(["new-generation"]);
});

test.each(["events", "result"] as const)("终态%s读取失败不阻断下一轮，并可显式补读", async side => {
  let phase = 0, fail = true, failedReads = 0;
  const fallback = handler;
  handler = (path, options) => {
    if (path.startsWith(base + "/A/turns?")) return json([
      turn("first", phase === 0 ? "running" : "completed"),
      { ...turn("second", phase === 0 ? "queued" : phase === 1 ? "running" : "completed"), ordinal: 2 },
    ]);
    if (path.includes("/first/events?")) {
      const cursor = Number(new URL("http://local" + path).searchParams.get("from_seq"));
      if (cursor === 0) return json([{ run_id: "run-first", seq: 0, event_id: "text-first", kind: "text", data: { step_index: 0, text: "已接收的部分" }, at: 1 }]);
      if (cursor === 1) {
        if (fail && side === "events") { failedReads++; return json({ error: { code: "storage_unavailable" } }, 503); }
        return json([{ run_id: "run-first", seq: 1, event_id: "terminal-first", kind: "terminal", data: { status: "completed" }, at: 2 }]);
      }
      return json([]);
    }
    if (path.endsWith("/first/result") && fail && side === "result") { failedReads++; return json({ error: { code: "storage_unavailable" } }, 503); }
    if (path.endsWith("/result")) return json({ run: { id: "run-" + path.split("/").at(-2) }, tools: [] });
    if (path.endsWith("/trace")) { const id = path.split("/").at(-2)!; return json({ session_id: "A", turn_id: id, trace: { run_id: "run-" + id, complete: true } }); }
    if (path.includes("/events?") || path.endsWith("/approvals")) return json([]);
    return fallback(path, options);
  };
  await mount();
  await until(() => expect(state?.turnEvents.first).toHaveLength(1));
  phase = 1;
  await act(async () => { await new Promise(resolve => setTimeout(resolve, 350)); });
  await until(() => expect(state?.history[1]?.status).toBe("running"));
  expect(state?.turnEvents.first?.[0]?.kind).toBe("text");
  expect(state?.turnFacts.first).toBeUndefined();
  phase = 2;
  await act(async () => { await new Promise(resolve => setTimeout(resolve, 350)); });
  await until(() => expect(state?.turnFacts.second).toMatchObject({ run: { id: "run-second" } }));
  expect(failedReads).toBe(1);
  expect(calls.some(value => value.method === "POST")).toBe(false);
  fail = false;
  await act(async () => state!.loadDetails(state!.history[0]!));
  expect(state?.turnEvents.first).toHaveLength(2);
  expect(state?.turnFacts.first).toMatchObject({ run: { id: "run-first" } });
  expect(state?.details.first?.traceLoaded).toBe(true);
});
