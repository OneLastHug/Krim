import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { useSubagents } from "../src/hooks/use-subagents";
import { parseSubagentDetail, parseSubagentPage, subagentState, type SubagentMember } from "../src/lib/subagent-contracts";
import type { SessionState } from "../src/hooks/use-session";

let root: Root, host: HTMLDivElement, latest: ReturnType<typeof useSubagents>;
type Body = Record<string, unknown>;
let route: (url: URL, method: string, body?: Body) => Response | Promise<Response>;
let calls: { url: URL; method: string; body?: Body }[];
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });
const member = (id = "a", status: NonNullable<SubagentMember["task"]>["status"] = "running"): SubagentMember => ({ agent_id: id, tree_id: "tree", path: id, role: "agent", status: "active", depth: 1, profile: "worker", provider: "sdk_session", workdir: null, revision: 1, current_task_id: `task-${id}`, parent_path: "lead", tree_status: "open", route_key: null, activation: { activation_id: `activation-${id}`, instance: 1, status: "resident" }, task: { task_id: `task-${id}`, status, run_id: `run-${id}`, ordinal: 1, input: { text: "check" } } });
const approval = { request_id: "approval-a", run_id: "run-a", call_id: "call-a", name: "shell", arguments: { command: "check" }, arguments_hash: "a".repeat(64), tool_hash: "b".repeat(64), expires_at: null };
const detail = (id = "a", status: NonNullable<SubagentMember["task"]>["status"] = "running") => ({ agent: member(id, status), messages: [], results: [], events: [{ run_id: `run-${id}`, seq: 0, event_id: `event-${id}`, kind: "run_started", data: {}, at: 1 }], approvals: status === "awaiting_approval" ? [approval] : [], run: { id: `run-${id}`, request_key: "key", status: "running", trace_id: "trace", root_span_id: "span", created_at: 1, finished_at: null, code: null, stop_requested: false }, run_id: `run-${id}`, events_truncated: false });
const page = (members = [member()], next: string | null = null) => ({ subagents: members, next_after_path: next });
const chat = (id = "session-a", running = true) => ({ ready: true, actor: { principal_id: "owner", generation: 1 }, workspace: "ws", id, session: { id, status: "open", deleted_at: null }, history: running ? [{ status: "running" }] : [] }) as unknown as SessionState;
const fence = { current: 1 };
function Harness({ id = "session-a", running = true }: { id?: string; running?: boolean }) { latest = useSubagents({ chat: chat(id, running), apiKey: "fixture", active: true, fence }); return null; }
async function render(id = "session-a", running = true) { await act(async () => root.render(<Harness id={id} running={running}/>)); }
async function until(check: () => void) { let last: unknown; for (let i = 0; i < 100; i++) { try { check(); return; } catch (error) { last = error; } await act(async () => { await new Promise(resolve => setTimeout(resolve, 2)); }); } throw last; }
async function focus() { await act(async () => { window.dispatchEvent(new Event("focus")); }); }
beforeEach(() => { calls = []; fence.current = 1; host = document.createElement("div"); document.body.append(host); root = createRoot(host); (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true; route = url => url.pathname.endsWith("/subagents") ? json(page()) : json(detail(url.pathname.split("/").at(-1))); vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => { const url = new URL(String(input), "http://localhost"), method = options.method ?? "GET", body = options.body ? JSON.parse(String(options.body)) as Body : undefined; calls.push({ url, method, body }); return route(url, method, body); })); });
afterEach(async () => { await act(async () => root.unmount()); host.remove(); vi.unstubAllGlobals(); vi.restoreAllMocks(); });

test("strict contracts reject unknown task states, duplicate pages, and cross-run details", () => {
  expect(() => parseSubagentPage(page([member(), member()]))).toThrow();
  expect(() => parseSubagentPage(page([member()], "wrong"))).toThrow();
  const bad = detail(); bad.events[0]!.run_id = "other"; expect(() => parseSubagentDetail(bad, "a")).toThrow();
  expect(() => parseSubagentDetail({ ...detail(), run: null }, "a")).toThrow();
  const badState = member(); (badState.task as { status: string }).status = "missing"; expect(() => parseSubagentPage(page([badState]))).toThrow();
  expect(subagentState(member("a", "awaiting_approval"))).toBe("approval");
});

test("loads paginated members, hides root, and refreshes read pages without duplication", async () => {
  route = url => url.searchParams.has("after_path") ? json(page([member("b")])) : json(page([member(), { ...member("0-lead"), role: "root" as const, depth: 0 }].sort((a, b) => a.path.localeCompare(b.path)), "a"));
  await render(); await until(() => expect(latest.members.map(m => m.agent_id)).toEqual(["a"]));
  await act(async () => { await latest.loadMore(); });
  await until(() => expect(latest.members.map(m => m.agent_id)).toEqual(["a", "b"]));
  await focus(); await until(() => expect(calls.filter(c => c.url.searchParams.get("after_path") === "a").length).toBeGreaterThanOrEqual(2));
  expect(latest.members).toHaveLength(2);
});

test("selecting a different agent immediately loads details and rejects late previous response", async () => {
  let release!: (response: Response) => void; const gate = new Promise<Response>(resolve => { release = resolve; });
  route = url => url.pathname.endsWith("/subagents") ? json(page([member(), member("b")])) : url.pathname.endsWith("/a") ? gate : json(detail("b"));
  await render(); await until(() => expect(latest.members).toHaveLength(2));
  await act(async () => latest.select("a")); await until(() => expect(calls.some(c => c.url.pathname.endsWith("/a"))).toBe(true));
  await act(async () => latest.select("b")); await until(() => expect(latest.detail?.agent.agent_id).toBe("b"));
  await act(async () => { release(json(detail())); await gate; }); expect(latest.detail?.agent.agent_id).toBe("b");
});

test("new session loads while old read is pending and never adopts the old response", async () => {
  let release!: (response: Response) => void; const gate = new Promise<Response>(resolve => { release = resolve; });
  route = url => url.pathname.includes("session-a") ? gate : json(page([member("b")]));
  await render(); await until(() => expect(calls).toHaveLength(1));
  await render("session-b"); await until(() => expect(latest.members[0]?.agent_id).toBe("b"));
  await act(async () => { release(json(page())); await gate; }); expect(latest.members.map(m => m.agent_id)).toEqual(["b"]);
});

test("network or 503 send retries use the same request key and successful response clears unknown receipt", async () => {
  let posts = 0;
  route = (url, method) => method === "POST" ? ++posts === 1 ? json({ error: { code: "storage_unavailable" } }, 503) : json({ agent_id: "a", message_id: "m", seq: 2, accepted: true, started: false, delivery: "queued" }) : url.pathname.endsWith("/subagents") ? json(page()) : json(detail());
  await render(); await until(() => expect(latest.members).toHaveLength(1)); await act(async () => latest.select("a")); await until(() => expect(latest.detail).not.toBeNull());
  let accepted = true; await act(async () => { accepted = await latest.send("followup", "queue_only"); }); expect(accepted).toBe(false); expect(latest.receipt).toContain("未确认");
  await act(async () => { accepted = await latest.send("followup", "queue_only"); }); expect(accepted).toBe(true);
  const writes = calls.filter(c => c.method === "POST"); expect(writes[0]!.body?.request_key).toBe(writes[1]!.body?.request_key); expect(latest.receipt).toContain("已发送");
});

test("control captures current activation/task and rejects a mismatched agent receipt", async () => {
  route = (url, method) => method === "POST" ? json({ agent_id: "other", interrupted: true }) : url.pathname.endsWith("/subagents") ? json(page()) : json(detail());
  await render(); await until(() => expect(latest.members).toHaveLength(1)); await act(async () => latest.select("a")); await until(() => expect(latest.detail).not.toBeNull());
  let ok = true; await act(async () => { ok = await latest.control("interrupt"); }); expect(ok).toBe(false);
  expect(calls.find(c => c.method === "POST")!.body).toEqual({ activation_id: "activation-a", task_id: "task-a" }); expect(latest.selectedId).toBe("a");
});

test("pending approvals appear without selecting agent and remain available after selection", async () => {
  route = url => url.pathname.endsWith("/subagents") ? json(page([member("a", "awaiting_approval")])) : json(detail("a", "awaiting_approval"));
  await render(); await until(() => expect(latest.approvalItems).toHaveLength(1));
  await act(async () => latest.select("a")); await until(() => expect(latest.detail).not.toBeNull()); expect(latest.approvalItems[0]!.approval.request_id).toBe("approval-a");
});

test("idle root can discover background tasks on refresh; unavailable capability stays quiet", async () => {
  let failing = false; route = () => failing ? json({ error: { code: "subagent_capability" } }, 409) : json(page());
  await render("session-a", false); await until(() => expect(latest.members).toHaveLength(1));
  await focus(); await until(() => expect(calls.length).toBeGreaterThan(1));
  failing = true; await act(async () => latest.reload()); await until(() => expect(latest.available).toBe(false)); expect(latest.error).toBe("");
});
