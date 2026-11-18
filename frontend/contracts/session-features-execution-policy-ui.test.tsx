import { act, useRef } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { useExecutionPolicy } from "../src/hooks/use-execution-policy";
import { useSession } from "../src/hooks/use-session";
import { ChatHistory } from "../src/components/ChatHistory";
import { SessionExecutionControl } from "../src/components/SessionExecutionControl";
import { ToolCallCard } from "../src/components/ToolCallCard";
import { parseSession } from "../src/lib/session-contracts";
import type { ExecutionPolicy } from "../src/lib/execution-policy";
import type { ToolPresentation } from "../src/lib/session-presentation";

type Call = { path: string; method: string; body?: Record<string, unknown> };
let root: Root, host: HTMLDivElement, calls: Call[];
let handle: (path: string, method: string, body?: Record<string, unknown>) => Response | Promise<Response>;
const json = (v: unknown, status = 200) => new Response(JSON.stringify(v), { status, headers: { "Content-Type": "application/json" } });
function policy(sid = "session-a", mode: ExecutionPolicy["mode"] = "manual", revision = 1): ExecutionPolicy {
  return { session_id: sid, mode, revision, updated_at: 1, updated_by: "owner", project_id: "project-" + sid,
    project_access: "write", project_access_revision: 1 };
}
function Harness({ sid = "session-a" }: { sid?: string }) {
  const fence = useRef(1);
  const execution = useExecutionPolicy({ apiKey: "synthetic-permission-test", principalId: "owner", generation: 1,
    workspaceId: "workspace", sessionId: sid, enabled: true, running: false, sessionRevision: 0, fence }, () => {});
  return <div><SessionExecutionControl execution={execution} canChange />
    <button onClick={() => void execution.openProject()}>打开项目详情</button></div>;
}
async function render(sid = "session-a") { await act(async () => root.render(<Harness sid={sid} />)); }
async function until(check: () => void) {
  let last: unknown;
  for (let i = 0; i < 100; i++) { try { check(); return; } catch (error) { last = error; }
    await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); }); }
  throw last;
}
function button(label: string, parent: ParentNode = document) {
  const found = [...parent.querySelectorAll<HTMLButtonElement>("button")].find(b => b.textContent?.trim() === label);
  expect(found, label).toBeTruthy(); return found!;
}
async function click(label: string) { await act(async () => button(label).click()); }
async function openMenu() { await act(async () => host.querySelector<HTMLButtonElement>('[aria-haspopup="menu"]')!.click()); }
function router(initial = policy()) {
  let value = initial;
  handle = (path, method, body) => {
    if (path.endsWith("/allow-rules")) return json({ session_id: value.session_id, rule_count: 0 });
    if (method === "PUT") value = { ...value, mode: body!.mode as ExecutionPolicy["mode"], revision: value.revision + 1 };
    return json(value);
  };
  return { get: () => value, set: (next: ExecutionPolicy) => { value = next; } };
}
beforeEach(() => {
  calls = []; handle = () => json({ error: { code: "not_found" } }, 404);
  host = document.createElement("div"); document.body.append(host); root = createRoot(host);
  (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => {
    const path = String(input), method = options.method ?? "GET";
    const body = typeof options.body === "string" ? JSON.parse(options.body) as Record<string, unknown> : undefined;
    calls.push({ path, method, body }); return handle(path, method, body);
  }));
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); vi.unstubAllGlobals(); vi.restoreAllMocks(); });

test("accepts the token-window response and keeps legacy byte policies readable", () => {
  const session = { id: "s", workspace_id: "w", owner_id: "o", create_key: "k", title: "t", target_key: "demo",
    target_fingerprint: "a".repeat(64), revision: 0, status: "open", dispatch_paused: false, created_at: 1, closed_at: null,
    context_policy: { max_bytes: null, max_messages: null, reserved_output_tokens: 2048,
      counter_version: "token-window/1", max_tokens: 1000000, compaction_trigger_tokens: 900000 } };
  expect(parseSession(session).context_policy.max_tokens).toBe(1000000);
  expect(parseSession({ ...session, context_policy: { max_bytes: 131072, max_messages: null,
    reserved_output_tokens: 512, counter_version: "approx-utf8/1" } }).context_policy.max_bytes).toBe(131072);
  expect(() => parseSession({ ...session, context_policy: { ...session.context_policy, max_tokens: 100 } })).toThrow();
});

test("hot-switches with CAS without displaying the desired mode before the server commits", async () => {
  let current = policy(); let finish!: (r: Response) => void;
  handle = (path, method) => path.endsWith("/allow-rules") ? json({ session_id: "session-a", rule_count: 0 })
    : method === "PUT" ? new Promise(resolve => { finish = resolve; }) : json(current);
  await render(); await until(() => expect(host.textContent).toContain("手动确认")); await openMenu();
  await click("免确认"); expect(host.querySelector('[aria-haspopup="menu"]')?.textContent).toContain("手动确认");
  expect(document.body.textContent).toContain("正在保存");
  const sent = calls.find(c => c.method === "PUT")!;
  expect(sent.body).toMatchObject({ mode: "bypass", expected_revision: 1 }); expect(sent.body!.request_key).toEqual(expect.any(String));
  current = policy("session-a", "bypass", 2); await act(async () => finish(json(current)));
  await until(() => expect(host.querySelector('[aria-haspopup="menu"]')?.textContent).toContain("免确认"));
});

test("a lost write response recovers server state rather than blindly reposting", async () => {
  let current = policy();
  handle = (path, method) => {
    if (path.endsWith("/allow-rules")) return json({ session_id: "session-a", rule_count: 0 });
    if (method === "PUT") { current = policy("session-a", "bypass", 2); throw new TypeError("synthetic network failure"); }
    return json(current);
  };
  await render(); await until(() => expect(host.textContent).toContain("手动确认")); await openMenu(); await click("免确认");
  await until(() => expect(host.querySelector('[aria-haspopup="menu"]')?.textContent).toContain("免确认"));
  expect(calls.filter(c => c.method === "PUT")).toHaveLength(1);
  expect(document.body.textContent).toContain("已从服务端恢复当前执行模式");
});

test("a revision conflict displays the actual newer mode", async () => {
  let current = policy();
  handle = (path, method) => {
    if (path.endsWith("/allow-rules")) return json({ session_id: "session-a", rule_count: 0 });
    if (method === "PUT") { current = policy("session-a", "auto", 3); return json({ error: { code: "revision_conflict" } }, 409); }
    return json(current);
  };
  await render(); await until(() => expect(host.textContent).toContain("手动确认")); await openMenu(); await click("免确认");
  await until(() => expect(host.querySelector('[aria-haspopup="menu"]')?.textContent).toContain("自动判断"));
  expect(document.body.textContent).toContain("模式已在其他位置更改");
});

test("read-only stays explicit even when the session selects bypass", async () => {
  router({ ...policy(), project_access: "read" }); await render();
  await until(() => expect(host.textContent).toContain("只读")); await openMenu(); await click("免确认");
  await until(() => expect(host.querySelector('[aria-haspopup="menu"]')?.textContent).toContain("只读 · 免确认"));
  await openMenu(); expect(document.body.textContent).toContain("不能修改文件或运行 Shell");
});

test("a late receipt from another session never changes the current session", async () => {
  let finish!: (r: Response) => void;
  handle = (path, method) => {
    const sid = path.includes("session-b") ? "session-b" : "session-a";
    if (path.endsWith("/allow-rules")) return json({ session_id: sid, rule_count: 0 });
    if (method === "PUT") return new Promise(resolve => { finish = resolve; });
    return json(policy(sid, sid === "session-b" ? "auto" : "manual"));
  };
  await render(); await until(() => expect(host.textContent).toContain("手动确认")); await openMenu(); await click("免确认");
  await render("session-b"); await until(() => expect(host.textContent).toContain("自动判断"));
  await act(async () => finish(json(policy("session-a", "bypass", 2))));
  expect(host.querySelector('[aria-haspopup="menu"]')?.textContent).toContain("自动判断");
});

test("clearing retries the same key and GET prevents an old response from hiding newly saved rules", async () => {
  let count = 1, deletes = 0;
  handle = (path, method) => {
    if (method === "DELETE") { deletes++; if (deletes === 1) throw new TypeError("synthetic lost response");
      count = 2; return json({ session_id: "session-a", cleared_count: 1, rule_count: 0 }); }
    if (path.endsWith("/allow-rules")) return json({ session_id: "session-a", rule_count: count });
    return json(policy());
  };
  await render(); await until(() => expect(host.textContent).toContain("手动确认")); await openMenu();
  await click("清除本会话允许规则"); await until(() => expect(document.body.textContent).toContain("清理结果未确认"));
  await click("重试原清理请求"); await until(() => expect(document.body.textContent).toContain("已保存 2 条允许规则"));
  const sent = calls.filter(c => c.method === "DELETE"); expect(sent).toHaveLength(2);
  expect(sent[0]!.body!.request_key).toBe(sent[1]!.body!.request_key);
});

test("project details use real project ownership and render inside the actual dialog portal", async () => {
  handle = path => path.endsWith("/allow-rules") ? json({ session_id: "session-a", rule_count: 0 }) : path.endsWith("/project")
    ? json({ session_id: "session-a", project_id: "project-session-a", title: "包含较长名称的真实项目详情", owner_id: "actual-project-owner", project_access: "read", is_creator: false, sandbox_status: "unprovisioned" })
    : json({ ...policy(), project_access: "read" });
  await render(); await until(() => expect(host.textContent).toContain("手动确认")); await click("打开项目详情");
  await until(() => expect(document.querySelector('[role="dialog"]')?.textContent).toContain("actual-project-owner"));
  expect(document.querySelector('[role="dialog"]')?.textContent).toContain("未准备");
  await click("关闭"); await until(() => expect(document.querySelector('[role="dialog"]')).toBeNull());
});

test("session allowance is explicit, resets per request, and is never sent for refusal", async () => {
  const decision = vi.fn();
  const tool = { type: "tool", key: "t", name: "shell", state: "approval", arguments: {}, source: null, risk: "write",
    approval: { request_id: "request-a", run_id: "run", call_id: "call", name: "shell", arguments: {},
      tool_hash: "a".repeat(64), arguments_hash: "b".repeat(64), expires_at: null } } as unknown as ToolPresentation;
  const show = async (value = tool) => { await act(async () => root.render(<ToolCallCard tool={value} canApprove canAllowSession busy={false} pending={false} onDecision={decision} onInspect={() => {}} />)); };
  await show(); expect(host.querySelector('[role="timer"]')).toBeNull();
  await act(async () => { const select = host.querySelector("select")!; select.value = "session"; select.dispatchEvent(new Event("change", { bubbles: true })); });
  await click("允许本会话相同调用"); expect(decision).toHaveBeenLastCalledWith(true, "session");
  await click("拒绝"); expect(decision).toHaveBeenLastCalledWith(false);
  await show({ ...tool, approval: { ...tool.approval!, request_id: "request-b" } });
  expect(host.querySelector("select")?.value).toBe("once");
});

test("a quickly completed paged turn is rehydrated after a permission-triggered refresh", async () => {
  const original = location.href;
  window.history.replaceState(null, "", "?workspace=workspace&session=session-a"); localStorage.clear();
  const session = { id: "session-a", workspace_id: "workspace", owner_id: "owner", create_key: "seed", title: "快速完成",
    target_key: "demo", target_fingerprint: "a".repeat(64), revision: 1, status: "open", dispatch_paused: false,
    created_at: 1, closed_at: null, context_policy: { max_bytes: null, max_messages: null, reserved_output_tokens: 2048,
      counter_version: "token-window/1", max_tokens: 1000000, compaction_trigger_tokens: 900000 } };
  const turn = { id: "turn", session_id: "session-a", workspace_id: "workspace", owner_id: "owner", ordinal: 0,
    request_key: "turn", text: "操作", accepted_revision: 0, status: "completed", run_id: "run", context_fingerprint: null,
    transcript: [{ role: "user", content: "操作", call_id: null, tool_calls: [] }], code: null, created_at: 1, started_at: 1, finished_at: 2 };
  const events = [
    { run_id: "run", seq: 0, event_id: "start", kind: "model_start", data: { step_index: 0, span_id: "span" } },
    { run_id: "run", seq: 1, event_id: "text", kind: "text", data: { step_index: 0, text: "快速完成后的回答" } },
    { run_id: "run", seq: 2, event_id: "end", kind: "model_end", data: { step_index: 0, span_id: "span", tool_calls: [] } },
  ];
  handle = path => {
    if (path === "/v2/me") return json({ principal_id: "owner", generation: 1, legacy: false,
      workspaces: [{ workspace_id: "workspace", role: "admin", approve: true }], target_keys: ["demo"] });
    if (path.endsWith("/allow-rules")) return json({ session_id: "session-a", rule_count: 0 });
    if (path.endsWith("/execution-policy")) return json(policy());
    if (path.includes("/session-sidebar")) return json({ groups: [], sessions: [], revision: 0 });
    if (path.includes("/events?")) return json(events);
    if (path.endsWith("/result")) return json({ run: { id: "run", status: "completed" }, tools: [], text: "快速完成后的回答" });
    if (path.includes("/turns?")) return json([turn]);
    if (path.endsWith("/session-a")) return json(session);
    if (path.includes("/sessions?")) return json([session]);
    return json({ error: { code: "not_found" } }, 404);
  };
  function HistoryHarness() {
    const fence = useRef(1);
    const chat = useSession("synthetic-hydrate-test", true, fence, () => {});
    return <ChatHistory chat={chat} />;
  }
  try {
    await act(async () => root.render(<HistoryHarness />));
    await until(() => expect(host.textContent).toContain("快速完成后的回答"));
    expect(calls.some(call => call.path.endsWith("/result"))).toBe(true);
  } finally { window.history.replaceState(null, "", original); }
});
