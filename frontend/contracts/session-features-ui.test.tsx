import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { SessionFeatures } from "../src/components/SessionFeatures";
import type { SessionState } from "../src/hooks/use-session";
import type { InteractionRequest, InteractionState, PlanConfirmationRequest, UserInputRequest } from "../src/lib/interaction-contracts";
import type { PlanSnapshot, PlanStep } from "../src/lib/planning-contracts";

type Call = { path: string; method: string; body?: Record<string, unknown> };
let root: Root, host: HTMLDivElement, calls: Call[];
let handleFetch: (path: string, method: string, body?: Record<string, unknown>) => Response | Promise<Response>;
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });
function question(id: string, sessionId = "session-a"): UserInputRequest {
  return { id, kind: "user_input", status: "pending", revision: 4, workspace_id: "workspace-a", owner_id: "owner-a", session_id: sessionId, run_id: "run-a", turn_id: "turn-a", request_key: `source-${id}`, mode_revision: 5, response: null, created_at: 1, updated_at: 1, payload: { questions: [{ id: "answer", prompt: `回答 ${id}`, mode: "text", allow_text: true, options: [] }] } };
}
function confirmation(): PlanConfirmationRequest {
  return { id: "plan-confirmation", kind: "plan_confirmation", status: "pending", revision: 6, workspace_id: "workspace-a", owner_id: "owner-a", session_id: "session-a", run_id: "run-a", turn_id: "turn-a", request_key: "source-plan", mode_revision: 5, response: null, created_at: 1, updated_at: 1, payload: { plan: "先检查，再执行" } };
}
function state(pending: InteractionRequest[] = [], mode: "execute" | "plan" = "plan"): InteractionState { return { mode, revision: 5, pending }; }
function plan(sessionId = "session-a", text = "整理现有实现"): PlanSnapshot {
  return { version: "planning/1", session_id: sessionId, revision: 3, content_hash: "a".repeat(64), updated_at: 123, counts: { pending: 1, in_progress: 0, completed: 0, blocked: 0 }, total: 1, matched_total: 1, steps: [{ id: "step-one", text, status: "pending" }], offset: 0, limit: 50, next_offset: null };
}
function page(steps: PlanStep[], revision: number, hash: string, offset: number, next: number | null): PlanSnapshot {
  return { version: "planning/1", session_id: "session-a", revision, content_hash: hash, updated_at: 123, counts: { pending: 1, in_progress: 1, completed: 1, blocked: 0 }, total: 3, matched_total: 3, steps, offset, limit: 2, next_offset: next };
}
function chat(overrides: { id?: string; principalId?: string; ownerId?: string; canWrite?: boolean; status?: "open" | "closed" } = {}) {
  const id = overrides.id ?? "session-a";
  return { actor: { principal_id: overrides.principalId ?? "owner-a", generation: 2 }, id, workspace: "workspace-a", history: [], canWrite: overrides.canWrite ?? true, refresh: vi.fn(), ready: true, session: { id, owner_id: overrides.ownerId ?? "owner-a", status: overrides.status ?? "open", archived_at: null, deleted_at: null } } as unknown as SessionState;
}
async function render(value = chat(), active = true) { await act(async () => root.render(<SessionFeatures chat={value} apiKey="test-key" active={active} fence={{ current: 1 }} />)); }
async function until(check: () => void) { let error: unknown; for (let i = 0; i < 100; i++) { try { check(); return; } catch (value) { error = value; } await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); }); } throw error; }
function button(label: string, scope: ParentNode = host) { const found = [...scope.querySelectorAll<HTMLButtonElement>("button")].find(item => item.textContent?.trim() === label); expect(found, label).toBeTruthy(); return found!; }
async function fill(element: HTMLTextAreaElement, value: string) { await act(async () => { Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, "value")!.set!.call(element, value); element.dispatchEvent(new Event("input", { bubbles: true })); }); }

beforeEach(() => {
  calls = []; handleFetch = () => json({ error: { code: "not_found" } }, 404);
  host = document.createElement("div"); document.body.append(host); root = createRoot(host);
  (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => { const path = String(input), method = options.method ?? "GET"; const body = typeof options.body === "string" ? JSON.parse(options.body) as Record<string, unknown> : undefined; calls.push({ path, method, body }); return handleFetch(path, method, body); }));
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); vi.unstubAllGlobals(); vi.restoreAllMocks(); });

test("loads plan, questions, and plan confirmation; sends revisioned answer, confirm, and cancel", async () => {
  const answer = question("question-answer"), cancel = question("question-cancel"), confirmationRequest = confirmation();
  let pending: InteractionRequest[] = [answer, cancel, confirmationRequest];
  handleFetch = (path, method) => {
    if (path === "/v2/workspaces/workspace-a/sessions/session-a/plan") return json(plan());
    if (path === "/v2/workspaces/workspace-a/sessions/session-a/interactions" && method === "GET") return json(state(pending));
    const id = [answer.id, cancel.id, confirmationRequest.id].find(value => path.endsWith(`/interactions/${value}/answer`) || path.endsWith(`/interactions/${value}/cancel`));
    if (method === "POST" && id) { const item = pending.find(value => value.id === id)!; pending = pending.filter(value => value.id !== id); return json({ ...item, status: path.endsWith("/cancel") ? "cancelled" : "answered", updated_at: 2 }); }
    return json({ error: { code: "not_found" } }, 404);
  };
  const value = chat(); await render(value);
  await until(() => { expect(host.querySelector(".task-plan")).toBeTruthy(); expect(host.querySelector("[data-request-id='question-answer']")).toBeTruthy(); expect(host.querySelector("[data-request-id='question-cancel']")).toBeTruthy(); expect(host.textContent).toContain("先检查，再执行"); });
  const answerSection = host.querySelector("[data-request-id='question-answer']")!;
  await fill(answerSection.querySelector("textarea")!, "按现有方案继续");
  await act(async () => button("提交回答", answerSection).click());
  await until(() => expect(calls.some(call => call.path.endsWith("/question-answer/answer"))).toBe(true));
  const answerCall = calls.find(call => call.path.endsWith("/question-answer/answer"))!;
  expect(answerCall.path).toBe("/v2/workspaces/workspace-a/sessions/session-a/interactions/question-answer/answer"); expect(answerCall.method).toBe("POST"); expect(answerCall.body).toMatchObject({ expected_revision: 4, confirmed: null, answers: [{ question_id: "answer", option_ids: [], text: "按现有方案继续" }] }); expect(answerCall.body!.request_key).toEqual(expect.any(String));
  await until(() => expect(host.querySelector("[data-request-id='question-answer']")).toBeNull());
  await act(async () => button("按计划执行").click());
  await until(() => expect(calls.some(call => call.path.endsWith("/plan-confirmation/answer"))).toBe(true));
  const confirmCall = calls.find(call => call.path.endsWith("/plan-confirmation/answer"))!;
  expect(confirmCall.body).toMatchObject({ expected_revision: 6, answers: [], confirmed: true }); expect(confirmCall.body!.request_key).toEqual(expect.any(String));
  const cancelSection = host.querySelector("[data-request-id='question-cancel']")!;
  await act(async () => button("取消问题", cancelSection).click());
  await until(() => expect(calls.some(call => call.path.endsWith("/question-cancel/cancel"))).toBe(true));
  const cancelCall = calls.find(call => call.path.endsWith("/question-cancel/cancel"))!;
  expect(cancelCall.method).toBe("POST"); expect(cancelCall.body).toMatchObject({ expected_revision: 4, reason: "user_cancelled" }); expect(cancelCall.body!.request_key).toEqual(expect.any(String));
  expect(value.refresh).toHaveBeenCalledTimes(3);
});

test("disables answers for read-only, non-owner, and closed sessions", async () => {
  handleFetch = path => path.endsWith("/plan") ? json(plan()) : json(state([question("permission-question")], "execute"));
  await render(); await until(() => expect(host.querySelector("[data-request-id='permission-question']")).toBeTruthy());
  await render(chat({ canWrite: false })); expect(button("提交回答").disabled).toBe(true); expect(button("取消问题").disabled).toBe(true);
  await render(chat({ principalId: "another-user" })); expect(button("提交回答").disabled).toBe(true); expect(button("取消问题").disabled).toBe(true);
  await render(chat({ status: "closed" })); expect(button("提交回答").disabled).toBe(true); expect(button("取消问题").disabled).toBe(true);
  expect(calls.filter(call => call.method === "POST")).toHaveLength(0);
});

test("session switch and inactive state ignore late responses and do not leak data", async () => {
  let releaseInteractions!: (response: Response) => void, releasePlan!: (response: Response) => void;
  const oldInteractions = new Promise<Response>(resolve => { releaseInteractions = resolve; });
  const oldPlan = new Promise<Response>(resolve => { releasePlan = resolve; });
  handleFetch = path => {
    if (path.includes("/sessions/session-a/interactions")) return oldInteractions;
    if (path.includes("/sessions/session-a/plan")) return oldPlan;
    if (path.includes("/sessions/session-b/interactions")) return json(state([question("question-b", "session-b")], "execute"));
    if (path.includes("/sessions/session-b/plan")) return json(plan("session-b", "新会话计划"));
    return json({ error: { code: "not_found" } }, 404);
  };
  await render(chat({ id: "session-a" }));
  await until(() => { expect(calls.some(call => call.path.endsWith("/sessions/session-a/interactions"))).toBe(true); expect(calls.some(call => call.path.endsWith("/sessions/session-a/plan"))).toBe(true); });
  await render(chat({ id: "session-b" }), false);
  await act(async () => {
    releaseInteractions(json(state([question("question-a", "session-a")], "execute")));
    releasePlan(json(plan("session-a", "旧会话计划")));
    await Promise.all([oldInteractions, oldPlan]);
  });
  expect(host.textContent).not.toContain("question-a"); expect(host.textContent).not.toContain("旧会话计划");
  await render(chat({ id: "session-b" }), true);
  await until(() => expect(host.querySelector("[data-request-id='question-b']")).toBeTruthy());
  expect(host.textContent).toContain("新会话计划"); expect(host.textContent).not.toContain("旧会话计划");
  expect(host.querySelector("[data-request-id='question-a']")).toBeNull();
});

test("transient polling errors preserve the current question draft", async () => {
  let failing = false;
  handleFetch = path => {
    if (path.endsWith("/plan")) return json(plan());
    return failing ? json({ error: { code: "storage_unavailable" } }, 503)
      : json(state([question("draft-question")], "execute"));
  };
  await render();
  await until(() => expect(host.querySelector("[data-request-id='draft-question'] textarea")).toBeTruthy());
  const field = host.querySelector<HTMLTextAreaElement>("[data-request-id='draft-question'] textarea")!;
  await fill(field, "保留尚未提交的回答");
  failing = true;
  await act(async () => window.dispatchEvent(new Event("focus")));
  await until(() => expect(host.textContent).toContain("待答问题暂时无法读取"));
  expect(host.querySelector<HTMLTextAreaElement>("[data-request-id='draft-question'] textarea")?.value).toBe("保留尚未提交的回答");
  expect(calls.every(call => call.method === "GET")).toBe(true);
});

test("read failure is visible and retry issues fresh requests", async () => {
  let fail = true; handleFetch = path => { if (path.endsWith("/plan")) return fail ? json({ error: { code: "execution_error" } }, 503) : json(plan("session-a", "重试后计划")); return fail ? json({ error: { code: "execution_error" } }, 503) : json(state([], "execute")); };
  await render(); await until(() => expect(host.querySelector("[role='alert']")?.textContent).toContain("计划暂时无法读取"));
  fail = false; await act(async () => button("重新读取任务状态").click()); await until(() => expect(host.textContent).toContain("重试后计划"));
  expect(calls.filter(call => call.path.endsWith("/plan"))).toHaveLength(2); expect(calls.filter(call => call.path.endsWith("/interactions"))).toHaveLength(2);
});

test("merges plan pages using the original revision and content hash", async () => {
  const hash = "b".repeat(64);
  const first = page([{ id: "one", text: "第一步", status: "pending" }, { id: "two", text: "第二步", status: "in_progress" }], 7, hash, 0, 2);
  const second = page([{ id: "three", text: "第三步", status: "completed" }], 7, hash, 2, null);
  handleFetch = path => path.includes("/plan?") ? json(second) : path.endsWith("/plan") ? json(first) : json(state([], "execute"));
  await render(); await until(() => expect(host.querySelectorAll(".task-plan-step")).toHaveLength(2));
  await act(async () => button("加载更多").click()); await until(() => expect(host.querySelectorAll(".task-plan-step")).toHaveLength(3));
  expect([...host.querySelectorAll(".task-plan-step-text")].map(item => item.textContent)).toEqual(["第一步", "第二步", "第三步"]);
  expect(host.querySelector(".task-plan")?.getAttribute("data-plan-revision")).toBe("7");
  const query = new URLSearchParams(calls.find(call => call.path.includes("/plan?"))!.path.split("?")[1]);
  expect(query.get("revision")).toBe("7"); expect(query.get("offset")).toBe("2"); expect(query.get("limit")).toBe("2");
});

test.each([
  { label: "revision", revision: 8, hash: "c".repeat(64) },
  { label: "content hash", revision: 7, hash: "d".repeat(64) },
])("rejects a pagination page with a different $label", async ({ revision, hash }) => {
  const first = page([{ id: "one", text: "第一步", status: "pending" }, { id: "two", text: "第二步", status: "in_progress" }], 7, "c".repeat(64), 0, 2);
  const changed = page([{ id: "new-step", text: "新版步骤", status: "completed" }], revision, hash, 2, null);
  handleFetch = path => path.includes("/plan?") ? json(changed) : path.endsWith("/plan") ? json(first) : json(state([], "execute"));
  await render(); await until(() => expect(host.querySelectorAll(".task-plan-step")).toHaveLength(2));
  await act(async () => button("加载更多").click()); await until(() => expect(host.querySelector("[role='alert']")?.textContent).toContain("剩余计划暂时无法读取"));
  expect(host.querySelectorAll(".task-plan-step")).toHaveLength(2); expect(host.textContent).not.toContain("新版步骤");
  expect(host.querySelector(".task-plan")?.getAttribute("data-plan-revision")).toBe("7");
});

