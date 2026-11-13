import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { act, createElement } from "react";
import { createRoot, type Root } from "react-dom/client";
import { SessionList } from "../src/components/SessionList";
import { WorkbenchLayout } from "../src/components/WorkbenchLayout";
import { parseSession, type Session } from "../src/lib/session-contracts";
import { useSession, type SessionState } from "../src/hooks/use-session";

let root: Root, container: HTMLDivElement;
(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
function setup(chat: SessionState, more = false) {
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
  const children = createElement(WorkbenchLayout, {
    view: "chat", identity: chat.actor, identityLoading: false, identityError: "",
    theme: "warm", onThemeSelect: vi.fn(), readingSize: 14, onReadingSizeSelect: vi.fn(),
    chatTitle: "设计讨论", chat, httpEnabled: false, onNavigate: vi.fn(),
    onConnect: vi.fn(), onOpenTask: vi.fn(), canOpenTask: false, actions: null,
    inspector: null, onObservabilityPage: vi.fn(), observabilityPage: "context",
    children: createElement("div"),
  });
  act(() => root.render(more ? children : createElement(SessionList, { chat })));
  return container;
}
afterEach(() => { act(() => root?.unmount()); container?.remove(); vi.unstubAllGlobals(); });

function state(overrides: Partial<SessionState> = {}): SessionState {
  const session = {
    id: "s1", workspace_id: "w1", owner_id: "p1", create_key: "c1", title: "设计讨论",
    target_key: "demo", target_fingerprint: "a".repeat(64),
    context_policy: { max_bytes: 1024, max_messages: 32, reserved_output_tokens: 256, counter_version: "approx-utf8/1" },
    revision: 2, status: "closed" as const, dispatch_paused: true, created_at: 1, closed_at: 2,
    archived_at: null, deleted_at: null, lifecycle_revision: 0,
  };
  return {
    actor: { principal_id: "p1", generation: 1, legacy: false, workspaces: [{ workspace_id: "w1", role: "admin", approve: true }] },
    workspace: "w1", id: "s1", list: [session], listView: "active", session,
    lifecycle: vi.fn(), setSessionListView: vi.fn(), ready: true, canWrite: true, canApprove: true,
    pendingControls: [], busy: false, controlling: false,
    history: [], details: {}, turnEvents: {}, turnFacts: {}, approvals: [], events: [], currentTurn: null,
    draft: "", notice: null, reading: false,
    pending: [], turnPending: undefined, newPending: undefined, sending: false, creating: false,
    posting: () => false, edit: vi.fn(), create: vi.fn(), submit: vi.fn(), recover: vi.fn(),
    recoverControl: vi.fn(), control: vi.fn(), rename: vi.fn(), loadDetails: vi.fn(), select: vi.fn(), refresh: vi.fn(),
    ...overrides,
  } as SessionState;
}

function rowLabels(row: Element) {
  return [...row.querySelectorAll("button")].map(button => button.getAttribute("aria-label"));
}

test("closed session row exposes archive and trash, while the filter changes scope", () => {
  const chat = state();
  const view = setup(chat);
  const filter = view.querySelector<HTMLSelectElement>('select[aria-label="对话列表范围"]')!;
  expect(filter.value).toBe("active");
  act(() => { filter.value = "archived"; filter.dispatchEvent(new Event("change", { bubbles: true })); });
  expect(chat.setSessionListView).toHaveBeenCalledWith("archived");
  const actions = [...view.querySelectorAll<HTMLButtonElement>("button")].map(button => button.getAttribute("aria-label"));
  expect(actions).toContain("归档此对话");
  expect(actions).toContain("移入回收站");
});

test("trash row provides restore and hides move-to-trash action, even when still archived", () => {
  const chat = state({ listView: "trash", list: [{ ...state().session!, archived_at: 5, deleted_at: 3 }] });
  const view = setup(chat);
  const actions = [...view.querySelectorAll<HTMLButtonElement>("button")].map(button => button.getAttribute("aria-label"));
  expect(actions).toContain("恢复此对话");
  expect(actions).not.toContain("移入回收站");
  expect(actions).not.toContain("取消归档");
});

test("row actions follow each row's own state, not the selected session", () => {
  const closed = state().session!;
  const open = { ...closed, id: "s2", title: "进行中讨论", status: "open" as const, closed_at: null };
  const chat = state({ list: [closed, open] });
  const view = setup(chat);
  const rows = [...view.querySelectorAll(".chat-session-row")];
  expect(rows).toHaveLength(2);
  expect(rowLabels(rows[0]!)).toContain("归档此对话");
  expect(rowLabels(rows[0]!)).toContain("移入回收站");
  expect(rowLabels(rows[1]!)).not.toContain("归档此对话");
  expect(rowLabels(rows[1]!)).not.toContain("移入回收站");
  act(() => rows[0]!.querySelector<HTMLButtonElement>('button[aria-label="归档此对话"]')!.click());
  expect(chat.lifecycle).toHaveBeenCalledWith("archive", "s1");
});

test("an open selected session no longer hides actions of closed rows", () => {
  const closed = state().session!;
  const open = { ...closed, id: "s2", title: "进行中讨论", status: "open" as const, closed_at: null };
  const chat = state({ id: "s2", session: open, list: [closed, open] });
  const view = setup(chat);
  const rows = [...view.querySelectorAll(".chat-session-row")];
  expect(rowLabels(rows[0]!)).toContain("归档此对话");
  expect(rowLabels(rows[0]!)).toContain("移入回收站");
  expect(rowLabels(rows[1]!)).not.toContain("归档此对话");
  expect(rowLabels(rows[1]!)).not.toContain("移入回收站");
});

test("busy flights and read-only identity hide row lifecycle actions", () => {
  const busy = setup(state({ busy: true }));
  expect([...busy.querySelectorAll("button")].map(button => button.getAttribute("aria-label"))).not.toContain("归档此对话");
});

test("read-only identity hides row lifecycle actions", () => {
  const view = setup(state({ canWrite: false }));
  const actions = [...view.querySelectorAll("button")].map(button => button.getAttribute("aria-label"));
  expect(actions).not.toContain("归档此对话");
  expect(actions).not.toContain("移入回收站");
});

test("foreign-owner closed session hides lifecycle row and menu actions for admins", () => {
  const foreign = { ...state().session!, owner_id: "someone-else" };
  const view = setup(state({ list: [foreign], session: foreign }));
  const actions = [...view.querySelectorAll("button")].map(button => button.getAttribute("aria-label"));
  expect(actions).not.toContain("归档此对话");
  expect(actions).not.toContain("移入回收站");
  act(() => root.unmount());
  view.remove();
  const menu = setup(state({ list: [foreign], session: foreign }), true);
  act(() => menu.querySelector<HTMLButtonElement>('button[aria-label="更多操作"]')!.click());
  const items = [...document.querySelectorAll<HTMLElement>('[role="menuitem"]')].map(item => item.textContent);
  expect(items).not.toContain("归档对话");
  expect(items).not.toContain("移入回收站");
});

test("more menu only adds lifecycle actions for a closed writable session", () => {
  const chat = state();
  const view = setup(chat, true);
  const more = view.querySelector<HTMLButtonElement>('button[aria-label="更多操作"]')!;
  act(() => more.click());
  const items = [...document.querySelectorAll<HTMLElement>('[role="menuitem"]')].map(item => item.textContent);
  expect(items).toContain("归档对话");
  expect(items).toContain("移入回收站");
});

test("archived selected session offers unarchive beside move-to-trash", () => {
  const archived = { ...state().session!, archived_at: 5 };
  const view = setup(state({ listView: "archived", list: [archived], session: archived }), true);
  act(() => view.querySelector<HTMLButtonElement>('button[aria-label="更多操作"]')!.click());
  const items = [...document.querySelectorAll<HTMLElement>('[role="menuitem"]')].map(item => item.textContent);
  expect(items).toContain("取消归档对话");
  expect(items).toContain("移入回收站");
  expect(items).not.toContain("归档对话");
  expect(items).not.toContain("恢复对话");
});

test("trashed selected session only offers restore, whatever its archive flag", () => {
  const trashed = { ...state().session!, archived_at: 5, deleted_at: 6 };
  const view = setup(state({ listView: "trash", list: [trashed], session: trashed }), true);
  act(() => view.querySelector<HTMLButtonElement>('button[aria-label="更多操作"]')!.click());
  const items = [...document.querySelectorAll<HTMLElement>('[role="menuitem"]')].map(item => item.textContent);
  expect(items).toContain("恢复对话");
  expect(items).not.toContain("移入回收站");
  expect(items).not.toContain("取消归档对话");
  expect(items).not.toContain("归档对话");
});

test("parseSession accepts a session that is archived and trashed at once", () => {
  const parsed = parseSession({
    id: "s1", workspace_id: "w1", owner_id: "p1", create_key: "c1", title: "双状态",
    target_key: "demo", target_fingerprint: "a".repeat(64),
    context_policy: { max_bytes: 1024, max_messages: 32, reserved_output_tokens: 256, counter_version: "approx-utf8/1" },
    revision: 2, status: "closed", dispatch_paused: true, created_at: 1, closed_at: 2,
    archived_at: 5, deleted_at: 6, lifecycle_revision: 3,
  });
  expect(parsed.archived_at).toBe(5);
  expect(parsed.deleted_at).toBe(6);
  expect(parsed.lifecycle_revision).toBe(3);
});

const CONTROL = "krim.agent-core.controls/1";
const hookBase = "/v2/workspaces/local/sessions";
let hookState: SessionState | null;
const hookCalls: { path: string; method: string; body: unknown }[] = [];
const fence = { current: 0 };
const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const identity = (role = "admin", generation = 1) => json({ principal_id: "local", generation, legacy: false, workspaces: [{ workspace_id: "local", role, approve: true }] });
const lifecycleSession = (over: Partial<Session> = {}): Session => ({
  id: "A", workspace_id: "local", owner_id: "local", create_key: "created", title: "归档候选",
  target_key: "demo", target_fingerprint: "0".repeat(64),
  context_policy: { max_bytes: 131072, max_messages: 256, reserved_output_tokens: 2048, counter_version: "approx-utf8/1" },
  revision: 7, status: "closed", dispatch_paused: true, created_at: 1, closed_at: 2,
  archived_at: null, deleted_at: null, lifecycle_revision: 0, ...over,
});
function serve(handler: (path: string, method: string, body: unknown) => Response | Promise<Response> | undefined) {
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => {
    const path = String(input), method = options.method ?? "GET", body = options.body ? JSON.parse(String(options.body)) : null;
    hookCalls.push({ path, method, body });
    return handler(path, method, body) ?? json({ error: { code: "not_found" } }, 404);
  }));
}
async function until(check: () => void) {
  let error: unknown;
  for (let index = 0; index < 120; index++) {
    try { check(); return; }
    catch (value) { error = value; await act(async () => { await new Promise(resolve => setTimeout(resolve, 10)); }); }
  }
  throw error;
}
function HookHarness() {
  hookState = useSession("synthetic-key", true, fence, () => undefined);
  return <output>{String(hookState.ready)}</output>;
}
async function mountHook() {
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
  await act(async () => root.render(<HookHarness />));
  await until(() => expect(hookState?.ready).toBe(true));
}

beforeEach(() => {
  localStorage.clear();
  history.replaceState({}, "", "/ui/?workspace=local&session=A");
  hookCalls.length = 0;
  hookState = null;
});

test("丢失响应后再次操作先查lifecycle回执，命中即不再重复提交", async () => {
  let losePost = true, applied = false;
  const postedKeys: string[] = [], receiptGets: string[] = [];
  serve((path, _method, body) => {
    if (path === "/v2/me") return identity();
    if (path === hookBase + "?limit=50") return json(applied ? [] : [lifecycleSession()]);
    if (path === hookBase + "/A") return json(applied ? lifecycleSession({ archived_at: 5, lifecycle_revision: 1 }) : lifecycleSession());
    if (path.startsWith(hookBase + "/A/turns?")) return json([]);
    if (path.startsWith(hookBase + "/A/lifecycle/by-key/")) {
      receiptGets.push(decodeURIComponent(path.split("/by-key/")[1]!));
      applied = true;
      return json(lifecycleSession({ archived_at: 5, lifecycle_revision: 1 }));
    }
    if (path === hookBase + "/A/lifecycle/archive") {
      postedKeys.push((body as { request_key: string }).request_key);
      if (losePost) return Promise.reject(new TypeError("accepted response lost"));
      applied = true;
      return json(lifecycleSession({ archived_at: 5, lifecycle_revision: 1 }));
    }
    return undefined;
  });
  await mountHook();
  await until(() => expect(hookState?.list).toHaveLength(1));
  await act(async () => hookState!.lifecycle("archive", "A"));
  await until(() => expect(postedKeys).toHaveLength(1));
  const ledger = JSON.parse(localStorage.getItem(CONTROL)!) as Record<string, { body: { request_key: string } }>;
  const entries = Object.values(ledger);
  expect(entries).toHaveLength(1);
  const originalKey = entries[0]!.body.request_key;
  expect(originalKey).toBe(postedKeys[0]);
  await act(async () => hookState!.lifecycle("archive", "A"));
  await until(() => expect(receiptGets).toHaveLength(1));
  expect(receiptGets[0]).toBe(originalKey);
  expect(postedKeys).toHaveLength(1);
  await until(() => expect(localStorage.getItem(CONTROL)).toBe("{}"));
  await until(() => expect(hookState?.session?.archived_at).toBe(5));
  await until(() => expect(hookState?.list).toHaveLength(0));
  expect(hookCalls.some(call => call.path.includes("/controls/by-key/"))).toBe(false);
});

test("回执404证明未执行，保留原键重新提交", async () => {
  let losePost = true, applied = false, missed = false;
  const postedKeys: string[] = [], receiptGets: string[] = [];
  serve((path, _method, body) => {
    if (path === "/v2/me") return identity();
    if (path === hookBase + "?limit=50") return json(applied ? [] : [lifecycleSession()]);
    if (path === hookBase + "/A") return json(applied ? lifecycleSession({ archived_at: 5, lifecycle_revision: 1 }) : lifecycleSession());
    if (path.startsWith(hookBase + "/A/turns?")) return json([]);
    if (path.startsWith(hookBase + "/A/lifecycle/by-key/")) {
      receiptGets.push(decodeURIComponent(path.split("/by-key/")[1]!));
      if (missed) { applied = true; return json(lifecycleSession({ archived_at: 5, lifecycle_revision: 1 })); }
      missed = true;
      return json({ error: { code: "not_found" } }, 404);
    }
    if (path === hookBase + "/A/lifecycle/archive") {
      postedKeys.push((body as { request_key: string }).request_key);
      if (losePost) { losePost = false; return Promise.reject(new TypeError("accepted response lost")); }
      applied = true;
      return json(lifecycleSession({ archived_at: 5, lifecycle_revision: 1 }));
    }
    return undefined;
  });
  await mountHook();
  await until(() => expect(hookState?.list).toHaveLength(1));
  await act(async () => hookState!.lifecycle("archive", "A"));
  await until(() => expect(postedKeys).toHaveLength(1));
  const originalKey = Object.values(JSON.parse(localStorage.getItem(CONTROL)!) as Record<string, { body: { request_key: string } }>)[0]!.body.request_key;
  await act(async () => hookState!.lifecycle("archive", "A"));
  await until(() => expect(receiptGets).toHaveLength(1));
  await until(() => expect(postedKeys).toHaveLength(2));
  expect(receiptGets[0]).toBe(originalKey);
  expect(postedKeys[1]).toBe(originalKey);
  expect(new Set(postedKeys).size).toBe(1);
  await until(() => expect(localStorage.getItem(CONTROL)).toBe("{}"));
  await until(() => expect(hookState?.session?.archived_at).toBe(5));
});

test("重挂载后保留的未确认lifecycle先查回执恢复", async () => {
  let losePost = true, applied = false;
  const postedKeys: string[] = [], receiptGets: string[] = [];
  serve((path, _method, body) => {
    if (path === "/v2/me") return identity();
    if (path === hookBase + "?limit=50") return json(applied ? [] : [lifecycleSession()]);
    if (path === hookBase + "/A") return json(applied ? lifecycleSession({ archived_at: 5, lifecycle_revision: 1 }) : lifecycleSession());
    if (path.startsWith(hookBase + "/A/turns?")) return json([]);
    if (path.startsWith(hookBase + "/A/lifecycle/by-key/")) {
      receiptGets.push(decodeURIComponent(path.split("/by-key/")[1]!));
      applied = true;
      return json(lifecycleSession({ archived_at: 5, lifecycle_revision: 1 }));
    }
    if (path === hookBase + "/A/lifecycle/archive") {
      postedKeys.push((body as { request_key: string }).request_key);
      if (losePost) return Promise.reject(new TypeError("accepted response lost"));
      applied = true;
      return json(lifecycleSession({ archived_at: 5, lifecycle_revision: 1 }));
    }
    return undefined;
  });
  await mountHook();
  await until(() => expect(hookState?.list).toHaveLength(1));
  await act(async () => hookState!.lifecycle("archive", "A"));
  await until(() => expect(postedKeys).toHaveLength(1));
  const originalKey = Object.values(JSON.parse(localStorage.getItem(CONTROL)!) as Record<string, { body: { request_key: string } }>)[0]!.body.request_key;
  await act(async () => root.unmount());
  container.remove();
  await mountHook();
  await until(() => expect(hookState?.pendingControls).toHaveLength(1));
  expect(hookState?.pendingControls[0]?.body.request_key).toBe(originalKey);
  await act(async () => hookState!.lifecycle("archive", "A"));
  await until(() => expect(receiptGets).toHaveLength(1));
  expect(receiptGets[0]).toBe(originalKey);
  expect(postedKeys).toHaveLength(1);
  await until(() => expect(localStorage.getItem(CONTROL)).toBe("{}"));
  await until(() => expect(hookState?.pendingControls).toHaveLength(0));
});

test("recoverControl将lifecycle未确认请求路由到回执GET并解析SessionView", async () => {
  let losePost = true, applied = false;
  const postedKeys: string[] = [], receiptGets: string[] = [];
  serve((path, _method, body) => {
    if (path === "/v2/me") return identity();
    if (path === hookBase + "?limit=50") return json(applied ? [] : [lifecycleSession()]);
    if (path === hookBase + "/A") return json(applied ? lifecycleSession({ archived_at: 5, lifecycle_revision: 1 }) : lifecycleSession());
    if (path.startsWith(hookBase + "/A/turns?")) return json([]);
    if (path.startsWith(hookBase + "/A/lifecycle/by-key/")) {
      receiptGets.push(decodeURIComponent(path.split("/by-key/")[1]!));
      applied = true;
      return json(lifecycleSession({ archived_at: 5, lifecycle_revision: 1 }));
    }
    if (path === hookBase + "/A/lifecycle/archive") {
      postedKeys.push((body as { request_key: string }).request_key);
      if (losePost) return Promise.reject(new TypeError("accepted response lost"));
      applied = true;
      return json(lifecycleSession({ archived_at: 5, lifecycle_revision: 1 }));
    }
    return undefined;
  });
  await mountHook();
  await until(() => expect(hookState?.list).toHaveLength(1));
  await act(async () => hookState!.lifecycle("archive", "A"));
  await until(() => expect(postedKeys).toHaveLength(1));
  const originalKey = Object.values(JSON.parse(localStorage.getItem(CONTROL)!) as Record<string, { body: { request_key: string } }>)[0]!.body.request_key;
  await act(async () => hookState!.recoverControl(hookState!.pendingControls[0]!));
  await until(() => expect(receiptGets).toEqual([originalKey]));
  expect(postedKeys).toHaveLength(1);
  await until(() => expect(localStorage.getItem(CONTROL)).toBe("{}"));
  await until(() => expect(hookState?.pendingControls).toHaveLength(0));
});

test("recoverControl查询404时保留原键等待安全重试", async () => {
  let miss = true;
  serve((path, _method) => {
    if (path === "/v2/me") return identity();
    if (path === hookBase + "?limit=50") return json([lifecycleSession()]);
    if (path === hookBase + "/A") return json(lifecycleSession());
    if (path.startsWith(hookBase + "/A/turns?")) return json([]);
    if (path.startsWith(hookBase + "/A/lifecycle/by-key/")) return miss ? json({ error: { code: "not_found" } }, 404) : json(lifecycleSession({ archived_at: 5, lifecycle_revision: 1 }));
    if (path === hookBase + "/A/lifecycle/archive") return Promise.reject(new TypeError("accepted response lost"));
    return undefined;
  });
  await mountHook();
  await until(() => expect(hookState?.list).toHaveLength(1));
  await act(async () => hookState!.lifecycle("archive", "A"));
  await until(() => expect(hookState?.pendingControls).toHaveLength(1));
  const intent = hookState!.pendingControls[0]!;
  await act(async () => hookState!.recoverControl(intent));
  await until(() => expect(hookState?.notice).toBe("未找到记录，请检查身份与对话"));
  expect(localStorage.getItem(CONTROL)).not.toBe("{}");
  expect(Object.values(JSON.parse(localStorage.getItem(CONTROL)!) as Record<string, { body: { request_key: string } }>)[0]!.body.request_key).toBe(intent.body.request_key);
  expect(hookCalls.filter(call => call.method === "POST")).toHaveLength(1);
  miss = false;
  await act(async () => hookState!.lifecycle("archive", "A"));
  await until(() => expect(localStorage.getItem(CONTROL)).toBe("{}"));
  expect(hookCalls.filter(call => call.method === "POST")).toHaveLength(1);
});

test("迟到的成功在身份更替后不落地，保留待确认可恢复", async () => {
  let generation = 1, applied = false;
  let release!: (response: Response) => void;
  const gate = new Promise<Response>(resolve => { release = resolve; });
  serve(path => {
    if (path === "/v2/me") return identity("admin", generation);
    if (path === hookBase + "?limit=50") return json(applied ? [] : [lifecycleSession()]);
    if (path === hookBase + "/A") return json(applied ? lifecycleSession({ archived_at: 5, lifecycle_revision: 1 }) : lifecycleSession());
    if (path.startsWith(hookBase + "/A/turns?")) return json([]);
    if (path.startsWith(hookBase + "/A/lifecycle/by-key/")) { applied = true; return json(lifecycleSession({ archived_at: 5, lifecycle_revision: 1 })); }
    if (path === hookBase + "/A/lifecycle/archive") return gate;
    return undefined;
  });
  await mountHook();
  await until(() => expect(hookState?.list).toHaveLength(1));
  let pending: Promise<void> = Promise.resolve();
  await act(async () => { pending = hookState!.lifecycle("archive", "A"); });
  generation = 2;
  await act(async () => hookState!.refresh());
  await until(() => expect(hookState?.actor?.generation).toBe(2));
  await act(async () => { release(json(lifecycleSession({ archived_at: 5, lifecycle_revision: 1 }))); await pending; });
  expect(hookState?.notice).toBeNull();
  expect(hookState?.session?.archived_at).toBeNull();
  expect(localStorage.getItem(CONTROL)).not.toBe("{}");
  await act(async () => hookState!.recoverControl(hookState!.pendingControls[0]!));
  await until(() => expect(localStorage.getItem(CONTROL)).toBe("{}"));
  await until(() => expect(hookState?.session?.archived_at).toBe(5));
});

test("lifecycle飞行计入controlling，迟到的失败跨工作区不写提示", async () => {
  let release!: (response: Response) => void;
  const gate = new Promise<Response>(resolve => { release = resolve; });
  serve(path => {
    if (path === "/v2/me") return json({ principal_id: "local", generation: 1, legacy: false, workspaces: [{ workspace_id: "local", role: "admin", approve: true }, { workspace_id: "other", role: "admin", approve: true }] });
    if (path === "/v2/workspaces/other/sessions?limit=50") return json([]);
    if (path === hookBase + "?limit=50") return json([lifecycleSession()]);
    if (path === hookBase + "/A") return json(lifecycleSession());
    if (path.startsWith(hookBase + "/A/turns?")) return json([]);
    if (path === hookBase + "/A/lifecycle/archive") return gate;
    return undefined;
  });
  await mountHook();
  await until(() => expect(hookState?.session?.id).toBe("A"));
  let pending: Promise<void> = Promise.resolve();
  await act(async () => { pending = hookState!.lifecycle("archive", "A"); });
  await until(() => expect(hookState?.controlling).toBe(true));
  await act(async () => hookState!.select("other", ""));
  await until(() => expect(hookState?.workspace).toBe("other"));
  await act(async () => { release(json({ error: { code: "storage_unavailable" } }, 503)); await pending; });
  expect(hookState?.notice).toBeNull();
  expect(localStorage.getItem(CONTROL)).not.toBe("{}");
  await until(() => expect(hookState?.controlling).toBe(false));
});

test("lifecycle修订冲突后清除未确认请求，刷新后按新修订换新键重试", async () => {
  let conflicts = true, rowRevision = 0, archived = false;
  const posted: { request_key: string; expected_revision: number }[] = [];
  serve((path, _method, body) => {
    if (path === "/v2/me") return identity();
    if (path === hookBase + "?limit=50") return json(archived ? [] : [lifecycleSession({ lifecycle_revision: rowRevision })]);
    if (path === hookBase + "/A") return json(archived ? lifecycleSession({ archived_at: 5, lifecycle_revision: rowRevision + 1 }) : lifecycleSession({ lifecycle_revision: rowRevision }));
    if (path.startsWith(hookBase + "/A/turns?")) return json([]);
    if (path === hookBase + "/A/lifecycle/archive") {
      posted.push(body as { request_key: string; expected_revision: number });
      if (conflicts) return json({ error: { code: "revision_conflict" } }, 409);
      archived = true;
      return json(lifecycleSession({ archived_at: 5, lifecycle_revision: rowRevision + 1 }));
    }
    return undefined;
  });
  await mountHook();
  await until(() => expect(hookState?.list[0]?.lifecycle_revision).toBe(0));
  await act(async () => hookState!.lifecycle("archive", "A"));
  await until(() => expect(localStorage.getItem(CONTROL)).toBe("{}"));
  await until(() => expect(hookState?.notice).toBe("对话已更新，请刷新检查原请求"));
  rowRevision = 1;
  conflicts = false;
  await act(async () => hookState!.refresh());
  await until(() => expect(hookState?.list[0]?.lifecycle_revision).toBe(1));
  await act(async () => hookState!.lifecycle("archive", "A"));
  await until(() => expect(posted).toHaveLength(2));
  expect(posted[1]!.request_key).not.toBe(posted[0]!.request_key);
  expect(posted[1]!.expected_revision).toBe(1);
  await until(() => expect(localStorage.getItem(CONTROL)).toBe("{}"));
  await until(() => expect(hookState?.session?.archived_at).toBe(5));
  await until(() => expect(hookState?.notice).toBeNull());
});

test("回收站选中的会话可恢复回正常列表，移入回收站后取消选中", async () => {
  let trashed = false;
  const restoreRevisions: number[] = [];
  serve((path, _method, body) => {
    if (path === "/v2/me") return identity();
    const row = trashed ? lifecycleSession({ archived_at: 5, deleted_at: 6, lifecycle_revision: 1 }) : lifecycleSession();
    if (path === hookBase + "?limit=50") return json(trashed ? [] : [lifecycleSession()]);
    if (path === hookBase + "/lifecycle/trash?limit=50") return json(trashed ? [row] : []);
    if (path === hookBase + "/lifecycle/archived?limit=50") return json([]);
    if (path === hookBase + "/A") return json(row);
    if (path.startsWith(hookBase + "/A/turns?")) return json([]);
    if (path === hookBase + "/A/lifecycle/trash") {
      trashed = true;
      return json(lifecycleSession({ deleted_at: 6, lifecycle_revision: 1 }));
    }
    if (path === hookBase + "/A/lifecycle/restore") {
      restoreRevisions.push((body as { expected_revision: number }).expected_revision);
      trashed = false;
      return json(lifecycleSession());
    }
    return undefined;
  });
  await mountHook();
  await until(() => expect(hookState?.session?.id).toBe("A"));
  await act(async () => hookState!.lifecycle("trash", "A"));
  await until(() => expect(hookState?.id).toBe(""));
  expect(new URL(location.href).searchParams.get("session")).toBeNull();
  await until(() => expect(hookState?.list).toHaveLength(0));
  await act(async () => hookState!.setSessionListView("trash"));
  await until(() => expect(hookState?.list[0]?.deleted_at).toBe(6));
  expect(hookState?.list[0]?.archived_at).toBe(5);
  await act(async () => hookState!.select("local", "A"));
  await until(() => expect(hookState?.session?.id).toBe("A"));
  await act(async () => hookState!.lifecycle("restore", "A"));
  await until(() => expect(hookState?.listView).toBe("active"));
  await until(() => expect(hookState?.list[0]?.deleted_at).toBeNull());
  await until(() => expect(localStorage.getItem(CONTROL)).toBe("{}"));
  expect(restoreRevisions).toEqual([1]);
});

test("viewer身份不触发lifecycle请求", async () => {
  serve(path => {
    if (path === "/v2/me") return identity("viewer");
    if (path === hookBase + "?limit=50") return json([lifecycleSession()]);
    if (path === hookBase + "/A") return json(lifecycleSession());
    if (path.startsWith(hookBase + "/A/turns?")) return json([]);
    return undefined;
  });
  await mountHook();
  await until(() => expect(hookState?.canWrite).toBe(false));
  await act(async () => hookState!.lifecycle("archive", "A"));
  expect(hookCalls.filter(call => call.method === "POST")).toHaveLength(0);
  expect(localStorage.getItem(CONTROL)).toBeNull();
});

test("非本人的已关闭会话即使admin也不触发生命周期请求", async () => {
  serve(path => {
    if (path === "/v2/me") return identity();
    if (path === hookBase + "?limit=50") return json([lifecycleSession({ owner_id: "someone-else" })]);
    if (path === hookBase + "/A") return json(lifecycleSession({ owner_id: "someone-else" }));
    if (path.startsWith(hookBase + "/A/turns?")) return json([]);
    return undefined;
  });
  await mountHook();
  await until(() => expect(hookState?.list).toHaveLength(1));
  await act(async () => hookState!.lifecycle("archive", "A"));
  expect(hookCalls.filter(call => call.method === "POST")).toHaveLength(0);
  expect(localStorage.getItem(CONTROL)).toBeNull();
});
