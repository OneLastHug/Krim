import { act, createElement } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { SessionList } from "../src/components/SessionList";
import type { SessionState } from "../src/hooks/use-session";
import type { Session } from "../src/lib/session-contracts";
import type { SidebarLayout } from "../src/lib/session-sidebar";
(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
let root: Root, host: HTMLDivElement, persisted: SidebarLayout, puts: unknown[], chat: SessionState;
let bindings: Record<string, string | null>;
let sidebarReads = 0, projectPosts: { title: string; workspace_id: string; request_key?: string }[] = [];
const response = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });
const row = (id: string): Session => ({ id, title: id, workspace_id: "local", owner_id: "owner", create_key: id, target_key: "demo", target_fingerprint: "a".repeat(64), context_policy: { max_bytes: 1000, max_messages: 10, reserved_output_tokens: 10, counter_version: "approx-utf8/1" }, revision: 1, status: "open", dispatch_paused: false, created_at: id === "one" ? 1 : 2, closed_at: null, archived_at: null, deleted_at: null, lifecycle_revision: 0 });
const project = (id: string, title: string) => ({ project_id: id, environment_id: id + "-env", principal_id: "owner", workspace_id: "local", title, generation: 1, status: "ready", created_at: 1, updated_at: 1 });
beforeEach(() => {
  puts = []; projectPosts = []; sidebarReads = 0;
  persisted = { revision: 0, groups: [{ id: "p1", title: "项目一" }], sessions: ["one", "two"].map(id => ({ session_id: id, group_id: "p1", parent_id: null, pinned: false })) };
  bindings = { two: "p1", one: "p1" };
  chat = { actor: { principal_id: "owner", generation: 1, legacy: true, workspaces: [{ workspace_id: "local", role: "admin", approve: true }] }, workspace: "local", id: "one", list: [row("two"), row("one")], session: row("one"), listView: "active", history: [], approvals: [], pendingControls: [], ready: true, canWrite: true, busy: false, controlling: false, notice: null, select: vi.fn(), setSessionListView: vi.fn(), lifecycle: vi.fn(), rename: vi.fn(), recoverControl: vi.fn(), refresh: vi.fn() } as unknown as SessionState;
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => {
    const path = String(input), method = options.method ?? "GET", body: unknown = options.body ? JSON.parse(String(options.body)) : null;
    if (path.endsWith("/session-sidebar")) {
      if (method === "GET") sidebarReads++;
      if (method === "PUT") { const payload = body as { expected_revision: number; groups: SidebarLayout["groups"]; sessions: SidebarLayout["sessions"] }; puts.push(payload); if (payload.expected_revision !== persisted.revision) return response({ error: { code: "sidebar_revision_conflict" } }, 409); persisted = { revision: persisted.revision + 1, groups: payload.groups, sessions: payload.sessions }; }
      return response(persisted);
    }
    const bindingMatch = path.match(/^\/v2\/me\/sandbox\/sessions\/([^/]+)\/project$/);
    if (bindingMatch) {
      const sessionId = decodeURIComponent(bindingMatch[1]!);
      if (method === "PUT") { const payload = body as { project_id: string | null }; bindings[sessionId] = payload.project_id; return response({ session_id: sessionId, project_id: payload.project_id, revision: 4, session_revision: 2, workspace_id: "local" }); }
      return response({ session_id: sessionId, project_id: bindings[sessionId] ?? null, revision: 3, session_revision: 1, workspace_id: "local" });
    }
    if (path === "/v2/me/sandbox/projects" && method === "POST") { const payload = body as { title: string; workspace_id: string }; projectPosts.push(payload); return response(project("p2", payload.title), 201); }
    return response(persisted);
  }));
  host = document.createElement("div"); document.body.append(host); root = createRoot(host);
});
afterEach(() => { act(() => root.unmount()); host.remove(); vi.unstubAllGlobals(); });
async function render() { await act(async () => { root.render(createElement(SessionList, { chat, apiKey: "test-only" })); }); }
async function settle() { for (let index = 0; index < 40; index++) { await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); }); } }
const ids = () => [...host.querySelectorAll<HTMLElement>("[data-session-row]")].map(element => element.dataset.sessionRow);
function pointer(target: EventTarget, type: string, y: number, pointerType = "mouse") {
  const event = new Event(type, { bubbles: true, cancelable: true });
  for (const [name, value] of Object.entries({ pointerType, pointerId: 1, isPrimary: true, button: 0, buttons: type === "pointerup" ? 0 : 1, clientX: 20, clientY: y })) Object.defineProperty(event, name, { value });
  target.dispatchEvent(event); return event;
}
function geometry(target = "two") {
  const element = host.querySelector<HTMLElement>(`[data-session-row="${target}"]`)!;
  vi.stubGlobal("document", document);
  Object.defineProperty(document, "elementFromPoint", { configurable: true, value: () => element });
  element.getBoundingClientRect = () => ({ top: 50, height: 30, bottom: 80, left: 0, right: 200, width: 200, x: 0, y: 50, toJSON: () => ({}) });
}
async function drag(after = true) {
  geometry();
  await act(async () => {
    pointer(host.querySelector('[data-session-row="one"] .session-select')!, "pointerdown", 10);
    pointer(document, "pointermove", after ? 76 : 55); pointer(document, "pointerup", after ? 76 : 55);
  });
}
test("primary drag follows DOM pointer path, persists, retains scroll, suppresses trailing click", async () => {
  await render(); const nav = host.querySelector<HTMLElement>(".session-tree")!; nav.scrollTop = 120; act(() => nav.dispatchEvent(new Event("scroll")));
  await drag(); expect(puts).toHaveLength(1); expect(ids()).toEqual(["two", "one"]); expect(nav.scrollTop).toBe(120);
  act(() => host.querySelector<HTMLButtonElement>('[data-session-row="one"] .session-select')!.click()); expect(chat.select).not.toHaveBeenCalled();
  act(() => { const button = host.querySelector<HTMLButtonElement>('[data-session-row="one"] .session-select')!; pointer(button, "pointerdown", 10); pointer(document, "pointerup", 10); button.click(); });
  expect(chat.select).toHaveBeenCalledWith("local", "one");
  act(() => root.unmount()); root = createRoot(host); await render(); expect(ids()).toEqual(["two", "one"]);
});
test("Escape cancels an active drag and never writes", async () => {
  await render(); geometry();
  await act(async () => { pointer(host.querySelector('[data-session-row="one"] .session-select')!, "pointerdown", 10); pointer(document, "pointermove", 76); document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true })); pointer(document, "pointerup", 76); });
  expect(puts).toHaveLength(0); expect(ids()).toEqual(["one", "two"]);
});
test("touch remains a native scroll path and never starts sorting", async () => {
  await render(); geometry();
  await act(async () => { const down = pointer(host.querySelector('[data-session-row="one"] .session-select')!, "pointerdown", 10, "touch"); const move = pointer(document, "pointermove", 76, "touch"); pointer(document, "pointerup", 76, "touch"); expect(down.defaultPrevented).toBe(false); expect(move.defaultPrevented).toBe(false); });
  expect(puts).toHaveLength(0);
});
test("Pin lives in the actual portal menu and survives reload", async () => {
  await render(); act(() => host.querySelector<HTMLButtonElement>('[aria-label="one 的操作"]')!.click());
  const menu = document.querySelector('[role="menu"][aria-label="会话操作"]')!; expect(host.contains(menu)).toBe(false);
  await act(async () => [...menu.querySelectorAll<HTMLButtonElement>("button")].find(button => button.textContent === "Pin")!.click());
  expect(persisted.sessions.find(entry => entry.session_id === "one")?.pinned).toBe(true); expect(host.textContent).toContain("Pinned");
});
test("selection and title updates cannot reorder groups or siblings", async () => {
  await render(); const before = ids(); chat = { ...chat, id: "two", session: row("two"), list: [{ ...row("two"), title: "renamed" }, row("one")] }; await render();
  expect(ids()).toEqual(before); expect(puts).toHaveLength(0);
});
test("group drag changes only explicit group order", async () => {
  persisted.groups.push({ id: "docs", title: "docs" }); persisted.sessions[1]!.group_id = "docs";
  await render(); const headings = [...host.querySelectorAll<HTMLElement>("[data-group-handle]")];
  const target = headings.find(element => element.dataset.groupHandle === "docs")!;
  Object.defineProperty(document, "elementFromPoint", { configurable: true, value: () => target });
  target.getBoundingClientRect = () => ({ top: 50, height: 30, bottom: 80, left: 0, right: 200, width: 200, x: 0, y: 50, toJSON: () => ({}) });
  await act(async () => { pointer(headings[0]!.querySelector("button")!, "pointerdown", 10); pointer(document, "pointermove", 76); pointer(document, "pointerup", 76); });
  expect(persisted.groups.map(group => group.id)).toEqual(["docs", "p1"]);
  expect(persisted.sessions[0]?.group_id).toBe("p1"); expect(persisted.sessions[1]?.group_id).toBe("docs");
});
test("parent moves with its rendered subtree and invalid child drop is rejected", async () => {
  persisted.sessions.splice(1, 0, { session_id: "child", group_id: "p1", parent_id: "one", pinned: false }); chat.list.push(row("child"));
  await render(); await drag(); expect(ids()).toEqual(["two", "one", "child"]); expect(persisted.sessions.find(entry => entry.session_id === "child")?.parent_id).toBe("one");
  const count = puts.length; geometry("child");
  await act(async () => { pointer(host.querySelector('[data-session-row="two"] .session-select')!, "pointerdown", 10); pointer(document, "pointermove", 76); pointer(document, "pointerup", 76); });
  expect(puts).toHaveLength(count);
});
test("archived pinned parent retains visible children", async () => {
  persisted.sessions[0]!.pinned = true;
  persisted.sessions.push({ session_id: "child", group_id: "p1", parent_id: "one", pinned: false });
  chat = { ...chat, listView: "archived", list: [...chat.list, row("child")] };
  await render(); expect(ids()).toEqual(["one", "child", "two"]);
});
test("a late response from another identity cannot expose its groups", async () => {
  let resolve!: (response: Response) => void; let requests = 0;
  vi.stubGlobal("fetch", vi.fn(() => ++requests === 1 ? new Promise<Response>(done => { resolve = done; }) : Promise.resolve(response({ revision: 0, groups: [], sessions: [] }))));
  await render(); const old = structuredClone(persisted); old.groups[0]!.title = "other-account-private-group";
  chat = { ...chat, actor: { ...chat.actor!, principal_id: "next-owner", generation: 2 } }; await render();
  await act(async () => resolve(response(old)));
  expect(host.textContent).not.toContain("other-account-private-group");
});
test("uncertain PUT is read back once, never silently replayed", async () => {
  await render(); let reads = 0;
  vi.stubGlobal("fetch", vi.fn(async (_path: string, options: RequestInit = {}) => { if (options.method === "PUT") { puts.push(options.body); throw new TypeError("connection lost"); } reads++; return response(persisted); }));
  await drag(); expect(puts).toHaveLength(1); expect(reads).toBe(1); expect(ids()).toEqual(["one", "two"]); expect(host.textContent).toContain("本次排序未保存");
});
test("sessions missing from the layout render under 未分组 without any write", async () => {
  chat = { ...chat, list: [...chat.list, row("loose")] };
  await render();
  expect(ids()).toEqual(["one", "two", "loose"]);
  expect(host.textContent).toContain("未分组");
  expect(puts).toHaveLength(0);
  const headings = [...host.querySelectorAll<HTMLElement>("[data-group-handle]")].map(element => element.dataset.groupHandle);
  expect(headings).toEqual(["p1"]);
});
test("project groups offer create-in-group and group menu, ungrouped offers neither", async () => {
  const onCreateInGroup = vi.fn();
  chat = { ...chat, list: [...chat.list, row("loose")] };
  await act(async () => { root.render(createElement(SessionList, { chat, apiKey: "test-only", onCreateInGroup })); });
  expect(host.querySelector("[aria-label='在 项目一 新建会话']")).toBeTruthy();
  expect(host.querySelector("[aria-label='在 未分组 新建会话']")).toBeNull();
  await act(async () => (host.querySelector<HTMLButtonElement>("[aria-label='在 项目一 新建会话']")!).click());
  expect(onCreateInGroup).toHaveBeenCalledWith("p1");
  expect(host.querySelector("[aria-label='未分组 的分组操作']")).toBeNull();
  await act(async () => (host.querySelector<HTMLButtonElement>("[aria-label='项目一 的分组操作']")!).click());
  const menu = document.querySelector('[role="menu"][aria-label="分组操作"]')!;
  expect(menu.textContent).toContain("重命名分组");
});
test("group rename keeps the project id and writes the alias through the sidebar PUT", async () => {
  await render();
  await act(async () => (host.querySelector<HTMLButtonElement>("[aria-label='项目一 的分组操作']")!).click());
  await act(async () => [...document.querySelectorAll<HTMLButtonElement>("[role='menuitem']")].find(button => button.textContent?.trim() === "重命名分组")!.click());
  const input = document.getElementById("sidebar-group-title") as HTMLInputElement;
  expect(input.value).toBe("项目一");
  await act(async () => {
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(input, "我的别名");
    input.dispatchEvent(new Event("input", { bubbles: true }));
  });
  await act(async () => (document.querySelector(".session-sidebar-dialog button[type='submit']") as HTMLButtonElement).click());
  await settle();
  expect(puts).toHaveLength(1);
  expect(puts[0]).toMatchObject({ groups: [{ id: "p1", title: "我的别名" }], sessions: persisted.sessions });
  expect(persisted.groups[0]).toEqual({ id: "p1", title: "我的别名" });
});
test("new project group creates a project instead of writing sidebar groups", async () => {
  await render();
  await act(async () => (host.querySelector<HTMLButtonElement>(".session-tree-toolbar > button")!).click());
  await act(async () => [...document.querySelectorAll<HTMLButtonElement>("[role='menuitem']")].find(button => button.textContent?.trim() === "新建项目分组")!.click());
  const input = document.getElementById("sidebar-title") as HTMLInputElement;
  await act(async () => {
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(input, "新项目");
    input.dispatchEvent(new Event("input", { bubbles: true }));
  });
  await act(async () => (document.querySelector(".session-sidebar-dialog button[type='submit']") as HTMLButtonElement).click());
  await settle();
  expect(projectPosts).toHaveLength(1);
  expect(projectPosts[0]).toMatchObject({ title: "新项目", workspace_id: "local" });
  expect(typeof projectPosts[0]!.request_key).toBe("string");
  expect(puts).toHaveLength(0);
  expect(sidebarReads).toBeGreaterThanOrEqual(2);
});
test("move dialog rebinds through the project API and reloads without touching layout", async () => {
  persisted.groups.push({ id: "p2", title: "项目二" });
  await render();
  await act(async () => (host.querySelector<HTMLButtonElement>("[aria-label='two 的操作']")!).click());
  await act(async () => [...document.querySelectorAll<HTMLButtonElement>("[role='menuitem']")].find(button => button.textContent?.trim() === "移动到项目")!.click());
  const select = document.getElementById("sidebar-group") as HTMLSelectElement;
  expect([...select.querySelectorAll("option")].map(option => option.value)).toEqual(["p1", "p2"]);
  await act(async () => {
    Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, "value")!.set!.call(select, "p2");
    select.dispatchEvent(new Event("change", { bubbles: true }));
  });
  const readsBefore = sidebarReads;
  const events: CustomEvent[] = [];
  const onSidebarChanged = (event: Event) => events.push(event as CustomEvent);
  window.addEventListener("krim:sidebar-changed", onSidebarChanged);
  await act(async () => (document.querySelector(".session-sidebar-dialog button[type='submit']") as HTMLButtonElement).click());
  await settle();
  window.removeEventListener("krim:sidebar-changed", onSidebarChanged);
  const bindingCalls = (fetch as ReturnType<typeof vi.fn>).mock.calls.filter(([path]: unknown[]) => String(path).endsWith("/sessions/two/project"));
  expect(bindingCalls).toHaveLength(2);
  expect(bindingCalls[0]![1].method ?? "GET").toBe("GET");
  expect(bindingCalls[1]![1].method).toBe("PUT");
  expect(JSON.parse(bindingCalls[1]![1].body).project_id).toBe("p2");
  expect(JSON.parse(bindingCalls[1]![1].body).expected_revision).toBe(3);
  expect(puts).toHaveLength(0);
  expect(sidebarReads).toBeGreaterThan(readsBefore);
  expect(events.some(event => event.detail?.principal === "owner" && event.detail?.workspace === "local")).toBe(true);
});
test("moving an unbound session shows a disabled placeholder and binds to a project", async () => {
  chat = { ...chat, list: [...chat.list, row("loose")] };
  await render();
  await act(async () => (host.querySelector<HTMLButtonElement>("[aria-label='loose 的操作']")!).click());
  await act(async () => [...document.querySelectorAll<HTMLButtonElement>("[role='menuitem']")].find(button => button.textContent?.trim() === "移动到项目")!.click());
  const select = document.getElementById("sidebar-group") as HTMLSelectElement;
  const options = [...select.querySelectorAll("option")];
  expect(options.map(option => option.value)).toEqual(["p1", "ungrouped"]);
  expect(options[1]!.disabled).toBe(true);
  expect(options[1]!.textContent).toBe("未分组（历史无绑定）");
  await act(async () => {
    Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, "value")!.set!.call(select, "p1");
    select.dispatchEvent(new Event("change", { bubbles: true }));
  });
  await act(async () => (document.querySelector(".session-sidebar-dialog button[type='submit']") as HTMLButtonElement).click());
  await settle();
  const bindingCalls = (fetch as ReturnType<typeof vi.fn>).mock.calls.filter(([path]: unknown[]) => String(path).endsWith("/sessions/loose/project"));
  expect(bindingCalls).toHaveLength(2);
  expect(JSON.parse(bindingCalls[1]![1].body).project_id).toBe("p1");
  expect(puts).toHaveLength(0);
});
test("submitting without picking a project keeps the dialog and sends nothing", async () => {
  chat = { ...chat, list: [...chat.list, row("loose")] };
  await render();
  await act(async () => (host.querySelector<HTMLButtonElement>("[aria-label='loose 的操作']")!).click());
  await act(async () => [...document.querySelectorAll<HTMLButtonElement>("[role='menuitem']")].find(button => button.textContent?.trim() === "移动到项目")!.click());
  await act(async () => (document.querySelector(".session-sidebar-dialog button[type='submit']") as HTMLButtonElement).click());
  await settle();
  const bindingCalls = (fetch as ReturnType<typeof vi.fn>).mock.calls.filter(([path]: unknown[]) => String(path).endsWith("/sessions/loose/project"));
  expect(bindingCalls).toHaveLength(0);
  expect(document.querySelector(".session-sidebar-dialog")).toBeTruthy();
});
test("project create resolving after a scope change does not write the stale dialog", async () => {
  let resolve!: (response: Response) => void;
  vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL, options: RequestInit = {}) => {
    const path = String(input), method = options.method ?? "GET";
    if (path === "/v2/me/sandbox/projects" && method === "POST") return new Promise<Response>(done => { resolve = done; });
    return Promise.resolve(response(persisted));
  }));
  await render();
  await act(async () => (host.querySelector<HTMLButtonElement>(".session-tree-toolbar > button")!).click());
  await act(async () => [...document.querySelectorAll<HTMLButtonElement>("[role='menuitem']")].find(button => button.textContent?.trim() === "新建项目分组")!.click());
  const input = document.getElementById("sidebar-title") as HTMLInputElement;
  await act(async () => {
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(input, "迟到的项目");
    input.dispatchEvent(new Event("input", { bubbles: true }));
  });
  await act(async () => (document.querySelector(".session-sidebar-dialog button[type='submit']") as HTMLButtonElement).click());
  chat = { ...chat, actor: { ...chat.actor!, principal_id: "next-owner", generation: 2 } };
  await render();
  await settle();
  await act(async () => resolve(response(project("p2", "迟到的项目"), 201)));
  await settle();
  expect(document.querySelector(".session-sidebar-dialog")).toBeNull();
  expect(host.textContent).not.toContain("创建结果未知");
  expect(host.textContent).not.toContain("项目身份不匹配");
});
test("a rejected rebind surfaces a notice instead of writing the old layout", async () => {
  persisted.groups.push({ id: "p2", title: "项目二" });
  await render();
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => {
    const path = String(input), method = options.method ?? "GET";
    if (path.endsWith("/sessions/two/project") && method === "PUT") return response({ error: { code: "revision_conflict" } }, 409);
    if (path.endsWith("/sessions/two/project")) return response({ session_id: "two", project_id: "p1", revision: 3, session_revision: 1, workspace_id: "local" });
    return response(persisted);
  }));
  await act(async () => (host.querySelector<HTMLButtonElement>("[aria-label='two 的操作']")!).click());
  await act(async () => [...document.querySelectorAll<HTMLButtonElement>("[role='menuitem']")].find(button => button.textContent?.trim() === "移动到项目")!.click());
  const select = document.getElementById("sidebar-group") as HTMLSelectElement;
  await act(async () => {
    Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, "value")!.set!.call(select, "p2");
    select.dispatchEvent(new Event("change", { bubbles: true }));
  });
  await act(async () => (document.querySelector(".session-sidebar-dialog button[type='submit']") as HTMLButtonElement).click());
  await settle();
  expect(host.textContent).toContain("状态已更新，本次未执行；请刷新后重试");
  expect(puts).toHaveLength(0);
  // 对话框保持打开：修复冲突后原样重试成功，旧错误提示随成功清除。
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => {
    const path = String(input), method = options.method ?? "GET";
    if (path.endsWith("/sessions/two/project")) {
      if (method === "PUT") { bindings.two = "p2"; return response({ session_id: "two", project_id: "p2", revision: 4, session_revision: 2, workspace_id: "local" }); }
      return response({ session_id: "two", project_id: "p1", revision: 3, session_revision: 1, workspace_id: "local" });
    }
    return response(persisted);
  }));
  await act(async () => (document.querySelector(".session-sidebar-dialog button[type='submit']") as HTMLButtonElement).click());
  await settle();
  expect(document.querySelector(".session-sidebar-dialog")).toBeNull();
  expect(host.textContent).not.toContain("状态已更新，本次未执行；请刷新后重试");
});
