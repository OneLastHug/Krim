import { act, createElement } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { SessionList } from "../src/components/SessionList";
import type { SessionState } from "../src/hooks/use-session";
import type { Session } from "../src/lib/session-contracts";
import type { SidebarLayout } from "../src/lib/session-sidebar";
(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
let root: Root, host: HTMLDivElement, persisted: SidebarLayout, puts: unknown[], chat: SessionState;
const response = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });
const row = (id: string): Session => ({ id, title: id, workspace_id: "local", owner_id: "owner", create_key: id, target_key: "demo", target_fingerprint: "a".repeat(64), context_policy: { max_bytes: 1000, max_messages: 10, reserved_output_tokens: 10, counter_version: "approx-utf8/1" }, revision: 1, status: "open", dispatch_paused: false, created_at: id === "one" ? 1 : 2, closed_at: null, archived_at: null, deleted_at: null, lifecycle_revision: 0 });
beforeEach(() => {
  puts = []; persisted = { revision: 0, groups: [{ id: "sessions", title: "会话" }], sessions: ["one", "two"].map(id => ({ session_id: id, group_id: "sessions", parent_id: null, pinned: false })) };
  chat = { actor: { principal_id: "owner", generation: 1, legacy: true, workspaces: [{ workspace_id: "local", role: "admin", approve: true }] }, workspace: "local", id: "one", list: [row("two"), row("one")], session: row("one"), listView: "active", history: [], approvals: [], pendingControls: [], ready: true, canWrite: true, busy: false, controlling: false, notice: null, select: vi.fn(), setSessionListView: vi.fn(), lifecycle: vi.fn(), rename: vi.fn(), recoverControl: vi.fn(), refresh: vi.fn() } as unknown as SessionState;
  vi.stubGlobal("fetch", vi.fn(async (_path: string, options: RequestInit = {}) => {
    if (options.method === "PUT") { const body = JSON.parse(String(options.body)); puts.push(body); if (body.expected_revision !== persisted.revision) return response({ error: { code: "sidebar_revision_conflict" } }, 409); persisted = { revision: persisted.revision + 1, groups: body.groups, sessions: body.sessions }; }
    return response(persisted);
  }));
  host = document.createElement("div"); document.body.append(host); root = createRoot(host);
});
afterEach(() => { act(() => root.unmount()); host.remove(); vi.unstubAllGlobals(); });
async function render() { await act(async () => { root.render(createElement(SessionList, { chat, apiKey: "test-only" })); }); }
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
  expect(persisted.groups.map(group => group.id)).toEqual(["docs", "sessions"]);
  expect(persisted.sessions[0]?.group_id).toBe("sessions"); expect(persisted.sessions[1]?.group_id).toBe("docs");
});
test("parent moves with its rendered subtree and invalid child drop is rejected", async () => {
  persisted.sessions.splice(1, 0, { session_id: "child", group_id: "sessions", parent_id: "one", pinned: false }); chat.list.push(row("child"));
  await render(); await drag(); expect(ids()).toEqual(["two", "one", "child"]); expect(persisted.sessions.find(entry => entry.session_id === "child")?.parent_id).toBe("one");
  const count = puts.length; geometry("child");
  await act(async () => { pointer(host.querySelector('[data-session-row="two"] .session-select')!, "pointerdown", 10); pointer(document, "pointermove", 76); pointer(document, "pointerup", 76); });
  expect(puts).toHaveLength(count);
});
test("archived pinned parent retains visible children", async () => {
  persisted.sessions[0]!.pinned = true;
  persisted.sessions.push({ session_id: "child", group_id: "sessions", parent_id: "one", pinned: false });
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
