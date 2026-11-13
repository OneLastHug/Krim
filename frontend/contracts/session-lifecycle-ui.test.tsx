import { afterEach, expect, test, vi } from "vitest";
import { act, createElement } from "react";
import { createRoot, type Root } from "react-dom/client";
import { SessionList } from "../src/components/SessionList";
import { WorkbenchLayout } from "../src/components/WorkbenchLayout";
import type { SessionState } from "../src/hooks/use-session";

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
afterEach(() => { act(() => root?.unmount()); container?.remove(); });

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

test("trash row provides restore and hides move-to-trash action", () => {
  const chat = state({ listView: "trash", list: [{ ...state().session!, deleted_at: 3 }] });
  const view = setup(chat);
  const actions = [...view.querySelectorAll<HTMLButtonElement>("button")].map(button => button.getAttribute("aria-label"));
  expect(actions).toContain("恢复此对话");
  expect(actions).not.toContain("移入回收站");
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
