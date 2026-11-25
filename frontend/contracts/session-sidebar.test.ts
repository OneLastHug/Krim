import { describe, expect, test } from "vitest";
import { UNGROUPED, emptySidebar, moveSidebar, parseSidebar, unrecordedEntries, type SidebarLayout } from "../src/lib/session-sidebar";
import type { Session } from "../src/lib/session-contracts";
const entry = (id: string, parent: string | null = null, group: string | null = "a") => ({ session_id: id, group_id: group, parent_id: parent, pinned: false });
const layout = (): SidebarLayout => ({ revision: 1, groups: [{ id: "a", title: "A" }, { id: "b", title: "B" }], sessions: [entry("one"), entry("child", "one"), entry("two"), entry("other", null, "b")] });
describe("explicit sidebar order", () => {
  test("moving a parent changes only sibling slots and leaves hierarchy intact", () => {
    const value = layout(), next = moveSidebar(value, "session", "two", "one", false);
    expect(next.sessions.map(item => item.session_id)).toEqual(["two", "child", "one", "other"]);
    expect(next.sessions.find(item => item.session_id === "child")?.parent_id).toBe("one");
    expect(value.sessions[0]?.session_id).toBe("one");
  });
  test("cross-group and cross-level targets cannot change order", () => {
    const value = layout();
    expect(moveSidebar(value, "session", "child", "two", true)).toBe(value);
    expect(moveSidebar(value, "session", "one", "other", true)).toBe(value);
  });
  test("unbound sessions can reorder among themselves but never across bindings", () => {
    const value: SidebarLayout = { revision: 1, groups: [{ id: "a", title: "A" }], sessions: [entry("loose1", null, null), entry("loose2", null, null), entry("bound", null, "a")] };
    const next = moveSidebar(value, "session", "loose2", "loose1", false);
    expect(next.sessions.map(item => item.session_id)).toEqual(["loose2", "loose1", "bound"]);
    expect(moveSidebar(value, "session", "loose1", "bound", true)).toBe(value);
  });
  test("Pinned is independent and does not participate in ordinary drags", () => {
    const value = layout(); value.sessions[0]!.pinned = true;
    expect(moveSidebar(value, "session", "one", "two", true)).toBe(value);
  });
  test("groups reorder without changing session metadata or project fields", () => {
    const value = layout(), next = moveSidebar(value, "group", "b", "a", false);
    expect(next.groups.map(item => item.id)).toEqual(["b", "a"]); expect(next.sessions).toBe(value.sessions);
  });
  test("rejects duplicate IDs, missing parents and cycles in stored metadata", () => {
    expect(() => parseSidebar({ ...layout(), sessions: [entry("one"), entry("one")] })).toThrow();
    expect(() => parseSidebar({ ...layout(), sessions: [entry("one", "missing")] })).toThrow();
    expect(() => parseSidebar({ ...layout(), sessions: [entry("one", "two"), entry("two", "one")] })).toThrow();
    expect(() => parseSidebar({ ...layout(), sessions: [entry("one", null, "missing-group")] })).toThrow();
  });
  test("accepts project groups, aliases and unbound legacy entries", () => {
    const value = parseSidebar({ revision: 3, groups: [{ id: "project-one", title: "自定义别名" }], sessions: [entry("one", null, "project-one"), entry("loose", null, null)] });
    expect(value.groups[0]).toEqual({ id: "project-one", title: "自定义别名" });
    expect(value.sessions[1]?.group_id).toBeNull();
    expect(() => parseSidebar({ revision: 0, groups: [], sessions: [] })).not.toThrow();
    expect(parseSidebar({ revision: 0, groups: [], sessions: [] })).toEqual(emptySidebar());
  });
  test("rejects cross-group parent chains and depth overflow in server data", () => {
    expect(() => parseSidebar({ revision: 1, groups: [{ id: "a", title: "A" }, { id: "b", title: "B" }], sessions: [entry("one"), entry("child", "one", "b")] })).toThrow();
    let deep: SidebarLayout = { revision: 1, groups: [{ id: "a", title: "A" }], sessions: [] };
    for (let index = 0; index < 33; index++) deep = { ...deep, sessions: [...deep.sessions, entry(`n${index}`, index ? `n${index - 1}` : null)] };
    expect(() => parseSidebar(deep)).toThrow();
  });
});
describe("unrecorded entries fill display gaps only", () => {
  const session = (id: string, created = 1): Session => ({ id, workspace_id: "local", owner_id: "owner", create_key: id, title: id, target_key: "demo", target_fingerprint: "a".repeat(64), context_policy: { max_bytes: 1000, max_messages: 10, reserved_output_tokens: 10, counter_version: "approx-utf8/1" }, revision: 1, status: "open", dispatch_paused: false, created_at: created, closed_at: null, archived_at: null, deleted_at: null, lifecycle_revision: 0 });
  test("unrecorded sessions become unbound display entries in list order", () => {
    const value = layout();
    const extra = unrecordedEntries(value, [session("fresh"), session("other"), session("loose")]);
    expect(extra).toEqual([{ session_id: "fresh", group_id: null, parent_id: null, pinned: false }, { session_id: "loose", group_id: null, parent_id: null, pinned: false }]);
  });
  test("empty layout yields all sessions unbound and never invents groups", () => {
    const extra = unrecordedEntries(emptySidebar(), [session("one"), session("two")]);
    expect(extra.map(item => item.session_id)).toEqual(["one", "two"]);
    expect(extra.every(item => item.group_id === null && item.pinned === false)).toBe(true);
  });
  test("the ungrouped sentinel stays a display-only id", () => {
    expect(UNGROUPED).toBe("ungrouped");
  });
});
