import { describe, expect, test } from "vitest";
import { DEFAULT_GROUP, emptySidebar, includeSessions, moveSidebar, parseSidebar, type SidebarLayout } from "../src/lib/session-sidebar";
import type { Session } from "../src/lib/session-contracts";
const entry = (id: string, parent: string | null = null, group = "a") => ({ session_id: id, group_id: group, parent_id: parent, pinned: false });
const layout = (): SidebarLayout => ({ revision: 1, groups: [{ id: "a", title: "A" }, { id: "b", title: "B" }], sessions: [entry("one"), entry("child", "one"), entry("two"), entry("other", null, "b")] });
describe("explicit sidebar order", () => {
  test("activity and API ordering cannot promote known sessions", () => {
    const value = layout();
    const sessions = [{ id: "two", created_at: 2 }, { id: "one", created_at: 1 }] as Session[];
    expect(includeSessions(value, sessions).sessions.map(item => item.session_id)).toEqual(["one", "child", "two", "other"]);
    expect(value.sessions).toHaveLength(4);
  });
  test("unrecorded sessions append by creation time even if API returns latest first", () => {
    const value = includeSessions(emptySidebar(), [{ id: "new", created_at: 20 }, { id: "old", created_at: 10 }] as Session[]);
    expect(value.groups).toEqual([{ id: DEFAULT_GROUP, title: "会话" }]);
    expect(value.sessions.map(item => item.session_id)).toEqual(["old", "new"]);
  });
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
    expect(() => parseSidebar({ ...layout(), sessions: [entry("one"), entry("child", "one", "b")] })).toThrow();
    expect(parseSidebar(layout())).toEqual(layout());
  });
});
