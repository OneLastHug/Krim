import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { groupFileTools, fileChangeCount, fileGroupSummary, type FileToolGroup } from "../src/lib/file-tool-groups";
import type { ToolPresentation, TurnPresentation } from "../src/lib/session-presentation";
import { FileToolGroupCard } from "../src/components/FileToolGroupCard";
import { ToolCallCard } from "../src/components/ToolCallCard";

function tool(id: string, overrides: Partial<ToolPresentation> = {}): ToolPresentation {
  return { type: "tool", key: id, providerCallId: id, stepIndex: 1, name: "edit", state: "ok", arguments: { path: "server.mjs", old_text: "old", new_text: "new" }, result: { ok: true, lines_added: 3, lines_removed: 1 }, resultAvailable: true, executorStarted: true, ...overrides };
}
const renderTool = (value: ToolPresentation) => <ToolCallCard key={value.key} tool={value} canApprove={false} busy={false} pending={false} onDecision={vi.fn()} onInspect={vi.fn()}/>;
let root: Root, host: HTMLDivElement;
beforeEach(() => { host = document.createElement("div"); document.body.append(host); root = createRoot(host); (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true; });
afterEach(async () => { await act(async () => root.unmount()); host.remove(); });
function groups(items: TurnPresentation[]) { return groupFileTools(items).filter((x): x is FileToolGroup => x.type === "file-group"); }

test("groups consecutive exact paths while preserving command, text and approval boundaries", () => {
  const approval = { request_id: "approval", run_id: "run", call_id: "call", name: "edit", arguments: { path: "server.mjs" }, arguments_hash: "a".repeat(64), tool_hash: "b".repeat(64), expires_at: null };
  const items = groupFileTools([
    tool("a"), tool("b", { name: "read" }), tool("shell", { name: "shell" }),
    tool("c"), { type: "assistant", key: "text", text: "测试后继续修改", partial: false },
    tool("d"), tool("approval", { state: "approval", approval }), tool("e"),
    tool("different", { arguments: { path: "./server.mjs" } }),
  ]);
  expect(items.map(x => x.type)).toEqual(["file-group", "tool", "file-group", "assistant", "file-group", "tool", "file-group", "file-group"]);
  expect((items[0] as FileToolGroup).tools.map(x => x.key)).toEqual(["a", "b"]);
  expect(groups([tool("legacy", { name: "dev_read" }), tool("new", { name: "read" })])).toHaveLength(1);
  expect(groupFileTools([tool("plugin", { name: "external_edit" })])[0]!.type).toBe("tool");
});

test("counts only confirmed successful receipt metadata, with no guesses for legacy or failed calls", () => {
  expect(fileChangeCount(tool("direct"))).toEqual({ added: 3, removed: 1 });
  expect(fileChangeCount(tool("wrapped", { result: { status: "ok", value: { ok: true, lines_added: 7, lines_removed: 4 } } }))).toEqual({ added: 7, removed: 4 });
  for (const overrides of [
    { state: "running" }, { state: "failed" }, { executorStarted: false }, { resultAvailable: false },
    { name: "read" }, { result: { ok: false, lines_added: 3, lines_removed: 1 } },
    { result: { lines_added: -1, lines_removed: 1 } }, { result: { lines_added: "3", lines_removed: 1 } },
    { result: { size: 123 } },
  ] as Partial<ToolPresentation>[]) expect(fileChangeCount(tool("invalid", overrides))).toBeNull();
  const summary = fileGroupSummary([tool("one"), tool("two"), tool("read", { name: "read" }), tool("legacy", { result: {} }), tool("bad", { state: "failed" })]);
  expect(summary).toMatchObject({ added: 6, removed: 2, counted: 2, uncounted: 1, writes: 4, reads: 1, status: "1 次失败" });
});

test("updates live counts and status without replacing the opened group", async () => {
  const first = tool("one", { state: "running", resultAvailable: false });
  const render = async (items: ToolPresentation[]) => {
    const group = groups(items)[0]!;
    await act(async () => root.render(<FileToolGroupCard key={group.key} group={group} renderTool={renderTool}/>));
  };
  await render([first]);
  const element = host.querySelector<HTMLDetailsElement>(".chat-file-group")!;
  expect(element.textContent).toContain("修改中");
  expect(element.querySelector(".chat-file-changes")).toBeNull();
  await act(async () => { element.open = true; });
  await render([tool("one"), tool("two", { state: "running", resultAvailable: false })]);
  expect(host.querySelector(".chat-file-group")).toBe(element);
  expect(element.open).toBe(true);
  expect(element.querySelector(".chat-file-changes")!.textContent).toBe("+3−1");
  expect(element.querySelector("summary")!.textContent).toContain("修改 2 次");
  await render([tool("one"), tool("two", { result: { lines_added: 5, lines_removed: 2 } })]);
  expect(element.querySelector(".chat-file-changes")!.textContent).toBe("+8−3");
  expect(element.querySelector("summary")!.textContent).toContain("已完成");
  expect(element.querySelectorAll(".chat-file-calls > details")).toHaveLength(2);
});

test("stable provider identity keeps group key when execution call id arrives", () => {
  const before = groups([tool("provider", { key: "tool:provider" })])[0]!;
  const after = groups([tool("provider", { key: "tool:execution", callId: "execution" })])[0]!;
  expect(before.key).toBe(after.key);
});

test("legacy missing counts are explicit and all child call details remain accessible", async () => {
  const group = groups([tool("a", { result: {} }), tool("b", { name: "read" })])[0]!;
  await act(async () => root.render(<FileToolGroupCard group={group} renderTool={renderTool}/>));
  const summary = host.querySelector("summary")!;
  expect(summary.textContent).toContain("行数未记录");
  expect(summary.textContent).toContain("修改 1 次 · 读取 1 次");
  expect(host.querySelectorAll(".chat-file-calls > details")).toHaveLength(2);
});
