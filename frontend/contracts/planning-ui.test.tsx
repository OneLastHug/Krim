import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { TaskPlan, type TaskPlanProps } from "../src/components/TaskPlan";
import {
  mergePlanPage, parsePlanSnapshot,
  type PlanCounts, type PlanSnapshot, type PlanStep, type PlanStatus,
} from "../src/lib/planning-contracts";
import { readPlan } from "../src/lib/planning-client";

let root: Root;
let host: HTMLDivElement;

const statuses: PlanStatus[] = ["pending", "in_progress", "completed", "blocked"];
const counts = (overrides: Partial<PlanCounts> = {}): PlanCounts =>
  ({ pending: 1, in_progress: 1, completed: 1, blocked: 1, ...overrides });
const step = (id: string, status: PlanStatus = "pending", text = `步骤 ${id}`): PlanStep => ({ id, text, status });

function page(overrides: Partial<PlanSnapshot> = {}): PlanSnapshot {
  const base: PlanSnapshot = {
    version: "planning/1", session_id: "session-a", revision: 2,
    content_hash: "a".repeat(64), updated_at: 100,
    counts: counts(), total: 4, matched_total: 4,
    steps: statuses.map((status, index) => step(`step-${index}`, status)),
    offset: 0, limit: 50, next_offset: null,
  };
  return { ...base, ...overrides };
}

beforeEach(() => {
  (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  host = document.createElement("div");
  document.body.append(host);
  root = createRoot(host);
});
afterEach(async () => {
  await act(async () => root.unmount());
  host.remove();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

async function renderTaskPlan(props: TaskPlanProps) {
  await act(async () => root.render(<TaskPlan {...props} />));
}
function button(label: string): HTMLButtonElement | undefined {
  return [...host.querySelectorAll("button")].find(item => item.textContent?.trim() === label);
}

test("task_plan_renders_global_counts_and_loaded_steps_read_only_and_collapsed", async () => {
  const longText = "很长的计划步骤".repeat(80) + "\n第二行保留换行";
  const snapshot = page({
    steps: [
      step("plan-a", "pending"),
      step("plan-b", "in_progress", longText),
      step("plan-c", "completed"),
      step("plan-d", "blocked"),
    ],
  });
  await renderTaskPlan({ snapshot });
  const details = host.querySelector("details.task-plan");
  expect(details).toBeTruthy();
  expect((details as HTMLDetailsElement).open).toBe(false);
  expect(details!.getAttribute("aria-label")).toBe("会话计划");
  expect(details!.textContent).toContain("计划");
  expect(details!.textContent).toContain("已完成 1 / 4 步");

  const summaryCounts = [...host.querySelectorAll(".task-plan-counts li")].map(item => item.textContent?.replace(/\s+/g, ""));
  expect(summaryCounts).toEqual(["待开始1", "进行中1", "已完成1", "受阻1"]);
  expect(host.textContent).toContain("计划共 4 步");
  expect(host.textContent).toContain("已加载 4 / 4 步");
  expect(host.querySelectorAll(".task-plan-step")).toHaveLength(4);
  expect(host.querySelector(".task-plan-step[data-status='in_progress'] .task-plan-step-text")?.textContent).toBe(longText);
  expect([...host.querySelectorAll(".task-plan-step-state")].map(item => item.textContent)).toEqual(
    ["待开始", "进行中", "已完成", "受阻"]);

  // 只读：没有编辑入口、表单控件或提交按钮。
  expect(host.querySelectorAll("input, textarea, select, form, [contenteditable]")).toHaveLength(0);
  expect(host.querySelectorAll("button")).toHaveLength(0);
  expect(host.textContent).not.toContain("编辑计划");
  expect(host.textContent).not.toContain("保存计划");
});

test("task_plan_distinguishes_filtered_page_from_whole_plan", async () => {
  const filtered = page({
    counts: counts({ pending: 2, in_progress: 0, completed: 2, blocked: 1 }),
    total: 5, matched_total: 2, steps: [step("only-a", "pending"), step("only-b", "completed")],
    limit: 2,
  });
  await renderTaskPlan({ snapshot: filtered });
  expect(host.textContent).toContain("计划共 5 步");
  expect(host.textContent).toContain("已加载 2 / 2 步");
  expect(host.textContent).toContain("筛选匹配，计划共 5 步");
  expect(host.textContent).not.toContain("计划共 2 步");
  expect(host.querySelectorAll(".task-plan-step")).toHaveLength(2);
  expect(button("加载更多")).toBeUndefined();
});

test("task_plan_paginates_with_load_more_and_accepts_merged_display_snapshot", async () => {
  const first = page({
    counts: counts({ pending: 1, in_progress: 1, completed: 1, blocked: 0 }),
    total: 3, matched_total: 3,
    steps: [step("page-a", "pending"), step("page-b", "in_progress")],
    limit: 2, next_offset: 2,
  });
  const onLoadMore = vi.fn(async () => undefined);
  await renderTaskPlan({ snapshot: first, onLoadMore });
  expect(host.textContent).toContain("已加载 2 / 3 步");
  expect(host.textContent).toContain("还有 1 步未加载");
  await act(async () => { button("加载更多")!.click(); });
  expect(onLoadMore).toHaveBeenCalledOnce();

  await renderTaskPlan({ snapshot: first, onLoadMore, loading: true });
  expect(host.textContent).toContain("正在读取计划");
  expect(button("加载更多")!.disabled).toBe(true);

  // 合并后的展示快照 steps 可超过单页 limit；组件不应再次按单页规则解析。
  const merged = mergePlanPage(first, page({
    counts: counts({ pending: 1, in_progress: 1, completed: 1, blocked: 0 }),
    total: 3, matched_total: 3,
    steps: [step("page-c", "completed")],
    offset: 2, limit: 2, next_offset: null,
  }));
  expect(merged.offset).toBe(0);
  expect(merged.steps).toHaveLength(3);
  expect(merged.next_offset).toBeNull();
  await renderTaskPlan({ snapshot: merged, onLoadMore });
  expect(host.textContent).toContain("已加载 3 / 3 步");
  expect(host.querySelectorAll(".task-plan-step")).toHaveLength(3);
  expect(button("加载更多")).toBeUndefined();
  expect(host.textContent).not.toContain("加载失败");
});

test("task_plan_empty_plan_returns_null_and_error_offers_retry", async () => {
  await renderTaskPlan({ snapshot: page({
    revision: 0, updated_at: null, counts: counts({ pending: 0, in_progress: 0, completed: 0, blocked: 0 }),
    total: 0, matched_total: 0, steps: [],
  }) });
  expect(host.innerHTML).toBe("");

  const onRetry = vi.fn();
  await renderTaskPlan({ snapshot: page(), error: "无法读取计划：网络错误", onRetry });
  expect(host.querySelector("[role='alert']")?.textContent).toBe("无法读取计划：网络错误");
  await act(async () => { button("重试读取计划")!.click(); });
  expect(onRetry).toHaveBeenCalledOnce();
  expect(host.querySelectorAll("button")).toHaveLength(1);
  expect(host.querySelectorAll("input, textarea, select")).toHaveLength(0);
});

test("task_plan_uses_load_more_as_pagination_retry_and_does_not_reject_caller_promise", async () => {
  const onLoadMore = vi.fn(async () => { throw new Error("network"); });
  const first = page({
    counts: counts({ pending: 2, in_progress: 0, completed: 0, blocked: 0 }),
    total: 2, matched_total: 2, steps: [step("retry-a")], limit: 1, next_offset: 1,
  });
  await renderTaskPlan({ snapshot: first, error: "加载下一页失败", onLoadMore });
  await act(async () => { button("重试读取计划")!.click(); });
  expect(onLoadMore).toHaveBeenCalledOnce();
  await act(async () => { await Promise.resolve(); });
  expect(host.textContent).toContain("加载下一页失败");
});

test("plan_parser_accepts_wire_page_and_rejects_malformed_snapshots", () => {
  const parsed = parsePlanSnapshot(page(), "session-a");
  expect(parsed.version).toBe("planning/1");
  expect(parsed.steps).toHaveLength(4);
  expect(parsePlanSnapshot(page({ total: 0, matched_total: 0, counts: counts({
    pending: 0, in_progress: 0, completed: 0, blocked: 0 }), steps: [] })).total).toBe(0);

  const invalid: [string, unknown][] = [
    ["bad session", { ...page(), session_id: "session-b" }],
    ["bad version", { ...page(), version: "planning/2" }],
    ["bad status", { ...page(), steps: [{ id: "a", text: "步骤", status: "running" }] }],
    ["bad type", { ...page(), revision: "2" }],
    ["bad counts", { ...page(), counts: counts({ pending: 2 }) }],
    ["bad total type", { ...page(), total: 4.5 }],
    ["bad hash", { ...page(), content_hash: "A".repeat(64) }],
    ["duplicate id", { ...page(), steps: [step("same"), step("same"), step("third"), step("fourth")] }],
    ["bad step id", { ...page(), steps: [{ id: "has space", text: "步骤", status: "pending" }] }],
    ["blank step text", { ...page(), steps: [{ id: "blank", text: "  ", status: "pending" }] }],
    ["missing count", { ...page(), counts: { pending: 1, in_progress: 1, completed: 1 } }],
    ["extra field", { ...page(), extra: true }],
    ["page length", { ...page(), steps: [step("one")] }],
    ["next offset", { ...page(), next_offset: 2 }],
    ["missing next offset", { ...page(), next_offset: undefined }],
    ["matched over total", { ...page(), matched_total: 5 }],
    ["offset without full page", { ...page(), offset: 1, steps: [step("one"), step("two")] }],
    ["bad updated_at", { ...page(), updated_at: Number.NaN }],
    ["bad limit", { ...page(), limit: 0 }],
  ];
  for (const [label, value] of invalid) {
    expect(() => parsePlanSnapshot(value, "session-a"), label).toThrow("计划响应无效");
  }
  expect(() => parsePlanSnapshot("not-an-object")).toThrow("计划响应无效");
  expect(() => parsePlanSnapshot([page()])).toThrow("计划响应无效");
});

test("plan_merge_requires_same_revision_and_hash_and_keeps_loaded_offsets_consistent", () => {
  const first = page({
    counts: counts({ pending: 2, in_progress: 0, completed: 0, blocked: 0 }),
    total: 2, matched_total: 2, steps: [step("merge-a")], limit: 1, next_offset: 1,
  });
  const second = page({
    counts: counts({ pending: 2, in_progress: 0, completed: 0, blocked: 0 }),
    total: 2, matched_total: 2, steps: [step("merge-b")], offset: 1, limit: 1, next_offset: null,
  });
  const merged = mergePlanPage(first, second);
  expect(merged.steps.map(item => item.id)).toEqual(["merge-a", "merge-b"]);
  expect(merged.counts).toEqual(first.counts);
  expect(merged.offset).toBe(0);
  expect(merged.next_offset).toBeNull();
  expect(merged.total).toBe(2);

  expect(() => mergePlanPage(first, { ...second, revision: 3 })).toThrow("计划响应无效");
  expect(() => mergePlanPage(first, { ...second, content_hash: "b".repeat(64) })).toThrow("计划响应无效");
  expect(() => mergePlanPage(first, { ...second, offset: 0 })).toThrow("计划响应无效");
  expect(() => mergePlanPage(first, { ...second, counts: counts({ pending: 1 }) })).toThrow("计划响应无效");
  expect(() => mergePlanPage(first, { ...second, steps: [step("merge-a")] })).toThrow("计划响应无效");
});

test("read_plan_uses_versioned_path_fixed_pagination_query_and_validates_session", async () => {
  const fetcher = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
    new Response(JSON.stringify(page({ session_id: "session two" })), { status: 200, headers: { "Content-Type": "application/json" } }));
  vi.stubGlobal("fetch", fetcher);
  const snapshot = await readPlan("secret", "workspace/one", "session two", {
    revision: 3, offset: 50, statuses: ["pending", "blocked"],
  });
  expect(snapshot.session_id).toBe("session two");
  const [path, init] = fetcher.mock.calls[0]!;
  expect(String(path)).toBe("/v2/workspaces/workspace%2Fone/sessions/session%20two/plan?revision=3&offset=50&statuses=pending&statuses=blocked");
  expect(init?.method).toBe("GET");
  expect((init?.headers as Record<string, string>).Authorization).toBe("Bearer secret");
  expect(init?.body).toBeUndefined();

  await expect(readPlan("secret", "workspace", "session-a", { offset: 1 })).rejects.toThrow("计划查询无效");
  await expect(readPlan("secret", "workspace", "session-a", { limit: 201 })).rejects.toThrow("计划查询无效");
  await expect(readPlan("secret", "workspace", "session-a", { statuses: ["pending", "pending"] }))
    .rejects.toThrow("计划查询无效");
  await expect(readPlan("secret", "workspace", "session-b")).rejects.toThrow("计划响应无效");
  expect(fetcher).toHaveBeenCalledTimes(2);
});
