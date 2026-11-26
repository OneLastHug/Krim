import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { SubagentApprovals, SubagentPanel, SubagentSummary, type SubagentsState } from "../src/components/SubagentPanel";
import { ToolCallCard } from "../src/components/ToolCallCard";
import type { SessionState } from "../src/hooks/use-session";
import type { SubagentMember } from "../src/lib/subagent-contracts";

let root: Root, host: HTMLDivElement;
const member = (status: NonNullable<SubagentMember["task"]>["status"] = "running", id = "worker-a"): SubagentMember => ({ agent_id: id, tree_id: "tree-a", path: `lead/${id}`, role: "agent", status: "active", depth: 1, profile: "worker", provider: "sdk_session", workdir: null, revision: 1, current_task_id: "task-a", parent_path: "lead", tree_status: "open", route_key: "subagent:http:worker", title: "检查运行时取消与恢复", activation: { activation_id: "activation-a", instance: 1, status: "resident" }, task: { task_id: "task-a", status, run_id: "run-a", ordinal: 1, input: { text: "核对取消与恢复" } } });
const chat = (canWrite = true) => ({ canWrite, canApprove: true, ready: true, actor: { principal_id: "owner-a", generation: 1 }, workspace: "workspace-a", id: "session-a", session: { status: "open", archived_at: null, deleted_at: null }, execution: { policy: { project_id: null } } }) as unknown as SessionState;
function state(agent = member(), overrides: Record<string, unknown> = {}): SubagentsState {
  return { available: true, members: [agent], selectedId: agent.agent_id, detail: { agent, messages: [{ id: "message-a", seq: 1, kind: "task", wake: "auto", body: { text: "核对取消后是否保留已生成输出。" }, sender_id: null, task_id: "task-a", accepted_at: 1 }], results: [], events: [], approvals: [], run: null }, loading: false, detailLoading: false, error: "", detailError: "", busy: false, receipt: "", hasMore: false, approvalItems: [], select: vi.fn(), reload: vi.fn(), loadMore: vi.fn(), send: vi.fn().mockResolvedValue(true), control: vi.fn().mockResolvedValue(true), approve: vi.fn().mockResolvedValue(true), ...overrides } as unknown as SubagentsState;
}
async function render(value: SubagentsState, currentChat = chat()) { await act(async () => root.render(<SubagentPanel state={value} chat={currentChat} onOpenFile={vi.fn()} onShowApproval={vi.fn()}/>)); }
async function fill(text: string) { const input = host.querySelector("textarea")!; await act(async () => { Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, "value")!.set!.call(input, text); input.dispatchEvent(new Event("input", { bubbles: true })); }); }
function button(label: string, scope: ParentNode = host): HTMLButtonElement { const found = [...scope.querySelectorAll<HTMLButtonElement>("button")].find(element => element.textContent?.trim() === label || element.getAttribute("aria-label") === label); expect(found, label).toBeTruthy(); return found!; }
beforeEach(() => { host = document.createElement("div"); document.body.append(host); root = createRoot(host); (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true; });
afterEach(async () => { await act(async () => root.unmount()); host.remove(); vi.restoreAllMocks(); });

test("keeps failed message draft and clears it only after accepted send", async () => {
  const send = vi.fn().mockResolvedValueOnce(false).mockResolvedValueOnce(true), value = state(member(), { send });
  await render(value); await fill("请补查审批恢复。");
  await act(async () => button("发送").click());
  expect(send).toHaveBeenLastCalledWith("请补查审批恢复。", "auto"); expect(host.querySelector("textarea")!.value).toBe("请补查审批恢复。");
  await act(async () => { const select = host.querySelector("select")!; select.value = "queue_only"; select.dispatchEvent(new Event("change", { bubbles: true })); });
  await act(async () => button("发送").click());
  expect(send).toHaveBeenLastCalledWith("请补查审批恢复。", "queue_only"); expect(host.querySelector("textarea")!.value).toBe("");
});

test("interrupt and close require distinct confirmation and honor write access", async () => {
  const value = state(); await render(value);
  await act(async () => button("中断").click());
  let dialog = document.querySelector('[role="dialog"]')!; expect(dialog.textContent).toContain("中断当前子任务？"); expect(value.control).not.toHaveBeenCalled();
  await act(async () => button("中断任务", dialog).click()); expect(value.control).toHaveBeenCalledWith("interrupt", { agentId: "worker-a", activationId: "activation-a", taskId: "task-a" });
  await act(async () => button("关闭此子代理及后代").click());
  dialog = document.querySelector('[role="dialog"]')!; expect(dialog.textContent).toContain("关闭子代理及其后代？");
  await act(async () => button("关闭子代理", dialog).click()); expect(value.control).toHaveBeenCalledWith("close", { agentId: "worker-a", activationId: "activation-a", taskId: "task-a" });
  await render(value, chat(false)); expect(button("中断").disabled).toBe(true); expect(button("关闭此子代理及后代").disabled).toBe(true); expect(host.querySelector("textarea")!.disabled).toBe(true);
});

test("cancelled child run reads as stopped instead of a generic failure", async () => {
  const agent = member("interrupted");
  const value = state(agent, { detail: { agent, messages: [], results: [], events: [], approvals: [], run: { id: "run-a", status: "aborted", code: "cancelled" } } });
  await render(value);
  expect(host.textContent).toContain("已停止");
  expect(host.textContent).not.toContain("执行未完成");
});

test("closed members cannot receive input and selection does not expose previous draft", async () => {
  await render(state()); await fill("只属于A的后续消息");
  await render(state(member("completed", "worker-b"))); expect(host.querySelector("textarea")!.value).toBe("");
  await render(state({ ...member(), status: "closed" })); expect(host.querySelector("textarea")!.disabled).toBe(true); expect(button("发送").disabled).toBe(true);
});

test("one background entry opens existing workspace and no collaboration board is rendered", async () => {
  const listener = vi.fn(); window.addEventListener("krim:development-action", listener);
  try { const value = state(); await act(async () => root.render(<SubagentSummary state={value} chat={chat()}/>)); await act(async () => host.querySelector<HTMLButtonElement>(".sa-task-toggle")!.click()); expect(listener).toHaveBeenCalledTimes(1); expect((listener.mock.calls[0]![0] as CustomEvent).detail).toMatchObject({ action: "subagents", sessionId: "session-a", principal: "owner-a" }); expect(host.textContent).not.toContain("协作任务"); } finally { window.removeEventListener("krim:development-action", listener); }
});

test("real pending child approval keeps agent attribution and approval callback", async () => {
  const agent = member("awaiting_approval"), approval = { request_id: "approval-a", run_id: "run-a", call_id: "call-a", name: "shell", arguments: { command: "pytest -q" }, arguments_hash: "a".repeat(64), tool_hash: "b".repeat(64), expires_at: null };
  const value = state(agent, { approvalItems: [{ agent, approval }] });
  await act(async () => root.render(<SubagentApprovals state={value} chat={chat()}/>)); expect(host.textContent).toContain("检查运行时取消与恢复");
  await act(async () => button("允许本次调用").click()); expect(value.approve).toHaveBeenCalledWith(agent.agent_id, approval, true, undefined);
});

test("spawn receipt opens the returned agent id without changing tool payload", async () => {
  const open = vi.fn(), result = { status: "ok", value: { ok: true, agent_id: "child-real" } };
  await act(async () => root.render(<ToolCallCard tool={{ type: "tool", key: "tool-a", stepIndex: 0, name: "spawn_agent", state: "ok", arguments: { task: "check" }, result, resultAvailable: true }} canApprove={false} busy={false} pending={false} onDecision={vi.fn()} onInspect={vi.fn()} onOpenSubagent={open}/>));
  await act(async () => button("查看子代理").click()); expect(open).toHaveBeenCalledWith("child-real"); expect(result.value.agent_id).toBe("child-real");
});

test("long Chinese history renders fully in the panel without any truncation notice", async () => {
  const long = "全量输出验证：" + "逐字保留超长中文内容，不做任何截断。".repeat(3000) + "结尾标记甲乙丙";
  const agent = member();
  const fullDetail = { agent, messages: [{ id: "message-a", seq: 1, kind: "task", wake: "auto", body: { text: long }, sender_id: null, task_id: "task-a", accepted_at: 1 }], results: [{ seq: 1, notification_id: "notification-a", notification_status: "delivered", result_id: "result-a", task_id: "task-a", producer_path: "lead/worker-a", stop_reason: "completed", code: null, output: { text: long }, diagnostics: null, settled_at: 2 }], events: [{ run_id: "run-a", seq: 0, event_id: "event-a", kind: "text", data: { text: long }, at: 1 }], approvals: [], run: { id: "run-a", request_key: "key", status: "completed", trace_id: "trace", root_span_id: "span", created_at: 1, finished_at: 2, code: null, stop_requested: false }, run_id: "run-a" };
  const value = state(agent, { detail: fullDetail });
  await render(value);
  expect(host.textContent).toContain("结尾标记甲乙丙");
  expect(host.textContent).toContain("全量输出验证：");
  expect(host.textContent).not.toContain("当前显示最近一段");
});
