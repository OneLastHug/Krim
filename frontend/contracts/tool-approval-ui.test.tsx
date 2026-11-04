import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { ToolCallCard } from "../src/components/ToolCallCard";
import type { ToolPresentation } from "../src/lib/session-presentation";

let root: Root;
let host: HTMLDivElement;
const decision = vi.fn();
const base: ToolPresentation = { type: "tool", key: "call-1", callId: "call-1", stepIndex: 0, name: "lookup_note", state: "approval", resultAvailable: false };

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(1_000_000);
  decision.mockReset();
  (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  host = document.createElement("div");
  document.body.append(host);
  root = createRoot(host);
});
afterEach(async () => {
  await act(async () => root.unmount());
  host.remove();
  vi.useRealTimers();
});
async function render(tool: ToolPresentation) {
  await act(async () => root.render(<ToolCallCard tool={tool} canApprove busy={false} pending={false} onDecision={decision} onInspect={() => {}} />));
}

test("审批倒计时使用expires_at但不自动作决定或改变授权", async () => {
  await render({ ...base, approval: { request_id: "request-1", run_id: "run-1", call_id: "call-1", name: "lookup_note", arguments: { key: "context" }, arguments_hash: "a".repeat(64), tool_hash: "b".repeat(64), expires_at: 1300 } });
  expect(host.querySelector('[role="timer"]')?.textContent).toBe("审批剩余 05:00");
  await act(async () => { vi.advanceTimersByTime(1200); });
  expect(host.querySelector('[role="timer"]')?.textContent).toBe("审批剩余 04:59");
  await act(async () => { vi.advanceTimersByTime(300_000); });
  expect(host.querySelector('[role="timer"]')?.textContent).toBe("等待更新审批状态");
  expect(decision).not.toHaveBeenCalled();
  const allow = host.querySelector<HTMLButtonElement>('[aria-label="允许 lookup_note 本次调用"]');
  expect(allow?.disabled).toBe(false);
  await act(async () => allow!.click());
  expect(decision).toHaveBeenCalledExactlyOnceWith(true);
  await render({ ...base, state: "denied", executorStarted: false, code: "approval_expired" });
  expect(host.querySelector('[role="timer"]')).toBeNull();
  expect(host.querySelector("summary")?.textContent).toContain("审批已过期");
  expect(vi.getTimerCount()).toBe(0);
});

test("已知工具失败不显示读取失败或泛化的结果未确认", async () => {
  await render({ ...base, state: "failed", executorStarted: null, code: "tool_error" });
  expect(host.textContent).toContain("工具调用失败，未取得结果");
  expect(host.querySelector(".chat-tool-error")?.textContent).toContain("工具执行失败");
  expect(host.textContent).not.toContain("读取失败");
  expect(host.textContent).not.toContain("结果未确认");
});

test("未知工具状态仍保留不确定结果", async () => {
  await render({ ...base, state: "unknown", executorStarted: null });
  expect(host.textContent).toContain("结果未确认");
});
