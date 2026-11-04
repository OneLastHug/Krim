import assert from "node:assert/strict";
import { projectTurn } from "../src/lib/session-presentation.ts";
import type { Turn } from "../src/lib/session-contracts.ts";

const turn = (status: Turn["status"] = "running", transcript: Turn["transcript"] = null): Turn => ({
  id: "turn-1", session_id: "session-1", workspace_id: "workspace-1", owner_id: "owner-1", ordinal: 1,
  request_key: "request-1", text: "用户原始问题", accepted_revision: 1, status, run_id: "run-1",
  context_fingerprint: null, transcript, code: null, created_at: 1, started_at: 2, finished_at: status === "running" ? null : 3,
});
const event = (seq: number, kind: string, data: Record<string, unknown>, runId = "run-1") => ({ run_id: runId, seq, event_id: `event-${seq}`, kind, data });
const start = (seq = 1, callId = "internal-1", providerCallId = "provider-1", name = "lookup", args: Record<string, unknown> = { query: "x" }) => event(seq, "tool_start", {
  call_id: callId, provider_call_id: providerCallId, step_index: 0, name, arguments: args,
});
const end = (seq: number, status: string, value: unknown, resultAvailable: boolean, callId = "internal-1", providerCallId = "provider-1") => event(seq, "tool_end", {
  call_id: callId, provider_call_id: providerCallId, step_index: 0, name: "lookup", arguments: { query: "x" }, status,
  executor_started: true, result_available: resultAvailable, ...(resultAvailable ? { value } : {}),
});
const approval = {
  request_id: "approval-1", run_id: "run-1", call_id: "internal-1", name: "lookup", arguments: { query: "x" },
  arguments_hash: "a".repeat(64), tool_hash: "b".repeat(64), expires_at: 99,
  risk: "read", source: { kind: "mcp", server: "synthetic", tool: "lookup" },
};
let checks = 0;

const mixed = projectTurn(turn(), [
  end(4, "ok", 0, true),
  event(3, "tool_intent", { call_id: "internal-1", step_index: 0, decision: "allow" }),
  start(2),
  event(1, "model_end", { step_index: 0, tool_calls: [{ kind: "tool_call", id: "provider-1", name: "lookup", arguments: { query: "x" } }] }),
  event(0, "text", { step_index: 0, text: "处理中" }),
  event(5, "tool_end", { call_id: "foreign", provider_call_id: "foreign-provider", step_index: 0, name: "foreign", arguments: {}, status: "ok", result_available: true, value: 1 }, "other-run"),
], []);
assert.deepEqual(mixed.map(item => item.type), ["assistant", "tool"]);
assert.equal(mixed[0]!.type, "assistant");
if (mixed[1]!.type !== "tool") throw new Error("tool projection missing");
assert.equal(mixed[1]!.callId, "internal-1");
assert.equal(mixed[1]!.providerCallId, "provider-1");
assert.equal(mixed[1]!.state, "ok");
assert.equal(mixed[1]!.resultAvailable, true);
assert.equal(mixed[1]!.result, 0);
checks++;

const waiting = projectTurn(turn(), [start(), event(2, "approval_request", { request_id: "history-only", run_id: "run-1", call_id: "internal-1", name: "lookup" })], [approval]);
assert.equal(waiting[0]!.type, "tool");
if (waiting[0]!.type !== "tool") throw new Error("approval projection missing");
assert.equal(waiting[0]!.state, "approval");
assert.deepEqual(waiting[0]!.approval, approval);
assert.equal(waiting[0]!.risk, "read");
assert.deepEqual(waiting[0]!.source, approval.source);
const historicalOnly = projectTurn(turn(), [start(), event(2, "approval_request", { ...approval })], []);
assert.equal(historicalOnly[0]!.type, "tool");
if (historicalOnly[0]!.type === "tool") assert.equal(historicalOnly[0]!.state, "preparing");
const pendingWithoutEvents = projectTurn(turn(), [], [approval]);
assert.equal(pendingWithoutEvents[0]!.type, "tool");
if (pendingWithoutEvents[0]!.type === "tool") {
  assert.equal(pendingWithoutEvents[0]!.callId, "internal-1");
  assert.equal(pendingWithoutEvents[0]!.stepIndex, -1);
  assert.equal(pendingWithoutEvents[0]!.state, "approval");
  assert.deepEqual(pendingWithoutEvents[0]!.approval, approval);
}
const approvedButNotPending = projectTurn(turn(), [
  start(), event(2, "approval_request", { ...approval }),
  event(3, "approval_reply", { request_id: "approval-1", reply: { allow: true } }),
  event(4, "tool_intent", { call_id: "internal-1", step_index: 0, decision: "allow" }),
], []);
assert.equal(approvedButNotPending[0]!.type, "tool");
if (approvedButNotPending[0]!.type === "tool") {
  assert.equal(approvedButNotPending[0]!.state, "running");
  assert.equal(approvedButNotPending[0]!.approval, undefined);
  assert.equal(approvedButNotPending[0]!.risk, "read");
  assert.deepEqual(approvedButNotPending[0]!.source, approval.source);
}
checks++;

for (const [status, expected] of [["denied", "denied"], ["cancelled", "cancelled"], ["failed", "failed"]] as const) {
  const projected = projectTurn(turn(), [start(), end(2, status, null, false)], [approval]);
  assert.equal(projected[0]!.type, "tool");
  if (projected[0]!.type === "tool") {
    assert.equal(projected[0]!.state, expected);
    assert.equal(projected[0]!.resultAvailable, false);
    assert.equal(projected[0]!.approval, undefined);
  }
  checks++;
}

const allowed = projectTurn(turn(), [start(), end(2, "ok", null, true)], [approval]);
assert.equal(allowed[0]!.type, "tool");
if (allowed[0]!.type === "tool") {
  assert.equal(allowed[0]!.state, "ok");
  assert.equal(allowed[0]!.resultAvailable, true);
  assert.equal(allowed[0]!.result, null);
  assert.equal(allowed[0]!.approval, undefined);
}
checks++;

for (const value of [false, null, 0]) {
  const projected = projectTurn(turn(), [start(), end(2, "ok", value, true)], []);
  assert.equal(projected[0]!.type, "tool");
  if (projected[0]!.type === "tool") {
    assert.equal(projected[0]!.resultAvailable, true);
    assert.equal(projected[0]!.result, value);
  }
  checks++;
}

const conflictingSequence = projectTurn(turn(), [start(1), end(2, "ok", "good", true), { ...end(2, "failed", "bad", true), event_id: "conflicting-event" }], []);
assert.equal(conflictingSequence.length, 1);
assert.equal(conflictingSequence[0]!.type, "tool");
if (conflictingSequence[0]!.type === "tool") assert.equal(conflictingSequence[0]!.state, "preparing");
const conflictingIdentity = projectTurn(turn(), [start(1, "internal-1", "shared-provider", "lookup"), start(2, "internal-2", "shared-provider", "different", { query: "different" })], []);
assert.deepEqual(conflictingIdentity, []);
const contradictoryFacts = projectTurn(turn(), [start(), end(2, "ok", "actual", true)], [], {
  facts: { run: { id: "run-1" }, tools: [{ call_id: "internal-1", provider_call_id: "provider-1", step_index: 0, name: "lookup", arguments: { query: "x" }, status: "failed", result_available: true, value: "wrong" }] },
});
assert.deepEqual(contradictoryFacts, []);
checks += 3;

const completed = turn("completed", [
  { role: "user", content: "原问题", call_id: null, tool_calls: [] },
  { role: "assistant", content: "调用工具", call_id: null, tool_calls: [{ kind: "tool_call", id: "provider-1", name: "lookup", arguments: { query: "x" } }] },
  { role: "tool", content: "false", call_id: "provider-1", tool_calls: [] },
  { role: "assistant", content: "完成", call_id: null, tool_calls: [] },
]);
const canonical = projectTurn(completed, [], []);
assert.deepEqual(canonical.map(item => item.type), ["assistant", "tool", "assistant"]);
assert.equal(canonical[0]!.type === "assistant" && canonical[0]!.partial, false);
assert.equal(canonical[1]!.type, "tool");
if (canonical[1]!.type === "tool") {
  assert.equal(canonical[1]!.resultAvailable, true);
  assert.equal(canonical[1]!.result, false);
  assert.equal(canonical[1]!.state, "ok");
}
const canonicalConflict = projectTurn(completed, [start(), end(2, "ok", "not-false", true)], []);
assert.equal(canonicalConflict[1]!.type, "tool");
if (canonicalConflict[1]!.type === "tool") {
  assert.equal(canonicalConflict[1]!.state, "unknown");
  assert.equal(canonicalConflict[1]!.resultAvailable, false);
}
for (const value of [null, false, 0]) {
  const transcriptOnly = turn("completed", [
    { role: "user", content: "合成输入", call_id: null, tool_calls: [] },
    { role: "assistant", content: "调用工具", call_id: null, tool_calls: [{ kind: "tool_call", id: "provider-1", name: "lookup", arguments: { query: "x" } }] },
    { role: "tool", content: JSON.stringify(value), call_id: "provider-1", tool_calls: [] },
    { role: "assistant", content: "完成", call_id: null, tool_calls: [] },
  ]);
  const transcriptProjection = projectTurn(transcriptOnly, [], []);
  const tool = transcriptProjection.find(item => item.type === "tool");
  assert.equal(tool?.type, "tool");
  if (tool?.type === "tool") {
    assert.equal(tool.name, "lookup");
    assert.deepEqual(tool.arguments, { query: "x" });
    assert.equal(tool.resultAvailable, true);
    assert.equal(tool.result, value);
    assert.equal(tool.state, "ok");
    assert.equal(tool.stepIndex, -1);
  }
}
const failed = projectTurn(turn("failed", [
  { role: "user", content: "[不可信历史状态数据] 内部失败标记", call_id: null, tool_calls: [] },
]), [event(0, "text", { step_index: 0, text: "部分回答" })], []);
assert.equal(failed.length, 1);
assert.deepEqual(failed[0], { type: "assistant", key: "assistant:step:0", text: "部分回答", partial: true });
checks += 6;

console.log(JSON.stringify({ checks, projection: "run-scoped and read-only" }));
