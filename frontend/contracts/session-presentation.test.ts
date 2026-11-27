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

// file_change_preview：预览事件先于完整参数到达，先建卡；数字为该调用的累计快照，整体覆盖。
const previewEvent = (seq: number, data: Record<string, unknown>, runId = "run-1") => event(seq, "file_change_preview", data, runId);
const writeArgs = { path: "src/app.ts", content: "完整内容" };
const fullPipeline = (extra: ReturnType<typeof event>[]) => projectTurn(turn(), [
  ...extra,
  event(30, "model_end", { step_index: 1, tool_calls: [{ kind: "tool_call", id: "provider-w", name: "write", arguments: writeArgs }] }),
  event(31, "tool_start", { call_id: "internal-w", provider_call_id: "provider-w", step_index: 1, name: "write", arguments: writeArgs }),
  event(32, "tool_intent", { call_id: "internal-w", step_index: 1, decision: "allow" }),
], []);

const generating = fullPipeline([
  previewEvent(10, { provider_call_id: "provider-w", step_index: 1, name: "write", path: "src/app.ts", revision: 0, lines_added: 4, lines_removed: null, complete: false }),
  previewEvent(11, { provider_call_id: "provider-w", step_index: 1, name: "write", path: "src/app.ts", revision: 1, lines_added: 9, lines_removed: 1, count_scope: "replacement", complete: true }),
]);
assert.equal(generating.length, 1);
if (generating[0]!.type === "tool") {
  assert.equal(generating[0]!.providerCallId, "provider-w");
  assert.equal(generating[0]!.name, "write");
  assert.deepEqual(generating[0]!.arguments, writeArgs);
  assert.equal(generating[0]!.state, "running");
  assert.deepEqual(generating[0]!.preview, { path: "src/app.ts", revision: 1, linesAdded: 9, linesRemoved: 1, countScope: "replacement", complete: true });
}
const previewOnly = projectTurn(turn(), [previewEvent(1, { provider_call_id: "provider-w", step_index: 1, name: "write", path: "src/app.ts", revision: 0, lines_added: 4, lines_removed: null, complete: false })], []);
assert.equal(previewOnly.length, 1);
if (previewOnly[0]!.type === "tool") {
  assert.equal(previewOnly[0]!.arguments, undefined);
  assert.equal(previewOnly[0]!.state, "preparing");
  assert.deepEqual(previewOnly[0]!.preview, { path: "src/app.ts", revision: 0, linesAdded: 4, linesRemoved: null, countScope: "file", complete: false });
}
checks += 2;

const finished = projectTurn(turn(), [
  previewEvent(10, { provider_call_id: "provider-w", step_index: 1, name: "write", path: "src/app.ts", revision: 3, lines_added: 12, lines_removed: 2, complete: true }),
  event(31, "tool_start", { call_id: "internal-w", provider_call_id: "provider-w", step_index: 1, name: "write", arguments: writeArgs }),
  event(32, "tool_end", { call_id: "internal-w", provider_call_id: "provider-w", step_index: 1, name: "write", arguments: writeArgs, status: "ok", executor_started: true, result_available: true, value: { ok: true, lines_added: 15, lines_removed: 3 } }),
], []);
assert.equal(finished.length, 1);
if (finished[0]!.type === "tool") {
  assert.equal(finished[0]!.state, "ok");
  assert.equal(finished[0]!.preview, undefined);
  assert.deepEqual(finished[0]!.result, { ok: true, lines_added: 15, lines_removed: 3 });
}
const failedPreview = projectTurn(turn(), [
  previewEvent(10, { provider_call_id: "provider-w", step_index: 1, name: "write", path: "src/app.ts", revision: 3, lines_added: 12, lines_removed: 2, complete: true }),
  event(31, "tool_start", { call_id: "internal-w", provider_call_id: "provider-w", step_index: 1, name: "write", arguments: writeArgs }),
  event(32, "tool_end", { call_id: "internal-w", provider_call_id: "provider-w", step_index: 1, name: "write", arguments: writeArgs, status: "failed", executor_started: true, result_available: false }),
], []);
if (failedPreview[0]!.type === "tool") {
  assert.equal(failedPreview[0]!.state, "failed");
  assert.equal(failedPreview[0]!.preview, undefined);
  assert.equal(failedPreview[0]!.resultAvailable, false);
}
checks += 2;

// 重放重复与乱序 revision：同 seq 重复投递去重，同 revision 重放幂等，迟到旧快照不回退。
const replayed = projectTurn(turn(), [
  previewEvent(10, { provider_call_id: "provider-r", step_index: 0, name: "edit", path: "a.md", revision: 2, lines_added: 5, lines_removed: 5, complete: false }),
  previewEvent(10, { provider_call_id: "provider-r", step_index: 0, name: "edit", path: "a.md", revision: 2, lines_added: 5, lines_removed: 5, complete: false }),
  previewEvent(11, { provider_call_id: "provider-r", step_index: 0, name: "edit", path: "a.md", revision: 2, lines_added: 5, lines_removed: 5, complete: false }),
], []);
assert.equal(replayed.length, 1);
if (replayed[0]!.type === "tool") assert.deepEqual(replayed[0]!.preview, { path: "a.md", revision: 2, linesAdded: 5, linesRemoved: 5, countScope: "file", complete: false });
const reordered = projectTurn(turn(), [
  previewEvent(10, { provider_call_id: "provider-r", step_index: 0, name: "edit", path: "a.md", revision: 5, lines_added: 9, lines_removed: 1, complete: false }),
  previewEvent(11, { provider_call_id: "provider-r", step_index: 0, name: "edit", path: "a.md", revision: 2, lines_added: 3, lines_removed: 0, complete: false }),
], []);
if (reordered[0]!.type === "tool") assert.equal(reordered[0]!.preview?.revision, 5);
checks += 2;

// 跨 run 忽略、跨调用隔离；未知增删保持 null 不伪造 0；非文件工具与非法字段不建卡。
const isolated = projectTurn(turn(), [
  previewEvent(1, { provider_call_id: "p-a", step_index: 0, name: "write", path: "a.ts", revision: 1, lines_added: 3, lines_removed: null, complete: false }),
  previewEvent(2, { provider_call_id: "p-b", step_index: 0, name: "edit", path: "b.ts", revision: 1, lines_added: null, lines_removed: 7, complete: false }),
  previewEvent(3, { provider_call_id: "p-a", step_index: 0, name: "write", path: "a.ts", revision: 2, lines_added: 99, lines_removed: 1, complete: false }, "other-run"),
], []);
assert.equal(isolated.length, 2);
if (isolated[0]!.type === "tool" && isolated[1]!.type === "tool") {
  assert.deepEqual(isolated[0]!.preview, { path: "a.ts", revision: 1, linesAdded: 3, linesRemoved: null, countScope: "file", complete: false });
  assert.deepEqual(isolated[1]!.preview, { path: "b.ts", revision: 1, linesAdded: null, linesRemoved: 7, countScope: "file", complete: false });
}
const rejected = projectTurn(turn(), [
  previewEvent(1, { provider_call_id: "p-s", step_index: 0, name: "shell", path: "x", revision: 0, lines_added: 1, lines_removed: 1, complete: true }),
  previewEvent(2, { provider_call_id: "p-v", step_index: 0, name: "write", path: "", revision: 0, lines_added: 1, lines_removed: 1, complete: true }),
  previewEvent(3, { provider_call_id: "p-v2", step_index: 0, name: "write", path: "v.ts", revision: -1, lines_added: 1, lines_removed: 1, complete: true }),
  previewEvent(4, { provider_call_id: "p-v3", step_index: 0, name: "write", path: "v.ts", revision: 0, lines_added: -3, lines_removed: "5", complete: true }),
], []);
assert.equal(rejected.length, 1);
if (rejected[0]!.type === "tool") {
  assert.deepEqual(rejected[0]!.preview, { path: "v.ts", revision: 0, linesAdded: null, linesRemoved: null, countScope: "file", complete: true });
}
checks += 2;

// dev_write 旧名属同一文件写入族：事件内一致使用时合并同一张卡（表外旧名不做前缀剥离）。
const legacyNamed = projectTurn(turn(), [
  previewEvent(1, { provider_call_id: "p-legacy", step_index: 0, name: "dev_write", path: "old.ts", revision: 0, lines_added: 2, lines_removed: null, complete: false }),
  event(2, "model_end", { step_index: 0, tool_calls: [{ kind: "tool_call", id: "p-legacy", name: "dev_write", arguments: { path: "old.ts", content: "c" } }] }),
], []);
assert.equal(legacyNamed.length, 1);
if (legacyNamed[0]!.type === "tool") {
  assert.equal(legacyNamed[0]!.name, "dev_write");
  assert.equal(legacyNamed[0]!.state, "preparing");
  assert.notEqual(legacyNamed[0]!.preview, undefined);
}
// replaces_provider_call_id：同 step 旧 id → 新 id 迁移同一张卡，不留两张卡。
const migrated = projectTurn(turn(), [
  previewEvent(1, { provider_call_id: "old-id", step_index: 1, name: "write", path: "m.ts", revision: 0, lines_added: 2, lines_removed: null, complete: false }),
  previewEvent(2, { provider_call_id: "new-id", replaces_provider_call_id: "old-id", step_index: 1, name: "write", path: "m.ts", revision: 1, lines_added: 6, lines_removed: 1, complete: false }),
  event(3, "model_end", { step_index: 1, tool_calls: [{ kind: "tool_call", id: "new-id", name: "write", arguments: { path: "m.ts", content: "c" } }] }),
], []);
assert.equal(migrated.length, 1);
if (migrated[0]!.type === "tool") {
  assert.equal(migrated[0]!.providerCallId, "new-id");
  assert.equal(migrated[0]!.preview?.revision, 1);
}
const migratedLate = projectTurn(turn(), [
  previewEvent(1, { provider_call_id: "old-id", step_index: 1, name: "write", path: "m.ts", revision: 0, lines_added: 2, lines_removed: null, complete: false }),
  event(2, "model_end", { step_index: 1, tool_calls: [{ kind: "tool_call", id: "new-id", name: "write", arguments: { path: "m.ts", content: "c" } }] }),
  previewEvent(3, { provider_call_id: "new-id", replaces_provider_call_id: "old-id", step_index: 1, name: "write", path: "m.ts", revision: 2, lines_added: 8, lines_removed: 1, complete: true }),
], []);
assert.equal(migratedLate.length, 1);
if (migratedLate[0]!.type === "tool") {
  assert.equal(migratedLate[0]!.providerCallId, "new-id");
  assert.equal(migratedLate[0]!.preview?.revision, 2);
  assert.equal(migratedLate[0]!.state, "preparing");
}
checks += 2;

// 本轮已终态却无最终工具结果：停止生成状态，数字回到未确认。
const settled = projectTurn(turn("cancelled"), [
  previewEvent(1, { provider_call_id: "p-c", step_index: 0, name: "write", path: "c.ts", revision: 1, lines_added: 8, lines_removed: 0, complete: true }),
], []);
assert.equal(settled.length, 1);
if (settled[0]!.type === "tool") {
  assert.equal(settled[0]!.state, "unknown");
  assert.equal(settled[0]!.preview, undefined);
  assert.equal(settled[0]!.previewPath, "c.ts");
  assert.equal(settled[0]!.resultAvailable, false);
}
checks++;

console.log(JSON.stringify({ checks, projection: "run-scoped and read-only" }));
