/** 严格Missing回退公共事实/微秒/过滤空页/可靠记录的安全合约。 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { verifyRollbackProposal, verifyRollbackProposalTransition, verifyRollbackAttempt, verifyRollbackQuery } from "../../src/krim_agent/web/missing-directory-rollback-contract.js";
import { readRollbackRecovery, saveRollbackRecovery } from "../../src/krim_agent/web/missing-directory-rollback-recovery.js";
const identity = { scope_id: "session:agent", workspace_key: "files", resource_fingerprint: "a".repeat(64) };
const proposal = {
  source: "workspace_missing_directory_baseline_rollback", source_plan_id: "source-plan", source_operation_id: "source-operation", source_hash: "b".repeat(64), baseline: { kind: "absent", fingerprint: "c".repeat(64) }, published_fingerprint: "d".repeat(64), approved_fingerprint: "e".repeat(64), rollback_state: "awaiting_approval",
  move: { proposal_id: "proposal", scope_id: identity.scope_id, workspace_key: identity.workspace_key, resource_fingerprint: identity.resource_fingerprint, source_path: "tree", target_path: "preserved", before_fingerprint: "e".repeat(64), entry_count: 3, byte_size: 4, revision: 0, status: "proposed", expires_at: 2000000000, active_operation_id: null, created_at: "2026-10-16T00:00:00.000001Z", updated_at: "2026-10-16T00:00:00.000002Z" },
};
const receipt = { source_path: "tree", target_path: "preserved", status: "done", before_fingerprint: proposal.approved_fingerprint, after_fingerprint: "f".repeat(64), changed: true, error_code: null, recovery_required: false };
const attempt = { source_plan_id: proposal.source_plan_id, source_operation_id: proposal.source_operation_id, action: "return_to_absence", operation: { operation_id: "operation", proposal_id: "proposal", scope_id: identity.scope_id, action: "apply", expected_revision: 0, status: "done", receipt, cancelled: false, recovery_required: false, created_at: proposal.move.created_at, updated_at: proposal.move.updated_at } };
test("当前approved可与原published不同，完整done保留原来源", () => { verifyRollbackProposal(proposal, identity); verifyRollbackAttempt(attempt, identity, proposal); });
for (const fault of ["scope", "generated", "absent", "fingerprint", "overlap", "private", "calendar", "state"]) test(`提案拒绝${fault}`, () => {
  const value = structuredClone(proposal);
  if (fault === "scope") value.move.scope_id = "session:other";
  if (fault === "generated") value.source = "workspace_generated_tree_undo";
  if (fault === "absent") value.baseline.kind = "present";
  if (fault === "fingerprint") value.approved_fingerprint = value.published_fingerprint;
  if (fault === "overlap") value.move.target_path = "tree/nested";
  if (fault === "private") value.move.token = "private";
  if (fault === "calendar") value.move.created_at = "2026-02-30T00:00:00Z";
  if (fault === "state") value.rollback_state = "preserved";
  assert.throws(() => verifyRollbackProposal(value, identity));
});
for (const fault of ["missing", "after", "changed", "recovery", "source", "action"]) test(`操作拒绝${fault}`, () => {
  const value = structuredClone(attempt);
  if (fault === "missing") value.operation.receipt = null;
  if (fault === "after") value.operation.receipt.after_fingerprint = null;
  if (fault === "changed") value.operation.receipt.changed = false;
  if (fault === "recovery") value.operation.recovery_required = true;
  if (fault === "source") value.source_operation_id = "other";
  if (fault === "action") value.action = "restore_tree";
  assert.throws(() => verifyRollbackAttempt(value, identity, proposal));
});
test("同版本事实不得变化，微秒更新时间不得后退", () => {
  for (const update of [{ move: { ...proposal.move, updated_at: "2026-10-16T00:00:00.000001Z" } }, { source_hash: "f".repeat(64) }, { move: { ...proposal.move, status: "cancelled" }, rollback_state: "cancelled" }]) assert.throws(() => verifyRollbackProposalTransition(proposal, { ...proposal, ...update }));
});
test("合法空过滤页继续，游标不能不前进/后退/越界", () => {
  const empty = { kind: "proposals", proposal: null, operation: null, history: null, proposals: { items: [], next_after: "cursor1" } };
  verifyRollbackQuery(empty, identity, { kind: "proposals", after: "cursor0", limit: 4 });
  for (const cursor of ["cursor0", "../wrong", true, "before"]) assert.throws(() => verifyRollbackQuery({ ...empty, proposals: { items: [], next_after: cursor } }, identity, { kind: "proposals", after: "cursor0", limit: 4 }));
});
test("可靠意图绑定原版本/Scope/资源，必须写后读回", () => {
  const memory = new Map(); globalThis.localStorage = { getItem: key => memory.get(key) ?? null, setItem: (key, value) => memory.set(key, value) };
  const recovery = { version: 1, ...identity, selected_id: "proposal", pending: { kind: "change", request: { proposal_id: "proposal", action: "return_to_absence", expected_revision: 0, request_key: "original-change" }, proposal, rejected: false } };
  saveRollbackRecovery("agent", recovery, identity); assert.deepEqual(readRollbackRecovery("agent", identity), recovery);
  assert.throws(() => readRollbackRecovery("other", identity));
  assert.throws(() => saveRollbackRecovery("agent", { ...recovery, resource_fingerprint: "f".repeat(64) }, identity));
  globalThis.localStorage.setItem = () => {}; memory.clear(); assert.throws(() => saveRollbackRecovery("agent", recovery, identity));
});
for (const fault of ["json", "identity", "private", "action", "proposal", "oversize"]) test(`坏存储拒绝${fault}`, () => {
  const value = { version: 1, ...identity, selected_id: "proposal", pending: { kind: "change", request: { proposal_id: "proposal", action: "return_to_absence", expected_revision: 0, request_key: "original-change" }, proposal: structuredClone(proposal), rejected: false } };
  if (fault === "identity") value.scope_id = "session:other";
  if (fault === "private") value.token = "private";
  if (fault === "action") value.pending.request.action = "remove_root";
  if (fault === "proposal") value.pending.proposal.move.proposal_id = "other";
  const raw = fault === "json" ? "{bad" : fault === "oversize" ? "x".repeat(24577) : JSON.stringify(value);
  globalThis.localStorage = { getItem: () => raw, setItem: () => {} }; assert.throws(() => readRollbackRecovery("agent", identity));
});
