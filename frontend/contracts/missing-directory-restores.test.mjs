/** 跨语言Missing事实与可靠游标/存储边界；样本为合成公共协议，不替代真实文件系统验收。 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { verifyRestoreView, verifyRestoreDiff, verifyRestoreOperation, verifyRestoreTransition } from "../../src/krim_agent/web/missing-directory-restore-contract.js";
import { readRestoreRecovery, saveRestoreRecovery } from "../../src/krim_agent/web/missing-directory-restore-recovery.js";

const identity = { scope_id: "session:actor", workspace_key: "files", resource_fingerprint: "a".repeat(64) };
const baseline = { kind: "absent", fingerprint: "c".repeat(64) };
const plan = {
  ...identity, source: "workspace_missing_directory_restore", plan_id: "original-plan", target: { kind: "checkpoint", identifier: "target" }, root_path: "tree",
  target_snapshot: { snapshot_fingerprint: "b".repeat(64), entry_count: 3, directory_count: 1, file_count: 2, byte_size: 4 }, baseline,
  request_key: "preview", execute_key: null, revision: 1, phase: "ready", status: "ready", operation_id: "original-plan", receipt: null,
  cleanup_pending: false, cancel_requested: false, expires_at: 2_000_000_000, error_code: null, created_at: "2026-10-16T01:00:00.000001Z", updated_at: "2026-10-16T01:00:00.000002Z",
};
const directory = { kind: "directory", mode: 0o755, uid: 1, gid: 1, byte_size: 0, sha256: null };
const file = { kind: "file", mode: 0o640, uid: 1, gid: 1, byte_size: 2, sha256: "d".repeat(64) };
const page = { ...identity, source: "workspace_missing_directory_restore_diff", plan_id: plan.plan_id, root_path: "tree", target_fingerprint: plan.target_snapshot.snapshot_fingerprint,
  baseline, offset: 0, limit: 8, total: 3, next_offset: 2,
  entries: [{ path: "tree", operation: "create", before: null, after: directory }, { path: "tree/甲", operation: "create", before: null, after: file }],
};

test("真实absent形状及短页剩余节点游标可读", () => {
  verifyRestoreView(plan, identity); verifyRestoreDiff(page, plan, 0, 8);
  verifyRestoreDiff({ ...page, offset: 2, entries: [{ path: "tree/乙", operation: "create", before: null, after: file }], next_offset: null }, plan, 2, 8);
});

for (const fault of ["source", "full-backup", "present", "private", "scope", "calendar", "unverified-finished", "wrong-operation"]) {
  test(`Missing计划拒绝${fault}`, () => {
    const value = structuredClone(plan);
    if (fault === "source") value.source = "workspace_directory_restore";
    if (fault === "full-backup") value.baseline = plan.target_snapshot;
    if (fault === "present") value.baseline.kind = "present";
    if (fault === "private") value.token = "private";
    if (fault === "scope") value.scope_id = "session:other";
    if (fault === "calendar") value.created_at = "2026-02-30T00:00:00Z";
    if (fault === "unverified-finished") { value.phase = "finished"; value.status = "done"; }
    if (fault === "wrong-operation") value.operation_id = "other-operation";
    assert.throws(() => verifyRestoreView(value, identity));
  });
}

for (const fault of ["next-null", "next-skip", "next-bool", "total", "empty", "before", "delete", "outside", "order", "root-type"]) {
  test(`Missing差异拒绝${fault}`, () => {
    const value = structuredClone(page);
    if (fault === "next-null") value.next_offset = null;
    if (fault === "next-skip") value.next_offset = 3;
    if (fault === "next-bool") value.next_offset = true;
    if (fault === "total") value.total = 4;
    if (fault === "empty") { value.entries = []; value.next_offset = 0; }
    if (fault === "before") value.entries[1].before = file;
    if (fault === "delete") value.entries[1].operation = "delete";
    if (fault === "outside") value.entries[1].path = "other/file";
    if (fault === "order") value.entries.reverse();
    if (fault === "root-type") value.entries[0].after = file;
    assert.throws(() => verifyRestoreDiff(value, plan, 0, 8));
  });
}

test("独立execute不能伪装prepare或已证实完成", () => {
  const operation = { operation_id: "execution", plan_id: plan.plan_id, scope_id: identity.scope_id, kind: "execute", request_key: "execute", status: "running", intent_recorded: false, receipt: null, error_code: null, created_at: plan.created_at, updated_at: plan.updated_at };
  verifyRestoreOperation(operation, identity.scope_id);
  assert.throws(() => verifyRestoreOperation({ ...operation, kind: "prepare", operation_id: plan.plan_id }, identity.scope_id));
  assert.throws(() => verifyRestoreOperation({ ...operation, status: "done" }, identity.scope_id));
});

test("已见absence和微秒时间不可回退", () => {
  assert.throws(() => verifyRestoreTransition(plan, { ...plan, baseline: { ...baseline, fingerprint: "d".repeat(64) } }));
  assert.throws(() => verifyRestoreTransition(plan, { ...plan, updated_at: plan.created_at }));
});

test("独立恢复记录写后读回，不读取旧完整家族记录", () => {
  const values = new Map([["krim-directory-restore-recovery:actor", "{old invalid full family}"]]);
  globalThis.localStorage = { getItem: key => values.get(key) ?? null, setItem: (key, value) => values.set(key, value) };
  const value = readRestoreRecovery("actor", identity);
  value.pending = { kind: "preview", request: { target: plan.target, request_key: "原键/?", ttl_seconds: 3600 }, root_path: "tree", target_fingerprint: plan.target_snapshot.snapshot_fingerprint, rejected: false };
  saveRestoreRecovery("actor", value, identity);
  assert.deepEqual(readRestoreRecovery("actor", identity), value);
  assert.equal(values.get("krim-directory-restore-recovery:actor"), "{old invalid full family}");
  assert.throws(() => readRestoreRecovery("actor", { ...identity, resource_fingerprint: "d".repeat(64) }));
  globalThis.localStorage.setItem = () => {};
  assert.throws(() => saveRestoreRecovery("actor", { ...value, selected_id: "different" }, identity));
});
