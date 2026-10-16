/** 完整目录恢复公共事实；只接收原身份与安全摘要，不接收私有材料。 */
export type RestoreTarget = { kind: "checkpoint" | "baseline"; identifier: string };
export type RestorePreview = { target: RestoreTarget; request_key: string; ttl_seconds: number };
export type RestoreExecute = { plan_id: string; expected_revision: number; request_key: string };
export type RestoreIdentity = { scope_id: string; workspace_key: string; resource_fingerprint: string };
export type RestoreSummary = { snapshot_fingerprint: string; entry_count: number; file_count: number; directory_count: number; byte_size: number };
export type RestorePhase = "preparing" | "ready" | "executing" | "finished" | "failed" | "cancelled" | "interrupted" | "expired" | "uncertain";
export type RestoreReceipt = {
  root_path: string; status: "done" | "rejected" | "uncertain"; before_fingerprint: string; target_fingerprint: string;
  after_fingerprint: string | null; retained_fingerprint: string | null; changed: boolean; error_code: string | null; recovery_required: boolean;
};
export type RestoreView = RestoreIdentity & {
  source: "workspace_directory_restore"; plan_id: string; target: RestoreTarget; root_path: string;
  target_snapshot: RestoreSummary; backup: RestoreSummary | null; request_key: string; execute_key: string | null;
  revision: number; phase: RestorePhase; status: "preparing" | "ready" | "running" | "done" | "failed" | "cancelled" | "interrupted" | "expired" | "uncertain";
  operation_id: string; receipt: RestoreReceipt | null; cleanup_pending: boolean; cancel_requested: boolean;
  expires_at: number; error_code: string | null; created_at: string; updated_at: string;
};
export type RestoreOperation = {
  operation_id: string; plan_id: string; scope_id: string; kind: "prepare" | "execute"; request_key: string;
  status: "running" | "done" | "rejected" | "uncertain" | "cancelled" | "interrupted" | "expired";
  intent_recorded: boolean; receipt: RestoreReceipt | null; error_code: string | null; created_at: string; updated_at: string;
};
export type RestoreNode = { kind: "file" | "directory"; mode: number; uid: number; gid: number; byte_size: number; sha256: string | null };
export type RestoreDiffEntry = { path: string; operation: "create" | "delete" | "modify" | "replace"; before: RestoreNode | null; after: RestoreNode | null };
export type RestoreDiffPage = RestoreIdentity & {
  plan_id: string; root_path: string; target_fingerprint: string; baseline_fingerprint: string;
  offset: number; limit: number; entries: RestoreDiffEntry[];
};

const ID = /^[A-Za-z0-9][A-Za-z0-9_-]{0,35}$/, SHA = /^[a-f0-9]{64}$/;
const MAX = Number.MAX_SAFE_INTEGER;
export function restoreId(value: unknown): value is string { return typeof value === "string" && ID.test(value); }
export function restoreKey(value: unknown): value is string { return typeof value === "string" && value.length > 0 && Array.from(value).length <= 128 && !/[\x00-\x1f\x7f\uD800-\uDFFF]/u.test(value); }
export function restorePath(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && new TextEncoder().encode(value).length <= 512
    && !/[\\:\x00-\x1f\x7f\uD800-\uDFFF]/u.test(value) && value.split("/").every(part => !["", ".", ".."].includes(part));
}
export function exact(value: unknown, fields: string[]): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    && Object.keys(value).length === fields.length && Object.keys(value).every(field => fields.includes(field));
}
function integer(value: unknown, min: number, max: number): value is number { return typeof value === "number" && Number.isSafeInteger(value) && value >= min && value <= max; }
function fingerprint(value: unknown): value is string { return typeof value === "string" && SHA.test(value); }
function errorCode(value: unknown): value is string { return typeof value === "string" && value.length > 0 && value.length <= 128; }
function target(value: unknown): value is RestoreTarget { return exact(value, ["kind", "identifier"]) && ["checkpoint", "baseline"].includes(String(value.kind)) && restoreId(value.identifier); }
function summary(value: unknown): value is RestoreSummary {
  return exact(value, ["snapshot_fingerprint", "entry_count", "file_count", "directory_count", "byte_size"])
    && fingerprint(value.snapshot_fingerprint) && integer(value.entry_count, 1, 1024) && integer(value.file_count, 0, 1023)
    && integer(value.directory_count, 1, 1024) && value.entry_count === value.file_count + value.directory_count
    && integer(value.byte_size, 0, 33_554_432) && (value.file_count !== 0 || value.byte_size === 0);
}
/** 按真实日历和微秒验证，避免Date.parse自动归一化非法日期或丢失时间回退。 */
function timestamp(value: unknown): bigint | null {
  if (typeof value !== "string") return null;
  const parts = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?(Z|[+-]\d{2}:\d{2})$/.exec(value);
  if (!parts) return null;
  const [year, month, day, hour, minute, second] = parts.slice(1, 7).map(Number);
  const days = [31, year! % 4 === 0 && (year! % 100 !== 0 || year! % 400 === 0) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  if (year! < 1 || month! < 1 || month! > 12 || day! < 1 || day! > days[month! - 1]! || hour! > 23 || minute! > 59 || second! > 59) return null;
  const seconds = Date.parse(value.replace(/\.\d+(?=Z|[+-]\d{2}:\d{2}$)/, ""));
  return Number.isFinite(seconds) ? BigInt(seconds) * 1000n + BigInt((parts[7] || "").padEnd(6, "0")) : null;
}
function dates(created: unknown, updated: unknown): boolean { const first = timestamp(created), last = timestamp(updated); return first !== null && last !== null && last >= first; }
function fail(): never { throw new Error("目录恢复回程与原事实不一致"); }
export function sameRestoreValue(left: unknown, right: unknown): boolean {
  if (left === right) return true;
  if (Array.isArray(left) && Array.isArray(right)) return left.length === right.length && left.every((value, index) => sameRestoreValue(value, right[index]));
  if (typeof left !== "object" || left === null || Array.isArray(left) || typeof right !== "object" || right === null || Array.isArray(right)) return false;
  const a = left as Record<string, unknown>, b = right as Record<string, unknown>;
  return Object.keys(a).length === Object.keys(b).length && Object.keys(a).every(key => Object.hasOwn(b, key) && sameRestoreValue(a[key], b[key]));
}
export function verifyRestorePreview(value: unknown): asserts value is RestorePreview {
  if (!exact(value, ["target", "request_key", "ttl_seconds"]) || !target(value.target) || !restoreKey(value.request_key)
    || typeof value.ttl_seconds !== "number" || !Number.isFinite(value.ttl_seconds) || value.ttl_seconds <= 0 || value.ttl_seconds > 86400) fail();
}
export function verifyRestoreExecute(value: unknown): asserts value is RestoreExecute {
  if (!exact(value, ["plan_id", "expected_revision", "request_key"]) || !restoreId(value.plan_id) || !integer(value.expected_revision, 0, MAX) || !restoreKey(value.request_key)) fail();
}
function receipt(value: unknown): value is RestoreReceipt {
  if (!exact(value, ["root_path", "status", "before_fingerprint", "target_fingerprint", "after_fingerprint", "retained_fingerprint", "changed", "error_code", "recovery_required"])
    || !restorePath(value.root_path) || !fingerprint(value.before_fingerprint) || !fingerprint(value.target_fingerprint) || typeof value.changed !== "boolean" || typeof value.recovery_required !== "boolean") return false;
  if (value.status === "done") return value.changed && !value.recovery_required && value.error_code === null && fingerprint(value.after_fingerprint) && fingerprint(value.retained_fingerprint);
  return ["rejected", "uncertain"].includes(String(value.status)) && errorCode(value.error_code) && value.after_fingerprint === null && value.retained_fingerprint === null
    && value.changed === (value.status === "uncertain") && value.recovery_required === (value.status === "uncertain");
}
export function verifyRestoreView(value: unknown, identity: RestoreIdentity): asserts value is RestoreView {
  if (!exact(value, ["source", "plan_id", "scope_id", "target", "root_path", "workspace_key", "resource_fingerprint", "target_snapshot", "backup", "request_key", "execute_key", "revision", "phase", "status", "operation_id", "receipt", "cleanup_pending", "cancel_requested", "expires_at", "error_code", "created_at", "updated_at"])
    || value.source !== "workspace_directory_restore" || !restoreId(value.plan_id) || !target(value.target) || !restorePath(value.root_path)
    || value.scope_id !== identity.scope_id || value.workspace_key !== identity.workspace_key || value.resource_fingerprint !== identity.resource_fingerprint || !fingerprint(value.resource_fingerprint)
    || !summary(value.target_snapshot) || value.backup !== null && !summary(value.backup) || !restoreKey(value.request_key) || value.execute_key !== null && !restoreKey(value.execute_key)
    || !integer(value.revision, 0, MAX) || !restoreId(value.operation_id) || typeof value.cleanup_pending !== "boolean" || typeof value.cancel_requested !== "boolean"
    || typeof value.expires_at !== "number" || !Number.isFinite(value.expires_at) || value.expires_at <= 0 || value.expires_at > MAX || !dates(value.created_at, value.updated_at)) fail();
  const phases = ["preparing", "ready", "executing", "finished", "failed", "cancelled", "interrupted", "expired", "uncertain"];
  if (!phases.includes(String(value.phase)) || value.status !== (value.phase === "executing" ? "running" : value.phase === "finished" ? "done" : value.phase)
    || (["preparing", "ready", "executing", "finished"].includes(String(value.phase)) ? value.error_code !== null : !errorCode(value.error_code))
    || value.phase === "cancelled" && !value.cancel_requested) fail();
  if (value.execute_key === null) { if (value.operation_id !== value.plan_id || value.receipt !== null || value.cleanup_pending) fail(); }
  else if (value.operation_id === value.plan_id || value.backup === null || value.revision < 2) fail();
  if (value.phase === "preparing" && value.revision !== 0 || value.phase === "ready" && (value.revision < 1 || value.execute_key !== null)
    || ["ready", "executing", "finished", "uncertain"].includes(String(value.phase)) && value.backup === null
    || ["executing", "finished", "uncertain"].includes(String(value.phase)) && value.execute_key === null) fail();
  if (value.receipt !== null) {
    if (!receipt(value.receipt) || value.backup === null || value.receipt.root_path !== value.root_path || value.receipt.target_fingerprint !== value.target_snapshot.snapshot_fingerprint
      || value.receipt.before_fingerprint !== value.backup.snapshot_fingerprint || value.phase !== (value.receipt.status === "done" ? "finished" : value.receipt.status === "rejected" ? "failed" : "uncertain")) fail();
  } else if (value.phase === "finished") fail();
}
export function verifyRestoreTransition(before: RestoreView, after: RestoreView): void {
  const fields = ["source", "plan_id", "scope_id", "root_path", "workspace_key", "resource_fingerprint", "request_key", "expires_at", "created_at"] as const;
  if (fields.some(field => before[field] !== after[field]) || !sameRestoreValue(before.target, after.target) || !sameRestoreValue(before.target_snapshot, after.target_snapshot)
    || before.backup !== null && !sameRestoreValue(before.backup, after.backup) || before.cancel_requested && !after.cancel_requested
    || before.execute_key !== null && (before.execute_key !== after.execute_key || before.operation_id !== after.operation_id)
    || after.revision < before.revision || before.phase !== after.phase && after.revision <= before.revision
    || timestamp(after.updated_at)! < timestamp(before.updated_at)!) fail();
  const next: Record<RestorePhase, RestorePhase[]> = {
    preparing: ["preparing", "ready", "executing", "finished", "uncertain", "failed", "cancelled", "interrupted", "expired"],
    ready: ["ready", "executing", "finished", "failed", "cancelled", "interrupted", "expired", "uncertain"],
    executing: ["executing", "finished", "failed", "cancelled", "interrupted", "expired", "uncertain"],
    uncertain: ["uncertain", "executing", "finished", "failed"],
    finished: ["finished"], failed: ["failed"], cancelled: ["cancelled"], interrupted: ["interrupted"], expired: ["expired"],
  };
  if (!next[before.phase].includes(after.phase) || ["finished", "failed", "cancelled", "interrupted", "expired"].includes(before.phase) && !sameRestoreValue(before, after)) fail();
}
export function verifyRestoreOperation(value: unknown, scope: string): asserts value is RestoreOperation {
  if (!exact(value, ["operation_id", "plan_id", "scope_id", "kind", "request_key", "status", "intent_recorded", "receipt", "error_code", "created_at", "updated_at"])
    || !restoreId(value.operation_id) || !restoreId(value.plan_id) || value.scope_id !== scope || !restoreKey(value.request_key) || typeof value.intent_recorded !== "boolean"
    || !["prepare", "execute"].includes(String(value.kind)) || !["running", "done", "rejected", "uncertain", "cancelled", "interrupted", "expired"].includes(String(value.status))
    || (value.kind === "prepare") !== (value.operation_id === value.plan_id) || !dates(value.created_at, value.updated_at)
    || (["running", "done"].includes(String(value.status)) ? value.error_code !== null : !errorCode(value.error_code))) fail();
  if (value.kind === "prepare" && (value.intent_recorded || value.receipt !== null) || value.status === "uncertain" && (value.kind !== "execute" || !value.intent_recorded)) fail();
  if (value.receipt !== null && (!receipt(value.receipt) || value.receipt.status !== value.status)) fail();
  if (value.kind === "execute" && value.status === "done" && (!value.intent_recorded || value.receipt === null || value.receipt.status !== "done")) fail();
}
function diffNode(value: unknown): value is RestoreNode {
  return exact(value, ["kind", "mode", "uid", "gid", "byte_size", "sha256"]) && integer(value.mode, 0, 0o777)
    && integer(value.uid, 0, 4_294_967_294) && integer(value.gid, 0, 4_294_967_294) && integer(value.byte_size, 0, 8_388_608)
    && (value.kind === "file" && fingerprint(value.sha256) || value.kind === "directory" && value.byte_size === 0 && value.sha256 === null);
}
/** 后端按Unicode码点排序；UTF-16默认sort会颠倒部分非BMP路径。 */
function comparePath(left: string, right: string): number {
  const a = Array.from(left, part => part.codePointAt(0)!), b = Array.from(right, part => part.codePointAt(0)!);
  for (let index = 0; index < Math.min(a.length, b.length); index++) if (a[index] !== b[index]) return a[index]! - b[index]!;
  return a.length - b.length;
}
export function verifyRestoreDiff(value: unknown, plan: RestoreView, offset: number, limit: number): asserts value is RestoreDiffPage {
  if (!exact(value, ["plan_id", "scope_id", "root_path", "workspace_key", "resource_fingerprint", "target_fingerprint", "baseline_fingerprint", "offset", "limit", "entries"])
    || value.plan_id !== plan.plan_id || value.scope_id !== plan.scope_id || value.root_path !== plan.root_path || value.workspace_key !== plan.workspace_key || value.resource_fingerprint !== plan.resource_fingerprint
    || value.target_fingerprint !== plan.target_snapshot.snapshot_fingerprint || plan.backup === null || value.baseline_fingerprint !== plan.backup.snapshot_fingerprint
    || !integer(value.offset, 0, 5000) || value.offset !== offset || !integer(value.limit, 1, 128) || value.limit !== limit || !Array.isArray(value.entries) || value.entries.length > limit) fail();
  let previous: string | null = null;
  for (const entry of value.entries) {
    if (!exact(entry, ["path", "operation", "before", "after"]) || !restorePath(entry.path) || !(entry.path === plan.root_path || entry.path.startsWith(plan.root_path + "/"))
      || previous !== null && comparePath(previous, entry.path) >= 0 || entry.before !== null && !diffNode(entry.before) || entry.after !== null && !diffNode(entry.after)
      || sameRestoreValue(entry.before, entry.after)) fail();
    const expected = entry.before === null ? "create" : entry.after === null ? "delete" : entry.before.kind !== entry.after.kind ? "replace" : "modify";
    if (entry.operation !== expected) fail();
    previous = entry.path;
  }
}
