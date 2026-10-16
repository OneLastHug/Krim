/** 只持久保存原请求与安全身份；写后读回失败即停止新的写入意图。 */
import {
  exact, restoreId, restorePath, sameRestoreValue, verifyRestoreExecute, verifyRestorePreview,
  type RestoreExecute, type RestoreIdentity, type RestorePreview,
} from "./directory-restore-contract.js";

export type RestoreTargetSelection = { target: RestorePreview["target"]; root_path: string; target_fingerprint: string };
export type RestorePreviewPending = { kind: "preview"; request: RestorePreview; root_path: string; target_fingerprint: string; rejected: boolean };
export type RestoreExecutePending = { kind: "execute"; request: RestoreExecute; root_path: string; target_fingerprint: string; baseline_fingerprint: string; rejected: boolean };
export type RestorePending = RestorePreviewPending | RestoreExecutePending;
export type RestoreRecovery = RestoreIdentity & { version: 1; selected_id: string | null; pending: RestorePending | null };
const SHA = /^[a-f0-9]{64}$/, MAX_BYTES = 24_576;
function sha(value: unknown): value is string { return typeof value === "string" && SHA.test(value); }
function invalid(): never { throw new Error("目录恢复记录不可用，新预览和发布已停用"); }

export function verifyRestoreRecovery(value: unknown, identity: RestoreIdentity): asserts value is RestoreRecovery {
  if (!exact(value, ["version", "scope_id", "workspace_key", "resource_fingerprint", "selected_id", "pending"]) || value.version !== 1
    || value.scope_id !== identity.scope_id || value.workspace_key !== identity.workspace_key || value.resource_fingerprint !== identity.resource_fingerprint
    || value.selected_id !== null && !restoreId(value.selected_id)) invalid();
  if (value.pending === null) return;
  const pending = value.pending;
  if (exact(pending, ["kind", "request", "root_path", "target_fingerprint", "rejected"]) && pending.kind === "preview") {
    if (!restorePath(pending.root_path) || !sha(pending.target_fingerprint) || typeof pending.rejected !== "boolean") invalid();
    verifyRestorePreview(pending.request);
  } else {
    if (!exact(pending, ["kind", "request", "root_path", "target_fingerprint", "baseline_fingerprint", "rejected"]) || pending.kind !== "execute"
      || !restorePath(pending.root_path) || !sha(pending.target_fingerprint) || !sha(pending.baseline_fingerprint) || typeof pending.rejected !== "boolean") invalid();
    verifyRestoreExecute(pending.request);
  }
}

function storageKey(sessionId: string): string {
  if (!restoreId(sessionId)) invalid();
  return `krim-directory-restore-recovery:${sessionId}`;
}
export function readRestoreRecovery(sessionId: string, identity: RestoreIdentity): RestoreRecovery {
  if (identity.scope_id !== `session:${sessionId}`) invalid();
  try {
    const raw = localStorage.getItem(storageKey(sessionId));
    if (raw === null) return { version: 1, ...identity, selected_id: null, pending: null };
    if (new TextEncoder().encode(raw).length > MAX_BYTES) invalid();
    const value: unknown = JSON.parse(raw);
    verifyRestoreRecovery(value, identity);
    return value;
  } catch { invalid(); }
}
export function saveRestoreRecovery(sessionId: string, value: RestoreRecovery, identity: RestoreIdentity): void {
  if (identity.scope_id !== `session:${sessionId}`) invalid();
  verifyRestoreRecovery(value, identity);
  try {
    const raw = JSON.stringify(value);
    if (new TextEncoder().encode(raw).length > MAX_BYTES) invalid();
    localStorage.setItem(storageKey(sessionId), raw);
    const stored = localStorage.getItem(storageKey(sessionId));
    if (stored !== raw) invalid();
    const actual: unknown = JSON.parse(stored);
    verifyRestoreRecovery(actual, identity);
    if (!sameRestoreValue(actual, value)) invalid();
  } catch { invalid(); }
}
