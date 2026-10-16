/** 独立版本化Missing回退意图；只保存公共事实，写后逐字读回。 */
import { exact, restoreId, restorePath, sameRestoreValue } from "./missing-directory-restore-contract.js";
import { rollbackSha, verifyRollbackAction, verifyRollbackAttempt, verifyRollbackPreview, verifyRollbackProposal,
  type RollbackAction, type RollbackAttempt, type RollbackIdentity, type RollbackPreview, type RollbackProposal } from "./missing-directory-rollback-contract.js";

export type RollbackPending = { kind: "preview"; request: RollbackPreview; source_operation_id: string; root_path: string; published_fingerprint: string; absence_fingerprint: string; rejected: boolean }
  | { kind: "change"; request: RollbackAction; proposal: RollbackProposal; rejected: boolean }
  | { kind: "cancel"; proposal: RollbackProposal; rejected: boolean }
  | { kind: "reconcile"; attempt: RollbackAttempt; proposal: RollbackProposal; rejected: boolean };
export type RollbackRecovery = RollbackIdentity & { version: 1; selected_id: string | null; pending: RollbackPending | null };
const MAX_BYTES = 24_576;
function invalid(): never { throw new Error("缺失基线回退记录不可用，写操作已停用"); }
function hasRejected(value: unknown): boolean { return typeof value === "object" && value !== null && typeof (value as Record<string, unknown>).rejected === "boolean"; }
export function verifyRollbackRecovery(value: unknown, identity: RollbackIdentity): asserts value is RollbackRecovery {
  if (!exact(value, ["version", "scope_id", "workspace_key", "resource_fingerprint", "selected_id", "pending"]) || value.version !== 1
    || value.scope_id !== identity.scope_id || value.workspace_key !== identity.workspace_key || value.resource_fingerprint !== identity.resource_fingerprint
    || value.selected_id !== null && !restoreId(value.selected_id)) invalid();
  const pending = value.pending; if (pending === null) return;
  if (!hasRejected(pending)) invalid();
  if (exact(pending, ["kind", "request", "source_operation_id", "root_path", "published_fingerprint", "absence_fingerprint", "rejected"]) && pending.kind === "preview") {
    verifyRollbackPreview(pending.request);
    if (!restoreId(pending.source_operation_id) || !restorePath(pending.root_path) || !rollbackSha(pending.published_fingerprint) || !rollbackSha(pending.absence_fingerprint)) invalid();
  } else if (exact(pending, ["kind", "request", "proposal", "rejected"]) && pending.kind === "change") {
    verifyRollbackAction(pending.request); verifyRollbackProposal(pending.proposal, identity);
    if (pending.request.proposal_id !== value.selected_id || pending.proposal.move.proposal_id !== value.selected_id || pending.request.expected_revision !== pending.proposal.move.revision) invalid();
  } else if (exact(pending, ["kind", "proposal", "rejected"]) && pending.kind === "cancel") {
    verifyRollbackProposal(pending.proposal, identity); if (pending.proposal.move.proposal_id !== value.selected_id) invalid();
  } else if (exact(pending, ["kind", "attempt", "proposal", "rejected"]) && pending.kind === "reconcile") {
    verifyRollbackProposal(pending.proposal, identity); verifyRollbackAttempt(pending.attempt, identity, pending.proposal);
    if (pending.proposal.move.proposal_id !== value.selected_id) invalid();
  } else invalid();
}
function storageKey(sessionId: string): string { if (!restoreId(sessionId)) invalid(); return `krim-missing-directory-rollback-recovery:${sessionId}`; }
export function readRollbackRecovery(sessionId: string, identity: RollbackIdentity): RollbackRecovery {
  if (identity.scope_id !== `session:${sessionId}`) invalid();
  try {
    const raw = localStorage.getItem(storageKey(sessionId));
    if (raw === null) return { version: 1, ...identity, selected_id: null, pending: null };
    if (new TextEncoder().encode(raw).length > MAX_BYTES) invalid();
    const value: unknown = JSON.parse(raw); verifyRollbackRecovery(value, identity); return value;
  } catch { invalid(); }
}
export function saveRollbackRecovery(sessionId: string, value: RollbackRecovery, identity: RollbackIdentity): void {
  if (identity.scope_id !== `session:${sessionId}`) invalid(); verifyRollbackRecovery(value, identity);
  try {
    const raw = JSON.stringify(value); if (new TextEncoder().encode(raw).length > MAX_BYTES) invalid();
    localStorage.setItem(storageKey(sessionId), raw);
    const stored = localStorage.getItem(storageKey(sessionId)); if (stored !== raw) invalid();
    const actual: unknown = JSON.parse(stored); verifyRollbackRecovery(actual, identity);
    if (!sameRestoreValue(actual, value)) invalid();
  } catch { invalid(); }
}
