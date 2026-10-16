/** 只持久保存原请求与安全身份；写后读回失败即停止新的写入意图。 */
import { exact, restoreId, restorePath, sameRestoreValue, verifyRestoreExecute, verifyRestorePreview, } from "./missing-directory-restore-contract.js";
const SHA = /^[a-f0-9]{64}$/, MAX_BYTES = 24_576;
function sha(value) { return typeof value === "string" && SHA.test(value); }
function invalid() { throw new Error("缺失目录恢复记录不可用，新预览和发布已停用"); }
export function verifyRestoreRecovery(value, identity) {
    if (!exact(value, ["version", "scope_id", "workspace_key", "resource_fingerprint", "selected_id", "pending"]) || value.version !== 1
        || value.scope_id !== identity.scope_id || value.workspace_key !== identity.workspace_key || value.resource_fingerprint !== identity.resource_fingerprint
        || value.selected_id !== null && !restoreId(value.selected_id))
        invalid();
    if (value.pending === null)
        return;
    const pending = value.pending;
    if (exact(pending, ["kind", "request", "root_path", "target_fingerprint", "rejected"]) && pending.kind === "preview") {
        if (!restorePath(pending.root_path) || !sha(pending.target_fingerprint) || typeof pending.rejected !== "boolean")
            invalid();
        verifyRestorePreview(pending.request);
    }
    else {
        if (!exact(pending, ["kind", "request", "root_path", "target_fingerprint", "absence_fingerprint", "rejected"]) || pending.kind !== "execute"
            || !restorePath(pending.root_path) || !sha(pending.target_fingerprint) || !sha(pending.absence_fingerprint) || typeof pending.rejected !== "boolean")
            invalid();
        verifyRestoreExecute(pending.request);
    }
}
function storageKey(sessionId) {
    if (!restoreId(sessionId))
        invalid();
    return `krim-missing-directory-restore-recovery:${sessionId}`;
}
export function readRestoreRecovery(sessionId, identity) {
    if (identity.scope_id !== `session:${sessionId}`)
        invalid();
    try {
        const raw = localStorage.getItem(storageKey(sessionId));
        if (raw === null)
            return { version: 1, ...identity, selected_id: null, pending: null };
        if (new TextEncoder().encode(raw).length > MAX_BYTES)
            invalid();
        const value = JSON.parse(raw);
        verifyRestoreRecovery(value, identity);
        return value;
    }
    catch {
        invalid();
    }
}
export function saveRestoreRecovery(sessionId, value, identity) {
    if (identity.scope_id !== `session:${sessionId}`)
        invalid();
    verifyRestoreRecovery(value, identity);
    try {
        const raw = JSON.stringify(value);
        if (new TextEncoder().encode(raw).length > MAX_BYTES)
            invalid();
        localStorage.setItem(storageKey(sessionId), raw);
        const stored = localStorage.getItem(storageKey(sessionId));
        if (stored !== raw)
            invalid();
        const actual = JSON.parse(stored);
        verifyRestoreRecovery(actual, identity);
        if (!sameRestoreValue(actual, value))
            invalid();
    }
    catch {
        invalid();
    }
}
