import { exact, restoreId, restoreKey, restorePath, sameRestoreValue } from "./missing-directory-restore-contract.js";
const states = { proposed: "awaiting_approval", applying: "preserving", applied: "preserved", undoing: "restoring", undone: "restored", keeping: "keeping", kept: "kept", rejected: "rejected", uncertain: "uncertain", cancelled: "cancelled" };
const actions = { return_to_absence: "apply", restore_tree: "undo", keep: "keep" };
const SHA = /^[a-f0-9]{64}$/;
export function rollbackFail() { throw new Error("缺失基线回退回程与原事实不一致"); }
export function rollbackSha(value) { return typeof value === "string" && SHA.test(value); }
function integer(value, min = 0, max = Number.MAX_SAFE_INTEGER) { return typeof value === "number" && Number.isSafeInteger(value) && value >= min && value <= max; }
function paths(source, target) { return restorePath(source) && restorePath(target) && source !== target && !source.startsWith(target + "/") && !target.startsWith(source + "/"); }
function stamp(value) {
    if (typeof value !== "string")
        return null;
    const parts = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?(Z|[+-]\d{2}:\d{2})$/.exec(value);
    if (!parts)
        return null;
    const [y, m, d, h, min, s] = parts.slice(1, 7).map(Number);
    const days = [31, y % 4 === 0 && (y % 100 !== 0 || y % 400 === 0) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if (y < 1 || m < 1 || m > 12 || d < 1 || d > days[m - 1] || h > 23 || min > 59 || s > 59)
        return null;
    const milliseconds = Date.parse(value.replace(/\.\d+(?=Z|[+-]\d{2}:\d{2}$)/, ""));
    return Number.isFinite(milliseconds) ? BigInt(milliseconds) * 1000n + BigInt((parts[7] || "").padEnd(6, "0")) : null;
}
function dates(created, updated) { const a = stamp(created), b = stamp(updated); return a !== null && b !== null && b >= a; }
export function verifyRollbackPreview(value) {
    if (!exact(value, ["plan_id", "preserve_path", "expected_fingerprint", "request_key", "ttl_seconds"]) || !restoreId(value.plan_id) || !restorePath(value.preserve_path)
        || !rollbackSha(value.expected_fingerprint) || !restoreKey(value.request_key) || typeof value.ttl_seconds !== "number" || !Number.isFinite(value.ttl_seconds) || value.ttl_seconds <= 0 || value.ttl_seconds > 86400)
        rollbackFail();
}
export function verifyRollbackAction(value) {
    if (!exact(value, ["proposal_id", "action", "expected_revision", "request_key"]) || !restoreId(value.proposal_id) || typeof value.action !== "string" || !Object.hasOwn(actions, value.action)
        || !integer(value.expected_revision, 0, Number.MAX_SAFE_INTEGER - 2) || !restoreKey(value.request_key))
        rollbackFail();
}
export function verifyRollbackProposal(value, identity) {
    if (!exact(value, ["source", "source_plan_id", "source_operation_id", "source_hash", "baseline", "published_fingerprint", "approved_fingerprint", "rollback_state", "move"])
        || value.source !== "workspace_missing_directory_baseline_rollback" || !restoreId(value.source_plan_id) || !restoreId(value.source_operation_id) || !rollbackSha(value.source_hash)
        || !exact(value.baseline, ["kind", "fingerprint"]) || value.baseline.kind !== "absent" || !rollbackSha(value.baseline.fingerprint) || !rollbackSha(value.published_fingerprint) || !rollbackSha(value.approved_fingerprint))
        rollbackFail();
    const move = value.move;
    if (!exact(move, ["proposal_id", "scope_id", "workspace_key", "resource_fingerprint", "source_path", "target_path", "before_fingerprint", "entry_count", "byte_size", "revision", "status", "expires_at", "active_operation_id", "created_at", "updated_at"])
        || !restoreId(move.proposal_id) || move.scope_id !== identity.scope_id || move.workspace_key !== identity.workspace_key || move.resource_fingerprint !== identity.resource_fingerprint
        || !paths(move.source_path, move.target_path) || move.before_fingerprint !== value.approved_fingerprint || !integer(move.entry_count, 1, 16384) || !integer(move.byte_size, 0, 134217728)
        || !integer(move.revision) || typeof move.status !== "string" || !Object.hasOwn(states, move.status) || states[move.status] !== value.rollback_state
        || typeof move.expires_at !== "number" || !Number.isFinite(move.expires_at) || move.expires_at <= 0 || move.active_operation_id !== null && !restoreId(move.active_operation_id) || !dates(move.created_at, move.updated_at))
        rollbackFail();
}
export function verifyRollbackProposalTransition(before, after) {
    const { status: _a, revision: _b, active_operation_id: _c, updated_at: _d, ...oldMove } = before.move;
    const { status: _e, revision: _f, active_operation_id: _g, updated_at: _h, ...newMove } = after.move;
    const { move: _i, rollback_state: _j, ...oldSource } = before, { move: _k, rollback_state: _l, ...newSource } = after;
    if (!sameRestoreValue(oldSource, newSource) || !sameRestoreValue(oldMove, newMove) || after.move.revision < before.move.revision || stamp(after.move.updated_at) < stamp(before.move.updated_at)
        || after.move.revision === before.move.revision && !sameRestoreValue(before, after)
        || ["cancelled", "rejected", "restored"].includes(before.rollback_state) && after.rollback_state !== before.rollback_state)
        rollbackFail();
}
export function verifyRollbackAttempt(value, identity, proposal) {
    if (!exact(value, ["source_plan_id", "source_operation_id", "action", "operation"]) || !restoreId(value.source_plan_id) || !restoreId(value.source_operation_id) || typeof value.action !== "string" || !Object.hasOwn(actions, value.action))
        rollbackFail();
    const op = value.operation;
    if (!exact(op, ["operation_id", "proposal_id", "scope_id", "action", "expected_revision", "status", "receipt", "cancelled", "recovery_required", "created_at", "updated_at"])
        || !restoreId(op.operation_id) || !restoreId(op.proposal_id) || op.scope_id !== identity.scope_id || op.action !== actions[value.action] || !integer(op.expected_revision, 0, Number.MAX_SAFE_INTEGER - 2)
        || !["running", "done", "rejected", "uncertain"].includes(String(op.status)) || typeof op.cancelled !== "boolean" || typeof op.recovery_required !== "boolean" || !dates(op.created_at, op.updated_at))
        rollbackFail();
    if (proposal && (value.source_plan_id !== proposal.source_plan_id || value.source_operation_id !== proposal.source_operation_id || op.proposal_id !== proposal.move.proposal_id || op.expected_revision > proposal.move.revision))
        rollbackFail();
    const receipt = op.receipt;
    if (receipt === null) {
        if (op.status === "done")
            rollbackFail();
        return;
    }
    if (!exact(receipt, ["source_path", "target_path", "status", "before_fingerprint", "after_fingerprint", "changed", "error_code", "recovery_required"])
        || !paths(receipt.source_path, receipt.target_path) || receipt.status !== op.status || !rollbackSha(receipt.before_fingerprint) || receipt.after_fingerprint !== null && !rollbackSha(receipt.after_fingerprint)
        || typeof receipt.changed !== "boolean" || receipt.recovery_required !== op.recovery_required || receipt.error_code !== null && (typeof receipt.error_code !== "string" || receipt.error_code.length < 1 || receipt.error_code.length > 128)
        || op.status === "done" && (receipt.after_fingerprint === null || receipt.error_code !== null || op.recovery_required || receipt.changed !== (value.action !== "keep")))
        rollbackFail();
    if (proposal && (receipt.source_path !== (value.action === "restore_tree" ? proposal.move.target_path : proposal.move.source_path) || receipt.target_path !== (value.action === "restore_tree" ? proposal.move.source_path : proposal.move.target_path)
        || value.action !== "restore_tree" && receipt.before_fingerprint !== proposal.approved_fingerprint))
        rollbackFail();
}
export function verifyRollbackAttemptTransition(before, after) {
    const { status: _a, receipt: _b, cancelled: _c, recovery_required: _d, updated_at: _e, ...oldOp } = before.operation;
    const { status: _f, receipt: _g, cancelled: _h, recovery_required: _i, updated_at: _j, ...newOp } = after.operation;
    if (before.source_plan_id !== after.source_plan_id || before.source_operation_id !== after.source_operation_id || before.action !== after.action || !sameRestoreValue(oldOp, newOp)
        || stamp(after.operation.updated_at) < stamp(before.operation.updated_at) || before.operation.cancelled && !after.operation.cancelled
        || ["done", "rejected"].includes(before.operation.status) && (after.operation.status !== before.operation.status || !sameRestoreValue(before.operation.receipt, after.operation.receipt)))
        rollbackFail();
}
export function verifyRollbackQuery(value, identity, request) {
    if (!exact(value, ["kind", "proposal", "operation", "history", "proposals"]) || value.kind !== request.kind)
        rollbackFail();
    const selected = { proposal: "proposal", by_key: "proposal", operation: "operation", operation_by_key: "operation", history: "history", proposals: "proposals" }[request.kind];
    for (const field of ["proposal", "operation", "history", "proposals"])
        if (field !== selected && value[field] !== null || field === selected && !["by_key", "operation_by_key"].includes(request.kind) && value[field] === null)
            rollbackFail();
    if (value.proposal !== null) {
        verifyRollbackProposal(value.proposal, identity);
        if (request.kind === "proposal" && value.proposal.move.proposal_id !== request.identifier)
            rollbackFail();
    }
    if (value.operation !== null) {
        verifyRollbackAttempt(value.operation, identity);
        if (request.kind === "operation" && value.operation.operation.operation_id !== request.identifier)
            rollbackFail();
    }
    const page = value.history ?? value.proposals;
    if (page === null)
        return;
    if (!exact(page, ["items", "next_after"]) || !Array.isArray(page.items) || page.items.length > (request.limit ?? 4))
        rollbackFail();
    const ids = [], sources = [];
    for (const item of page.items) {
        if (request.kind === "history") {
            verifyRollbackAttempt(item, identity);
            if (item.operation.proposal_id !== request.identifier)
                rollbackFail();
            ids.push(item.operation.operation_id);
            sources.push(item.source_plan_id + "/" + item.source_operation_id);
        }
        else {
            verifyRollbackProposal(item, identity);
            ids.push(item.move.proposal_id);
        }
    }
    if (new Set(sources).size > 1 || ids.some((id, index) => Boolean(index && id <= ids[index - 1]) || Boolean(request.after && id <= request.after))
        || page.next_after !== null && (!restoreId(page.next_after) || Boolean(request.after && page.next_after <= request.after) || Boolean(ids.length && page.next_after < ids.at(-1))))
        rollbackFail();
}
