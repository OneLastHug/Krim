const SELECTORS = new Set(["plan_id", "operation_id", "proposal_id", "checkpoint_id", "batch_id", "command_id", "request_key", "expected_revision"]);
export function recoveryFail() { throw new Error("会话恢复回程与原事实不一致"); }
export function recoveryObject(value) { return typeof value === "object" && value !== null && !Array.isArray(value); }
export function recoveryExact(value, keys) { return recoveryObject(value) && Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key)); }
export function recoveryInteger(value) { return typeof value === "number" && Number.isSafeInteger(value) && value >= 0; }
function text(value, max) { return typeof value === "string" && Array.from(value).length >= 1 && Array.from(value).length <= max && !Array.from(value).some(char => { const code = char.codePointAt(0); return code >= 0xd800 && code <= 0xdfff; }); }
export function recoveryId(value) { return typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9_-]{0,35}$/.exec(value)?.[0] === value; }
export function recoveryKey(value) { return text(value, 128) && !/[\x00-\x1f\x7f]/.test(value); }
function sha(value) { return typeof value === "string" && value.length === 64 && /^[a-f0-9]{64}$/.test(value); }
export function sameRecoveryValue(a, b) {
    if (a === b)
        return true;
    if (Array.isArray(a) && Array.isArray(b))
        return a.length === b.length && a.every((item, index) => sameRecoveryValue(item, b[index]));
    if (!recoveryObject(a) || !recoveryObject(b))
        return false;
    const keys = Object.keys(a);
    return keys.length === Object.keys(b).length && keys.every(key => Object.hasOwn(b, key) && sameRecoveryValue(a[key], b[key]));
}
function material(value) { if (new TextEncoder().encode(JSON.stringify(value)).length > 65536)
    recoveryFail(); }
export function verifyConversationIdentity(value) {
    if (!recoveryObject(value) || !recoveryId(value.agent_id) || value.session_id !== value.agent_id || !recoveryId(value.topic_id))
        recoveryFail();
}
export function verifyConversationPreview(value, identity) {
    if (!recoveryExact(value, ["agent_id", "session_id", "topic_id", "session_revision", "available", "source_path_hash", "source_branch_key", "source_run_id", "source_run_status", "anchor_message_id", "excluded_message_ids", "pending_calls", "answered_calls"])
        || value.agent_id !== identity.agent_id || value.session_id !== identity.session_id || value.topic_id !== identity.topic_id
        || !recoveryInteger(value.session_revision) || typeof value.available !== "boolean" || !sha(value.source_path_hash) || !sha(value.source_branch_key)
        || !Array.isArray(value.excluded_message_ids) || value.excluded_message_ids.length > 65 || value.excluded_message_ids.some(id => !recoveryId(id))
        || !Array.isArray(value.pending_calls) || !Array.isArray(value.answered_calls) || value.pending_calls.length + value.answered_calls.length > 64
        || value.available !== Boolean(value.pending_calls.length))
        recoveryFail();
    if (value.available) {
        if (!recoveryId(value.source_run_id) || typeof value.source_run_status !== "string" || !["interrupted", "failed", "aborted"].includes(value.source_run_status)
            || value.anchor_message_id !== null && !recoveryId(value.anchor_message_id)
            || value.excluded_message_ids.length !== value.answered_calls.length + 1 || value.excluded_message_ids.includes(value.anchor_message_id))
            recoveryFail();
    }
    else if (value.source_run_id !== null || value.source_run_status !== null || value.anchor_message_id !== null || value.excluded_message_ids.length || value.answered_calls.length)
        recoveryFail();
    const calls = [];
    for (const call of value.pending_calls) {
        if (!recoveryExact(call, ["call_id", "name", "arguments_hash", "selectors"]) || !text(call.call_id, 128) || !text(call.name, 64) || !sha(call.arguments_hash) || !recoveryObject(call.selectors))
            recoveryFail();
        for (const [key, item] of Object.entries(call.selectors))
            if (!SELECTORS.has(key) || (key === "expected_revision" ? !recoveryInteger(item) : !recoveryKey(item)))
                recoveryFail();
        calls.push(call.call_id);
    }
    for (const [index, call] of value.answered_calls.entries()) {
        if (!recoveryExact(call, ["call_id", "message_id", "message_hash"]) || !text(call.call_id, 128) || !recoveryId(call.message_id) || !sha(call.message_hash)
            || call.message_id !== value.excluded_message_ids[index + 1])
            recoveryFail();
        calls.push(call.call_id);
    }
    if (new Set(calls).size !== calls.length || new Set(value.excluded_message_ids).size !== value.excluded_message_ids.length)
        recoveryFail();
    material(value);
}
export function verifyConversationRequest(value, source) {
    if (!recoveryExact(value, ["source_run_id", "expected_path_hash", "expected_session_revision", "request_key"]) || !recoveryId(value.source_run_id)
        || !sha(value.expected_path_hash) || !recoveryInteger(value.expected_session_revision) || !recoveryKey(value.request_key)
        || source && (!source.available || value.source_run_id !== source.source_run_id || value.expected_path_hash !== source.source_path_hash || value.expected_session_revision !== source.session_revision))
        recoveryFail();
}
function validDate(value) {
    if (typeof value !== "string")
        return false;
    const parts = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,6})?(Z|[+-]\d{2}:\d{2})$/.exec(value);
    if (!parts || parts[0] !== value)
        return false;
    const [y, m, d, h, min, sec] = parts.slice(1, 7).map(Number);
    const days = [31, y % 4 === 0 && (y % 100 !== 0 || y % 400 === 0) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    return y >= 1 && m >= 1 && m <= 12 && d >= 1 && d <= days[m - 1] && h < 24 && min < 60 && sec < 60 && Number.isFinite(Date.parse(value));
}
/** 存储读取先作纯结构核对；HTTP确认另用WebCrypto核对v1消息主键。 */
export function verifyConversationRecord(value, identity, request, source) {
    if (!recoveryExact(value, ["version", "kind", "status", "recovery_message_id", "request", "source", "recovered_branch_key", "session_revision", "created_at"])
        || value.version !== 1 || value.kind !== "conversation_recovery_branch" || value.status !== "context_recovered" || !recoveryId(value.recovery_message_id)
        || !sha(value.recovered_branch_key) || !recoveryInteger(value.session_revision) || !validDate(value.created_at))
        recoveryFail();
    verifyConversationPreview(value.source, identity);
    verifyConversationRequest(value.request, value.source);
    if (value.session_revision !== value.source.session_revision + 1 || request && !sameRecoveryValue(value.request, request) || source && !sameRecoveryValue(value.source, source))
        recoveryFail();
    material(value);
}
export async function recoveryMessageId(topicId, requestKey) {
    if (!recoveryId(topicId) || !recoveryKey(requestKey))
        recoveryFail();
    const namespace = new Uint8Array([0x6b, 0xa7, 0xb8, 0x11, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8]);
    const name = new TextEncoder().encode(JSON.stringify(["krim-conversation-recovery-v1", topicId, requestKey]));
    const bytes = new Uint8Array(namespace.length + name.length);
    bytes.set(namespace);
    bytes.set(name, namespace.length);
    const digest = new Uint8Array(await crypto.subtle.digest("SHA-1", bytes)).slice(0, 16);
    digest[6] = (digest[6] & 0x0f) | 0x50;
    digest[8] = (digest[8] & 0x3f) | 0x80;
    const hex = Array.from(digest, byte => byte.toString(16).padStart(2, "0")).join("");
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
export async function checkedConversationRecord(value, identity, request, source) {
    verifyConversationRecord(value, identity, request, source);
    if (value.recovery_message_id !== await recoveryMessageId(value.source.topic_id, value.request.request_key))
        recoveryFail();
    return value;
}
