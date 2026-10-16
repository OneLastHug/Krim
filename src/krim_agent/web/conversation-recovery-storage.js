/** 持久化原恢复意图；任何自动恢复都只能GET，写后逐字读回才可POST。 */
import { recoveryExact, sameRecoveryValue, verifyConversationIdentity, verifyConversationPreview, verifyConversationRecord, verifyConversationRequest } from "./conversation-recovery-contract.js";
const MAX_BYTES = 131072;
function invalid() { throw new Error("会话恢复记录不可用，写操作已停用"); }
function key(identity) { verifyConversationIdentity(identity); return `krim-conversation-recovery:${identity.agent_id}`; }
export function verifyConversationState(value, identity) {
    if (!recoveryExact(value, ["version", "agent_id", "session_id", "topic_id", "pending", "record"]) || value.version !== 1
        || value.agent_id !== identity.agent_id || value.session_id !== identity.session_id || value.topic_id !== identity.topic_id || value.pending !== null && value.record !== null)
        invalid();
    if (value.pending !== null) {
        if (!recoveryExact(value.pending, ["request", "source", "rejected"]) || typeof value.pending.rejected !== "boolean")
            invalid();
        verifyConversationPreview(value.pending.source, identity);
        verifyConversationRequest(value.pending.request, value.pending.source);
    }
    if (value.record !== null)
        verifyConversationRecord(value.record, identity);
}
export function readConversationState(identity) {
    try {
        const raw = localStorage.getItem(key(identity));
        if (raw === null)
            return { version: 1, ...identity, pending: null, record: null };
        if (new TextEncoder().encode(raw).length > MAX_BYTES)
            invalid();
        const value = JSON.parse(raw);
        verifyConversationState(value, identity);
        return value;
    }
    catch {
        invalid();
    }
}
export function saveConversationState(value, identity) {
    try {
        verifyConversationState(value, identity);
        const raw = JSON.stringify(value);
        if (new TextEncoder().encode(raw).length > MAX_BYTES)
            invalid();
        localStorage.setItem(key(identity), raw);
        const actual = localStorage.getItem(key(identity));
        if (actual !== raw)
            invalid();
        const parsed = JSON.parse(actual);
        verifyConversationState(parsed, identity);
        if (!sameRecoveryValue(parsed, value))
            invalid();
    }
    catch {
        invalid();
    }
}
