/** 持久化原恢复意图；任何自动恢复都只能GET，写后逐字读回才可POST。 */
import { recoveryExact, sameRecoveryValue, verifyConversationIdentity, verifyConversationPreview, verifyConversationRecord, verifyConversationRequest,
  type ConversationIdentity, type ConversationRecoveryPreview, type ConversationRecoveryRecord, type ConversationRecoveryRequest } from "./conversation-recovery-contract.js";

export type ConversationPending = { request: ConversationRecoveryRequest; source: ConversationRecoveryPreview; rejected: boolean };
export type ConversationRecoveryState = ConversationIdentity & { version: 1; pending: ConversationPending | null; record: ConversationRecoveryRecord | null };
const MAX_BYTES = 131072;
function invalid(): never { throw new Error("会话恢复记录不可用，写操作已停用"); }
function key(identity: ConversationIdentity): string { verifyConversationIdentity(identity); return `krim-conversation-recovery:${identity.agent_id}`; }
export function verifyConversationState(value: unknown, identity: ConversationIdentity): asserts value is ConversationRecoveryState {
  if (!recoveryExact(value, ["version", "agent_id", "session_id", "topic_id", "pending", "record"]) || value.version !== 1
    || value.agent_id !== identity.agent_id || value.session_id !== identity.session_id || value.topic_id !== identity.topic_id || value.pending !== null && value.record !== null) invalid();
  if (value.pending !== null) {
    if (!recoveryExact(value.pending, ["request", "source", "rejected"]) || typeof value.pending.rejected !== "boolean") invalid();
    verifyConversationPreview(value.pending.source, identity); verifyConversationRequest(value.pending.request, value.pending.source);
  }
  if (value.record !== null) verifyConversationRecord(value.record, identity);
}
export function readConversationState(identity: ConversationIdentity): ConversationRecoveryState {
  try {
    const raw = localStorage.getItem(key(identity));
    if (raw === null) return { version: 1, ...identity, pending: null, record: null };
    if (new TextEncoder().encode(raw).length > MAX_BYTES) invalid();
    const value: unknown = JSON.parse(raw); verifyConversationState(value, identity); return value;
  } catch { invalid(); }
}
export function saveConversationState(value: ConversationRecoveryState, identity: ConversationIdentity): void {
  try {
    verifyConversationState(value, identity);
    const raw = JSON.stringify(value); if (new TextEncoder().encode(raw).length > MAX_BYTES) invalid();
    localStorage.setItem(key(identity), raw);
    const actual = localStorage.getItem(key(identity)); if (actual !== raw) invalid();
    const parsed: unknown = JSON.parse(actual); verifyConversationState(parsed, identity);
    if (!sameRecoveryValue(parsed, value)) invalid();
  } catch { invalid(); }
}
