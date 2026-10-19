import { isUnadmittedCreate, request } from "../api.ts";
import { parseSession, parseTurn, sessionPath, type Session, type Turn } from "./session-contracts.ts";

export const INTENTS = "krim.agent-core.intents/1", KNOWN = "krim.agent-core.known/1", CURRENT = "krim.agent-core.current/1", DRAFTS = "krim.agent-core.drafts/1";
export interface Scope { principal_id: string; generation: number; workspace_id: string; session_id: string | null }
export interface Intent extends Scope { version: 1; id: string; kind: "session" | "turn"; body: { request_key: string; title?: string; target_key?: string; text?: string; expected_revision?: number } }
export function sameScope(a: Scope, b: Scope) { return a.principal_id === b.principal_id && a.generation === b.generation && a.workspace_id === b.workspace_id && a.session_id === b.session_id; }
export function readIntents(storage: Storage): Intent[] {
  const raw = storage.getItem(INTENTS); if (raw === null) return [];
  const values: unknown = JSON.parse(raw);
  if (!Array.isArray(values) || values.length > 128) throw new Error("无法读取未确认请求");
  const ids = new Set<string>();
  for (const value of values) {
    const i = value as Intent;
    if (!i || i.version !== 1 || typeof i.id !== "string" || ids.has(i.id) || typeof i.principal_id !== "string" || !Number.isSafeInteger(i.generation) || typeof i.workspace_id !== "string" || (i.session_id !== null && typeof i.session_id !== "string") || !["session", "turn"].includes(i.kind) || typeof i.body?.request_key !== "string" || (i.kind === "turn" && (typeof i.body.text !== "string" || !Number.isSafeInteger(i.body.expected_revision) || i.session_id === null)) || (i.kind === "session" && (typeof i.body.title !== "string" || typeof i.body.target_key !== "string" || i.session_id !== null))) throw new Error("无法读取未确认请求");
    ids.add(i.id);
    Object.freeze(i.body); Object.freeze(i);
  }
  return values as Intent[];
}
function save(storage: Storage, key: string, value: unknown) { const raw = JSON.stringify(value); storage.setItem(key, raw); if (storage.getItem(key) !== raw) throw new Error("无法保存请求"); }
export function prepareIntent(storage: Storage, scope: Scope, kind: Intent["kind"], body: Omit<Intent["body"], "request_key">): { intent: Intent; first: boolean } {
  const values = readIntents(storage), found = values.find(i => i.principal_id === scope.principal_id && i.workspace_id === scope.workspace_id && i.session_id === scope.session_id && i.kind === kind);
  if (found) return { intent: found, first: false };
  if (values.length >= 128) throw new Error("未确认请求过多");
  const intent: Intent = JSON.parse(JSON.stringify({ ...scope, version: 1, id: crypto.randomUUID(), kind, body: { ...body, request_key: crypto.randomUUID() } })) as Intent;
  Object.freeze(intent.body); Object.freeze(intent);
  save(storage, INTENTS, [...values, intent]); return { intent, first: true };
}
function remove(storage: Storage, intent: Intent) { save(storage, INTENTS, readIntents(storage).filter(i => !(i.id === intent.id && JSON.stringify(i) === JSON.stringify(intent)))); }
export function remember(storage: Storage, intent: Intent, value: Session | Turn) {
  const raw = storage.getItem(KNOWN), values = raw ? JSON.parse(raw) as Record<string, unknown> : {};
  values[intent.id] = { principal_id: intent.principal_id, generation: intent.generation, workspace_id: intent.workspace_id, session_id: intent.kind === "session" ? value.id : intent.session_id, turn_id: intent.kind === "turn" ? value.id : null, request_key: intent.body.request_key };
  save(storage, KNOWN, values);
}
export async function performIntent(storage: Storage, intent: Intent, key: string, first: boolean, accept: (value: Session | Turn) => boolean): Promise<Session | Turn> {
  const base = sessionPath(intent.workspace_id) + (intent.kind === "turn" ? "/" + encodeURIComponent(intent.session_id!) + "/turns" : "");
  let raw: unknown;
  if (first) {
    try { raw = await request(base, key, intent.body); }
    catch (error) { if (isUnadmittedCreate(error)) remove(storage, intent); throw error; }
  } else raw = await request(base + "/by-key/" + encodeURIComponent(intent.body.request_key), key);
  const value = intent.kind === "session" ? parseSession(raw) : parseTurn(raw);
  if (value.workspace_id !== intent.workspace_id || (intent.kind === "session" ? (value as Session).create_key : (value as Turn).request_key) !== intent.body.request_key || (intent.kind === "turn" && (value as Turn).session_id !== intent.session_id)) throw new Error("响应身份不匹配");
  if (intent.kind === "turn" && ((value as Turn).text !== intent.body.text || (value as Turn).accepted_revision !== intent.body.expected_revision)) throw new Error("原请求内容不匹配");
  if (accept(value)) { remember(storage, intent, value); remove(storage, intent); }
  return value;
}
export function saveSelection(storage: Storage, scope: Scope, sessionId: string, turnId: string | null) { save(storage, CURRENT, { ...scope, session_id: sessionId, turn_id: turnId }); }
export function draftKey(scope: Scope) { return JSON.stringify(scope); }
export function readDraft(storage: Storage, scope: Scope): string { const raw = storage.getItem(DRAFTS), values = raw ? JSON.parse(raw) as Record<string, unknown> : {}; const value = values[draftKey(scope)]; return typeof value === "string" ? value : ""; }
export function saveDraft(storage: Storage, scope: Scope, text: string) { const raw = storage.getItem(DRAFTS), values = raw ? JSON.parse(raw) as Record<string, unknown> : {}; values[draftKey(scope)] = text; save(storage, DRAFTS, values); }
