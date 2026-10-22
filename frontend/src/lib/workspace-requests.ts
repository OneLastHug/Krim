import { object, request, RequestError } from "../api.ts";
import { parseTurn } from "./session-contracts.ts";
import {
  definitelyNotAdmitted, inputRequestHash, parseArtifact, parseEffect, parseInputSnapshot,
  parsePlan, parseSummary, parseVerification, parseWorkspaceTurn,
  type ArtifactRevision, type InputSnapshotRequest, type WorkspaceTurnRequest,
} from "./workspace-contracts.ts";

export const WORK_INTENTS = "krim.agent-core.workspace-intents/1";
export const WORK_KNOWN = "krim.agent-core.workspace-known/1";
export interface WorkScope {
  principal_id: string; generation: number; workspace_id: string; session_id: string;
  target: "target-workspace";
}
export interface WorkIntent extends WorkScope {
  version: 1; id: string; kind: "turn" | "input";
  body: WorkspaceTurnRequest | InputSnapshotRequest;
}
export interface WorkKnown extends WorkScope {
  id: string; request_key: string; kind: "turn" | "input";
  identity: string; manifest_hash: string | null;
}
export interface WorkView {
  schema_version: 1; session_id: string; turn_id: string; run_id: string | null;
  request_key: string; original_request: WorkspaceTurnRequest;
  requested_input: WorkspaceTurnRequest["input"]; resolved_input_revision: string | null;
  phase: string; plan: ReturnType<typeof parsePlan>; compactions: ReturnType<typeof parseSummary>[];
  effects: ReturnType<typeof parseEffect>[]; verification: ReturnType<typeof parseVerification> | null;
  effective_fingerprint: string | null;
}
const clone = <T,>(value: T): T => JSON.parse(JSON.stringify(value)) as T;
export const canonical = (value: unknown): string => {
  if (Array.isArray(value)) return "["+value.map(canonical).join(",")+"]";
  if (value && typeof value === "object") return "{"+Object.keys(value).sort().map(key =>
    JSON.stringify(key)+":"+canonical((value as Record<string, unknown>)[key])).join(",")+"}";
  return JSON.stringify(value);
};
export const workBase = (scope: Pick<WorkScope, "workspace_id" | "session_id">) =>
  "/v3/workspaces/"+encodeURIComponent(scope.workspace_id)+"/sessions/"+encodeURIComponent(scope.session_id);
function save(storage: Storage, key: string, value: unknown) {
  const raw = JSON.stringify(value); storage.setItem(key, raw);
  if (storage.getItem(key) !== raw) throw new Error("无法保存工作区请求");
}
export function sameWorkObject(left: WorkScope, right: WorkScope) {
  return left.principal_id === right.principal_id &&
    left.workspace_id === right.workspace_id && left.session_id === right.session_id &&
    left.target === right.target;
}
export function sameWorkScope(left: WorkScope, right: WorkScope) {
  return sameWorkObject(left, right) && left.generation === right.generation;
}
export function readWorkIntents(storage: Storage): WorkIntent[] {
  const raw: unknown = JSON.parse(storage.getItem(WORK_INTENTS) ?? "[]");
  if (!Array.isArray(raw) || raw.length > 128) throw new Error("工作区请求账本无效");
  const ids = new Set<string>();
  for (const item of raw) {
    const value = object(item), body = object(value.body);
    if (value.version !== 1 || typeof value.id !== "string" || ids.has(value.id) ||
        !["turn", "input"].includes(String(value.kind)) || value.target !== "target-workspace" ||
        typeof value.principal_id !== "string" || !Number.isSafeInteger(value.generation) ||
        typeof value.workspace_id !== "string" || typeof value.session_id !== "string" ||
        typeof body.request_key !== "string" || Object.keys(value).length !== 9)
      throw new Error("工作区请求账本无效");
    if (value.kind === "turn") parseWorkspaceTurn(body);
    ids.add(value.id);
  }
  return clone(raw) as WorkIntent[];
}
// /1命名空间隐含版本与target；保持已发出的八字段账本可读，不存凭据。
export function readWorkKnown(storage: Storage): WorkKnown[] {
  const text = storage.getItem(WORK_KNOWN) ?? "{}";
  if (new TextEncoder().encode(text).length > 131072) throw new Error("已确认身份账本过大");
  const raw = object(JSON.parse(text)), entries = Object.entries(raw);
  if (entries.length > 128) throw new Error("已确认身份过多");
  const uuid = (value: unknown): value is string => typeof value === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(value);
  const name = (value: unknown) => typeof value === "string" && /^[a-zA-Z0-9_.-]{1,64}$/.test(value);
  const fields = ["principal_id", "generation", "workspace_id", "session_id", "request_key",
    "kind", "identity", "manifest_hash"];
  return entries.map(([id, item]) => {
    const value = object(item);
    if (!uuid(id) || Object.keys(value).length !== fields.length || fields.some(field => !(field in value)) ||
        !name(value.principal_id) || !name(value.workspace_id) || !uuid(value.session_id) ||
        !Number.isSafeInteger(value.generation) || Number(value.generation) < 1 ||
        typeof value.request_key !== "string" || !/^[A-Za-z0-9_.:-]{1,128}$/.test(value.request_key) ||
        !["turn", "input"].includes(String(value.kind)) || !uuid(value.identity) ||
        (value.kind === "turn" ? value.manifest_hash !== null :
          typeof value.manifest_hash !== "string" || !/^[0-9a-f]{64}$/.test(value.manifest_hash)))
      throw new Error("已确认身份账本无效");
    return { ...clone(value), id, target: "target-workspace" } as unknown as WorkKnown;
  });
}
export async function recoverWorkKnown(storage: Storage, record: WorkKnown, scope: WorkScope,
  key: string, capture: () => () => boolean, signal?: AbortSignal): Promise<WorkView | ArtifactRevision | null> {
  // 旧generation只指保存时身份；GET使用当前凭据重新授权，每个出口仍捕获当前代次。
  const frozen = clone(record), current = capture();
  if (!sameWorkObject(frozen, scope) || !readWorkKnown(storage).some(value => canonical(value) === canonical(frozen)))
    throw new Error("已确认身份作用域不符");
  const base = workBase(scope), path = frozen.kind === "input" ?
    "/artifacts/"+encodeURIComponent(frozen.identity) : "/turns/"+encodeURIComponent(frozen.identity)+"/work";
  if (!current()) return null;
  const raw = await request(base+path, key, undefined, signal);
  if (!current()) return null;
  const keyedCurrent = capture();
  const keyed = await request(base+(frozen.kind === "input" ? "/inputs" : "/turns")+
    "/by-key/"+encodeURIComponent(frozen.request_key), key, undefined, signal);
  if (!current() || !keyedCurrent()) return null;
  if (frozen.kind === "input") {
    const value = parseArtifact(raw), original = parseArtifact(keyed);
    if (value.session_id !== frozen.session_id || value.revision_id !== frozen.identity ||
        value.request_key !== frozen.request_key || value.source_kind !== "input" ||
        value.manifest_hash !== frozen.manifest_hash || canonical(value) !== canonical(original))
      throw new Error("已确认输入身份不符");
    return value;
  }
  const value = parseWorkView(raw), original = parseWorkView(keyed);
  if (value.session_id !== frozen.session_id || value.turn_id !== frozen.identity ||
      value.request_key !== frozen.request_key || value.original_request.request_key !== frozen.request_key ||
      original.session_id !== value.session_id || original.turn_id !== value.turn_id ||
      original.request_key !== value.request_key || canonical(original.original_request) !== canonical(value.original_request))
    throw new Error("已确认Turn身份不符");
  return value;
}
export async function prepareWorkIntent(storage: Storage, scope: WorkScope,
  kind: WorkIntent["kind"], raw: WorkspaceTurnRequest | InputSnapshotRequest) {
  // 首await之前脱离调用者全部嵌套dict/list和base64；不保存凭据。
  const body = clone(raw), frozenScope = clone(scope);
  if (kind === "turn") parseWorkspaceTurn(body); else await parseInputSnapshot(body);
  const pending = readWorkIntents(storage), found = pending.find(value =>
    sameWorkObject(value, frozenScope) && value.kind === kind);
  if (found) {
    if (canonical({ ...found.body, request_key: "" }) !== canonical({ ...body, request_key: "" }))
      throw new Error("原工作区请求尚未确认，不能覆盖内容");
    return { intent: found, first: false };
  }
  if (pending.length >= 128) throw new Error("未确认请求过多");
  const intent: WorkIntent = { ...frozenScope, version: 1, id: crypto.randomUUID(), kind, body };
  save(storage, WORK_INTENTS, [...pending, intent]); return { intent: clone(intent), first: true };
}
export function parseWorkView(raw: unknown): WorkView {
  const value = object(raw), fields = ["schema_version", "session_id", "turn_id", "run_id",
    "request_key", "original_request", "requested_input", "resolved_input_revision", "phase",
    "plan", "compactions", "effects", "verification", "effective_fingerprint"];
  if (Object.keys(value).length !== fields.length || fields.some(key => !(key in value)) ||
      value.schema_version !== 1 || typeof value.session_id !== "string" ||
      typeof value.turn_id !== "string" || typeof value.request_key !== "string" ||
      !["queued", "preparing", "running", "verifying", "settled", "interrupted"].includes(String(value.phase)) ||
      !Array.isArray(value.compactions) || value.compactions.length > 2 ||
      !Array.isArray(value.effects) || value.effects.length > 32) throw new Error("工作视图无效");
  parseWorkspaceTurn(value.original_request); parsePlan(value.plan);
  value.compactions.forEach(parseSummary); value.effects.forEach(parseEffect);
  if (value.verification !== null) parseVerification(value.verification);
  return value as unknown as WorkView;
}
export async function performWorkIntent(storage: Storage, intent: WorkIntent, key: string,
  first: boolean, capture: () => () => boolean, accept: (value: WorkView | ArtifactRevision) => void) {
  const confirmed = readWorkKnown(storage);
  const frozen = clone(intent), path = workBase(frozen)+(frozen.kind === "turn" ? "/turns" : "/inputs");
  if (confirmed.length >= 128 && !confirmed.some(value => value.id === frozen.id))
    throw new Error("已确认身份过多，原请求仍保留");
  const originalCurrent = capture();
  let raw: unknown;
  if (first) {
    try { raw = await request(path, key, frozen.body); }
    catch (error) {
      if (originalCurrent() && error instanceof RequestError && definitelyNotAdmitted(
        true, frozen.kind, error.status, error.code)) save(storage, WORK_INTENTS,
          readWorkIntents(storage).filter(value => value.id !== frozen.id));
      throw error;
    }
    if (!originalCurrent()) return;
    if (frozen.kind === "turn") {
      const turn = parseTurn(raw), body = frozen.body as WorkspaceTurnRequest;
      if (turn.session_id !== frozen.session_id || turn.workspace_id !== frozen.workspace_id ||
          turn.request_key !== body.request_key || turn.text !== body.text ||
          turn.accepted_revision !== body.expected_revision) throw new Error("原Turn身份不符");
    }
  }
  // Turn确认还核对完整original_request；两个出口各自捕获身份/epoch。
  if (!first || frozen.kind === "turn") {
    const getCurrent = capture();
    raw = await request(path+"/by-key/"+encodeURIComponent(frozen.body.request_key), key);
    if (!originalCurrent() || !getCurrent()) return;
  }
  let value: WorkView | ArtifactRevision;
  if (frozen.kind === "turn") {
    value = parseWorkView(raw);
    if (value.session_id !== frozen.session_id || value.request_key !== frozen.body.request_key ||
        canonical(value.original_request) !== canonical(frozen.body)) throw new Error("原工作请求不符");
  } else {
    value = parseArtifact(raw);
    const digest = await inputRequestHash(frozen.body);
    if (!originalCurrent()) return;
    if (value.session_id !== frozen.session_id || value.source_kind !== "input" ||
        value.request_key !== frozen.body.request_key || value.request_hash !== digest)
      throw new Error("原输入身份不符");
  }
  if (!originalCurrent()) return;
  const currentKnown = readWorkKnown(storage);
  if (currentKnown.length >= 128 && !currentKnown.some(item => item.id === frozen.id))
    throw new Error("已确认身份过多，原请求仍保留");
  const known = object(JSON.parse(storage.getItem(WORK_KNOWN) ?? "{}"));
  known[frozen.id] = { principal_id: frozen.principal_id, generation: frozen.generation,
    workspace_id: frozen.workspace_id, session_id: frozen.session_id,
    request_key: frozen.body.request_key, kind: frozen.kind,
    identity: frozen.kind === "turn" ? (value as WorkView).turn_id : (value as ArtifactRevision).revision_id,
    manifest_hash: frozen.kind === "input" ? (value as ArtifactRevision).manifest_hash : null };
  save(storage, WORK_KNOWN, known); // durable身份先写入并读回确认
  readWorkKnown(storage); // 本次实际写出的完整账本也必须符合严格边界
  save(storage, WORK_INTENTS, readWorkIntents(storage).filter(item =>
    !(item.id === frozen.id && canonical(item) === canonical(frozen))));
  accept(value);
}
