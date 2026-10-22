// D1_DATA纯合同：无fetch、组件、存储owner或执行入口。
export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export interface InputFile { path: string; encoding: "base64"; data: string; sha256: string }
export interface InputSnapshotRequest { schema_version: 1; request_key: string; expected_head_revision: number; files: InputFile[] }
export interface CompletionCondition { id: string; kind: "json_equal" | "ast_symbols"; path: string; pointer: string; expected: Json; symbols: string[] }
export interface Completion { profile: "python-unittest/1"; required_paths: string[]; conditions: CompletionCondition[] }
export interface WorkspaceTurnRequest { schema_version: 1; request_key: string; text: string; expected_revision: number; input: { mode: "head_at_dispatch" | "revision"; revision_id: string | null }; completion: Completion }
export interface ArtifactFile { path: string; size: number; sha256: string }
export interface ArtifactRevision { revision_id: string; session_id: string; parent_id: string | null; revision: number; source_kind: "input" | "effect"; request_key: string; request_hash: string; manifest: ArtifactFile[]; manifest_hash: string; producer_run_id: string | null; producer_call_id: string | null }
export interface ArtifactChunk { revision_id: string; path: string; offset: number; total_size: number; length: number; sha256: string; data: string }
export interface Effect { call_id: string; kind: string; phase: "intent" | "dispatch_intent" | "settled" | "unknown"; executor_started: boolean | null; result_available: boolean | null; exit_code: number | null; output_complete: boolean | null; cleanup: "pending" | "clean" | "unknown"; artifact_revision_id: string | null; code: string | null }
const fail = (): never => { throw new Error("工作区合同无效"); };
const utf8 = (v: string) => new TextEncoder().encode(v);
const object = (v: unknown): Record<string, unknown> => v !== null && typeof v === "object" && !Array.isArray(v) ? v as Record<string, unknown> : fail();
function exact(v: Record<string, unknown>, fields: string[]) { if (Object.keys(v).length !== fields.length || fields.some(k => !(k in v))) fail(); }
function str(v: unknown): string { if (typeof v !== "string") fail(); return v as string; }
function integer(v: unknown, max = Number.MAX_SAFE_INTEGER): number { if (typeof v !== "number" || !Number.isSafeInteger(v) || v < 0 || v > max) fail(); return v as number; }
function digest(v: unknown): string { const s = str(v); if (!/^[0-9a-f]{64}$/.test(s)) fail(); return s; }
function id(v: unknown): string { const s = str(v); if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(s)) fail(); return s; }
function key(v: unknown): string { const s = str(v); if (!/^[A-Za-z0-9_.:-]{1,128}$/.test(s)) fail(); return s; }
function values(v: unknown, max: number): unknown[] { if (!Array.isArray(v) || v.length > max) fail(); return v as unknown[]; }
export function artifactPath(v: unknown): string {
  const s = str(v), parts = s.split("/");
  if (utf8(s).length > 512 || parts.length > 16 || s.normalize("NFC") !== s || /[\\\x00-\x1f\x7f]/.test(s) ||
      Array.from(s).some(c => { const n = c.codePointAt(0)!; return n >= 0xd800 && n <= 0xdfff; }) ||
      parts.some(p => !p || [".", "..", ".git", ".ssh", ".aws", ".codex", ".env"].includes(p) ||
        p.startsWith(".env.") || ["credentials", "id_rsa", "id_ed25519"].includes(p.toLowerCase()))) fail();
  return s;
}
function canonical(value: Json): string {
  if (value === null || typeof value !== "object") {
    if (typeof value === "number" && !Number.isFinite(value)) fail();
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) return "[" + value.map(canonical).join(",") + "]";
  return "{" + Object.keys(value).sort().map(k => JSON.stringify(k) + ":" + canonical(value[k]!)).join(",") + "}";
}
export async function sha256(bytes: Uint8Array): Promise<string> {
  return [...new Uint8Array(await crypto.subtle.digest("SHA-256", new Uint8Array(bytes)))].map(v => v.toString(16).padStart(2, "0")).join("");
}
function base64(value: unknown): Uint8Array {
  const s = str(value); let raw: string;
  try { raw = atob(s); } catch { return fail(); }
  if (btoa(raw) !== s) fail();
  return Uint8Array.from(raw, c => c.charCodeAt(0));
}
function inputFile(raw: unknown): InputFile {
  const v = object(raw); exact(v, ["path", "encoding", "data", "sha256"]);
  artifactPath(v.path); digest(v.sha256);
  if (v.encoding !== "base64" || base64(v.data).length > 32768) fail();
  return v as unknown as InputFile;
}
export async function parseInputSnapshot(raw: unknown): Promise<InputSnapshotRequest> {
  const v = object(raw); exact(v, ["schema_version", "request_key", "expected_head_revision", "files"]);
  if (v.schema_version !== 1) fail(); key(v.request_key); integer(v.expected_head_revision);
  const files = values(v.files, 32).map(inputFile);
  let total = 0; const paths = new Set<string>();
  for (const file of files) {
    const bytes = base64(file.data); total += bytes.length;
    if (paths.has(file.path) || await sha256(bytes) !== file.sha256) fail();
    paths.add(file.path);
  }
  if (total > 32768 || utf8(JSON.stringify(v)).length > 65536) fail();
  return v as unknown as InputSnapshotRequest;
}
export async function inputRequestHash(raw: unknown): Promise<string> {
  const v = await parseInputSnapshot(raw);
  const files = v.files.map(f => ({ path: f.path, size: base64(f.data).length, sha256: f.sha256 }))
    .sort((a, b) => {
      const x = Array.from(a.path, c => c.codePointAt(0)!), y = Array.from(b.path, c => c.codePointAt(0)!);
      for (let i = 0; i < Math.min(x.length, y.length); i++) if (x[i] !== y[i]) return x[i]! - y[i]!;
      return x.length - y.length;
    });
  return sha256(utf8(canonical({schema_version: 1, request_key: v.request_key, expected_head_revision: v.expected_head_revision, files})));
}
export function parseWorkspaceTurn(raw: unknown): WorkspaceTurnRequest {
  const v = object(raw); exact(v, ["schema_version", "request_key", "text", "expected_revision", "input", "completion"]);
  if (v.schema_version !== 1) fail(); key(v.request_key); integer(v.expected_revision);
  const text = str(v.text); if (!text.trim() || utf8(text).length > 32768 || utf8(JSON.stringify(v)).length > 65536) fail();
  const input = object(v.input); exact(input, ["mode", "revision_id"]);
  if (input.mode === "head_at_dispatch") { if (input.revision_id !== null) fail(); }
  else if (input.mode === "revision") id(input.revision_id); else fail();
  const c = object(v.completion); exact(c, ["profile", "required_paths", "conditions"]);
  if (c.profile !== "python-unittest/1") fail();
  const paths = values(c.required_paths, 16).map(artifactPath), conditions = values(c.conditions, 16), ids = new Set<string>();
  if (new Set(paths).size !== paths.length || paths.length + conditions.length > 16) fail();
  for (const condition of conditions) {
    const x = object(condition); exact(x, ["id", "kind", "path", "pointer", "expected", "symbols"]);
    const name = key(x.id); if (ids.has(name)) fail(); ids.add(name);
    if (x.kind !== "json_equal" && x.kind !== "ast_symbols") fail();
    artifactPath(x.path); str(x.pointer); values(x.symbols, 16).forEach(str);
    if (utf8(canonical(x.expected as Json)).length > 4096) fail();
  }
  return v as unknown as WorkspaceTurnRequest;
}
export function modelArguments(raw: Json): string {
  const text = canonical(raw); if (utf8(text).length > 4096) fail(); return text;
}
export function parseArtifact(raw: unknown): ArtifactRevision {
  const v = object(raw); exact(v, ["revision_id", "session_id", "parent_id", "revision", "source_kind", "request_key", "request_hash", "manifest", "manifest_hash", "producer_run_id", "producer_call_id"]);
  id(v.revision_id); id(v.session_id); integer(v.revision); key(v.request_key); digest(v.request_hash); digest(v.manifest_hash);
  for (const field of ["parent_id", "producer_run_id", "producer_call_id"]) if (v[field] !== null) id(v[field]);
  if (v.source_kind !== "input" && v.source_kind !== "effect") fail();
  if (v.source_kind === "input" && (v.producer_run_id !== null || v.producer_call_id !== null) ||
      v.source_kind === "effect" && (v.producer_run_id === null || v.producer_call_id === null)) fail();
  let total = 0; const paths = new Set<string>();
  for (const rawFile of values(v.manifest, 32)) {
    const file = object(rawFile); exact(file, ["path", "size", "sha256"]);
    const path = artifactPath(file.path); if (paths.has(path)) fail(); paths.add(path);
    total += integer(file.size, 1048576); digest(file.sha256);
  }
  if (total > 8388608) fail();
  return v as unknown as ArtifactRevision;
}
export function parseArtifactChunk(raw: unknown): ArtifactChunk {
  const v = object(raw); exact(v, ["revision_id", "path", "offset", "total_size", "length", "sha256", "data"]);
  id(v.revision_id); artifactPath(v.path); integer(v.offset); digest(v.sha256);
  const total = integer(v.total_size, 1048576), size = integer(v.length, 16384);
  if (base64(v.data).length !== size || size > Math.max(0, total - integer(v.offset))) fail();
  return v as unknown as ArtifactChunk;
}
export function parseEffect(raw: unknown): Effect {
  const v = object(raw); exact(v, ["call_id", "kind", "phase", "executor_started", "result_available", "exit_code", "output_complete", "cleanup", "artifact_revision_id", "code"]);
  id(v.call_id); str(v.kind);
  if (!["intent", "dispatch_intent", "settled", "unknown"].includes(str(v.phase)) || !["pending", "clean", "unknown"].includes(str(v.cleanup))) fail();
  for (const field of ["executor_started", "result_available", "output_complete"]) if (v[field] !== null && typeof v[field] !== "boolean") fail();
  if (v.exit_code !== null && (typeof v.exit_code !== "number" || !Number.isSafeInteger(v.exit_code))) fail();
  if (v.artifact_revision_id !== null) id(v.artifact_revision_id); if (v.code !== null) str(v.code);
  return v as unknown as Effect;
}
// 只分类精确、服务端保证未准入的首次POST；GET和未知回程始终保留intent。
export function definitelyNotAdmitted(firstPost: boolean, kind: "turn" | "input", status: number, code: string): boolean {
  if (!firstPost) return false;
  return status === 401 && code === "unauthorized" || status === 429 && code === "capacity" ||
    status === 422 && ["invalid_input", "input_limit"].includes(code) || status === 413 && code === "body_too_large" ||
    kind === "input" && status === 409 && ["workspace_busy", "input_revision_conflict"].includes(code);
}

export interface Verification { state: "pending" | "passed" | "failed" | "unknown"; revision_id: string | null; command_call_id: string | null; results: Record<string, Json>[] }
export function parseVerification(raw: unknown): Verification {
  const v = object(raw); exact(v, ["state", "revision_id", "command_call_id", "results"]);
  if (!["pending", "passed", "failed", "unknown"].includes(str(v.state))) fail();
  if (v.revision_id !== null) id(v.revision_id); if (v.command_call_id !== null) id(v.command_call_id);
  values(v.results, 16).forEach(object);
  return v as unknown as Verification;
}
export function parseSummary(raw: unknown): Record<string, unknown> {
  const v = object(raw); exact(v, ["pass_index", "status", "source_digest", "model_step", "summary_hash", "summary"]);
  integer(v.pass_index, 1); integer(v.model_step, 11); digest(v.source_digest);
  if (!["pending", "completed", "failed", "interrupted"].includes(str(v.status))) fail();
  if (v.summary_hash !== null) digest(v.summary_hash);
  if (v.summary !== null && utf8(str(v.summary)).length > 2048) fail();
  return v;
}
export function parsePlan(raw: unknown): Record<string, unknown> {
  const v = object(raw); exact(v, ["revision", "steps"]); integer(v.revision);
  for (const rawStep of values(v.steps, 8)) {
    const s = object(rawStep); exact(s, ["id", "text", "status", "proposed_by"]); key(s.id);
    if (!str(s.text) || str(s.text).length > 1024 ||
        !["pending", "running", "completed", "failed"].includes(str(s.status)) ||
        !["model", "user"].includes(str(s.proposed_by))) fail();
  }
  return v; // 计划状态仅为数据；不会批准工具或替代Verification。
}
