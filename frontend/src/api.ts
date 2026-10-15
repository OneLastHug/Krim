/** 与 Python 公开 HTTP/SSE 合约同步；浏览器不绕过服务端权限。 */
export interface TopicView {
  id: string;
  title: string;
  created_at: string;
  updated_at: string;
  active_branches: Record<string, string | null>;
  locale: "zh-CN" | "en-US";
  agent_session_id: string | null;
}
export interface LocaleView { requested: string; effective: "zh-CN" | "en-US"; fallback_chain: string[]; }
export interface MessageView {
  id: string;
  topic_id: string;
  run_id: string;
  sequence: number;
  role: "system" | "user" | "assistant" | "tool";
  content: string;
  details: Record<string, unknown>;
  created_at: string;
  parent_id: string | null;
}
export interface MentionRef {
  id: string;
  kind: "text" | "file" | "resource";
  label: string;
  content: string;
  metadata: Record<string, unknown>;
}
export interface MentionOccurrence { mention_id: string; start: number; end: number; }
export interface ContextTag { name: string; content: string; metadata: Record<string, unknown>; }
export interface PromptContext { mentions: MentionRef[]; context_tags: ContextTag[]; }
export interface SkillSummary { name: string; description: string; loading: "deferred" | "always"; license: string | null; compatibility: string | null; metadata: Record<string, string>; allowed_tools: string[]; resource_paths: string[]; digest: string; }
export interface SkillCatalog { skills: SkillSummary[]; }
export interface SkillReadResult { name: string; resource: string | null; content: string; }
export interface MessageTreeView {
  topic: TopicView;
  messages: MessageView[];
  active_path: string[];
}
export interface RunView {
  id: string;
  topic_id: string;
  input_message_id: string;
  status: "queued" | "running" | "completed" | "failed" | "aborted" | "interrupted";
  error: string | null;
  error_code: string | null;
}
export interface StreamFrame {
  stream_id: string;
  seq: number;
  kind: "start" | "chunk" | "end" | "error";
  data: Record<string, unknown> | null;
}
export interface RunCursor { run_id: string; topic_id: string; next_seq: number; }
export interface RunStreamFrame { type: "run-frame"; topic_id: string; frame: StreamFrame; }
export interface ActiveRunView { run: RunView; events: { count: number; terminal: boolean }; }
export interface CompactResponse {
  topic_id: string;
  status: "proposed" | "completed" | "unchanged";
  persisted: boolean;
  branch_key: string;
  source_message_ids: string[];
  summary: string | null;
  reason: string | null;
  omitted_count: number;
  messages: MessageView[];
  tokenizer: "approx-v1" | "cl100k_base" | "o200k_base" | "custom";
  estimated_tokens: number;
  reserved_output_tokens: number;
  proposal_id: string | null;
  proposal_revision: number | null;
  proposal_expires_at: number | null;
}
export interface ContextProposal {
  id: string; topic_id: string; run_id: string | null; step: number | null;
  branch_key: string; source_branch_key: string;
  status: "proposed" | "completed" | "rejected" | "expired" | "stale" | "cancelled";
  revision: number; expires_at: number; created_at: string;
  options: { max_messages: number | null };
  result: Omit<CompactResponse, "topic_id" | "status" | "persisted" | "branch_key" | "proposal_id" | "proposal_revision" | "proposal_expires_at">;
}
export interface InteractionView {
  id: string;
  run_id: string;
  tool_name: string;
  kind: "approval" | "question";
  payload: Record<string, unknown>;
  status: "pending" | "approved" | "denied" | "answered" | "cancelled" | "expired";
  response: Record<string, unknown> | null;
  expires_at: number;
}
export interface AgentDescriptor {
  name: string;
  description: string;
  risk: "read" | "write";
  max_turns: number;
  exposed_as_tool: boolean;
  profile_revision?: number | null;
  available: boolean;
  error_code: string | null;
}
export interface AgentResourceView { key: string; kind: string; name: string; version: string | null; fingerprint: string; owned: boolean; available: boolean; }
export interface AgentSessionView {
  session_id: string; status: "open" | "closed" | "expired" | "destroyed";
  revision: number; turn: number; profile_revision: number | null;
  expires_at: number | null; permissions: Record<string, unknown>;
}
export interface AgentTurnView { run_id: string; turn: number; status: string; }
export interface AgentView {
  agent_id: string;
  name: string;
  topic_id: string;
  status: string;
  current_run_id: string | null;
  turn: number;
  result: { text: string; code: string | null; error: string | null } | null;
  session_id: string;
  session: AgentSessionView;
}
export interface CommandAccepted { command_id: string; request_key: string; }
export interface CommandHistoryEntry {
  command_id: string; request_key: string; command_preview: string;
  phase: "preparing" | "dispatched" | "running" | "finished" | "interrupted";
  outcome: "completed" | "failed" | "timed_out" | "cancelled" | "output_limit" | "setup_failed" | "uncertain" | null;
  started: boolean; processes_reaped: boolean; error_code: string | null; created_at: string;
}
export interface CommandHistoryPage { entries: CommandHistoryEntry[]; next_after: string | null; }
export interface CommandSpecView { argv: string[]; cwd: string; }
export interface AgentCommandRequest {
  request_key: string; command: CommandSpecView; input_paths: string[]; directories: string[];
}
export interface CommandState {
  command_id: string;
  phase: CommandHistoryEntry["phase"]; revision: number;
  dispatched: boolean; started: boolean; outcome: CommandHistoryEntry["outcome"];
  exit_code: number | null; output_truncated: boolean; stdout_bytes: number; stderr_bytes: number;
  input_files: number; artifact_files: number; artifacts_complete: boolean; processes_reaped: boolean;
  error_code: string | null;
}
export interface CommandView extends CommandState {
  scope_id: string; topic_id: string; request: { request_key: string; command: CommandSpecView };
  resource: { key: string; version: string; fingerprint: string };
  run_id: string | null; call_id: string | null; cancel_requested: boolean;
  created_at: string; updated_at: string; interrupted_at: string | null;
}
export interface CommandSummary extends CommandState {
  stdout_text: string; stderr_text: string; stdout_base64: string; stderr_base64: string;
  stdout_bytes: number; stderr_bytes: number; artifacts: Array<{ path: string; sha256: string; byte_size: number; mode: number }>;
  artifacts_truncated: boolean;
}
export interface CommandSlice {
  command_id: string; source: "stdout" | "stderr" | "artifact"; path: string | null;
  offset: number; next_offset: number; byte_size: number; data_base64: string; text: string; truncated: boolean;
}
export type CommandChangeKind = "added" | "modified" | "deleted" | "unchanged";
export interface CommandDelta {
  path: string; kind: CommandChangeKind; before_sha256: string | null; after_sha256: string | null;
  before_bytes: number | null; after_bytes: number | null; before_mode: number | null; after_mode: number | null;
}
export interface CommandChangesPage {
  command_id: string; source_hash: string; outcome: NonNullable<CommandHistoryEntry["outcome"]>;
  requires_failed_acceptance: boolean; entries: CommandDelta[]; next_after: string | null;
}
export interface CommandProposalView {
  command_id: string; source_hash: string; accept_failed: boolean; change: CommandDelta;
  proposal: WorkspaceChangeView;
}
export interface WorkspaceView { key: string; version: string; fingerprint: string; available: boolean; tool_names: string[]; max_file_bytes: number; writable: boolean; }
export interface WorkspaceEntry { path: string; kind: "directory" | "file"; byte_size: number; }
export interface WorkspaceListing { path: string; entries: WorkspaceEntry[]; truncated: boolean; entries_scanned: number; }
export interface WorkspaceFile { path: string; sha256: string; byte_size: number; start_line: number; lines: number; content: string; truncated: boolean; }
export interface WorkspaceSearch { matches: Array<{ path: string; line: number; text: string }>; files_scanned: number; files_skipped: number; truncated: boolean; }
export type WorkspaceAction = "apply" | "undo" | "keep";
export interface DirectorySpec {
  path: string; operation: "create" | "delete" | "move"; expected_fingerprint?: string | null;
  mode?: number | null; target_path?: string | null;
}
export interface DirectoryPreviewRequest { spec: DirectorySpec; request_key: string; }
export interface DirectoryPreviewView {
  proposal_id: string; scope_id: string; workspace_key: string; resource_fingerprint: string;
  path: string; operation: "create" | "delete" | "move"; target_path?: string | null;
  before_fingerprint: string | null; mode: number | null;
  revision: number; status: WorkspaceChangeView["status"]; expires_at: number;
}
export interface DirectoryAction {
  proposal_id: string; action: WorkspaceAction; expected_revision: number; request_key: string;
}
export interface DirectoryAttemptView {
  operation_id: string; proposal_id: string; scope_id: string; action: WorkspaceAction;
  expected_revision: number; status: "running" | "done" | "rejected" | "uncertain";
  receipt: { path: string; status: "done" | "rejected" | "uncertain"; before_fingerprint: string | null;
    after_fingerprint: string | null; changed: boolean; error_code: string | null; recovery_required: boolean } | null;
  recovery_required: boolean; cleanup_pending: boolean; created_at: number; updated_at: number;
}
export interface DirectoryPage {
  kind: "proposals" | "history"; proposals: DirectoryPreviewView[]; history: DirectoryAttemptView[];
  proposal: DirectoryPreviewView | null; operation: DirectoryAttemptView | null; next_after: string | null;
}
export interface DirectoryInspectView {
  path: string; found: boolean; fingerprint: string | null; mode: number | null; empty: boolean | null;
}
export interface TreeMoveInspectView {
  path: string; found: boolean; fingerprint: string | null; entry_count: number;
  directory_count: number; file_count: number; byte_size: number; mode: number | null;
}
export interface TreeMovePreviewRequest {
  source_path: string; target_path: string; expected_fingerprint: string;
  request_key: string; ttl_seconds?: number;
}
export interface TreeMovePreviewView {
  proposal_id: string; scope_id: string; workspace_key: string; resource_fingerprint: string;
  source_path: string; target_path: string; before_fingerprint: string; entry_count: number;
  byte_size: number; revision: number; status: WorkspaceChangeView["status"];
  expires_at: number; active_operation_id: string | null; created_at: string; updated_at: string;
}
export type TreeMoveAction = DirectoryAction;
export interface TreeMoveAttemptView {
  operation_id: string; proposal_id: string; scope_id: string; action: WorkspaceAction;
  expected_revision: number; status: "running" | "done" | "rejected" | "uncertain";
  receipt: { source_path: string; target_path: string; status: "done" | "rejected" | "uncertain";
    before_fingerprint: string; after_fingerprint: string | null; changed: boolean;
    error_code: string | null; recovery_required: boolean } | null;
  cancelled: boolean; recovery_required: boolean; created_at: string; updated_at: string;
}
export type TreeMoveQueryKind = "proposal" | "operation" | "by_key" | "operation_by_key" | "history" | "proposals";
export interface TreeMoveQueryResult {
  kind: TreeMoveQueryKind; proposal: TreeMovePreviewView | null; operation: TreeMoveAttemptView | null;
  history: { items: TreeMoveAttemptView[]; next_after: string | null } | null;
  proposals: { items: TreeMovePreviewView[]; next_after: string | null } | null;
}
export type TreeExchangeInspectView = TreeMoveInspectView;
export interface TreeExchangePreviewRequest {
  left_path: string; right_path: string; expected_left: string; expected_right: string;
  request_key: string; ttl_seconds?: number; source_plan_id?: string;
}
export interface TreeExchangePreviewView {
  proposal_id: string; scope_id: string; workspace_key: string; resource_fingerprint: string;
  left_path: string; right_path: string; left_before_fingerprint: string; right_before_fingerprint: string;
  left_entry_count: number; right_entry_count: number; left_byte_size: number; right_byte_size: number;
  origin: "existing_trees" | "completed_tree"; source_plan_id: string | null;
  revision: number; status: TreeMovePreviewView["status"]; expires_at: number;
  active_operation_id: string | null; created_at: string; updated_at: string;
}
export type TreeExchangeAction = DirectoryAction;
export interface TreeExchangeAttemptView {
  operation_id: string; proposal_id: string; scope_id: string; action: WorkspaceAction;
  expected_revision: number; status: "running" | "done" | "rejected" | "uncertain";
  receipt: { left_path: string; right_path: string; status: "done" | "rejected" | "uncertain";
    left_before_fingerprint: string; right_before_fingerprint: string;
    left_after_fingerprint: string | null; right_after_fingerprint: string | null;
    changed: boolean; error_code: string | null; recovery_required: boolean } | null;
  cancelled: boolean; recovery_required: boolean; created_at: string; updated_at: string;
}
export type TreeExchangeQueryKind = TreeMoveQueryKind;
export interface TreeExchangeQueryResult {
  kind: TreeExchangeQueryKind; proposal: TreeExchangePreviewView | null; operation: TreeExchangeAttemptView | null;
  history: { items: TreeExchangeAttemptView[]; next_after: string | null } | null;
  proposals: { items: TreeExchangePreviewView[]; next_after: string | null } | null;
}
export interface CompletedTreeOriginView {
  plan_id: string; scope_id: string; command_id: string; source_hash: string;
  outcome: "completed" | "failed" | "output_limit"; accept_failed: boolean;
  snapshot_revision: string; root_path: string; workspace_key: string; resource_fingerprint: string;
  tree_fingerprint: string; entry_count: number; byte_size: number; proof_hash: string;
}
export interface CompletedTreeOriginLookupView { plan_id: string; origin: CompletedTreeOriginView | null; }
export type GeneratedTreeUndoActionKind = "remove_root" | "restore_root" | "keep";
export type GeneratedTreeRootState = "awaiting_approval" | "removing" | "preserved" | "restoring" | "restored" | "keeping" | "kept" | "rejected" | "uncertain" | "cancelled";
export interface GeneratedTreeUndoPreviewRequest {
  plan_id: string; preserve_path: string; expected_fingerprint: string; request_key: string;
  ttl_seconds?: number; previous_restore_operation_id?: string | null;
}
export interface GeneratedTreeUndoAction {
  proposal_id: string; action: GeneratedTreeUndoActionKind; expected_revision: number; request_key: string;
}
export interface GeneratedTreeUndoPreviewView {
  source_plan_id: string; source_command_id: string; source_hash: string;
  command_outcome: "completed" | "failed" | "output_limit"; accept_failed: boolean;
  snapshot_revision: string; sealed_fingerprint: string; proof_hash: string;
  previous_restore_operation_id: string | null; generation_status: "done"; root_state: GeneratedTreeRootState;
  move: Omit<TreeMovePreviewView, "status"> & { status: TreeMovePreviewView["status"] | "cancelled" };
}
export interface GeneratedTreeUndoAttemptView {
  source_plan_id: string; action: GeneratedTreeUndoActionKind; operation: TreeMoveAttemptView;
}
export interface GeneratedTreeUndoQueryResult {
  kind: TreeMoveQueryKind; proposal: GeneratedTreeUndoPreviewView | null; operation: GeneratedTreeUndoAttemptView | null;
  history: { items: GeneratedTreeUndoAttemptView[]; next_after: string | null } | null;
  proposals: { items: GeneratedTreeUndoPreviewView[]; next_after: string | null } | null;
}
export interface CommandDirectoryDelta {
  path: string; mode: number; empty: boolean; creatable: boolean; proposal_id: string | null;
}
export interface CommandDirectoryChangesPage {
  command_id: string; source_hash: string; outcome: NonNullable<CommandHistoryEntry["outcome"]>;
  complete: boolean; entries: CommandDirectoryDelta[]; next_after: string | null;
}
export interface CommandDirectoryProposalView {
  command_id: string; source_hash: string; outcome: CommandDirectoryChangesPage["outcome"];
  accept_failed: boolean; directory: { path: string; mode: number; empty: boolean; creatable: boolean };
  proposal: DirectoryPreviewView;
}
export interface TreePlanRequest {
  command_id: string; source_hash: string; root_path: string; request_key: string;
  accept_failed: boolean; max_entries: number;
}
export interface TreePlanView {
  plan_id: string; scope_id: string; command_id: string; source_hash: string; root_path: string;
  workspace_key: string; resource_fingerprint: string; request_key: string; snapshot_revision: string;
  status: "planned" | "running" | "done" | "partial" | "failed" | "uncertain" | "cancelled" | "interrupted";
  producer_status: "planned" | "running" | "finished" | "cancelled" | "interrupted";
  step_count: number; done_count: number; stop_reason: string | null; created_at: string; updated_at: string;
}
export interface TreePlanStepView {
  position: number; parent_position: number | null; source: "file" | "directory"; path: string;
  mode: number; sha256: string | null; byte_size: number | null;
  status: "pending" | "running" | "done" | "rejected" | "uncertain" | "skipped";
  error_code: string | null; proposal_id: string | null; operation: WorkspaceAttemptView | DirectoryAttemptView | null;
}
export interface TreePlanStepPage { plan_id: string; scope_id: string; items: TreePlanStepView[]; next_after: number | null; }
export interface FileChangeSpec {
  path: string; operation: "write" | "replace" | "delete"; expected_sha256: string | null;
  content?: string | null; replacements?: Array<{ old_text: string; new_text: string; occurrences: number }>;
}
export interface WorkspaceChangeView {
  proposal_id: string; scope_id: string; topic_id: string; workspace_key: string;
  resource_fingerprint: string; revision: number;
  status: "proposed" | "applying" | "applied" | "undoing" | "undone" | "keeping" | "kept" | "rejected" | "uncertain";
  preview: { path: string; operation: "write" | "replace" | "delete"; before_sha256: string | null;
    after_sha256: string | null; before_bytes: number; after_bytes: number; diff: string; truncated: boolean };
  active_operation_id: string | null; expires_at: number; created_at: string; updated_at: string;
}
export interface WorkspaceChangeAction {
  proposal_id: string; action: WorkspaceAction; expected_revision: number; request_key: string;
}
export interface WorkspaceAttemptView {
  operation_id: string; proposal_id: string; scope_id: string; action: WorkspaceAction;
  expected_revision: number; status: "running" | "done" | "rejected" | "uncertain";
  receipt: { path: string; status: "done" | "rejected" | "uncertain"; before_sha256: string | null;
    after_sha256: string | null; changed: boolean; error_code: string | null; recovery_required: boolean } | null;
  cleanup_pending: boolean; run_id: string | null; call_id: string | null; created_at: string; updated_at: string;
}
export interface WorkspaceOperationProofView {
  operation_id: string; proposal_id: string; scope_id: string;
  status: WorkspaceAttemptView["status"]; kind: "not_applicable" | "exact_after" | "successor_chain";
  tip_proposal_id: string | null; tip_operation_id: string | null; tip_after_sha256: string | null;
  effect_sequence: number | null; successor_count: number;
}
export type NativeListFilter = "active" | "history" | "all";
export interface NativeChangeItem {
  proposal_id: string; revision: number; path: string; operation: "write" | "replace" | "delete";
  status: "applying" | "applied" | "undoing" | "undone" | "keeping" | "kept" | "uncertain";
  workspace_key: string; resource_fingerprint: string; apply_sequence: number | null;
  order_known: boolean; active_operation_id: string | null;
}
export interface NativeChangePage {
  source: "workspace"; scope_id: string; snapshot_revision: string; filter: NativeListFilter;
  total: number; items: NativeChangeItem[]; next_cursor: string | null;
}
export interface NativeBatchRequest {
  action: "keep" | "undo"; snapshot_revision: string;
  targets: Array<{ proposal_id: string; expected_revision: number }>; request_key: string;
}
export interface NativeBatchItemView {
  position: number; proposal_id: string; expected_revision: number; path: string;
  apply_sequence: number | null; status: "pending" | "running" | "done" | "rejected" | "uncertain" | "skipped";
  operation: WorkspaceAttemptView | null; error_code: string | null;
}
export interface NativeBatchView {
  source: "workspace"; batch_id: string; scope_id: string; request_key: string; snapshot_revision: string;
  action: "keep" | "undo"; status: "running" | "done" | "partial" | "failed" | "uncertain" | "cancelled" | "interrupted";
  producer_status: "running" | "finished" | "cancelled" | "interrupted";
  items: NativeBatchItemView[]; stop_reason: string | null; created_at: string; updated_at: string;
}
export interface NativeBatchSummary {
  batch_id: string; request_key: string; action: "keep" | "undo"; status: NativeBatchView["status"];
  producer_status: NativeBatchView["producer_status"]; snapshot_revision: string; item_count: number; done_count: number;
  stop_reason: string | null; created_at: string; updated_at: string;
}
export interface NativeBatchHistoryPage {
  source: "workspace"; scope_id: string; total: number; items: NativeBatchSummary[]; next_cursor: string | null;
}
export interface NativeCheckpointCreate { title: string; paths: string[]; request_key: string; capture_format?: "text" | "bytes"; }
export interface MixedBatchItemView {
  source: "file" | "directory"; position: number; proposal_id: string; expected_revision: number;
  path: string; target_path: string | null; status: NativeBatchItemView["status"];
  operation: WorkspaceAttemptView | DirectoryAttemptView | null; error_code: string | null;
}
export interface MixedBatchRequest {
  action: WorkspaceAction; request_key: string;
  targets: Array<{ source: "file" | "directory"; proposal_id: string; expected_revision: number }>;
}
export interface MixedBatchView {
  batch_id: string; scope_id: string; request_key: string; snapshot_revision: string;
  action: WorkspaceAction; status: NativeBatchView["status"]; producer_status: NativeBatchView["producer_status"];
  items: MixedBatchItemView[]; stop_reason: string | null; created_at: string; updated_at: string;
}
export interface MixedBatchSummary {
  batch_id: string; request_key: string; action: WorkspaceAction; status: MixedBatchView["status"];
  producer_status: MixedBatchView["producer_status"]; snapshot_revision: string; item_count: number; done_count: number;
  stop_reason: string | null; created_at: string; updated_at: string;
}
export interface MixedBatchHistoryPage {
  scope_id: string; total: number; items: MixedBatchSummary[]; next_cursor: string | null;
}
export interface NativeCheckpointView {
  source: "workspace"; checkpoint_id: string; scope_id: string; title: string; request_key: string;
  workspace_key: string; resource_fingerprint: string;
  capture_format?: "text" | "bytes";
  status: "capturing" | "available" | "failed" | "cancelled" | "interrupted";
  files: Array<{ path: string; captured: boolean; exists: boolean | null; sha256: string | null; byte_size: number | null; mode: number | null; content_kind?: "text" | "binary" | "absent" }>;
  error_code: string | null; created_at: string; updated_at: string;
}
export interface NativeRestorePreview { checkpoint_id: string; request_key: string; ttl_seconds?: number; baseline_capture_format?: "text" | "bytes"; }
export interface NativeRestoreExecute { plan_id: string; expected_revision: number; request_key: string; }
export interface NativeRestoreView {
  source: "workspace"; plan_id: string; scope_id: string; checkpoint_id: string; baseline_checkpoint_id: string | null;
  baseline_capture_format?: "text" | "bytes";
  request_key: string; execute_key: string | null; revision: number;
  phase: "preparing" | "ready" | "executing" | "finished" | "failed" | "cancelled" | "interrupted" | "expired";
  status: "preparing" | "ready" | "running" | "done" | "partial" | "failed" | "cancelled" | "interrupted" | "expired" | "uncertain";
  items: Array<{ position: number; path: string; proposal: WorkspaceChangeView | null; operation: WorkspaceAttemptView | null;
    status: "pending" | "running" | "done" | "rejected" | "uncertain" | "skipped"; current_mode: number | null; target_mode: number | null; error_code: string | null }>;
  expires_at: number; error_code: string | null; created_at: string; updated_at: string;
}
export interface WorkflowRunView {
  run_id: string;
  status: "queued" | "running" | "paused" | "completed" | "failed" | "aborted";
  epoch: number;
  current_node: string | null;
  output: unknown;
  node_statuses: Record<string, string>;
  node_outputs: Record<string, Record<string, unknown>>;
  error: string | null;
  code: string | null;
  event_count: number;
}
export interface WorkflowEvent { run_id: string; seq: number; epoch: number; type: string; node_id: string | null; data: unknown; }
export interface WorkflowPort { name: string; json_schema?: Record<string, unknown>; required?: boolean; multiple?: boolean; }
export interface WorkflowGraph { nodes: Array<{ id: string; kind: string; config: Record<string, unknown>; developer_only?: boolean; input_ports?: WorkflowPort[] | null; output_ports?: WorkflowPort[] | null }>; edges: Array<{ source: string; target: string; source_port?: string; target_port?: string; when?: string | null }>; output_node?: string | null; }
export interface HealthView { provider: string; service: string; }

let apiKey = "";
export function setApiKey(key: string): void { apiKey = key; }
export function errorText(error: unknown): string {
  return error instanceof Error ? error.message : "操作失败，请检查连接与运行状态";
}
function headers(): Headers {
  const value = new Headers({ "Content-Type": "application/json" });
  if (apiKey) value.set("Authorization", `Bearer ${apiKey}`);
  return value;
}
export class ApiError extends Error {
  constructor(message: string, readonly status: number, readonly code: string | null) {
    super(message); this.name = "ApiError";
  }
}
async function readJson(path: string, init: RequestInit): Promise<unknown> {
  const merged = headers();
  new Headers(init.headers).forEach((value, key) => merged.set(key, value));
  const response = await fetch(path, { ...init, headers: merged });
  let value: unknown;
  try { value = await response.json(); }
  catch { throw new ApiError(`HTTP ${response.status} · 服务没有返回有效的数据，请检查连接`, response.status, null); }
  if (!response.ok) {
    const body = value as { error?: { message?: unknown; code?: unknown } } | null;
    throw new ApiError(typeof body?.error?.message === "string" ? body.error.message : `HTTP ${response.status}`,
      response.status, typeof body?.error?.code === "string" ? body.error.code : null);
  }
  return value;
}
export async function requestJson<T>(path: string, init: RequestInit = {}): Promise<T> {
  const value = await readJson(path, init);
  if (value === null) throw new Error("服务没有返回有效的数据，请检查连接");
  return value as T;
}
/** 只有明确允许 null 的查询使用此入口；空正文或无效 JSON 仍拒绝。 */
export async function requestJsonNullable<T>(path: string, init: RequestInit = {}): Promise<T | null> {
  return await readJson(path, init) as T | null;
}
export async function consumeEvents(
  runId: string,
  fromSeq: number,
  onFrame: (frame: StreamFrame) => void,
  signal?: AbortSignal,
): Promise<number> {
  const response = await fetch(`/v1/runs/${encodeURIComponent(runId)}/events?from_seq=${fromSeq}`, {
    headers: headers(), signal,
  });
  if (!response.ok) {
    const body = await response.json() as { error?: { message?: string } };
    throw new Error(body.error?.message || `HTTP ${response.status}`);
  }
  if (!response.body) throw new Error("浏览器未提供流式响应");
  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let buffer = "";
  let cursor = fromSeq;
  let terminal = false;
  try {
    while (true) {
      const { value, done } = await reader.read();
      buffer += decoder.decode(value ?? new Uint8Array(), { stream: !done });
      if (buffer.length > 262144) throw new Error("流帧超过大小限制");
      const blocks = buffer.split("\n\n");
      buffer = blocks.pop() ?? "";
      for (const block of blocks) {
        if (block.startsWith(":")) continue;
        let id = "", kind = "";
        const data: string[] = [];
        for (const line of block.split("\n")) {
          if (line.startsWith("data:")) data.push(line.slice(5).trimStart());
          if (line.startsWith("id:")) id = line.slice(3).trim();
          if (line.startsWith("event:")) kind = line.slice(6).trim();
        }
        if (!data.length) continue;
        const raw = data.join("\n");
        if (new TextEncoder().encode(raw).length > 131072) throw new Error("流事件超过大小限制");
        const frame = JSON.parse(raw) as StreamFrame;
        if (frame.stream_id !== runId || frame.seq !== cursor || id !== String(cursor) || kind !== frame.kind) {
          throw new Error("流事件关联或序号不正确");
        }
        cursor += 1;
        onFrame(frame);
        if (frame.kind === "end" || frame.kind === "error") {
          terminal = true;
          await reader.cancel();
          return cursor;
        }
      }
      if (done) break;
    }
  } finally { reader.releaseLock(); }
  if (!terminal) throw new Error("连接已断开，可从已保存的游标继续读取");
  return cursor;
}
export async function consumeWorkflowEvents(runId: string, fromSeq: number, onEvent: (event: WorkflowEvent) => void): Promise<number> {
  const path = "/v1/workflows/runs/" + encodeURIComponent(runId);
  const response = await fetch(path + "/events?from_seq=" + fromSeq, { headers: headers() });
  if (!response.ok) throw new Error("Workflow 请求失败，请检查权限、状态或游标");
  if (!response.body) throw new Error("浏览器未提供 Workflow 流");
  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let buffer = "", cursor = fromSeq, epoch = -1;
  try {
    while (true) {
      const { value, done } = await reader.read();
      buffer += decoder.decode(value ?? new Uint8Array(), { stream: !done });
      const blocks = buffer.split("\n\n");
      buffer = blocks.pop() ?? "";
      if (buffer.length > 262144) throw new Error("Workflow 流帧超过大小限制");
      for (const block of blocks) {
        let id = "", kind = "";
        const lines: string[] = [];
        for (const line of block.split("\n")) {
          if (line.startsWith("data:")) lines.push(line.slice(5).trimStart());
          if (line.startsWith("id:")) id = line.slice(3).trim();
          if (line.startsWith("event:")) kind = line.slice(6).trim();
        }
        if (!lines.length) continue;
        const data = lines.join("\n");
        if (new TextEncoder().encode(data).length > 131072) throw new Error("Workflow 流事件超过大小限制");
        const event = JSON.parse(data) as WorkflowEvent;
        if (event.run_id !== runId || event.seq !== cursor || id !== String(cursor) || kind !== event.type
          || !Number.isSafeInteger(event.epoch) || event.epoch < epoch) {
          throw new Error("Workflow 事件关联、代次或游标不正确");
        }
        epoch = event.epoch;
        cursor += 1;
        onEvent(event);
        if (event.type === "end") { await reader.cancel(); return cursor; }
      }
      if (done) break;
    }
  } finally { reader.releaseLock(); }
  const current = await requestJson<WorkflowRunView>(path);
  if (["queued", "running"].includes(current.status) || current.event_count !== cursor) {
    throw new Error("Workflow 流连接已断开，可从游标继续");
  }
  return cursor;
}

export async function consumeMultiplex(sources: RunCursor[], onFrame: (item: RunStreamFrame) => void, signal: AbortSignal): Promise<void> {
  const response = await fetch("/v1/runs/events", { method: "POST", headers: headers(), body: JSON.stringify({ sources }), signal });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  if (!response.body) throw new Error("浏览器未提供流式响应");
  const expected = new Map(sources.map((source) => [source.run_id, source.next_seq]));
  const topics = new Map(sources.map((source) => [source.run_id, source.topic_id]));
  const reader = response.body.getReader(); const decoder = new TextDecoder("utf-8", { fatal: true });
  let buffer = "";
  try {
    while (true) {
      const { value, done } = await reader.read();
      buffer += decoder.decode(value ?? new Uint8Array(), { stream: !done });
      const blocks = buffer.split("\n\n"); buffer = blocks.pop() ?? "";
      if (buffer.length > 524288) throw new Error("多路流帧超过大小限制");
      for (const block of blocks) {
        if (block.startsWith(":")) continue;
        let id = "", kind = ""; const lines: string[] = [];
        for (const line of block.split("\n")) {
          if (line.startsWith("data:")) lines.push(line.slice(5).trimStart());
          if (line.startsWith("id:")) id = line.slice(3).trim();
          if (line.startsWith("event:")) kind = line.slice(6).trim();
        }
        if (!lines.length) continue;
        const raw = lines.join("\n");
        if (new TextEncoder().encode(raw).length > 262144) throw new Error("多路流事件超过大小限制");
        const item = JSON.parse(raw) as RunStreamFrame; const frame = item.frame;
        const next = expected.get(frame.stream_id);
        if (item.type !== "run-frame" || kind !== "run-frame" || item.topic_id !== topics.get(frame.stream_id)
          || next === undefined || frame.seq !== next || id !== `${frame.stream_id}:${frame.seq}`) throw new Error("多路流来源或序号不正确");
        expected.set(frame.stream_id, next + 1); onFrame(item);
        if (frame.kind === "end" || frame.kind === "error") expected.delete(frame.stream_id);
      }
      if (!expected.size) { await reader.cancel(); return; }
      if (done) break;
    }
  } finally { reader.releaseLock(); }
  if (expected.size) throw new Error("连接已断开，可从已保存的游标继续读取");
}
