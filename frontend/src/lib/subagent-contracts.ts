// 子代理只读数据合同：严格解析服务端成员列表与单成员详情，不含请求、组件或审批入口。
import type { ApprovalRecord } from "./session-presentation";

export const SUBAGENT_PAGE_MAX = 50;

export type WakeMode = "auto" | "queue_only";
export type SubagentRole = "root" | "agent";
export type SubagentMemberStatus = "active" | "closed";
export type SubagentDelivery = "next_input_boundary" | "queued" | "started";

/** 列表成员的展示状态派生。 */
export type SubagentState = "running" | "approval" | "completed" | "error" | "idle" | "closed";
export type SubagentTaskStatus = "accepted" | "running" | "waiting_input" | "awaiting_approval" | "completed" | "aborted" | "error" | "max_tokens" | "refusal" | "interrupted";

export interface SubagentActivation { activation_id: string; instance: number; status: string }
export interface SubagentTaskRef { task_id: string; status: SubagentTaskStatus; run_id: string | null; ordinal: number; input: unknown }
export interface SubagentMember {
  agent_id: string;
  tree_id: string;
  path: string;
  role: SubagentRole;
  status: SubagentMemberStatus;
  depth: number;
  profile: string;
  provider: string;
  workdir: string | null;
  revision: number;
  current_task_id: string | null;
  parent_path: string | null;
  tree_status: "open" | "closed";
  route_key: string | null;
  activation: SubagentActivation | null;
  task: SubagentTaskRef | null;
  /** 详情返回的展示标题；列表可缺省。 */
  title?: string | null;
}
export interface SubagentPage { subagents: SubagentMember[]; next_after_path: string | null }

export interface SubagentMessage {
  seq: number;
  id: string;
  kind: "task" | "message";
  wake: WakeMode;
  body: unknown;
  sender_id: string | null;
  task_id: string | null;
  accepted_at: number;
}
export interface SubagentResult {
  seq: number;
  notification_id: string;
  notification_status: string;
  result_id: string;
  task_id: string | null;
  producer_path?: string | null;
  stop_reason: string;
  code: string | null;
  output: unknown;
  diagnostics: unknown;
  settled_at: number;
}
export interface SubagentEvent { run_id: string; seq: number; event_id: string; kind: string; data: Record<string, unknown>; at: number }
export type SubagentRunStatus = "queued" | "running" | "awaiting" | "completed" | "failed" | "aborted" | "interrupted";
export interface SubagentRun {
  id: string;
  request_key: string;
  status: SubagentRunStatus;
  trace_id: string;
  root_span_id: string;
  created_at: number;
  finished_at: number | null;
  code: string | null;
  stop_requested: boolean;
}
export interface SubagentDetail {
  agent: SubagentMember;
  messages: SubagentMessage[];
  results: SubagentResult[];
  events: SubagentEvent[];
  approvals: ApprovalRecord[];
  run: SubagentRun | null;
  /** 当前或最近child run；无run为null。 */
  run_id: string | null;
  /** 兼容旧服务；新服务始终返回完整记录。 */
  events_truncated: boolean;
}

export interface SubagentSendReceipt {
  agent_id: string;
  message_id: string;
  seq: number;
  accepted: true;
  started: boolean;
  delivery: SubagentDelivery;
}

function fail(message = "子代理响应无效"): never { throw new Error(message); }
const object_ = (value: unknown): Record<string, unknown> => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) fail();
  return value as Record<string, unknown>;
};
const bounded = (value: unknown, max: number): string => {
  if (typeof value !== "string" || !value || value.length > max || /[\u0000-\u001f]/.test(value)) fail();
  return value;
};
const nullable = (value: unknown, max: number): string | null => value === null ? null : bounded(value, max);
const integer = (value: unknown, min = 0): number => {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < min) fail();
  return value;
};
const moment = (value: unknown): number => {
  if (typeof value !== "number" || !Number.isFinite(value)) fail();
  return value;
};
function exact(value: Record<string, unknown>, fields: readonly string[], optional: readonly string[] = []) {
  if (Object.keys(value).some(key => !fields.includes(key) && !optional.includes(key)) || fields.some(key => !(key in value))) fail();
}

const MEMBER_FIELDS = ["agent_id", "tree_id", "path", "role", "status", "depth", "profile", "provider",
  "workdir", "revision", "current_task_id", "parent_path", "tree_status", "route_key", "activation", "task"] as const;

function parseActivation(raw: unknown): SubagentActivation | null {
  if (raw === null) return null;
  const value = object_(raw);
  exact(value, ["activation_id", "instance", "status"]);
  if (typeof value.status !== "string" || !value.status || value.status.length > 64) fail("子代理驻留无效");
  return { activation_id: bounded(value.activation_id, 256), instance: integer(value.instance, 1), status: value.status };
}

const TASK_STATUSES = ["accepted", "running", "waiting_input", "awaiting_approval", "completed", "aborted", "error", "max_tokens", "refusal", "interrupted"];

function parseTaskRef(raw: unknown): SubagentTaskRef | null {
  if (raw === null) return null;
  const value = object_(raw);
  exact(value, ["task_id", "status", "run_id", "ordinal", "input"]);
  if (!TASK_STATUSES.includes(String(value.status))) fail("子代理任务状态无效");
  const input = value.input;
  if (typeof input !== "object" || input === null || Array.isArray(input)) fail("子代理任务无效");
  return {
    task_id: bounded(value.task_id, 256),
    status: value.status as SubagentTaskStatus,
    run_id: nullable(value.run_id, 256),
    ordinal: integer(value.ordinal, 1),
    input,
  };
}

export function parseSubagentMember(raw: unknown): SubagentMember {
  const value = object_(raw);
  exact(value, MEMBER_FIELDS, ["title"]);
  if (value.role !== "root" && value.role !== "agent") fail("子代理成员无效");
  if (value.status !== "active" && value.status !== "closed") fail("子代理成员无效");
  if (value.tree_status !== "open" && value.tree_status !== "closed") fail("子代理成员无效");
  if (value.title !== undefined && value.title !== null && typeof value.title !== "string") fail("子代理成员无效");
  const task = parseTaskRef(value.task);
  const member: SubagentMember = {
    agent_id: bounded(value.agent_id, 256),
    tree_id: bounded(value.tree_id, 256),
    path: bounded(value.path, 256),
    role: value.role,
    status: value.status,
    depth: integer(value.depth),
    profile: bounded(value.profile, 128),
    provider: bounded(value.provider, 128),
    workdir: nullable(value.workdir, 4096),
    revision: integer(value.revision, 1),
    current_task_id: nullable(value.current_task_id, 256),
    parent_path: nullable(value.parent_path, 256),
    tree_status: value.tree_status,
    route_key: nullable(value.route_key, 128),
    activation: parseActivation(value.activation),
    task,
  };
  if (task !== null && member.current_task_id !== task.task_id) fail("子代理成员无效");
  return value.title === undefined ? member : { ...member, title: value.title };
}

/** 列表按 path 升序分页；游标必须等于本页最后一条 path，防止跳页与重复。 */
export function parseSubagentPage(raw: unknown): SubagentPage {
  const page = object_(raw);
  exact(page, ["subagents", "next_after_path"]);
  if (!Array.isArray(page.subagents) || page.subagents.length > SUBAGENT_PAGE_MAX) fail("子代理列表无效");
  const members = page.subagents.map(parseSubagentMember);
  const seen = new Set<string>();
  let previous = "";
  for (const member of members) {
    if (seen.has(member.agent_id)) fail("子代理列表重复");
    seen.add(member.agent_id);
    if (previous && member.path < previous) fail("子代理列表无效");
    previous = member.path;
  }
  const next = page.next_after_path === null ? null : bounded(page.next_after_path, 256);
  if (next !== null && (!members.length || next !== previous)) fail("子代理分页无效");
  return { subagents: members, next_after_path: next };
}

function parseMessage(raw: unknown): SubagentMessage {
  const value = object_(raw);
  exact(value, ["seq", "id", "kind", "wake", "body", "sender_id", "task_id", "accepted_at"]);
  if ((value.kind !== "task" && value.kind !== "message") || (value.wake !== "auto" && value.wake !== "queue_only")) fail("子代理消息无效");
  const body = value.body;
  if (typeof body !== "object" || body === null || Array.isArray(body)) fail("子代理消息无效");
  const sender = nullable(value.sender_id, 256);
  const accepted = moment(value.accepted_at);
  return {
    seq: integer(value.seq, 1),
    id: bounded(value.id, 256),
    kind: value.kind,
    wake: value.wake,
    body,
    sender_id: sender,
    task_id: nullable(value.task_id, 256),
    accepted_at: accepted,
  };
}

function parseResult(raw: unknown): SubagentResult {
  const value = object_(raw);
  exact(value, ["seq", "notification_id", "notification_status", "result_id", "task_id",
    "producer_path", "stop_reason", "code", "output", "diagnostics", "settled_at"]);
  if (typeof value.notification_status !== "string" || !value.notification_status || value.notification_status.length > 64) fail("子代理结果无效");
  if (typeof value.stop_reason !== "string" || !value.stop_reason || value.stop_reason.length > 64) fail("子代理结果无效");
  // producer_path 由后端补齐前可缺省；其余身份字段一律严格。
  const producer = value.producer_path === undefined ? undefined : nullable(value.producer_path, 256);
  return {
    seq: integer(value.seq, 1),
    notification_id: bounded(value.notification_id, 256),
    notification_status: value.notification_status,
    result_id: bounded(value.result_id, 256),
    task_id: nullable(value.task_id, 256),
    ...(producer === undefined ? {} : { producer_path: producer }),
    stop_reason: value.stop_reason,
    code: nullable(value.code, 256),
    output: value.output,
    diagnostics: value.diagnostics,
    settled_at: moment(value.settled_at),
  };
}

function parseEvent(raw: unknown): SubagentEvent {
  const value = object_(raw);
  exact(value, ["run_id", "seq", "event_id", "kind", "data", "at"]);
  const data = value.data;
  if (typeof data !== "object" || data === null || Array.isArray(data)) fail("子代理事件无效");
  return {
    run_id: bounded(value.run_id, 256),
    seq: integer(value.seq),
    event_id: bounded(value.event_id, 256),
    kind: bounded(value.kind, 128),
    data: data as Record<string, unknown>,
    at: moment(value.at),
  };
}

const RUN_STATUSES = ["queued", "running", "awaiting", "completed", "failed", "aborted", "interrupted"];

function parseRunView(raw: unknown): SubagentRun {
  const value = object_(raw);
  exact(value, ["id", "request_key", "status", "trace_id", "root_span_id", "created_at", "finished_at", "code", "stop_requested"]);
  if (!RUN_STATUSES.includes(String(value.status)) || typeof value.stop_requested !== "boolean") fail("子代理运行无效");
  return {
    id: bounded(value.id, 256),
    request_key: bounded(value.request_key, 256),
    status: value.status as SubagentRunStatus,
    trace_id: bounded(value.trace_id, 256),
    root_span_id: bounded(value.root_span_id, 256),
    created_at: moment(value.created_at),
    finished_at: value.finished_at === null ? null : moment(value.finished_at),
    code: nullable(value.code, 256),
    stop_requested: value.stop_requested,
  };
}

function parseApproval(raw: unknown): ApprovalRecord {
  const value = object_(raw);
  for (const key of ["request_id", "run_id", "call_id", "name", "arguments_hash", "tool_hash"]) {
    if (typeof value[key] !== "string" || !(value[key] as string).length) fail("子代理审批无效");
  }
  if (typeof value.arguments !== "object" || value.arguments === null || Array.isArray(value.arguments)) fail("子代理审批无效");
  if (value.expires_at !== null && typeof value.expires_at !== "number") fail("子代理审批无效");
  return value as unknown as ApprovalRecord;
}

/**
 * 单成员详情：返回全量历史——消息、结果、事件完整保留不截断；消息与
 * 结果按 seq 升序去重；事件、审批只能来自详情声明的run（run_id字段或
 * run.id），run为空时不允许残留审批。
 */
export function parseSubagentDetail(raw: unknown, agentId?: string): SubagentDetail {
  const value = object_(raw);
  exact(value, ["agent", "messages", "results", "events", "approvals", "run"], ["run_id", "events_truncated"]);
  const agent = parseSubagentMember(value.agent);
  if (agentId !== undefined && agent.agent_id !== agentId) fail("子代理身份不匹配");
  for (const key of ["messages", "results", "events", "approvals"]) {
    if (!Array.isArray(value[key])) fail("子代理详情无效");
  }
  const run = value.run === null ? null : parseRunView(value.run);
  const declared = typeof value.run_id === "string" && value.run_id ? bounded(value.run_id, 256) : run === null ? null : run.id;
  const truncated = value.events_truncated === undefined ? false : value.events_truncated;
  if (typeof truncated !== "boolean") fail("子代理详情无效");
  if ((run === null) !== (declared === null) || run && declared !== run.id) fail("子代理运行身份不匹配");
  const approvals = (value.approvals as unknown[]).map(parseApproval);
  if (approvals.some(approval => declared === null || approval.run_id !== declared)) fail("子代理审批跨运行");
  const messages = (value.messages as unknown[]).map(parseMessage);
  const results = (value.results as unknown[]).map(parseResult);
  const events = (value.events as unknown[]).map(parseEvent);
  const messageSeqs = new Set<number>();
  const resultIds = new Set<string>();
  let previousMessage = 0, previousResult = 0;
  for (const message of messages) {
    if (messageSeqs.has(message.seq) || message.seq < previousMessage) fail("子代理消息无效");
    messageSeqs.add(message.seq); previousMessage = message.seq;
  }
  for (const result of results) {
    if (resultIds.has(result.notification_id) || result.seq < previousResult) fail("子代理结果无效");
    resultIds.add(result.notification_id); previousResult = result.seq;
  }
  const positions = new Set<string>();
  const eventIds = new Set<string>();
  for (const event of events) {
    if (declared === null || event.run_id !== declared) fail("子代理事件跨运行");
    const position = event.run_id + "#" + String(event.seq);
    if (positions.has(position) || eventIds.has(event.event_id)) fail("子代理事件重复");
    positions.add(position); eventIds.add(event.event_id);
  }
  return { agent, messages, results, events, approvals, run, run_id: declared, events_truncated: truncated };
}

export function parseSubagentSendReceipt(raw: unknown, agentId?: string): SubagentSendReceipt {
  const value = object_(raw);
  exact(value, ["agent_id", "message_id", "seq", "accepted", "started", "delivery"],
    ["task_id", "created", "wake"]);
  if (value.accepted !== true || typeof value.started !== "boolean") fail("子代理发送回执无效");
  if (!["next_input_boundary", "queued", "started"].includes(String(value.delivery))) fail("子代理发送回执无效");
  const agent = bounded(value.agent_id, 256);
  if (agentId !== undefined && agent !== agentId) fail("子代理身份不匹配");
  return {
    agent_id: agent,
    message_id: bounded(value.message_id, 256),
    seq: integer(value.seq, 1),
    accepted: true,
    started: value.started,
    delivery: value.delivery as SubagentDelivery,
  };
}

/** 控制回执只承诺真实 agent 归属；interrupt/close 的域内字段保持原样返回。 */
export function parseSubagentControlReceipt(raw: unknown, agentId?: string): Record<string, unknown> {
  const value = object_(raw);
  if (typeof value.agent_id !== "string" || !value.agent_id) fail("子代理控制回执无效");
  if (agentId !== undefined && value.agent_id !== agentId) fail("子代理身份不匹配");
  return value;
}

/** 审批复执是域内结果字典（request_key 回显或冲突说明），不做更细的结构假设。 */
export function parseSubagentApprovalReply(raw: unknown): Record<string, unknown> {
  return object_(raw);
}

const TASK_STATES: Record<string, SubagentState> = {
  accepted: "running",
  running: "running",
  waiting_input: "running",
  awaiting_approval: "approval",
  completed: "completed",
  aborted: "idle",
  error: "error",
  max_tokens: "error",
  refusal: "error",
  interrupted: "idle",
};

export function subagentState(member: SubagentMember): SubagentState {
  if (member.status === "closed") return "closed";
  const state = member.task ? TASK_STATES[member.task.status] : undefined;
  return state ?? "idle";
}
export function memberTitle(member: SubagentMember): string {
  const title = typeof member.title === "string" ? member.title.trim() : "";
  return title || member.path;
}
