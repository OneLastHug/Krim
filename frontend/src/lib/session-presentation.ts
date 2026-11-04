import type { Turn } from "./session-contracts.ts";

export type ToolState = "preparing" | "approval" | "running" | "ok" | "denied" | "failed" | "cancelled" | "unknown";

export interface ApprovalRecord extends Record<string, unknown> {
  request_id: string;
  run_id: string;
  call_id: string;
  name: string;
  arguments: Record<string, unknown>;
  arguments_hash: string;
  tool_hash: string;
  expires_at: number;
}

export interface AssistantPresentation {
  type: "assistant";
  key: string;
  text: string;
  partial: boolean;
}

export interface ToolPresentation {
  type: "tool";
  key: string;
  callId?: string;
  providerCallId?: string;
  stepIndex: number;
  name: string;
  state: ToolState;
  arguments?: unknown;
  result?: unknown;
  resultAvailable: boolean;
  executorStarted?: boolean | null;
  code?: string;
  risk?: unknown;
  source?: unknown;
  approval?: ApprovalRecord;
}

export type TurnPresentation = AssistantPresentation | ToolPresentation;
export interface PresentationDetail {
  facts?: Record<string, unknown> | null;
  events?: Record<string, unknown>[];
}

interface EventRecord {
  run_id: string;
  seq: number;
  event_id?: string;
  kind: string;
  data: Record<string, unknown>;
}
interface ToolAccumulator {
  callId?: string;
  providerCallId?: string;
  stepIndex: number;
  name: string;
  arguments?: unknown;
  result?: unknown;
  resultAvailable: boolean;
  executorStarted?: boolean | null;
  code?: string;
  risk?: unknown;
  source?: unknown;
  state: ToolState;
  terminal: boolean;
  conflict: boolean;
  order: number;
  orderIndex: number;
  approval?: ApprovalRecord;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);
const nonempty = (value: unknown): value is string => typeof value === "string" && value.length > 0;
const step = (value: unknown): value is number => Number.isSafeInteger(value) && Number(value) >= 0;
const has = (value: object, key: PropertyKey) => Object.prototype.hasOwnProperty.call(value, key);

function stable(value: unknown): string {
  if (value === null) return "null";
  if (Array.isArray(value)) return `[${value.map(stable).join(",")}]`;
  if (isRecord(value)) return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${stable(value[key])}`).join(",")}}`;
  if (typeof value === "string") return JSON.stringify(value);
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return String(value);
}
function parseJson(value: unknown): unknown {
  if (typeof value !== "string") return value;
  try { return JSON.parse(value) as unknown; } catch { return value; }
}
function eventRecords(turn: Turn, events: unknown[], detail?: PresentationDetail): EventRecord[] {
  if (!turn.run_id) return [];
  const source = [...events, ...(detail?.events ?? []), ...(Array.isArray(detail?.facts?.events) ? detail!.facts!.events as unknown[] : [])];
  const bySequence = new Map<number, EventRecord>();
  const conflictingSequences = new Set<number>();
  const byEventId = new Map<string, EventRecord>();
  const conflictingIds = new Set<string>();
  for (const raw of source) {
    if (!isRecord(raw) || raw.run_id !== turn.run_id || !step(raw.seq) || !nonempty(raw.kind) || !isRecord(raw.data)) continue;
    const event: EventRecord = {
      run_id: turn.run_id,
      seq: raw.seq,
      ...(nonempty(raw.event_id) ? { event_id: raw.event_id } : {}),
      kind: raw.kind,
      data: raw.data,
    };
    const oldSequence = bySequence.get(event.seq);
    if (oldSequence && stable(oldSequence) !== stable(event)) conflictingSequences.add(event.seq);
    else bySequence.set(event.seq, event);
    if (event.event_id) {
      const oldId = byEventId.get(event.event_id);
      if (oldId && stable(oldId) !== stable(event)) conflictingIds.add(event.event_id);
      else byEventId.set(event.event_id, event);
    }
  }
  return [...bySequence.values()]
    .filter(event => !conflictingSequences.has(event.seq) && !(event.event_id && conflictingIds.has(event.event_id)))
    .sort((a, b) => a.seq - b.seq);
}

function toolStatus(value: unknown): ToolState | null {
  return value === "ok" || value === "denied" || value === "failed" || value === "cancelled" || value === "unknown" ? value : null;
}
function approvalRecord(raw: unknown, runId: string): ApprovalRecord | null {
  if (!isRecord(raw) || raw.run_id !== runId || !nonempty(raw.request_id) || !nonempty(raw.call_id) ||
    !nonempty(raw.name) || !isRecord(raw.arguments) || !/^[a-f0-9]{64}$/.test(String(raw.arguments_hash)) ||
    !/^[a-f0-9]{64}$/.test(String(raw.tool_hash)) || typeof raw.expires_at !== "number" || !Number.isFinite(raw.expires_at)) return null;
  return raw as ApprovalRecord;
}
function sameOptional(left: unknown, right: unknown): boolean {
  return left === undefined || right === undefined || stable(left) === stable(right);
}
function stateFromFact(value: unknown): ToolState {
  return toolStatus(value) ?? "unknown";
}

/**
 * 将正式会话的一轮文本、工具事件和结果事实投影成纯展示数据。
 * 这里只读投影，不把历史审批事件变成新的审批能力，也不重放工具。
 */
export function projectTurn(
  turn: Turn,
  events: unknown[],
  pendingApprovals: unknown[],
  detail?: PresentationDetail,
): TurnPresentation[] {
  if (!turn.run_id) return [];
  const timeline = eventRecords(turn, events, detail);
  const tools: ToolAccumulator[] = [];
  const byCallId = new Map<string, ToolAccumulator>();
  const byProviderId = new Map<string, ToolAccumulator>();
  const maximumSequence = timeline.at(-1)?.seq ?? -1;

  const markConflict = (item: ToolAccumulator) => {
    item.conflict = true;
    item.terminal = true;
    item.state = "unknown";
    item.resultAvailable = false;
    delete item.result;
    delete item.approval;
  };

  function upsert(candidate: {
    callId?: string; providerCallId?: string; stepIndex?: number; name?: string;
    arguments?: unknown; order: number; orderIndex?: number;
  }): ToolAccumulator | null {
    if (!candidate.callId && !candidate.providerCallId) return null;
    const viaCall = candidate.callId ? byCallId.get(candidate.callId) : undefined;
    const viaProvider = candidate.providerCallId ? byProviderId.get(candidate.providerCallId) : undefined;
    if (viaCall && viaProvider && viaCall !== viaProvider) {
      markConflict(viaCall); markConflict(viaProvider); return null;
    }
    const item = viaCall ?? viaProvider;
    if (item) {
      if ((candidate.callId && item.callId && candidate.callId !== item.callId) ||
        (candidate.providerCallId && item.providerCallId && candidate.providerCallId !== item.providerCallId) ||
        (step(candidate.stepIndex) && item.stepIndex >= 0 && candidate.stepIndex !== item.stepIndex) ||
        !sameOptional(candidate.name, item.name || undefined) || !sameOptional(candidate.arguments, item.arguments)) {
        markConflict(item);
        return item;
      }
      if (candidate.callId && !item.callId) { item.callId = candidate.callId; byCallId.set(candidate.callId, item); }
      if (candidate.providerCallId && !item.providerCallId) { item.providerCallId = candidate.providerCallId; byProviderId.set(candidate.providerCallId, item); }
      if (step(candidate.stepIndex) && item.stepIndex < 0) item.stepIndex = candidate.stepIndex;
      if (candidate.name && !item.name) item.name = candidate.name;
      if (candidate.arguments !== undefined && item.arguments === undefined) item.arguments = candidate.arguments;
      if (candidate.order < item.order || candidate.order === item.order && (candidate.orderIndex ?? 0) < item.orderIndex) {
        item.order = candidate.order; item.orderIndex = candidate.orderIndex ?? 0;
      }
      return item;
    }
    const created: ToolAccumulator = {
      ...(candidate.callId ? { callId: candidate.callId } : {}),
      ...(candidate.providerCallId ? { providerCallId: candidate.providerCallId } : {}),
      stepIndex: step(candidate.stepIndex) ? candidate.stepIndex : -1,
      name: candidate.name ?? "",
      ...(candidate.arguments !== undefined ? { arguments: candidate.arguments } : {}),
      resultAvailable: false,
      state: "preparing",
      terminal: false,
      conflict: false,
      order: candidate.order,
      orderIndex: candidate.orderIndex ?? 0,
    };
    tools.push(created);
    if (created.callId) byCallId.set(created.callId, created);
    if (created.providerCallId) byProviderId.set(created.providerCallId, created);
    return created;
  }

  function setTerminal(item: ToolAccumulator, state: ToolState, data: Record<string, unknown>) {
    if (item.conflict) return;
    const executorStarted = data.executor_started;
    const code = typeof data.code === "string" ? data.code : undefined;
    const available = data.result_available;
    if (item.terminal && (item.state !== state ||
      (typeof item.executorStarted === "boolean" && typeof executorStarted === "boolean" && item.executorStarted !== executorStarted) ||
      (item.code !== undefined && code !== undefined && item.code !== code) ||
      (item.resultAvailable !== (available === true) && (available === true || available === false)) ||
      (item.resultAvailable && available === true && has(data, "value") && item.result !== undefined && stable(item.result) !== stable(data.value)))) {
      markConflict(item);
      return;
    }
    item.terminal = true;
    item.state = state;
    if (typeof executorStarted === "boolean" || executorStarted === null) item.executorStarted = executorStarted;
    if (code !== undefined) item.code = code;
    if (available === true) {
      item.resultAvailable = true;
      if (has(data, "value")) item.result = data.value;
    } else if (available === false) {
      item.resultAvailable = false;
      delete item.result;
    }
  }

  const textByStep = new Map<number, { text: string; firstSeq: number }>();
  for (const event of timeline) {
    const data = event.data;
    const index = step(data.step_index) ? data.step_index : -1;
    if (event.kind === "text" && typeof data.text === "string" && index >= 0) {
      const current = textByStep.get(index);
      if (current) current.text += data.text;
      else textByStep.set(index, { text: data.text, firstSeq: event.seq });
      continue;
    }
    if (event.kind === "model_end" && Array.isArray(data.tool_calls)) {
      data.tool_calls.forEach((rawCall, callIndex) => {
        if (!isRecord(rawCall) || rawCall.kind !== "tool_call" || !nonempty(rawCall.id) || !nonempty(rawCall.name) || !isRecord(rawCall.arguments)) return;
        upsert({ providerCallId: rawCall.id, stepIndex: index, name: rawCall.name, arguments: rawCall.arguments, order: event.seq, orderIndex: callIndex });
      });
      continue;
    }
    if (event.kind === "tool_start" || event.kind === "tool_end") {
      const callId = nonempty(data.call_id) ? data.call_id : undefined;
      const providerCallId = nonempty(data.provider_call_id) ? data.provider_call_id : undefined;
      const name = nonempty(data.name) ? data.name : undefined;
      const args = isRecord(data.arguments) ? data.arguments : undefined;
      const item = upsert({ callId, providerCallId, stepIndex: index, name, arguments: args, order: event.seq });
      if (!item) continue;
      if (event.kind === "tool_start") {
        if (!item.terminal && !item.conflict) item.state = "preparing";
      } else {
        const status = toolStatus(data.status);
        if (status) setTerminal(item, status, data);
      }
      continue;
    }
    if (event.kind === "approval_request") {
      const metadata = approvalRecord(data, turn.run_id);
      if (!metadata) continue;
      const item = upsert({ callId: metadata.call_id, name: metadata.name, arguments: metadata.arguments, order: event.seq });
      if (!item || item.conflict || item.terminal) continue;
      if (has(data, "risk")) item.risk = data.risk;
      if (has(data, "source")) item.source = data.source;
      continue;
    }
    if (event.kind === "tool_intent" && nonempty(data.call_id)) {
      const item = byCallId.get(data.call_id);
      if (item && !item.terminal && !item.conflict) item.state = "running";
    }
  }

  const facts = detail?.facts;
  const factsRun = isRecord(facts?.run) ? facts!.run as Record<string, unknown> : null;
  if (facts && factsRun?.id === turn.run_id && Array.isArray(facts.tools)) {
    for (const raw of facts.tools) {
      if (!isRecord(raw) || !nonempty(raw.call_id) || !nonempty(raw.name)) continue;
      const args = isRecord(raw.arguments) ? raw.arguments : undefined;
      const item = upsert({ callId: raw.call_id, providerCallId: nonempty(raw.provider_call_id) ? raw.provider_call_id : undefined,
        stepIndex: step(raw.step_index) ? raw.step_index : undefined, name: raw.name, arguments: args, order: maximumSequence + 1, orderIndex: tools.length });
      if (!item) continue;
      const status = stateFromFact(raw.status);
      setTerminal(item, status, raw);
    }
  }

  const pending: ApprovalRecord[] = [];
  for (const raw of pendingApprovals) {
    const approval = approvalRecord(raw, turn.run_id);
    if (approval) pending.push(approval);
  }
  for (const approval of pending) {
    const item = upsert({ callId: approval.call_id, name: approval.name, arguments: approval.arguments,
      order: maximumSequence + 1, orderIndex: tools.length });
    if (!item || item.terminal || item.conflict || (item.name && item.name !== approval.name) || !sameOptional(item.arguments, approval.arguments)) continue;
    item.approval = approval;
    if (approval.risk !== undefined) item.risk = approval.risk;
    if (approval.source !== undefined) item.source = approval.source;
    item.state = "approval";
  }

  const validTools = tools.filter(item => !item.conflict);
  if (turn.status === "completed" && Array.isArray(turn.transcript)) {
    const resultByProvider = new Map<string, unknown>();
    for (const message of turn.transcript) {
      if (message.role === "tool" && nonempty(message.call_id)) resultByProvider.set(message.call_id, parseJson(message.content));
    }
    const result: TurnPresentation[] = [];
    turn.transcript.forEach((message, messageIndex) => {
      if (message.role !== "assistant") return;
      if (message.content) result.push({ type: "assistant", key: `assistant:${messageIndex}`, text: message.content, partial: false });
      for (const rawCall of message.tool_calls) {
        if (!isRecord(rawCall) || rawCall.kind !== "tool_call" || !nonempty(rawCall.id) || !nonempty(rawCall.name) || !isRecord(rawCall.arguments)) continue;
        const providerCallId = rawCall.id;
        const args = rawCall.arguments;
        const item = byProviderId.get(providerCallId);
        const transcriptHasResult = resultByProvider.has(providerCallId);
        const transcriptResult = resultByProvider.get(providerCallId);
        const conflict = !!item && (item.conflict || (item.name !== "" && item.name !== rawCall.name) ||
          (item.arguments !== undefined && stable(item.arguments) !== stable(args)) ||
          (item.terminal && item.state !== "ok") ||
          (item.resultAvailable && transcriptHasResult && stable(item.result) !== stable(transcriptResult)));
        const available = !conflict && (item?.resultAvailable === true || transcriptHasResult);
        const value = item?.resultAvailable === true ? item.result : transcriptResult;
        result.push({
          type: "tool",
          key: `tool:${item?.callId ?? providerCallId}`,
          ...(item?.callId ? { callId: item.callId } : {}),
          providerCallId,
          stepIndex: item && item.stepIndex >= 0 ? item.stepIndex : -1,
          name: item?.name || rawCall.name,
          state: conflict ? "unknown" : item?.terminal ? item.state : available ? "ok" : "unknown",
          arguments: item?.arguments ?? args,
          ...(available ? { result: value } : {}),
          resultAvailable: available,
          ...(item?.executorStarted !== undefined ? { executorStarted: item.executorStarted } : {}),
          ...(item?.code ? { code: item.code } : {}),
          ...(item?.risk !== undefined ? { risk: item.risk } : {}),
          ...(item?.source !== undefined ? { source: item.source } : {}),
        });
      }
    });
    return result;
  }

  const result: { order: number; orderIndex: number; item: TurnPresentation }[] = [];
  for (const [stepIndex, segment] of textByStep) {
    result.push({ order: segment.firstSeq, orderIndex: -1, item: { type: "assistant", key: `assistant:step:${stepIndex}`, text: segment.text, partial: true } });
  }
  for (const item of validTools) {
    result.push({ order: item.order, orderIndex: item.orderIndex, item: {
      type: "tool",
      key: `tool:${item.callId ?? item.providerCallId}`,
      ...(item.callId ? { callId: item.callId } : {}),
      ...(item.providerCallId ? { providerCallId: item.providerCallId } : {}),
      stepIndex: item.stepIndex,
      name: item.name,
      state: item.conflict ? "unknown" : item.state,
      ...(item.arguments !== undefined ? { arguments: item.arguments } : {}),
      ...(item.resultAvailable ? { result: item.result } : {}),
      resultAvailable: item.resultAvailable,
      ...(item.executorStarted !== undefined ? { executorStarted: item.executorStarted } : {}),
      ...(item.code ? { code: item.code } : {}),
      ...(item.approval ? { approval: item.approval } : {}),
      ...(item.risk !== undefined ? { risk: item.risk } : {}),
      ...(item.source !== undefined ? { source: item.source } : {}),
    } });
  }
  return result.sort((a, b) => a.order - b.order || a.orderIndex - b.orderIndex).map(entry => entry.item);
}
