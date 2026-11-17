// 会话计划只读数据合同：严格解析服务端单页快照，不含请求、组件或编辑入口。
export const PLAN_PAGE_MAX = 200;
export const PLAN_STATUSES = ["pending", "in_progress", "completed", "blocked"] as const;
export type PlanStatus = (typeof PLAN_STATUSES)[number];

export interface PlanStep {
  id: string;
  text: string;
  status: PlanStatus;
}

export interface PlanCounts {
  pending: number;
  in_progress: number;
  completed: number;
  blocked: number;
}

export interface PlanSnapshot {
  version: "planning/1";
  session_id: string;
  revision: number;
  content_hash: string;
  updated_at: number | null;
  counts: PlanCounts;
  total: number;
  matched_total: number;
  steps: PlanStep[];
  offset: number;
  limit: number;
  next_offset: number | null;
}

export interface PlanReadQuery {
  revision?: number;
  offset?: number;
  limit?: number;
  statuses?: readonly PlanStatus[];
}

function fail(): never { throw new Error("计划响应无效"); }
const text = (value: unknown): string => {
  if (typeof value !== "string") fail();
  return value;
};
const integer = (value: unknown, max = Number.MAX_SAFE_INTEGER): number => {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0 || value > max) fail();
  return value;
};
const object = (value: unknown): Record<string, unknown> => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) fail();
  return value as Record<string, unknown>;
};
const exact = (value: Record<string, unknown>, fields: readonly string[]): void => {
  if (Object.keys(value).length !== fields.length || fields.some(field => !(field in value))) fail();
};
const planStatus = (value: unknown): PlanStatus => {
  if (typeof value !== "string" || !(PLAN_STATUSES as readonly string[]).includes(value)) fail();
  return value as PlanStatus;
};

function parseCounts(raw: unknown): PlanCounts {
  const value = object(raw);
  exact(value, PLAN_STATUSES);
  return {
    pending: integer(value.pending),
    in_progress: integer(value.in_progress),
    completed: integer(value.completed),
    blocked: integer(value.blocked),
  };
}

function parseStep(raw: unknown): PlanStep {
  const value = object(raw);
  exact(value, ["id", "text", "status"]);
  const id = text(value.id);
  if (!/^[A-Za-z0-9_.:-]{1,128}$/.test(id)) fail();
  const stepText = text(value.text);
  if (!stepText.trim()) fail();
  return { id, text: stepText, status: planStatus(value.status) };
}

export function parsePlanSnapshot(raw: unknown, expectedSessionId?: string): PlanSnapshot {
  const value = object(raw);
  exact(value, [
    "version", "session_id", "revision", "content_hash", "updated_at", "counts",
    "total", "matched_total", "steps", "offset", "limit", "next_offset",
  ]);
  if (value.version !== "planning/1") fail();
  const sessionId = text(value.session_id);
  if (!sessionId) fail();
  if (expectedSessionId !== undefined && sessionId !== expectedSessionId) fail();

  const revision = integer(value.revision);
  const hash = text(value.content_hash);
  if (!/^[0-9a-f]{64}$/.test(hash)) fail();

  const updatedAt = value.updated_at;
  if (updatedAt !== null && (typeof updatedAt !== "number" || !Number.isFinite(updatedAt) || updatedAt < 0)) fail();

  const counts = parseCounts(value.counts);
  const total = integer(value.total);
  const matchedTotal = integer(value.matched_total);
  if (counts.pending + counts.in_progress + counts.completed + counts.blocked !== total) fail();
  if (matchedTotal > total) fail();

  if (!Array.isArray(value.steps) || value.steps.length > PLAN_PAGE_MAX) fail();
  const steps = value.steps.map(parseStep);
  if (new Set(steps.map(step => step.id)).size !== steps.length) fail();

  const offset = integer(value.offset);
  const limit = integer(value.limit, PLAN_PAGE_MAX);
  if (limit < 1) fail();
  const expectedLength = Math.min(limit, Math.max(0, matchedTotal - offset));
  if (steps.length !== expectedLength) fail();
  const end = offset + steps.length;
  if (!Number.isSafeInteger(end)) fail();

  const nextOffset = value.next_offset === null ? null : integer(value.next_offset);
  if (end < matchedTotal ? nextOffset !== end : nextOffset !== null) fail();

  return {
    version: "planning/1",
    session_id: sessionId,
    revision,
    content_hash: hash,
    updated_at: updatedAt as number | null,
    counts,
    total,
    matched_total: matchedTotal,
    steps,
    offset,
    limit,
    next_offset: nextOffset,
  };
}

/**
 * 合并同一版本、同一查询的后续页，返回仅用于展示的快照。
 * 合并后 offset 固定为 0，next_offset 等于已加载步骤数；steps 可能超过单页 limit，
 * 因此展示快照不得再交给 parsePlanSnapshot 校验。
 */
export function mergePlanPage(current: PlanSnapshot, next: PlanSnapshot): PlanSnapshot {
  if (next.session_id !== current.session_id || next.revision !== current.revision ||
      next.content_hash !== current.content_hash || next.total !== current.total ||
      next.matched_total !== current.matched_total || next.offset !== current.steps.length ||
      PLAN_STATUSES.some(status => next.counts[status] !== current.counts[status])) fail();
  const seen = new Set(current.steps.map(step => step.id));
  for (const step of next.steps) {
    if (seen.has(step.id)) fail();
    seen.add(step.id);
  }
  const steps = [...current.steps, ...next.steps];
  return {
    ...current,
    updated_at: next.updated_at,
    steps,
    offset: 0,
    limit: next.limit,
    next_offset: next.next_offset === null ? null : steps.length,
  };
}
