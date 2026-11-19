import { request } from "../api";
import {
  PLAN_PAGE_MAX,
  PLAN_STATUSES,
  parsePlanSnapshot,
  type PlanReadQuery,
  type PlanSnapshot,
} from "./planning-contracts";

function planPath(workspaceId: string, sessionId: string): string {
  if (!workspaceId || !sessionId) throw new Error("计划查询无效");
  return "/v2/workspaces/" + encodeURIComponent(workspaceId) + "/sessions/" +
    encodeURIComponent(sessionId) + "/plan";
}

function planQuery(query: PlanReadQuery): string {
  const params = new URLSearchParams();
  if (query.revision !== undefined) {
    if (!Number.isSafeInteger(query.revision) || query.revision < 0) throw new Error("计划查询无效");
    params.set("revision", String(query.revision));
  }
  if (query.offset !== undefined) {
    if (!Number.isSafeInteger(query.offset) || query.offset < 0) throw new Error("计划查询无效");
    if (query.offset > 0 && query.revision === undefined) throw new Error("计划查询无效");
    params.set("offset", String(query.offset));
  }
  if (query.limit !== undefined) {
    if (!Number.isSafeInteger(query.limit) || query.limit < 1 || query.limit > PLAN_PAGE_MAX) throw new Error("计划查询无效");
    params.set("limit", String(query.limit));
  }
  if (query.statuses !== undefined) {
    if (query.statuses.length > PLAN_STATUSES.length ||
        new Set(query.statuses).size !== query.statuses.length ||
        query.statuses.some(status => !(PLAN_STATUSES as readonly string[]).includes(status))) {
      throw new Error("计划查询无效");
    }
    for (const status of query.statuses) params.append("statuses", status);
  }
  const text = params.toString();
  return text ? "?" + text : "";
}

/** GET a single immutable plan page; counts and matched_total always describe the whole revision. */
export async function readPlan(
  apiKey: string,
  workspaceId: string,
  sessionId: string,
  query: PlanReadQuery = {},
  signal?: AbortSignal,
): Promise<PlanSnapshot> {
  const path = planPath(workspaceId, sessionId) + planQuery(query);
  return parsePlanSnapshot(await request(path, apiKey, undefined, signal), sessionId);
}
