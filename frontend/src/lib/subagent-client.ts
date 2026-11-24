// 子代理控制面请求：全部经统一 request 入口，回执一律先过合同解析。
import { request } from "../api";
import {
  SUBAGENT_PAGE_MAX,
  parseSubagentApprovalReply,
  parseSubagentControlReceipt,
  parseSubagentDetail,
  parseSubagentPage,
  parseSubagentSendReceipt,
  type SubagentPage,
  type SubagentDetail,
  type SubagentSendReceipt,
  type WakeMode,
} from "./subagent-contracts";

export interface SubagentListQuery { afterPath?: string; limit?: number }
export interface SubagentSendBody { text: string; wake: WakeMode; request_key: string }
export interface SubagentControlBody { activation_id?: string | null; task_id?: string | null }
export interface SubagentApprovalBody {
  request_key: string;
  call_id: string;
  arguments_hash: string;
  tool_hash: string;
  allow: boolean;
  scope?: "once" | "session";
}

function subagentBase(workspaceId: string, sessionId: string): string {
  if (!workspaceId || !sessionId) throw new Error("子代理查询无效");
  return "/v2/workspaces/" + encodeURIComponent(workspaceId) + "/sessions/" + encodeURIComponent(sessionId) + "/subagents";
}

export function subagentPath(workspaceId: string, sessionId: string, agentId?: string): string {
  const base = subagentBase(workspaceId, sessionId);
  return agentId === undefined ? base : base + "/" + encodeURIComponent(agentId);
}

export async function listSubagents(
  apiKey: string,
  workspaceId: string,
  sessionId: string,
  query: SubagentListQuery = {},
  signal?: AbortSignal,
): Promise<SubagentPage> {
  const params = new URLSearchParams();
  if (query.afterPath !== undefined) {
    if (typeof query.afterPath !== "string" || !query.afterPath || query.afterPath.length > 256) throw new Error("子代理查询无效");
    params.set("after_path", query.afterPath);
  }
  if (query.limit !== undefined) {
    if (!Number.isSafeInteger(query.limit) || query.limit < 1 || query.limit > SUBAGENT_PAGE_MAX) throw new Error("子代理查询无效");
    params.set("limit", String(query.limit));
  }
  const suffix = params.toString();
  return parseSubagentPage(await request(subagentBase(workspaceId, sessionId) + (suffix ? "?" + suffix : ""), apiKey, undefined, signal));
}

export async function readSubagentDetail(
  apiKey: string,
  workspaceId: string,
  sessionId: string,
  agentId: string,
  signal?: AbortSignal,
): Promise<SubagentDetail> {
  return parseSubagentDetail(await request(subagentPath(workspaceId, sessionId, agentId), apiKey, undefined, signal), agentId);
}

export async function sendSubagentMessage(
  apiKey: string,
  workspaceId: string,
  sessionId: string,
  agentId: string,
  body: SubagentSendBody,
): Promise<SubagentSendReceipt> {
  return parseSubagentSendReceipt(
    await request(subagentPath(workspaceId, sessionId, agentId) + "/messages", apiKey, body),
    agentId,
  );
}

export async function controlSubagent(
  apiKey: string,
  workspaceId: string,
  sessionId: string,
  agentId: string,
  action: "interrupt" | "close",
  body: SubagentControlBody = {},
): Promise<Record<string, unknown>> {
  return parseSubagentControlReceipt(
    await request(subagentPath(workspaceId, sessionId, agentId) + "/" + action, apiKey, body),
    agentId,
  );
}

export async function replySubagentApproval(
  apiKey: string,
  workspaceId: string,
  sessionId: string,
  agentId: string,
  requestId: string,
  body: SubagentApprovalBody,
): Promise<Record<string, unknown>> {
  return parseSubagentApprovalReply(
    await request(subagentPath(workspaceId, sessionId, agentId) + "/approvals/" + encodeURIComponent(requestId), apiKey, body),
  );
}
