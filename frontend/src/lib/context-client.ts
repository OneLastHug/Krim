import { object, request } from "../api";

export interface CompactionReceipt {
  request_key: string;
  session_id: string;
  revision: number;
  reason: string | null;
  status: "pending" | "fulfilled";
  created_at: number;
  created: boolean;
}

/** Enqueue only; actual compaction runs at a safe model-request boundary. */
export async function scheduleCompaction(
  apiKey: string, workspaceId: string, sessionId: string, requestKey: string,
): Promise<CompactionReceipt> {
  const path = "/v2/workspaces/" + encodeURIComponent(workspaceId)
    + "/sessions/" + encodeURIComponent(sessionId) + "/context/compactions";
  const value = object(await request(path, apiKey, { request_key: requestKey }));
  const fields = ["request_key", "session_id", "revision", "reason", "status", "created_at", "created"];
  if (Object.keys(value).length !== fields.length || fields.some(field => !(field in value))
    || value.request_key !== requestKey || value.session_id !== sessionId
    || !Number.isSafeInteger(value.revision) || (value.revision as number) < 1
    || (value.reason !== null && typeof value.reason !== "string")
    || (value.status !== "pending" && value.status !== "fulfilled")
    || typeof value.created_at !== "number" || !Number.isFinite(value.created_at)
    || typeof value.created !== "boolean") {
    throw new Error("整理请求响应不匹配，请重试原请求");
  }
  return value as unknown as CompactionReceipt;
}
