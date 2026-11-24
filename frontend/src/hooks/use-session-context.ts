import { useEffect, useState } from "react";
import { request } from "../api";
import { chatReason } from "../lib/copy";
import { turnPath } from "../lib/session-contracts";
import { parseTurnObservations, type ObsRequestRecord, type ObsTurnObservations } from "../observability/obs-contracts";
import type { SessionState } from "./use-session";

export interface ReportedContextUsage {
  input: number | null;
  output: number | null;
  total: number | null;
  cached: number | null;
  cacheWrite: number | null;
  reasoning: number | null;
  final: boolean | null;
}

export function contextRequestSnapshot(requests: readonly ObsRequestRecord[]) {
  const request = [...requests].reverse().find(item => item.purpose !== "compaction_summary" && item.purpose !== "summary") ?? null;
  const usage = request?.usage?.usage;
  const reported: ReportedContextUsage | null = usage ? {
    input: usage.input_tokens, output: usage.output_tokens, total: usage.total_tokens,
    cached: usage.cached_input_tokens, cacheWrite: usage.cache_write_input_tokens,
    reasoning: usage.reasoning_output_tokens, final: request!.usage!.usage_final,
  } : null;
  // Keep estimate and reported usage paired to the same request, including missing manifests.
  return { requestId: request?.id ?? "", manifest: request?.manifest ?? null, reported };
}

export function useSessionContext(chat: SessionState, apiKey: string, active: boolean) {
  // 运行中的当前轮优先：长 run 期间侧栏读当前轮观测；结束后按终态选轮，再读最终一次。
  const liveTurn = chat.currentTurn && ["queued", "running"].includes(chat.currentTurn.status) && !!chat.currentTurn.run_id ? chat.currentTurn : null;
  const terminal = [...chat.history].reverse().find(turn => !["queued", "running"].includes(turn.status) && !!turn.run_id);
  const turnId = (liveTurn ?? terminal)?.id ?? "";
  // 事件边界：仅当前轮新增 model_start/usage 事件才重读；事件按批到达，不逐 token 拉取。
  const boundary = liveTurn ? chat.events.filter(event => event.kind === "model_start" || event.kind === "usage").length : 0;
  // 身份键不含 boundary/status：边界与终态刷新复用同轮旧快照，身份或轮次变化则不沿用。
  const owner = JSON.stringify([apiKey, chat.actor?.principal_id, chat.actor?.generation, chat.workspace, chat.id, turnId]);
  const scope = JSON.stringify([owner, (liveTurn ?? terminal)?.status, boundary]);
  const [revision, setRevision] = useState(0);
  const query = JSON.stringify([scope, revision]);
  const [reading, setReading] = useState<{ query: string; owner: string; value: ObsTurnObservations | null; error: string; loading: boolean }>({ query: "", owner: "", value: null, error: "", loading: false });
  useEffect(() => {
    if (!active || !chat.ready || !chat.session || !turnId) return;
    const controller = new AbortController();
    setReading(previous => ({ query, owner, value: previous.owner === owner ? previous.value : null, error: "", loading: true }));
    void request(turnPath(chat.workspace, chat.id, turnId) + "/observations", apiKey, undefined, controller.signal).then(raw => {
      const value = parseTurnObservations(raw, { session_id: chat.id, turn_id: turnId });
      if (!controller.signal.aborted) setReading({ query, owner, value, error: "", loading: false });
    }).catch(error => { if (!controller.signal.aborted) setReading({ query, owner, value: null, error: chatReason(error), loading: false }); });
    return () => controller.abort();
  }, [active, chat.ready, chat.session?.id, query, apiKey, chat.workspace, chat.id, turnId]);
  // 同轮边界刷新期间保留上一次快照，侧栏不在读取间隙闪回"—"；换身份或轮次不沿用旧数据。
  const data = chat.ready && (reading.query === query || reading.loading && reading.owner === owner) ? reading.value : null;
  const { manifest, reported, requestId } = contextRequestSnapshot(data?.requests ?? []);
  const limit = chat.session?.context_policy.compaction_trigger_tokens ?? chat.session?.context_policy.max_tokens
    ?? (manifest?.model_context_window ? Math.max(0, manifest.model_context_window - (manifest.reserved_output_tokens ?? 0)) : null);
  // This is the last request's input snapshot, not a preview of the next unsent request.
  const used = manifest?.estimated_tokens ?? (!turnId && chat.ready && !chat.reading ? 0 : null);
  return { data, manifest, reported, requestId, turnId, limit, used, loading: !!turnId && active && (reading.query !== query || reading.loading), error: reading.query === query ? reading.error : "", refresh: () => setRevision(value => value + 1) };
}
export type SessionContextState = ReturnType<typeof useSessionContext>;
