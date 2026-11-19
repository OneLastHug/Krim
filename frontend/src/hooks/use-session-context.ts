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
  const terminal = [...chat.history].reverse().find(turn => !["queued", "running"].includes(turn.status) && !!turn.run_id);
  const turnId = terminal?.id ?? "";
  const scope = JSON.stringify([apiKey, chat.actor?.principal_id, chat.actor?.generation, chat.workspace, chat.id, turnId, terminal?.status]);
  const [revision, setRevision] = useState(0);
  const query = JSON.stringify([scope, revision]);
  const [reading, setReading] = useState<{ query: string; value: ObsTurnObservations | null; error: string; loading: boolean }>({ query: "", value: null, error: "", loading: false });
  useEffect(() => {
    if (!active || !chat.ready || !chat.session || !turnId) return;
    const controller = new AbortController();
    setReading({ query, value: null, error: "", loading: true });
    void request(turnPath(chat.workspace, chat.id, turnId) + "/observations", apiKey, undefined, controller.signal).then(raw => {
      const value = parseTurnObservations(raw, { session_id: chat.id, turn_id: turnId });
      if (!controller.signal.aborted) setReading({ query, value, error: "", loading: false });
    }).catch(error => { if (!controller.signal.aborted) setReading({ query, value: null, error: chatReason(error), loading: false }); });
    return () => controller.abort();
  }, [active, chat.ready, chat.session?.id, query, apiKey, chat.workspace, chat.id, turnId]);
  const data = chat.ready && reading.query === query ? reading.value : null;
  const { manifest, reported, requestId } = contextRequestSnapshot(data?.requests ?? []);
  const limit = chat.session?.context_policy.compaction_trigger_tokens ?? chat.session?.context_policy.max_tokens
    ?? (manifest?.model_context_window ? Math.max(0, manifest.model_context_window - (manifest.reserved_output_tokens ?? 0)) : null);
  // This is the last request's input snapshot, not a preview of the next unsent request.
  const used = manifest?.estimated_tokens ?? (!turnId && chat.ready && !chat.reading ? 0 : null);
  return { data, manifest, reported, requestId, turnId, limit, used, loading: !!turnId && active && (reading.query !== query || reading.loading), error: reading.query === query ? reading.error : "", refresh: () => setRevision(value => value + 1) };
}
export type SessionContextState = ReturnType<typeof useSessionContext>;
