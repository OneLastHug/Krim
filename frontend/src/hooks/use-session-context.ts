import { useEffect, useState } from "react";
import { request } from "../api";
import { chatReason } from "../lib/copy";
import { turnPath } from "../lib/session-contracts";
import { parseTurnObservations, type ObsTurnObservations } from "../observability/obs-contracts";
import type { SessionState } from "./use-session";

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
  const manifest = [...(data?.requests ?? [])].reverse().find(item => item.manifest && item.purpose !== "compaction_summary")?.manifest ?? null;
  const limit = chat.session?.context_policy.compaction_trigger_tokens ?? chat.session?.context_policy.max_tokens
    ?? (manifest?.model_context_window ? Math.max(0, manifest.model_context_window - (manifest.reserved_output_tokens ?? 0)) : null);
  const used = manifest?.estimated_tokens ?? (!turnId && chat.ready && !chat.reading ? 0 : null);
  return { data, manifest, turnId, limit, used, loading: !!turnId && active && (reading.query !== query || reading.loading), error: reading.query === query ? reading.error : "", refresh: () => setRevision(value => value + 1) };
}
export type SessionContextState = ReturnType<typeof useSessionContext>;
