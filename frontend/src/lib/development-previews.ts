import type { SessionState } from "../hooks/use-session";
import { projectTurn } from "./session-presentation";
import { object } from "../api";

export interface RegisteredPreview { preview_id: string; project_id: string; state: string; port: number }
// Only successful structured tool results from this session can introduce a preview ID.
// Model prose, addresses and raw access_path strings are never navigation targets.
export function registeredPreviews(chat: SessionState, projectId: string | null): RegisteredPreview[] {
  if (!projectId || !chat.actor) return [];
  const previews = new Map<string, RegisteredPreview>();
  for (const turn of chat.history ?? []) {
    const detail = chat.details?.[turn.id];
    const events = chat.turnEvents?.[turn.id] ?? (turn.id === chat.currentTurn?.id ? chat.events : []);
    for (const item of projectTurn(turn, events, [], { events: detail?.events, facts: chat.turnFacts?.[turn.id] ?? detail?.facts })) {
      if (item.type !== "tool" || item.state !== "ok" || !item.resultAvailable || !["dev_preview_register", "dev_preview_status", "dev_preview_stop"].includes(item.name)) continue;
      try {
        const result = object(item.result), environment = object(result.environment), id = result.preview_id;
        if (typeof id !== "string" || !/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/.test(id) || environment.project_id !== projectId || environment.owner_id !== chat.actor.principal_id || !Number.isInteger(result.port) || Number(result.port) < 1 || Number(result.port) > 65535 || !["pending", "ready", "stopped", "revoked", "unknown"].includes(String(result.state))) continue;
        previews.set(id, { preview_id: id, project_id: projectId, port: Number(result.port), state: String(result.state) });
      } catch { /* A malformed tool output cannot create a browser action. */ }
    }
  }
  return [...previews.values()];
}
export function parsePreviewBootstrap(raw: unknown): { action: string; ticket: string } {
  const value = object(raw);
  if (typeof value.action !== "string" || typeof value.ticket !== "string" || !/^[A-Za-z0-9_-]{40,128}$/.test(value.ticket) || typeof value.expires_at !== "number" || !Number.isFinite(value.expires_at) || value.expires_at <= Date.now() / 1000 || value.expires_at > Date.now() / 1000 + 60) throw new Error("预览票据无效");
  const action = new URL(value.action);
  if (action.protocol !== "https:" || action.username || action.password || action.search || action.hash || action.pathname !== "/.krim-preview/session" || !/^p-[a-f0-9]{40}\.[a-z0-9.-]+$/.test(action.hostname) || action.origin === location.origin) throw new Error("预览地址无效");
  return { action: action.href, ticket: value.ticket };
}
