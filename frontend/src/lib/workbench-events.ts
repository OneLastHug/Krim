import type { SessionState } from "../hooks/use-session";

export type DevelopmentAction = "files" | "browser" | "info" | "context" | "project" | "environment" | "subagents";
export interface DevelopmentActionDetail {
  principal: string; generation: number; workspace: string; sessionId: string; action: DevelopmentAction; agentId?: string;
}
export interface DevelopmentPanelSnapshot {
  principal: string; generation: number; workspace: string; sessionId: string;
  projectTitle: string | null; infoOpen: boolean; filesOpen: boolean; browserOpen: boolean;
}
export function openDevelopmentPanel(chat: SessionState, action: DevelopmentAction, agentId?: string) {
  if (!chat.ready || !chat.actor) return;
  const detail: DevelopmentActionDetail = { principal: chat.actor.principal_id, generation: chat.actor.generation, workspace: chat.workspace, sessionId: chat.id, action, ...(agentId ? { agentId } : {}) };
  window.dispatchEvent(new CustomEvent("krim:development-action", { detail }));
}
