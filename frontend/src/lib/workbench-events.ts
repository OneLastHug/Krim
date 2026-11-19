import type { SessionState } from "../hooks/use-session";

export type DevelopmentAction = "files" | "browser" | "info" | "context" | "project" | "environment";
export interface DevelopmentActionDetail {
  principal: string; generation: number; workspace: string; sessionId: string; action: DevelopmentAction;
}
export interface DevelopmentPanelSnapshot {
  principal: string; generation: number; workspace: string; sessionId: string;
  projectTitle: string | null; infoOpen: boolean; filesOpen: boolean; browserOpen: boolean;
}
export function openDevelopmentPanel(chat: SessionState, action: DevelopmentAction) {
  if (!chat.ready || !chat.actor) return;
  const detail: DevelopmentActionDetail = { principal: chat.actor.principal_id, generation: chat.actor.generation, workspace: chat.workspace, sessionId: chat.id, action };
  window.dispatchEvent(new CustomEvent("krim:development-action", { detail }));
}
