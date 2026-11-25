import type { SessionState } from "../hooks/use-session";
import { projectTurn } from "./session-presentation";
import { canonicalToolName } from "./tool-names";
import { object } from "../api";

export interface RegisteredPreview { preview_id: string; project_id: string; state: string; port: number }
// Only successful structured tool results from this session can introduce a preview ID.
// Model prose, addresses and raw access_path strings are never navigation targets.
// 列表按端口收敛：同一端口只保留最新注册的 preview_id，服务重启后的重注册取代旧行；
// 被取代旧 ID 迟到的状态/停止事件不得覆盖新注册，不得把端口切回旧 ID。
const PREVIEW_STATES = ["pending", "ready", "stopped", "revoked", "unknown"];
// preview_status 的失败探测不抛工具错误，而是返回结构化 ok:false。只消费这些明确错误码：
// 记录确已死亡的置 stopped，暂时不可判定的置 unknown；其余错误码（如授权失败）不改动任何条目。
const CLOSED_PREVIEW_CODES = ["not_found", "preview_inactive"];
const UNCERTAIN_PREVIEW_CODES = ["preview_not_ready", "preview_route_unavailable", "preview_route_invalidated", "preview_route_capacity", "workspace_busy"];
export function registeredPreviews(chat: SessionState, projectId: string | null): RegisteredPreview[] {
  if (!projectId || !chat.actor) return [];
  const previews = new Map<number, RegisteredPreview>();
  const portOfId = new Map<string, number>();
  for (const turn of chat.history ?? []) {
    const detail = chat.details?.[turn.id];
    const events = chat.turnEvents?.[turn.id] ?? (turn.id === chat.currentTurn?.id ? chat.events : []);
    for (const item of projectTurn(turn, events, [], { events: detail?.events, facts: chat.turnFacts?.[turn.id] ?? detail?.facts })) {
      if (item.type !== "tool" || item.state !== "ok" || !item.resultAvailable) continue;
      const tool = canonicalToolName(item.name);
      if (!["preview_register", "preview_status", "preview_stop"].includes(tool)) continue;
      try {
        const result = object(item.result);
        if (tool === "preview_status" && result.ok === false) {
          const id = result.preview_id;
          const state = CLOSED_PREVIEW_CODES.includes(String(result.code)) ? "stopped"
            : UNCERTAIN_PREVIEW_CODES.includes(String(result.code)) ? "unknown" : null;
          const port = typeof id === "string" && /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/.test(id) ? portOfId.get(id) : undefined;
          const current = port !== undefined ? previews.get(port) : undefined;
          // 只降级该端口当前登记、仍处于活跃状态的条目；已停用条目与被取代的旧 ID 不受影响。
          if (port !== undefined && current && current.preview_id === id && state && ["pending", "ready"].includes(current.state)) previews.set(port, { ...current, state });
          continue;
        }
        const environment = object(result.environment), id = result.preview_id;
        if (typeof id !== "string" || !/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/.test(id) || environment.project_id !== projectId || environment.owner_id !== chat.actor.principal_id || !Number.isInteger(result.port) || Number(result.port) < 1 || Number(result.port) > 65535 || !PREVIEW_STATES.includes(String(result.state))) continue;
        const port = Number(result.port);
        portOfId.set(id, port);
        const current = previews.get(port);
        // 注册总是采用（最新注册优先）；状态与停止事件只作用于该端口当前登记的同一 ID。
        if (tool === "preview_register" || !current || current.preview_id === id) {
          previews.set(port, { preview_id: id, project_id: projectId, port, state: String(result.state) });
        }
      } catch { /* A malformed tool output cannot create a browser action. */ }
    }
  }
  return [...previews.values()];
}
export function parsePreviewBootstrap(raw: unknown): PreviewBootstrap {
  const value = object(raw);
  if (typeof value.action !== "string" || typeof value.ticket !== "string" || !/^[A-Za-z0-9_-]{40,128}$/.test(value.ticket) || typeof value.expires_at !== "number" || !Number.isFinite(value.expires_at) || value.expires_at <= Date.now() / 1000 || value.expires_at > Date.now() / 1000 + 60) throw new Error("预览票据无效");
  const action = new URL(value.action);
  // The configured label family is server-side knowledge (p- legacy default or
  // a same-site prefix such as preview-); the 40-hex key and DNS shape hold either way.
  if (action.protocol !== "https:" || action.username || action.password || action.search || action.hash || action.pathname !== "/.krim-preview/session" || !/^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?-[a-f0-9]{40}\.[a-z0-9.-]+$/.test(action.hostname) || action.origin === location.origin) throw new Error("预览地址无效");
  // 嵌入能力由服务端逐次授权；仅同站兄弟子域部署返回 true，独立站点保持新窗口。
  const allow = value.allow_embedding;
  if (allow !== undefined && typeof allow !== "boolean") throw new Error("预览票据无效");
  return { action: action.href, ticket: value.ticket, allow_embedding: allow === true };
}
export interface PreviewBootstrap { action: string; ticket: string; allow_embedding: boolean }
// 每次打开生成唯一目标名：iframe 与 form target 一一对应，旧票据与新帧不会混淆。
export function previewTargetName(): string {
  return "krim-preview-" + crypto.randomUUID();
}
// 仅开启脚本、同源（保留预览域自身 cookie）、表单与下载；预览域与控制域不同源，不给弹窗与顶级导航。
export const PREVIEW_FRAME_SANDBOX = "allow-scripts allow-same-origin allow-forms allow-downloads";
