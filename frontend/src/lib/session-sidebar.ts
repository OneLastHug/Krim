import { object } from "../api";
import type { Session } from "./session-contracts";

export interface SidebarGroup { id: string; title: string }
export interface SidebarEntry { session_id: string; group_id: string | null; parent_id: string | null; pinned: boolean }
export interface SidebarLayout { revision: number; groups: SidebarGroup[]; sessions: SidebarEntry[] }
export const emptySidebar = (): SidebarLayout => ({ revision: 0, groups: [], sessions: [] });
// 项目即分组：组 id 为项目 id，title 为主体独立别名；无绑定的旧会话 group_id 为 null，
// 归入仅用于展示的"未分组"区，该 id 永不写回布局。
export const UNGROUPED = "ungrouped";
export function parseSidebar(raw: unknown): SidebarLayout {
  const value = object(raw);
  // 数量由真实项目/会话集合自然约束（schema21 起无人为上限）；仅校验字段与结构。
  if (!Number.isSafeInteger(value.revision) || Number(value.revision) < 0 || !Array.isArray(value.groups) || !Array.isArray(value.sessions)) throw new Error("会话排序响应无效");
  const groupIds = new Set<string>(), sessionIds = new Set<string>();
  for (const rawGroup of value.groups) {
    const group = object(rawGroup);
    if (typeof group.id !== "string" || !group.id || group.id.length > 128 || groupIds.has(group.id) || typeof group.title !== "string" || !group.title.trim() || group.title.length > 256) throw new Error("会话分组响应无效");
    groupIds.add(group.id);
  }
  for (const rawEntry of value.sessions) {
    const entry = object(rawEntry);
    if (typeof entry.session_id !== "string" || !entry.session_id || sessionIds.has(entry.session_id) || typeof entry.pinned !== "boolean" || entry.group_id !== null && (typeof entry.group_id !== "string" || !groupIds.has(entry.group_id)) || entry.parent_id !== null && typeof entry.parent_id !== "string") throw new Error("会话排序响应无效");
    sessionIds.add(entry.session_id);
  }
  const layout = value as unknown as SidebarLayout, byId = new Map(layout.sessions.map(entry => [entry.session_id, entry]));
  const depths = new Map<string, number>();
  for (const entry of layout.sessions) {
    const path = new Set<string>(); let current: SidebarEntry | undefined = entry;
    while (current && !depths.has(current.session_id)) {
      if (path.has(current.session_id)) throw new Error("会话层级响应无效");
      path.add(current.session_id);
      if (current.parent_id === null) break;
      const parent = byId.get(current.parent_id);
      if (!parent || parent.group_id !== current.group_id) throw new Error("会话层级响应无效");
      current = parent;
    }
    let depth = current ? depths.get(current.session_id) ?? 0 : 0;
    for (const id of [...path].reverse()) { if (++depth > 32) throw new Error("会话层级超过上限"); depths.set(id, depth); }
  }
  return layout;
}
// 列表中尚无排序记录的会话（刚创建或服务端尚未收录）按列表顺序以未绑定条目补齐；
// 仅用于渲染，任何保存仍只提交服务端返回的布局。
export function unrecordedEntries(layout: SidebarLayout, sessions: Session[]): SidebarEntry[] {
  const known = new Set(layout.sessions.map(entry => entry.session_id));
  return sessions.filter(session => !known.has(session.id))
    .map(session => ({ session_id: session.id, group_id: null, parent_id: null, pinned: false }));
}
export function moveSidebar(layout: SidebarLayout, kind: "group" | "session", source: string, target: string, after: boolean): SidebarLayout {
  if (source === target) return layout;
  if (kind === "group") {
    const value = layout.groups.find(group => group.id === source);
    if (!value || !layout.groups.some(group => group.id === target)) return layout;
    const groups = layout.groups.filter(group => group.id !== source), index = groups.findIndex(group => group.id === target);
    groups.splice(index + Number(after), 0, value); return { ...layout, groups };
  }
  const value = layout.sessions.find(entry => entry.session_id === source), destination = layout.sessions.find(entry => entry.session_id === target);
  if (!value || !destination || value.pinned || destination.pinned || value.group_id !== destination.group_id || value.parent_id !== destination.parent_id) return layout;
  const siblings = layout.sessions.filter(entry => !entry.pinned && entry.group_id === value.group_id && entry.parent_id === value.parent_id);
  const reordered = siblings.filter(entry => entry.session_id !== source); reordered.splice(reordered.findIndex(entry => entry.session_id === target) + Number(after), 0, value);
  let index = 0; const siblingIds = new Set(siblings.map(entry => entry.session_id));
  return { ...layout, sessions: layout.sessions.map(entry => siblingIds.has(entry.session_id) ? reordered[index++]! : entry) };
}
