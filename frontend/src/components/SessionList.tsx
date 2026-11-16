import { useEffect, useLayoutEffect, useRef, useState, type ReactNode, type PointerEvent as ReactPointerEvent } from "react";
import { createPortal } from "react-dom";
import { Archive, ChevronDown, ChevronRight, CodeXml, Ellipsis, FolderInput, GitFork, Pin, Plus, RotateCcw, Trash2 } from "lucide-react";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "./ui/dialog";
import { PopoverMenu } from "./popover-menu";
import type { SessionState } from "../hooks/use-session";
import { useSessionSidebar } from "../hooks/use-session-sidebar";
import { DEFAULT_GROUP, moveSidebar, type SidebarEntry, type SidebarLayout } from "../lib/session-sidebar";
import type { Session } from "../lib/session-contracts";
import "./session-sidebar.css";

function SidebarMenu({ open, anchor, onClose, label, children }: { open: boolean; anchor: HTMLElement | null; onClose: () => void; label: string; children: ReactNode }) {
  const host = useRef<HTMLDivElement>(null);
  useEffect(() => { if (!open) return; const frame = requestAnimationFrame(() => host.current?.querySelector<HTMLElement>('[role="menuitem"]:not(:disabled)')?.focus({ preventScroll: true })); return () => cancelAnimationFrame(frame); }, [open]);
  if (!open) return null;
  return createPortal(<div className="session-tree-menu-layer" ref={host} onKeyDown={event => {
    const items = [...(host.current?.querySelectorAll<HTMLElement>('[role="menuitem"]:not(:disabled)') ?? [])];
    const index = items.indexOf(document.activeElement as HTMLElement);
    if (["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) { event.preventDefault(); const next = event.key === "Home" ? 0 : event.key === "End" ? items.length - 1 : (index + (event.key === "ArrowDown" ? 1 : -1) + items.length) % items.length; items[next]?.focus(); }
    if (event.key === "Tab") { event.preventDefault(); onClose(); }
  }}><PopoverMenu open anchor={anchor} onClose={onClose} label={label}>{children}</PopoverMenu></div>, document.body);
}

interface Drag { kind: "group" | "session"; id: string; pointerId: number; x: number; y: number; active: boolean; drop: { id: string; after: boolean } | null; handle: HTMLElement }
export function SessionList({ chat, apiKey = "", beforeSelect, onCreateInGroup }: { chat: SessionState; apiKey?: string; beforeSelect?: (workspace: string, sessionId: string) => void; onCreateInGroup?: (groupId: string) => void }) {
  const sidebar = useSessionSidebar(chat, apiKey), layout = sidebar.layout;
  const [menu, setMenu] = useState<{ id: string; anchor: HTMLElement } | null>(null), [viewMenu, setViewMenu] = useState<HTMLElement | null>(null);
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set()), [nodes, setNodes] = useState<Set<string>>(new Set());
  const [dialog, setDialog] = useState<{ kind: "rename" | "archive" | "unarchive" | "trash" | "restore" | "move" | "group"; id: string } | null>(null);
  const [text, setText] = useState(""), [group, setGroup] = useState(DEFAULT_GROUP), [submitted, setSubmitted] = useState<string | null>(null), [notice, setNotice] = useState("");
  const tree = useRef<HTMLElement>(null), scroll = useRef(0), drag = useRef<Drag | null>(null), suppress = useRef(false), [dragUI, setDragUI] = useState<Drag | null>(null);
  const live = useRef({ layout, sidebar }); live.current = { layout, sidebar };
  const scope = JSON.stringify([chat.actor?.principal_id, chat.actor?.generation, chat.workspace]);
  useEffect(() => { setMenu(null); setViewMenu(null); setDialog(null); setSubmitted(null); setCollapsed(new Set()); setNodes(new Set()); setNotice(""); setDragUI(null); suppress.current = false; scroll.current = 0; }, [scope]);
  useLayoutEffect(() => { if (tree.current) tree.current.scrollTop = scroll.current; });
  const edited = dialog ? chat.list.find(session => session.id === dialog.id) ?? (chat.session?.id === dialog.id ? chat.session : null) : null;
  const pendingRename = chat.pendingControls.find(value => value.session_id === dialog?.id && value.target.endsWith("/rename"));
  useEffect(() => { if (submitted && edited?.title === submitted) { setDialog(null); setSubmitted(null); } }, [submitted, edited?.title]);
  useEffect(() => { if (submitted && chat.notice && !pendingRename && !chat.controlling) setSubmitted(null); }, [submitted, chat.notice, pendingRename, chat.controlling]);
  const byId = new Map(chat.list.map(session => [session.id, session])), entries = new Map(layout.sessions.map(entry => [entry.session_id, entry]));
  function closeMenu() { const anchor = menu?.anchor ?? viewMenu; setMenu(null); setViewMenu(null); anchor?.focus({ preventScroll: true }); }
  function coming() { closeMenu(); setNotice("敬请期待"); }
  function openDialog(kind: NonNullable<typeof dialog>["kind"], id: string) {
    closeMenu(); const selected = byId.get(id); setText(kind === "rename" ? selected?.title ?? "" : ""); setSubmitted(null); setGroup(entries.get(id)?.group_id ?? DEFAULT_GROUP); setDialog({ kind, id });
  }
  function select(id: string) { if (chat.id !== id) beforeSelect?.(chat.workspace, id); chat.select(chat.workspace, id); }
  async function save(next: SidebarLayout) { scroll.current = tree.current?.scrollTop ?? 0; return sidebar.save(next); }
  async function togglePin(id: string) { closeMenu(); await save({ ...layout, sessions: layout.sessions.map(entry => entry.session_id === id ? { ...entry, pinned: !entry.pinned } : entry) }); }
  function cancelDrag() { const value = drag.current; drag.current = null; setDragUI(null); if (value?.active) suppress.current = true; if (value?.handle.hasPointerCapture?.(value.pointerId)) value.handle.releasePointerCapture(value.pointerId); }
  useEffect(() => {
    function dropAt(event: PointerEvent) {
      const value = drag.current; if (!value) return;
      const hit = document.elementFromPoint(event.clientX, event.clientY), row = hit?.closest<HTMLElement>(value.kind === "group" ? "[data-group-handle]" : "[data-session-row]");
      value.drop = null;
      if (!row || !tree.current?.contains(row)) { setDragUI({ ...value }); return; }
      const id = value.kind === "group" ? row.dataset.groupHandle! : row.dataset.sessionRow!;
      if (id === value.id || id === "Pinned") { setDragUI({ ...value }); return; }
      if (value.kind === "session") {
        const a = live.current.layout.sessions.find(entry => entry.session_id === value.id), b = live.current.layout.sessions.find(entry => entry.session_id === id);
        if (!a || !b || a.pinned || b.pinned || a.group_id !== b.group_id || a.parent_id !== b.parent_id) { setDragUI({ ...value }); return; }
      }
      const rect = row.getBoundingClientRect(); value.drop = { id, after: event.clientY >= rect.top + rect.height / 2 }; setDragUI({ ...value });
    }
    function move(event: PointerEvent) {
      const value = drag.current; if (!value || value.pointerId !== event.pointerId) return;
      if (!(event.buttons & 1)) { cancelDrag(); return; }
      if (!value.active) { if (Math.hypot(event.clientX - value.x, event.clientY - value.y) < 6) return; value.active = true; try { value.handle.setPointerCapture(event.pointerId); } catch { /* Synthetic test pointers have no hardware capture. */ } }
      event.preventDefault(); dropAt(event);
    }
    function up(event: PointerEvent) {
      const value = drag.current; if (!value || event.pointerId !== value.pointerId) return;
      if (value.active) { event.preventDefault(); dropAt(event); }
      const drop = value.drop; cancelDrag();
      if (value.active && drop && live.current.sidebar.ready && !live.current.sidebar.saving) {
        scroll.current = tree.current?.scrollTop ?? 0;
        void live.current.sidebar.save(moveSidebar(live.current.layout, value.kind, value.id, drop.id, drop.after));
      }
    }
    function key(event: KeyboardEvent) { suppress.current = false; if (event.key === "Escape" && drag.current) { event.preventDefault(); event.stopImmediatePropagation(); cancelDrag(); } }
    function down() { suppress.current = false; }
    document.addEventListener("pointerdown", down, true); document.addEventListener("pointermove", move, { passive: false }); document.addEventListener("pointerup", up); document.addEventListener("pointercancel", cancelDrag); document.addEventListener("keydown", key, true); window.addEventListener("blur", cancelDrag);
    return () => { document.removeEventListener("pointerdown", down, true); document.removeEventListener("pointermove", move); document.removeEventListener("pointerup", up); document.removeEventListener("pointercancel", cancelDrag); document.removeEventListener("keydown", key, true); window.removeEventListener("blur", cancelDrag); drag.current = null; };
  }, [scope]);
  function startDrag(event: ReactPointerEvent<HTMLElement>, kind: Drag["kind"], id: string) {
    if (event.pointerType !== "mouse" || event.button !== 0 || !event.isPrimary || !chat.canWrite || !sidebar.ready || sidebar.saving || chat.listView !== "active" || menu || dialog || id === "Pinned") return;
    if ((event.target as Element).closest("[data-no-drag]")) return;
    if (kind === "session" && entries.get(id)?.pinned) return;
    drag.current = { kind, id, pointerId: event.pointerId, x: event.clientX, y: event.clientY, active: false, drop: null, handle: event.currentTarget };
  }
  function sortClass(kind: Drag["kind"], id: string) { return `${dragUI?.kind === kind && dragUI.id === id ? " is-drag-source" : ""}${dragUI?.kind === kind && dragUI.drop?.id === id ? dragUI.drop.after ? " drop-after" : " drop-before" : ""}`; }
  function status(session: Session) {
    if (session.id !== chat.id) return ["idle", session.status === "closed" ? "已关闭" : "状态未读取"];
    if (chat.approvals.length || session.dispatch_paused) return ["waiting", chat.approvals.length ? "等待授权" : "已暂停"];
    if (chat.history.some(turn => turn.status === "running" || turn.status === "queued")) return ["running", "运行中"];
    return ["idle", session.status === "closed" ? "已关闭" : "空闲"];
  }
  const canArrange = chat.canWrite && sidebar.ready && !sidebar.saving;
  function row(entry: SidebarEntry, depth: number) {
    const session = byId.get(entry.session_id); if (!session) return null;
    const children = entry.pinned && chat.listView === "active" ? [] : childrenByParent.get(session.id) ?? [], closed = nodes.has(session.id), [state, label] = status(session);
    return <li key={session.id} className="session-tree-node"><div className={`chat-session-row session-depth-${depth}${sortClass("session", session.id)}`} data-session-row={session.id}>
      <Button variant="ghost" className="session-select" aria-label={session.title} aria-current={chat.id === session.id ? "page" : undefined} onPointerDown={event => startDrag(event, "session", session.id)} onClick={() => select(session.id)}><CodeXml aria-hidden="true"/><span className="chat-session-label"><span>{session.title}</span></span></Button>
      {!!children.length && <button type="button" data-no-drag className="session-tree-disclosure" aria-label={`${closed ? "展开" : "收起"} ${session.title} 的子会话`} aria-expanded={!closed} onClick={() => setNodes(old => { const next = new Set(old); next.has(session.id) ? next.delete(session.id) : next.add(session.id); return next; })}>{closed ? <ChevronRight/> : <ChevronDown/>}</button>}
      <span className={`session-status ${state}`} title={label} role="img" aria-label={label} tabIndex={0}/>
      <Button variant="ghost" className="session-more" aria-label={`${session.title} 的操作`} aria-haspopup="menu" aria-expanded={menu?.id === session.id} onClick={event => setMenu({ id: session.id, anchor: event.currentTarget })}><Ellipsis aria-hidden="true"/></Button>
    </div>{!!children.length && !closed && <ul className="session-tree-children">{children.map(child => row(child, depth + 1))}</ul>}</li>;
  }
  const views = { active: "More", archived: "已归档", trash: "回收站" };
  const visible = layout.sessions.filter(entry => byId.has(entry.session_id));
  const childrenByParent = new Map<string, SidebarEntry[]>();
  for (const entry of visible) if (!entry.pinned && entry.parent_id) { const children = childrenByParent.get(entry.parent_id) ?? []; children.push(entry); childrenByParent.set(entry.parent_id, children); }
  const groups = chat.listView === "active" ? [{ id: "Pinned", title: "Pinned" }, ...layout.groups] : [{ id: "lifecycle", title: views[chat.listView] }];
  const menuSession = menu ? byId.get(menu.id) : null;
  const manageable = !!menuSession && chat.canWrite && !chat.busy && menuSession.owner_id === chat.actor?.principal_id;
  const lifecycleAllowed = manageable && menuSession?.status === "closed";
  return <section className="chat-sidebar" aria-label="对话列表">
    {chat.actor && chat.actor.workspaces.length > 1 && <div className="chat-workspace-select"><Label htmlFor="chat-workspace">工作区</Label><select id="chat-workspace" value={chat.workspace} onChange={event => { beforeSelect?.(event.target.value, ""); chat.select(event.target.value, ""); }}>{chat.actor.workspaces.map(workspace => <option key={workspace.workspace_id} value={workspace.workspace_id}>{workspace.workspace_id}</option>)}</select></div>}
    <div className="session-tree-toolbar"><button type="button" aria-haspopup="menu" aria-expanded={!!viewMenu} onClick={event => setViewMenu(event.currentTarget)}>{views[chat.listView]}<ChevronDown aria-hidden="true"/></button>{sidebar.saving && <span role="status">保存中…</span>}</div>
    {(notice || sidebar.message) && <div className="session-sidebar-notice" role="status">{notice || sidebar.message}{sidebar.message && <button onClick={sidebar.reload}>重新读取</button>}</div>}
    <nav ref={tree} className={`chat-session-list session-tree${dragUI?.active ? " is-sorting" : ""}`} aria-label="已存对话" onLostPointerCapture={event => { if (drag.current?.pointerId === event.pointerId) cancelDrag(); }} onScroll={event => { scroll.current = event.currentTarget.scrollTop; if (drag.current?.active) { drag.current.drop = null; setDragUI({ ...drag.current }); } }} onDragStart={event => event.preventDefault()} onClickCapture={event => { if (suppress.current) { event.preventDefault(); event.stopPropagation(); } }}>
      {groups.map(item => {
        const list = visible.filter(entry => chat.listView !== "active" || (item.id === "Pinned" ? entry.pinned : !entry.pinned && entry.group_id === item.id));
        if (!list.length && item.id === "Pinned") return null;
        const closed = collapsed.has(item.id), memberIds = new Set(list.map(entry => entry.session_id)), roots = list.filter(entry => entry.pinned || !memberIds.has(entry.parent_id ?? ""));
        return <section className={`session-tree-group${sortClass("group", item.id)}`} key={item.id}>
          <div className="session-tree-heading" data-group-handle={item.id}><button type="button" aria-expanded={!closed} onPointerDown={event => startDrag(event, "group", item.id)} onClick={() => setCollapsed(old => { const next = new Set(old); next.has(item.id) ? next.delete(item.id) : next.add(item.id); return next; })}>{item.title}<span data-no-drag>{closed ? <ChevronRight/> : <ChevronDown/>}</span></button>{item.id !== "Pinned" && chat.listView === "active" && onCreateInGroup && <Button variant="ghost" className="session-group-new" aria-label={`在 ${item.title} 新建会话`} disabled={!chat.canWrite} onClick={() => onCreateInGroup(item.id)}><Plus aria-hidden="true"/></Button>}</div>
          {!closed && <ul className="session-tree-list">{roots.map(entry => row(entry, 0))}</ul>}
        </section>;
      })}
      {!chat.list.length && <p className="chat-list-empty">{chat.ready ? chat.listView === "trash" ? "回收站为空" : chat.listView === "archived" ? "暂无已归档对话" : "还没有会话" : "连接后显示对话"}</p>}
    </nav>
    <SidebarMenu open={!!viewMenu} anchor={viewMenu} onClose={closeMenu} label="会话列表范围">{(["active", "archived", "trash"] as const).map(view => <button key={view} role="menuitem" className="menu-item" onClick={() => { closeMenu(); chat.setSessionListView(view); }}>{view === "active" ? "所有会话" : views[view]}</button>)}<hr className="menu-separator"/><button role="menuitem" className="menu-item" disabled={!canArrange} onClick={() => openDialog("group", "")}>新建分组</button></SidebarMenu>
    <SidebarMenu open={!!menuSession} anchor={menu?.anchor ?? null} onClose={closeMenu} label="会话操作">
      <button role="menuitem" className="menu-item" onClick={() => { if (menu) select(menu.id); closeMenu(); }}>在主会话打开</button>
      <button role="menuitem" className="menu-item" onClick={() => { if (menuSession && chat.actor) window.dispatchEvent(new CustomEvent("krim:open-aux", { detail: { principal: chat.actor.principal_id, generation: chat.actor.generation, workspace: chat.workspace, sessionId: menuSession.id } })); closeMenu(); }}>在辅助会话打开</button>
      <button role="menuitem" className="menu-item" disabled={!canArrange} onClick={() => { if (menu) void togglePin(menu.id); }}><Pin/>{entries.get(menu?.id ?? "")?.pinned ? "取消置顶" : "Pin"}</button>
      <button role="menuitem" className="menu-item" onClick={coming}>标记为未读</button>
      {manageable && menuSession?.status === "open" && <button role="menuitem" className="menu-item" onClick={() => openDialog("rename", menu!.id)} aria-label={menu?.id === chat.id ? "重命名当前对话" : "重命名会话"}>重命名</button>}
      <button role="menuitem" className="menu-item" onClick={coming}><GitFork/>Fork</button>
      <button role="menuitem" className="menu-item" disabled={!canArrange} onClick={() => openDialog("move", menu!.id)}><FolderInput/>移动到分组</button>
      <hr className="menu-separator"/>
      {lifecycleAllowed && chat.listView === "active" && <button role="menuitem" className="menu-item" aria-label="归档此对话" onClick={() => openDialog("archive", menu!.id)}><Archive/>归档</button>}
      {lifecycleAllowed && chat.listView === "archived" && <button role="menuitem" className="menu-item" aria-label="取消归档" onClick={() => openDialog("unarchive", menu!.id)}><RotateCcw/>取消归档</button>}
      {lifecycleAllowed && chat.listView === "trash" && <button role="menuitem" className="menu-item" aria-label="恢复此对话" onClick={() => openDialog("restore", menu!.id)}><RotateCcw/>恢复</button>}
      {lifecycleAllowed && chat.listView !== "trash" && <button role="menuitem" className="menu-item" aria-label="移入回收站" onClick={() => openDialog("trash", menu!.id)}><Trash2/>删除</button>}
      {manageable && menuSession?.status === "open" && menuSession.id === chat.id && <button role="menuitem" className="menu-item" disabled={chat.controlling} onClick={() => { closeMenu(); void chat.control(null, "close"); }}>关闭会话</button>}
      {manageable && menuSession?.status === "open" && <button role="menuitem" className="menu-item" onClick={() => { closeMenu(); setNotice("关闭会话后可归档或移入回收站"); }}>归档与删除</button>}
    </SidebarMenu>
    <Dialog open={!!dialog} onOpenChange={open => { if (!open && !submitted) setDialog(null); }}><DialogContent className={`session-sidebar-dialog${dialog?.kind === "rename" ? " session-rename-dialog" : ""}`} aria-describedby={undefined}><DialogHeader><DialogTitle>{dialog?.kind === "group" ? "新建分组" : dialog?.kind === "move" ? "移动到分组" : dialog?.kind === "rename" ? "重命名会话" : dialog?.kind === "trash" ? "移入回收站" : dialog?.kind === "restore" ? "恢复会话" : dialog?.kind === "unarchive" ? "取消归档" : "归档会话"}</DialogTitle></DialogHeader>
      <form onSubmit={event => { event.preventDefault(); if (!dialog) return;
        if (dialog.kind === "rename") { if (!text.trim() || pendingRename || !edited) return; if (text.trim() === edited.title) { setDialog(null); return; } setSubmitted(text.trim()); void chat.rename(text.trim(), dialog.id); }
        else if (dialog.kind === "group") { if (!text.trim()) return; void save({ ...layout, groups: [...layout.groups, { id: crypto.randomUUID(), title: text.trim() }] }).then(ok => { if (ok) setDialog(null); }); }
        else if (dialog.kind === "move") {
          const children = new Map<string, string[]>(); for (const entry of layout.sessions) if (entry.parent_id) children.set(entry.parent_id, [...(children.get(entry.parent_id) ?? []), entry.session_id]);
          const moved = new Set<string>(), queue = [dialog.id]; for (let i = 0; i < queue.length; i++) { const id = queue[i]!; if (moved.has(id)) continue; moved.add(id); queue.push(...(children.get(id) ?? [])); }
          if (entries.get(dialog.id)?.group_id === group) { setDialog(null); return; }
          const retained = layout.sessions.filter(entry => !moved.has(entry.session_id)), transferred = layout.sessions.filter(entry => moved.has(entry.session_id)).map(entry => ({ ...entry, group_id: group, parent_id: entry.session_id === dialog.id ? null : entry.parent_id }));
          void save({ ...layout, sessions: [...retained, ...transferred] }).then(ok => { if (ok) setDialog(null); });
        } else { void chat.lifecycle(dialog.kind, dialog.id); setDialog(null); }
      }}>
        {(dialog?.kind === "rename" || dialog?.kind === "group") ? <><Label htmlFor={dialog.kind === "rename" ? "rename-session-title" : "sidebar-title"}>名称</Label><Input id={dialog.kind === "rename" ? "rename-session-title" : "sidebar-title"} value={text} maxLength={dialog.kind === "group" ? 256 : 128} required disabled={!!submitted || !!pendingRename} onChange={event => setText(event.target.value)}/></> : dialog?.kind === "move" ? <><Label htmlFor="sidebar-group">分组</Label><select id="sidebar-group" value={group} onChange={event => setGroup(event.target.value)}>{layout.groups.map(item => <option key={item.id} value={item.id}>{item.title}</option>)}</select></> : <p>{dialog?.kind === "trash" ? "移入回收站后可恢复。" : "确认操作此会话？"}<strong>{edited?.title}</strong></p>}
        {dialog?.kind === "rename" && chat.notice && <p role="alert">{chat.notice}</p>}
        {submitted && <p role="status">{pendingRename ? "操作结果尚未确认，请查找原操作。" : "等待名称同步。"}</p>}
        {submitted && !pendingRename && <Button type="button" variant="outline" onClick={chat.refresh}>重新读取名称</Button>}
        {pendingRename && <Button type="button" variant="outline" onClick={() => void chat.recoverControl(pendingRename)}>查找操作结果</Button>}
        <div className="dialog-actions"><Button type="button" variant="outline" disabled={!!submitted} onClick={() => setDialog(null)}>取消</Button><Button type="submit" disabled={!!submitted || !!pendingRename || sidebar.saving}>{dialog?.kind === "rename" || dialog?.kind === "group" || dialog?.kind === "move" ? "保存" : "确认"}</Button></div>
      </form></DialogContent></Dialog>
  </section>;
}
