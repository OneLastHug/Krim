import { useEffect, useRef, useState, type ReactNode } from "react";
import { Activity, ArrowUpRight, Database, Eye, FlaskConical, Folder, Globe, MessageSquare, MoreVertical, PanelLeft, PanelRight, RefreshCw, SquarePlus } from "lucide-react";
import type { AccountTab } from "./AccountSettings";
import type { Actor } from "../lib/session-contracts";
import type { SessionState } from "../hooks/use-session";
import { workbenchTitles, type WorkbenchView } from "../lib/workbench-route";
import type { DisplayTheme } from "../lib/use-display-theme";
import type { ReadingSize } from "../hooks/use-reading-size";
import type { ObsPage } from "../observability/obs-route";
import { openDevelopmentPanel } from "../lib/workbench-events";
import { request } from "../api";
import { parseSidebar } from "../lib/session-sidebar";
import { AppSidebar } from "./AppSidebar";
import { PopoverMenu } from "./popover-menu";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "./ui/dialog";

function useMediaQuery(query: string) {
  const fallbackWidth = Number(query.match(/max-width:\s*(\d+)px/)?.[1] ?? 1023);
  const [matches, setMatches] = useState(() => (typeof window !== "undefined" && window.matchMedia?.(query).matches) ?? window.innerWidth <= fallbackWidth);
  useEffect(() => {
    const media = window.matchMedia?.(query);
    if (!media) {
      const update = () => setMatches(window.innerWidth <= fallbackWidth);
      window.addEventListener("resize", update);
      return () => window.removeEventListener("resize", update);
    }
    const update = () => setMatches(media.matches);
    update(); media.addEventListener?.("change", update);
    return () => media.removeEventListener?.("change", update);
  }, [query, fallbackWidth]);
  return matches;
}

const entries: { id: WorkbenchView; label: string; icon: typeof MessageSquare }[] = [
  { id: "chat", label: "对话", icon: MessageSquare }, { id: "run", label: "运行记录", icon: Activity },
  { id: "trace", label: "Trace", icon: Eye }, { id: "datasets", label: "数据集", icon: Database },
  { id: "eval", label: "评测", icon: FlaskConical },
];
const themes: { id: DisplayTheme; label: string }[] = [
  { id: "warm", label: "暖纸" }, { id: "bright", label: "浅色" }, { id: "dark", label: "深色" },
];
const readingSizes = [12, 14, 16] as const;

export function WorkbenchLayout({ view, apiKey = "", identity, identityLoading, identityError, accountName = "", gatewayName = "", workspaceNames = {}, accountAdmin = false, onAccountTab, onLogout, theme, onThemeSelect, readingSize, onReadingSizeSelect, chatTitle, chat, httpEnabled, beforeSessionSelect, onNavigate, onConnect, onOpenTask, canOpenTask, actions, inspector, children, onObservabilityPage, observabilityPage }: {
  apiKey?: string;
  view: WorkbenchView; identity: Actor | null; identityLoading: boolean; identityError: string;
  accountName?: string; gatewayName?: string; workspaceNames?: Record<string, string>; accountAdmin?: boolean;
  onAccountTab?: (tab: AccountTab) => void; onLogout?: () => void;
  theme: DisplayTheme; onThemeSelect: (theme: DisplayTheme) => void;
  readingSize: ReadingSize; onReadingSizeSelect: (size: ReadingSize) => void;
  chatTitle: string; chat: SessionState | null;
  httpEnabled: boolean; beforeSessionSelect?: (workspace: string, sessionId: string) => void;
  onNavigate: (view: WorkbenchView) => void; onObservabilityPage?: (page: ObsPage) => void; observabilityPage: ObsPage; onConnect: () => void;
  onOpenTask: () => void; canOpenTask: boolean;
  actions: ReactNode; inspector: ReactNode; children: ReactNode;
}) {
  const narrow = useMediaQuery("(max-width: 1200px)"), mobile = useMediaQuery("(max-width: 700px)");
  const [navOpen, setNavOpen] = useState(false), [inspectorOpen, setInspectorOpen] = useState(false);
  const [collapsed, setCollapsed] = useState(false), [moreOpen, setMoreOpen] = useState(false);
  const [searchGroups, setSearchGroups] = useState<Record<string, string>>({});
  const [searchOpen, setSearchOpen] = useState(false), [query, setQuery] = useState(""), [searchIndex, setSearchIndex] = useState(0);
  const moreRef = useRef<HTMLButtonElement>(null);
  const previousIdentity = useRef("");
  const currentTitle = view === "chat" ? chatTitle || workbenchTitles[view] : workbenchTitles[view];
  const sidebarProps = { active: view, apiKey, onCreateInGroup: openCreate, identity, identityLoading, identityError, chat, httpEnabled, beforeSessionSelect, onClose: () => setNavOpen(false), accountName, gatewayName, workspaceNames, accountAdmin, onAccountTab, onLogout, onObservabilityPage, observabilityPage };
  const chatSession = view === "chat" ? chat : null;
  const canCreate = !!chatSession?.ready && chatSession.canWrite;
  const canRefresh = !!chatSession?.ready && !!chatSession.session;
  const canCloseSession = canRefresh && chatSession.session?.status === "open" && chatSession.canWrite && !chatSession.controlling;
  const paused = chatSession?.session?.dispatch_paused === true;
  const results = chat?.ready ? chat.list.filter(item => item.title.toLocaleLowerCase().includes(query.toLocaleLowerCase())) : [];
  function openCreate(groupId?: string) {
    if (!chat?.ready || !chat.canWrite || chat.creating || chat.newPending) return;
    beforeSessionSelect?.(chat.workspace, "");
    chat.select(chat.workspace, "");
    window.dispatchEvent(new CustomEvent("krim:new-session", { detail: { principal: chat.actor?.principal_id, generation: chat.actor?.generation, workspace: chat.workspace, group: groupId } }));
    if (view !== "chat") onNavigate("chat");
    setNavOpen(false);
    requestAnimationFrame(() => document.getElementById("chat-prompt")?.focus());
  }
  useEffect(() => {
    setSearchGroups({});
    if (!searchOpen || !chat?.ready) return;
    const controller = new AbortController();
    void request(`/v2/workspaces/${encodeURIComponent(chat.workspace)}/session-sidebar`, apiKey, undefined, controller.signal).then(raw => {
      const layout = parseSidebar(raw), titles = new Map(layout.groups.map(group => [group.id, group.title]));
      if (!controller.signal.aborted) setSearchGroups(Object.fromEntries(layout.sessions.map(item => [item.session_id, titles.get(item.group_id ?? "") ?? "未分组"])));
    }).catch(() => { /* Search remains usable when grouping metadata is unavailable. */ });
    return () => controller.abort();
  }, [searchOpen, apiKey, chat?.ready, chat?.workspace, identity?.principal_id, identity?.generation]);
  function openSearch() {
    if (!identity) return;
    if (view !== "chat") onNavigate("chat");
    setNavOpen(false); setQuery(""); setSearchIndex(0); setSearchOpen(true);
  }
  function selectSearch(id: string) {
    if (!chat?.ready) return;
    beforeSessionSelect?.(chat.workspace, id); chat.select(chat.workspace, id); setSearchOpen(false);
  }
  useEffect(() => {
    const next = identity ? `${identity.principal_id}:${identity.generation}` : "";
    if (identityError || next && previousIdentity.current && next !== previousIdentity.current) { setSearchOpen(false); setQuery(""); }
    if (next) previousIdentity.current = next;
  }, [identity?.principal_id, identity?.generation, identityError]);
  useEffect(() => {
    const keydown = (event: KeyboardEvent) => {
      if ((event.ctrlKey || event.metaKey) && !event.isComposing && !document.querySelector('[role="dialog"]')) {
        if (event.shiftKey && event.key.toLowerCase() === "n") { event.preventDefault(); openCreate(); return; }
        if (!event.shiftKey && event.key.toLowerCase() === "b") { event.preventDefault(); if (mobile) setNavOpen(value => !value); else setCollapsed(value => !value); return; }
      }
      if ((event.ctrlKey || event.metaKey) && !event.shiftKey && !event.isComposing && event.key.toLowerCase() === "k") {
        if (document.querySelector('[role="dialog"]') && !searchOpen) return;
        event.preventDefault(); openSearch();
      }
    };
    window.addEventListener("keydown", keydown);
    return () => window.removeEventListener("keydown", keydown);
  });

  return <div className={`workbench-shell${collapsed ? " sidebar-collapsed" : ""}`}>
    {!mobile && <div className="desktop-sidebar" inert={collapsed}>
      <AppSidebar {...sidebarProps} footerExtra={actions} onNavigate={onNavigate} onOpenSearch={openSearch} onConnect={onConnect}
        theme={theme} onThemeSelect={onThemeSelect} readingSize={readingSize} onReadingSizeSelect={onReadingSizeSelect}
        onCreate={() => openCreate()} />
    </div>}
    <div className="workbench-main">
      <header className="chat-toolbar" aria-label="会话标题栏">
        <Button variant="ghost" className="icon-button" aria-label={mobile ? "打开导航" : collapsed ? "展开侧栏" : "收起侧栏"} title={mobile ? "打开导航" : collapsed ? "展开侧栏" : "收起侧栏"} aria-expanded={mobile ? navOpen : !collapsed} onClick={() => mobile ? setNavOpen(true) : setCollapsed(value => !value)}><PanelLeft aria-hidden="true" /></Button>
        <span className="actions-divider" aria-hidden="true"></span>
        <div className="toolbar-title"><h1 title={currentTitle}>{currentTitle}</h1></div>
        <div className="toolbar-actions">
          {view === "chat" && <Button variant="ghost" className="icon-button" aria-label="新建对话" title="新建对话" disabled={!canCreate} onClick={() => openCreate()}><SquarePlus aria-hidden="true" /></Button>}
          {view === "chat" && <>
            <Button variant="ghost" className="icon-button" aria-label="项目文件工作区" disabled={!chat?.ready} onClick={() => chat && openDevelopmentPanel(chat, "files")}><Folder aria-hidden="true"/></Button>
            <Button variant="ghost" className="icon-button" aria-label="浏览器预览" disabled={!chat?.ready} onClick={() => chat && openDevelopmentPanel(chat, "browser")}><Globe aria-hidden="true"/></Button>
            <Button variant="ghost" className="icon-button" aria-label="开发信息栏" disabled={!chat?.ready} onClick={() => chat && openDevelopmentPanel(chat, "info")}><PanelRight aria-hidden="true"/></Button>
          </>}
          <Button variant="ghost" className="icon-button" ref={moreRef} aria-label="更多操作" title="更多操作" aria-haspopup="menu" aria-expanded={moreOpen} onClick={() => setMoreOpen(open => !open)}><MoreVertical aria-hidden="true" /></Button>
        </div>
        <PopoverMenu open={moreOpen} anchor={moreRef.current} onClose={() => setMoreOpen(false)} label="更多操作">
          {canOpenTask && <button type="button" role="menuitem" className="menu-item" onClick={() => { setMoreOpen(false); onOpenTask(); }}>
            <ArrowUpRight aria-hidden="true" />打开任务
          </button>}
          {canRefresh && <button type="button" role="menuitem" className="menu-item" onClick={() => { setMoreOpen(false); void chatSession.refresh(); }}>
            <RefreshCw aria-hidden="true" />刷新对话
          </button>}
          {paused && <button type="button" role="menuitem" className="menu-item" disabled={!chatSession.canWrite || chatSession.controlling} onClick={() => { setMoreOpen(false); void chatSession.control(null, "resume"); }}>恢复分发</button>}
          {canCloseSession && <button type="button" role="menuitem" className="menu-item" onClick={() => { setMoreOpen(false); void chatSession.control(null, "close"); }}>关闭对话</button>}
          {chatSession?.ready && chatSession.canWrite && chatSession.session?.status === "closed" && chatSession.session?.owner_id === chatSession.actor?.principal_id && !chatSession.controlling && <>
            {chatSession.listView !== "trash" && !chatSession.session.deleted_at && <button type="button" role="menuitem" className="menu-item" onClick={() => { setMoreOpen(false); void chatSession.lifecycle(chatSession.session?.archived_at ? "unarchive" : "archive"); }}>{chatSession.session.archived_at ? "取消归档对话" : "归档对话"}</button>}
            {chatSession.listView !== "trash" && <button type="button" role="menuitem" className="menu-item" onClick={() => { setMoreOpen(false); void chatSession.lifecycle("trash"); }}>移入回收站</button>}
            {chatSession.listView === "trash" && <button type="button" role="menuitem" className="menu-item" onClick={() => { setMoreOpen(false); void chatSession.lifecycle("restore"); }}>恢复对话</button>}
          </>}
          <button type="button" role="menuitem" className="menu-item" onClick={() => { setMoreOpen(false); if (view === "chat" && chat) openDevelopmentPanel(chat, "info"); else setInspectorOpen(open => !open); }}>
            {inspectorOpen ? "关闭详情面板" : "打开详情面板"}
          </button>
        </PopoverMenu>
      </header>
      <div className={`workbench-content${inspectorOpen && view !== "chat" && !narrow ? " inspector-open" : ""}`}>
        <main className="workbench-page" id="main-content" tabIndex={-1}>{children}</main>
        {inspectorOpen && view !== "chat" && !narrow && <div className="inspector-desktop">{inspector}</div>}
      </div>
    </div>
    <Dialog open={navOpen && mobile} onOpenChange={setNavOpen}><DialogContent className="mobile-drawer-content" aria-label="主导航与对话" aria-describedby={undefined}><DialogHeader className="drawer-heading"><DialogTitle>主导航与对话</DialogTitle></DialogHeader><AppSidebar {...sidebarProps} footerExtra={actions} onNavigate={onNavigate} onOpenSearch={openSearch} onConnect={onConnect}
      theme={theme} onThemeSelect={onThemeSelect} readingSize={readingSize} onReadingSizeSelect={onReadingSizeSelect}
      onCreate={() => openCreate()} className="drawer-sidebar" /></DialogContent></Dialog>
    <Dialog open={searchOpen} onOpenChange={setSearchOpen}><DialogContent className="session-search-dialog" aria-label="查找对话" aria-describedby={undefined}>
      <DialogHeader><DialogTitle>查找对话</DialogTitle></DialogHeader>
      <Input aria-label="搜索当前列表" placeholder="按名称查找当前列表…" value={query} onChange={event => { setQuery(event.target.value); setSearchIndex(0); }} onKeyDown={event => {
        if (event.nativeEvent.isComposing) return;
        if (event.key === "ArrowDown") { event.preventDefault(); setSearchIndex(value => Math.max(0, Math.min(value + 1, results.length - 1))); }
        if (event.key === "ArrowUp") { event.preventDefault(); setSearchIndex(value => Math.max(0, value - 1)); }
        if (event.key === "Enter" && results[searchIndex]) { event.preventDefault(); selectSearch(results[searchIndex]!.id); }
      }} />
      <div className="session-search-results">{results.map((item, index) => <Button key={item.id} variant="ghost" className={index === searchIndex ? "is-highlighted" : ""} onMouseEnter={() => setSearchIndex(index)} onClick={() => selectSearch(item.id)}><span>{item.title}<small className="session-search-group">{searchGroups[item.id] ?? workspaceNames[chat?.workspace ?? ""] ?? "当前工作区"}</small></span>{item.status === "closed" && <span>已关闭</span>}</Button>)}{!results.length && <p>{chat?.ready ? "当前列表中没有匹配的对话" : "正在读取对话列表"}</p>}</div>
      <p className="session-search-scope">仅查找当前工作区已加载的对话</p>
    </DialogContent></Dialog>
    <Dialog open={inspectorOpen && view !== "chat" && narrow} onOpenChange={setInspectorOpen}><DialogContent className="mobile-inspector-content" aria-label="对象详情" aria-describedby={undefined}><DialogHeader className="drawer-heading"><DialogTitle>对象详情</DialogTitle></DialogHeader>{inspector}</DialogContent></Dialog>
  </div>;
}

export { entries as workbenchNavigationEntries, themes as workbenchThemeOptions, readingSizes as workbenchReadingSizes };
