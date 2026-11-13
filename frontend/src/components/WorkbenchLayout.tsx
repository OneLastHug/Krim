import { useEffect, useRef, useState, type ReactNode } from "react";
import { Activity, ArrowUpRight, ChevronDown, Database, Eye, FlaskConical, Folder, MessageSquare, MoreVertical, PanelLeft, Play, RefreshCw, SquarePlus, Terminal } from "lucide-react";
import type { AccountTab } from "./AccountSettings";
import type { Actor } from "../lib/session-contracts";
import type { SessionState } from "../hooks/use-session";
import { workbenchTitles, type WorkbenchView } from "../lib/workbench-route";
import type { DisplayTheme } from "../lib/use-display-theme";
import type { ReadingSize } from "../hooks/use-reading-size";
import type { ObsPage } from "../observability/obs-route";
import { chatCopy as c } from "../lib/copy";
import { AppSidebar } from "./AppSidebar";
import { PopoverMenu } from "./popover-menu";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
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

export function WorkbenchLayout({ view, identity, identityLoading, identityError, accountName = "", gatewayName = "", workspaceNames = {}, accountAdmin = false, onAccountTab, onLogout, theme, onThemeSelect, readingSize, onReadingSizeSelect, chatTitle, chat, httpEnabled, beforeSessionSelect, onNavigate, onConnect, onOpenTask, canOpenTask, actions, inspector, children, onObservabilityPage, observabilityPage }: {
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
  const narrow = useMediaQuery("(max-width: 1100px)"), mobile = useMediaQuery("(max-width: 700px)");
  const [navOpen, setNavOpen] = useState(false), [inspectorOpen, setInspectorOpen] = useState(false);
  const [collapsed, setCollapsed] = useState(false), [createOpen, setCreateOpen] = useState(false), [moreOpen, setMoreOpen] = useState(false);
  const [createTitle, setCreateTitle] = useState("新对话"), [createTarget, setCreateTarget] = useState("demo");
  const [searchOpen, setSearchOpen] = useState(false), [query, setQuery] = useState(""), [searchIndex, setSearchIndex] = useState(0);
  const moreRef = useRef<HTMLButtonElement>(null);
  const previousIdentity = useRef("");
  const currentTitle = view === "chat" ? chatTitle || workbenchTitles[view] : workbenchTitles[view];
  const sidebarProps = { active: view, identity, identityLoading, identityError, chat, httpEnabled, beforeSessionSelect, onClose: () => setNavOpen(false), accountName, gatewayName, workspaceNames, accountAdmin, onAccountTab, onLogout, onObservabilityPage, observabilityPage };
  const chatSession = view === "chat" ? chat : null;
  const canCreate = !!chatSession?.ready && chatSession.canWrite;
  const workspaceConfigured = chat?.actor?.target_keys?.includes("target-workspace") === true;
  const canRefresh = !!chatSession?.ready && !!chatSession.session;
  const canCloseSession = canRefresh && chatSession.session?.status === "open" && chatSession.canWrite && !chatSession.controlling;
  const paused = chatSession?.session?.dispatch_paused === true;
  const results = chat?.ready ? chat.list.filter(item => item.title.toLocaleLowerCase().includes(query.toLocaleLowerCase())) : [];
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
        onCreate={() => setCreateOpen(true)} />
    </div>}
    <div className="workbench-main">
      <header className="chat-toolbar" aria-label="会话标题栏">
        <Button variant="ghost" className="icon-button" aria-label={mobile ? "打开导航" : collapsed ? "展开侧栏" : "收起侧栏"} title={mobile ? "打开导航" : collapsed ? "展开侧栏" : "收起侧栏"} aria-expanded={mobile ? navOpen : !collapsed} onClick={() => mobile ? setNavOpen(true) : setCollapsed(value => !value)}><PanelLeft aria-hidden="true" /></Button>
        <span className="actions-divider" aria-hidden="true"></span>
        <div className="toolbar-title"><h1 title={currentTitle}>{currentTitle}</h1></div>
        <div className="toolbar-actions">
          {view === "chat" && <Button variant="ghost" className="icon-button" aria-label="新建对话" title="新建对话" disabled={!canCreate} onClick={() => setCreateOpen(true)}><SquarePlus aria-hidden="true" /></Button>}
          <Button variant="ghost" className="icon-button" aria-label="终端（规划中）" title="终端（规划中）" disabled><Terminal aria-hidden="true" /></Button>
          <Button variant="ghost" className="icon-button" aria-label="运行（规划中）" title="运行（规划中）" disabled><Play aria-hidden="true" /></Button>
          <Button variant="ghost" className="icon-button" aria-label="打开工作目录（规划中）" title="打开工作目录（规划中）" disabled><Folder aria-hidden="true" /></Button>
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
          {chatSession?.ready && chatSession.canWrite && chatSession.session?.status === "closed" && !chatSession.controlling && <>
            {chatSession.listView !== "trash" && !chatSession.session.deleted_at && <button type="button" role="menuitem" className="menu-item" onClick={() => { setMoreOpen(false); void chatSession.lifecycle(chatSession.session?.archived_at ? "unarchive" : "archive"); }}>{chatSession.session.archived_at ? "取消归档对话" : "归档对话"}</button>}
            {chatSession.listView !== "trash" && <button type="button" role="menuitem" className="menu-item" onClick={() => { setMoreOpen(false); void chatSession.lifecycle("trash"); }}>移入回收站</button>}
            {chatSession.listView === "trash" && <button type="button" role="menuitem" className="menu-item" onClick={() => { setMoreOpen(false); void chatSession.lifecycle("restore"); }}>恢复对话</button>}
          </>}
          <button type="button" role="menuitem" className="menu-item" onClick={() => { setMoreOpen(false); setInspectorOpen(open => !open); }}>
            {inspectorOpen ? "关闭详情面板" : "打开详情面板"}
          </button>
        </PopoverMenu>
      </header>
      <div className={`workbench-content${inspectorOpen && !narrow ? " inspector-open" : ""}`}>
        <main className="workbench-page" id="main-content" tabIndex={-1}>{children}</main>
        {inspectorOpen && !narrow && <div className="inspector-desktop">{inspector}</div>}
      </div>
    </div>
    <Dialog open={navOpen && mobile} onOpenChange={setNavOpen}><DialogContent className="mobile-drawer-content" aria-label="主导航与对话" aria-describedby={undefined}><DialogHeader className="drawer-heading"><DialogTitle>主导航与对话</DialogTitle></DialogHeader><AppSidebar {...sidebarProps} footerExtra={actions} onNavigate={onNavigate} onOpenSearch={openSearch} onConnect={onConnect}
      theme={theme} onThemeSelect={onThemeSelect} readingSize={readingSize} onReadingSizeSelect={onReadingSizeSelect}
      onCreate={() => setCreateOpen(true)} className="drawer-sidebar" /></DialogContent></Dialog>
    <Dialog open={createOpen} onOpenChange={setCreateOpen}><DialogContent className="session-create-dialog" aria-describedby={undefined}>
      <DialogHeader><DialogTitle>{c.create}</DialogTitle></DialogHeader>
      {chat && <form className="session-create-form" onSubmit={e => { e.preventDefault(); beforeSessionSelect?.(chat.workspace, ""); void chat.create(createTitle, createTarget).then(() => setCreateOpen(false)); }}>
        <div className="session-create-field">
          <Label htmlFor="session-title">{c.title}</Label>
          <Input id="session-title" value={createTitle} onChange={e => setCreateTitle(e.target.value)} maxLength={128} required autoComplete="off" />
        </div>
        <div className="session-create-field chat-target">
          <Label htmlFor="chat-target">{c.target}</Label>
          <div className="session-target-select">
            <select id="chat-target" aria-describedby={workspaceConfigured ? undefined : "session-target-help"} value={createTarget} onChange={e => setCreateTarget(e.target.value)}>
              <option value="demo">{c.defaultTarget}</option>
              {httpEnabled && chat.actor?.target_keys?.includes("target-http") && <option value="target-http">HTTP</option>}
              <option value="target-workspace" disabled={!workspaceConfigured}>工作区</option>
            </select>
            <ChevronDown aria-hidden="true" />
          </div>
          {!workspaceConfigured && <p className="session-target-help" id="session-target-help" role="status">工作区未配置</p>}
        </div>
        <div className="dialog-actions"><Button type="submit" disabled={chat.creating || !!chat.newPending || !createTitle.trim()}>{chat.creating ? "提交中" : c.createSubmit}</Button></div>
      </form>}
    </DialogContent></Dialog>
    <Dialog open={searchOpen} onOpenChange={setSearchOpen}><DialogContent className="session-search-dialog" aria-label="查找对话" aria-describedby={undefined}>
      <DialogHeader><DialogTitle>查找对话</DialogTitle></DialogHeader>
      <Input aria-label="搜索当前列表" placeholder="按名称查找当前列表…" value={query} onChange={event => { setQuery(event.target.value); setSearchIndex(0); }} onKeyDown={event => {
        if (event.nativeEvent.isComposing) return;
        if (event.key === "ArrowDown") { event.preventDefault(); setSearchIndex(value => Math.max(0, Math.min(value + 1, results.length - 1))); }
        if (event.key === "ArrowUp") { event.preventDefault(); setSearchIndex(value => Math.max(0, value - 1)); }
        if (event.key === "Enter" && results[searchIndex]) { event.preventDefault(); selectSearch(results[searchIndex]!.id); }
      }} />
      <div className="session-search-results">{results.map((item, index) => <Button key={item.id} variant="ghost" className={index === searchIndex ? "is-highlighted" : ""} onMouseEnter={() => setSearchIndex(index)} onClick={() => selectSearch(item.id)}>{item.title}{item.status === "closed" && <span>已关闭</span>}</Button>)}{!results.length && <p>{chat?.ready ? "当前列表中没有匹配的对话" : "正在读取对话列表"}</p>}</div>
      <p className="session-search-scope">仅查找当前工作区已加载的对话</p>
    </DialogContent></Dialog>
    <Dialog open={inspectorOpen && narrow} onOpenChange={setInspectorOpen}><DialogContent className="mobile-inspector-content" aria-label="对象详情" aria-describedby={undefined}><DialogHeader className="drawer-heading"><DialogTitle>对象详情</DialogTitle></DialogHeader>{inspector}</DialogContent></Dialog>
  </div>;
}

export { entries as workbenchNavigationEntries, themes as workbenchThemeOptions, readingSizes as workbenchReadingSizes };
