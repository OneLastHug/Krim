import { useEffect, useRef, useState, type ReactNode } from "react";
import { Activity, Database, Eye, FlaskConical, MessageSquare, Palette, PanelLeft, PanelRight, Search, Settings2 } from "lucide-react";
import type { Actor } from "../lib/session-contracts";
import type { SessionState } from "../hooks/use-session";
import { workbenchTitles, type WorkbenchView } from "../lib/workbench-route";
import type { DisplayTheme } from "../lib/use-display-theme";
import { AppSidebar } from "./AppSidebar";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "./ui/dialog";

function useMediaQuery(query: string) {
  const fallbackWidth = Number(query.match(/max-width:\s*(\d+)px/)?.[1] ?? 1023);
  const [matches, setMatches] = useState(() => typeof window !== "undefined" && (window.matchMedia?.(query).matches ?? window.innerWidth <= fallbackWidth));
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
  { id: "eval", label: "评测", icon: FlaskConical }, { id: "settings", label: "设置", icon: Settings2 },
];
function WorkbenchNavigation({ active, onNavigate, onClose }: { active: WorkbenchView; onNavigate: (view: WorkbenchView) => void; onClose?: () => void }) {
  return <nav className="primary-navigation" aria-label="主导航">{entries.map(({ id, label, icon: Icon }) => <Button key={id} type="button" variant="ghost" className="navigation-item" aria-label={`导航到${label}`} aria-current={active === id ? "page" : undefined} title={label} onClick={() => { onNavigate(id); onClose?.(); }}><Icon aria-hidden="true" /><span>{label}</span></Button>)}</nav>;
}
const themes: { id: DisplayTheme; label: string; description: string }[] = [
  { id: "warm", label: "暖纸", description: "柔和暖底与陶土色点缀" },
  { id: "bright", label: "浅色", description: "中性浅色界面" },
  { id: "dark", label: "深色", description: "低亮度深色界面" },
];

export function WorkbenchLayout({ view, identity, identityLoading, identityError, theme, onThemeSelect, chatTitle, chat, httpEnabled, beforeSessionSelect, onNavigate, actions, inspector, children }: {
  view: WorkbenchView; identity: Actor | null; identityLoading: boolean; identityError: string;
  theme: DisplayTheme; onThemeSelect: (theme: DisplayTheme) => void; chatTitle: string; chat: SessionState | null;
  httpEnabled: boolean; beforeSessionSelect?: (workspace: string, sessionId: string) => void;
  onNavigate: (view: WorkbenchView) => void; actions: ReactNode; inspector: ReactNode; children: ReactNode;
}) {
  const narrow = useMediaQuery("(max-width: 1100px)"), mobile = useMediaQuery("(max-width: 700px)");
  const [navOpen, setNavOpen] = useState(false), [inspectorOpen, setInspectorOpen] = useState(false);
  const [themeOpen, setThemeOpen] = useState(false), [collapsed, setCollapsed] = useState(false);
  const [searchOpen, setSearchOpen] = useState(false), [query, setQuery] = useState(""), [searchIndex, setSearchIndex] = useState(0);
  const previousIdentity = useRef("");
  const currentTitle = view === "chat" ? chatTitle || workbenchTitles[view] : workbenchTitles[view];
  const sidebarProps = { active: view, identity, identityLoading, identityError, chat, httpEnabled, beforeSessionSelect, onClose: () => setNavOpen(false) };
  const candidateWorkspace = view === "chat" ? chat?.workspace : new URL(location.href).searchParams.get("workspace");
  const workspace = identity?.workspaces.find(item => item.workspace_id === candidateWorkspace)?.workspace_id ?? identity?.workspaces[0]?.workspace_id;
  const connection = identity ? "已连接" : identityLoading ? "读取身份" : identityError ? "连接失败" : "未连接";
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
    <header className="workbench-globalbar" aria-label="全局工具栏">
      <Button variant="ghost" className="sidebar-collapse" aria-label={mobile ? "打开导航" : collapsed ? "展开侧栏" : "收起侧栏"} title={mobile ? "打开导航" : collapsed ? "展开侧栏" : "收起侧栏"} aria-expanded={mobile ? navOpen : !collapsed} onClick={() => mobile ? setNavOpen(true) : setCollapsed(value => !value)}><PanelLeft aria-hidden="true" /></Button>
      <Button variant="ghost" className="global-search-button" aria-label="查找对话" title="查找对话 Ctrl K" disabled={!identity} onClick={openSearch}><Search aria-hidden="true" /></Button>
      {!mobile && <WorkbenchNavigation active={view} onNavigate={onNavigate} />}
    </header>
    {!mobile && <div className="desktop-sidebar" inert={collapsed}><AppSidebar {...sidebarProps} /></div>}
    <div className="workbench-main">
      <div className="workbench-topbar">
        <div className="topbar-title brand"><h1 title={currentTitle}>{currentTitle}</h1>{workspace && <span className="brand-capability" title="当前工作区">{workspace}</span>}</div>
        <div className="topbar-status" role="status" aria-label={connection}><span className={`identity-mark${identity ? " is-ready" : ""}`} aria-hidden="true" /><span>{connection}</span></div>
        <div className="topbar-actions"><div className="access-actions-slot">{actions}</div><Button variant="ghost" aria-label="选择显示主题" title="选择显示主题" onClick={() => setThemeOpen(true)}><Palette aria-hidden="true" /></Button><Button variant="ghost" aria-label={inspectorOpen ? "关闭详情面板" : "打开详情面板"} aria-pressed={inspectorOpen} onClick={() => setInspectorOpen(open => !open)}><PanelRight aria-hidden="true" /></Button></div>
      </div>
      <div className={`workbench-content${inspectorOpen && !narrow ? " inspector-open" : ""}`}>
        <main className="workbench-page" id="main-content" tabIndex={-1}>{children}</main>
        {inspectorOpen && !narrow && <div className="inspector-desktop">{inspector}</div>}
      </div>
    </div>
    <Dialog open={navOpen && mobile} onOpenChange={setNavOpen}><DialogContent className="mobile-drawer-content" aria-label="主导航与对话" aria-describedby={undefined}><DialogHeader className="drawer-heading"><DialogTitle>主导航与对话</DialogTitle></DialogHeader><WorkbenchNavigation active={view} onNavigate={onNavigate} onClose={() => setNavOpen(false)} /><AppSidebar {...sidebarProps} className="drawer-sidebar" /></DialogContent></Dialog>
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
    <Dialog open={themeOpen} onOpenChange={setThemeOpen}><DialogContent className="theme-picker-content" aria-label="显示主题" aria-describedby={undefined}><DialogHeader><DialogTitle>显示主题</DialogTitle></DialogHeader><div className="theme-options" role="group" aria-label="选择显示主题">{themes.map(option => <Button key={option.id} type="button" variant={theme === option.id ? "secondary" : "outline"} aria-pressed={theme === option.id} onClick={() => { onThemeSelect(option.id); setThemeOpen(false); }}><span className={`theme-swatch theme-swatch-${option.id}`} aria-hidden="true" /><span><strong>{option.label}</strong><small>{option.description}</small></span></Button>)}</div></DialogContent></Dialog>
    <Dialog open={inspectorOpen && narrow} onOpenChange={setInspectorOpen}><DialogContent className="mobile-inspector-content" aria-label="对象详情" aria-describedby={undefined}><DialogHeader className="drawer-heading"><DialogTitle>对象详情</DialogTitle></DialogHeader>{inspector}</DialogContent></Dialog>
  </div>;
}
