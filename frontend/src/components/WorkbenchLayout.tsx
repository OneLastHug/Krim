import { useEffect, useState, type ReactNode } from "react";
import { Menu, Palette, PanelRight } from "lucide-react";
import type { Actor } from "../lib/session-contracts";
import type { SessionState } from "../hooks/use-session";
import { workbenchTitles, type WorkbenchView } from "../lib/workbench-route";
import type { DisplayTheme } from "../lib/use-display-theme";
import { AppSidebar } from "./AppSidebar";
import { Button } from "./ui/button";
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
    update();
    media.addEventListener?.("change", update);
    return () => media.removeEventListener?.("change", update);
  }, [query, fallbackWidth]);
  return matches;
}

const themes: { id: DisplayTheme; label: string; description: string }[] = [
  { id: "warm", label: "暖纸", description: "柔和暖底与陶土色点缀" },
  { id: "bright", label: "亮彩", description: "白底与蓝紫色点缀" },
  { id: "dark", label: "深色", description: "低亮度深色界面" },
];

export function WorkbenchLayout({ view, identity, identityLoading, identityError, backendStatus, theme, onThemeSelect, chatTitle, chat, httpEnabled, beforeSessionSelect, onNavigate, actions, inspector, children }: {
  view: WorkbenchView;
  identity: Actor | null;
  identityLoading: boolean;
  identityError: string;
  backendStatus: string;
  theme: DisplayTheme;
  onThemeSelect: (theme: DisplayTheme) => void;
  chatTitle: string;
  chat: SessionState | null;
  httpEnabled: boolean;
  beforeSessionSelect?: (workspace: string, sessionId: string) => void;
  onNavigate: (view: WorkbenchView) => void;
  actions: ReactNode;
  inspector: ReactNode;
  children: ReactNode;
}) {
  const narrow = useMediaQuery("(max-width: 1023px)");
  const mobile = useMediaQuery("(max-width: 767px)");
  const [navOpen, setNavOpen] = useState(false);
  const [inspectorOpen, setInspectorOpen] = useState(false);
  const [themeOpen, setThemeOpen] = useState(false);
  const [collapsed, setCollapsed] = useState(false);
  const currentTitle = view === "chat" ? chatTitle || workbenchTitles[view] : workbenchTitles[view];
  const sidebarProps = { active: view, identity, identityLoading, identityError, chat, httpEnabled, beforeSessionSelect };
  return <div className={`workbench-shell${collapsed ? " sidebar-collapsed" : ""}`}>
    {!mobile && <div className="desktop-sidebar"><AppSidebar {...sidebarProps} onNavigate={onNavigate}/><Button variant="ghost" className="sidebar-collapse" aria-label={collapsed ? "展开导航" : "收起导航"} onClick={() => setCollapsed(value => !value)}>{collapsed ? "›" : "‹"}</Button></div>}
    <div className="workbench-main">
      <header className="workbench-topbar">
        <Button variant="ghost" className="mobile-menu-button" aria-label="打开导航" onClick={() => setNavOpen(true)}><Menu aria-hidden="true"/></Button>
        <div className="topbar-title brand"><h1 title={currentTitle}>{currentTitle}</h1><span className="brand-capability">{backendStatus}</span></div>
        <div className="topbar-status" role="status"><span className={`identity-mark${identity ? " is-ready" : ""}`} aria-hidden="true"/><span>{identity ? "已连接" : identityLoading ? "读取身份" : identityError ? "连接失败" : "未连接"}</span></div>
        <div className="topbar-actions">
          <div className="access-actions-slot">{actions}</div>
          <Button variant="ghost" aria-label="选择显示主题" title="选择显示主题" onClick={() => setThemeOpen(true)}><Palette aria-hidden="true"/></Button>
          <Button variant="ghost" aria-label={inspectorOpen ? "关闭详情面板" : "打开详情面板"} aria-pressed={inspectorOpen} onClick={() => setInspectorOpen(open => !open)}><PanelRight aria-hidden="true"/></Button>
        </div>
      </header>
      <div className={`workbench-content${inspectorOpen && !narrow ? " inspector-open" : ""}`}>
        <main className="workbench-page" id="main-content" tabIndex={-1}>{children}</main>
        {inspectorOpen && !narrow && <div className="inspector-desktop">{inspector}</div>}
      </div>
    </div>
    <Dialog open={navOpen && mobile} onOpenChange={setNavOpen}>
      <DialogContent className="mobile-drawer-content" aria-label="主导航" aria-describedby={undefined}>
        <DialogHeader className="drawer-heading"><DialogTitle>主导航</DialogTitle></DialogHeader>
        <AppSidebar {...sidebarProps} onNavigate={next => { onNavigate(next); setNavOpen(false); }} onClose={() => setNavOpen(false)} className="drawer-sidebar"/>
      </DialogContent>
    </Dialog>
    <Dialog open={themeOpen} onOpenChange={setThemeOpen}>
      <DialogContent className="theme-picker-content" aria-label="显示主题" aria-describedby={undefined}>
        <DialogHeader><DialogTitle>显示主题</DialogTitle></DialogHeader>
        <div className="theme-options" role="group" aria-label="选择显示主题">
          {themes.map(option => <Button key={option.id} type="button" variant={theme === option.id ? "secondary" : "outline"} aria-pressed={theme === option.id} onClick={() => { onThemeSelect(option.id); setThemeOpen(false); }}>
            <span className={`theme-swatch theme-swatch-${option.id}`} aria-hidden="true"/><span><strong>{option.label}</strong><small>{option.description}</small></span>
          </Button>)}
        </div>
      </DialogContent>
    </Dialog>
    <Dialog open={inspectorOpen && narrow} onOpenChange={open => setInspectorOpen(open)}>
      <DialogContent className="mobile-inspector-content" aria-label="对象详情" aria-describedby={undefined}>
        <DialogHeader className="drawer-heading"><DialogTitle>对象详情</DialogTitle></DialogHeader>
        {inspector}
      </DialogContent>
    </Dialog>
  </div>;
}
