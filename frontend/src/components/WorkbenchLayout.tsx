import { useEffect, useState, type ReactNode } from "react";
import { Menu, Moon, PanelRight, Sun } from "lucide-react";
import type { Actor } from "../lib/session-contracts";
import { workbenchTitles, type WorkbenchView } from "../lib/workbench-route";
import type { DisplayTheme } from "../lib/use-display-theme";
import { AppSidebar } from "./AppSidebar";
import { Button } from "./ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "./ui/dialog";

function useMediaQuery(query: string) {
  const [matches, setMatches] = useState(() => typeof window !== "undefined" && (window.matchMedia?.(query).matches ?? window.innerWidth <= 1023));
  useEffect(() => {
    const media = window.matchMedia?.(query);
    if (!media) {
      const update = () => setMatches(window.innerWidth <= 1023);
      window.addEventListener("resize", update);
      return () => window.removeEventListener("resize", update);
    }
    const update = () => setMatches(media.matches);
    update();
    media.addEventListener?.("change", update);
    return () => media.removeEventListener?.("change", update);
  }, [query]);
  return matches;
}

export function WorkbenchLayout({ view, identity, identityLoading, identityError, backendStatus, theme, onThemeToggle, onNavigate, actions, inspector, children }: {
  view: WorkbenchView;
  identity: Actor | null;
  identityLoading: boolean;
  identityError: string;
  backendStatus: string;
  theme: DisplayTheme;
  onThemeToggle: () => void;
  onNavigate: (view: WorkbenchView) => void;
  actions: ReactNode;
  inspector: ReactNode;
  children: ReactNode;
}) {
  const narrow = useMediaQuery("(max-width: 1023px)");
  const [navOpen, setNavOpen] = useState(false);
  const [inspectorOpen, setInspectorOpen] = useState(false);
  const [collapsed, setCollapsed] = useState(false);
  return <div className={`workbench-shell${collapsed ? " sidebar-collapsed" : ""}`}>
    <div className="desktop-sidebar"><AppSidebar active={view} identity={identity} identityLoading={identityLoading} identityError={identityError} onNavigate={onNavigate}/><Button variant="ghost" className="sidebar-collapse" aria-label={collapsed ? "展开导航" : "收起导航"} onClick={() => setCollapsed(value => !value)}>{collapsed ? "›" : "‹"}</Button></div>
    <div className="workbench-main">
      <header className="workbench-topbar">
        <Button variant="ghost" className="mobile-menu-button" aria-label="打开导航" onClick={() => setNavOpen(true)}><Menu aria-hidden="true"/></Button>
        <div className="topbar-title brand"><span className="topbar-context">Krim Agent</span><h1>{workbenchTitles[view]}</h1><span className="brand-capability">{backendStatus}</span></div>
        <div className="topbar-status" role="status"><span className={`identity-mark${identity ? " is-ready" : ""}`} aria-hidden="true"/><span>{identity ? identity.legacy ? "旧版兼容" : "工作区" : identityLoading ? "读取身份" : identityError ? "连接失败" : "未连接"}</span><span className="status-divider" aria-hidden="true"/><span>{backendStatus}</span></div>
        <div className="topbar-actions">
          <div className="access-actions-slot">{actions}</div>
          <Button variant="ghost" aria-label={theme === "dark" ? "切换到浅色主题" : "切换到深色主题"} title={theme === "dark" ? "切换到浅色主题" : "切换到深色主题"} onClick={onThemeToggle}>{theme === "dark" ? <Sun aria-hidden="true"/> : <Moon aria-hidden="true"/>}</Button>
          <Button variant="ghost" aria-label={inspectorOpen ? "关闭详情面板" : "打开详情面板"} aria-pressed={inspectorOpen} onClick={() => setInspectorOpen(open => !open)}><PanelRight aria-hidden="true"/></Button>
        </div>
      </header>
      <div className={`workbench-content${inspectorOpen && !narrow ? " inspector-open" : ""}`}>
        <main className="workbench-page" id="main-content" tabIndex={-1}>{children}</main>
        {inspectorOpen && !narrow && <div className="inspector-desktop">{inspector}</div>}
      </div>
    </div>
    <Dialog open={navOpen} onOpenChange={setNavOpen}>
      <DialogContent className="mobile-drawer-content" aria-label="主导航" aria-describedby={undefined}>
        <DialogHeader className="drawer-heading"><DialogTitle>主导航</DialogTitle></DialogHeader>
        <AppSidebar active={view} identity={identity} identityLoading={identityLoading} identityError={identityError} onNavigate={view => { onNavigate(view); setNavOpen(false); }} onClose={() => setNavOpen(false)} className="drawer-sidebar"/>
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
