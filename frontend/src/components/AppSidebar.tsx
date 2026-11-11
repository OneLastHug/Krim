import { useRef, useState, type ReactNode } from "react";
import { ChevronDown, KeyRound, LogOut, Search, Settings2, SquarePen, UserRound, Users } from "lucide-react";
import type { AccountTab } from "./AccountSettings";
import type { Actor } from "../lib/session-contracts";
import type { SessionState } from "../hooks/use-session";
import type { WorkbenchView } from "../lib/workbench-route";
import type { ObsPage } from "../observability/obs-route";
import type { DisplayTheme } from "../lib/use-display-theme";
import type { ReadingSize } from "../hooks/use-reading-size";
import { workbenchNavigationEntries, workbenchThemeOptions, workbenchReadingSizes } from "./WorkbenchLayout";
import { PopoverMenu } from "./popover-menu";
import { SessionList } from "./SessionList";
import { Button } from "./ui/button";

const sectionLabels: Record<WorkbenchView, string> = {
  chat: "对话",
  run: "运行记录",
  trace: "Trace",
  datasets: "数据集",
  eval: "评测",
  settings: "设置",
};

const observationEntries = [
  ["overview", "概览"], ["runs", "运行"], ["session", "会话"], ["context", "上下文"],
  ["eval", "评测"], ["review", "复核"], ["status", "数据状态"],
] as const;

export function AppSidebar({ active, identity, identityLoading, identityError, accountName = "", gatewayName = "", workspaceNames = {}, accountAdmin = false, onAccountTab, onLogout, chat, beforeSessionSelect, onClose, onNavigate, onObservabilityPage, observabilityPage = "context", onOpenSearch, onConnect, onCreate, theme, onThemeSelect, readingSize, onReadingSizeSelect, footerExtra, className = "" }: {
  active: WorkbenchView;
  identity: Actor | null;
  identityLoading: boolean;
  identityError: string;
  accountName?: string; gatewayName?: string; workspaceNames?: Record<string, string>; accountAdmin?: boolean;
  onAccountTab?: (tab: AccountTab) => void; onLogout?: () => void;
  chat: SessionState | null;
  beforeSessionSelect?: (workspace: string, sessionId: string) => void;
  onClose?: () => void;
  onNavigate: (view: WorkbenchView) => void;
  onObservabilityPage?: (page: ObsPage) => void;
  observabilityPage?: ObsPage;
  onOpenSearch: () => void;
  onConnect: () => void;
  onCreate: () => void;
  theme: DisplayTheme;
  onThemeSelect: (theme: DisplayTheme) => void;
  readingSize: ReadingSize;
  onReadingSizeSelect: (size: ReadingSize) => void;
  footerExtra?: ReactNode;
  className?: string;
}) {
  const [accountOpen, setAccountOpen] = useState(false), [settingsOpen, setSettingsOpen] = useState(false);
  const accountRef = useRef<HTMLButtonElement>(null), settingsRef = useRef<HTMLButtonElement>(null);
  const chatMode = active === "chat";
  const identityLabel = identity ? accountName || "账号" : identityLoading ? "正在读取账号" : identityError ? "登录失效" : "未登录";
  const connection = identity ? gatewayName || "正在读取接入" : identityLoading ? "读取账号" : identityError ? "请重新登录" : "未登录";
  const canCreate = chatMode && !!chat?.ready && chat.canWrite;

  return <aside className={`app-sidebar ${className}`} aria-label="Krim Agent 工作区侧栏">
    <div className="sidebar-buttons" role="toolbar" aria-label="工作台操作">
      <Button variant="ghost" className="icon-button" aria-label="新建对话" title="新建对话" disabled={!canCreate} onClick={() => { onCreate(); onClose?.(); }}><SquarePen aria-hidden="true" /></Button>
      <span className="sidebar-buttons-divider" aria-hidden="true"></span>
      {workbenchNavigationEntries.map(({ id, label, icon: Icon }) => <Button key={id} variant="ghost" className="icon-button" aria-label={`导航到${label}`} aria-current={active === id ? "page" : undefined} title={label} onClick={() => { onNavigate(id); onClose?.(); }}><Icon aria-hidden="true" /></Button>)}
      <span className="spacer" aria-hidden="true"></span>
      <Button variant="ghost" className="icon-button" aria-label="查找对话" title="查找对话 Ctrl K" disabled={!identity} onClick={() => { onOpenSearch(); onClose?.(); }}><Search aria-hidden="true" /></Button>
    </div>
    {chatMode && chat && <div className="sidebar-group-label">{workspaceNames[chat.workspace] ?? "个人工作区"}</div>}
    {chatMode && chat && <SessionList chat={chat} beforeSelect={(workspace, sessionId) => { beforeSessionSelect?.(workspace, sessionId); onClose?.(); }} />}
    {active === "trace" ? <nav className="sidebar-current-section sidebar-observability-nav" aria-label="观测分析">
      <span className="workspace-label">观测分析</span>
      {observationEntries.map(([id, label]) => <button key={id} type="button" className="sidebar-observe-link" aria-current={observabilityPage === id ? "page" : undefined} onClick={() => { onObservabilityPage?.(id); onClose?.(); }}>{label}</button>)}
    </nav> : !chatMode && <section className="sidebar-current-section" aria-label="当前分区"><span className="workspace-label">当前分区</span><strong>{sectionLabels[active]}</strong></section>}
    {footerExtra && <div className="sidebar-footer-extra">{footerExtra}</div>}
    <div className="sidebar-account">
      <Button variant="ghost" className="account-button" ref={accountRef} aria-label="账号菜单" title={identity ? `${identityLabel} · ${connection}` : identityLabel} aria-haspopup="menu" aria-expanded={accountOpen} onClick={() => identity ? setAccountOpen(open => !open) : onConnect()}>
        <span className="account-avatar" aria-hidden="true">{identity ? identityLabel[0] : <KeyRound aria-hidden="true" />}</span>
        <span className="account-identity-line"><strong title={identityLabel}>{identityLabel}</strong>{identity && <><span className="identity-dot">·</span><span className="account-gateway" title={connection}>{connection}</span></>}</span>
        <ChevronDown aria-hidden="true" />
      </Button>
      <Button variant="ghost" className="icon-button small" ref={settingsRef} aria-label="设置" title="设置" aria-haspopup="menu" aria-expanded={settingsOpen} onClick={() => setSettingsOpen(open => !open)}><Settings2 aria-hidden="true" /></Button>
    </div>

    <PopoverMenu open={accountOpen && !!identity} anchor={accountRef.current} onClose={() => setAccountOpen(false)} placement="above" label="账号菜单">
      <div className="menu-identity">
        <div><strong title={identityLabel}>{identityLabel}</strong><span>{connection}</span></div>
      </div>
      <button type="button" role="menuitem" className="menu-item" onClick={() => { setAccountOpen(false); onAccountTab?.("profile"); onClose?.(); }}><UserRound aria-hidden="true" />个人资料</button>
      {accountAdmin && <button type="button" role="menuitem" className="menu-item" onClick={() => { setAccountOpen(false); onAccountTab?.("accounts"); onClose?.(); }}><Users aria-hidden="true" />账号管理</button>}
      <hr className="menu-separator" />
      <button type="button" role="menuitem" className="menu-item" onClick={() => { setAccountOpen(false); onConnect(); }}><KeyRound aria-hidden="true" />切换账号</button>
      <button type="button" role="menuitem" className="menu-item" onClick={() => { setAccountOpen(false); onLogout?.(); onClose?.(); }}><LogOut aria-hidden="true" />退出登录</button>
    </PopoverMenu>

    <PopoverMenu open={settingsOpen} anchor={settingsRef.current} onClose={() => setSettingsOpen(false)} label="设置" className="settings-popover">
      <h3>设置</h3>
      <section>
        <h4>主题</h4>
        <div className="theme-options" role="group" aria-label="选择显示主题">
          {workbenchThemeOptions.map(option => <Button key={option.id} variant="ghost" className={theme === option.id ? "is-selected" : ""} aria-pressed={theme === option.id} onClick={() => { onThemeSelect(option.id); setSettingsOpen(false); }}><span className={`theme-swatch theme-swatch-${option.id}`} aria-hidden="true"></span>{option.label}</Button>)}
        </div>
      </section>
      <section>
        <h4>阅读字号</h4>
        <div className="reading-size-options" role="group" aria-label="选择阅读字号">
          {workbenchReadingSizes.map(size => <Button key={size} variant="ghost" className={readingSize === size ? "is-selected" : ""} aria-pressed={readingSize === size} onClick={() => { onReadingSizeSelect(size); setSettingsOpen(false); }}>{size}</Button>)}
        </div>
      </section>
      <button type="button" role="menuitem" className="menu-item" onClick={() => { setSettingsOpen(false); onNavigate("settings"); onClose?.(); }}>全部设置…</button>
    </PopoverMenu>
  </aside>;
}
