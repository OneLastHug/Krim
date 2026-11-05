import { useRef, useState, type ReactNode } from "react";
import { ChevronDown, KeyRound, Search, Settings2, SquarePen } from "lucide-react";
import type { Actor } from "../lib/session-contracts";
import type { SessionState } from "../hooks/use-session";
import type { WorkbenchView } from "../lib/workbench-route";
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

export function AppSidebar({ active, identity, identityLoading, identityError, chat, beforeSessionSelect, onClose, onNavigate, onOpenSearch, onConnect, onCreate, theme, onThemeSelect, readingSize, onReadingSizeSelect, footerExtra, className = "" }: {
  active: WorkbenchView;
  identity: Actor | null;
  identityLoading: boolean;
  identityError: string;
  chat: SessionState | null;
  beforeSessionSelect?: (workspace: string, sessionId: string) => void;
  onClose?: () => void;
  onNavigate: (view: WorkbenchView) => void;
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
  const identityLabel = identity ? identity.principal_id : identityLoading ? "正在读取身份" : identityError ? "连接失败" : "未连接";
  const connection = identity ? "已连接" : identityLoading ? "读取身份" : identityError ? "连接失败" : "未连接";
  const canCreate = chatMode && !!chat?.ready && chat.canWrite;

  return <aside className={`app-sidebar ${className}`} aria-label="Krim Agent 工作区侧栏">
    <div className="sidebar-buttons" role="toolbar" aria-label="工作台操作">
      <Button variant="ghost" className="icon-button" aria-label="新建对话" title="新建对话" disabled={!canCreate} onClick={() => { onCreate(); onClose?.(); }}><SquarePen aria-hidden="true" /></Button>
      <span className="sidebar-buttons-divider" aria-hidden="true"></span>
      {workbenchNavigationEntries.map(({ id, label, icon: Icon }) => <Button key={id} variant="ghost" className="icon-button" aria-label={`导航到${label}`} aria-current={active === id ? "page" : undefined} title={label} onClick={() => { onNavigate(id); onClose?.(); }}><Icon aria-hidden="true" /></Button>)}
      <span className="spacer" aria-hidden="true"></span>
      <Button variant="ghost" className="icon-button" ref={settingsRef} aria-label="设置" title="设置" aria-haspopup="menu" aria-expanded={settingsOpen} onClick={() => setSettingsOpen(open => !open)}><Settings2 aria-hidden="true" /></Button>
    </div>
    {chatMode && chat && <div className="sidebar-group-label">{chat.workspace}</div>}
    {chatMode && chat && <SessionList chat={chat} beforeSelect={(workspace, sessionId) => { beforeSessionSelect?.(workspace, sessionId); onClose?.(); }} />}
    {!chatMode && <section className="sidebar-current-section" aria-label="当前分区"><span className="workspace-label">当前分区</span><strong>{sectionLabels[active]}</strong></section>}
    {footerExtra && <div className="sidebar-footer-extra">{footerExtra}</div>}
    <div className="sidebar-account">
      <Button variant="ghost" className="account-button" ref={accountRef} aria-label="密钥身份" title="密钥身份" aria-haspopup="menu" aria-expanded={accountOpen} onClick={() => setAccountOpen(open => !open)}>
        <span className="account-avatar" aria-hidden="true"><KeyRound aria-hidden="true" /></span>
        <span className="account-label"><strong title={identityLabel}>{identityLabel}</strong><span className="account-sub"><span className={`identity-mark${identity ? " is-ready" : ""}`} aria-hidden="true"></span>{connection}{identity && identity.workspaces.length ? ` · ${identity.workspaces[0]!.workspace_id}` : ""}</span></span>
        <ChevronDown aria-hidden="true" />
      </Button>
      <Button variant="ghost" className="icon-button small" aria-label="查找对话" title="查找对话 Ctrl K" onClick={() => { onOpenSearch(); onClose?.(); }}><Search aria-hidden="true" /></Button>
    </div>

    <PopoverMenu open={accountOpen} anchor={accountRef.current} onClose={() => setAccountOpen(false)} placement="above" label="密钥身份">
      <div className="menu-identity">
        <span className={`identity-mark${identity ? " is-ready" : ""}`} aria-hidden="true"></span>
        <div><strong title={identityLabel}>{identityLabel}</strong><span>{connection}{identity && identity.workspaces.length ? ` · ${identity.workspaces[0]!.workspace_id}` : ""}</span></div>
      </div>
      <button type="button" role="menuitem" className="menu-item" onClick={() => { setAccountOpen(false); setSettingsOpen(true); }}><Settings2 aria-hidden="true" />设置</button>
      <button type="button" role="menuitem" className="menu-item" onClick={() => { setAccountOpen(false); onConnect(); }}><KeyRound aria-hidden="true" />连接 / 切换密钥</button>
      <hr className="menu-separator" />
      <button type="button" role="menuitem" className="menu-item danger" disabled title="注销能力规划中">注销当前密钥</button>
      <button type="button" role="menuitem" className="menu-item danger" disabled title="退出登录规划中">退出登录</button>
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
