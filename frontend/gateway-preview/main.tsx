import { useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import { ArrowLeft } from "lucide-react";
import { WorkbenchLayout } from "../src/components/WorkbenchLayout";
import { ChatWorkbench } from "../src/components/ChatWorkbench";
import { AppearanceSettings } from "../src/components/AppearanceSettings";
import { WorkbenchInspector } from "../src/components/WorkbenchInspector";
import { Button } from "../src/components/ui/button";
import type { WorkbenchView } from "../src/lib/workbench-route";
import type { DisplayTheme } from "../src/lib/use-display-theme";
import type { ReadingSize } from "../src/hooks/use-reading-size";
import { GatewayFixtureProvider, useVisualFixture } from "./fixture-hooks";
import { GatewayPreviewProvider, useGatewayPreview } from "./gateway-state";
import { GatewaySettings } from "./GatewaySettings";
import "../src/styles/fonts.css";
import "../src/index.css";
import "../src/styles/workbench-refinements.css";
import "../src/styles/approved-workbench.css";
import "../src/styles/approved-workbench-adapters.css";
import "../src/components/account-settings.css";
import "./gateway-preview.css";
import "./shell.css";

const fence = { current: 0 };
const noop = () => {};
type Tab = "profile" | "gateways" | "appearance" | "accounts" | "events";
function PreviewApp() {
  const { chat } = useVisualFixture();
  const gateways = useGatewayPreview();
  const [view, setView] = useState<WorkbenchView>("settings");
  const [tab, setTab] = useState<Tab>("gateways");
  const [theme, setTheme] = useState<DisplayTheme>(() => {
    const value = localStorage.getItem("krim.gateway-preview-theme.v1");
    return value === "dark" || value === "warm" ? value : "bright";
  });
  const [size, setSize] = useState<ReadingSize>(14);
  const [accountName, setAccountName] = useState(() => localStorage.getItem("krim.gateway-preview-name.v1") || "本地账号");
  const [nameDraft, setNameDraft] = useState(accountName), [savedName, setSavedName] = useState(false);
  const selection = gateways.forSession(chat.id || "new");
  const model = gateways.models.find(item => item.id === selection.modelId);
  const gatewayName = gateways.gateways.find(item => item.id === model?.gatewayId)?.name ?? "未选择接入";
  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    document.documentElement.classList.toggle("dark", theme === "dark");
    document.documentElement.style.colorScheme = theme === "dark" ? "dark" : "light";
    localStorage.setItem("krim.gateway-preview-theme.v1", theme);
  }, [theme]);
  useEffect(() => {
    const open = () => { setTab("gateways"); setView("settings"); };
    window.addEventListener("krim:gateway-settings", open);
    return () => window.removeEventListener("krim:gateway-settings", open);
  }, []);
  function accountTab(next: "profile" | "accounts" | "events" | "appearance") { setTab(next); setView("settings"); }
  function navigate(next: WorkbenchView) { if (next === "settings") setTab("gateways"); setView(next); }
  const tabs: { id: Tab; label: string }[] = [{ id: "profile", label: "个人资料" }, { id: "gateways", label: "接入与模型" }, { id: "accounts", label: "账号管理" }, { id: "events", label: "管理记录" }, { id: "appearance", label: "外观与偏好" }];
  return <div className="app-shell core-member" data-reading-size={size}>
    <WorkbenchLayout apiKey="" view={view} identity={chat.actor} identityLoading={false} identityError="" accountName={accountName} gatewayName={gatewayName} workspaceNames={{ local: "个人工作区" }} accountAdmin onAccountTab={accountTab} onLogout={() => { setTab("profile"); setView("settings"); }} theme={theme} onThemeSelect={setTheme} readingSize={size} onReadingSizeSelect={setSize} chatTitle={chat.session?.title ?? ""} chat={chat} httpEnabled beforeSessionSelect={noop} onNavigate={navigate} observabilityPage="context" onObservabilityPage={noop} onConnect={() => { setTab("profile"); setView("settings"); }} onOpenTask={noop} canOpenTask={false} actions={null} inspector={<WorkbenchInspector view={view} identity={chat.actor} objectId={chat.id} accountName={accountName} gatewayName={gatewayName} workspaceNames={{ local: "个人工作区" }}/>}>
      <section className="page-view" hidden={view !== "chat"} aria-label="对话页面"><ChatWorkbench apiKey="" active={view === "chat"} fence={fence} theme={theme} onLegacy={noop} onConnect={() => { setTab("gateways"); setView("settings"); }}/></section>
      {view === "settings" && <section className="gateway-settings-page page-view" aria-label="设置页面"><div className={`accounts-settings${tab === "gateways" ? " gateway-settings-wide" : ""}`}>
        <div className="gateway-back-row"><Button variant="ghost" onClick={() => setView("chat")}><ArrowLeft aria-hidden="true"/>返回对话</Button></div>
        <nav className="account-tabs" aria-label="账号设置">{tabs.map(item => <button type="button" className="account-tab" aria-current={tab === item.id ? "page" : undefined} key={item.id} onClick={() => setTab(item.id)}>{item.label}</button>)}</nav>
        {tab === "gateways" && <GatewaySettings/>}
        {tab === "appearance" && <AppearanceSettings theme={theme} onTheme={setTheme} size={size} onSize={setSize}/>}
        {tab === "profile" && <><div className="account-heading"><div><h2>个人资料</h2><p>管理你在 Krim 中的名称和访问密钥。</p></div></div><div className="account-field account-profile-name"><label htmlFor="preview-account-name">显示名称</label><input id="preview-account-name" className="account-input" value={nameDraft} maxLength={64} onChange={e => { setNameDraft(e.target.value); setSavedName(false); }}/><p className="account-hint">这会显示在账号菜单和管理列表中。</p></div><div className="account-form-actions account-save-row"><Button disabled={!nameDraft.trim()} onClick={() => { setAccountName(nameDraft.trim()); localStorage.setItem("krim.gateway-preview-name.v1", nameDraft.trim()); setSavedName(true); }}>保存更改</Button>{savedName && <span className="account-hint" role="status">已保存</span>}</div><section className="account-section"><h3>当前接入</h3><dl className="account-detail-list"><div className="account-detail-row"><dt>接入方式</dt><dd>{gatewayName}</dd></div><div className="account-detail-row"><dt>工作区</dt><dd>个人工作区</dd></div><div className="account-detail-row"><dt>平台角色</dt><dd>管理员</dd></div></dl><div className="account-key-actions"><Button variant="outline" onClick={() => setTab("gateways")}>管理接入与模型</Button></div></section><section className="account-section"><h3>访问密钥</h3><p className="account-section-description">当前使用本地预览账号，无需登录密钥。</p></section></>}
        {tab === "accounts" && <><div className="account-heading"><div><h2>账号管理</h2><p>当前工作区的账号与访问状态。</p></div></div><table className="account-table"><thead><tr><th>账号</th><th>平台角色</th><th>状态</th></tr></thead><tbody><tr><td><span className="account-name">{accountName}</span></td><td>管理员</td><td><span className="account-status">正常</span></td></tr></tbody></table></>}
        {tab === "events" && <><div className="account-heading"><div><h2>管理记录</h2><p>查看账号与接入设置的变更。</p></div></div><div className="account-empty"><h3>暂无管理记录</h3><p>当前预览仅在此浏览器保存设置。</p></div></>}
      </div></section>}
    </WorkbenchLayout>
  </div>;
}
createRoot(document.getElementById("root")!).render(<GatewayPreviewProvider><GatewayFixtureProvider><PreviewApp/></GatewayFixtureProvider></GatewayPreviewProvider>);
