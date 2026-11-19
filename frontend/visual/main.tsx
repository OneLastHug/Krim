import { useCallback, useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import { WorkbenchLayout } from "../src/components/WorkbenchLayout";
import { ChatWorkbench } from "../src/components/ChatWorkbench";
import { AppearanceSettings } from "../src/components/AppearanceSettings";
import { WorkbenchInspector } from "../src/components/WorkbenchInspector";
import { FixtureProvider, useVisualFixture } from "./fixture-hooks";
import type { WorkbenchView } from "../src/lib/workbench-route";
import type { DisplayTheme } from "../src/lib/use-display-theme";
import type { ReadingSize } from "../src/hooks/use-reading-size";
import "../src/styles/fonts.css";
import "../src/index.css";
import "../src/styles/workbench-refinements.css";
import "../src/styles/approved-workbench.css";
import "../src/styles/approved-workbench-adapters.css";

const fence = { current: 0 };
const noOp = () => {};
function ReviewPage() {
  const { chat } = useVisualFixture();
  const [view, setView] = useState<WorkbenchView>("chat");
  const [theme, setTheme] = useState<DisplayTheme>((new URL(location.href).searchParams.get("theme") as DisplayTheme) || "bright");
  const [size, setSize] = useState<ReadingSize>(14);
  useEffect(() => { document.documentElement.dataset.theme = theme; document.documentElement.classList.toggle("dark", theme === "dark"); document.documentElement.style.colorScheme = theme === "dark" ? "dark" : "light"; }, [theme]);
  useEffect(() => {
    if (new URL(location.href).searchParams.get("scene") !== "empty") return;
    const timer = setTimeout(() => document.querySelector<HTMLButtonElement>('.chat-toolbar [aria-label="新建对话"]')?.click(), 80);
    return () => clearTimeout(timer);
  }, []);
  const navigate = useCallback((next: WorkbenchView) => setView(next), []);
  return <div className="app-shell core-member" data-reading-size={size}>
    <WorkbenchLayout apiKey="" view={view} identity={chat.actor} identityLoading={false} identityError="" accountName="OneLastHug" gatewayName="Magpie Gateway" workspaceNames={{local:"个人工作区"}} accountAdmin onAccountTab={() => setView("settings")} onLogout={noOp} theme={theme} onThemeSelect={setTheme} readingSize={size} onReadingSizeSelect={setSize} chatTitle={chat.session?.title ?? ""} chat={chat} httpEnabled beforeSessionSelect={noOp} onNavigate={navigate} observabilityPage="context" onObservabilityPage={noOp} onConnect={noOp} onOpenTask={noOp} canOpenTask={false} actions={null} inspector={<WorkbenchInspector view={view} identity={chat.actor} objectId={chat.id} accountName="OneLastHug" gatewayName="Magpie Gateway" workspaceNames={{local:"个人工作区"}}/>}>
      <section className="page-view" hidden={view !== "chat"} aria-label="对话页面"><ChatWorkbench apiKey="" active={view === "chat"} fence={fence} theme={theme} onLegacy={noOp} onConnect={noOp}/></section>
      {view === "settings" && <AppearanceSettings theme={theme} onTheme={setTheme} size={size} onSize={setSize}/>}
    </WorkbenchLayout>
  </div>;
}
createRoot(document.getElementById("root")!).render(<FixtureProvider><ReviewPage/></FixtureProvider>);
