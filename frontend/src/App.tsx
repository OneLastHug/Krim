import { useCallback, useEffect, useRef, useState, type ReactNode } from "react";
import { object, request } from "./api";
import { ChatWorkbench } from "./components/ChatWorkbench";
import { RunComposer } from "./components/RunComposer";
import { RunAccess } from "./components/RunAccess";
import { RunWorkspace } from "./components/RunWorkspace";
import { useRun } from "./hooks/use-run";
import { EvaluationWorkbench } from "./components/EvaluationWorkbench";
import { WorkbenchLayout } from "./components/WorkbenchLayout";
import { WorkbenchInspector } from "./components/WorkbenchInspector";
import { parseActor, type Actor } from "./lib/session-contracts";
import { readWorkbenchView, type WorkbenchView } from "./lib/workbench-route";
import { useDisplayTheme } from "./lib/use-display-theme";
import { chatReason, ui } from "./lib/copy";

function UnavailablePage({ title, children }: { title: string; children: ReactNode }) {
  return <section className="capability-page" aria-labelledby="capability-heading">
    <span className="page-section-label">Krim Agent</span>
    <h2 id="capability-heading">{title}</h2>
    <p>{children}</p>
  </section>;
}

export function App() {
  const [key, setKey] = useState(""), [keyOpen, setKeyOpen] = useState(false);
  const run = useRun(key);
  const chatFence = useRef(0);
  const [mode, setMode] = useState<WorkbenchView>(() => readWorkbenchView(new URL(location.href)));
  const [sourceRun, setSourceRun] = useState("");
  const [selectedChatId, setSelectedChatId] = useState("");
  const [legacy, setLegacy] = useState<boolean | null>(null);
  const [identity, setIdentity] = useState<Actor | null>(null);
  const [identityLoading, setIdentityLoading] = useState(() => readWorkbenchView(new URL(location.href)) !== "chat"), [identityError, setIdentityError] = useState("");
  const [httpEnabled, setHttpEnabled] = useState<boolean | null>(null);
  const [chatMount, setChatMount] = useState(0);
  const { theme, toggleTheme } = useDisplayTheme();

  useEffect(() => {
    let current = true;
    const controller = new AbortController();
    setHttpEnabled(null);
    void request("/healthz", key, undefined, controller.signal).then(raw => {
      if (current) {
        const value = object(raw).http_enabled;
        setHttpEnabled(typeof value === "boolean" ? value : null);
      }
    }).catch(() => { if (current) setHttpEnabled(null); });
    return () => { current = false; controller.abort(); };
  }, [key]);

  // 非对话页面仍读取同一个只读身份接口；会话账本仍只由 useSession 管理。
  useEffect(() => {
    if (mode === "chat") return;
    let current = true;
    const controller = new AbortController();
    setIdentityLoading(true);
    void request("/v2/me", key, undefined, controller.signal).then(raw => {
      const value = parseActor(raw);
      if (current) {
        setIdentity(value);
        setLegacy(value.legacy);
        setIdentityLoading(false);
        setIdentityError("");
      }
    }).catch(error => {
      if (current) {
        setIdentity(null);
        setIdentityLoading(false);
        setIdentityError(chatReason(error));
      }
    });
    return () => { current = false; controller.abort(); };
  }, [key, mode]);

  const acceptChatIdentity = useCallback((value: Actor | null, error = "") => {
    setIdentity(value);
    setLegacy(value?.legacy ?? null);
    setIdentityLoading(value === null && !error);
    setIdentityError(error);
  }, []);

  useEffect(() => {
    const onPopState = () => {
      chatFence.current++;
      const next = readWorkbenchView(new URL(location.href));
      setMode(next);
      if (next !== "chat") setIdentityLoading(true);
      setChatMount(value => value + 1);
      setSourceRun("");
    };
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  function choose(view: WorkbenchView) {
    if (view !== mode) {
      chatFence.current++;
      const url = new URL(location.href);
      url.searchParams.set("view", view);
      window.history.pushState(null, "", url);
      if (view !== "chat" && legacy === null) setIdentityLoading(true);
      setMode(view);
    }
  }

  function recordChatSelection(workspace: string, sessionId: string) {
    const url = new URL(location.href);
    if (url.searchParams.get("view") === "chat" && url.searchParams.get("workspace") === workspace && (url.searchParams.get("session") ?? "") === sessionId) return;
    window.history.pushState(null, "", url);
  }

  function recordEvaluationSelection(currentId: string) {
    const url = new URL(location.href);
    if (currentId && url.searchParams.get("eval_id") === currentId) return;
    window.history.pushState(null, "", url);
  }

  const isLegacyRouteAvailable = legacy === true || (legacy === null && (!identityLoading || !!identityError));
  const backendStatus = httpEnabled === false ? ui.offline : ui.agent;
  const currentUrl = new URL(location.href);
  const objectId = mode === "chat" ? selectedChatId
    : mode === "run" ? run.id || currentUrl.hash.slice(1) || currentUrl.searchParams.get("run_id") || ""
      : mode === "eval" ? currentUrl.searchParams.get("eval_id") ?? "" : "";

  return <div className={`app-shell${legacy === false ? " core-member" : ""}`}>
    <WorkbenchLayout
      view={mode}
      identity={identity}
      identityLoading={identityLoading}
      identityError={identityError}
      backendStatus={backendStatus}
      theme={theme}
      onThemeToggle={toggleTheme}
      onNavigate={choose}
      actions={<RunAccess apiKey={key} onKey={value => { chatFence.current++; run.invalidate(); setSelectedChatId(""); setKey(value); run.refresh(); }} onOpen={id => { choose("run"); run.select(id); }} keyOpen={keyOpen} setKeyOpen={setKeyOpen} isLegacy={isLegacyRouteAvailable}/>}
      inspector={<WorkbenchInspector view={mode} objectId={objectId} identity={identity}/>}
    >
      <section className="page-view" hidden={mode !== "chat"} aria-label="对话页面">
        <ChatWorkbench key={chatMount} apiKey={key} active={mode === "chat"} fence={chatFence} onLegacy={setLegacy} onIdentity={acceptChatIdentity} onSelection={setSelectedChatId} beforeSessionSelect={recordChatSelection} httpEnabled={httpEnabled === true} onConnect={() => setKeyOpen(true)}/>
      </section>
      <section className="page-view" hidden={mode !== "run"} aria-label="运行记录页面">
        {legacy === false ? <UnavailablePage title="运行记录暂不可用">当前身份未提供旧版运行接口。工作区任务仍可从对话入口发起。</UnavailablePage>
          : legacy === null && identityLoading && !identityError ? <UnavailablePage title="正在读取身份">确认当前身份的运行能力后显示对应记录。</UnavailablePage>
            : <div className="workbench"><RunComposer run={run} onConnect={() => setKeyOpen(true)}/><RunWorkspace key={run.id} run={run} onCandidate={id => { setSourceRun(id); choose("eval"); }}/></div>}
      </section>
      <section className="page-view" hidden={mode !== "datasets"} aria-label="数据集页面">
        <UnavailablePage title="数据集工作流尚未独立接入">候选审核、样本编辑与冻结目前仍位于旧版评测流程中。{identityLoading ? "正在读取当前身份的能力。" : legacy === false ? "当前身份没有旧版评测能力。" : " 可前往评测查看现有能力。"}</UnavailablePage>
        {isLegacyRouteAvailable && <button className="inline-navigation" type="button" onClick={() => choose("eval")}>打开旧版评测</button>}
      </section>
      <section className="page-view" hidden={mode !== "eval"} aria-label="评测页面">
        {legacy === false ? <UnavailablePage title="旧版评测能力不可用">当前身份未提供旧版评测接口；中性评测页面尚未接入。</UnavailablePage>
          : legacy === null && identityLoading && !identityError ? <UnavailablePage title="正在读取身份">确认当前身份的评测能力后显示可用入口。</UnavailablePage>
            : <EvaluationWorkbench apiKey={key} active={mode === "eval"} sourceRun={sourceRun} onRun={id => { choose("run"); run.select(id); }} beforeSelect={recordEvaluationSelection}/>}
      </section>
      <section className="page-view" hidden={mode !== "settings"} aria-label="设置页面">
        <section className="settings-page" aria-labelledby="settings-heading">
          <span className="page-section-label">Krim Agent</span><h2 id="settings-heading">设置</h2>
          <div className="settings-section"><h3>当前身份与工作区</h3>{identity ? <><p>{identity.principal_id}</p><ul>{identity.workspaces.map(workspace => <li key={workspace.workspace_id}><span>{workspace.workspace_id}</span><span>{workspace.role === "admin" ? "管理员" : workspace.role === "viewer" ? "只读" : "成员"}</span></li>)}</ul><p>{identity.legacy ? "旧版运行与评测能力可用。" : "当前身份提供工作区会话能力。"}</p></> : <p role="status">{identityLoading ? "正在读取身份与工作区能力。" : identityError || "连接后读取身份与工作区能力。"}</p>}</div>
          <div className="settings-section"><h3>显示</h3><p>当前使用{theme === "dark" ? "深色" : "浅色"}主题。</p><button className="inline-navigation" type="button" onClick={toggleTheme}>切换到{theme === "dark" ? "浅色" : "深色"}主题</button></div>
        </section>
      </section>
    </WorkbenchLayout>
  </div>;
}
