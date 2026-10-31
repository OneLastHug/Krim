import { useCallback, useEffect, useRef, useState, type ReactNode } from "react";
import { object, request } from "./api";
import { ChatWorkbench } from "./components/ChatWorkbench";
import { TraceWorkbench } from "./components/TraceWorkbench";
import { RunComposer } from "./components/RunComposer";
import { RunAccess } from "./components/RunAccess";
import { RunWorkspace } from "./components/RunWorkspace";
import { useRun } from "./hooks/use-run";
import { EvaluationWorkbench } from "./components/EvaluationWorkbench";
import { EvaluationProductWorkbench } from "./components/EvaluationProductWorkbench";
import { EvaluationAutomationPanel } from "./components/EvaluationAutomationPanel";
import { Button } from "./components/ui/button";
import { WorkbenchLayout } from "./components/WorkbenchLayout";
import { WorkbenchInspector } from "./components/WorkbenchInspector";
import { parseActor, type Actor } from "./lib/session-contracts";
import { readWorkbenchView, type WorkbenchView } from "./lib/workbench-route";
import { useDisplayTheme } from "./lib/use-display-theme";
import { chatReason, ui } from "./lib/copy";
import type { SessionState } from "./hooks/use-session";

function UnavailablePage({ title, children }: { title: string; children: ReactNode }) {
  return <section className="capability-page" aria-labelledby="capability-heading">
    <span className="page-section-label">Krim Agent</span>
    <h2 id="capability-heading">{title}</h2>
    <p>{children}</p>
  </section>;
}

export function App() {
  const [key, setKey] = useState(""), [keyOpen, setKeyOpen] = useState(false);
  const chatFence = useRef(0);
  const [mode, setMode] = useState<WorkbenchView>(() => readWorkbenchView(new URL(location.href)));
  const [sourceRun, setSourceRun] = useState(() => new URL(location.href).searchParams.get("source_run") ?? "");
  const [selectedChatId, setSelectedChatId] = useState("");
  const [selectedTraceRunId, setSelectedTraceRunId] = useState("");
  const chatNavigation = useRef<{ key: string; state: SessionState } | null>(null);
  const liveKey = useRef(key);
  liveKey.current = key;
  const [, setChatNavigationRevision] = useState(0);
  const [selectedProductTaskId, setSelectedProductTaskId] = useState("");
  const [, redrawRoute] = useState(0);
  const [legacy, setLegacy] = useState<boolean | null>(null);
  const [identityState, setIdentity] = useState<{ key: string; actor: Actor } | null>(null);
  const identity = identityState?.key === key ? identityState.actor : null;
  const [identityLoading, setIdentityLoading] = useState(() => readWorkbenchView(new URL(location.href)) !== "chat"), [identityError, setIdentityError] = useState("");
  const [httpEnabled, setHttpEnabled] = useState<boolean | null>(null);
  const [productEnabled, setProductEnabled] = useState<boolean | null>(null);
  const [automationEnabled, setAutomationEnabled] = useState(false);
  const [automationOpen, setAutomationOpen] = useState(false);
  const [chatMount, setChatMount] = useState(0);
  const { theme, selectTheme } = useDisplayTheme();
  const isLegacyRouteAvailable = !!identity?.legacy && !identityLoading;
  const run = useRun(key, mode === "run", isLegacyRouteAvailable ? JSON.stringify([identity!.principal_id, identity!.generation]) : null);

  const receiveChatNavigation = useCallback((value: SessionState, sourceKey: string) => {
    if (sourceKey !== liveKey.current) return;
    chatNavigation.current = { key: sourceKey, state: value };
    setChatNavigationRevision(revision => revision + 1);
  }, []);

  useEffect(() => {
    let current = true;
    const controller = new AbortController();
    setHttpEnabled(null); setProductEnabled(null); setAutomationEnabled(false);
    void request("/healthz", key, undefined, controller.signal).then(raw => {
      if (current) {
        const health = object(raw), value = health.http_enabled;
        setHttpEnabled(typeof value === "boolean" ? value : null);
        setProductEnabled(health.product_eval_enabled === true);
        setAutomationEnabled(health.evaluation_automation_enabled === true);
      }
    }).catch(() => { if (current) { setHttpEnabled(null); setProductEnabled(false); } });
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
      if (current && key === liveKey.current) {
        setIdentity({ key, actor: value });
        setLegacy(value.legacy);
        setIdentityLoading(false);
        setIdentityError("");
      }
    }).catch(error => {
      if (current && key === liveKey.current) {
        setIdentity(null);
        setIdentityLoading(false);
        setIdentityError(chatReason(error));
      }
    });
    return () => { current = false; controller.abort(); };
  }, [key, mode, chatMount]);

  const acceptChatIdentity = useCallback((value: Actor | null, error: string, sourceKey: string) => {
    if (sourceKey !== liveKey.current) return;
    setIdentity(value ? { key: sourceKey, actor: value } : null);
    setLegacy(value?.legacy ?? null);
    setIdentityLoading(value === null && !error);
    setIdentityError(error);
  }, []);

  function applyKey(value: string) {
    chatFence.current++;
    liveKey.current = value;
    chatNavigation.current = null;
    run.invalidate();
    setSelectedChatId(""); setSelectedTraceRunId(""); setSelectedProductTaskId(""); setSourceRun("");
    const url = new URL(location.href);
    url.searchParams.delete("source_run"); url.searchParams.delete("product_candidate_id");
    window.history.replaceState(null, "", url);
    setIdentity(null); setLegacy(null); setIdentityError(""); setIdentityLoading(true);
    setKey(value); setChatMount(revision => revision + 1);
  }

  useEffect(() => {
    const onPopState = () => {
      chatFence.current++;
      const next = readWorkbenchView(new URL(location.href));
      setMode(next);
      setSelectedTraceRunId("");
      if (next !== "chat") setIdentityLoading(true);
      setChatMount(value => value + 1);
      setSourceRun(new URL(location.href).searchParams.get("source_run") ?? ""); setSelectedProductTaskId("");
    };
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  function choose(view: WorkbenchView) {
    const url = new URL(location.href);
    url.searchParams.set("view", view);
    if (productEnabled && (view === "eval" || view === "datasets")) {
      url.searchParams.delete("eval_id"); url.searchParams.delete("legacy_eval");
    }
    if (view !== mode || url.href !== location.href) {
      chatFence.current++;
      window.history.pushState(null, "", url);
      if (view !== "chat" && legacy === null) setIdentityLoading(true);
      setMode(view); redrawRoute(value => value + 1);
    }
  }

  function chooseWorkspace(workspace: string) {
    if (!identity?.workspaces.some(value => value.workspace_id === workspace)) return;
    chatFence.current++;
    chatNavigation.current = null;
    const url = new URL(location.href);
    url.searchParams.set("workspace", workspace);
    for (const field of ["session", "turn", "eval_id", "legacy_eval", "product_eval_id", "product_candidate_id", "source_run"]) url.searchParams.delete(field);
    window.history.pushState(null, "", url);
    setSelectedChatId(""); setSelectedTraceRunId(""); setSelectedProductTaskId(""); setSourceRun("");
    setChatMount(value => value + 1);
  }

  function evaluateRun(workspace: string, id: string) {
    const membership = identity?.workspaces.find(value => value.workspace_id === workspace);
    if (productEnabled !== true || !membership || membership.role === "viewer" || !id) return;
    chatFence.current++;
    const url = new URL(location.href);
    url.searchParams.set("view", "datasets"); url.searchParams.set("workspace", workspace);
    url.searchParams.set("source_run", id);
    for (const field of ["eval_id", "legacy_eval", "product_eval_id", "product_candidate_id"]) url.searchParams.delete(field);
    window.history.pushState(null, "", url);
    setSourceRun(id); setSelectedProductTaskId(""); setMode("datasets");
  }

  function openTrace(workspace: string, sessionId: string, turnId: string) {
    if (!workspace || !sessionId || !turnId) return;
    chatFence.current++;
    const url = new URL(location.href);
    url.searchParams.set("view", "trace");
    url.searchParams.set("workspace", workspace);
    url.searchParams.set("session", sessionId);
    url.searchParams.set("turn", turnId);
    window.history.pushState(null, "", url);
    setMode("trace");
    setSelectedTraceRunId("");
    redrawRoute(value => value + 1);
  }

  function returnToTraceSession(sessionId: string) {
    if (!sessionId) return;
    chatFence.current++;
    const url = new URL(location.href);
    url.searchParams.set("view", "chat");
    url.searchParams.set("session", sessionId);
    url.searchParams.delete("turn");
    window.history.pushState(null, "", url);
    setMode("chat");
    setSelectedChatId("");
    setSelectedTraceRunId("");
    setChatMount(value => value + 1);
  }

  function openFeedbackCandidate(runId: string, candidateId: string) {
    const membership = identity?.workspaces.find(value => value.workspace_id === productWorkspace);
    if (!membership || !productEnabled || !candidateId || !runId) return;
    const url = new URL(location.href);
    url.searchParams.set("view", "datasets"); url.searchParams.set("workspace", productWorkspace);
    url.searchParams.set("source_run", runId); url.searchParams.set("product_candidate_id", candidateId);
    for (const field of ["eval_id", "legacy_eval", "product_eval_id"]) url.searchParams.delete(field);
    chatFence.current++; window.history.pushState(null, "", url);
    setSourceRun(runId); setSelectedProductTaskId(""); setMode("datasets"); setAutomationOpen(false);
  }

  const selectProductCandidate = useCallback((id: string, runId?: string) => {
    const url = new URL(location.href);
    if (url.searchParams.get("view") !== "datasets" || url.searchParams.get("product_candidate_id") === id) return;
    url.searchParams.set("product_candidate_id", id);
    if (runId) { url.searchParams.set("source_run", runId); setSourceRun(runId); }
    else { url.searchParams.delete("source_run"); setSourceRun(""); }
    window.history.pushState(null, "", url); redrawRoute(value => value + 1);
  }, []);

  const selectProductTask = useCallback((id: string) => {
    setSelectedProductTaskId(id);
    const url = new URL(location.href);
    if (url.searchParams.get("view") === "eval" && url.searchParams.get("product_eval_id") !== id) {
      url.searchParams.set("product_eval_id", id);
      window.history.pushState(null, "", url);
    }
  }, []);

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

  const backendStatus = httpEnabled === false ? ui.offline : ui.agent;
  const currentUrl = new URL(location.href);
  const productWorkspace = identity?.workspaces.find(value => value.workspace_id === currentUrl.searchParams.get("workspace"))?.workspace_id ?? identity?.workspaces[0]?.workspace_id ?? "local";
  const productMembership = identity?.workspaces.find(value => value.workspace_id === productWorkspace);
  const productCanWrite = !!productMembership && productMembership.role !== "viewer";
  const legacyEvaluation = productEnabled !== true || currentUrl.searchParams.has("eval_id") || currentUrl.searchParams.get("legacy_eval") === "1";
  const objectId = mode === "chat" ? selectedChatId
    : mode === "run" ? isLegacyRouteAvailable ? run.id || currentUrl.hash.slice(1) || currentUrl.searchParams.get("run_id") || "" : ""
      : mode === "trace" ? selectedTraceRunId || (currentUrl.searchParams.get("turn") ?? "")
        : mode === "eval" ? (legacyEvaluation ? currentUrl.searchParams.get("eval_id") ?? "" : selectedProductTaskId) : "";
  const navigationState = chatNavigation.current?.key === key ? chatNavigation.current.state : null;
  const sidebarChat = identity && navigationState?.actor && navigationState.actor.principal_id === identity.principal_id && navigationState.actor.generation === identity.generation ? navigationState : null;
  const chatTitle = sidebarChat?.session?.title ?? sidebarChat?.list.find(value => value.id === sidebarChat?.id)?.title ?? "";

  return <div className={`app-shell${legacy === false ? " core-member" : ""}`}>
    <WorkbenchLayout
      view={mode}
      identity={identity}
      identityLoading={identityLoading}
      identityError={identityError}
      backendStatus={backendStatus}
      theme={theme}
      onThemeSelect={selectTheme}
      chatTitle={chatTitle}
      chat={sidebarChat}
      httpEnabled={httpEnabled === true}
      beforeSessionSelect={recordChatSelection}
      onNavigate={choose}
      actions={<>
        {mode !== "chat" && identity && identity.workspaces.length > 1 && <select className="workbench-workspace-select" aria-label="当前工作区" value={productWorkspace} onChange={event => chooseWorkspace(event.target.value)}>
          {identity.workspaces.map(value => <option key={value.workspace_id} value={value.workspace_id}>{value.workspace_id}</option>)}
        </select>}
        <RunAccess apiKey={key} onKey={applyKey} onOpen={id => { choose("run"); run.select(id); }} keyOpen={keyOpen} setKeyOpen={setKeyOpen} isLegacy={isLegacyRouteAvailable}/>
      </>}
      inspector={<WorkbenchInspector view={mode} objectId={objectId} identity={identity}/>}
    >
      <section className="page-view" hidden={mode !== "chat"} aria-label="对话页面">
        <ChatWorkbench key={chatMount} apiKey={key} active={mode === "chat"} fence={chatFence} onLegacy={setLegacy} onIdentity={acceptChatIdentity} onChatState={receiveChatNavigation} onTrace={openTrace} onSelection={setSelectedChatId} onConnect={() => setKeyOpen(true)}/>
      </section>
      <section className="page-view" hidden={mode !== "run"} aria-label="运行记录页面">
        {mode === "run" && (legacy === false ? <UnavailablePage title="运行记录暂不可用">当前身份未提供旧版运行接口。工作区任务仍可从对话入口发起。</UnavailablePage>
          : !isLegacyRouteAvailable ? <UnavailablePage title={identityLoading ? "正在读取身份" : "请连接以读取运行记录"}>{identityError || "确认当前身份的运行能力后显示对应记录。"}</UnavailablePage>
            : <div className="workbench"><RunComposer run={run} onConnect={() => setKeyOpen(true)}/><RunWorkspace key={run.id} run={run} onCandidate={id => { if (productEnabled) evaluateRun(productWorkspace, id); else { setSourceRun(id); choose("eval"); } }}/></div>)}
      </section>
      <section className="page-view" hidden={mode !== "trace"} aria-label="Trace 页面">
        <TraceWorkbench apiKey={key} workspace={currentUrl.searchParams.get("workspace") ?? productWorkspace} identity={identity} active={mode === "trace" && !identityLoading} sessionId={currentUrl.searchParams.get("session") ?? undefined} turnId={currentUrl.searchParams.get("turn") ?? undefined} onSelection={setSelectedTraceRunId} onSession={returnToTraceSession} onEvaluate={productEnabled ? (workspace, runId) => evaluateRun(workspace, runId) : undefined}/>
      </section>
      {productEnabled && (mode === "datasets" || mode === "eval" && !legacyEvaluation) && (identityLoading || !identity) && <UnavailablePage title={identityLoading ? "正在读取工作区" : "请连接工作区"}>{identityError || "正在确认当前身份与工作区能力。"}</UnavailablePage>}
      {productEnabled && automationEnabled && mode === "eval" && !legacyEvaluation && <nav className="product-eval-actions" aria-label="评测功能"><Button variant={automationOpen ? "ghost" : "secondary"} aria-pressed={!automationOpen} onClick={() => setAutomationOpen(false)}>评测任务</Button><Button variant={automationOpen ? "secondary" : "ghost"} aria-pressed={automationOpen} onClick={() => setAutomationOpen(true)}>持续回归</Button></nav>}
      {productEnabled && identity && <EvaluationProductWorkbench key={`product-${chatMount}`} apiKey={key} workspace={productWorkspace} identity={identity} canWrite={productCanWrite} canApprove={productCanWrite && productMembership?.approve === true} active={(mode === "datasets" || mode === "eval" && !legacyEvaluation && !(automationEnabled && automationOpen)) && !identityLoading} view={mode === "datasets" ? "datasets" : "eval"} sourceRun={sourceRun} candidateId={currentUrl.searchParams.get("product_candidate_id") ?? undefined} onCandidateSelection={selectProductCandidate} taskId={currentUrl.searchParams.get("product_eval_id") ?? undefined} onTaskSelection={selectProductTask} onRun={isLegacyRouteAvailable ? id => { choose("run"); run.select(id); } : undefined}/>}
      {automationEnabled && identity && <EvaluationAutomationPanel key={`automation-${chatMount}`} apiKey={key} workspace={productWorkspace} identity={identity} active={mode === "eval" && !legacyEvaluation && automationOpen && !identityLoading} canWrite={productCanWrite} canApprove={productCanWrite && productMembership?.approve === true} isAdmin={productMembership?.role === "admin"} sourceRun={sourceRun} onCandidate={openFeedbackCandidate} onTask={id => { setAutomationOpen(false); selectProductTask(id); }}/> }
      <section className="page-view" hidden={mode !== "datasets" || productEnabled === true} aria-label="数据集页面">
        <UnavailablePage title="数据集工作流尚未独立接入">候选审核、样本编辑与冻结目前仍位于旧版评测流程中。{identityLoading ? "正在读取当前身份的能力。" : legacy === false ? "当前身份没有旧版评测能力。" : " 可前往评测查看现有能力。"}</UnavailablePage>
        {isLegacyRouteAvailable && <button className="inline-navigation" type="button" onClick={() => choose("eval")}>打开旧版评测</button>}
      </section>
      <section className="page-view" hidden={mode !== "eval" || !legacyEvaluation} aria-label="评测页面">
        {legacy === false ? <UnavailablePage title="旧版评测能力不可用">当前身份未提供旧版评测接口；中性评测页面尚未接入。</UnavailablePage>
          : !isLegacyRouteAvailable && <UnavailablePage title={identityLoading ? "正在读取身份" : "请连接以读取评测"}>{identityError || "确认当前身份的评测能力后显示可用入口。"}</UnavailablePage>}
        <EvaluationWorkbench apiKey={key} active={mode === "eval" && legacyEvaluation && isLegacyRouteAvailable} available={isLegacyRouteAvailable && legacyEvaluation} sourceRun={sourceRun} onRun={id => { choose("run"); run.select(id); }} beforeSelect={recordEvaluationSelection}/>
      </section>
      <section className="page-view" hidden={mode !== "settings"} aria-label="设置页面">
        <section className="settings-page">
          <div className="settings-section"><h3>当前身份与工作区</h3>{identity ? <><p>{identity.principal_id}</p><ul>{identity.workspaces.map(workspace => <li key={workspace.workspace_id}><span>{workspace.workspace_id}</span><span>{workspace.role === "admin" ? "管理员" : workspace.role === "viewer" ? "只读" : "成员"}</span></li>)}</ul><p>{identity.legacy ? "旧版运行与评测能力可用。" : "当前身份提供工作区会话能力。"}</p></> : <p role="status">{identityLoading ? "正在读取身份与工作区能力。" : identityError || "连接后读取身份与工作区能力。"}</p>}</div>
          <div className="settings-section"><h3>显示</h3><p>当前主题：{theme === "warm" ? "暖纸" : theme === "bright" ? "亮彩" : "深色"}</p><div className="theme-settings" role="group" aria-label="选择显示主题"><Button variant={theme === "warm" ? "secondary" : "outline"} aria-pressed={theme === "warm"} onClick={() => selectTheme("warm")}>暖纸</Button><Button variant={theme === "bright" ? "secondary" : "outline"} aria-pressed={theme === "bright"} onClick={() => selectTheme("bright")}>亮彩</Button><Button variant={theme === "dark" ? "secondary" : "outline"} aria-pressed={theme === "dark"} onClick={() => selectTheme("dark")}>深色</Button></div></div>
        </section>
      </section>
    </WorkbenchLayout>
  </div>;
}
