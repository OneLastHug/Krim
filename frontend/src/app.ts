import {
  AgentDescriptor,
  AgentView,
  InteractionView,
  CompactResponse,
  MessageTreeView,
  MessageView,
  LocaleView,
  RunView,
  StreamFrame,
  TopicView,
  WorkflowEvent,
  WorkflowGraph,
  WorkflowRunView,
  consumeWorkflowEvents,
  consumeEvents,
  errorText,
  requestJson,
  setApiKey,
} from "./api.js";
import { HostEndpoint, type HostJson } from "./host.js";
import { appendLinks, clearLinkCache } from "./links.js";
import { PromptContextUI } from "./prompt-context-ui.js";
import { SkillsUI } from "./skills-ui.js";
import { DragDropUI } from "./drag-drop.js";
import { applyLocale, currentLocale, loadLocale, localDate, t } from "./i18n.js";

type State = {
  topics: TopicView[];
  topicId: string | null;
  tree: MessageTreeView | null;
  messages: MessageView[];
  runId: string | null;
  cursor: number;
  streaming: boolean;
  busy: boolean;
  disconnected: boolean;
  streamText: string;
  streamAbort: AbortController | null;
  editingMessageId: string | null;
  compactPreview: CompactResponse | null;
  workflowGraph: WorkflowGraph;
  workflowRun: WorkflowRunView | null;
  workflowSelected: string[];
  interactions: InteractionView[];
  interactionRuns: Set<string>;
  agents: AgentDescriptor[];
  handles: AgentView[];
};
const state: State = {
  topics: [], topicId: null, tree: null, messages: [], runId: null, cursor: 0,
  streaming: false, busy: false, disconnected: false, streamText: "", streamAbort: null, editingMessageId: null,
  compactPreview: null, agents: [], handles: [],
  workflowGraph: { nodes: [], edges: [], output_node: null }, workflowRun: null, workflowSelected: [],
  interactions: [], interactionRuns: new Set(),
};
const $ = (id: string): HTMLElement => document.getElementById(id)!;
let interactionLoadVersion = 0;
const text = (id: string): HTMLInputElement | HTMLTextAreaElement => $(id) as HTMLInputElement;
const labels: Record<string, string> = { user: "用户", assistant: "助手", tool: "工具", system: "系统" };
const statuses: Record<string, string> = { created: "待执行", queued: "排队中", running: "执行中", completed: "已完成", failed: "失败", aborted: "已停止", interrupted: "已中断", destroyed: "已销毁", paused: "已暂停" };
const idempotency = (): string => crypto.randomUUID?.() || `${Date.now()}-${Math.random()}`;
const hostEndpoint = new HostEndpoint();
const promptContextUI = new PromptContextUI(showNotice);
const skillsUI = new SkillsUI(showNotice);
const dragDropUI = new DragDropUI((topicId, reference) => promptContextUI.acceptDrop(topicId, reference), showNotice);
let localeChangeQueue: Promise<unknown> = Promise.resolve();
let localeLoadVersion = 0;
let lastNotice: string | (() => string) = "";
let lastNoticeError = false;
let lastNoticeLocal = true;
let workflowRuns: WorkflowRunView[] = [];
const localStatus = (status: string): string => t(statuses[status] || status);

function redrawLocale(): void {
  renderTopics(); renderBranchSelect(); renderMessages(); renderAgents();
  renderCompact(state.compactPreview); renderInteractions(); workflowView();
  promptContextUI.refreshLocale();
  skillsUI.refreshLocale();
  dragDropUI.refreshLocale();
  $("topic-heading").textContent = state.tree?.topic.title || t("请选择话题");
  $("composer-mode").textContent = t(state.editingMessageId ? "编辑后会创建新分支，原消息保留" : "Enter 发送 · Shift+Enter 换行");
  $("connection").textContent = t($("connection").classList.contains("ready") ? "本地服务已连接" : "需要连接");
  renderWorkflowSelection(); renderWorkflowRuns();
  showNotice(lastNotice, lastNoticeError, lastNoticeLocal);
}
async function restoreLocale(locale: string): Promise<void> {
  const version = ++localeLoadVersion;
  const bundle = await loadLocale(locale);
  if (version !== localeLoadVersion) return;
  applyLocale(bundle); redrawLocale();
}
function changeLocale(locale: string): Promise<LocaleView> {
  const topicId = state.topicId;
  const run = localeChangeQueue.catch(() => undefined).then(async () => {
    const bundle = await loadLocale(locale);
    const result = topicId ? await requestJson<LocaleView>(`/v1/topics/${encodeURIComponent(topicId)}/locale`, { method: "PATCH", body: JSON.stringify({ locale }) }) : { requested: bundle.requested, effective: bundle.effective, fallback_chain: bundle.fallback_chain };
    if (topicId) state.topics = (await requestJson<{ topics: TopicView[] }>("/v1/topics")).topics;
    const topic = state.topics.find((item) => item.id === topicId); if (topic) topic.locale = result.effective;
    if (state.tree?.topic.id === topicId) state.tree.topic.locale = result.effective;
    if (state.topicId === topicId) { localeLoadVersion += 1; applyLocale(bundle); redrawLocale(); }
    hostEndpoint.notify("host/state", { connected: $("connection").classList.contains("ready"), topic_id: state.topicId, run_id: state.runId, iframe: window.parent !== window, locale: currentLocale() });
    return result;
  });
  localeChangeQueue = run;
  return run;
}

function hostObject(data: HostJson): Record<string, HostJson> {
  if (typeof data === "object" && data !== null && !Array.isArray(data)) return data as Record<string, HostJson>;
  return {};
}

function setupHostBridge(): void {
  hostEndpoint.on("host/get-state", () => ({
    connected: $("connection").classList.contains("ready"),
    topic_id: state.topicId,
    run_id: state.runId,
    locale: document.documentElement.lang || "zh-CN",
    iframe: window.parent !== window,
  }));
  hostEndpoint.on("host/request", (data) => {
    const request = hostObject(data);
    if (typeof request.channel !== "string" || !["checkpoint/create", "checkpoint/rollback", "checkpoint/canRollback", "changeList/invoke", "host/callback"].includes(request.channel)) throw new Error("宿主回程信道无效");
    return hostEndpoint.request(request.channel, request.data ?? null);
  });
  hostEndpoint.on("host/set-key", async (data) => {
    const key = hostObject(data).key;
    if (typeof key !== "string" || key.length > 512) throw new Error("访问密钥无效");
    setApiKey(key);
    await boot();
    return { connected: $("connection").classList.contains("ready") };
  });
  hostEndpoint.on("host/set-locale", (data) => {
    const locale = hostObject(data).locale;
    if (typeof locale !== "string") throw new Error("locale 无效");
    return changeLocale(locale).then((result) => result as unknown as HostJson);
  });
  hostEndpoint.on("host/resize", (data) => {
    const width = hostObject(data).width;
    if (typeof width !== "number" || !Number.isFinite(width) || width < 320 || width > 1600) throw new Error("宽度必须在 320–1600 之间");
    document.documentElement.style.setProperty("--host-width", `${Math.round(width)}px`);
    return { width: Math.round(width) };
  });
  hostEndpoint.on("host/theme", (data) => {
    const theme = hostObject(data).theme;
    if (theme !== "light" && theme !== "dark" && theme !== "system") throw new Error("主题无效");
    document.documentElement.dataset.theme = theme === "system" ? (matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light") : theme;
    return { theme };
  });
  hostEndpoint.on("host/prompt", (data) => {
    const object = hostObject(data);
    const value = object.value;
    const mode = object.mode === "append" ? "append" : "replace";
    if (typeof value !== "string" || value.length > 8192) throw new Error("Prompt 无效");
    const prompt = text("prompt");
    const next = mode === "append" ? `${prompt.value}${value}` : value;
    if (next.length > 8192) throw new Error("Prompt 超过上限");
    promptContextUI.setText(next);
    prompt.focus();
    return { value: prompt.value, mode };
  });
  hostEndpoint.on("host/notice", (data) => {
    const message = hostObject(data).message;
    if (typeof message !== "string" || message.length > 1024) throw new Error("通知无效");
    showNotice(message, false, false);
    return { shown: true };
  });
  for (const channel of [
    "mention/get", "mention/add", "mention/remove", "mention/clear",
    "context/getTags", "context/addTag", "context/removeTag", "context/clearTags",
  ]) {
    hostEndpoint.on(channel, (data) => promptContextUI.bridge(channel, data));
  }
  for (const channel of ["skill/list", "skill/inject", "skill/remove", "skill/read"]) hostEndpoint.on(channel, (data) => skillsUI.bridge(channel, data));
  for (const channel of ["drag/enter", "drag/leave", "drag/drop"]) hostEndpoint.on(channel, (data) => dragDropUI.bridge(channel, data));
  hostEndpoint.announceReady({ protocol_version: 2, channels: [
    "host/get-state", "host/set-key", "host/set-locale", "host/resize", "host/prompt", "host/notice", "host/theme",
    "mention/get", "mention/add", "mention/remove", "mention/clear",
    "context/getTags", "context/addTag", "context/removeTag", "context/clearTags",
    "skill/list", "skill/inject", "skill/remove", "skill/read",
    "drag/enter", "drag/leave", "drag/drop",
  ] });
}

function showNotice(message: string | (() => string) = "", error = false, local = true): void {
  lastNotice = message; lastNoticeError = error; lastNoticeLocal = local;
  const value = typeof message === "function" ? message() : local ? t(message) : message;
  const node = $("notice"); node.textContent = value; node.className = error ? "notice error" : "notice";
}
function disableComposer(value: boolean): void {
  state.busy = value;
  promptContextUI.setBusy(value);
  skillsUI.setBusy(value);
  dragDropUI.setBusy(value);
  ($("send") as HTMLButtonElement).disabled = value;
  text("prompt").disabled = value;
  ($("compact-preview") as HTMLButtonElement).disabled = value || !state.topicId;
  ($("compact-confirm") as HTMLButtonElement).disabled = value || state.compactPreview?.status !== "proposed";
  ($("new-topic") as HTMLButtonElement).disabled = value;
  ($("branch-select") as HTMLSelectElement).disabled = value;
  ($("run-stop") as HTMLButtonElement).disabled = !state.runId || !(state.streaming || state.disconnected);
  ($("run-resume") as HTMLButtonElement).disabled = !state.runId || state.streaming || !state.disconnected;
  for (const button of document.querySelectorAll<HTMLButtonElement>(".topic-button, .message-controls button")) button.disabled = value;
  hostEndpoint.notify("host/state", { connected: $("connection").classList.contains("ready"), topic_id: state.topicId, run_id: state.runId, iframe: window.parent !== window, locale: document.documentElement.lang || "zh-CN" });
}
function renderTopics(): void {
  const root = $("topics"); root.replaceChildren();
  if (!state.topics.length) { const empty = document.createElement("p"); empty.className = "muted"; empty.textContent = t("还没有话题"); root.append(empty); return; }
  for (const topic of state.topics) {
    const button = document.createElement("button"); button.type = "button";
    button.className = `topic-button${topic.id === state.topicId ? " active" : ""}`;
    button.disabled = state.busy;
    const strong = document.createElement("strong"); strong.textContent = topic.title;
    const small = document.createElement("small"); small.textContent = localDate(topic.updated_at);
    button.append(strong, small); button.addEventListener("click", () => void loadTopic(topic.id).catch((error: unknown) => showNotice(errorText(error), true))); root.append(button);
  }
}
function renderBranchSelect(): void {
  const select = $("branch-select") as HTMLSelectElement; select.replaceChildren();
  const current = document.createElement("option"); current.value = ""; current.textContent = t("当前活动分支"); select.append(current);
  for (const message of state.tree?.messages || []) {
    if (message.role === "tool" || (Array.isArray(message.details.tool_calls) && message.details.tool_calls.length > 0)) continue;
    const option = document.createElement("option"); option.value = message.id;
    option.textContent = `${message.sequence} · ${t(labels[message.role] || message.role)} · ${message.content.slice(0, 40)}`;
    select.append(option);
  }
}
function renderMessages(): void {
  const root = $("messages"); root.replaceChildren();
  if (!state.messages.length && !state.streaming) { const empty = document.createElement("div"); empty.className = "empty"; empty.textContent = t("发送第一条消息，开始本地对话。"); root.append(empty); return; }
  for (const message of state.messages) {
    const node = document.createElement("article"); node.className = `message ${message.role}`;
    const role = document.createElement("span"); role.className = "role"; role.textContent = t(labels[message.role] || message.role);
    const content = document.createElement("div"); appendLinks(content, message.content, message.topic_id, message.id, (data) => hostEndpoint.notify("link/navigate", data));
    node.append(role, content);
    if (message.role === "tool" || (Array.isArray(message.details.tool_calls) && message.details.tool_calls.length > 0)) {
      node.classList.add("tool-card");
      const details = document.createElement("details");
      const title = document.createElement("summary"); title.textContent = message.role === "tool" ? t("查看工具结果") : t("查看工具调用");
      const output = document.createElement("pre"); output.textContent = message.role === "tool" ? message.content : JSON.stringify(message.details.tool_calls, null, 2);
      details.append(title, output); node.append(details);
    }
    if (message.role === "user" || message.role === "assistant") {
      const controls = document.createElement("div"); controls.className = "message-controls";
      if (message.role === "user") {
        const edit = document.createElement("button"); edit.type = "button"; edit.textContent = t("编辑"); edit.onclick = () => beginEdit(message);
        edit.disabled = state.busy; controls.append(edit);
      } else {
        const regenerate = document.createElement("button"); regenerate.type = "button"; regenerate.textContent = t("重新生成"); regenerate.onclick = () => regenerateMessage(message.id);
        regenerate.disabled = state.busy; controls.append(regenerate);
      }
      node.append(controls);
    }
    root.append(node);
  }
  if (state.streaming || state.disconnected) {
    const node = document.createElement("article"); node.className = "message assistant streaming";
    const role = document.createElement("span"); role.className = "role"; role.textContent = state.streaming ? t("助手（生成中）") : t("助手（连接中断，可继续读取）");
    const content = document.createElement("div"); content.textContent = state.streamText || "……"; node.append(role, content); root.append(node);
  }
  root.scrollTop = root.scrollHeight;
}
function renderCompact(result: CompactResponse | null): void {
  const panel = $("compact-panel");
  if (!result) { panel.hidden = true; ($("compact-confirm") as HTMLButtonElement).disabled = true; return; }
  panel.hidden = false; $("compact-title").textContent = t("{status} · 覆盖 {count} 条", { status: t(result.status === "proposed" ? "压缩预览" : result.status === "completed" ? "摘要已保存" : "暂无需压缩的历史"), count: result.omitted_count });
  $("compact-sources").textContent = t("来源 {count} 条历史消息；{saved}", { count: result.source_message_ids.length, saved: t(result.persisted ? "已保存" : "尚未保存") });
  $("compact-summary").textContent = result.summary || result.reason || t("没有需要压缩的历史");
  ($("compact-confirm") as HTMLButtonElement).disabled = state.busy || result.status !== "proposed";
}
function renderInteractions(): void {
  const root = $("interactions");
  const answers = new Map([...root.querySelectorAll<HTMLElement>(".interaction-card")].map((card) => [card.dataset.interactionId, card.querySelector("input")?.value || ""]));
  root.replaceChildren();
  for (const interaction of state.interactions.filter((item) => item.status === "pending")) {
    const card = document.createElement("div"); card.className = "interaction-card"; card.dataset.interactionId = interaction.id;
    const title = document.createElement("strong"); title.textContent = interaction.kind === "approval" ? t("工具执行需要批准：{tool}", { tool: interaction.tool_name || "" }) : t("请回答问题");
    const detail = document.createElement("p"); detail.textContent = interaction.kind === "approval" ? JSON.stringify(interaction.payload.arguments, null, 2) : String(interaction.payload.question || t("问题"));
    const actions = document.createElement("div"); actions.className = "actions"; card.append(title, detail);
    if (interaction.kind === "approval") {
      for (const [action, label] of [["approve", "批准此次调用"], ["deny", "拒绝此次调用"]] as const) {
        const button = document.createElement("button"); button.type = "button"; button.className = "secondary"; button.textContent = t(label); button.onclick = () => void replyInteraction(interaction, action); actions.append(button);
      }
    } else {
      const input = document.createElement("input"); input.type = "text"; input.maxLength = 4000; input.placeholder = t("输入真实答案"); input.setAttribute("aria-label", t("用户答案")); input.value = answers.get(interaction.id) || "";
      if (Array.isArray(interaction.payload.options)) {
        const options = document.createElement("div"); options.className = "actions";
        for (const value of interaction.payload.options) {
          if (typeof value !== "string") continue;
          const choice = document.createElement("button"); choice.type = "button"; choice.className = "secondary"; choice.textContent = value; choice.onclick = () => { input.value = value; input.focus(); }; options.append(choice);
        }
        card.append(options);
      }
      const button = document.createElement("button"); button.type = "button"; button.className = "primary"; button.textContent = t("提交答案"); button.onclick = () => void replyInteraction(interaction, "answer", input.value); card.append(input); actions.append(button);
    }
    card.append(actions); root.append(card);
  }
}
async function loadInteractions(): Promise<void> {
  const version = ++interactionLoadVersion;
  const ids = new Set(state.interactionRuns); if (state.runId) ids.add(state.runId);
  for (const agent of state.handles) if (agent.current_run_id) ids.add(agent.current_run_id);
  const values = await Promise.all([...ids].map((id) => requestJson<InteractionView[]>(`/v1/runs/${encodeURIComponent(id)}/interactions?pending_only=true`)));
  if (version !== interactionLoadVersion) return;
  const next = values.flat();
  if (JSON.stringify(next) !== JSON.stringify(state.interactions)) { state.interactions = next; renderInteractions(); }
}
async function replyInteraction(interaction: InteractionView, action: "approve" | "deny" | "answer", answer?: string): Promise<void> {
  if (action === "answer" && !answer?.trim()) { showNotice("请输入真实答案", true); return; }
  const card = [...$("interactions").querySelectorAll<HTMLElement>(".interaction-card")].find((item) => item.dataset.interactionId === interaction.id);
  card?.querySelectorAll<HTMLButtonElement>("button").forEach((button) => { button.disabled = true; });
  try {
    await requestJson<InteractionView>(`/v1/runs/${encodeURIComponent(interaction.run_id)}/interactions/${encodeURIComponent(interaction.id)}/respond`, { method: "POST", body: JSON.stringify({ action, ...(action === "answer" ? { answer } : {}) }) });
    state.interactions = state.interactions.filter((item) => item.id !== interaction.id); renderInteractions(); showNotice(action === "answer" ? "答案已提交，继续生成" : action === "approve" ? "此次工具调用已批准" : "此次工具调用已拒绝");
  } catch (error) { showNotice(errorText(error), true); await loadInteractions(); }
  finally { card?.querySelectorAll<HTMLButtonElement>("button").forEach((button) => { button.disabled = false; }); }
}
function renderAgents(): void {
  const select = $("agent-kind") as HTMLSelectElement; const selected = select.value; select.replaceChildren();
  for (const agent of state.agents) { const option = document.createElement("option"); option.value = agent.name; option.textContent = `${agent.name} · ${agent.risk === "read" ? t("只读任务") : t("受控任务")}`; select.append(option); }
  if (state.agents.some((agent) => agent.name === selected)) select.value = selected;
  $("agent-count").textContent = `${state.handles.length}/${state.agents.length}`;
  const root = $("agents"); root.replaceChildren();
  for (const agent of state.handles) {
    const card = document.createElement("div"); card.className = "agent-card";
    const title = document.createElement("strong"); title.textContent = `${agent.name} · ${localStatus(agent.status)}`;
    const id = document.createElement("small"); id.textContent = agent.agent_id;
    const actions = document.createElement("div"); actions.className = "actions";
    const resume = document.createElement("button"); resume.className = "secondary"; resume.type = "button"; resume.textContent = t("继续任务"); resume.disabled = ["created", "queued", "running"].includes(agent.status); resume.onclick = () => void resumeAgent(agent.agent_id);
    const stop = document.createElement("button"); stop.className = "secondary"; stop.type = "button"; stop.textContent = t("停止"); stop.disabled = !["queued", "running"].includes(agent.status); stop.onclick = () => stopAgent(agent.agent_id);
    const destroy = document.createElement("button"); destroy.className = "secondary"; destroy.type = "button"; destroy.textContent = t("销毁"); destroy.onclick = () => destroyAgent(agent.agent_id);
    actions.append(resume, stop, destroy); card.append(title, id, actions);
    if (agent.result?.text) { const output = document.createElement("pre"); output.textContent = agent.result.text; card.append(output); }
    root.append(card);
  }
}
function graphFromEditor(): WorkflowGraph {
  const value = JSON.parse(text("workflow-json").value) as WorkflowGraph;
  if (!Array.isArray(value.nodes) || !Array.isArray(value.edges)) throw new Error(t("Workflow 图需要 nodes 和 edges 数组"));
  return value;
}
function writeGraph(graph: WorkflowGraph): void { state.workflowGraph = graph; text("workflow-json").value = JSON.stringify(graph, null, 2); renderWorkflowGraph(); }
function renderWorkflowGraph(): void {
  const svg = $("workflow-svg"); svg.replaceChildren();
  let graph: WorkflowGraph;
  try { graph = graphFromEditor(); } catch { $("workflow-selection").textContent = t("图 JSON 无效，修正后才能运行"); return; }
  state.workflowGraph = graph;
  const ns = "http://www.w3.org/2000/svg"; const defs = document.createElementNS(ns, "defs"); const marker = document.createElementNS(ns, "marker"); marker.id = "workflow-arrow"; marker.setAttribute("markerWidth", "8"); marker.setAttribute("markerHeight", "8"); marker.setAttribute("refX", "7"); marker.setAttribute("refY", "3"); marker.setAttribute("orient", "auto"); const arrow = document.createElementNS(ns, "path"); arrow.setAttribute("d", "M0,0 L0,6 L7,3 z"); arrow.setAttribute("fill", "#aebbd6"); marker.append(arrow); defs.append(marker); svg.append(defs);
  const positions = new Map<string, { x: number; y: number }>(); graph.nodes.forEach((node, index) => positions.set(node.id, { x: 28 + (index % 4) * 170, y: 28 + Math.floor(index / 4) * 82 }));
  for (const edge of graph.edges) { const source = positions.get(edge.source); const target = positions.get(edge.target); if (!source || !target) continue; const line = document.createElementNS(ns, "line"); line.classList.add("workflow-edge"); line.setAttribute("x1", String(source.x + 130)); line.setAttribute("y1", String(source.y + 22)); line.setAttribute("x2", String(target.x)); line.setAttribute("y2", String(target.y + 22)); svg.append(line); }
  for (const node of graph.nodes) { const position = positions.get(node.id)!; const group = document.createElementNS(ns, "g"); group.classList.add("workflow-node"); group.setAttribute("data-workflow-node", node.id); if (state.workflowSelected.includes(node.id)) group.classList.add("selected"); group.setAttribute("transform", `translate(${position.x},${position.y})`); const rect = document.createElementNS(ns, "rect"); rect.setAttribute("width", "130"); rect.setAttribute("height", "44"); const title = document.createElementNS(ns, "text"); title.setAttribute("x", "9"); title.setAttribute("y", "19"); title.textContent = node.id; const kind = document.createElementNS(ns, "text"); kind.setAttribute("x", "9"); kind.setAttribute("y", "35"); kind.textContent = node.kind; group.append(rect, title, kind); group.addEventListener("click", () => selectWorkflowNode(node.id)); svg.append(group); }
  renderWorkflowProperties(graph);
  renderWorkflowSelection();
}
function renderWorkflowSelection(): void {
  try { graphFromEditor(); } catch { $("workflow-selection").textContent = t("图 JSON 无效，修正后才能运行"); return; }
  $("workflow-selection").textContent = state.workflowSelected.length ? t("已选：{nodes}；再次点击可取消", { nodes: state.workflowSelected.join(" → ") }) : t("点击图中的节点编辑配置");
}
function renderWorkflowProperties(graph: WorkflowGraph): void {
  const selected = state.workflowSelected[0]; const node = graph.nodes.find((item) => item.id === selected);
  text("workflow-node-id").value = node?.id || ""; ($( "workflow-node-kind") as HTMLSelectElement).value = node?.kind || "constant";
  text("workflow-node-config").value = JSON.stringify(node?.config || {}, null, 2); text("workflow-input-ports").value = node?.input_ports ? JSON.stringify(node.input_ports, null, 2) : ""; text("workflow-output-ports").value = node?.output_ports ? JSON.stringify(node.output_ports, null, 2) : ""; (text("workflow-developer-only") as HTMLInputElement).checked = Boolean(node?.developer_only);
  const edges = $("workflow-edge-list") as HTMLSelectElement; edges.replaceChildren(); graph.edges.forEach((edge, index) => { const option = document.createElement("option"); option.value = String(index); option.textContent = `${edge.source}:${edge.source_port || "out"} → ${edge.target}:${edge.target_port || "in"}${edge.when ? ` [${edge.when}]` : ""}`; edges.append(option); });
}
function selectWorkflowNode(nodeId: string): void { if (state.workflowSelected.includes(nodeId)) state.workflowSelected = state.workflowSelected.filter((id) => id !== nodeId); else { if (state.workflowSelected.length >= 2) state.workflowSelected = []; state.workflowSelected.push(nodeId); } renderWorkflowGraph(); workflowView(); }
function addWorkflowNode(kind: "constant" | "output"): void { try { const graph = graphFromEditor(); const base = kind === "constant" ? "constant" : "output"; let index = 1; while (graph.nodes.some((node) => node.id === `${base}_${index}`)) index += 1; graph.nodes.push({ id: `${base}_${index}`, kind, config: kind === "constant" ? { value: 1 } : {} }); writeGraph(graph); } catch (error) { showNotice(errorText(error), true); } }
function connectWorkflowNodes(): void { try { const graph = graphFromEditor(); if (state.workflowSelected.length !== 2) throw new Error(t("请先选择两个节点作为来源和目标")); const source = state.workflowSelected[0]!; const target = state.workflowSelected[1]!; const source_port = text("workflow-edge-source-port").value.trim() || "out"; const target_port = text("workflow-edge-target-port").value.trim() || "in"; const when = text("workflow-edge-when").value.trim() || null; if (!graph.edges.some((edge) => edge.source === source && edge.target === target && (edge.source_port || "out") === source_port && (edge.target_port || "in") === target_port && (edge.when || null) === when)) graph.edges.push({ source, target, source_port, target_port, when }); writeGraph(graph); } catch (error) { showNotice(errorText(error), true); } }
function saveWorkflowNode(): void { try { const graph = graphFromEditor(); const oldId = state.workflowSelected[0]; const id = text("workflow-node-id").value.trim(); if (!oldId || !id) throw new Error(t("请先选择节点并填写 ID")); if (graph.nodes.some((node) => node.id === id && node.id !== oldId)) throw new Error(t("节点 ID 重复")); const node = graph.nodes.find((item) => item.id === oldId)!; node.id = id; node.kind = ($("workflow-node-kind") as HTMLSelectElement).value; node.config = JSON.parse(text("workflow-node-config").value || "{}"); node.developer_only = (text("workflow-developer-only") as HTMLInputElement).checked; node.input_ports = text("workflow-input-ports").value.trim() ? JSON.parse(text("workflow-input-ports").value) : null; node.output_ports = text("workflow-output-ports").value.trim() ? JSON.parse(text("workflow-output-ports").value) : null; graph.edges.forEach((edge) => { if (edge.source === oldId) edge.source = id; if (edge.target === oldId) edge.target = id; }); state.workflowSelected = [id]; writeGraph(graph); } catch (error) { showNotice(errorText(error), true); } }
function deleteWorkflowNode(): void { try { const graph = graphFromEditor(); const id = state.workflowSelected[0]; if (!id) throw new Error(t("请先选择节点")); graph.nodes = graph.nodes.filter((node) => node.id !== id); graph.edges = graph.edges.filter((edge) => edge.source !== id && edge.target !== id); state.workflowSelected = []; writeGraph(graph); } catch (error) { showNotice(errorText(error), true); } }
function deleteWorkflowEdge(): void { try { const graph = graphFromEditor(); const index = Number(($("workflow-edge-list") as HTMLSelectElement).value); if (!Number.isInteger(index) || !graph.edges[index]) throw new Error(t("请先选择边")); graph.edges.splice(index, 1); writeGraph(graph); } catch (error) { showNotice(errorText(error), true); } }
function workflowView(): void { const run = state.workflowRun; const status = $("workflow-status"); status.textContent = run ? t("运行 {status}", { status: localStatus(run.status) }) + (run.current_node ? t(" · 当前节点 {node}", { node: run.current_node }) : "") + (run.output !== null && run.output !== undefined ? t(" · 输出 {output}", { output: JSON.stringify(run.output) }) : "") : t("尚未运行"); for (const [id, value] of Object.entries(run?.node_statuses || {})) { const node = document.querySelector(`[data-workflow-node="${CSS.escape(id)}"]`); if (node) node.setAttribute("data-status", value); } $("workflow-pause").toggleAttribute("disabled", !run || !["queued", "running"].includes(run.status)); $("workflow-resume").toggleAttribute("disabled", !run || run.status !== "paused"); $("workflow-stop").toggleAttribute("disabled", !run || !["queued", "running", "paused"].includes(run.status)); $("workflow-rerun").toggleAttribute("disabled", !run || !state.workflowSelected.length); $("workflow-restore").toggleAttribute("disabled", !run || !["paused", "queued", "running"].includes(run.status)); }
function renderWorkflowRuns(): void { const select = $("workflow-run-list") as HTMLSelectElement; const selected = select.value; select.replaceChildren(); for (const run of workflowRuns) { const option = document.createElement("option"); option.value = run.run_id; option.textContent = t("{id} · {status}", { id: run.run_id.slice(0, 8), status: localStatus(run.status) }); select.append(option); } if (workflowRuns.some((run) => run.run_id === selected)) select.value = selected; }
async function loadWorkflowRuns(): Promise<void> { workflowRuns = await requestJson<WorkflowRunView[]>("/v1/workflows/runs"); renderWorkflowRuns(); }
function recordWorkflowEvent(event: WorkflowEvent): void {
  const run = state.workflowRun;
  if (!run || run.run_id !== event.run_id) return;
  const log = $("workflow-events");
  log.textContent += `${event.seq} ${event.type}${event.node_id ? ` ${event.node_id}` : ""}\n`;
  if (log.textContent.length > 100000) log.textContent = log.textContent.slice(-80000);
  run.event_count = event.seq + 1; run.epoch = event.epoch;
  if (event.node_id && event.type === "node_start") { run.node_statuses[event.node_id] = "running"; run.current_node = event.node_id; }
  if (event.node_id && event.type === "node_end") { run.node_statuses[event.node_id] = "completed"; if (event.data && typeof event.data === "object" && !Array.isArray(event.data)) run.node_outputs[event.node_id] = event.data as Record<string, unknown>; }
  if (event.node_id && event.type === "node_skip") run.node_statuses[event.node_id] = "skipped";
  if (event.type === "paused") { run.status = "paused"; run.current_node = event.node_id; }
  if (event.type === "start" || event.type === "resumed") run.status = "running";
  if (event.type === "end" && event.data && typeof event.data === "object") { const data = event.data as { status?: WorkflowRunView["status"] }; if (data.status) run.status = data.status; }
  workflowView();
}
async function runWorkflow(): Promise<void> { try { const graph = graphFromEditor(); const input = JSON.parse(text("workflow-input").value || "null"); const pauseBefore = text("workflow-pause-before").value.split(",").map((value) => value.trim()).filter(Boolean); const pauseAfter = text("workflow-pause-after").value.split(",").map((value) => value.trim()).filter(Boolean); const run = await requestJson<WorkflowRunView>("/v1/workflows/runs", { method: "POST", body: JSON.stringify({ idempotency_key: idempotency(), graph, input_value: input, production: (text("workflow-production") as HTMLInputElement).checked, pause_before: pauseBefore, pause_after: pauseAfter }) }); state.workflowRun = run; workflowView(); await loadWorkflowRuns(); ($( "workflow-run-list") as HTMLSelectElement).value = run.run_id; $("workflow-events").textContent = ""; await consumeWorkflowEvents(run.run_id, 0, (event: WorkflowEvent) => { recordWorkflowEvent(event); }); state.workflowRun = await requestJson<WorkflowRunView>(`/v1/workflows/runs/${run.run_id}`); workflowView(); await loadWorkflowRuns(); ($( "workflow-run-list") as HTMLSelectElement).value = run.run_id; } catch (error) { showNotice(errorText(error), true); } }
async function workflowControl(action: "pause" | "resume" | "stop"): Promise<void> { if (!state.workflowRun) return; try { const endpoint = `/v1/workflows/runs/${state.workflowRun.run_id}/${action}`; const body = action === "resume" ? { has_input_override: (text("workflow-has-override") as HTMLInputElement).checked, input_override: JSON.parse(text("workflow-override").value || "null") } : undefined; state.workflowRun = await requestJson<WorkflowRunView>(endpoint, { method: "POST", body: body ? JSON.stringify(body) : undefined }); workflowView(); await loadWorkflowRuns(); } catch (error) { showNotice(errorText(error), true); } }
async function rerunWorkflow(): Promise<void> {
  if (!state.workflowRun || !state.workflowSelected[0]) return;
  const runId = state.workflowRun.run_id;
  const timer = setInterval(() => void requestJson<WorkflowRunView>(`/v1/workflows/runs/${runId}`).then((run) => { if (state.workflowRun?.run_id === runId) { state.workflowRun = run; workflowView(); } }).catch(() => undefined), 250);
  try {
    state.workflowRun = await requestJson<WorkflowRunView>(`/v1/workflows/runs/${runId}/rerun`, { method: "POST", body: JSON.stringify({ node_id: state.workflowSelected[0], has_input_override: (text("workflow-has-override") as HTMLInputElement).checked, input_override: JSON.parse(text("workflow-override").value || "null"), allow_side_effects: (text("workflow-allow-effects") as HTMLInputElement).checked }) });
    workflowView(); await loadWorkflowRuns();
  } catch (error) { showNotice(errorText(error), true); }
  finally { clearInterval(timer); }
}
async function restoreWorkflow(): Promise<void> { if (!state.workflowRun) return; try { state.workflowRun = await requestJson<WorkflowRunView>(`/v1/workflows/runs/${state.workflowRun.run_id}/restore`, { method: "POST", body: JSON.stringify({ replay_side_effects: (text("workflow-allow-effects") as HTMLInputElement).checked }) }); workflowView(); await consumeWorkflowEvents(state.workflowRun.run_id, state.workflowRun.event_count, () => undefined); } catch (error) { showNotice(errorText(error), true); } }
async function loadWorkflowRun(): Promise<void> { const id = ($("workflow-run-list") as HTMLSelectElement).value; if (!id) return; try { state.workflowRun = await requestJson<WorkflowRunView>(`/v1/workflows/runs/${id}`); const graph = await requestJson<WorkflowGraph>(`/v1/workflows/runs/${id}/graph`); writeGraph(graph); workflowView(); } catch (error) { showNotice(errorText(error), true); } }
async function loadTopic(topicId: string): Promise<void> {
  if (state.streaming) return;
  const switched = state.topicId !== topicId;
  if (switched) { state.runId = null; state.cursor = 0; state.disconnected = false; state.streamText = ""; state.interactionRuns.clear(); state.interactions = []; interactionLoadVersion += 1; renderInteractions(); }
  state.topicId = topicId;
  dragDropUI.selectTopic(topicId);
  const topic = state.topics.find((item) => item.id === topicId);
  $("topic-heading").textContent = topic?.title || t("话题");
  const [messages, tree] = await Promise.all([
    requestJson<{ messages: MessageView[] }>(`/v1/topics/${encodeURIComponent(topicId)}/messages`),
    requestJson<MessageTreeView>(`/v1/topics/${encodeURIComponent(topicId)}/tree`),
  ]);
  if (state.topicId !== topicId) return;
  const url = new URL(location.href); url.searchParams.set("topic", topicId); history.replaceState(null, "", url);
  state.messages = messages.messages; state.tree = tree; state.compactPreview = null; renderTopics(); renderBranchSelect(); renderMessages(); renderCompact(null);
  state.topics = state.topics.map((item) => item.id === topicId ? tree.topic : item);
  if (switched && state.messages.at(-1)) {
    const latest = state.messages.at(-1)!;
    const run = await requestJson<RunView>(`/v1/runs/${encodeURIComponent(latest.run_id)}`);
    if (state.topicId === topicId && ["queued", "running"].includes(run.status)) {
      state.runId = run.id; state.cursor = 0; state.disconnected = true;
      showNotice("此话题仍在生成，可以继续读取或停止");
    }
  }
  disableComposer(state.busy);
  await loadInteractions();
  await promptContextUI.selectTopic(topicId);
  await skillsUI.selectTopic(topicId);
  if (state.topicId === topicId) await restoreLocale(tree.topic.locale);
}
async function loadTopics(): Promise<void> {
  state.topics = (await requestJson<{ topics: TopicView[] }>("/v1/topics")).topics; renderTopics();
  if (!state.topicId && state.topics[0]) await loadTopic(state.topics[0].id);
}
async function loadAgents(): Promise<void> {
  state.agents = await requestJson<AgentDescriptor[]>("/v1/agents/catalog");
  state.handles = await requestJson<AgentView[]>("/v1/agents"); renderAgents(); await loadInteractions();
}
async function createTopic(): Promise<void> {
  if (state.busy) return;
  disableComposer(true);
  try {
  const title = text("topic-title").value.trim() || t("新话题");
  const topic = await requestJson<TopicView>("/v1/topics", { method: "POST", body: JSON.stringify({ title, locale: currentLocale() }) });
  text("topic-title").value = ""; state.topics.unshift(topic); await loadTopic(topic.id);
  } catch (error) { showNotice(errorText(error), true); }
  finally { disableComposer(false); }
}
async function consume(runId: string, fromSeq = 0): Promise<void> {
  state.runId = runId; state.cursor = fromSeq; state.streaming = true; state.disconnected = false;
  if (fromSeq === 0) state.streamText = "";
  disableComposer(true); renderTopics(); renderMessages();
  const controller = new AbortController(); state.streamAbort = controller;
  const interactionTimer = setInterval(() => void loadInteractions().catch(() => undefined), 500);
  try {
    state.cursor = await consumeEvents(runId, fromSeq, (frame: StreamFrame) => {
      state.cursor = frame.seq + 1;
      if (frame.kind === "chunk" && frame.data?.type === "text_delta") { state.streamText += String(frame.data.text || ""); renderMessages(); }
      if (frame.kind === "chunk" && frame.data?.type === "context_compact") showNotice("上下文已压缩并继续生成");
      if (frame.kind === "chunk" && frame.data?.type === "skill_loaded" && typeof frame.data.name === "string") skillsUI.loadedSkill(frame.data.name, typeof frame.data.resource === "string" ? frame.data.resource : null);
      if (frame.kind === "chunk" && frame.data?.type === "interaction_request") {
        const interaction = frame.data.interaction as InteractionView | undefined;
        if (interaction?.run_id) state.interactionRuns.add(interaction.run_id);
        void loadInteractions().catch(() => undefined);
      }
      if (frame.kind === "end") showNotice(() => t("生成{status}", { status: localStatus(String(frame.data?.status || "结束")) }));
    }, controller.signal);
  } catch (error) { state.disconnected = true; throw error; }
  finally { clearInterval(interactionTimer); interactionLoadVersion += 1; state.streaming = false; state.streamAbort = null; state.interactions = []; state.interactionRuns.clear(); renderInteractions(); disableComposer(false); renderTopics(); renderMessages(); }
  if (state.topicId) await loadTopic(state.topicId);
}
async function sendMessage(event: SubmitEvent): Promise<void> {
  event.preventDefault(); if (!state.topicId || state.busy) return;
  const prompt = text("prompt").value; if (!prompt.trim()) return;
  disableComposer(true); showNotice("生成中……");
  try {
    const path = state.editingMessageId ? `/v1/topics/${encodeURIComponent(state.topicId)}/messages/${encodeURIComponent(state.editingMessageId)}/edit` : `/v1/topics/${encodeURIComponent(state.topicId)}/messages`;
    const body = { text: prompt, idempotency_key: idempotency(), mentions: promptContextUI.occurrences, skill_names: skillsUI.selectedNames };
    const run = await requestJson<RunView>(path, { method: "POST", body: JSON.stringify(body) });
    state.editingMessageId = null; promptContextUI.setText("", true); $("edit-cancel").hidden = true; $("composer-mode").textContent = t("Enter 发送 · Shift+Enter 换行");
    skillsUI.clearSelection();
    await loadTopic(run.topic_id); await consume(run.id);
  } catch (error) { showNotice(errorText(error), true); state.streaming = false; disableComposer(false); }
}
async function selectBranch(messageId: string): Promise<void> {
  if (!state.topicId || !messageId) return;
  try { await requestJson(`/v1/topics/${encodeURIComponent(state.topicId)}/branch`, { method: "PUT", body: JSON.stringify({ message_id: messageId }) }); await loadTopic(state.topicId); showNotice("活动分支已切换"); } catch (error) { showNotice(errorText(error), true); }
}
function beginEdit(message: MessageView): void { if (state.busy) return; state.editingMessageId = message.id; promptContextUI.setText(message.content, true); $("edit-cancel").hidden = false; $("composer-mode").textContent = t("编辑后会创建新分支，原消息保留"); text("prompt").focus(); }
async function regenerateMessage(messageId: string): Promise<void> {
  if (!state.topicId || state.busy) return;
  disableComposer(true);
  try { const run = await requestJson<RunView>(`/v1/topics/${encodeURIComponent(state.topicId)}/messages/${encodeURIComponent(messageId)}/regenerate`, { method: "POST", body: JSON.stringify({ idempotency_key: idempotency() }) }); await consume(run.id); } catch (error) { showNotice(errorText(error), true); }
  finally { disableComposer(false); }
}
async function stopRun(): Promise<void> {
  if (!state.runId) return;
  try {
    const runId = state.runId;
    await requestJson(`/v1/runs/${encodeURIComponent(runId)}/stop`, { method: "POST" });
    for (let attempt = 0; attempt < 20; attempt += 1) {
      const run = await requestJson<RunView>(`/v1/runs/${encodeURIComponent(runId)}`);
      if (["completed", "failed", "aborted", "interrupted"].includes(run.status)) {
        showNotice(() => run.status === "aborted" ? t("已停止：终态已保存") : t("运行{status}", { status: localStatus(run.status) }));
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    showNotice("已请求停止，等待持久化终态");
  } catch (error) { showNotice(errorText(error), true); }
}
async function resumeRun(): Promise<void> { if (!state.runId) return; try { await consume(state.runId, state.cursor); } catch (error) { showNotice(errorText(error), true); } }
async function compact(confirm: boolean): Promise<void> {
  if (!state.topicId || state.busy || (confirm && state.compactPreview?.status !== "proposed")) return;
  try {
    const body: Record<string, unknown> = { max_messages: Number(text("compact-budget").value) || 20, confirm };
    if (confirm && state.compactPreview) body.expected_branch_key = state.compactPreview.branch_key;
    const result = await requestJson<CompactResponse>(`/v1/topics/${encodeURIComponent(state.topicId)}/compact`, { method: "POST", body: JSON.stringify(body) });
    state.compactPreview = result; renderCompact(result); showNotice(result.status === "completed" ? "历史摘要已保存，原始消息仍然保留" : result.status === "proposed" ? "请检查摘要内容，再确认保存" : "没有需要压缩的历史");
  } catch (error) { showNotice(errorText(error), true); }
}
async function createAgent(): Promise<void> {
  const name = (($("agent-kind") as HTMLSelectElement).value || "default"); const prompt = text("agent-prompt").value.trim();
  if (!prompt) { showNotice("请先输入交给 Agent 的任务"); return; }
  let timer: ReturnType<typeof setInterval> | undefined;
  try { const agent = await requestJson<AgentView>("/v1/agents", { method: "POST", body: JSON.stringify({ name }) }); state.handles = [...state.handles, agent]; renderAgents(); timer = setInterval(() => void loadAgents().catch(() => undefined), 500); const result = await requestJson<AgentView>(`/v1/agents/${agent.agent_id}/execute`, { method: "POST", body: JSON.stringify({ prompt, idempotency_key: idempotency() }) }); state.handles = [...state.handles.filter((item) => item.agent_id !== agent.agent_id), result]; text("agent-prompt").value = ""; renderAgents(); showNotice(() => t("Agent {status}", { status: localStatus(result.status) })); } catch (error) { showNotice(errorText(error), true); }
  finally { if (timer) clearInterval(timer); }
}
async function resumeAgent(agentId: string): Promise<void> {
  const prompt = text("agent-prompt").value.trim(); if (!prompt) { showNotice("请先输入要继续的 Agent 任务"); return; }
  const timer = setInterval(() => void loadAgents().catch(() => undefined), 500);
  try { const agent = await requestJson<AgentView>(`/v1/agents/${agentId}/resume`, { method: "POST", body: JSON.stringify({ prompt, idempotency_key: idempotency() }) }); state.handles = state.handles.map((item) => item.agent_id === agentId ? agent : item); renderAgents(); showNotice(() => t("Agent {status}", { status: localStatus(agent.status) })); }
  catch (error) { showNotice(errorText(error), true); }
  finally { clearInterval(timer); }
}
async function stopAgent(agentId: string): Promise<void> { try { const agent = await requestJson<AgentView>(`/v1/agents/${agentId}/stop`, { method: "POST" }); state.handles = state.handles.map((item) => item.agent_id === agentId ? agent : item); renderAgents(); } catch (error) { showNotice(errorText(error), true); } }
async function destroyAgent(agentId: string): Promise<void> { try { await requestJson(`/v1/agents/${agentId}`, { method: "DELETE" }); state.handles = state.handles.filter((item) => item.agent_id !== agentId); renderAgents(); } catch (error) { showNotice(errorText(error), true); } }
async function refresh(): Promise<void> { if (state.topicId) await loadTopic(state.topicId); await Promise.all([loadTopics(), loadAgents()]); }
function setup(): void {
  $("locale-select").addEventListener("change", (event) => { void changeLocale((event.target as HTMLSelectElement).value).catch((error: unknown) => { ($("locale-select") as HTMLSelectElement).value = currentLocale(); showNotice(errorText(error), true); }); });
  $("new-topic").addEventListener("click", () => void createTopic()); $("refresh").addEventListener("click", () => void refresh()); $("composer").addEventListener("submit", (event) => void sendMessage(event as SubmitEvent));
  $("compact-preview").addEventListener("click", () => void compact(false)); $("compact-confirm").addEventListener("click", () => void compact(true)); $("run-stop").addEventListener("click", () => void stopRun()); $("run-resume").addEventListener("click", () => void resumeRun()); $("agent-create").addEventListener("click", () => void createAgent()); $("branch-select").addEventListener("change", (event) => void selectBranch((event.target as HTMLSelectElement).value));
  $("edit-cancel").addEventListener("click", () => { state.editingMessageId = null; promptContextUI.setText("", true); $("edit-cancel").hidden = true; $("composer-mode").textContent = t("Enter 发送 · Shift+Enter 换行"); }); $("key-toggle").addEventListener("click", () => { $("key-form").hidden = !$("key-form").hidden; }); $("key-form").addEventListener("submit", (event) => { event.preventDefault(); setApiKey(text("api-key").value); void boot(); });
  text("compact-budget").addEventListener("input", () => { state.compactPreview = null; renderCompact(null); });
  text("workflow-json").addEventListener("input", () => renderWorkflowGraph()); $("workflow-add-constant").addEventListener("click", () => addWorkflowNode("constant")); $("workflow-add-output").addEventListener("click", () => addWorkflowNode("output")); $("workflow-connect").addEventListener("click", () => connectWorkflowNodes()); $("workflow-run").addEventListener("click", () => void runWorkflow()); $("workflow-pause").addEventListener("click", () => void workflowControl("pause")); $("workflow-resume").addEventListener("click", () => void workflowControl("resume")); $("workflow-stop").addEventListener("click", () => void workflowControl("stop")); $("workflow-save-node").addEventListener("click", () => saveWorkflowNode()); $("workflow-delete-node").addEventListener("click", () => deleteWorkflowNode()); $("workflow-delete-edge").addEventListener("click", () => deleteWorkflowEdge()); $("workflow-rerun").addEventListener("click", () => void rerunWorkflow()); $("workflow-restore").addEventListener("click", () => void restoreWorkflow()); $("workflow-load-run").addEventListener("click", () => void loadWorkflowRun()); renderWorkflowGraph(); workflowView();
  text("prompt").addEventListener("keydown", (event) => { const keyEvent = event as KeyboardEvent; if (keyEvent.key === "Enter" && !keyEvent.shiftKey && !keyEvent.isComposing) { keyEvent.preventDefault(); ($("composer") as HTMLFormElement).requestSubmit(); } });
}
async function boot(): Promise<void> { clearLinkCache(); try { const health = await requestJson<{ provider: string }>("/healthz"); $("model-label").textContent = health.provider; state.topics = (await requestJson<{ topics: TopicView[] }>("/v1/topics")).topics; state.agents = await requestJson<AgentDescriptor[]>("/v1/agents/catalog"); state.handles = await requestJson<AgentView[]>("/v1/agents"); await loadWorkflowRuns(); await restoreLocale(currentLocale()); renderTopics(); renderAgents(); if (!state.topicId && state.topics[0]) { const selected = new URLSearchParams(location.search).get("topic"); await loadTopic(state.topics.find((topic) => topic.id === selected)?.id || state.topics[0].id); } $("connection").textContent = t("本地服务已连接"); $("connection").classList.add("ready"); $("key-form").hidden = true; text("api-key").value = ""; showNotice(); } catch (error) { $("connection").textContent = t("需要连接"); $("connection").classList.remove("ready"); showNotice(errorText(error), true); $("key-form").hidden = false; } }
setupHostBridge();
setup();
void boot();
