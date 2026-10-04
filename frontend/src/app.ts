import {
  AgentDescriptor,
  AgentView,
  CompactResponse,
  MessageTreeView,
  MessageView,
  RunView,
  StreamFrame,
  TopicView,
  consumeEvents,
  errorText,
  requestJson,
  setApiKey,
} from "./api.js";

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
  agents: AgentDescriptor[];
  handles: AgentView[];
};
const state: State = {
  topics: [], topicId: null, tree: null, messages: [], runId: null, cursor: 0,
  streaming: false, busy: false, disconnected: false, streamText: "", streamAbort: null, editingMessageId: null,
  compactPreview: null, agents: [], handles: [],
};
const $ = (id: string): HTMLElement => document.getElementById(id)!;
const text = (id: string): HTMLInputElement | HTMLTextAreaElement => $(id) as HTMLInputElement;
const labels: Record<string, string> = { user: "用户", assistant: "助手", tool: "工具", system: "系统" };
const statuses: Record<string, string> = { created: "待执行", queued: "排队中", running: "执行中", completed: "已完成", failed: "失败", aborted: "已停止", interrupted: "已中断", destroyed: "已销毁" };
const idempotency = (): string => crypto.randomUUID?.() || `${Date.now()}-${Math.random()}`;

function showNotice(message = "", error = false): void {
  const node = $("notice"); node.textContent = message; node.className = error ? "notice error" : "notice";
}
function disableComposer(value: boolean): void {
  state.busy = value;
  ($("send") as HTMLButtonElement).disabled = value;
  text("prompt").disabled = value;
  ($("compact-preview") as HTMLButtonElement).disabled = value || !state.topicId;
  ($("compact-confirm") as HTMLButtonElement).disabled = value || state.compactPreview?.status !== "proposed";
  ($("new-topic") as HTMLButtonElement).disabled = value;
  ($("branch-select") as HTMLSelectElement).disabled = value;
  ($("run-stop") as HTMLButtonElement).disabled = !state.runId || !(state.streaming || state.disconnected);
  ($("run-resume") as HTMLButtonElement).disabled = !state.runId || state.streaming || !state.disconnected;
  for (const button of document.querySelectorAll<HTMLButtonElement>(".topic-button, .message-controls button")) button.disabled = value;
}
function renderTopics(): void {
  const root = $("topics"); root.replaceChildren();
  if (!state.topics.length) { root.innerHTML = '<p class="muted">还没有话题</p>'; return; }
  for (const topic of state.topics) {
    const button = document.createElement("button"); button.type = "button";
    button.className = `topic-button${topic.id === state.topicId ? " active" : ""}`;
    button.disabled = state.busy;
    const strong = document.createElement("strong"); strong.textContent = topic.title;
    const small = document.createElement("small"); small.textContent = new Date(topic.updated_at).toLocaleString("zh-CN");
    button.append(strong, small); button.addEventListener("click", () => void loadTopic(topic.id).catch((error: unknown) => showNotice(errorText(error), true))); root.append(button);
  }
}
function renderBranchSelect(): void {
  const select = $("branch-select") as HTMLSelectElement; select.replaceChildren();
  const current = document.createElement("option"); current.value = ""; current.textContent = "当前活动分支"; select.append(current);
  for (const message of state.tree?.messages || []) {
    if (message.role === "tool" || (Array.isArray(message.details.tool_calls) && message.details.tool_calls.length > 0)) continue;
    const option = document.createElement("option"); option.value = message.id;
    option.textContent = `${message.sequence} · ${labels[message.role] || message.role} · ${message.content.slice(0, 40)}`;
    select.append(option);
  }
}
function renderMessages(): void {
  const root = $("messages"); root.replaceChildren();
  if (!state.messages.length && !state.streaming) { root.innerHTML = '<div class="empty">发送第一条消息，开始本地对话。</div>'; return; }
  for (const message of state.messages) {
    const node = document.createElement("article"); node.className = `message ${message.role}`;
    const role = document.createElement("span"); role.className = "role"; role.textContent = labels[message.role] || message.role;
    const content = document.createElement("div"); content.textContent = message.content;
    node.append(role, content);
    if (message.role === "tool" || (Array.isArray(message.details.tool_calls) && message.details.tool_calls.length > 0)) {
      node.classList.add("tool-card");
      const details = document.createElement("details");
      const title = document.createElement("summary"); title.textContent = message.role === "tool" ? "查看工具结果" : "查看工具调用";
      const output = document.createElement("pre"); output.textContent = message.role === "tool" ? message.content : JSON.stringify(message.details.tool_calls, null, 2);
      details.append(title, output); node.append(details);
    }
    if (message.role === "user" || message.role === "assistant") {
      const controls = document.createElement("div"); controls.className = "message-controls";
      if (message.role === "user") {
        const edit = document.createElement("button"); edit.type = "button"; edit.textContent = "编辑"; edit.onclick = () => beginEdit(message);
        edit.disabled = state.busy; controls.append(edit);
      } else {
        const regenerate = document.createElement("button"); regenerate.type = "button"; regenerate.textContent = "重新生成"; regenerate.onclick = () => regenerateMessage(message.id);
        regenerate.disabled = state.busy; controls.append(regenerate);
      }
      node.append(controls);
    }
    root.append(node);
  }
  if (state.streaming || state.disconnected) {
    const node = document.createElement("article"); node.className = "message assistant streaming";
    const role = document.createElement("span"); role.className = "role"; role.textContent = state.streaming ? "助手（生成中）" : "助手（连接中断，可继续读取）";
    const content = document.createElement("div"); content.textContent = state.streamText || "……"; node.append(role, content); root.append(node);
  }
  root.scrollTop = root.scrollHeight;
}
function renderCompact(result: CompactResponse | null): void {
  const panel = $("compact-panel");
  if (!result) { panel.hidden = true; ($("compact-confirm") as HTMLButtonElement).disabled = true; return; }
  panel.hidden = false; $("compact-title").textContent = `${result.status === "proposed" ? "压缩预览" : result.status === "completed" ? "摘要已保存" : "暂无需压缩的历史"} · 覆盖 ${result.omitted_count} 条`;
  $("compact-sources").textContent = `来源 ${result.source_message_ids.length} 条历史消息；${result.persisted ? "已保存" : "尚未保存"}`;
  $("compact-summary").textContent = result.summary || result.reason || "没有需要压缩的历史";
  ($("compact-confirm") as HTMLButtonElement).disabled = state.busy || result.status !== "proposed";
}
function renderAgents(): void {
  const select = $("agent-kind") as HTMLSelectElement; select.replaceChildren();
  for (const agent of state.agents) { const option = document.createElement("option"); option.value = agent.name; option.textContent = `${agent.name} · ${agent.risk === "read" ? "只读任务" : "受控任务"}`; select.append(option); }
  $("agent-count").textContent = `${state.handles.length}/${state.agents.length}`;
  const root = $("agents"); root.replaceChildren();
  for (const agent of state.handles) {
    const card = document.createElement("div"); card.className = "agent-card";
    const title = document.createElement("strong"); title.textContent = `${agent.name} · ${statuses[agent.status] || agent.status}`;
    const id = document.createElement("small"); id.textContent = agent.agent_id;
    const actions = document.createElement("div"); actions.className = "actions";
    const resume = document.createElement("button"); resume.className = "secondary"; resume.type = "button"; resume.textContent = "继续任务"; resume.disabled = ["created", "queued", "running"].includes(agent.status); resume.onclick = () => void resumeAgent(agent.agent_id);
    const stop = document.createElement("button"); stop.className = "secondary"; stop.type = "button"; stop.textContent = "停止"; stop.disabled = !["queued", "running"].includes(agent.status); stop.onclick = () => stopAgent(agent.agent_id);
    const destroy = document.createElement("button"); destroy.className = "secondary"; destroy.type = "button"; destroy.textContent = "销毁"; destroy.onclick = () => destroyAgent(agent.agent_id);
    actions.append(resume, stop, destroy); card.append(title, id, actions);
    if (agent.result?.text) { const output = document.createElement("pre"); output.textContent = agent.result.text; card.append(output); }
    root.append(card);
  }
}
async function loadTopic(topicId: string): Promise<void> {
  if (state.streaming) return;
  const switched = state.topicId !== topicId;
  if (switched) { state.runId = null; state.cursor = 0; state.disconnected = false; state.streamText = ""; }
  state.topicId = topicId;
  const topic = state.topics.find((item) => item.id === topicId);
  $("topic-heading").textContent = topic?.title || "话题";
  const [messages, tree] = await Promise.all([
    requestJson<{ messages: MessageView[] }>(`/v1/topics/${encodeURIComponent(topicId)}/messages`),
    requestJson<MessageTreeView>(`/v1/topics/${encodeURIComponent(topicId)}/tree`),
  ]);
  if (state.topicId !== topicId) return;
  state.messages = messages.messages; state.tree = tree; state.compactPreview = null; renderTopics(); renderBranchSelect(); renderMessages(); renderCompact(null);
  if (switched && state.messages.at(-1)) {
    const latest = state.messages.at(-1)!;
    const run = await requestJson<RunView>(`/v1/runs/${encodeURIComponent(latest.run_id)}`);
    if (state.topicId === topicId && ["queued", "running"].includes(run.status)) {
      state.runId = run.id; state.cursor = 0; state.disconnected = true;
      showNotice("此话题仍在生成，可以继续读取或停止");
    }
  }
  disableComposer(state.busy);
}
async function loadTopics(): Promise<void> {
  state.topics = (await requestJson<{ topics: TopicView[] }>("/v1/topics")).topics; renderTopics();
  if (!state.topicId && state.topics[0]) await loadTopic(state.topics[0].id);
}
async function loadAgents(): Promise<void> {
  state.agents = await requestJson<AgentDescriptor[]>("/v1/agents/catalog");
  state.handles = await requestJson<AgentView[]>("/v1/agents"); renderAgents();
}
async function createTopic(): Promise<void> {
  if (state.busy) return;
  disableComposer(true);
  try {
  const title = text("topic-title").value.trim() || "新话题";
  const topic = await requestJson<TopicView>("/v1/topics", { method: "POST", body: JSON.stringify({ title }) });
  text("topic-title").value = ""; state.topics.unshift(topic); await loadTopic(topic.id);
  } catch (error) { showNotice(errorText(error), true); }
  finally { disableComposer(false); }
}
async function consume(runId: string, fromSeq = 0): Promise<void> {
  state.runId = runId; state.cursor = fromSeq; state.streaming = true; state.disconnected = false;
  if (fromSeq === 0) state.streamText = "";
  disableComposer(true); renderTopics(); renderMessages();
  const controller = new AbortController(); state.streamAbort = controller;
  try {
    state.cursor = await consumeEvents(runId, fromSeq, (frame: StreamFrame) => {
      state.cursor = frame.seq + 1;
      if (frame.kind === "chunk" && frame.data?.type === "text_delta") { state.streamText += String(frame.data.text || ""); renderMessages(); }
      if (frame.kind === "chunk" && frame.data?.type === "context_compact") showNotice("上下文已压缩并继续生成");
      if (frame.kind === "end") showNotice(`生成${statuses[String(frame.data?.status)] || "结束"}`);
    }, controller.signal);
  } catch (error) { state.disconnected = true; throw error; }
  finally { state.streaming = false; state.streamAbort = null; disableComposer(false); renderTopics(); renderMessages(); }
  if (state.topicId) await loadTopic(state.topicId);
}
async function sendMessage(event: SubmitEvent): Promise<void> {
  event.preventDefault(); if (!state.topicId || state.busy) return;
  const prompt = text("prompt").value.trim(); if (!prompt) return;
  disableComposer(true); showNotice("生成中……");
  try {
    const path = state.editingMessageId ? `/v1/topics/${encodeURIComponent(state.topicId)}/messages/${encodeURIComponent(state.editingMessageId)}/edit` : `/v1/topics/${encodeURIComponent(state.topicId)}/messages`;
    const body = state.editingMessageId ? { text: prompt, idempotency_key: idempotency() } : { text: prompt, idempotency_key: idempotency() };
    const run = await requestJson<RunView>(path, { method: "POST", body: JSON.stringify(body) });
    state.editingMessageId = null; text("prompt").value = ""; $("edit-cancel").hidden = true; $("composer-mode").textContent = "Enter 发送 · Shift+Enter 换行";
    await loadTopic(run.topic_id); await consume(run.id);
  } catch (error) { showNotice(errorText(error), true); state.streaming = false; disableComposer(false); }
}
async function selectBranch(messageId: string): Promise<void> {
  if (!state.topicId || !messageId) return;
  try { await requestJson(`/v1/topics/${encodeURIComponent(state.topicId)}/branch`, { method: "PUT", body: JSON.stringify({ message_id: messageId }) }); await loadTopic(state.topicId); showNotice("活动分支已切换"); } catch (error) { showNotice(errorText(error), true); }
}
function beginEdit(message: MessageView): void { if (state.busy) return; state.editingMessageId = message.id; text("prompt").value = message.content; $("edit-cancel").hidden = false; $("composer-mode").textContent = "编辑后会创建新分支，原消息保留"; text("prompt").focus(); }
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
        showNotice(run.status === "aborted" ? "已停止：终态已保存" : `运行${statuses[run.status] || run.status}`);
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
  try { const agent = await requestJson<AgentView>("/v1/agents", { method: "POST", body: JSON.stringify({ name }) }); state.handles = [...state.handles, agent]; renderAgents(); timer = setInterval(() => void loadAgents().catch(() => undefined), 500); const result = await requestJson<AgentView>(`/v1/agents/${agent.agent_id}/execute`, { method: "POST", body: JSON.stringify({ prompt, idempotency_key: idempotency() }) }); state.handles = [...state.handles.filter((item) => item.agent_id !== agent.agent_id), result]; text("agent-prompt").value = ""; renderAgents(); showNotice(`Agent ${statuses[result.status] || result.status}`); } catch (error) { showNotice(errorText(error), true); }
  finally { if (timer) clearInterval(timer); }
}
async function resumeAgent(agentId: string): Promise<void> {
  const prompt = text("agent-prompt").value.trim(); if (!prompt) { showNotice("请先输入要继续的 Agent 任务"); return; }
  const timer = setInterval(() => void loadAgents().catch(() => undefined), 500);
  try { const agent = await requestJson<AgentView>(`/v1/agents/${agentId}/resume`, { method: "POST", body: JSON.stringify({ prompt, idempotency_key: idempotency() }) }); state.handles = state.handles.map((item) => item.agent_id === agentId ? agent : item); renderAgents(); showNotice(`Agent ${agent.status}`); }
  catch (error) { showNotice(errorText(error), true); }
  finally { clearInterval(timer); }
}
async function stopAgent(agentId: string): Promise<void> { try { const agent = await requestJson<AgentView>(`/v1/agents/${agentId}/stop`, { method: "POST" }); state.handles = state.handles.map((item) => item.agent_id === agentId ? agent : item); renderAgents(); } catch (error) { showNotice(errorText(error), true); } }
async function destroyAgent(agentId: string): Promise<void> { try { await requestJson(`/v1/agents/${agentId}`, { method: "DELETE" }); state.handles = state.handles.filter((item) => item.agent_id !== agentId); renderAgents(); } catch (error) { showNotice(errorText(error), true); } }
async function refresh(): Promise<void> { if (state.topicId) await loadTopic(state.topicId); await Promise.all([loadTopics(), loadAgents()]); }
function setup(): void {
  $("new-topic").addEventListener("click", () => void createTopic()); $("refresh").addEventListener("click", () => void refresh()); $("composer").addEventListener("submit", (event) => void sendMessage(event as SubmitEvent));
  $("compact-preview").addEventListener("click", () => void compact(false)); $("compact-confirm").addEventListener("click", () => void compact(true)); $("run-stop").addEventListener("click", () => void stopRun()); $("run-resume").addEventListener("click", () => void resumeRun()); $("agent-create").addEventListener("click", () => void createAgent()); $("branch-select").addEventListener("change", (event) => void selectBranch((event.target as HTMLSelectElement).value));
  $("edit-cancel").addEventListener("click", () => { state.editingMessageId = null; text("prompt").value = ""; $("edit-cancel").hidden = true; $("composer-mode").textContent = "Enter 发送 · Shift+Enter 换行"; }); $("key-toggle").addEventListener("click", () => { $("key-form").hidden = !$("key-form").hidden; }); $("key-form").addEventListener("submit", (event) => { event.preventDefault(); setApiKey(text("api-key").value); void boot(); });
  text("compact-budget").addEventListener("input", () => { state.compactPreview = null; renderCompact(null); });
  text("prompt").addEventListener("keydown", (event) => { const keyEvent = event as KeyboardEvent; if (keyEvent.key === "Enter" && !keyEvent.shiftKey && !keyEvent.isComposing) { keyEvent.preventDefault(); ($("composer") as HTMLFormElement).requestSubmit(); } });
}
async function boot(): Promise<void> { try { const health = await requestJson<{ provider: string }>("/healthz"); $("model-label").textContent = health.provider; state.topics = (await requestJson<{ topics: TopicView[] }>("/v1/topics")).topics; state.agents = await requestJson<AgentDescriptor[]>("/v1/agents/catalog"); state.handles = await requestJson<AgentView[]>("/v1/agents"); renderTopics(); renderAgents(); if (!state.topicId && state.topics[0]) await loadTopic(state.topics[0].id); $("connection").textContent = "本地服务已连接"; $("connection").classList.add("ready"); $("key-form").hidden = true; text("api-key").value = ""; showNotice(); } catch (error) { $("connection").textContent = "需要连接"; $("connection").classList.remove("ready"); showNotice(errorText(error), true); $("key-form").hidden = false; } }
setup(); void boot();
