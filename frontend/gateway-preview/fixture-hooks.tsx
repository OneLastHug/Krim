/* Gateway preview data adapter. The rendered controls are the production React components; all data stays local and no requests are made. */
import { createContext, useContext, useEffect, useMemo, useState, type ReactNode } from "react";
import cases from "../visual/approved-cases.json";
import { DEFAULT_GROUP, type SidebarLayout } from "../src/lib/session-sidebar";
import type { SessionState } from "../src/hooks/use-session";
import type { DevelopmentState } from "../src/hooks/use-development";
import type { SessionContextState } from "../src/hooks/use-session-context";
import type { useSubagents as realUseSubagents } from "../src/hooks/use-subagents";
import type { PlanSnapshot } from "../src/lib/planning-contracts";
import type { Session, Turn, HistoryMessage } from "../src/lib/session-contracts";

export const TRANSITIONAL = ["creating", "starting", "stopping"];
const ACCOUNT_NAME = "本地账号";
const actor = { principal_id: "preview-owner", generation: 1, legacy: false, target_keys: ["target-development"], workspaces: [{ workspace_id: "local", role: "admin", approve: true }] };
const noOp = () => {};
const policy = { max_bytes: null, max_messages: null, reserved_output_tokens: 32000, counter_version: "approx-utf8/1", max_tokens: 1000000, compaction_trigger_tokens: 900000 };
const projects = cases.projects.map(p => ({ project_id: p.id, environment_id: `env-${p.id}`, principal_id: actor.principal_id, workspace_id: "local", title: p.title, generation: 1, status: "ready", created_at: 1, updated_at: 1 }));
type VisualTurn = { id: string; runId: string; prompt: string; answerHtml: string; tools: { name: string; args?: Record<string, unknown>; formatted?: string; state?: string }[]; approval?: { id: string; name: string; command: string; remainingSec: number }; planConfirm?: { text: string }; question?: { title: string; options: { id: string; label: string; desc: string }[] } };
type VisualScenario = { id: string; turns: VisualTurn[]; plan: { steps: { id?: string; text: string; status: string }[] } | null; context: typeof cases.context; outputs: string[] };
const scenarios = cases.scenarios as unknown as VisualScenario[];
const fileText = (path: string) => /\.md$/i.test(path) ? "# Krim 工作台\n\n项目开发说明。\n\n```ts\nconst ready = true;\n```\n" : "import { useState } from 'react';\n\nexport function Workbench() {\n  const [ready] = useState(true);\n  return ready;\n}\n";
function markdown(html: string): string {
  if (!html) return "已完成底栏和 Context 的对照检查。";
  const document = new DOMParser().parseFromString(html, "text/html");
  const text = (node: Node): string => {
    if (node.nodeType === Node.TEXT_NODE) return node.textContent ?? "";
    const element = node as Element, content = [...node.childNodes].map(text).join("");
    if (element.classList.contains("message-code-block")) {
      const pre = element.querySelector("pre");
      const header = element.querySelector(".message-code-head > span")?.textContent ?? "";
      return "\n\n```ts " + header + "\n" + (pre?.textContent ?? "") + "\n```\n\n";
    }
    if (element.tagName === "TABLE") {
      const rows = [...element.querySelectorAll("tr")].map(row => `| ${[...row.children].map(cell => [...cell.childNodes].map(text).join("").replaceAll("|", "\\|")).join(" | ")} |`);
      if (rows.length) rows.splice(1, 0, "| " + [...element.querySelectorAll("tr")[0]!.children].map(() => "---").join(" | ") + " |");
      return "\n\n" + rows.join("\n") + "\n\n";
    }
    if (element.tagName === "OL") return [...element.children].map((item, index) => `${index + 1}. ${[...item.childNodes].map(text).join("")}`).join("\n") + "\n\n";
    if (element.tagName === "PRE") return "\n\n```text\n" + (element.textContent ?? "") + "\n```\n\n";
    if (element.tagName === "CODE") return `\`${content}\``;
    if (element.tagName === "STRONG") return `**${content}**`;
    if (element.tagName === "LI") return `- ${content}\n`;
    if (["P", "OL", "UL", "DIV"].includes(element.tagName)) return `${content}\n\n`;
    if (element.tagName === "BR") return "\n";
    return content;
  };
  return text(document.body).trim();
}
function session(raw: { id: string; title?: string; lifecycle?: string }, title?: string): Session {
  return { id: raw.id, workspace_id: "local", owner_id: actor.principal_id, create_key: `create-${raw.id}`, title: title ?? raw.title ?? "", target_key: "target-development", target_fingerprint: "a".repeat(64), context_policy: policy, revision: 1, status: "open", dispatch_paused: false, created_at: 1, closed_at: null, archived_at: raw.lifecycle === "archived" ? 1 : null, deleted_at: null, lifecycle_revision: 0 };
}
/** 本地回复：轻量回显原文片段，明确是本地预览，不伪装模型。 */
function replyText(text: string): string {
  const clean = text.trim().replace(/\s+/g, " ");
  const quote = clean.length > 40 ? `${clean.slice(0, 40)}…` : clean;
  return `已收到${quote ? `「${quote}」` : ""}。这是本地预览对话，你可以继续切换模型或编辑接入设置。`;
}
const STORE_KEY = "krim.gateway-preview-chat.v1";
interface StoredMessage { role: "user" | "assistant"; text: string; at: number }
interface CreatedStub { id: string; title: string; group: string; project: string | null }
interface StoredChat { messages: Record<string, StoredMessage[]>; created: CreatedStub[]; active: string }
/** 只存本地消息、新建会话骨架与当前会话 id；不含访问密钥或其它凭据。 */
function loadStore(): StoredChat {
  try {
    const parsed = JSON.parse(localStorage.getItem(STORE_KEY) ?? "null") as Partial<StoredChat> | null;
    const messages: Record<string, StoredMessage[]> = {};
    for (const [key, value] of Object.entries(parsed?.messages ?? {})) {
      if (!Array.isArray(value)) continue;
      const kept = value.filter((item): item is StoredMessage => !!item && typeof item.text === "string" && (item.role === "user" || item.role === "assistant"));
      if (kept.length) messages[key] = kept;
    }
    const created = (Array.isArray(parsed?.created) ? parsed.created : []).filter((item): item is CreatedStub => !!item && typeof item.id === "string");
    return { messages, created, active: typeof parsed?.active === "string" ? parsed.active : "" };
  } catch { return { messages: {}, created: [], active: "" }; }
}
type SubagentsFixture = ReturnType<typeof realUseSubagents>;
/** 空 but ready 的子代理视图：无成员、无错误、能力可用，不发任何请求。 */
const emptySubagents = {
  members: [], selectedId: null, detail: null, loading: false, detailLoading: false, error: "", detailError: "", approvalError: "",
  available: true, busy: false, hasMore: false, receipt: "", approvalReceipt: "", approvalItems: [],
  select: noOp, reload: noOp, loadMore: async () => {}, send: async () => false, control: async () => false, approve: async () => false,
} as unknown as SubagentsFixture;

interface Model { chat: SessionState; dev: DevelopmentState; context: SessionContextState; sidebar: { layout: SidebarLayout; ready: boolean; saving: boolean; message: string; save: (value: SidebarLayout) => Promise<boolean>; reload: () => void }; features: Record<string, unknown>; accountName: string }
const FixtureContext = createContext<Model | null>(null);
function useFixture() { const value = useContext(FixtureContext); if (!value) throw new Error("Gateway preview fixture provider is required"); return value; }

export function GatewayFixtureProvider({ children }: { children: ReactNode }) {
  const stored = useMemo(loadStore, []);
  const [id, setId] = useState(() => new URL(location.href).searchParams.get("scene") === "question" ? "s-sidebar-proj"
    : stored.active && (cases.sessions.some(s => s.id === stored.active) || stored.created.some(c => c.id === stored.active)) ? stored.active
      : cases.currentSessionId);
  const [draft, setDraft] = useState("");
  const [answered, setAnswered] = useState<string[]>([]);
  const [stoppedTurns, setStoppedTurns] = useState<string[]>([]);
  const [mode, setMode] = useState<"manual" | "auto" | "bypass">("auto");
  const [projectOpen, setProjectOpen] = useState(false), [titles, setTitles] = useState<Record<string, string>>({});
  const [listView, setListView] = useState<"active" | "archived" | "trash">("active");
  const [localMessages, setLocalMessages] = useState<Record<string, StoredMessage[]>>(stored.messages);
  const [created, setCreated] = useState<CreatedStub[]>(stored.created);
  const [layout, setLayout] = useState<SidebarLayout>(() => ({
    revision: 0, groups: cases.groups,
    sessions: [...cases.sessions.map(s => ({ session_id: s.id, group_id: s.projectId, parent_id: s.parentId, pinned: s.pinned })), ...stored.created.map(c => ({ session_id: c.id, group_id: c.group, parent_id: null, pinned: false }))],
  }));
  useEffect(() => {
    try { localStorage.setItem(STORE_KEY, JSON.stringify({ messages: localMessages, created, active: id })); } catch { /* 存储不可用时保持内存态 */ }
  }, [localMessages, created, id]);
  const value = useMemo<Model>(() => {
    const rawFor = (sessionId: string) => {
      const fromCases = cases.sessions.find(s => s.id === sessionId);
      if (fromCases) return { id: fromCases.id, title: titles[sessionId] ?? fromCases.title, projectId: fromCases.projectId, lifecycle: fromCases.lifecycle };
      const made = created.find(c => c.id === sessionId);
      return made ? { id: made.id, title: titles[made.id] ?? made.title, projectId: made.project ?? "", lifecycle: "active" } : null;
    };
    const raw = rawFor(id);
    const scenario = scenarios.find(s => s.id === id);
    const contextData = scenario?.context ?? cases.context;
    const selected = raw ? session(raw, raw.title) : null;
    const project = selected ? projects.find(p => p.project_id === raw?.projectId) ?? projects[0]! : null;
    const scene = new URL(location.href).searchParams.get("scene");
    const scenarioTurns: Turn[] = selected ? (scenario?.turns ?? []).map((turn, index) => {
      const last = index === (scenario?.turns.length ?? 0) - 1;
      const status: Turn["status"] = stoppedTurns.includes(turn.id) ? "cancelled" : last && scene === "running" ? "running" : last && scene === "queued" ? "queued" : "completed";
      const prompt = last ? new URL(location.href).searchParams.get("prompt") ?? turn.prompt : turn.prompt;
      const transcript: HistoryMessage[] = [{ role: "user", content: prompt, call_id: null, tool_calls: [] }];
      for (const [toolIndex, tool] of turn.tools.entries()) {
        const callId = `${turn.id}-call-${toolIndex}`;
        transcript.push({ role: "assistant", content: "", call_id: null, tool_calls: [{ kind: "tool_call", id: callId, name: tool.name, arguments: tool.args && typeof tool.args === "object" && !Array.isArray(tool.args) ? tool.args : { path: cases.outputs[toolIndex % cases.outputs.length] } }] });
        transcript.push({ role: "tool", content: tool.formatted ?? "已完成", call_id: callId, tool_calls: [] });
      }
      transcript.push({ role: "assistant", content: markdown(turn.answerHtml), call_id: null, tool_calls: [] });
      return { id: turn.id, session_id: selected.id, workspace_id: "local", owner_id: actor.principal_id, ordinal: index + 1, request_key: `turn-${turn.id}`, text: prompt, accepted_revision: 0, status, run_id: status === "queued" ? null : turn.runId, context_fingerprint: "b".repeat(64), transcript, code: null, created_at: 1, started_at: status === "queued" ? null : 1, finished_at: status === "running" || status === "queued" ? null : 2 };
    }) : [];
    // 本地消息按 user/assistant 成对重建为 Turn；id 由存储序号推导，重载后保持稳定。
    const storedMessages = localMessages[id] ?? [];
    const baseOrdinal = scenario?.turns.length ?? 0;
    const localTurns: Turn[] = [];
    for (let index = 0; index + 1 < storedMessages.length; index += 2) {
      const user = storedMessages[index]!, assistant = storedMessages[index + 1]!;
      if (user.role !== "user" || assistant.role !== "assistant") continue;
      const localId = `local-${id}-${index}`;
      localTurns.push({ id: localId, session_id: id, workspace_id: "local", owner_id: actor.principal_id, ordinal: baseOrdinal + localTurns.length + 1, request_key: `turn-${localId}`, text: user.text, accepted_revision: 0, status: stoppedTurns.includes(localId) ? "cancelled" : "completed", run_id: `preview-run-${localId}`, context_fingerprint: "b".repeat(64), transcript: [{ role: "user", content: user.text, call_id: null, tool_calls: [] }, { role: "assistant", content: assistant.text, call_id: null, tool_calls: [] }], code: null, created_at: user.at, started_at: user.at, finished_at: assistant.at });
    }
    const turns = [...scenarioTurns, ...localTurns];
    const manifest = selected ? { version: 1, counter_version: "approx-utf8/1", input_bytes: contextData.usedTokens * 4, estimated_tokens: contextData.usedTokens, model_context_window: 1000000, reserved_output_tokens: 32000, input_budget_bytes: null, compaction_changes: null, items_truncated: false, omitted_items: 0, source_refs_truncated: false,
      items: contextData.categories.map((category, index) => ({ key: `item-${index}`, category: category.key, label: category.key, content_hash: "c".repeat(64), input_bytes: category.tokens * 4, estimated_tokens: category.tokens, message_index: null, tool_index: null, call_id: null, provider_call_id: null, source_refs: null, source_refs_truncated: false })) } : null;
    const usageScene = new URL(location.href).searchParams.get("usage");
    const reported = !selected || usageScene === "missing" ? null : usageScene === "partial"
      ? { input: 9123, output: 17, total: null, cached: 0, cacheWrite: null, reasoning: null, final: false }
      : { input: 571937, output: 2173, total: 574110, cached: 251123, cacheWrite: null, reasoning: 432, final: true };
    const context = { data: selected ? { configuration: { model: "model-general", reasoning_effort: "Extra", thinking: null } } : null, manifest, reported, requestId: selected ? "fixture-request" : "", used: selected ? contextData.usedTokens : 0, limit: selected ? 900000 : null, turnId: turns.at(-1)?.id ?? "", loading: false, error: "", refresh: noOp } as unknown as SessionContextState;
    const execution = { identity: `preview:${id}`, loading: false, policy: { mode, project_id: project?.project_id ?? null, project_access: "write", revision: 1 }, error: "", changing: false, clearing: false, clearError: "", ruleCount: 0, hasPendingClear: false, hasPendingMode: false, projectOpen, project: project ? { title: project.title, is_creator: true, owner_id: actor.principal_id, project_access: "write", sandbox_status: "ready" } : null, projectError: "", openProject: async () => setProjectOpen(true), closeProject: () => setProjectOpen(false), changeMode: async (next: typeof mode) => { setMode(next); return true; }, retryMode: async () => false, clearRules: async () => {}, reload: noOp };
    const appendLocal = (sessionId: string, text: string) => {
      const body = text.trim();
      if (!body) return;
      const at = Date.now();
      setLocalMessages(previous => ({ ...previous, [sessionId]: [...(previous[sessionId] ?? []), { role: "user" as const, text: body, at }, { role: "assistant" as const, text: replyText(body), at }] }));
    };
    const openSession = (sessionId: string) => {
      setId(sessionId); setDraft("");
      const url = new URL(location.href);
      url.searchParams.set("view", "chat"); url.searchParams.set("workspace", "local"); url.searchParams.set("session", sessionId);
      window.history.replaceState(null, "", url);
    };
    const create = async (title?: string, _target?: string, group?: string, _text?: string, project?: string | null) => {
      const sessionId = crypto.randomUUID();
      const entry: CreatedStub = { id: sessionId, title: (title ?? "").trim() || "新的对话", group: group || DEFAULT_GROUP, project: project || null };
      setCreated(previous => [...previous, entry]);
      setLayout(previous => ({ revision: previous.revision + 1, groups: previous.groups, sessions: [...previous.sessions, { session_id: sessionId, group_id: entry.group, parent_id: null, pinned: false }] }));
      openSession(sessionId);
      return session({ id: sessionId, title: titles[sessionId] ?? entry.title, lifecycle: "active" });
    };
    const chat = { actor, ready: true, canWrite: true, canApprove: true, workspace: "local", id, session: selected, list: [...cases.sessions.filter(s => listView === "archived" ? s.lifecycle === "archived" : listView === "trash" ? false : s.lifecycle === "active").map(s => session(s, titles[s.id])), ...(listView === "active" ? created.map(c => session({ id: c.id, title: titles[c.id] ?? c.title, lifecycle: "active" })) : [])], listView, history: turns, events: [], turnEvents: {}, turnFacts: {}, details: {}, currentTurn: turns.at(-1) ?? null, approvals: [], pending: [], pendingControls: [], notice: null, reading: false, creating: false, sending: false, busy: false, controlling: false, turnPending: undefined, newPending: undefined, draft, execution,
      edit: setDraft, select: (_workspace: string, next: string) => { setId(next); setDraft(""); },
      create,
      createAndSubmit: async (text: string, _target?: string, group?: string) => {
        const body = text.trim();
        const made = await create(body.split("\n")[0]?.slice(0, 128) || "新的对话", undefined, group);
        if (made && body) appendLocal(made.id, body);
        setDraft("");
      },
      submit: async (text?: string, _recipient?: Session | null) => {
        const body = (typeof text === "string" ? text : draft).trim();
        if (!body) return;
        appendLocal(id, body);
        setDraft("");
      },
      refresh: noOp, posting: () => false, recover: async () => {}, recoverControl: async () => {}, control: async (turn: Turn | null, action: string) => { if (turn && action === "cancel") setStoppedTurns(previous => [...previous, turn.id]); }, loadDetails: async () => {}, rename: async (title: string, target = id) => setTitles(previous => ({ ...previous, [target]: title })), lifecycle: async () => {}, setSessionListView: setListView } as unknown as SessionState;
    const dev = { binding: selected && project ? { session_id: selected.id, project_id: project.project_id, workspace_id: "local", revision: 1, session_revision: 1 } : null, project,
      environment: project ? { owner_id: actor.principal_id, project_id: project.project_id, environment_id: `env-${project.project_id}`, generation: 1, state: "ready" } : null,
      tools: cases.tools.map((tool, index) => ({ ...tool, version: "1", input_schema: {}, output_schema: {}, fingerprint: String(index).padStart(64, "0"), source: null })),
      connections: [{ id: "preview-docs", name: "文档服务", url: "https://example.invalid/mcp", allowed_hosts: ["example.invalid"], selected_tools: [], readonly_tools: [], timeout_seconds: 10, revision: 1, credential_revision: 1, credential_configured: true }, { id: "preview-files", name: "项目资料", url: "https://example.invalid/files", allowed_hosts: ["example.invalid"], selected_tools: [], readonly_tools: [], timeout_seconds: 10, revision: 1, credential_revision: 1, credential_configured: true }],
      secretRefs: [{ name: "API_TOKEN", updated_at: 1 }, { name: "CI_TOKEN", updated_at: 1 }, { name: "REGISTRY_TOKEN", updated_at: 1 }],
      notice: null, writes: [], ready: !!selected, canWrite: !!selected, canApprove: true, busy: false, pendingBind: false, preparing: false, stopping: false,
      reload: noOp, dismissNotice: noOp, bindProject: async () => {}, prepareEnvironment: async () => true, stopEnvironment: async () => {}, writeSecret: async () => {}, recoverPending: async () => {}, discardPending: noOp } as unknown as DevelopmentState;
    const steps = scenario?.plan?.steps ?? [];
    const counts = { pending: 0, in_progress: 0, completed: 0, blocked: 0 };
    for (const step of steps) if (step.status in counts) counts[step.status as keyof typeof counts]++;
    const plan = selected && steps.length ? { session_id: id, revision: 1, total: steps.length, matched_total: steps.length, next_offset: null, counts, steps: steps.map((step, index) => ({ ...step, id: step.id ?? `step-${index}` })) } as unknown as PlanSnapshot : null;
    const pending = (scenario?.turns ?? []).flatMap(turn => {
      const base = { revision: 1, status: "pending", workspace_id: "local", owner_id: actor.principal_id, session_id: id, run_id: turn.runId, turn_id: turn.id, request_key: turn.id, mode_revision: 1, response: null, created_at: 1, updated_at: 1 };
      const items = [];
      if (turn.planConfirm && !answered.includes(turn.id + "-plan")) items.push({ ...base, id: turn.id + "-plan", kind: "plan_confirmation", payload: { plan: turn.planConfirm.text } });
      if (turn.question && !answered.includes(turn.id + "-question")) items.push({ ...base, id: turn.id + "-question", kind: "user_input", payload: { questions: [{ id: "legacy-group", prompt: turn.question.title, mode: "single", options: turn.question.options.map(option => ({ ...option, description: option.desc })), allow_text: true }] } });
      return items;
    });
    const answer = async (request: { id: string }) => { setAnswered(previous => [...previous, request.id]); };
    return { chat, dev, context, sidebar: { layout, ready: true, saving: false, message: "", save: async next => { setLayout({ ...next, revision: layout.revision + 1 }); return true; }, reload: noOp }, features: { plan, planError: "", interactionError: "", interactions: { mode: pending.some(item => item.kind === "plan_confirmation") ? "plan" : "execute", revision: 1, pending }, paging: false, loadMore: async () => {}, reload: noOp, answer, cancel: answer }, accountName: ACCOUNT_NAME };
  }, [id, draft, mode, projectOpen, titles, listView, layout, answered, stoppedTurns, localMessages, created]);
  return <FixtureContext.Provider value={value}>{children}</FixtureContext.Provider>;
}
export function useSession() { return useFixture().chat; }
export function useDevelopment() { return useFixture().dev; }
export function useSessionContext() { return useFixture().context; }
export function useSessionSidebar() { return useFixture().sidebar; }
export function useSessionFeatures() { return useFixture().features; }
export function useSubagents() { return emptySubagents; }
export async function fetchProjectsPage(_apiKey: string, _signal?: AbortSignal) { return { items: projects, total: projects.length, offset: 0, limit: 100 }; }
export async function fetchDirectory(_apiKey: string, projectId: string, path: string, _signal?: AbortSignal, _offset = 0) { return { project_id: projectId, path, total: cases.outputs.length, offset: 0, next_offset: null, entries: cases.outputs.map(name => ({ name: name.split("/").at(-1)!, type: "file", size: fileText(name).length })) }; }
export async function fetchFileBytes(_apiKey: string, _project: string, path: string, _signal?: AbortSignal) { const bytes = new TextEncoder().encode(fileText(path)); return { bytes, sha256: "d".repeat(64), size: bytes.length, eof: true, may_change: false }; }
export function useVisualFixture() { return useFixture(); }
