/* Test-only data adapter. The rendered controls are the production React components. */
import { createContext, useContext, useMemo, useState, type ReactNode } from "react";
import cases from "./approved-cases.json";
import type { SessionState } from "../src/hooks/use-session";
import type { DevelopmentState } from "../src/hooks/use-development";
import type { SessionContextState } from "../src/hooks/use-session-context";
import type { PlanSnapshot } from "../src/lib/planning-contracts";
import type { SidebarLayout } from "../src/lib/session-sidebar";
import type { Session, Turn, HistoryMessage } from "../src/lib/session-contracts";

const actor = { principal_id: "visual-owner", generation: 1, legacy: false, target_keys: ["target-development"], workspaces: [{ workspace_id: "local", role: "admin", approve: true }] };
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
function session(raw: typeof cases.sessions[number], title?: string): Session {
  return { id: raw.id, workspace_id: "local", owner_id: actor.principal_id, create_key: `create-${raw.id}`, title: title ?? raw.title, target_key: "target-development", target_fingerprint: "a".repeat(64), context_policy: policy, revision: 1, status: "open", dispatch_paused: false, created_at: 1, closed_at: null, archived_at: raw.lifecycle === "archived" ? 1 : null, deleted_at: null, lifecycle_revision: 0 };
}
interface Model { chat: SessionState; dev: DevelopmentState; context: SessionContextState; sidebar: { layout: SidebarLayout; ready: boolean; saving: boolean; message: string; save: (value: SidebarLayout) => Promise<boolean>; reload: () => void }; features: Record<string, unknown> }
const FixtureContext = createContext<Model | null>(null);
function useFixture() { const value = useContext(FixtureContext); if (!value) throw new Error("Visual fixture provider is required"); return value; }

export function FixtureProvider({ children }: { children: ReactNode }) {
  const [id, setId] = useState(new URL(location.href).searchParams.get("scene") === "question" ? "s-sidebar-proj" : cases.currentSessionId), [draft, setDraft] = useState("");
  const [answered, setAnswered] = useState<string[]>([]);
  const [stoppedTurns, setStoppedTurns] = useState<string[]>([]);
  const [mode, setMode] = useState<"manual" | "auto" | "bypass">("auto");
  const [projectOpen, setProjectOpen] = useState(false), [titles, setTitles] = useState<Record<string, string>>({});
  const [listView, setListView] = useState<"active" | "archived" | "trash">("active");
  const [layout, setLayout] = useState<SidebarLayout>({ revision: 0, groups: cases.groups, sessions: cases.sessions.map(s => ({ session_id: s.id, group_id: s.projectId, parent_id: s.parentId, pinned: s.pinned })) });
  const value = useMemo<Model>(() => {
    const raw = cases.sessions.find(s => s.id === id);
    const scenario = scenarios.find(s => s.id === id);
    const contextData = scenario?.context ?? cases.context;
    const selected = raw ? session(raw, titles[id]) : null;
    const project = selected ? projects.find(p => p.project_id === raw?.projectId) ?? projects[0]! : null;
    const scene = new URL(location.href).searchParams.get("scene");
    const turns: Turn[] = selected ? (scenario?.turns ?? []).map((turn, index) => {
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
    const manifest = selected ? { version: 1, counter_version: "approx-utf8/1", input_bytes: contextData.usedTokens * 4, estimated_tokens: contextData.usedTokens, model_context_window: 1000000, reserved_output_tokens: 32000, input_budget_bytes: null, compaction_changes: null, items_truncated: false, omitted_items: 0, source_refs_truncated: false,
      items: contextData.categories.map((category, index) => ({ key: `item-${index}`, category: category.key, label: category.key, content_hash: "c".repeat(64), input_bytes: category.tokens * 4, estimated_tokens: category.tokens, message_index: null, tool_index: null, call_id: null, provider_call_id: null, source_refs: null, source_refs_truncated: false })) } : null;
    const usageScene = new URL(location.href).searchParams.get("usage");
    const reported = !selected || usageScene === "missing" ? null : usageScene === "partial"
      ? { input: 9123, output: 17, total: null, cached: 0, cacheWrite: null, reasoning: null, final: false }
      : { input: 571937, output: 2173, total: 574110, cached: 251123, cacheWrite: null, reasoning: 432, final: true };
    const context = { data: selected ? { configuration: { model: "GPT-6 Luna", reasoning_effort: "Extra", thinking: null } } : null, manifest, reported, requestId: selected ? "fixture-request" : "", used: selected ? contextData.usedTokens : 0, limit: selected ? 900000 : null, turnId: turns.at(-1)?.id ?? "", loading: false, error: "", refresh: noOp } as unknown as SessionContextState;
    const execution = { identity: `visual:${id}`, loading: false, policy: { mode, project_id: project?.project_id ?? null, project_access: "write", revision: 1 }, error: "", changing: false, clearing: false, clearError: "", ruleCount: 0, hasPendingClear: false, hasPendingMode: false, projectOpen, project: project ? { title: project.title, is_creator: true, owner_id: actor.principal_id, project_access: "write", sandbox_status: "ready" } : null, projectError: "", openProject: async () => setProjectOpen(true), closeProject: () => setProjectOpen(false), changeMode: async (next: typeof mode) => { setMode(next); return true; }, retryMode: async () => false, clearRules: async () => {}, reload: noOp };
    const chat = { actor, ready: true, canWrite: true, canApprove: true, workspace: "local", id, session: selected, list: cases.sessions.filter(s => listView === "archived" ? s.lifecycle === "archived" : listView === "trash" ? false : s.lifecycle === "active").map(s => session(s, titles[s.id])), listView, history: turns, events: [], turnEvents: {}, turnFacts: {}, details: {}, currentTurn: turns.at(-1) ?? null, approvals: [], pending: [], pendingControls: [], notice: null, reading: false, creating: false, sending: false, busy: false, controlling: false, turnPending: undefined, newPending: undefined, draft, execution,
      edit: setDraft, select: (_workspace: string, next: string) => { setId(next); setDraft(""); }, create: async () => null, createAndSubmit: async () => { setId(cases.currentSessionId); setDraft(""); }, submit: async () => setDraft(""), refresh: noOp, posting: () => false, recover: async () => {}, recoverControl: async () => {}, control: async (turn: Turn | null, action: string) => { if (turn && action === "cancel") setStoppedTurns(previous => [...previous, turn.id]); }, loadDetails: async () => {}, rename: async (title: string, target = id) => setTitles(previous => ({ ...previous, [target]: title })), lifecycle: async () => {}, setSessionListView: setListView } as unknown as SessionState;
    const dev = { binding: selected && project ? { session_id: selected.id, project_id: project.project_id, workspace_id: "local", revision: 1, session_revision: 1 } : null, project,
      environment: project ? { owner_id: actor.principal_id, project_id: project.project_id, environment_id: `env-${project.project_id}`, generation: 1, state: "ready" } : null,
      tools: cases.tools.map((tool, index) => ({ ...tool, version: "1", input_schema: {}, output_schema: {}, fingerprint: String(index).padStart(64, "0"), source: null })),
      connections: [{ id: "visual-docs", name: "文档服务", url: "https://example.invalid/mcp", allowed_hosts: ["example.invalid"], selected_tools: [], readonly_tools: [], timeout_seconds: 10, revision: 1, credential_revision: 1, credential_configured: true }, { id: "visual-files", name: "项目资料", url: "https://example.invalid/files", allowed_hosts: ["example.invalid"], selected_tools: [], readonly_tools: [], timeout_seconds: 10, revision: 1, credential_revision: 1, credential_configured: true }],
      secretRefs: [{ name: "API_TOKEN", updated_at: 1 }, { name: "CI_TOKEN", updated_at: 1 }, { name: "REGISTRY_TOKEN", updated_at: 1 }],
      notice: null, writes: [], ready: !!selected, canWrite: !!selected, canApprove: true, busy: false, pendingBind: false, preparing: false, stopping: false,
      reload: noOp, dismissNotice: noOp, bindProject: async () => {}, prepareEnvironment: async () => {}, stopEnvironment: async () => {}, writeSecret: async () => {}, recoverPending: async () => {}, discardPending: noOp } as unknown as DevelopmentState;
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
    return { chat, dev, context, sidebar: { layout, ready: true, saving: false, message: "", save: async next => { setLayout({ ...next, revision: layout.revision + 1 }); return true; }, reload: noOp }, features: { plan, planError: "", interactionError: "", interactions: { mode: pending.some(item => item.kind === "plan_confirmation") ? "plan" : "execute", revision: 1, pending }, paging: false, loadMore: async () => {}, reload: noOp, answer, cancel: answer } };
  }, [id, draft, mode, projectOpen, titles, listView, layout, answered, stoppedTurns]);
  return <FixtureContext.Provider value={value}>{children}</FixtureContext.Provider>;
}
export function useSession() { return useFixture().chat; }
export function useDevelopment() { return useFixture().dev; }
export function useSessionContext() { return useFixture().context; }
export function useSessionSidebar() { return useFixture().sidebar; }
export function useSessionFeatures() { return useFixture().features; }
export async function fetchProjectsPage() { return { items: projects, total: projects.length, offset: 0, limit: 100 }; }
export async function fetchDirectory(_key: string, projectId: string, path: string) { return { project_id: projectId, path, total: cases.outputs.length, offset: 0, next_offset: null, entries: cases.outputs.map(name => ({ name: name.split("/").at(-1)!, type: "file", size: fileText(name).length })) }; }
export async function fetchFileBytes(_key: string, _project: string, path: string) { const bytes = new TextEncoder().encode(fileText(path)); return { bytes, sha256: "d".repeat(64), size: bytes.length, eof: true, may_change: false }; }
export function useVisualFixture() { return useFixture(); }
