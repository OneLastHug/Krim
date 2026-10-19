import { useEffect, useRef, useState } from "react";
import { object, request, RequestError } from "../api";
import { chatReason } from "../lib/copy";
import { parseActor, parseSession, parseTurn, sessionPath, turnPath, type Actor, type Session, type Turn } from "../lib/session-contracts";
import { CURRENT, performIntent, prepareIntent, readDraft, readIntents, saveDraft, saveSelection, type Intent, type Scope } from "../lib/session-requests";

interface Detail { facts: Record<string, unknown> | null; trace: Record<string, unknown>; events: Record<string, unknown>[] }
interface ControlIntent extends Scope { target: string; turn_id: string; body: Record<string, unknown>; rejected: boolean }
const CONTROL = "krim.agent-core.controls/1";
function controls(): Record<string, ControlIntent> {
  const raw = localStorage.getItem(CONTROL), value = object(raw ? JSON.parse(raw) : {});
  for (const item of Object.values(value)) {
    const i = object(item), body = object(i.body);
    if (typeof i.principal_id !== "string" || !Number.isSafeInteger(i.generation) || typeof i.workspace_id !== "string" || typeof i.session_id !== "string" || typeof i.turn_id !== "string" || typeof i.target !== "string" || typeof i.rejected !== "boolean" || typeof body.request_key !== "string") throw new Error("无法读取未确认请求");
    const base = sessionPath(i.workspace_id) + "/" + encodeURIComponent(i.session_id);
    const allowed = [base + "/close", base + "/resume", turnPath(i.workspace_id, i.session_id, i.turn_id) + "/cancel"];
    if (!allowed.includes(i.target) && !i.target.startsWith(turnPath(i.workspace_id, i.session_id, i.turn_id) + "/approvals/")) throw new Error("无法读取未确认请求");
  }
  return value as unknown as Record<string, ControlIntent>;
}
function saveControls(value: Record<string, ControlIntent>) { const raw = JSON.stringify(value); localStorage.setItem(CONTROL, raw); if (localStorage.getItem(CONTROL) !== raw) throw new Error("无法保存请求"); }
function pending() { try { return { values: readIntents(localStorage), error: "" }; } catch { return { values: [] as Intent[], error: "无法读取未确认请求" }; } }
function delay(signal: AbortSignal) { return new Promise<void>(resolve => { const finish = () => { clearTimeout(timer); signal.removeEventListener("abort", finish); resolve(); }; const timer = setTimeout(finish, 300); signal.addEventListener("abort", finish, { once: true }); }); }
export function useSession(apiKey: string, active: boolean, fence: { current: number }, onLegacy: (value: boolean) => void) {
  const initial = new URL(location.href);
  const [workspace, setWorkspace] = useState(initial.searchParams.get("workspace") ?? "");
  const [id, setId] = useState(initial.searchParams.get("session") ?? "");
  const [actor, setActor] = useState<{ key: string; value: Actor } | null>(null);
  const [list, setList] = useState<Session[]>([]), [session, setSession] = useState<Session | null>(null), [history, setHistory] = useState<Turn[]>([]);
  const [draft, setDraft] = useState(""), [notice, setNotice] = useState<string | null>(null), [reading, setReading] = useState(false);
  const [pendingState, setPending] = useState(pending), [details, setDetails] = useState<Record<string, Detail>>({});
  const [approvals, setApprovals] = useState<Record<string, unknown>[]>([]), [events, setEvents] = useState<Record<string, unknown>[]>([]);
  const [version, setVersion] = useState(0), [identityVersion, setIdentityVersion] = useState(0), [, setFlights] = useState(0);
  const fenceGeneration = fence.current;
  const mounted = useRef(false), epoch = useRef(0), reader = useRef<AbortController | null>(null), flights = useRef(new Set<string>());
  const selected = useRef({ workspace, id }); selected.current = { workspace, id };
  const keyRef = useRef(apiKey), activeRef = useRef(active), actorRef = useRef(actor); keyRef.current = apiKey; activeRef.current = active; actorRef.current = actor;
  const ready = actor !== null && actor.key === apiKey;
  const membership = ready ? actor.value.workspaces.find(w => w.workspace_id === workspace) : undefined;
  const scope = (sessionId: string | null = selected.current.id || null, ws = selected.current.workspace): Scope => {
    const value = actorRef.current;
    if (!value || value.key !== keyRef.current) throw new Error("请先连接");
    return { principal_id: value.value.principal_id, generation: value.value.generation, workspace_id: ws, session_id: sessionId };
  };
  function invalidate() { epoch.current++; reader.current?.abort(); }
  function capture() { const e = epoch.current, f = fence.current, key = apiKey, view = { ...selected.current }; return () => mounted.current && activeRef.current && e === epoch.current && f === fence.current && key === keyRef.current && view.id === selected.current.id && view.workspace === selected.current.workspace; }
  function refreshPending() { if (mounted.current) setPending(pending()); }
  function flight(key: string, value: boolean) { if (value) flights.current.add(key); else flights.current.delete(key); if (mounted.current) setFlights(v => v + 1); }
  function select(ws: string, sessionId: string, turnId: string | null = null) {
    invalidate();
    const target = scope(sessionId || null, ws);
    saveSelection(localStorage, target, sessionId, turnId);
    const url = new URL(location.href); url.searchParams.set("view", "chat"); url.searchParams.set("workspace", ws);
    if (sessionId) url.searchParams.set("session", sessionId); else url.searchParams.delete("session");
    if (turnId) url.searchParams.set("turn", turnId); else url.searchParams.delete("turn");
    historyReplace(url);
    if (selected.current.workspace !== ws) setList([]);
    selected.current = { workspace: ws, id: sessionId }; setWorkspace(ws); setId(sessionId); setSession(null); setHistory([]); setDetails({}); setEvents([]); setApprovals([]); setNotice(null);
    setDraft(readDraft(localStorage, target)); setVersion(v => v + 1);
  }
  function historyReplace(url: URL) { window.history.replaceState(null, "", url); }
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; invalidate(); }; }, []);
  useEffect(() => {
    if (!active) return;
    setActor(null); actorRef.current = null; setList([]); setSession(null); setHistory([]); setDetails({}); setApprovals([]); setEvents([]);
    const controller = new AbortController(), f = fence.current, key = apiKey;
    const current = () => mounted.current && activeRef.current && !controller.signal.aborted && key === keyRef.current && f === fence.current;
    void request("/v2/me", key, undefined, controller.signal).then(raw => {
      if (!current()) return;
      const value = parseActor(raw); if (!value.workspaces.length) throw new Error("没有可用工作区");
      let ws = selected.current.workspace, sid = selected.current.id;
      if (!value.workspaces.some(w => w.workspace_id === ws)) { ws = value.workspaces[0]!.workspace_id; sid = ""; }
      if (!sid) {
        try { const stored = JSON.parse(localStorage.getItem(CURRENT) ?? "null") as (Scope & { turn_id: string | null }) | null; if (stored && stored.principal_id === value.principal_id && stored.generation === value.generation && stored.workspace_id === ws) sid = stored.session_id ?? ""; } catch { /* 原创建intent由独立严格账本保护。 */ }
      }
      actorRef.current = { key, value }; selected.current = { workspace: ws, id: sid }; setActor({ key, value }); setWorkspace(ws); setId(sid); onLegacy(value.legacy);
      setDraft(readDraft(localStorage, { principal_id: value.principal_id, generation: value.generation, workspace_id: ws, session_id: sid || null })); refreshPending(); setNotice(null);
    }).catch(error => { if (current()) setNotice(chatReason(error)); });
    return () => controller.abort();
  }, [apiKey, active, identityVersion, fenceGeneration]);
  useEffect(() => {
    if (!active || !ready || !workspace) return;
    const controller = new AbortController(); reader.current = controller;
    const check = capture(), current = () => check() && !controller.signal.aborted;
    setReading(true);
    async function load() {
      try {
        const rows = await request(sessionPath(workspace) + "?limit=50", apiKey, undefined, controller.signal);
        if (!current()) return; if (!Array.isArray(rows)) throw new Error("对话列表无效");
        const sessions = rows.map(parseSession); if (sessions.some(s => s.workspace_id !== workspace)) throw new Error("对话身份不匹配"); setList(sessions);
        if (!id) { setSession(null); setHistory([]); return; }
        let cursor = 0, runId = ""; const timeline: Record<string, unknown>[] = [];
        while (current()) {
          const value = parseSession(await request(sessionPath(workspace) + "/" + encodeURIComponent(id), apiKey, undefined, controller.signal));
          if (!current()) return; if (value.id !== id || value.workspace_id !== workspace) throw new Error("对话身份不匹配");
          const turns: Turn[] = [];
          for (let offset = 0; offset <= 1024; offset += 50) {
            const raw = await request(sessionPath(workspace) + "/" + encodeURIComponent(id) + `/turns?offset=${offset}&limit=50`, apiKey, undefined, controller.signal);
            if (!current()) return; if (!Array.isArray(raw)) throw new Error("历史无效");
            const page = raw.map(parseTurn); if (page.some(t => t.session_id !== id || t.workspace_id !== workspace)) throw new Error("历史身份不匹配"); turns.push(...page);
            if (page.length < 50) break;
            if (offset === 1000) throw new Error("历史容量超限");
          }
          if (new Set(turns.map(t => t.id)).size !== turns.length) throw new Error("历史重复");
          setSession(value); setHistory(turns); setReading(false);
          const running = turns.find(t => t.status === "running");
          if (running?.run_id) {
            if (runId !== running.run_id) { runId = running.run_id; cursor = 0; timeline.length = 0; }
            let raw: unknown;
            do {
              raw = await request(turnPath(workspace, id, running.id) + "/events?from_seq=" + cursor, apiKey, undefined, controller.signal);
              if (!current()) return; if (!Array.isArray(raw)) throw new Error("事件无效");
              for (const item of raw) { const event = object(item); if (event.run_id !== runId || event.seq !== cursor) throw new Error("事件身份不匹配"); timeline.push(event); cursor++; }
            } while ((raw as unknown[]).length === 64);
            const pending = await request(turnPath(workspace, id, running.id) + "/approvals", apiKey, undefined, controller.signal);
            if (!current()) return; if (!Array.isArray(pending)) throw new Error("审批无效");
            setEvents([...timeline]); setApprovals(pending.map(object));
          } else { setEvents([]); setApprovals([]); }
          if (!turns.some(t => t.status === "running" || t.status === "queued") || value.dispatch_paused && !running) break;
          await delay(controller.signal);
        }
      } catch (error) { if (current()) setNotice(chatReason(error)); }
      finally { if (current()) setReading(false); }
    }
    void load(); return () => controller.abort();
  }, [apiKey, active, ready, actor, workspace, id, version, fenceGeneration]);
  function edit(text: string) { try { saveDraft(localStorage, scope(), text); setDraft(text); } catch (error) { setNotice(chatReason(error)); } }
  async function create(title: string, target: string) {
    let marker = "", ownsFlight = false; let current = () => false;
    try {
      const targetScope = scope(null); marker = "post:" + JSON.stringify(targetScope); if (flights.current.has(marker)) return;
      flight(marker, true); ownsFlight = true; invalidate(); current = capture(); setVersion(v => v + 1); saveDraft(localStorage, targetScope, title);
      const { intent, first } = prepareIntent(localStorage, targetScope, "session", { title, target_key: target }); refreshPending();
      await performIntent(localStorage, intent, apiKey, first, value => { if (!current()) return false; select(intent.workspace_id, value.id); setSession(value as Session); return true; });
    } catch (error) { if (current()) setNotice(chatReason(error)); }
    finally { if (marker && ownsFlight) flight(marker, false); refreshPending(); }
  }
  async function submit() {
    let marker = "", ownsFlight = false; let current = () => false;
    try {
      if (!session || session.id !== id) return;
      if (new TextEncoder().encode(draft).length > 32768) { setNotice("输入超出容量，请修改后提交"); return; }
      const targetScope = scope(id); marker = "post:" + JSON.stringify(targetScope); if (flights.current.has(marker)) return;
      flight(marker, true); ownsFlight = true; invalidate(); current = capture(); setVersion(v => v + 1);
      const { intent, first } = prepareIntent(localStorage, targetScope, "turn", { text: draft, expected_revision: session.revision }); refreshPending();
      await performIntent(localStorage, intent, apiKey, first, value => { if (!current()) return false; select(intent.workspace_id, intent.session_id!, value.id); saveDraft(localStorage, targetScope, ""); setDraft(""); return true; });
    } catch (error) { if (current()) setNotice(chatReason(error)); }
    finally { if (marker && ownsFlight) flight(marker, false); refreshPending(); }
  }
  async function recover(intent: Intent) {
    const marker = "find:" + intent.id; if (flights.current.has(marker)) return;
    let current = () => false;
    try {
      const who = scope(); if (intent.principal_id !== who.principal_id) throw new Error("请连接原身份");
      select(intent.workspace_id, intent.session_id ?? ""); current = capture(); flight(marker, true);
      await performIntent(localStorage, intent, apiKey, false, value => { if (!current()) return false; select(intent.workspace_id, intent.kind === "session" ? value.id : intent.session_id!, intent.kind === "turn" ? value.id : null); if (intent.kind === "turn") { saveDraft(localStorage, intent, ""); setDraft(""); } return true; });
    } catch (error) { if (current()) setNotice(chatReason(error)); }
    finally { flight(marker, false); refreshPending(); }
  }
  async function control(turn: Turn | null, action: "cancel" | "close" | "resume" | "approval", approval?: Record<string, unknown>, allow?: boolean) {
    const marker = action + ":" + (approval?.request_id ?? turn?.id ?? id); if (flights.current.has(marker)) return;
    let current = () => false;
    try {
      if (!session) return; const targetScope = scope(id); let cacheKey = JSON.stringify({ ...targetScope, marker });
      flight(marker, true); invalidate(); current = capture(); setVersion(v => v + 1);
      const values = controls();
      const expectedTarget = action === "approval" ? turnPath(workspace, id, turn!.id) + "/approvals/" + encodeURIComponent(String(approval!.request_id)) : action === "cancel" ? turnPath(workspace, id, turn!.id) + "/cancel" : sessionPath(workspace) + "/" + encodeURIComponent(id) + "/" + action;
      const previous = Object.entries(values).find(([, value]) => value.principal_id === targetScope.principal_id && value.workspace_id === workspace && value.session_id === id && value.target === expectedTarget);
      if (previous) cacheKey = previous[0];
      let saved = previous?.[1] ?? values[cacheKey], first = saved === undefined;
      if (!saved) {
        const path = action === "approval" ? turnPath(workspace, id, turn!.id) + "/approvals/" + encodeURIComponent(String(approval!.request_id)) : action === "cancel" ? turnPath(workspace, id, turn!.id) + "/cancel" : sessionPath(workspace) + "/" + encodeURIComponent(id) + "/" + action;
        const body = action === "approval" ? { request_key: crypto.randomUUID(), call_id: approval!.call_id, arguments_hash: approval!.arguments_hash, tool_hash: approval!.tool_hash, allow } : { request_key: crypto.randomUUID(), expected_revision: session.revision };
        saved = { ...targetScope, target: path, turn_id: turn?.id ?? "", body, rejected: false }; values[cacheKey] = saved; saveControls(values);
      }
      first = first || saved.rejected;
      let raw: unknown;
      try { raw = await request(first ? saved.target : sessionPath(saved.workspace_id) + "/" + encodeURIComponent(saved.session_id!) + "/controls/by-key/" + encodeURIComponent(String(saved.body.request_key)), apiKey, first ? saved.body : undefined); }
      catch (error) { if (first && error instanceof RequestError && error.status === 401 && error.code === "unauthorized") { saved.rejected = true; values[cacheKey] = saved; saveControls(values); } throw error; }
      const value = object(raw); if (value.session_id !== saved.session_id || value.request_key !== saved.body.request_key) throw new Error("控制身份不匹配");
      if (current() && value.status === "completed") { const latest = controls(); delete latest[cacheKey]; saveControls(latest); setVersion(v => v + 1); setNotice(null); }
    } catch (error) { if (current()) setNotice(chatReason(error)); }
    finally { flight(marker, false); }
  }
  async function loadDetails(turn: Turn) {
    const current = capture();
    try {
      const path = turnPath(workspace, id, turn.id); const raw = object(await request(path + "/trace", apiKey));
      if (!current()) return; if (raw.turn_id !== turn.id || raw.session_id !== id || object(raw.trace).run_id !== turn.run_id) throw new Error("Trace身份不匹配");
      let facts: Record<string, unknown> | null = null;
      if (!["queued", "running"].includes(turn.status)) { facts = object(await request(path + "/result", apiKey)); if (!current()) return; if (object(facts.run).id !== turn.run_id) throw new Error("结果身份不匹配"); }
      setDetails(value => ({ ...value, [turn.id]: { facts, trace: object(raw.trace), events: [] } }));
    } catch (error) { if (current()) setNotice(chatReason(error)); }
  }
  async function recoverControl(saved: ControlIntent) {
    const marker = "control-find:" + String(saved.body.request_key); if (flights.current.has(marker)) return;
    let current = () => false;
    try {
      const actorScope = scope(); if (saved.principal_id !== actorScope.principal_id) throw new Error("请连接原身份");
      select(saved.workspace_id, saved.session_id!, saved.turn_id || null); current = capture(); flight(marker, true);
      const raw = object(await request(saved.rejected ? saved.target : sessionPath(saved.workspace_id) + "/" + encodeURIComponent(saved.session_id!) + "/controls/by-key/" + encodeURIComponent(String(saved.body.request_key)), apiKey, saved.rejected ? saved.body : undefined));
      if (!current()) return; if (raw.session_id !== saved.session_id || raw.request_key !== saved.body.request_key) throw new Error("控制身份不匹配");
      if (raw.status === "completed") { const values = controls(); for (const [key, value] of Object.entries(values)) if (value.body.request_key === saved.body.request_key && value.session_id === saved.session_id) delete values[key]; saveControls(values); setVersion(v => v + 1); }
    } catch (error) { if (current()) setNotice(chatReason(error)); }
    finally { flight(marker, false); }
  }
  const visiblePending = ready ? pendingState.values.filter(i => i.principal_id === actor.value.principal_id && actor.value.workspaces.some(w => w.workspace_id === i.workspace_id)) : [];
  const turnPending = visiblePending.find(i => i.kind === "turn" && i.workspace_id === workspace && i.session_id === id);
  const newPending = visiblePending.find(i => i.kind === "session" && i.workspace_id === workspace);
  const currentTurn = history.find(t => t.status === "running") ?? history.find(t => t.status === "queued") ?? history.at(-1) ?? null;
  let pendingControls: ControlIntent[] = []; try { pendingControls = ready ? Object.values(controls()).filter(i => i.principal_id === actor.value.principal_id && i.workspace_id === workspace) : []; } catch { /* 控制事件会明确报告损坏，不能重新POST。 */ }
  const controlling = flights.current.has("close:" + id) || flights.current.has("resume:" + id) || history.some(t => flights.current.has("cancel:" + t.id)) || approvals.some(a => flights.current.has("approval:" + String(a.request_id)));
  const posting = (i: Intent) => flights.current.has("post:" + JSON.stringify({ principal_id: i.principal_id, generation: i.generation, workspace_id: i.workspace_id, session_id: i.session_id }));
  return { actor: ready ? actor.value : null, workspace, id, list: ready ? list : [], session: ready && session?.id === id ? session : null, history: ready ? history : [], details: ready ? details : {}, approvals: ready ? approvals : [], events: ready ? events : [], currentTurn: ready ? currentTurn : null, draft, notice: notice ?? (pendingState.error || null), reading, ready, canWrite: !!membership && membership.role !== "viewer", canApprove: !!membership?.approve, pending: visiblePending, pendingControls, turnPending, newPending, sending: ready && flights.current.has("post:" + JSON.stringify(scope(id))), creating: ready && flights.current.has("post:" + JSON.stringify(scope(null))), busy: flights.current.size > 0, controlling, posting, edit, create, submit, recover, recoverControl, control, loadDetails, select, refresh: () => { invalidate(); setIdentityVersion(v => v + 1); setVersion(v => v + 1); refreshPending(); } };
}
export type SessionState = ReturnType<typeof useSession>;
