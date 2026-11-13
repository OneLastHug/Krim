import { useEffect, useRef, useState } from "react";
import { object, request, RequestError } from "../api";
import { chatReason } from "../lib/copy";
import { parseActor, parseSession, parseTurn, sessionPath, turnPath, type Actor, type Session, type Turn } from "../lib/session-contracts";
import { CURRENT, performIntent, prepareIntent, readDraft, readIntents, saveDraft, saveSelection, type Intent, type Scope } from "../lib/session-requests";

interface Detail { facts: Record<string, unknown> | null; trace: Record<string, unknown>; events: Record<string, unknown>[]; loadedForStatus?: Turn["status"]; traceLoaded?: boolean }
interface ControlIntent extends Scope { target: string; turn_id: string; body: Record<string, unknown>; rejected: boolean }
const CONTROL = "krim.agent-core.controls/1";
function controls(): Record<string, ControlIntent> {
  const raw = localStorage.getItem(CONTROL), value = object(raw ? JSON.parse(raw) : {});
  for (const item of Object.values(value)) {
    const i = object(item), body = object(i.body);
    if (typeof i.principal_id !== "string" || !Number.isSafeInteger(i.generation) || typeof i.workspace_id !== "string" || typeof i.session_id !== "string" || typeof i.turn_id !== "string" || typeof i.target !== "string" || typeof i.rejected !== "boolean" || typeof body.request_key !== "string") throw new Error("无法读取未确认请求");
    const base = sessionPath(i.workspace_id) + "/" + encodeURIComponent(i.session_id);
    const allowed = [base + "/close", base + "/resume", base + "/rename", turnPath(i.workspace_id, i.session_id, i.turn_id) + "/cancel", base + "/lifecycle/archive", base + "/lifecycle/unarchive", base + "/lifecycle/trash", base + "/lifecycle/restore"];
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
  const [listView, setListView] = useState<"active" | "archived" | "trash">("active");
  const [actor, setActor] = useState<{ key: string; value: Actor } | null>(null);
  const [list, setList] = useState<Session[]>([]), [session, setSession] = useState<Session | null>(null), [history, setHistory] = useState<Turn[]>([]);
  const [draft, setDraft] = useState(""), [notice, setNotice] = useState<string | null>(null), [reading, setReading] = useState(false);
  const [pendingState, setPending] = useState(pending), [details, setDetails] = useState<Record<string, Detail>>({});
  const [approvals, setApprovals] = useState<Record<string, unknown>[]>([]), [events, setEvents] = useState<Record<string, unknown>[]>([]);
  const [turnEvents, setTurnEvents] = useState<Record<string, Record<string, unknown>[]>>({}), [turnFacts, setTurnFacts] = useState<Record<string, Record<string, unknown> | null>>({});
  const [version, setVersion] = useState(0), [identityVersion, setIdentityVersion] = useState(0), [, setFlights] = useState(0);
  const fenceGeneration = fence.current;
  const mounted = useRef(false), epoch = useRef(0), reader = useRef<AbortController | null>(null), flights = useRef(new Set<string>());
  const detailFlights = useRef(new Map<string, { status: Turn["status"]; promise: Promise<void> }>());
  const selected = useRef({ workspace, id }); selected.current = { workspace, id };
  const historyRef = useRef(history); historyRef.current = history;
  const keyRef = useRef(apiKey), activeRef = useRef(active), actorRef = useRef(actor); keyRef.current = apiKey; activeRef.current = active; actorRef.current = actor;
  const ready = actor !== null && actor.key === apiKey;
  const membership = ready ? actor.value.workspaces.find(w => w.workspace_id === workspace) : undefined;
  const scope = (sessionId: string | null = selected.current.id || null, ws = selected.current.workspace): Scope => {
    const value = actorRef.current;
    if (!value || value.key !== keyRef.current) throw new Error("请先连接");
    return { principal_id: value.value.principal_id, generation: value.value.generation, workspace_id: ws, session_id: sessionId };
  };
  function invalidate() { epoch.current++; reader.current?.abort(); }
  function capture() { const e = epoch.current, f = fence.current, key = apiKey, view = { ...selected.current }, identity = actorRef.current ? `${actorRef.current.value.principal_id}:${actorRef.current.value.generation}` : null; return () => mounted.current && activeRef.current && e === epoch.current && f === fence.current && key === keyRef.current && view.id === selected.current.id && view.workspace === selected.current.workspace && identity !== null && identity === (actorRef.current ? `${actorRef.current.value.principal_id}:${actorRef.current.value.generation}` : null); }
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
    selected.current = { workspace: ws, id: sessionId }; setWorkspace(ws); setId(sessionId); setSession(null); setHistory([]); setDetails({}); setTurnEvents({}); setTurnFacts({}); setEvents([]); setApprovals([]); setNotice(null);
    setDraft(readDraft(localStorage, target)); setVersion(v => v + 1);
  }
  function historyReplace(url: URL) { window.history.replaceState(null, "", url); }
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; invalidate(); }; }, []);
  useEffect(() => {
    if (!active) return;
    setActor(null); actorRef.current = null; setList([]); setSession(null); setHistory([]); setDetails({}); setTurnEvents({}); setTurnFacts({}); setApprovals([]); setEvents([]);
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
      let cursor = 0, runId = ""; const timeline: Record<string, unknown>[] = [];
      let activeRun: { turn: Turn; cursor: number; events: Record<string, unknown>[] } | null = null;
      let turns: Turn[] = [], historyLoaded = false, recoveredPartial = false;
      async function readEvents(turn: Turn, start: number, target: Record<string, unknown>[]) {
        let next = start, raw: unknown;
        do {
          raw = await request(turnPath(workspace, id, turn.id) + "/events?from_seq=" + next, apiKey, undefined, controller.signal);
          if (!current()) return null;
          if (!Array.isArray(raw)) throw new Error("事件无效");
          for (const item of raw) { const event = object(item); if (event.run_id !== turn.run_id || event.seq !== next) throw new Error("事件身份不匹配"); target.push(event); next++; }
        } while ((raw as unknown[]).length === 64);
        return next;
      }
      async function readFacts(turn: Turn) {
        const value = object(await request(turnPath(workspace, id, turn.id) + "/result", apiKey, undefined, controller.signal));
        if (!current()) return null;
        if (object(value.run).id !== turn.run_id) throw new Error("结果身份不匹配");
        return value;
      }
      function keepRunData(turn: Turn, runEvents: Record<string, unknown>[], facts?: Record<string, unknown>) {
        const capturedEvents = [...runEvents];
        setTurnEvents(previous => ({ ...previous, [turn.id]: capturedEvents }));
        if (facts) setTurnFacts(previous => ({ ...previous, [turn.id]: facts }));
        setDetails(previous => {
          const prior = previous[turn.id];
          return prior ? { ...previous, [turn.id]: { ...prior, events: capturedEvents, traceLoaded: prior.loadedForStatus === turn.status && prior.traceLoaded === true, ...(facts ? { facts, loadedForStatus: turn.status } : {}) } } : previous;
        });
      }
      async function hydrateFinished(turn: Turn, runEvents: Record<string, unknown>[], start: number) {
        try {
          const end = await readEvents(turn, start, runEvents);
          if (end === null) return false;
          const facts = await readFacts(turn);
          if (facts === null) return false;
          keepRunData(turn, runEvents, facts);
        } catch (error) {
          if (!current()) return false;
          if (error instanceof RequestError && (error.status === 401 || error.status === 403)) throw error;
          keepRunData(turn, runEvents);
          setNotice("部分运行记录暂时无法读取，可展开运行详情重试");
        }
        return true;
      }
      try {
        const suffix = listView === "active" ? "" : listView === "trash" ? "/lifecycle/trash" : "/lifecycle/archived";
        const rows = await request(sessionPath(workspace) + suffix + "?limit=50", apiKey, undefined, controller.signal);
        if (!current()) return; if (!Array.isArray(rows)) throw new Error("对话列表无效");
        const sessions = rows.map(parseSession); if (sessions.some(s => s.workspace_id !== workspace)) throw new Error("对话身份不匹配"); setList(sessions);
        if (!id) { setSession(null); setHistory([]); return; }
        while (current()) {
          const value = parseSession(await request(sessionPath(workspace) + "/" + encodeURIComponent(id), apiKey, undefined, controller.signal));
          if (!current()) return; if (value.id !== id || value.workspace_id !== workspace) throw new Error("对话身份不匹配");
          const firstMutable = historyLoaded ? turns.findIndex(t => t.status === "queued" || t.status === "running") : 0;
          if (historyLoaded && firstMutable < 0) break;
          const offset = historyLoaded ? Math.floor(firstMutable / 50) * 50 : 0;
          const refreshed = historyLoaded ? turns.slice(0, offset) : [];
          for (let pageOffset = offset; pageOffset <= 1024; pageOffset += 50) {
            const raw = await request(sessionPath(workspace) + "/" + encodeURIComponent(id) + `/turns?offset=${pageOffset}&limit=50`, apiKey, undefined, controller.signal);
            if (!current()) return; if (!Array.isArray(raw)) throw new Error("历史无效");
            const page = raw.map(parseTurn); if (page.some(t => t.session_id !== id || t.workspace_id !== workspace)) throw new Error("历史身份不匹配"); refreshed.push(...page);
            if (page.length < 50) break;
            if (pageOffset === 1000) throw new Error("历史容量超限");
          }
          if (new Set(refreshed.map(t => t.id)).size !== refreshed.length) throw new Error("历史重复");
          turns = refreshed; historyLoaded = true;
          setSession(value); setHistory(turns); setReading(false);
          const running = turns.find(t => t.status === "running");
          if (!recoveredPartial) {
            const recentPartial = [...turns].reverse().find(t => ["failed", "cancelled", "interrupted"].includes(t.status) && !!t.run_id);
            if (recentPartial && recentPartial.id !== running?.id) {
              const partialEvents: Record<string, unknown>[] = [];
              if (!await hydrateFinished(recentPartial, partialEvents, 0)) return;
              recoveredPartial = true;
              if (!running) setEvents([...partialEvents]);
            }
          }
          if (activeRun) {
            const finished = turns.find(t => t.id === activeRun!.turn.id && t.run_id === activeRun!.turn.run_id && t.status !== "running" && t.status !== "queued");
            if (finished) {
              if (!await hydrateFinished(finished, activeRun.events, activeRun.cursor)) return;
              setEvents([...activeRun.events]); setApprovals([]); activeRun = null; recoveredPartial = true;
            }
          }
          if (running?.run_id) {
            if (!activeRun || activeRun.turn.id !== running.id || activeRun.turn.run_id !== running.run_id) {
              runId = running.run_id; cursor = 0; timeline.length = 0;
              activeRun = { turn: running, cursor: 0, events: timeline };
            } else { activeRun.turn = running; runId = running.run_id; cursor = activeRun.cursor; }
            const end = await readEvents(running, cursor, timeline);
            if (end === null) return;
            cursor = end; activeRun.cursor = end;
            const pendingRaw = await request(turnPath(workspace, id, running.id) + "/approvals", apiKey, undefined, controller.signal);
            if (!current()) return; if (!Array.isArray(pendingRaw)) throw new Error("审批无效");
            const pendingApprovals = pendingRaw.map(object);
            if (pendingApprovals.some(item => item.run_id !== runId)) throw new Error("审批身份不匹配");
            const capturedEvents = [...timeline];
            setEvents(capturedEvents); setTurnEvents(previous => ({ ...previous, [running.id]: capturedEvents })); setApprovals(pendingApprovals);
          } else if (!activeRun) {
            setApprovals([]);
            if (!turns.some(t => t.status === "failed" || t.status === "cancelled" || t.status === "interrupted") && !turns.some(t => t.status === "running")) setEvents([]);
          }
          if (!turns.some(t => t.status === "running" || t.status === "queued") || value.dispatch_paused && !running) break;
          await delay(controller.signal);
        }
      } catch (error) { if (current()) setNotice(chatReason(error)); }
      finally { if (current()) setReading(false); }
    }
    void load(); return () => controller.abort();
  }, [apiKey, active, ready, actor, workspace, id, version, fenceGeneration]);
  function setSessionListView(view: "active" | "archived" | "trash") {
    if (view === listView) return;
    invalidate();
    setList([]);
    setListView(view);
    setVersion(value => value + 1);
  }
  async function lifecycle(action: "archive" | "unarchive" | "trash" | "restore", sessionId = id) {
    if (!ready || !membership || membership.role === "viewer" || !sessionId) return;
    const selectedSession = list.find(item => item.id === sessionId)
      ?? (session?.id === sessionId ? session : null);
    if (!selectedSession) return;
    const marker = "lifecycle:" + sessionId;
    if (flights.current.has(marker)) return;
    const identity = actorRef.current?.value;
    if (!identity || selectedSession.owner_id !== identity.principal_id) return;
    flight(marker, true);
    const intentKey = "lifecycle:" + JSON.stringify({
      principal_id: identity.principal_id,
      generation: identity.generation,
      workspace_id: workspace,
      session_id: sessionId,
      action,
    });
    const requestKey = crypto.randomUUID();
    const lifecycleBase = sessionPath(workspace) + "/" + encodeURIComponent(sessionId) + "/lifecycle";
    const target = lifecycleBase + "/" + action;
    const values = controls();
    const existing = values[intentKey];
    const saved: ControlIntent = existing ?? {
      principal_id: identity.principal_id,
      generation: identity.generation,
      workspace_id: workspace,
      session_id: sessionId,
      turn_id: "",
      target,
      body: { request_key: requestKey, expected_revision: selectedSession.lifecycle_revision },
      rejected: false,
    };
    if (!existing) {
      values[intentKey] = saved;
      try { saveControls(values); }
      catch (error) { flight(marker, false); setNotice(chatReason(error)); return; }
    }
    const sameView = () => { const latest = actorRef.current?.value; return mounted.current && !!latest && latest.principal_id === identity.principal_id && latest.generation === identity.generation && selected.current.workspace === workspace; };
    try {
      let raw: unknown = undefined;
      if (existing && !saved.rejected) {
        // 结果未知时先查回执：命中即已生效；404 证明未执行，保留原键提交。
        try { raw = await request(lifecycleBase + "/by-key/" + encodeURIComponent(String(saved.body.request_key)), apiKey); }
        catch (error) { if (!(error instanceof RequestError && error.status === 404)) { if (sameView()) setNotice(chatReason(error)); return; } }
      }
      if (raw === undefined) {
        try {
          raw = await request(saved.rejected ? saved.target : target, apiKey, saved.rejected ? saved.body : {
            request_key: saved.body.request_key,
            expected_revision: saved.body.expected_revision,
          });
        } catch (error) {
          if (error instanceof RequestError && error.status === 401 && error.code === "unauthorized") {
            saved.rejected = true;
            values[intentKey] = saved;
            saveControls(values);
          }
          throw error;
        }
      }
      const updated = parseSession(raw);
      if (updated.id !== sessionId || updated.workspace_id !== workspace) throw new Error("对话身份不匹配");
      if (!sameView()) return;
      const latest = controls();
      delete latest[intentKey];
      saveControls(latest);
      if (action === "trash" && selected.current.id === sessionId) select(workspace, "");
      if (action === "restore" && selected.current.id === sessionId) setSessionListView("active");
      invalidate();
      setNotice(null);
      setVersion(value => value + 1);
    } catch (error) {
      if (error instanceof RequestError && error.status === 409 && error.code === "revision_conflict") {
        const latest = controls();
        if (latest[intentKey]?.body.request_key === saved.body.request_key) delete latest[intentKey];
        saveControls(latest);
        if (sameView()) { invalidate(); setVersion(value => value + 1); setNotice(chatReason(error)); }
        return;
      }
      if (sameView()) setNotice(chatReason(error));
    } finally {
      flight(marker, false);
      refreshPending();
    }
  }
  function edit(text: string) { try { saveDraft(localStorage, scope(), text); setDraft(text); } catch (error) { setNotice(chatReason(error)); } }
  async function create(title: string, target: string) {
    let marker = "", ownsFlight = false; let current = () => false;
    try {
      const targetScope = scope(null); marker = "post:" + JSON.stringify(targetScope); if (flights.current.has(marker)) return;
      flight(marker, true); ownsFlight = true; invalidate(); current = capture(); setVersion(v => v + 1); saveDraft(localStorage, targetScope, title);
      const { intent, first } = prepareIntent(localStorage, targetScope, "session", { title, target_key: target }); refreshPending();
      await performIntent(localStorage, intent, apiKey, first, value => { if (!current()) return false; setListView("active"); select(intent.workspace_id, value.id); setSession(value as Session); return true; });
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
      await performIntent(localStorage, intent, apiKey, first, value => { if (!current()) return false; setListView("active"); select(intent.workspace_id, intent.session_id!, value.id); saveDraft(localStorage, targetScope, ""); setDraft(""); return true; });
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
  async function control(turn: Turn | null, action: "cancel" | "close" | "resume" | "approval" | "rename", approval?: Record<string, unknown>, allow?: boolean, title?: string) {
    const who = actorRef.current, member = who?.value.workspaces.find(w => w.workspace_id === workspace);
    if (!activeRef.current || !who || who.key !== keyRef.current || !session || session.id !== id || session.workspace_id !== workspace || selected.current.id !== id || selected.current.workspace !== workspace) return;
    const canWriteNow = !!member && member.role !== "viewer";
    const canApproveNow = canWriteNow && member.approve;
    if (action === "rename") {
      if (!canWriteNow || typeof title !== "string" || !title.trim() || title.trim().length > 128 || session.status !== "open") return;
    } else if (action === "approval") {
      if (!canApproveNow || !turn || !turn.run_id || typeof allow !== "boolean" || turn.status !== "running" || turn.session_id !== id || turn.workspace_id !== workspace || !history.some(item => item.id === turn.id && item.run_id === turn.run_id && item.status === "running") || !approval) return;
      const stillPending = approvals.some(item => item.request_id === approval.request_id && item.run_id === turn.run_id && item.call_id === approval.call_id && item.arguments_hash === approval.arguments_hash && item.tool_hash === approval.tool_hash);
      if (!stillPending) return;
    } else if (!canWriteNow) return;
    if (action === "cancel" && (!turn || turn.session_id !== id || turn.workspace_id !== workspace || !["queued", "running"].includes(turn.status) || !history.some(item => item.id === turn.id && item.status === turn.status))) return;
    if (action === "close" && session.status !== "open" || action === "resume" && session.status !== "closed") return;
    const marker = action + ":" + (approval?.request_id ?? turn?.id ?? id); if (flights.current.has(marker)) return;
    let current = () => false;
    try {
      const targetScope = scope(id); let cacheKey = JSON.stringify({ ...targetScope, marker });
      flight(marker, true); invalidate(); current = capture(); setVersion(v => v + 1);
      const values = controls(), sessionTarget = sessionPath(workspace) + "/" + encodeURIComponent(id);
      const expectedTarget = action === "approval" ? turnPath(workspace, id, turn!.id) + "/approvals/" + encodeURIComponent(String(approval!.request_id)) : action === "cancel" ? turnPath(workspace, id, turn!.id) + "/cancel" : sessionTarget + "/" + action;
      const previous = Object.entries(values).find(([, value]) => value.principal_id === targetScope.principal_id && value.generation === targetScope.generation && value.workspace_id === workspace && value.session_id === id && value.target === expectedTarget);
      if (previous) cacheKey = previous[0];
      let saved = previous?.[1] ?? values[cacheKey], first = saved === undefined;
      if (!saved) {
        const path = expectedTarget;
        const body = action === "approval" ? { request_key: crypto.randomUUID(), call_id: approval!.call_id, arguments_hash: approval!.arguments_hash, tool_hash: approval!.tool_hash, allow } : { request_key: crypto.randomUUID(), expected_revision: session.revision, ...(action === "rename" ? { title: title!.trim() } : {}) };
        saved = { ...targetScope, target: path, turn_id: turn?.id ?? "", body, rejected: false }; values[cacheKey] = saved; saveControls(values);
      }
      first = first || saved.rejected;
      let raw: unknown;
      try { raw = await request(first ? saved.target : sessionPath(saved.workspace_id) + "/" + encodeURIComponent(saved.session_id!) + "/controls/by-key/" + encodeURIComponent(String(saved.body.request_key)), apiKey, first ? saved.body : undefined); }
      catch (error) {
        if (first && error instanceof RequestError && error.status === 401 && error.code === "unauthorized") { saved.rejected = true; values[cacheKey] = saved; saveControls(values); }
        if (first && error instanceof RequestError && error.status === 409 && error.code === "revision_conflict") {
          const latest = controls();
          if (latest[cacheKey]?.body.request_key === saved.body.request_key) delete latest[cacheKey];
          saveControls(latest); refreshPending();
          if (current()) setNotice("对话已更新；本次操作未执行。请刷新检查状态后重新操作。");
          return;
        }
        throw error;
      }
      const value = object(raw); if (value.session_id !== saved.session_id || value.request_key !== saved.body.request_key) throw new Error("控制身份不匹配");
      if (current() && value.status === "completed") { const latest = controls(); delete latest[cacheKey]; saveControls(latest); setVersion(v => v + 1); setNotice(null); }
    } catch (error) { if (current()) setNotice(chatReason(error)); }
    finally { flight(marker, false); }
  }
  async function rename(title: string) { await control(null, "rename", undefined, undefined, title); }
  function loadDetails(turn: Turn): Promise<void> {
    const identity = actorRef.current?.value;
    if (!identity || identity.principal_id !== actor?.value.principal_id || turn.session_id !== id || turn.workspace_id !== workspace || !turn.run_id) return Promise.resolve();
    const stamp = JSON.stringify([identity.principal_id, identity.generation, workspace, id, turn.id, turn.run_id]);
    const cached = details[turn.id];
    if (cached?.loadedForStatus === turn.status && cached.traceLoaded === true) return Promise.resolve();
    const active = detailFlights.current.get(stamp);
    if (active?.status === turn.status) return active.promise;
    const sameScope = capture();
    const current = () => sameScope() && historyRef.current.some(value => value.id === turn.id && value.run_id === turn.run_id && value.status === turn.status);
    let promise!: Promise<void>;
    promise = (async () => {
      try {
        const path = turnPath(workspace, id, turn.id);
        const [rawTrace] = await Promise.all([request(path + "/trace", apiKey)]);
        if (!current()) return;
        const traceEnvelope = object(rawTrace), trace = object(traceEnvelope.trace);
        if (traceEnvelope.turn_id !== turn.id || traceEnvelope.session_id !== id || trace.run_id !== turn.run_id) throw new Error("Trace身份不匹配");
        const runEvents = [...(turnEvents[turn.id] ?? cached?.events ?? [])];
        let cursor = runEvents.length, raw: unknown;
        do {
          raw = await request(path + "/events?from_seq=" + cursor, apiKey);
          if (!current()) return;
          if (!Array.isArray(raw)) throw new Error("事件无效");
          for (const item of raw) { const event = object(item); if (event.run_id !== turn.run_id || event.seq !== cursor) throw new Error("事件身份不匹配"); runEvents.push(event); cursor++; }
        } while ((raw as unknown[]).length === 64);
        let facts = turnFacts[turn.id] ?? cached?.facts ?? null;
        if (facts && (facts.run === null || typeof facts.run !== "object" || Array.isArray(facts.run) || object(facts.run).id !== turn.run_id)) facts = null;
        if (!["queued", "running"].includes(turn.status) && !facts) {
          facts = object(await request(path + "/result", apiKey));
          if (!current()) return;
          if (object(facts.run).id !== turn.run_id) throw new Error("结果身份不匹配");
        }
        if (!current()) return;
        setTurnEvents(previous => ({ ...previous, [turn.id]: [...runEvents!] }));
        if (facts) setTurnFacts(previous => ({ ...previous, [turn.id]: facts }));
        setDetails(previous => ({ ...previous, [turn.id]: { facts, trace, events: [...runEvents!], loadedForStatus: turn.status, traceLoaded: true } }));
      } catch (error) { if (current()) setNotice(chatReason(error)); }
    })().finally(() => {
      if (detailFlights.current.get(stamp)?.promise === promise) detailFlights.current.delete(stamp);
    });
    detailFlights.current.set(stamp, { status: turn.status, promise });
    return promise;
  }
  async function recoverControl(saved: ControlIntent) {
    const marker = "control-find:" + String(saved.body.request_key); if (flights.current.has(marker)) return;
    let current = () => false;
    try {
      const actorScope = scope(); if (saved.principal_id !== actorScope.principal_id) throw new Error("请连接原身份");
      select(saved.workspace_id, saved.session_id!, saved.turn_id || null); current = capture(); flight(marker, true);
      const isLifecycle = saved.target.includes("/lifecycle/");
      const raw = await request(saved.rejected ? saved.target : sessionPath(saved.workspace_id) + "/" + encodeURIComponent(saved.session_id!) + (isLifecycle ? "/lifecycle/by-key/" : "/controls/by-key/") + encodeURIComponent(String(saved.body.request_key)), apiKey, saved.rejected ? saved.body : undefined);
      if (!current()) return;
      if (isLifecycle) {
        // lifecycle回执即SessionView：能查到即已生效；404保留原键等待安全重试。
        const value = parseSession(raw);
        if (value.id !== saved.session_id || value.workspace_id !== saved.workspace_id) throw new Error("对话身份不匹配");
        const values = controls(); for (const [key, item] of Object.entries(values)) if (item.body.request_key === saved.body.request_key && item.session_id === saved.session_id) delete values[key]; saveControls(values); setVersion(v => v + 1);
        return;
      }
      const control = object(raw); if (control.session_id !== saved.session_id || control.request_key !== saved.body.request_key) throw new Error("控制身份不匹配");
      if (control.status === "completed") { const values = controls(); for (const [key, value] of Object.entries(values)) if (value.body.request_key === saved.body.request_key && value.session_id === saved.session_id) delete values[key]; saveControls(values); setVersion(v => v + 1); }
    } catch (error) { if (current()) setNotice(chatReason(error)); }
    finally { flight(marker, false); }
  }
  const visiblePending = ready ? pendingState.values.filter(i => i.principal_id === actor.value.principal_id && actor.value.workspaces.some(w => w.workspace_id === i.workspace_id)) : [];
  const turnPending = visiblePending.find(i => i.kind === "turn" && i.workspace_id === workspace && i.session_id === id);
  const newPending = visiblePending.find(i => i.kind === "session" && i.workspace_id === workspace);
  const currentTurn = history.find(t => t.status === "running") ?? history.find(t => t.status === "queued") ?? history.at(-1) ?? null;
  let pendingControls: ControlIntent[] = []; try { pendingControls = ready ? Object.values(controls()).filter(i => i.principal_id === actor.value.principal_id && i.workspace_id === workspace) : []; } catch { /* 控制事件会明确报告损坏，不能重新POST。 */ }
  const controlling = flights.current.has("close:" + id) || flights.current.has("resume:" + id) || flights.current.has("rename:" + id) || flights.current.has("lifecycle:" + id) || history.some(t => flights.current.has("cancel:" + t.id)) || approvals.some(a => flights.current.has("approval:" + String(a.request_id)));
  const posting = (i: Intent) => flights.current.has("post:" + JSON.stringify({ principal_id: i.principal_id, generation: i.generation, workspace_id: i.workspace_id, session_id: i.session_id }));
  return { actor: ready ? actor.value : null, workspace, id, list: ready ? list : [], listView, setSessionListView, lifecycle, session: ready && session?.id === id ? session : null, history: ready ? history : [], details: ready ? details : {}, turnEvents: ready ? turnEvents : {}, turnFacts: ready ? turnFacts : {}, approvals: ready ? approvals : [], events: ready ? events : [], currentTurn: ready ? currentTurn : null, draft, notice: notice ?? (pendingState.error || null), reading, ready, canWrite: !!membership && membership.role !== "viewer", canApprove: !!membership && membership.role !== "viewer" && membership.approve, pending: visiblePending, pendingControls, turnPending, newPending, sending: ready && flights.current.has("post:" + JSON.stringify(scope(id))), creating: ready && flights.current.has("post:" + JSON.stringify(scope(null))), busy: flights.current.size > 0, controlling, posting, edit, create, submit, recover, recoverControl, control, rename, loadDetails, select, refresh: () => { invalidate(); setIdentityVersion(v => v + 1); setVersion(v => v + 1); refreshPending(); } };
}
export type SessionState = ReturnType<typeof useSession>;
