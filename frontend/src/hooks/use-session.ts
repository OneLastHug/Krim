import { useEffect, useRef, useState } from "react";
import { object, request, RequestError } from "../api";
import { chatReason } from "../lib/copy";
import type { WireSelection } from "../lib/model-gateways";
import { parseActor, parseSession, parseTurn, sessionPath, turnPath, type Actor, type Session, type Turn } from "../lib/session-contracts";
import { CURRENT, performIntent, prepareIntent, readDraft, readIntents, saveDraft, saveSelection, type Intent, type Scope } from "../lib/session-requests";
import { readTurnStream, StreamProtocolError, StreamUnavailableError, type RunStreamEvent } from "../lib/session-stream";
import { useExecutionPolicy } from "./use-execution-policy";
import type { ApprovalScope } from "../lib/execution-policy";

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
function delay(signal: AbortSignal, milliseconds = 300) { return new Promise<void>(resolve => { if (signal.aborted) { resolve(); return; } const finish = () => { clearTimeout(timer); signal.removeEventListener("abort", finish); resolve(); }; const timer = setTimeout(finish, milliseconds); signal.addEventListener("abort", finish, { once: true }); }); }
function sameValue(left: unknown, right: unknown) { return left === right || JSON.stringify(left) === JSON.stringify(right); }
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
  const turnEventsRef = useRef(turnEvents); turnEventsRef.current = turnEvents;
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
    // A turn receipt refreshes the current session; retain its transcript until reload.
    if (selected.current.workspace !== ws || selected.current.id !== sessionId) {
      setSession(null); setHistory([]); setDetails({}); setTurnEvents({}); setTurnFacts({}); setEvents([]); setApprovals([]);
    }
    selected.current = { workspace: ws, id: sessionId }; setWorkspace(ws); setId(sessionId); setNotice(null);
    setDraft(readDraft(localStorage, target)); setVersion(v => v + 1);
  }
  function historyReplace(url: URL) { window.history.replaceState(null, "", url); }
  useEffect(() => {
    const restore = () => {
      const url = new URL(location.href);
      if (url.searchParams.get("view") && url.searchParams.get("view") !== "chat") return;
      const ws = url.searchParams.get("workspace") ?? selected.current.workspace;
      const sid = url.searchParams.get("session") ?? "";
      if (!actorRef.current?.value.workspaces.some(item => item.workspace_id === ws)) return;
      if (ws !== selected.current.workspace || sid !== selected.current.id) select(ws, sid, url.searchParams.get("turn"));
    };
    window.addEventListener("popstate", restore);
    return () => window.removeEventListener("popstate", restore);
  }, [apiKey]);
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; invalidate(); }; }, []);
  useEffect(() => {
    const refreshIdentity = () => setIdentityVersion(value => value + 1);
    window.addEventListener("krim:model-gateways-updated", refreshIdentity);
    return () => window.removeEventListener("krim:model-gateways-updated", refreshIdentity);
  }, []);
  useEffect(() => {
    if (!active) return;
    if (actorRef.current?.key !== apiKey) {
      setActor(null); actorRef.current = null; setList([]); setSession(null); setHistory([]); setDetails({}); setTurnEvents({}); setTurnFacts({}); setApprovals([]); setEvents([]);
    }
    const controller = new AbortController(), f = fence.current, key = apiKey;
    const current = () => mounted.current && activeRef.current && !controller.signal.aborted && key === keyRef.current && f === fence.current;
    void request("/v2/me", key, undefined, controller.signal).then(raw => {
      if (!current()) return;
      const value = parseActor(raw); if (!value.workspaces.length) throw new Error("没有可用工作区");
      const route = new URL(location.href);
      let ws = route.searchParams.get("workspace") ?? selected.current.workspace;
      let sid = route.searchParams.get("session") ?? selected.current.id;
      if (!value.workspaces.some(w => w.workspace_id === ws)) { ws = value.workspaces[0]!.workspace_id; sid = ""; }
      if (!sid) {
        try { const stored = JSON.parse(localStorage.getItem(CURRENT) ?? "null") as (Scope & { turn_id: string | null }) | null; if (stored && stored.principal_id === value.principal_id && stored.generation === value.generation && stored.workspace_id === ws) sid = stored.session_id ?? ""; } catch { /* 原创建intent由独立严格账本保护。 */ }
      }
      const previous = actorRef.current?.value;
      if (previous && (previous.principal_id !== value.principal_id || previous.generation !== value.generation || ws !== selected.current.workspace || JSON.stringify(previous.workspaces) !== JSON.stringify(value.workspaces))) {
        setList([]); setSession(null); setHistory([]); setDetails({}); setTurnEvents({}); setTurnFacts({}); setApprovals([]); setEvents([]);
      }
      actorRef.current = { key, value }; selected.current = { workspace: ws, id: sid }; setActor({ key, value }); setWorkspace(ws); setId(sid); onLegacy(value.legacy);
      setDraft(readDraft(localStorage, { principal_id: value.principal_id, generation: value.generation, workspace_id: ws, session_id: sid || null })); refreshPending(); setNotice(null);
    }).catch(error => {
      if (!current()) return;
      if (error instanceof RequestError && (error.status === 401 || error.status === 403)) {
        actorRef.current = null; setActor(null); setList([]); setSession(null); setHistory([]); setDetails({}); setTurnEvents({}); setTurnFacts({}); setApprovals([]); setEvents([]); setDraft("");
      }
      setNotice(chatReason(error));
    });
    return () => controller.abort();
  }, [apiKey, active, identityVersion, fenceGeneration]);
  useEffect(() => {
    if (!active || !ready || !workspace) return;
    const controller = new AbortController(); reader.current = controller;
    const check = capture(), current = () => check() && !controller.signal.aborted;
    type LiveRun = {
      turn: Turn; cursor: number; events: Record<string, unknown>[]; published: number;
      controller: AbortController; frame: (() => void) | null; terminal: boolean;
      fallback: boolean; fallbackRead: Promise<number | null> | null; approvalsLoading: boolean; approvalsDirty: boolean;
    };
    let live: LiveRun | null = null, failure: unknown;
    let wake: (() => void) | null = null, refreshRequested = false;
    const clearApprovals = () => setApprovals(previous => previous.length ? [] : previous);
    function wakeMetadata() { refreshRequested = true; wake?.(); }
    function waitForMetadata(milliseconds: number) {
      if (refreshRequested || !current()) { refreshRequested = false; return Promise.resolve(); }
      return new Promise<void>(resolve => {
        const finish = () => { clearTimeout(timer); controller.signal.removeEventListener("abort", finish); if (wake === finish) wake = null; refreshRequested = false; resolve(); };
        const timer = setTimeout(finish, milliseconds);
        wake = finish; controller.signal.addEventListener("abort", finish, { once: true });
      });
    }
    function owns(run: LiveRun) { return current() && live === run && !run.controller.signal.aborted; }
    function publish(run: LiveRun) {
      run.frame?.(); run.frame = null;
      if (!owns(run) || run.published === run.events.length) return;
      const captured = [...run.events]; run.published = captured.length;
      setEvents(captured);
      setTurnEvents(previous => ({ ...previous, [run.turn.id]: captured }));
    }
    function schedulePublish(run: LiveRun) {
      if (!owns(run) || run.frame) return;
      if (typeof requestAnimationFrame === "function") {
        let committed = false;
        const commit = () => {
          if (committed) return;
          committed = true; cancelAnimationFrame(frame); clearTimeout(timer);
          run.frame = null; publish(run);
        };
        const frame = requestAnimationFrame(commit);
        // Background/occluded views can suspend animation frames while fetch keeps reading.
        const timer = setTimeout(commit, 50);
        run.frame = () => { committed = true; cancelAnimationFrame(frame); clearTimeout(timer); };
      } else {
        const timer = setTimeout(() => { run.frame = null; publish(run); }, 16);
        run.frame = () => clearTimeout(timer);
      }
    }
    function stopRun(run: LiveRun) { run.frame?.(); run.frame = null; run.controller.abort(); }
    function fail(error: unknown) {
      if (!current() || failure !== undefined) return;
      failure = error; setNotice(chatReason(error));
      if (live) { publish(live); stopRun(live); } wakeMetadata();
    }
    function refreshApprovals(run: LiveRun) {
      if (!owns(run) || run.terminal) return;
      run.approvalsDirty = true;
      if (run.approvalsLoading) return;
      run.approvalsLoading = true;
      void (async () => {
        try {
          while (owns(run) && !run.terminal && run.approvalsDirty) {
            run.approvalsDirty = false;
            const raw = await request(turnPath(workspace, id, run.turn.id) + "/approvals", apiKey, undefined, run.controller.signal);
            if (!owns(run) || run.terminal) return;
            if (!Array.isArray(raw)) throw new Error("审批无效");
            const pendingApprovals = raw.map(object);
            if (pendingApprovals.some(item => item.run_id !== run.turn.run_id)) throw new Error("审批身份不匹配");
            setApprovals(previous => sameValue(previous, pendingApprovals) ? previous : pendingApprovals);
          }
        } catch (error) {
          if (!owns(run) || run.terminal) return;
          if (error instanceof RequestError && (error.status === 401 || error.status === 403)) fail(error);
          else setNotice(chatReason(error));
        } finally { run.approvalsLoading = false; }
      })();
    }
    function receive(run: LiveRun, event: RunStreamEvent) {
      if (!owns(run)) return;
      if (event.run_id !== run.turn.run_id || event.seq !== run.cursor) throw new StreamProtocolError("事件身份不匹配");
      run.events.push(event); run.cursor++;
      if (event.kind === "terminal") {
        run.terminal = true; publish(run); clearApprovals(); wakeMetadata();
      } else {
        schedulePublish(run);
        if (event.kind === "approval_request" || event.kind === "approval_reply") refreshApprovals(run);
      }
    }
    async function readEvents(turn: Turn, start: number, target: Record<string, unknown>[], signal = controller.signal, onEvent?: (event: RunStreamEvent) => void) {
      let next = start, raw: unknown;
      do {
        raw = await request(turnPath(workspace, id, turn.id) + "/events?from_seq=" + next, apiKey, undefined, signal);
        if (!current() || signal.aborted) return null;
        if (!Array.isArray(raw)) throw new Error("事件无效");
        for (const item of raw) {
          const event = object(item);
          if (event.run_id !== turn.run_id || event.seq !== next || typeof event.kind !== "string") throw new StreamProtocolError("事件身份不匹配");
          if (onEvent) onEvent(event as RunStreamEvent); else target.push(event);
          next++;
        }
      } while ((raw as unknown[]).length === 64);
      return next;
    }
    async function readFallback(run: LiveRun) {
      if (run.fallbackRead) return run.fallbackRead;
      const pending = readEvents(run.turn, run.cursor, run.events, run.controller.signal, event => receive(run, event));
      run.fallbackRead = pending;
      try { return await pending; }
      finally { if (run.fallbackRead === pending) run.fallbackRead = null; }
    }
    async function streamRun(run: LiveRun) {
      let retries = 0;
      try {
        while (owns(run) && !run.terminal) {
          const before = run.cursor;
          try {
            await readTurnStream(turnPath(workspace, id, run.turn.id) + "/stream?from_seq=" + run.cursor, apiKey, run.turn.run_id!, run.cursor, run.controller.signal, event => receive(run, event));
            if (!owns(run) || run.terminal) return;
          } catch (error) {
            if (!owns(run)) return;
            if (error instanceof StreamUnavailableError) {
              run.fallback = true;
              const end = await readFallback(run);
              if (end !== null) publish(run);
              if (owns(run) && !run.terminal) { await delay(run.controller.signal); wakeMetadata(); }
              return;
            }
            if (error instanceof StreamProtocolError || error instanceof RequestError && error.status < 500 && error.status !== 408 && error.status !== 429) throw error;
            if (!(error instanceof TypeError) && !(error instanceof RequestError)) throw error;
          }
          // The accepted cursor survives EOF and transient failures; only reading is retried.
          retries = run.cursor > before ? 0 : retries + 1;
          publish(run);
          await delay(run.controller.signal, Math.min(5000, 300 * 2 ** Math.min(retries, 4)));
        }
      } catch (error) { if (owns(run)) fail(error); }
    }
    function startRun(turn: Turn) {
      if (live?.turn.id === turn.id && live.turn.run_id === turn.run_id) { live.turn = turn; refreshApprovals(live); return; }
      if (live) { publish(live); stopRun(live); }
      clearApprovals();
      const cached = turnEventsRef.current[turn.id];
      const events = cached?.every((event, seq) => event.run_id === turn.run_id && event.seq === seq) ? [...cached] : [];
      const run: LiveRun = { turn, cursor: events.length, events, published: -1, controller: new AbortController(), frame: null, terminal: events.at(-1)?.kind === "terminal", fallback: false, fallbackRead: null, approvalsLoading: false, approvalsDirty: false };
      live = run; publish(run);
      // Stream and ancillary reads start independently, so a slow approval GET cannot hold text.
      void streamRun(run); refreshApprovals(run);
    }
    setReading(true);
    async function load() {
      let turns: Turn[] = [], historyLoaded = false, recoveredPartial = false;
      const hydratedFinished = new Set<string>();
      async function readFacts(turn: Turn) {
        const value = object(await request(turnPath(workspace, id, turn.id) + "/result", apiKey, undefined, controller.signal));
        if (!current()) return null;
        if (object(value.run).id !== turn.run_id) throw new Error("结果身份不匹配");
        return value;
      }
      function keepRunData(turn: Turn, runEvents: Record<string, unknown>[], facts?: Record<string, unknown>) {
        const capturedEvents = [...runEvents];
        setTurnEvents(previous => sameValue(previous[turn.id], capturedEvents) ? previous : { ...previous, [turn.id]: capturedEvents });
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
          hydratedFinished.add(turn.id);
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
        const sessions: Session[] = [];
        for (let offset = 0; offset < 10000; offset += 50) {
          const rows = await request(sessionPath(workspace) + suffix + (offset ? `?offset=${offset}&limit=50` : "?limit=50"), apiKey, undefined, controller.signal);
          if (!current()) return; if (!Array.isArray(rows)) throw new Error("对话列表无效");
          const page = rows.map(parseSession);
          if (page.some(s => s.workspace_id !== workspace)) throw new Error("对话身份不匹配");
          sessions.push(...page); if (page.length < 50) break;
          if (offset === 9950) throw new Error("对话列表容量超限");
        }
        if (new Set(sessions.map(s => s.id)).size !== sessions.length) throw new Error("对话列表重复，请刷新");
        setList(previous => sameValue(previous, sessions) ? previous : sessions);
        if (!id) { setSession(null); setHistory(previous => previous.length ? [] : previous); return; }
        while (current()) {
          if (failure !== undefined) throw failure;
          const sessionRequest = request(sessionPath(workspace) + "/" + encodeURIComponent(id), apiKey, undefined, controller.signal);
          const firstMutable = historyLoaded ? turns.findIndex(t => t.status === "queued" || t.status === "running") : 0;
          if (historyLoaded && firstMutable < 0) { await sessionRequest; break; }
          const offset = historyLoaded ? Math.floor(firstMutable / 50) * 50 : 0;
          const refreshed = historyLoaded ? turns.slice(0, offset) : [];
          const historyRequest = (async () => {
            for (let pageOffset = offset; pageOffset <= 1024; pageOffset += 50) {
              const raw = await request(sessionPath(workspace) + "/" + encodeURIComponent(id) + `/turns?offset=${pageOffset}&limit=50`, apiKey, undefined, controller.signal);
              if (!current()) return;
              if (!Array.isArray(raw)) throw new Error("历史无效");
              const page = raw.map(parseTurn);
              if (page.some(t => t.session_id !== id || t.workspace_id !== workspace)) throw new Error("历史身份不匹配");
              refreshed.push(...page); if (page.length < 50) break;
              if (pageOffset === 1000) throw new Error("历史容量超限");
            }
          })();
          const [rawSession] = await Promise.all([sessionRequest, historyRequest]);
          if (!current()) return;
          if (failure !== undefined) throw failure;
          const value = parseSession(rawSession);
          if (value.id !== id || value.workspace_id !== workspace) throw new Error("对话身份不匹配");
          if (new Set(refreshed.map(t => t.id)).size !== refreshed.length) throw new Error("历史重复");
          const prior = new Map(turns.map(turn => [turn.id, turn]));
          const next = refreshed.map(turn => { const old = prior.get(turn.id); return old && sameValue(old, turn) ? old : turn; });
          if (turns.length !== next.length || next.some((turn, index) => turn !== turns[index])) turns = next;
          historyLoaded = true;
          setSession(previous => sameValue(previous, value) ? previous : value);
          setHistory(previous => previous.length === turns.length && previous.every((turn, index) => turn === turns[index]) ? previous : turns);
          setReading(false);
          const running = turns.find(t => t.status === "running" && !!t.run_id);
          const finished = live && turns.find(t => t.id === live!.turn.id && t.run_id === live!.turn.run_id && t.status !== "running" && t.status !== "queued");
          let completedRun: LiveRun | null = null;
          if (finished && live) { completedRun = live; publish(live); stopRun(live); live = null; clearApprovals(); }
          // Attach to the next running turn before slower final-result hydration.
          if (running) startRun(running);
          const fallbackRun = live;
          if (fallbackRun?.fallback && !fallbackRun.terminal) {
            await readFallback(fallbackRun);
          }
          if (completedRun && finished) {
            if (!await hydrateFinished(finished, completedRun.events, completedRun.cursor)) return;
            if (!running) setEvents([...completedRun.events]);
            recoveredPartial = true;
          }
          if (!recoveredPartial) {
            const recentPartial = [...turns].reverse().find(t => ["failed", "cancelled", "interrupted"].includes(t.status) && !!t.run_id);
            if (recentPartial && recentPartial.id !== running?.id) {
              const partialEvents: Record<string, unknown>[] = [];
              if (!await hydrateFinished(recentPartial, partialEvents, 0)) return;
              recoveredPartial = true;
              if (!running) setEvents([...partialEvents]);
            }
          }
          const recentCompleted = [...turns].reverse().find(item => item.status === "completed" && !!item.run_id
            && !!item.transcript?.length && !item.transcript.some(message => message.role === "assistant"));
          if (recentCompleted && !hydratedFinished.has(recentCompleted.id)) {
            const finishedEvents: Record<string, unknown>[] = [];
            if (!await hydrateFinished(recentCompleted, finishedEvents, 0)) return;
          }
          if (!running && !live) {
            clearApprovals();
            if (!turns.some(t => t.status === "failed" || t.status === "cancelled" || t.status === "interrupted")) setEvents(previous => previous.length ? [] : previous);
          }
          if (!turns.some(t => t.status === "running" || t.status === "queued") || value.dispatch_paused && !running) break;
          await waitForMetadata(live && !live.fallback && !live.terminal ? 1500 : 300);
        }
      } catch (error) { if (current()) setNotice(chatReason(error)); }
      finally { if (live) stopRun(live); if (current()) setReading(false); }
    }
    void load();
    return () => { controller.abort(); if (live) stopRun(live); wake?.(); };
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
      // 恢复后会话重新可见，归属由服务端按绑定补齐；通知侧栏重读而不是沿用旧布局。
      if (action === "restore") window.dispatchEvent(new CustomEvent("krim:sidebar-changed", { detail: { principal: identity.principal_id, workspace } }));
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
  async function create(title: string, target: string, projectId?: string, initialDraft = title, modelSelection?: WireSelection): Promise<Session | null | undefined> {
    let marker = "", ownsFlight = false; let current = () => false; let created: Session | null = null;
    try {
      const targetScope = scope(null); marker = "post:" + JSON.stringify(targetScope); if (flights.current.has(marker)) return;
      flight(marker, true); ownsFlight = true; invalidate(); current = capture(); setVersion(v => v + 1); saveDraft(localStorage, targetScope, initialDraft);
      const body = projectId ? { title, target_key: target, project_id: projectId } : { title, target_key: target };
      // 显式选择时随创建冻结模型配置；省略键则沿用账号默认（后端 default_selection）。
      const intentBody = modelSelection === undefined ? body : { ...body, model_selection: modelSelection };
      const { intent, first } = prepareIntent(localStorage, targetScope, "session", intentBody); refreshPending();
      await performIntent(localStorage, intent, apiKey, first, value => { if (!current()) return false; created = value as Session; setListView("active"); select(intent.workspace_id, value.id); setSession(value as Session); return true; });
      // 项目即分组：归属由后端按绑定在 GET 时补齐；这里只通知侧栏重读，不再手工写排序。
      const who = actorRef.current?.value;
      if (created && who && mounted.current && activeRef.current && keyRef.current === apiKey
        && actorRef.current?.value.principal_id === targetScope.principal_id && actorRef.current?.value.generation === targetScope.generation
        && selected.current.workspace === targetScope.workspace_id) {
        window.dispatchEvent(new CustomEvent("krim:sidebar-changed", { detail: { principal: targetScope.principal_id, workspace: targetScope.workspace_id } }));
      }
      return created;
    } catch (error) { if (current()) setNotice(chatReason(error)); return null; }
    finally { if (marker && ownsFlight) flight(marker, false); refreshPending(); }
  }
  async function submit(text = draft, recipient = session) {
    let marker = "", ownsFlight = false; let current = () => false;
    try {
      if (!recipient || recipient.id !== selected.current.id || recipient.workspace_id !== selected.current.workspace || (recipient === session && reading)) return;
      const targetScope = scope(recipient.id); marker = "post:" + JSON.stringify(targetScope); if (flights.current.has(marker)) return;
      flight(marker, true); ownsFlight = true; invalidate(); current = capture(); setVersion(v => v + 1);
      const { intent, first } = prepareIntent(localStorage, targetScope, "turn", { text, expected_revision: recipient.revision }); refreshPending();
      await performIntent(localStorage, intent, apiKey, first, value => { if (!current()) return false; setListView("active"); select(intent.workspace_id, intent.session_id!, value.id); saveDraft(localStorage, targetScope, ""); setDraft(""); return true; });
    } catch (error) { if (current()) setNotice(chatReason(error)); }
    finally { if (marker && ownsFlight) flight(marker, false); refreshPending(); }
  }
  async function createAndSubmit(text: string, target: string, projectId?: string, modelSelection?: WireSelection) {
    if (!text.trim()) return;
    const created = await create(text.trim().split("\n")[0]!.slice(0, 128), target, projectId, text, modelSelection);
    if (!created || selected.current.id !== created.id) return;
    saveDraft(localStorage, scope(created.id), text); setDraft(text);
    await submit(text, created);
  }
  async function recover(intent: Intent) {
    const marker = "find:" + intent.id; if (flights.current.has(marker)) return;
    let current = () => false;
    try {
      const who = scope(); if (intent.principal_id !== who.principal_id) throw new Error("请连接原身份");
      select(intent.workspace_id, intent.session_id ?? ""); current = capture(); flight(marker, true);
      await performIntent(localStorage, intent, apiKey, false, value => {
        if (!current()) return false;
        const firstDraft = intent.kind === "session" ? readDraft(localStorage, intent) : null;
        select(intent.workspace_id, intent.kind === "session" ? value.id : intent.session_id!, intent.kind === "turn" ? value.id : null);
        if (intent.kind === "turn") { saveDraft(localStorage, intent, ""); setDraft(""); }
        else if (firstDraft !== null) { saveDraft(localStorage, { ...intent, session_id: value.id }, firstDraft); setDraft(firstDraft); }
        return true;
      });
    } catch (error) { if (current()) setNotice(chatReason(error)); }
    finally { flight(marker, false); refreshPending(); }
  }
  async function control(turn: Turn | null, action: "cancel" | "close" | "resume" | "approval" | "rename", approval?: Record<string, unknown>, allow?: boolean, title?: string, target?: Session, approvalScope: ApprovalScope = "once") {
    const who = actorRef.current, member = who?.value.workspaces.find(w => w.workspace_id === workspace);
    const controlled = target ?? session;
    if (!activeRef.current || !who || who.key !== keyRef.current || !controlled || controlled.workspace_id !== workspace || !target && controlled.id !== id || selected.current.id !== id || selected.current.workspace !== workspace) return;
    if (target && (action !== "rename" || !list.some(value => value.id === target.id && value.revision === target.revision) || target.workspace_id !== workspace || target.owner_id !== who.value.principal_id)) return;
    const targetId = controlled.id;
    const canWriteNow = !!member && member.role !== "viewer";
    const canApproveNow = canWriteNow && member.approve;
    if (action === "rename") {
      if (!canWriteNow || typeof title !== "string" || !title.trim() || title.trim().length > 128 || controlled.status !== "open") return;
    } else if (action === "approval") {
      if (!canApproveNow || !turn || !turn.run_id || typeof allow !== "boolean" || turn.status !== "running" || turn.session_id !== id || turn.workspace_id !== workspace || !history.some(item => item.id === turn.id && item.run_id === turn.run_id && item.status === "running") || !approval) return;
      const stillPending = approvals.some(item => item.request_id === approval.request_id && item.run_id === turn.run_id && item.call_id === approval.call_id && item.arguments_hash === approval.arguments_hash && item.tool_hash === approval.tool_hash);
      if (!stillPending) return;
    } else if (!canWriteNow) return;
    if (action === "cancel" && (!turn || turn.session_id !== id || turn.workspace_id !== workspace || !["queued", "running"].includes(turn.status) || !history.some(item => item.id === turn.id && item.status === turn.status))) return;
    if (action === "close" && controlled.status !== "open" || action === "resume" && controlled.status !== "closed") return;
    const marker = action + ":" + (approval?.request_id ?? turn?.id ?? targetId); if (flights.current.has(marker)) return;
    let current = () => false;
    try {
      const targetScope = scope(targetId); let cacheKey = JSON.stringify({ ...targetScope, marker });
      flight(marker, true); invalidate(); current = capture(); setVersion(v => v + 1);
      const values = controls(), sessionTarget = sessionPath(workspace) + "/" + encodeURIComponent(targetId);
      const expectedTarget = action === "approval" ? turnPath(workspace, id, turn!.id) + "/approvals/" + encodeURIComponent(String(approval!.request_id)) : action === "cancel" ? turnPath(workspace, id, turn!.id) + "/cancel" : sessionTarget + "/" + action;
      const previous = Object.entries(values).find(([, value]) => value.principal_id === targetScope.principal_id && value.generation === targetScope.generation && value.workspace_id === workspace && value.session_id === targetId && value.target === expectedTarget);
      if (previous) cacheKey = previous[0];
      let saved = previous?.[1] ?? values[cacheKey], first = saved === undefined;
      if (!saved) {
        const path = expectedTarget;
        const body = action === "approval" ? { request_key: crypto.randomUUID(), call_id: approval!.call_id, arguments_hash: approval!.arguments_hash, tool_hash: approval!.tool_hash, allow, ...(allow && approvalScope === "session" ? { scope: "session" } : {}) } : { request_key: crypto.randomUUID(), expected_revision: controlled.revision, ...(action === "rename" ? { title: title!.trim() } : {}) };
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
      if (current() && value.status === "completed") { const latest = controls(); delete latest[cacheKey]; saveControls(latest); setVersion(v => v + 1); setNotice(null); if (action === "approval") execution.reload(); }
    } catch (error) { if (current()) setNotice(chatReason(error)); }
    finally { flight(marker, false); }
  }
  async function rename(title: string, sessionId = id) { const target = sessionId === id ? undefined : list.find(value => value.id === sessionId); if (sessionId !== id && !target) return; await control(null, "rename", undefined, undefined, title, target); }
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
        setTurnEvents(previous => {
          const latest = previous[turn.id];
          // A live stream may have advanced while the detail request was in flight.
          return latest && latest.length >= runEvents.length && latest.every((event, seq) => event.run_id === turn.run_id && event.seq === seq)
            ? previous : { ...previous, [turn.id]: [...runEvents] };
        });
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
  const execution = useExecutionPolicy({ apiKey, principalId: ready ? actor.value.principal_id : "",
    generation: ready ? actor.value.generation : 0, workspaceId: workspace, sessionId: id,
    sessionRevision: session?.revision ?? 0,
    enabled: active && ready && session?.id === id && session.workspace_id === workspace && !session.deleted_at,
    running: history.some(turn => turn.status === "queued" || turn.status === "running"), fence,
  }, () => setVersion(value => value + 1));
  return { execution, actor: ready ? actor.value : null, workspace, id, list: ready ? list : [], listView, setSessionListView, lifecycle, session: ready && session?.id === id ? session : null, history: ready ? history : [], details: ready ? details : {}, turnEvents: ready ? turnEvents : {}, turnFacts: ready ? turnFacts : {}, approvals: ready ? approvals : [], events: ready ? events : [], currentTurn: ready ? currentTurn : null, draft, notice: notice ?? (pendingState.error || null), reading, ready, canWrite: !!membership && membership.role !== "viewer", canApprove: !!membership && membership.role !== "viewer" && membership.approve, pending: visiblePending, pendingControls, turnPending, newPending, sending: ready && flights.current.has("post:" + JSON.stringify(scope(id))), creating: ready && flights.current.has("post:" + JSON.stringify(scope(null))), busy: flights.current.size > 0, controlling, posting, edit, create, createAndSubmit, submit, recover, recoverControl, control, rename, loadDetails, select, refresh: () => { invalidate(); setIdentityVersion(v => v + 1); setVersion(v => v + 1); refreshPending(); } };
}
export type SessionState = ReturnType<typeof useSession>;
