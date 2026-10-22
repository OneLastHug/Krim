import { useEffect, useRef, useState } from "react";
import { request } from "../api";
import { parseArtifact, type ArtifactRevision, type Completion, type InputSnapshotRequest,
  type WorkspaceTurnRequest } from "../lib/workspace-contracts";
import { parseWorkView, performWorkIntent, prepareWorkIntent, readWorkIntents, readWorkKnown,
  recoverWorkKnown, sameWorkObject, workBase, type WorkKnown, type WorkIntent, type WorkScope, type WorkView } from "../lib/workspace-requests";
import type { SessionState } from "./use-session";

export function useWorkspace(chat: SessionState, apiKey: string, active: boolean,
  fence: { current: number }) {
  const [view, setView] = useState<WorkView | null>(null);
  const [artifact, setArtifact] = useState<ArtifactRevision | null>(null);
  const [notice, setNotice] = useState("");
  const [pending, setPending] = useState<WorkIntent[]>([]), [busy, setBusy] = useState(false);
  const [version, refresh] = useState(0);
  const [known, setKnown] = useState<WorkKnown[]>([]);
  const epoch = useRef(0), mounted = useRef(false), flights = useRef(new Set<string>());
  const reads = useRef<AbortController | null>(null);
  const live = useRef({ chat, apiKey, active, fence: fence.current });
  live.current = { chat, apiKey, active, fence: fence.current };
  const identity = JSON.stringify([chat.actor?.principal_id, chat.actor?.generation,
    chat.workspace, chat.id, chat.session?.target_key, apiKey, active, fence.current]);
  const identityRef = useRef(identity); identityRef.current = identity;
  const scope = (): WorkScope => {
    const value = live.current.chat;
    if (!value.actor || !value.id || value.session?.target_key !== "target-workspace")
      throw new Error("工作区未就绪");
    return { principal_id: value.actor.principal_id, generation: value.actor.generation,
      workspace_id: value.workspace, session_id: value.id, target: "target-workspace" };
  };
  function capture() {
    const currentEpoch = epoch.current, currentIdentity = identityRef.current;
    const key = live.current.apiKey, generation = fence.current;
    return () => mounted.current && live.current.active && epoch.current === currentEpoch &&
      identityRef.current === currentIdentity && live.current.apiKey === key && fence.current === generation;
  }
  function refreshPending() {
    const currentScope = scope();
    setPending(readWorkIntents(localStorage).filter(value => sameWorkObject(value, currentScope)));
  }
  useEffect(() => {
    mounted.current = true;
    return () => { mounted.current = false; epoch.current++; reads.current?.abort(); };
  }, []);
  useEffect(() => {
    reads.current?.abort();
    epoch.current++; setView(null); setArtifact(null); setNotice(""); setPending([]); setKnown([]);
    if (!active || !chat.actor || !chat.id || chat.session?.target_key !== "target-workspace") return;
    const controller = new AbortController(), current = capture(), currentScope = scope();
    void (async () => {
      refreshPending();
      const confirmed = readWorkKnown(localStorage).filter(value => sameWorkObject(value, currentScope));
      setKnown(confirmed);
      const latest = confirmed.at(-1);
      // 已确认输入可能没有Turn；始终以原身份恢复，不能让/work覆盖该revision。
      const raw = latest ? await recoverWorkKnown(localStorage, latest, currentScope, apiKey,
        capture, controller.signal) : await request(workBase(currentScope)+"/work", apiKey,
        undefined, controller.signal);
      if (!current() || raw === null) return;
      if (latest?.kind === "input") {
        setArtifact(parseArtifact(raw)); setNotice("已按原身份找回输入快照"); return;
      }
      const result = parseWorkView(raw);
      if (result.session_id !== currentScope.session_id) throw new Error("工作视图身份不符");
      setView(result);
      const revision = result.verification?.revision_id ?? [...result.effects].reverse().find(
        effect => effect.artifact_revision_id)?.artifact_revision_id;
      if (revision) {
        const artifactCurrent = capture();
        const item = parseArtifact(await request(workBase(currentScope)+"/artifacts/"+
          encodeURIComponent(revision), apiKey, undefined, controller.signal));
        if (!current() || !artifactCurrent()) return;
        if (item.session_id !== currentScope.session_id || item.revision_id !== revision) throw new Error("产物身份不符");
        setArtifact(item);
      }
    })().catch(() => { if (current() && !controller.signal.aborted)
      setNotice("无法核实原工作身份；保留账本，可刷新核实"); });
    return () => { epoch.current++; controller.abort(); reads.current?.abort(); };
  }, [identity, version]);

  async function perform(kind: "turn" | "input", body?: WorkspaceTurnRequest | InputSnapshotRequest,
    saved?: WorkIntent) {
    const current = capture();
    const currentScope = scope(), marker = JSON.stringify(currentScope)+kind;
    if (flights.current.has(marker)) return;
    flights.current.add(marker); setBusy(true);
    try {
      const prepared = saved ? { intent: saved, first: false } : await prepareWorkIntent(
        localStorage, currentScope, kind, body!);
      if (!sameWorkObject(prepared.intent, currentScope)) throw new Error("请连接原请求的身份");
      if (!current()) return;
      refreshPending();
      await performWorkIntent(localStorage, prepared.intent, apiKey, prepared.first, capture, value => {
        if (!current()) return;
        if (kind === "turn") chat.select(currentScope.workspace_id, currentScope.session_id,
          (value as WorkView).turn_id);
        else setArtifact(value as ArtifactRevision);
        setKnown(readWorkKnown(localStorage).filter(item => sameWorkObject(item, currentScope)));
        setNotice("已保存持久身份");
      });
    } catch { if (current()) setNotice("请求未确认；保留原内容，可查找原请求"); }
    finally {
      flights.current.delete(marker);
      if (mounted.current) { setBusy(flights.current.size > 0); if (current()) refreshPending(); }
    }
  }
  async function submit(text: string, completion: Completion) {
    const body: WorkspaceTurnRequest = { schema_version: 1, request_key: crypto.randomUUID(), text,
      expected_revision: chat.session!.revision, input: { mode: "head_at_dispatch", revision_id: null },
      completion: JSON.parse(JSON.stringify(completion)) as Completion };
    return perform("turn", body);
  }
  async function recoverConfirmed(record: WorkKnown) {
    reads.current?.abort();
    const controller = new AbortController(); reads.current = controller;
    epoch.current++; setView(null); setArtifact(null);
    const current = capture(), currentScope = scope();
    try {
      const value = await recoverWorkKnown(localStorage, record, currentScope, apiKey, capture, controller.signal);
      if (!current() || value === null) return;
      if (record.kind === "input") setArtifact(parseArtifact(value));
      else setView(parseWorkView(value));
      setNotice("已按原身份找回工作对象");
    } catch { if (current()) setNotice("无法核实原工作身份；保留账本"); }
    finally { if (reads.current === controller) reads.current = null; }
  }
  return { view, artifact, notice, pending, known, busy, scope, capture,
    recoverConfirmed,
    submit, input: (body: InputSnapshotRequest) => perform("input", body),
    recover: (intent: WorkIntent) => perform(intent.kind, undefined, intent),
    refresh: () => refresh(value => value+1) };
}
export type WorkspaceState = ReturnType<typeof useWorkspace>;
