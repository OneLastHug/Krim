import { useCallback, useEffect, useRef, useState } from "react";
import { RequestError } from "../api";
import {
  clearSessionAllowRules, readAllowRuleCount, readExecutionPolicy, readProjectPermission, updateExecutionMode,
  type ExecutionMode, type ExecutionPolicy, type ModeUpdate, type ProjectPermission,
} from "../lib/execution-policy";

interface Scope {
  apiKey: string; principalId: string; generation: number; workspaceId: string; sessionId: string;
  enabled: boolean; running: boolean; sessionRevision: number; fence: { current: number };
}
interface State {
  identity: string; policy: ExecutionPolicy | null; ruleCount: number; project: ProjectPermission | null;
  loading: boolean; changing: boolean; clearing: boolean; projectOpen: boolean;
  error: string; projectError: string; clearError: string;
}
const initial = (identity: string): State => ({ identity, policy: null, ruleCount: 0, project: null,
  loading: true, changing: false, clearing: false, projectOpen: false, error: "", projectError: "", clearError: "" });
const inaccessible = (error: unknown) => error instanceof RequestError && [401, 403, 404].includes(error.status);

/** Server state only. Session/identity epochs reject late reads and write receipts. */
export function useExecutionPolicy(scope: Scope, onChanged: () => void) {
  const { apiKey, principalId, generation, workspaceId, sessionId, enabled, running, sessionRevision, fence } = scope;
  const fenceGeneration = fence.current;
  const identity = JSON.stringify([apiKey, principalId, generation, workspaceId, sessionId, enabled, fenceGeneration]);
  const identityRef = useRef(identity), epoch = useRef(0), mounted = useRef(false);
  if (identityRef.current !== identity) epoch.current++;
  identityRef.current = identity;
  const [state, setState] = useState<State>(() => initial(identity));
  const stateRef = useRef(state); stateRef.current = state;
  const [refresh, setRefresh] = useState(0);
  const changed = useRef(onChanged); changed.current = onChanged;
  const modeIntent = useRef<{ identity: string; body: ModeUpdate } | null>(null);
  const clearIntent = useRef<{ identity: string; body: { request_key: string } } | null>(null);
  const writes = useRef(new Set<string>()), writeEpoch = useRef(0);
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);
  const capture = useCallback(() => {
    const e = epoch.current;
    return () => mounted.current && enabled && identityRef.current === identity && epoch.current === e
      && fence.current === fenceGeneration;
  }, [identity, enabled, fence, fenceGeneration]);
  const patch = useCallback((update: Partial<State>) => {
    setState(previous => ({ ...(previous.identity === identity ? previous : initial(identity)), ...update }));
  }, [identity]);
  const reload = useCallback(() => setRefresh(value => value + 1), []);
  const revokeView = useCallback(() => {
    patch({ policy: null, ruleCount: 0, project: null, projectOpen: false, loading: false,
      error: "项目或会话已无法访问，请重新读取。" });
  }, [patch]);
  useEffect(() => {
    if (!enabled || !principalId || !workspaceId || !sessionId) return;
    const controller = new AbortController(), owns = capture();
    const current = () => owns() && !controller.signal.aborted;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let loading = false;
    async function load() {
      if (!current() || loading) return;
      loading = true;
      const readEpoch = writeEpoch.current;
      try {
        const [policy, ruleCount] = await Promise.all([
          readExecutionPolicy(apiKey, workspaceId, sessionId, controller.signal),
          readAllowRuleCount(apiKey, workspaceId, sessionId, controller.signal),
        ]);
        if (current() && readEpoch === writeEpoch.current && !writes.current.has(identity)) setState(previous => {
          const before = previous.identity === identity ? previous : initial(identity);
          // A delayed poll must not roll back a newer mutation receipt.
          if (before.policy && before.policy.revision > policy.revision) return before;
          return { ...before, policy, ruleCount, loading: false,
            error: before.policy === null || before.error === "执行模式暂时无法读取，请重试。" ? "" : before.error,
            project: before.project?.project_id === policy.project_id ? before.project : null };
        });
      } catch (error) {
        if (current() && readEpoch === writeEpoch.current) {
          if (inaccessible(error)) revokeView();
          else patch({ loading: false, error: "执行模式暂时无法读取，请重试。" });
        }
      } finally {
        loading = false;
        if (current() && running) timer = setTimeout(() => void load(), 1500);
      }
    }
    const focus = () => { if (document.visibilityState !== "hidden") { clearTimeout(timer); void load(); } };
    void load(); window.addEventListener("focus", focus); document.addEventListener("visibilitychange", focus);
    return () => { controller.abort(); clearTimeout(timer); window.removeEventListener("focus", focus);
      document.removeEventListener("visibilitychange", focus); };
  }, [identity, enabled, principalId, workspaceId, sessionId, sessionRevision, running, refresh, capture, patch, revokeView, apiKey]);

  async function changeMode(mode: ExecutionMode, retry = false): Promise<boolean> {
    const owns = capture(), before = stateRef.current;
    if (!owns() || before.identity !== identity || !before.policy || !before.policy.project_id || writes.current.has(identity)) return false;
    const saved = retry && modeIntent.current?.identity === identity ? modeIntent.current.body
      : { mode, expected_revision: before.policy.revision, request_key: crypto.randomUUID() };
    modeIntent.current = { identity, body: saved }; writes.current.add(identity); writeEpoch.current++;
    patch({ changing: true, error: "" });
    try {
      const receipt = await updateExecutionMode(apiKey, workspaceId, sessionId, saved);
      if (!owns()) return false;
      // Replays can return their original revision; GET is the current authority.
      const latest = await readExecutionPolicy(apiKey, workspaceId, sessionId);
      if (!owns()) return false;
      if (latest.revision < receipt.revision) throw new Error("执行模式版本无效");
      patch({ policy: latest, loading: false, error: "" }); modeIntent.current = null;
      changed.current(); return true;
    } catch (error) {
      if (!owns()) return false;
      let recovered: ExecutionPolicy | null = null;
      try { recovered = await readExecutionPolicy(apiKey, workspaceId, sessionId); }
      catch (readError) { if (owns() && inaccessible(readError)) { revokeView(); return false; } }
      if (!owns()) return false;
      if (recovered) patch({ policy: recovered, loading: false });
      if (error instanceof RequestError && error.code === "revision_conflict") {
        modeIntent.current = null; patch({ error: "模式已在其他位置更改，已重新读取。请按当前状态选择。" });
      } else if (recovered?.mode === saved.mode) {
        modeIntent.current = null; patch({ error: "已从服务端恢复当前执行模式。" }); changed.current();
      } else patch({ error: "模式修改结果未确认，已保留服务端状态。可重试原请求。" });
      return false;
    } finally { writes.current.delete(identity); if (owns()) { patch({ changing: false }); reload(); } }
  }
  async function clearRules(): Promise<void> {
    const owns = capture();
    if (!owns() || writes.current.has(identity)) return;
    const body = clearIntent.current?.identity === identity ? clearIntent.current.body : { request_key: crypto.randomUUID() };
    clearIntent.current = { identity, body }; writes.current.add(identity); writeEpoch.current++; patch({ clearing: true, clearError: "" });
    try {
      await clearSessionAllowRules(apiKey, workspaceId, sessionId, body);
      const ruleCount = await readAllowRuleCount(apiKey, workspaceId, sessionId);
      if (!owns()) return;
      patch({ ruleCount, clearError: "" }); clearIntent.current = null; changed.current();
    } catch (error) {
      if (!owns()) return;
      if (inaccessible(error)) { revokeView(); return; }
      try { const ruleCount = await readAllowRuleCount(apiKey, workspaceId, sessionId); if (owns()) patch({ ruleCount }); } catch { /* Keep the original key for an explicit retry. */ }
      if (owns()) patch({ clearError: "清理结果未确认，请重试原清理请求。" });
    } finally { writes.current.delete(identity); if (owns()) { patch({ clearing: false }); reload(); } }
  }
  async function openProject(): Promise<void> {
    const owns = capture();
    if (!owns()) return;
    patch({ projectOpen: true, project: null, projectError: "" });
    try {
      const policy = stateRef.current.identity === identity && stateRef.current.policy
        ? stateRef.current.policy : await readExecutionPolicy(apiKey, workspaceId, sessionId);
      if (!owns()) return;
      if (!policy.project_id) { patch({ projectError: "此历史会话尚未绑定项目。" }); return; }
      patch({ policy });
      const project = await readProjectPermission(apiKey, workspaceId, sessionId);
      if (!owns()) return;
      const currentPolicy = stateRef.current.identity === identity ? stateRef.current.policy : null;
      if (project.project_id !== policy.project_id || currentPolicy && currentPolicy.project_id !== project.project_id) { reload(); throw new Error("项目绑定已更改"); }
      patch({ project, projectError: "" });
    } catch (error) {
      if (!owns()) return;
      if (inaccessible(error)) revokeView();
      else patch({ projectError: "项目详情暂时无法读取，请重试。" });
    }
  }
  const visible = state.identity === identity && enabled ? state : initial(identity);
  return { ...visible, reload, changeMode, clearRules, openProject,
    closeProject: () => patch({ projectOpen: false }),
    retryMode: () => modeIntent.current?.identity === identity
      ? changeMode(modeIntent.current.body.mode, true) : Promise.resolve(false),
    hasPendingMode: modeIntent.current?.identity === identity,
    hasPendingClear: clearIntent.current?.identity === identity,
  };
}
export type SessionExecutionState = ReturnType<typeof useExecutionPolicy>;
