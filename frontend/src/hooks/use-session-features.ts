import { useCallback, useEffect, useRef, useState } from "react";
import { RequestError } from "../api";
import { answerInteraction, cancelInteraction, readInteractionState } from "../lib/interaction-client";
import type { InteractionAnswerBody, InteractionCancelBody, InteractionRequest, InteractionState } from "../lib/interaction-contracts";
import { readPlan } from "../lib/planning-client";
import { mergePlanPage, type PlanSnapshot } from "../lib/planning-contracts";

export interface SessionFeatureScope {
  apiKey: string;
  principalId: string;
  generation: number;
  workspaceId: string;
  sessionId: string;
  enabled: boolean;
  running: boolean;
  fence: { current: number };
}

interface FeatureState {
  identity: string;
  plan: PlanSnapshot | null;
  interactions: InteractionState | null;
  planError: string;
  interactionError: string;
  paging: boolean;
}

const initial = (identity: string): FeatureState => ({
  identity, plan: null, interactions: null, planError: "", interactionError: "", paging: false,
});
const unavailable = (error: unknown) => error instanceof RequestError
  && (error.status === 404 || error.code === "schema_upgrade_required");
const inaccessible = (error: unknown) => unavailable(error)
  || error instanceof RequestError && (error.status === 401 || error.status === 403);

/** Read state outside compacted transcripts, and fence every response to its session. */
export function useSessionFeatures(scope: SessionFeatureScope, onChanged?: () => void) {
  const { apiKey, principalId, generation, workspaceId, sessionId, enabled, running, fence } = scope;
  const fenceGeneration = fence.current;
  const identity = JSON.stringify([apiKey, principalId, generation, workspaceId, sessionId, fenceGeneration, enabled]);
  const identityRef = useRef(identity);
  const scopeEpoch = useRef(0);
  if (identityRef.current !== identity) scopeEpoch.current += 1;
  identityRef.current = identity;
  const onChangedRef = useRef(onChanged);
  onChangedRef.current = onChanged;
  const [state, setState] = useState<FeatureState>(() => initial(identity));
  const stateRef = useRef(state);
  stateRef.current = state;
  const [refresh, setRefresh] = useState(0);
  const [revisionHint, setRevisionHint] = useState(0);
  const reads = useRef<AbortController | null>(null);
  const pagingFlight = useRef<string | null>(null);
  const mounted = useRef(false);
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);

  const capture = useCallback(() => {
    const epoch = fence.current, capturedScope = scopeEpoch.current;
    return () => mounted.current && enabled && identityRef.current === identity
      && scopeEpoch.current === capturedScope && epoch === fenceGeneration && epoch === fence.current;
  }, [identity, enabled, fence, fenceGeneration]);

  useEffect(() => {
    if (!enabled || !principalId || !workspaceId || !sessionId) return;
    const controller = new AbortController();
    reads.current = controller;
    const owns = capture();
    const current = () => owns() && !controller.signal.aborted;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let loading = false;
    const patch = (update: (value: FeatureState) => FeatureState) => {
      if (current()) setState(previous => update(previous.identity === identity ? previous : initial(identity)));
    };
    async function load() {
      if (loading || !current()) return;
      loading = true;
      let waitingForAnswer = false;
      try {
        await Promise.allSettled([
          (async () => {
            try {
              const page = await readPlan(apiKey, workspaceId, sessionId, {}, controller.signal);
              patch(previous => ({
                ...previous,
                plan: previous.plan && previous.plan.revision === page.revision && previous.plan.content_hash === page.content_hash
                  ? previous.plan : page,
                planError: "",
              }));
            } catch (error) {
              patch(previous => ({ ...previous,
                plan: inaccessible(error) ? null : previous.plan,
                planError: unavailable(error) ? "" : "计划暂时无法读取",
              }));
            }
          })(),
          (async () => {
            try {
              const interactions = await readInteractionState(workspaceId, sessionId, apiKey, controller.signal);
              if (interactions.pending.some(item => item.workspace_id !== workspaceId || item.session_id !== sessionId)) {
                throw new Error("交互状态身份不匹配");
              }
              waitingForAnswer = interactions.pending.length > 0;
              patch(previous => ({ ...previous, interactions, interactionError: "" }));
            } catch (error) {
              patch(previous => ({ ...previous, interactions: inaccessible(error) ? null : previous.interactions,
                interactionError: unavailable(error) ? "" : "待答问题暂时无法读取",
              }));
            }
          })(),
        ]);
      } finally {
        loading = false;
        if (current() && (running || waitingForAnswer)) timer = setTimeout(() => void load(), 1500);
      }
    }
    const focus = () => {
      if (document.visibilityState !== "hidden") { clearTimeout(timer); void load(); }
    };
    void load();
    window.addEventListener("focus", focus);
    document.addEventListener("visibilitychange", focus);
    return () => {
      controller.abort();
      clearTimeout(timer);
      window.removeEventListener("focus", focus);
      document.removeEventListener("visibilitychange", focus);
    };
  }, [identity, enabled, running, refresh, revisionHint, capture]);

  const reload = useCallback(() => setRefresh(value => value + 1), []);

  async function loadMore() {
    const previous = stateRef.current;
    const plan = previous.identity === identity ? previous.plan : null;
    if (!enabled || !plan || plan.next_offset === null || pagingFlight.current?.startsWith(`${identity}:`)) return;
    const owns = capture();
    const controller = reads.current;
    const stamp = `${identity}:${plan.revision}:${plan.next_offset}`;
    pagingFlight.current = stamp;
    setState(value => value.identity === identity ? { ...value, paging: true, planError: "" } : value);
    try {
      const page = await readPlan(apiKey, workspaceId, sessionId, {
        revision: plan.revision, offset: plan.next_offset, limit: plan.limit,
      }, controller?.signal);
      if (!owns()) return;
      const merged = mergePlanPage(plan, page);
      setState(value => value.identity === identity && value.plan?.revision === plan.revision
        && value.plan.content_hash === plan.content_hash
        ? { ...value, plan: merged, planError: "" } : value);
    } catch {
      if (owns() && !controller?.signal.aborted) setState(value => value.identity === identity
        ? { ...value, planError: "剩余计划暂时无法读取，请重试" } : value);
    } finally {
      if (pagingFlight.current === stamp) pagingFlight.current = null;
      if (owns()) setState(value => value.identity === identity ? { ...value, paging: false } : value);
    }
  }

  async function respond(item: InteractionRequest, body: InteractionAnswerBody | InteractionCancelBody, cancel: boolean) {
    const owns = capture();
    if (!owns() || item.workspace_id !== workspaceId || item.session_id !== sessionId
      || item.owner_id !== principalId || item.status !== "pending") {
      throw new RequestError(403, "permission_denied");
    }
    const result = cancel
      ? await cancelInteraction(workspaceId, sessionId, item.id, body as InteractionCancelBody, apiKey)
      : await answerInteraction(workspaceId, sessionId, item.id, body as InteractionAnswerBody, apiKey);
    if (!owns()) return;
    if (result.id !== item.id || result.session_id !== sessionId || result.workspace_id !== workspaceId) {
      throw new Error("交互请求身份不匹配");
    }
    setState(value => value.identity === identity && value.interactions ? {
      ...value, interactions: { ...value.interactions, pending: value.interactions.pending.filter(request => request.id !== item.id) },
    } : value);
    setRevisionHint(value => value + 1);
    onChangedRef.current?.();
  }

  const visible = state.identity === identity && enabled ? state : initial(identity);
  return {
    ...visible, reload, loadMore,
    answer: (item: InteractionRequest, body: InteractionAnswerBody) => respond(item, body, false),
    cancel: (item: InteractionRequest, body: InteractionCancelBody) => respond(item, body, true),
  };
}
