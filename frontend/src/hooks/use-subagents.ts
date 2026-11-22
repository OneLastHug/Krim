import { useCallback, useEffect, useRef, useState } from "react";
import { RequestError } from "../api";
import { chatReason } from "../lib/copy";
import {
  controlSubagent,
  listSubagents,
  readSubagentDetail,
  replySubagentApproval,
  sendSubagentMessage,
} from "../lib/subagent-client";
import {
  SUBAGENT_PAGE_MAX,
  subagentState,
  type SubagentDetail,
  type SubagentMember,
  type WakeMode,
} from "../lib/subagent-contracts";
import type { ApprovalRecord } from "../lib/session-presentation";
import type { SessionState } from "./use-session";

/** 能力不可用（未升级/未启用）或条目不存在：收起入口且不持续报错。 */
const unavailable = (error: unknown) => error instanceof RequestError
  && (error.status === 404 || error.code === "schema_upgrade_required" || error.code === "subagent_capability");
/** 身份失效：清空本地视图，不保留半份数据。 */
const inaccessible = (error: unknown) => error instanceof RequestError && (error.status === 401 || error.status === 403);

const LIST_ERROR = "子代理列表暂时无法读取";
const DETAIL_ERROR = "子代理详情暂时无法读取";
const APPROVAL_ERROR = "等待批准的子代理暂时无法读取";
const FAST_INTERVAL = 1500, IDLE_INTERVAL = 4000;
/** 服务端已明确拒绝：换新键重试只会重复同样错误；其余错误保留原键。 */
const definitelyRejected = (error: unknown) => error instanceof RequestError && !(error.status >= 500 || error.status === 0);

interface MutationFlight { identity: string; marker: string }
const flightMarker = (flight: MutationFlight | null, identity: string) => flight !== null && flight.identity === identity ? flight.marker : "";

interface Snapshot {
  identity: string;
  members: SubagentMember[];
  nextAfterPath: string | null;
  hasMore: boolean;
  loading: boolean;
  error: string;
  available: boolean;
  selectedId: string | null;
  detail: SubagentDetail | null;
  detailLoading: boolean;
  detailError: string;
  approvals: Record<string, ApprovalRecord[]>;
  approvalError: string;
}

const initial = (identity: string): Snapshot => ({
  identity, members: [], nextAfterPath: null, hasMore: false, loading: true, error: "",
  available: false, selectedId: null, detail: null, detailLoading: false, detailError: "",
  approvals: {}, approvalError: "",
});

/** 按 agent_id 去重合并并保持 path 升序；成员不删除（closed 仍在列）。 */
function mergeMembers(current: SubagentMember[], additions: SubagentMember[]): SubagentMember[] {
  const merged = new Map(current.map(member => [member.agent_id, member]));
  for (const member of additions) merged.set(member.agent_id, member);
  return [...merged.values()].sort((a, b) => a.path < b.path ? -1 : a.path > b.path ? 1 : a.agent_id < b.agent_id ? -1 : 1);
}

export interface SubagentApprovalItem { agent: SubagentMember; approval: ApprovalRecord }

/**
 * 会话子代理数据层：列表分页、成员详情、消息投递与控制、审批聚合。
 * 所有响应按身份与 fence 归属校验；切换会话立即呈现空视图，迟到的读写一律丢弃。
 */
export function useSubagents({ chat, apiKey, active, fence }: {
  chat: SessionState; apiKey: string; active: boolean; fence: { current: number };
}) {
  const principalId = chat.actor?.principal_id ?? "";
  const generation = chat.actor?.generation ?? 0;
  const workspaceId = chat.workspace;
  const sessionId = chat.id;
  const enabled = active && chat.ready && !!chat.session && !chat.session.deleted_at;
  const rootRunning = chat.history.some(turn => turn.status === "running" || turn.status === "queued");
  const fenceGeneration = fence.current;
  const identity = JSON.stringify([apiKey, principalId, generation, workspaceId, sessionId, fenceGeneration, enabled]);

  const identityRef = useRef(identity);
  const scopeEpoch = useRef(0);
  if (identityRef.current !== identity) scopeEpoch.current += 1;
  identityRef.current = identity;

  const enabledRef = useRef(enabled), rootRunningRef = useRef(rootRunning);
  const apiKeyRef = useRef(apiKey), workspaceRef = useRef(workspaceId), sessionRef = useRef(sessionId);
  enabledRef.current = enabled; rootRunningRef.current = rootRunning;
  apiKeyRef.current = apiKey; workspaceRef.current = workspaceId; sessionRef.current = sessionId;

  const mounted = useRef(false);
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);

  const [state, setState] = useState<Snapshot>(() => initial(identity));
  const stateRef = useRef(state);
  stateRef.current = state;
  const selectedRef = useRef<string | null>(state.identity === identity ? state.selectedId : null);
  selectedRef.current = state.identity === identity ? state.selectedId : null;

  const [refresh, setRefresh] = useState(0);
  const controllerRef = useRef<AbortController | null>(null);
  /** 读循环占用按身份记录：旧会话的读取绝不阻塞新会话的首轮加载。 */
  const readFlights = useRef(new Set<string>());
  const cursorsRef = useRef<string[]>([""]);
  const cursorsOwner = useRef("");
  const approvalsFlight = useRef(new Set<string>());
  const approvalsFailed = useRef(new Set<string>());
  const pendingSend = useRef<{ identity: string; agentId: string; text: string; wake: WakeMode; requestKey: string } | null>(null);
  const approvalKeys = useRef(new Map<string, string>());
  const mutationFlight = useRef<MutationFlight | null>(null);
  const [, setFlightTick] = useState(0);
  const [receipt, setReceiptState] = useState({ identity: "", text: "" });
  const [approvalReceipt, setApprovalReceiptState] = useState({ identity: "", text: "" });

  const capture = useCallback(() => {
    const epoch = scopeEpoch.current, fenceEpoch = fenceGeneration;
    return () => mounted.current && enabledRef.current && identityRef.current === identity
      && scopeEpoch.current === epoch && fence.current === fenceEpoch;
  }, [identity, fence, fenceGeneration]);

  const reload = useCallback(() => setRefresh(value => value + 1), []);
  const notify = () => window.dispatchEvent(new Event("krim:subagents-changed"));
  const beginFlight = (marker: string) => {
    mutationFlight.current = { identity: identityRef.current, marker };
    setFlightTick(value => value + 1);
  };
  const endFlight = () => {
    if (mutationFlight.current?.identity === identity) {
      mutationFlight.current = null;
      setFlightTick(value => value + 1);
    }
  };
  const say = (text: string) => setReceiptState({ identity: identity + ":" + state.selectedId, text });
  const sayApproval = (text: string) => setApprovalReceiptState({ identity: identityRef.current, text });

  useEffect(() => {
    if (!enabled || !principalId || !workspaceId || !sessionId) return;
    if (cursorsOwner.current !== identity) { cursorsRef.current = [""]; cursorsOwner.current = identity; }
    const controller = new AbortController();
    controllerRef.current = controller;
    const owns = capture();
    const current = () => owns() && !controller.signal.aborted;
    const key = apiKey, workspace = workspaceId, session = sessionId;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const patch = (update: (value: Snapshot) => Snapshot) => {
      if (current()) setState(previous => update(previous.identity === identity ? previous : initial(identity)));
    };
    const schedule = (interval: number) => { clearTimeout(timer); timer = setTimeout(() => { void tick(); }, interval); };

    /** 返回 false 表示能力或身份不可用，停止本轮循环。 */
    function listError(error: unknown): boolean {
      if (unavailable(error)) { patch(() => ({ ...initial(identity), loading: false })); return false; }
      if (inaccessible(error)) {
        patch(value => ({ ...value, members: [], nextAfterPath: null, hasMore: false, approvals: {},
          detail: null, selectedId: null, detailError: "", error: "", approvalError: "", loading: false }));
        return false;
      }
      patch(value => ({ ...value, available: true, loading: false, error: LIST_ERROR }));
      return true;
    }

    function applyMembers(members: SubagentMember[], next: string | null) {
      const merged = mergeMembers(stateRef.current.identity === identity ? stateRef.current.members : [], members.filter(member => member.role !== "root"));
      patch(value => ({ ...value, members: merged, nextAfterPath: next, hasMore: next !== null, loading: false, error: "", available: true }));
      // 审批缓存只保留仍在等待批准的成员；读取失败的成员恢复后重算错误。
      const awaiting = new Set(merged.filter(member => member.task?.status === "awaiting_approval").map(member => member.agent_id));
      for (const agentId of [...approvalsFailed.current]) if (!awaiting.has(agentId)) approvalsFailed.current.delete(agentId);
      patch(value => {
        const kept: Record<string, ApprovalRecord[]> = {};
        for (const [agentId, approvals] of Object.entries(value.approvals)) if (awaiting.has(agentId)) kept[agentId] = approvals;
        return { ...value, approvals: kept, approvalError: approvalsFailed.current.size ? APPROVAL_ERROR : "" };
      });
      return merged;
    }

    function fetchDetail(selected: string): Promise<void> {
      patch(value => ({ ...value, detailLoading: true }));
      return (async () => {
        try {
          const detail = await readSubagentDetail(key, workspace, session, selected, controller.signal);
          if (!current() || selectedRef.current !== selected) return;
          patch(value => ({
            ...value, detail, detailLoading: false, detailError: "",
            approvals: { ...value.approvals, [selected]: detail.approvals },
            approvalError: [...approvalsFailed.current].filter(id => id !== selected).length ? APPROVAL_ERROR : "",
          }));
        } catch (error) {
          if (!current() || selectedRef.current !== selected) return;
          if (inaccessible(error) || error instanceof RequestError && error.status === 404) {
            // 成员已不可见或身份不符：清空选择，不保留半份详情。
            patch(value => ({ ...value, selectedId: null, detail: null, detailLoading: false, detailError: "" }));
            return;
          }
          patch(value => ({ ...value, detailLoading: false, detailError: DETAIL_ERROR }));
        }
      })();
    }

    function fetchApprovals(agentId: string): Promise<void> {
      const flightId = identity + ":" + agentId;
      if (approvalsFlight.current.has(flightId)) return Promise.resolve();
      approvalsFlight.current.add(flightId);
      return (async () => {
        try {
          const detail = await readSubagentDetail(key, workspace, session, agentId, controller.signal);
          if (!current()) return;
          approvalsFailed.current.delete(agentId);
          patch(value => ({
            ...value,
            approvals: { ...value.approvals, [agentId]: detail.approvals },
            approvalError: approvalsFailed.current.size ? APPROVAL_ERROR : "",
          }));
        } catch {
          if (!current()) return;
          // 列表显示等待批准却读不到详情：如实报错，不静默吞掉。
          approvalsFailed.current.add(agentId);
          patch(value => ({ ...value, approvalError: APPROVAL_ERROR, approvals:
            Object.fromEntries(Object.entries(value.approvals).filter(([id]) => id !== agentId)) }));
        } finally {
          approvalsFlight.current.delete(flightId);
        }
      })();
    }

    async function tick() {
      if (!current()) return;
      if (readFlights.current.has(identity)) { schedule(50); return; }
      readFlights.current.add(identity);
      let stopped = false;
      try {
        // 逐页重读已加载游标，保留分页进度；不越过未读边界。
        const cursors = cursorsRef.current;
        const collected: SubagentMember[] = [];
        let next: string | null = null;
        for (let index = 0; index < cursors.length; index++) {
          const page = await listSubagents(key, workspace, session,
            { afterPath: cursors[index] || undefined, limit: SUBAGENT_PAGE_MAX }, controller.signal);
          if (!current()) return;
          collected.push(...page.subagents);
          next = page.next_after_path;
          if (next === null || index + 1 >= cursors.length) break;
        }
        const members = applyMembers(collected, next);
        const tasks: Promise<void>[] = [];
        const selected = selectedRef.current;
        if (selected) tasks.push(fetchDetail(selected));
        for (const member of members) {
          if (member.agent_id !== selected && member.task?.status === "awaiting_approval") tasks.push(fetchApprovals(member.agent_id));
        }
        await Promise.all(tasks);
      } catch (error) {
        if (!current()) return;
        stopped = !listError(error);
      } finally {
        readFlights.current.delete(identity);
        if (!stopped && current()) {
          const busy = rootRunningRef.current || stateRef.current.members.some(member => subagentState(member) === "running");
          schedule(busy ? FAST_INTERVAL : IDLE_INTERVAL);
        }
      }
    }

    const focus = () => { if (document.visibilityState !== "hidden") { clearTimeout(timer); void tick(); } };
    void tick();
    window.addEventListener("focus", focus);
    document.addEventListener("visibilitychange", focus);
    return () => {
      controller.abort();
      readFlights.current.delete(identity);
      clearTimeout(timer);
      window.removeEventListener("focus", focus);
      window.removeEventListener("visibilitychange", focus);
    };
  }, [identity, enabled, principalId, workspaceId, sessionId, rootRunning, refresh, capture]);

  const select = useCallback((id: string) => {
    const target = typeof id === "string" && id ? id : null;
    selectedRef.current = target;
    setState(previous => previous.identity !== identity || previous.selectedId === target ? previous
      : { ...previous, selectedId: target, detail: null, detailError: "", detailLoading: !!target });
    setRefresh(value => value + 1);
  }, [identity]);

  const loadMore = useCallback(async () => {
    const snapshot = stateRef.current;
    if (!enabledRef.current || snapshot.identity !== identity || snapshot.nextAfterPath === null) return;
    if (readFlights.current.has(identity)) return;
    const owns = capture();
    const cursor = snapshot.nextAfterPath;
    readFlights.current.add(identity);
    try {
      const page = await listSubagents(apiKeyRef.current, workspaceRef.current, sessionRef.current,
        { afterPath: cursor, limit: SUBAGENT_PAGE_MAX }, controllerRef.current?.signal);
      if (!owns()) return;
      cursorsRef.current = [...cursorsRef.current, cursor];
      const merged = mergeMembers(stateRef.current.members, page.subagents.filter(member => member.role !== "root"));
      setState(value => value.identity !== identity ? value : {
        ...value, members: merged, nextAfterPath: page.next_after_path, hasMore: page.next_after_path !== null,
        loading: false, error: "", available: true,
      });
    } catch (error) {
      if (!owns()) return;
      if (unavailable(error)) { setState(() => ({ ...initial(identity), available: false })); return; }
      if (inaccessible(error)) {
        setState(value => value.identity !== identity ? value : {
          ...value, members: [], nextAfterPath: null, hasMore: false, approvals: {}, approvalError: "",
          detail: null, selectedId: null, detailError: "", error: "", loading: false,
        });
        return;
      }
      setState(value => value.identity !== identity ? value : { ...value, error: "更多子代理暂时无法读取，请重试" });
    } finally {
      readFlights.current.delete(identity);
    }
  }, [identity, capture]);

  async function send(text: string, wake: WakeMode): Promise<boolean> {
    const owns = capture();
    const snapshot = stateRef.current;
    const member = snapshot.members.find(item => item.agent_id === snapshot.selectedId);
    const body = typeof text === "string" ? text : "";
    if (!owns() || !member || member.role !== "agent" || !body.trim() || flightMarker(mutationFlight.current, identity)) return false;
    const agentId = member.agent_id;
    // 未知结果保留原 request_key：相同消息重试命中服务端重放，不重复交付。
    const pending = pendingSend.current;
    const reuse = !!pending && pending.identity === identity && pending.agentId === agentId && pending.text === body && pending.wake === wake;
    const requestKey = reuse && pending ? pending.requestKey : crypto.randomUUID();
    beginFlight("send:" + agentId);
    if (reuse) say("发送结果未确认，正在用原请求重试…");
    try {
      const receipt = await sendSubagentMessage(apiKeyRef.current, workspaceRef.current, sessionRef.current, agentId,
        { text: body, wake, request_key: requestKey });
      if (!owns()) return false;
      pendingSend.current = null;
      say(receipt.delivery === "started" ? "已发送，子代理正在处理"
        : receipt.delivery === "queued" ? "已发送，等待当前任务结束后处理" : "已发送，将在下一次输入边界送达");
      reload(); notify();
      return true;
    } catch (error) {
      if (!owns()) return false;
      if (definitelyRejected(error)) {
        pendingSend.current = null;
        say(chatReason(error));
      } else {
        pendingSend.current = { identity, agentId, text: body, wake, requestKey };
        say("发送结果未确认，请检查连接后重试原消息");
      }
      return false;
    } finally { endFlight(); }
  }

  async function control(action: "interrupt" | "close", expected?: { agentId: string; activationId: string | null; taskId: string | null }): Promise<boolean> {
    const owns = capture();
    const snapshot = stateRef.current;
    const member = snapshot.members.find(item => item.agent_id === (expected?.agentId ?? snapshot.selectedId));
    if (!owns() || !member || member.role !== "agent" || flightMarker(mutationFlight.current, identity)) return false;
    const agentId = member.agent_id;
    // 捕获当前句柄；句柄失效由服务端显式拒绝，不误伤已切换的其他成员。
    const body = { activation_id: expected ? expected.activationId : member.activation?.activation_id ?? null, task_id: expected ? expected.taskId : member.current_task_id };
    beginFlight(action + ":" + agentId);
    try {
      await controlSubagent(apiKeyRef.current, workspaceRef.current, sessionRef.current, agentId, action, body);
      if (!owns()) return false;
      say(action === "interrupt" ? "已请求中断，子代理将在安全边界停止" : "已关闭子代理及其后代");
      reload(); notify();
      return true;
    } catch (error) {
      if (!owns()) return false;
      say(error instanceof RequestError ? chatReason(error) : "操作结果未确认，请刷新后检查子代理状态");
      return false;
    } finally { endFlight(); }
  }

  async function approve(agentId: string, approval: ApprovalRecord, allow: boolean, requestedScope: "once" | "session" = "once"): Promise<boolean> {
    const owns = capture();
    const member = stateRef.current.members.find(item => item.agent_id === agentId);
    if (!owns() || !member || member.role !== "agent" || flightMarker(mutationFlight.current, identity)) return false;
    if (!approval || typeof approval.request_id !== "string" || !approval.request_id) return false;
    // 拒绝永远是单次；只有批准可以携带会话范围。
    const scope: "once" | "session" = allow ? requestedScope : "once";
    // 审批重试键按完整决定（身份+request_id+allow+scope）标记：改判必须换新键。
    const marker = [identity, agentId, approval.request_id, String(allow), scope].join("|");
    const saved = approvalKeys.current.get(marker);
    const requestKey = saved ?? crypto.randomUUID();
    approvalKeys.current.set(marker, requestKey);
    beginFlight("approval:" + agentId + ":" + approval.request_id);
    try {
      await replySubagentApproval(apiKeyRef.current, workspaceRef.current, sessionRef.current, agentId, approval.request_id, {
        request_key: requestKey,
        call_id: String(approval.call_id),
        arguments_hash: String(approval.arguments_hash),
        tool_hash: String(approval.tool_hash),
        allow,
        ...(scope === "session" ? { scope: "session" as const } : { scope: "once" as const }),
      });
      if (!owns()) return false;
      approvalKeys.current.delete(marker);
      sayApproval(allow ? "已批准该工具调用" : "已拒绝该工具调用");
      reload(); notify();
      return true;
    } catch (error) {
      if (!owns()) return false;
      if (definitelyRejected(error)) approvalKeys.current.delete(marker);
      sayApproval(definitelyRejected(error) ? chatReason(error) : "审批结果未确认，请用原决定重试");
      return false;
    } finally { endFlight(); }
  }

  const visible = state.identity === identity && enabled ? state : initial(identity);
  const approvalItems: SubagentApprovalItem[] = [];
  for (const member of visible.members) {
    if (member.task?.status !== "awaiting_approval") continue;
    for (const approval of visible.approvals[member.agent_id] ?? []) approvalItems.push({ agent: member, approval });
  }
  return {
    members: visible.members,
    selectedId: visible.selectedId,
    detail: visible.detail,
    loading: visible.loading,
    detailLoading: visible.detailLoading,
    error: visible.error,
    detailError: visible.detailError,
    approvalError: visible.approvalError,
    available: visible.available,
    busy: flightMarker(mutationFlight.current, identity) !== "",
    hasMore: visible.hasMore,
    receipt: receipt.identity === identity + ":" + visible.selectedId ? receipt.text : "",
    approvalReceipt: approvalReceipt.identity === identity ? approvalReceipt.text : "",
    approvalItems,
    select,
    reload,
    loadMore,
    send,
    control,
    approve,
  };
}
