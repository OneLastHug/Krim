import { useEffect, useRef, useState } from "react";
import { RequestError } from "../api";
import { scheduleCompaction } from "../lib/context-client";
import type { SessionState } from "./use-session";

const PREFIX = "krim.agent-core.compaction-request/1:";

export function useSessionCompaction(chat: SessionState, apiKey: string, active: boolean, fence: { current: number }) {
  const renderFence = fence.current;
  const identity = JSON.stringify([apiKey, chat.actor?.principal_id, chat.actor?.generation, chat.workspace, chat.id, active, renderFence]);
  const currentIdentity = useRef(identity), epoch = useRef(0);
  if (currentIdentity.current !== identity) epoch.current += 1;
  currentIdentity.current = identity;
  const mounted = useRef(false), inFlight = useRef(new Set<string>());
  const [state, setState] = useState({ identity, busy: false, notice: "" });
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);
  const canSchedule = active && chat.ready && !!chat.actor && !!chat.session
    && chat.session.id === chat.id && chat.session.workspace_id === chat.workspace
    && chat.session.owner_id === chat.actor.principal_id && chat.canWrite
    && chat.session.status === "open" && !chat.session.archived_at && !chat.session.deleted_at;

  async function compact() {
    if (!canSchedule || !chat.actor || fence.current !== renderFence
      || currentIdentity.current !== identity || inFlight.current.has(identity)) return;
    const generation = fence.current, capturedEpoch = epoch.current;
    const owns = () => mounted.current && currentIdentity.current === identity
      && epoch.current === capturedEpoch && fence.current === generation;
    const storageKey = PREFIX + JSON.stringify([chat.actor.principal_id, chat.actor.generation, chat.workspace, chat.id]);
    inFlight.current.add(identity);
    setState({ identity, busy: true, notice: "" });
    try {
      // Persist before POST: an unknown result always retries the original key.
      let key = localStorage.getItem(storageKey);
      if (key === null) {
        key = crypto.randomUUID();
        localStorage.setItem(storageKey, key);
        if (localStorage.getItem(storageKey) !== key) throw new Error("request_not_saved");
      }
      if (!/^[!-~]{1,128}$/.test(key)) throw new Error("invalid_request_key");
      const receipt = await scheduleCompaction(apiKey, chat.workspace, chat.id, key);
      if (localStorage.getItem(storageKey) === key) localStorage.removeItem(storageKey);
      if (owns()) setState({ identity, busy: false, notice: receipt.status === "pending"
        ? "整理请求已排队，将在下次安全的模型请求边界执行。"
        : "原整理请求已处理。" });
    } catch (error) {
      let notice = "整理请求结果未确认，请点击 Compact session 重试原请求。";
      if (error instanceof RequestError) {
        if (error.code === "session_closed") notice = "对话已关闭，无法整理上下文。";
        else if (error.status === 401 || error.status === 403) notice = "当前身份无法整理此对话的上下文。";
        else if (error.code === "schema_upgrade_required") notice = "当前服务尚未提供上下文整理。";
      }
      if (owns()) setState({ identity, busy: false, notice });
    } finally {
      inFlight.current.delete(identity);
      if (owns()) setState(previous => previous.identity === identity ? { ...previous, busy: false } : previous);
    }
  }
  const visible = state.identity === identity ? state : { identity, busy: false, notice: "" };
  return { ...visible, disabled: !canSchedule || visible.busy, compact, dismiss: () => setState({ identity, busy: visible.busy, notice: "" }) };
}
