import { useEffect, useMemo, useRef, useState } from "react";
import { request, RequestError } from "../api";
import type { Actor } from "../lib/session-contracts";
import {
  parseObservationsPage,
  parseTurnObservations,
  type ObservationsIndexItem,
  type ObservationsPage,
  type ObsTurnObservations,
} from "./obs-contracts";

/** 观测数据 hooks：与 useTraceIndex 同构的有界分页 + 轮次详情读取。
 * 作用域绑定 [key, principal, generation, workspace, role, active, status, session]；
 * 任一变化即失效旧数据并中断在途请求，不保留上一工作区内容。 */

function notice(error: unknown): string {
  if (error instanceof RequestError) {
    if (error.status === 401) return "请重新连接工作区。";
    if (error.status === 403 || error.status === 404) return "记录不存在或当前身份无权读取。";
    if (error.status === 409) return error.code === "not_terminal" ? "该轮还没有可观测的运行记录。" : "运行事实关联损坏，无法读取。";
    if (error.status === 416) return "游标已失效，请刷新后重试。";
    if (error.code === "closed") return "服务已关闭，请重新连接。";
  }
  return "读取失败，请刷新重试。";
}

export interface ObservationsOptions {
  apiKey: string;
  workspace: string;
  identity: Actor | null;
  active: boolean;
  status: string;
  sessionId: string;
  /** 期望查看的轮次；存在但不在当前页时自动向后翻页（有界 4 页）。 */
  turnId: string;
}

type Pagination = { scope: string; cursor: string | null; previous: (string | null)[] };
type Listing = { query: string; page: ObservationsPage | null; loading: boolean; error: string };
type Reading = { query: string; detail: ObsTurnObservations | null; loading: boolean; error: string };

export function useObservations({ apiKey, workspace, identity, active, status, sessionId, turnId }: ObservationsOptions) {
  const membership = identity?.workspaces.find(value => value.workspace_id === workspace);
  const ready = active && !!identity && !!membership && !!workspace;
  const scope = JSON.stringify([
    apiKey, identity?.principal_id ?? "", identity?.generation ?? null, workspace, membership?.role ?? "",
    active, status,
  ]);
  const [pagination, setPagination] = useState<Pagination>({ scope: "", cursor: null, previous: [] });
  const [revision, setRevision] = useState(0);
  const scoped = pagination.scope === scope ? pagination : null;
  const cursor = scoped?.cursor ?? null;
  const query = JSON.stringify([scope, cursor, revision]);
  const liveScope = useRef(scope), liveQuery = useRef(query);
  liveScope.current = scope;
  liveQuery.current = query;

  const [listing, setListing] = useState<Listing>({ query: "", page: null, loading: false, error: "" });
  const [reading, setReading] = useState<Reading>({ query: "", detail: null, loading: false, error: "" });

  const page = ready && listing.query === query ? listing.page : null;
  const items = page?.items ?? [];
  const selected = turnId ? items.find(item => item.session_id === sessionId && item.turn_id === turnId) ?? null : null;
  // 深链详情独立读取：不依赖有界列表页命中，旧轮次可直接用授权 detail 端点读取。
  const detailTarget = useMemo(() => selected ?? (sessionId && turnId ? { session_id: sessionId, turn_id: turnId } : null), [selected, sessionId, turnId]);
  const selectionQuery = detailTarget ? JSON.stringify([scope, detailTarget.session_id, detailTarget.turn_id, selected?.run_id ?? "deep-link"]) : "";
  const selectionRef = useRef(selectionQuery);
  selectionRef.current = selectionQuery;

  useEffect(() => {
    if (!ready) return;
    const controller = new AbortController();
    const valid = () => !controller.signal.aborted && liveScope.current === scope && liveQuery.current === query;
    setListing({ query, page: null, loading: true, error: "" });
    const parameters = new URLSearchParams({ limit: "20" });
    if (cursor) parameters.set("cursor", cursor);
    if (status) parameters.set("status", status);
    // The list remains an authorized, bounded page across sessions so the context selector can switch sessions.
    void request(`/v2/workspaces/${encodeURIComponent(workspace)}/observations?${parameters}`, apiKey, undefined, controller.signal).then(raw => {
      if (!valid()) return;
      const parsed = parseObservationsPage(raw);
      if (status && parsed.items.some(item => item.status !== status)) throw new Error("观测列表筛选不匹配");
      setListing({ query, page: parsed, loading: false, error: "" });
    }).catch(error => {
      if (valid()) setListing({ query, page: null, loading: false, error: notice(error) });
    });
    return () => controller.abort();
  }, [ready, query, scope, workspace, apiKey, cursor, status]);

  useEffect(() => {
    if (!ready || !detailTarget) return;
    const controller = new AbortController();
    const itemQuery = selectionQuery;
    const target = detailTarget;
    const valid = () => !controller.signal.aborted && liveScope.current === scope && selectionRef.current === itemQuery;
    setReading({ query: itemQuery, detail: null, loading: true, error: "" });
    void request(`/v2/workspaces/${encodeURIComponent(workspace)}/sessions/${encodeURIComponent(target.session_id)}/turns/${encodeURIComponent(target.turn_id)}/observations`, apiKey, undefined, controller.signal).then(raw => {
      if (!valid()) return;
      setReading({ query: itemQuery, detail: parseTurnObservations(raw, target), loading: false, error: "" });
    }).catch(error => {
      if (valid()) setReading({ query: itemQuery, detail: null, loading: false, error: notice(error) });
    });
    return () => controller.abort();
  }, [ready, detailTarget, selectionQuery, scope, workspace, apiKey]);

  const detail = detailTarget && reading.query === selectionQuery ? reading.detail : null;
  return {
    ready,
    scope,
    items,
    page,
    loading: ready && (listing.query !== query || listing.loading),
    error: ready && listing.query === query ? listing.error : "",
    selected,
    detail,
    detailLoading: !!detailTarget && (reading.query !== selectionQuery || reading.loading),
    detailError: detailTarget && reading.query === selectionQuery ? reading.error : "",
    canPrevious: (scoped?.previous.length ?? 0) > 0,
    canNext: !!page?.next_cursor,
    refresh() {
      if (ready) {
        setPagination({ scope, cursor: null, previous: [] });
        setRevision(value => value + 1);
      }
    },
    next() {
      if (ready && page?.next_cursor && page.next_cursor !== cursor) {
        setPagination({ scope, cursor: page.next_cursor, previous: [...(scoped?.previous ?? []), cursor] });
      }
    },
    previous() {
      if (ready && scoped?.previous.length) setPagination({ scope, cursor: scoped.previous.at(-1) ?? null, previous: scoped.previous.slice(0, -1) });
    },
  };
}

export type ObservationsState = ReturnType<typeof useObservations>;

/** 按会话分组的已加载条目；聚合与概览一律标注“仅当前已加载页”。 */
export function groupBySession(items: ObservationsIndexItem[]): { session_id: string; session_title: string; items: ObservationsIndexItem[] }[] {
  const groups = new Map<string, { session_id: string; session_title: string; items: ObservationsIndexItem[] }>();
  for (const item of items) {
    const group = groups.get(item.session_id) ?? { session_id: item.session_id, session_title: item.session_title, items: [] };
    group.items.push(item);
    groups.set(item.session_id, group);
  }
  return [...groups.values()];
}
