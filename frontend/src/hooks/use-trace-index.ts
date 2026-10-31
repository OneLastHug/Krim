import { useEffect, useRef, useState } from "react";
import { request, RequestError } from "../api";
import { turnPath, type Actor } from "../lib/session-contracts";
import { parseTraceDetail, parseTracePage, type TraceDetail, type TraceIndexItem, type TraceIndexPage, type TraceStatus } from "../lib/trace-contracts";

type Pagination = { scope: string; cursor: string | null; previous: (string | null)[] };
type Listing = { query: string; page: TraceIndexPage | null; loading: boolean; error: string };
type Selection = { scope: string; item: TraceIndexItem };
type Reading = { query: string; runId: string; detail: TraceDetail | null; loading: boolean; error: string };

function notice(error: unknown): string {
  if (error instanceof RequestError) {
    if (error.status === 401) return "请重新连接工作区。";
    if (error.status === 403 || error.status === 404) return "记录不存在或当前身份无权读取。";
    if (error.code === "closed") return "服务已关闭，请重新连接。";
  }
  if (error instanceof Error && error.message.startsWith("Trace")) return error.message;
  return "读取失败，请刷新重试。";
}

export interface TraceIndexOptions {
  apiKey: string;
  workspace: string;
  identity: Actor | null;
  active: boolean;
  sessionId?: string;
  turnId?: string;
  onSelection?: (runId: string) => void;
  onSession?: (sessionId: string) => void;
}

export function useTraceIndex({ apiKey, workspace, identity, active, sessionId, turnId, onSelection, onSession }: TraceIndexOptions) {
  const [status, setStatus] = useState<TraceStatus | "">("");
  const [pagination, setPagination] = useState<Pagination>({ scope: "", cursor: null, previous: [] });
  const [revision, setRevision] = useState(0);
  const membership = identity?.workspaces.find(value => value.workspace_id === workspace);
  const ready = active && !!identity && !!membership && !!workspace;
  const makeScope = (filterStatus: TraceStatus | "") => JSON.stringify([
    apiKey, identity?.principal_id ?? "", identity?.generation ?? null, workspace,
    membership?.role ?? "", active, filterStatus, sessionId ?? "", turnId ?? "",
  ]);
  const scope = makeScope(status);
  const scopedPagination = pagination.scope === scope ? pagination : null;
  const cursor = scopedPagination?.cursor ?? null;
  const previous = scopedPagination?.previous ?? [];
  const query = JSON.stringify([scope, cursor, revision]);
  const currentScope = useRef(scope), currentQuery = useRef(query);
  const selectionCallback = useRef(onSelection), sessionCallback = useRef(onSession);
  currentScope.current = scope;
  currentQuery.current = query;
  selectionCallback.current = onSelection;
  sessionCallback.current = onSession;

  const pageSequence = useRef(0), detailSequence = useRef(0);
  const [listing, setListing] = useState<Listing>({ query: "", page: null, loading: false, error: "" });
  const [selection, setSelection] = useState<Selection | null>(null);
  const [reading, setReading] = useState<Reading>({ query: "", runId: "", detail: null, loading: false, error: "" });
  const selected = ready && selection?.scope === scope ? selection.item : null;
  const selectedRunId = selected?.run_id ?? "";
  const selectedSessionId = selected?.session_id ?? "";
  const selectedTurnId = selected?.turn_id ?? "";
  const selectedTraceId = selected?.trace_id ?? "";
  const detailQuery = JSON.stringify([scope, selectedRunId, selectedSessionId, selectedTurnId, selectedTraceId]);
  const currentDetailQuery = useRef(detailQuery);
  currentDetailQuery.current = detailQuery;

  useEffect(() => {
    if (!ready) return;
    const controller = new AbortController();
    const sequence = ++pageSequence.current;
    const valid = () => !controller.signal.aborted && pageSequence.current === sequence && currentScope.current === scope && currentQuery.current === query;
    setListing({ query, page: null, loading: true, error: "" });
    const parameters = new URLSearchParams({ limit: "20" });
    if (cursor) parameters.set("cursor", cursor);
    if (status) parameters.set("status", status);
    if (sessionId) parameters.set("session_id", sessionId);
    if (turnId) parameters.set("turn_id", turnId);
    void request(`/v2/workspaces/${encodeURIComponent(workspace)}/traces?${parameters}`, apiKey, undefined, controller.signal).then(raw => {
      if (!valid()) return;
      const page = parseTracePage(raw);
      if (page.items.some(item => (sessionId && item.session_id !== sessionId) || (turnId && item.turn_id !== turnId) || (status && item.status !== status))) throw new Error("Trace 筛选来源不匹配");
      if (sessionId && turnId && page.items.length > 1) throw new Error("Trace 深链结果不唯一");
      setListing({ query, page, loading: false, error: "" });
      if (sessionId && turnId) {
        const item = page.items[0];
        if (!item) setSelection(previousSelection => previousSelection?.scope === scope ? null : previousSelection);
        else if (selection?.scope !== scope || selection.item.run_id !== item.run_id) {
          setSelection({ scope, item });
          selectionCallback.current?.(item.run_id);
        }
      }
    }).catch(error => {
      if (valid()) setListing({ query, page: null, loading: false, error: notice(error) });
    });
    return () => controller.abort();
  }, [ready, query, scope, workspace, apiKey, cursor, status, sessionId, turnId]);

  useEffect(() => {
    if (!ready || !selectedRunId || !selectedSessionId || !selectedTurnId || !selectedTraceId) return;
    const controller = new AbortController();
    const sequence = ++detailSequence.current;
    const requestQuery = detailQuery;
    const item = selected;
    if (!item) return;
    const valid = () => !controller.signal.aborted && detailSequence.current === sequence && currentScope.current === scope && currentDetailQuery.current === requestQuery;
    setReading({ query: requestQuery, runId: selectedRunId, detail: null, loading: true, error: "" });
    void request(turnPath(workspace, selectedSessionId, selectedTurnId) + "/trace", apiKey, undefined, controller.signal).then(raw => {
      if (!valid()) return;
      const detail = parseTraceDetail(raw, item);
      setReading({ query: requestQuery, runId: selectedRunId, detail, loading: false, error: "" });
    }).catch(error => {
      if (valid()) setReading({ query: requestQuery, runId: selectedRunId, detail: null, loading: false, error: notice(error) });
    });
    return () => controller.abort();
  }, [ready, detailQuery, selectedRunId, selectedSessionId, selectedTurnId, selectedTraceId, scope, workspace, apiKey, revision]);

  const page = ready && listing.query === query ? listing.page : null;
  const detailVisible = !!selectedRunId && reading.query === detailQuery && reading.runId === selectedRunId;
  return {
    ready, status, selected, page,
    loading: ready && (listing.query !== query || listing.loading),
    error: ready && listing.query === query ? listing.error : "",
    detail: detailVisible ? reading.detail : null,
    detailLoading: !!selectedRunId && (!detailVisible || reading.loading),
    detailError: detailVisible ? reading.error : "",
    canPrevious: previous.length > 0,
    filter(value: TraceStatus | "") {
      setPagination({ scope: makeScope(value), cursor: null, previous: [] });
      setStatus(value);
    },
    refresh() { if (ready) setRevision(value => value + 1); },
    next() {
      if (ready && page?.next_cursor && page.next_cursor !== cursor) {
        setPagination({ scope, cursor: page.next_cursor, previous: [...previous, cursor] });
      }
    },
    previous() {
      if (ready && previous.length) setPagination({ scope, cursor: previous.at(-1) ?? null, previous: previous.slice(0, -1) });
    },
    choose(item: TraceIndexItem) {
      if (!ready || currentScope.current !== scope || !page?.items.some(row => row.run_id === item.run_id)) return;
      setSelection({ scope, item });
      selectionCallback.current?.(item.run_id);
    },
    returnToSession(session = selectedSessionId) {
      if (session) sessionCallback.current?.(session);
    },
  };
}
