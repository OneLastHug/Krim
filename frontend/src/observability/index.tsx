import { Suspense, lazy, useCallback, useEffect, useRef, useState } from "react";
import type { Actor } from "../lib/session-contracts";
import type { WorkbenchView } from "../lib/workbench-route";
import type { DisplayTheme } from "../lib/use-display-theme";
import { readObsState, writeObsState, type ObsState } from "./obs-route";
import { useObservations } from "./use-observations";
import { ObsEmpty } from "./obs-ui";
import { ContextView } from "./context-view";
import { RunsView } from "./runs-view";
import { SessionView } from "./session-view";
import { OverviewView } from "./overview-view";
import { StatusView } from "./status-view";
import "./obs.css";

/** 观测工作区：挂在原 WorkbenchLayout 主内容区的内嵌模块。
 * 页面按需加载（React.lazy），数据与筛选集中在模块根部，
 * URL /obs* 参数保存页面、筛选、选中对象与分布模式，浏览器回退可恢复。 */

const EvaluationsView = lazy(() => import("./quality").then(module => ({ default: module.ObservabilityEvaluations })));
const ReviewView = lazy(() => import("./quality").then(module => ({ default: module.ObservabilityReview })));
const scrollPositions = new Map<string, number>();
const maxScrollPositions = 80;

export interface ObservabilityModuleProps {
  apiKey: string;
  workspace: string;
  identity: Actor | null;
  identityLoading: boolean;
  theme: DisplayTheme;
  page: ObsState["page"];
  onPageChange: (page: ObsState["page"]) => void;
  onNavigate: (view: WorkbenchView) => void;
}

function pushUrl(next: URL, replace: boolean) {
  if (replace) window.history.replaceState(null, "", next);
  else window.history.pushState(null, "", next);
}

export function ObservabilityModule({ apiKey, workspace, identity, identityLoading, theme, page, onPageChange, onNavigate }: ObservabilityModuleProps) {
  const dark = theme === "dark";
  const [state, setState] = useState<ObsState>(() => readObsState(new URL(location.href)));
  const scrollRef = useRef<HTMLDivElement | null>(null);
  const previousIdentityScope = useRef<string | null>(null);
  const routeKey = JSON.stringify([
    identity?.principal_id ?? "", identity?.generation ?? null, workspace, state.page,
    state.page === "context" ? [state.session, state.turn, state.request, state.base, state.item, state.mode]
      : state.page === "runs" ? [state.status, state.run, state.item]
        : state.page === "session" ? [state.session, state.turn]
          : [state.status],
  ]);

  // Hydrate a deep-linked page from the host route, but never clear its initial selection.
  useEffect(() => {
    setState(previous => previous.page === page ? previous : { ...previous, page });
  }, [page]);

  // Clear selections only after an established identity/workspace scope truly changes.
  useEffect(() => {
    if (identityLoading) return;
    const nextScope = JSON.stringify([workspace, identity?.principal_id ?? "", identity?.generation ?? null]);
    const previous = previousIdentityScope.current;
    previousIdentityScope.current = nextScope;
    if (previous === null || previous === nextScope) return;
    setState(current => {
      const next = { ...current, session: "", turn: "", run: "", request: "", base: "", item: "" };
      pushUrl(writeObsState(new URL(location.href), next), true);
      return next;
    });
  }, [workspace, identity?.principal_id, identity?.generation, identityLoading]);

  useEffect(() => {
    const onPopState = () => {
      setState(readObsState(new URL(location.href)));
    };
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  const onState = useCallback((next: ObsState, replace = false) => {
    setState(next);
    onPageChange(next.page);
    pushUrl(writeObsState(new URL(location.href), next), replace);
  }, [onPageChange]);

  const data = useObservations({
    apiKey,
    workspace,
    identity,
    active: !!identity && !identityLoading,
    status: state.status,
    sessionId: state.session,
    turnId: state.turn,
  });

  // Default context view chooses an actual authorized turn after the first bounded page arrives.
  useEffect(() => {
    if (identityLoading || !identity || state.page !== "context" || state.turn || !data.items.length) return;
    const item = state.session
      ? data.items.find(entry => entry.session_id === state.session)
      : data.items[0];
    if (!item) return;
    onState({ ...state, session: item.session_id, turn: item.turn_id, request: "", base: "", run: "", item: "" }, true);
  }, [identityLoading, identity, state, data.items, onState]);

  useEffect(() => {
    const moduleRoot = scrollRef.current;
    if (!moduleRoot) return;
    const element = (moduleRoot.closest(".workbench-page") as HTMLElement | null) ?? moduleRoot;
    // 数据到达前高度可能不足以支撑目标 scrollTop；恢复完成前不记录新位置，
    // 避免把 clamp 成 0 的中间态覆盖存量。
    let restored = false;
    let pending: number | undefined = scrollPositions.get(routeKey);
    const save = () => {
      if (!restored || new URL(location.href).searchParams.get("view") !== "observe") return;
      scrollPositions.set(routeKey, element.scrollTop);
      if (scrollPositions.size > maxScrollPositions) {
        const oldest = scrollPositions.keys().next().value;
        if (oldest !== undefined) scrollPositions.delete(oldest);
      }
    };
    const tryRestore = () => {
      const target = pending ?? 0;
      element.scrollTop = target;
      if (pending === undefined || Math.abs(element.scrollTop - target) <= 1) {
        restored = true;
        pending = undefined;
        return true;
      }
      return false;
    };
    if (!tryRestore()) {
      const observer = typeof ResizeObserver === "undefined" ? null : new ResizeObserver(() => {
        if (tryRestore()) observer?.disconnect();
      });
      observer?.observe(moduleRoot);
      element.addEventListener("scroll", save, { passive: true });
      return () => {
        observer?.disconnect();
        element.removeEventListener("scroll", save);
      };
    }
    element.addEventListener("scroll", save, { passive: true });
    return () => element.removeEventListener("scroll", save);
  }, [routeKey, identityLoading, data.loading, data.detailLoading, data.detail]);

  if (identityLoading) {
    return <div ref={scrollRef} className="observability-root"><ObsEmpty title="正在读取身份…"/></div>;
  }
  if (!identity) {
    return <div ref={scrollRef} className="observability-root">
      <ObsEmpty title="需要登录后使用观测">
        <button type="button" className="obs-button" onClick={() => onNavigate("settings")}>前往账号设置</button>
      </ObsEmpty>
    </div>;
  }
  const membership = identity.workspaces.find(value => value.workspace_id === workspace);
  if (!membership) {
    return <div ref={scrollRef} className="observability-root"><ObsEmpty title="当前工作区不可用">切换工作区后查看观测。</ObsEmpty></div>;
  }

  const viewProps = { apiKey, workspace, identity, onNavigate, state, onState, dark, data };

  return <div ref={scrollRef} className="observability-root" data-obs-page={state.page}>
    <div className="obs-content">
      <div className="obs-page-body" key={state.page}>
        <Suspense fallback={<ObsEmpty title="正在加载页面…"/>}>
          {state.page === "overview" && <OverviewView {...viewProps}/>}
          {state.page === "runs" && <RunsView {...viewProps}/>}
          {state.page === "session" && <SessionView {...viewProps}/>}
          {state.page === "context" && <ContextView {...viewProps}/>}
          {state.page === "eval" && <EvaluationsView {...viewProps}/>}
          {state.page === "review" && <ReviewView {...viewProps}/>}
          {state.page === "status" && <StatusView {...viewProps}/>}
        </Suspense>
      </div>
    </div>
  </div>;
}
