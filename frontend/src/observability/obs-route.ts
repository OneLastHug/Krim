/** 原 Trace 分区内的观测页状态；view=observe 仅兼容已有链接。
 * 读取原 session/turn 深链，新增筛选与选中对象继续使用 obs 前缀。 */

export type ObsPage = "overview" | "runs" | "session" | "context" | "eval" | "review" | "status";

export const obsPages: { id: ObsPage; label: string }[] = [
  { id: "overview", label: "概览" },
  { id: "runs", label: "运行" },
  { id: "session", label: "会话" },
  { id: "context", label: "上下文" },
  { id: "eval", label: "评测" },
  { id: "review", label: "复核" },
];

export const obsStatusLabel = "数据状态";

const knownPages = new Set<ObsPage>(["overview", "runs", "session", "context", "eval", "review", "status"]);
const paramNames = ["obs", "obs_status", "obs_session", "obs_turn", "obs_req", "obs_base", "obs_run", "obs_item", "obs_mode"] as const;
const idPattern = /^[\w:.-]{0,256}$/;

export interface ObsState {
  page: ObsPage;
  status: string;
  session: string;
  turn: string;
  request: string;
  base: string;
  run: string;
  /** 页内选中对象：上下文页为清单条目 key，运行页为 span id。 */
  item: string;
  /** 上下文分布图范围：content 字节刻度（默认）或 window 估算 token（窗口未知时不可用）。 */
  mode: "content" | "window";
}

export const defaultObsState: ObsState = { page: "context", status: "", session: "", turn: "", request: "", base: "", run: "", item: "", mode: "content" };

function readId(value: string | null): string {
  return value && idPattern.test(value) ? value : "";
}

/** 读取观测深链；非法或缺省值回退到默认上下文页。 */
export function readObsState(url: URL): ObsState {
  const page = url.searchParams.get("obs");
  const mode = url.searchParams.get("obs_mode");
  const legacyTrace = url.searchParams.get("view") === "trace" && !url.searchParams.has("obs_session");
  const session = readId(url.searchParams.get("obs_session") ?? (legacyTrace ? url.searchParams.get("session") : null));
  const turn = readId(url.searchParams.get("obs_turn") ?? (legacyTrace ? url.searchParams.get("turn") : null));
  return {
    page: page && knownPages.has(page as ObsPage) ? page as ObsPage : legacyTrace && session && turn ? "runs" : "context",
    status: url.searchParams.get("obs_status") ?? "",
    session,
    turn,
    request: readId(url.searchParams.get("obs_req")),
    base: readId(url.searchParams.get("obs_base")),
    run: readId(url.searchParams.get("obs_run")),
    item: readId(url.searchParams.get("obs_item")),
    mode: mode === "window" ? "window" : "content",
  };
}

/** 写回观测参数；空值删除对应参数，保持 URL 精简。 */
export function writeObsState(url: URL, state: ObsState): URL {
  const next = new URL(url.href);
  next.searchParams.set("view", "trace");
  next.searchParams.delete("session");
  next.searchParams.delete("turn");
  const values: Record<(typeof paramNames)[number], string> = {
    obs: state.page === "context" ? "" : state.page,
    obs_status: state.status,
    obs_session: state.session,
    obs_turn: state.turn,
    obs_req: state.request,
    obs_base: state.base,
    obs_run: state.run,
    obs_item: state.item,
    obs_mode: state.mode === "window" ? "window" : "",
  };
  for (const name of paramNames) {
    const value = values[name];
    if (value) next.searchParams.set(name, value);
    else next.searchParams.delete(name);
  }
  return next;
}

/** 工作区或身份变化时需要清除的观测参数。 */
export const obsWorkspaceParamNames = [...paramNames];

export function obsStateKey(state: ObsState): string {
  return JSON.stringify([state.page, state.status, state.session, state.turn, state.request, state.base, state.run, state.item, state.mode]);
}
