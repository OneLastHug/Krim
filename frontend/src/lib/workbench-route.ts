export type WorkbenchView = "chat" | "run" | "trace" | "datasets" | "eval" | "settings";

const knownViews = new Set<WorkbenchView>(["chat", "run", "trace", "datasets", "eval", "settings"]);

/** 读取新工作台和旧版 query/hash 深链接的当前页。 */
export function readWorkbenchView(url: URL): WorkbenchView {
  const explicit = url.searchParams.get("view");
  // 已生成的观测链接兼容到原 Trace 分区，不再创建第二个工作台入口。
  if (explicit === "observe") return "trace";
  if (explicit && knownViews.has(explicit as WorkbenchView)) return explicit as WorkbenchView;
  if (url.searchParams.has("eval_id") || url.searchParams.has("product_eval_id")) return "eval";
  if (url.searchParams.has("session") || url.searchParams.has("workspace") || url.searchParams.has("turn")) return "chat";
  if (url.searchParams.has("run_id") || url.hash.length > 1) return "run";
  // 首页改为对话；旧版 run hash 与 eval query 在上面继续按原协议定位。
  return "chat";
}

export const workbenchTitles: Record<WorkbenchView, string> = {
  chat: "对话",
  run: "运行记录",
  trace: "Trace",
  datasets: "数据集",
  eval: "评测",
  settings: "设置",
};
