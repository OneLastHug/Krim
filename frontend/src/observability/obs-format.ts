/** 观测页面共用格式化：bytes 为准确分母，token 显示为估算；未知保持未知。 */

export function formatBytes(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  const abs = Math.abs(value);
  const sign = value < 0 ? "−" : "";
  if (abs >= 1_000_000) return sign + (abs / 1_000_000).toFixed(abs >= 10_000_000 ? 0 : 1).replace(/\.0$/, "") + " MB";
  if (abs >= 1_000) return sign + (abs / 1_000).toFixed(abs >= 10_000 ? 0 : 1).replace(/\.0$/, "") + " kB";
  return sign + String(Math.round(abs)) + " B";
}

/** estimated_tokens：估算值带 ≈ 前缀，null 显示"未知"。 */
export function formatTokens(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "未知";
  const abs = Math.abs(value);
  const sign = value < 0 ? "−" : "";
  if (abs >= 1_000_000) return "≈" + sign + (abs / 1_000_000).toFixed(1).replace(/\.0$/, "") + "M";
  if (abs >= 10_000) return "≈" + sign + Math.round(abs / 1_000) + "K";
  if (abs >= 1_000) return "≈" + sign + (abs / 1_000).toFixed(1).replace(/\.0$/, "") + "K";
  return "≈" + sign + String(Math.round(abs));
}

export function formatCount(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  return new Intl.NumberFormat("zh-CN").format(value);
}

export function formatReportedTokens(value: number | null | undefined): string {
  return value === null || value === undefined || !Number.isFinite(value) ? "未知" : formatCount(value);
}

export function formatPercent(part: number, whole: number): string {
  if (!(whole > 0) || !(part >= 0)) return "—";
  const ratio = part / whole * 100;
  if (ratio >= 10) return Math.round(ratio) + "%";
  if (ratio >= 1) return ratio.toFixed(1) + "%";
  return "<1%";
}

export function formatDeltaBytes(value: number): string {
  if (value === 0) return "0 B";
  return (value > 0 ? "+" : "") + formatBytes(value);
}

/** 恒为 null + unpriced 的费用：显示"未计价"，不推算、不加货币符号。 */
export function formatCost(cost: null, status: string): string {
  return cost === null && status === "unpriced" ? "未计价" : "—";
}

export function formatDuration(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || !Number.isFinite(ms)) return "—";
  if (ms < 1000) return Math.round(ms) + " ms";
  return (ms / 1000).toFixed(ms < 10_000 ? 2 : 1) + " s";
}

export function formatTime(seconds: number | null | undefined): string {
  if (seconds === null || seconds === undefined || !Number.isFinite(seconds)) return "—";
  const date = new Date(seconds * 1000);
  return Number.isNaN(date.getTime()) ? "—" : date.toLocaleString("zh-CN", { hour12: false });
}

export function formatClock(seconds: number | null | undefined): string {
  if (seconds === null || seconds === undefined || !Number.isFinite(seconds)) return "—";
  const date = new Date(seconds * 1000);
  return Number.isNaN(date.getTime()) ? "—" : date.toLocaleTimeString("zh-CN", { hour12: false });
}

/** duration_source 口径：mono 单调时钟 / wall_clock 事件墙钟差值。 */
export function durationSourceLabel(source: string | null | undefined): string {
  if (source === "mono") return "单调时钟";
  if (source === "wall_clock") return "墙钟差值";
  return source || "口径未知";
}
