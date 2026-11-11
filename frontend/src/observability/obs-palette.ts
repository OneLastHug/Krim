import type { ObsCategory } from "./obs-contracts";

/** 观测类别色：浅深两套均通过 dataviz 六项校验（相邻序 CVD ≥9.6、正常视觉 ≥16、
 * 色度下限、亮度带、对比度；浅色金/粉两项对比 WARN 由常显图例与明细表提供 relief）。
 * 验证脚本：validate_palette.js，浅色 #ffffff、深色 #191c1e（正式工作台表面）。
 * 类别顺序即图例顺序，颜色绑定类别不随排名变化；unknown 为中性灰，始终带文字。 */

export interface ObsCategoryColor { color: string; darkColor: string }

export const obsPalette: Record<ObsCategory, ObsCategoryColor> = {
  system: { color: "#245f9e", darkColor: "#3a66ad" },
  tools: { color: "#1e97cc", darkColor: "#389fc4" },
  files: { color: "#1a8a4f", darkColor: "#237e47" },
  tool_result: { color: "#c9a53a", darkColor: "#b08d35" },
  memory: { color: "#cf7fae", darkColor: "#c4769e" },
  history: { color: "#bf4d1f", darkColor: "#b84d24" },
  summary: { color: "#9d7dd2", darkColor: "#9d7dd2" },
  unknown: { color: "#8a9196", darkColor: "#7d868b" },
};

/** 时间线/趋势的图形序列色：模型与工具两个序列，浅深各自验证过对比。 */
export const obsSeries = {
  model: { color: "#245f9e", darkColor: "#5b93d6" },
  tool: { color: "#1a8a4f", darkColor: "#4fae72" },
};

export function categoryColor(category: ObsCategory, dark: boolean): string {
  const entry = obsPalette[category];
  return dark ? entry.darkColor : entry.color;
}

/** hex → rgba 染色，用于树图块的低透明底色（边框保持实色）。 */
export function tint(hex: string, alpha: number): string {
  const match = /^#([0-9a-f]{6})$/i.exec(hex);
  if (!match) return hex;
  const value = match[1]!;
  const channels = [0, 2, 4].map(start => parseInt(value.slice(start, start + 2), 16));
  return `rgba(${channels.join(",")},${alpha})`;
}
