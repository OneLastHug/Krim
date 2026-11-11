import { useEffect, useMemo, useRef, useState, type CSSProperties } from "react";
import { categoryColor, tint } from "./obs-palette";
import type { ObsCategory } from "./obs-contracts";

/** 观测图形：原生 React 实现（无新依赖）。
 * treemap 为二分 slice-dice 布局（自样板 charts.js 移植），数值守恒可测；
 * 块间 2px 间隙，文字用文本色不占用语义色，全部数值在明细表中可读。 */

export interface TreemapEntry {
  id: string;
  name: string;
  value: number;
  kind: "group" | "item" | "ghost";
  category?: ObsCategory;
  sublabel?: string;
  children?: TreemapEntry[];
}

export interface TreemapRect {
  entry: TreemapEntry;
  x: number;
  y: number;
  w: number;
  h: number;
}

const GAP = 2;

/** 二分 slice-dice：按累计值最接近一半处切分，长边优先。纯函数，供组件与测试使用。 */
export function layoutTreemap(entries: TreemapEntry[], x: number, y: number, w: number, h: number, out: TreemapRect[] = []): TreemapRect[] {
  if (!(w > 0) || !(h > 0) || entries.length === 0) return out;
  if (entries.length === 1) {
    out.push({ entry: entries[0]!, x, y, w, h });
    return out;
  }
  const total = entries.reduce((sum, entry) => sum + entry.value, 0);
  let best = 1;
  let bestDiff = Infinity;
  let acc = 0;
  for (let index = 0; index < entries.length - 1; index++) {
    acc += entries[index]!.value;
    const diff = Math.abs(acc - total / 2);
    if (diff < bestDiff) {
      bestDiff = diff;
      best = index + 1;
    }
  }
  const leftValue = entries.slice(0, best).reduce((sum, entry) => sum + entry.value, 0);
  const fraction = total > 0 ? leftValue / total : best / entries.length;
  if (w >= h) {
    const cut = Math.min(Math.max(Math.round(w * fraction), 1), w - 1);
    layoutTreemap(entries.slice(0, best), x, y, cut - GAP / 2, h, out);
    layoutTreemap(entries.slice(best), x + cut + GAP / 2, y, w - cut - GAP / 2, h, out);
  } else {
    const cut = Math.min(Math.max(Math.round(h * fraction), 1), h - 1);
    layoutTreemap(entries.slice(0, best), x, y, w, cut - GAP / 2, out);
    layoutTreemap(entries.slice(best), x, y + cut + GAP / 2, w, h - cut - GAP / 2, out);
  }
  return out;
}

function useElementSize<T extends HTMLElement>() {
  const ref = useRef<T | null>(null);
  const [size, setSize] = useState({ width: 0, height: 0 });
  useEffect(() => {
    const element = ref.current;
    if (!element) return;
    const measure = () => setSize({ width: element.clientWidth, height: element.clientHeight });
    measure();
    if (typeof ResizeObserver !== "function") {
      window.addEventListener("resize", measure);
      return () => window.removeEventListener("resize", measure);
    }
    const observer = new ResizeObserver(measure);
    observer.observe(element);
    return () => observer.disconnect();
  }, []);
  return { ref, size };
}

function entryColor(entry: TreemapEntry, dark: boolean): string {
  return entry.category ? categoryColor(entry.category, dark) : "var(--muted-foreground)";
}

function tileLabel(rect: TreemapRect): { name: boolean; sub: boolean; category: boolean } {
  return {
    name: rect.w >= 56 && rect.h >= 24,
    sub: rect.w >= 92 && rect.h >= 42 && !!rect.entry.sublabel,
    category: rect.entry.kind === "group" && rect.w >= 130 && rect.h >= 60 && !!rect.entry.category,
  };
}

function TreemapTile({ rect, total, dark, selected, onSelect }: {
  rect: TreemapRect;
  total: number;
  dark: boolean;
  selected: string | null;
  onSelect?: (id: string) => void;
}) {
  const entry = rect.entry;
  if (rect.w < 2 || rect.h < 2) return null;
  const x = Math.round(rect.x);
  const y = Math.round(rect.y);
  const w = Math.round(rect.x + rect.w) - x;
  const h = Math.round(rect.y + rect.h) - y;
  if (w < 1 || h < 1) return null;
  const color = entryColor(entry, dark);
  const isSelected = entry.kind === "item" && selected === entry.id;
  const style: CSSProperties = {
    left: x,
    top: y,
    width: w,
    height: h,
    background: entry.kind === "ghost" ? "var(--muted)" : tint(color, isSelected ? 0.3 : 0.14),
    borderColor: entry.kind === "ghost" ? "var(--border)" : color,
    color: entry.kind === "ghost" ? "var(--muted-foreground)" : "var(--foreground)",
    opacity: entry.kind === "ghost" ? 0.55 : 1,
  };
  const label = tileLabel(rect);
  const interactive = entry.kind !== "ghost" && !!onSelect;
  const ariaValue = entry.sublabel ? `，${entry.sublabel}` : "";
  return <button
    type="button"
    className={`obs-tile${entry.kind === "ghost" ? " obs-tile-ghost" : ""}`}
    style={style}
    aria-pressed={isSelected || undefined}
    aria-label={`${entry.name}：${entry.value}（${total > 0 ? Math.round(entry.value / total * 100) : 0}%）${ariaValue}`}
    title={`${entry.name}${ariaValue}`}
    disabled={!interactive}
    onClick={interactive ? () => onSelect!(entry.id) : undefined}
  >
    {label.category && entry.category && <span className="obs-tile-cat">{entry.name}</span>}
    {label.name && entry.kind !== "group" && <span className="obs-tile-name">{entry.name}</span>}
    {label.name && entry.kind === "group" && !label.category && <span className="obs-tile-name">{entry.name}</span>}
    {label.sub && <span className="obs-tile-sub">{entry.sublabel}</span>}
  </button>;
}

export function ObsTreemap({ entries, height, dark, selected, onSelect, label }: {
  entries: TreemapEntry[];
  height: number;
  dark: boolean;
  selected: string | null;
  onSelect?: (id: string) => void;
  label: string;
}) {
  const { ref, size } = useElementSize<HTMLDivElement>();
  const total = entries.reduce((sum, entry) => sum + entry.value, 0);
  const rects = useMemo(() => {
    if (size.width < 80 || height < 60 || !(total > 0)) return [];
    const top: TreemapRect[] = [];
    layoutTreemap(entries, 0, 0, size.width, height, top);
    const all: TreemapRect[] = [];
    for (const rect of top) {
      if (rect.entry.children?.length) layoutTreemap(rect.entry.children, rect.x, rect.y, rect.w, rect.h, all);
      else all.push(rect);
    }
    return all;
  }, [entries, size.width, height, total]);
  return <div ref={ref} className="obs-treemap" style={{ height }} role="group" aria-label={label}>
    {!(total > 0) && <p className="obs-treemap-empty">没有可展示的内容数据</p>}
    {rects.map(rect => <TreemapTile key={rect.entry.id + ":" + Math.round(rect.x) + "," + Math.round(rect.y)} rect={rect} total={total} dark={dark} selected={selected} onSelect={onSelect}/>)}
  </div>;
}

export interface SparkPoint { id: string; label: string; value: number | null }
export interface SparkEvent { index: number; label: string }

export function ObsSparkline({ points, events = [], selected, onSelect, height = 96, ariaLabel }: {
  points: SparkPoint[];
  events?: SparkEvent[];
  selected: string | null;
  onSelect?: (id: string) => void;
  height?: number;
  ariaLabel: string;
}) {
  const { ref, size } = useElementSize<HTMLDivElement>();
  const width = size.width;
  const pad = { top: 14, right: 14, bottom: 24, left: 46 };
  const plotW = Math.max(10, width - pad.left - pad.right);
  const plotH = Math.max(10, height - pad.top - pad.bottom);
  const knowns = points.filter(point => point.value !== null && Number.isFinite(point.value));
  const lo = knowns.length ? Math.min(...knowns.map(point => point.value!)) : 0;
  const hiRaw = knowns.length ? Math.max(...knowns.map(point => point.value!)) : 1;
  const hi = hiRaw === lo ? lo + 1 : hiRaw;
  const xAt = (index: number) => points.length > 1 ? pad.left + index / (points.length - 1) * plotW : pad.left + plotW / 2;
  const yAt = (value: number) => pad.top + (1 - (value - lo) / (hi - lo)) * plotH;
  const compact = (value: number) => value >= 10_000 ? Math.round(value / 1000) + "K" : value >= 1000 ? (value / 1000).toFixed(1) + "K" : String(Math.round(value));
  const path = knowns.map((point, index) => `${index ? "L" : "M"}${xAt(points.indexOf(point)).toFixed(1)} ${yAt(point.value!).toFixed(1)}`).join("");
  return <div ref={ref} style={{ height }} className="obs-spark">
    {width >= 90 && <svg width={width} height={height} role="group" aria-label={ariaLabel}>
      {[0, 0.5, 1].map(fraction => <line key={fraction} className="obs-grid" x1={pad.left} x2={pad.left + plotW} y1={pad.top + fraction * plotH} y2={pad.top + fraction * plotH}/>)}
      <line className="obs-axis" x1={pad.left} x2={pad.left} y1={pad.top} y2={pad.top + plotH}/>
      <line className="obs-axis" x1={pad.left} x2={pad.left + plotW} y1={pad.top + plotH} y2={pad.top + plotH}/>
      <text className="obs-axis-label" x={pad.left - 6} y={pad.top + 4} textAnchor="end">{compact(hi)}</text>
      <text className="obs-axis-label" x={pad.left - 6} y={pad.top + plotH + 4} textAnchor="end">{compact(lo)}</text>
      {points.length > 0 && <>
        <text className="obs-axis-label" x={pad.left} y={height - 6}>{(points[0]!.label || "#1").slice(0, 14)}</text>
        {points.length > 1 && <text className="obs-axis-label" x={pad.left + plotW} y={height - 6} textAnchor="end">{(points[points.length - 1]!.label || "#" + points.length).slice(0, 14)}</text>}
      </>}
      {knowns.length === 0 && <text className="obs-axis-label" x={pad.left + plotW / 2} y={pad.top + plotH / 2} textAnchor="middle">暂无数值点</text>}
      {events.map(event => {
        const index = Math.min(Math.max(0, Math.round(event.index)), Math.max(0, points.length - 1));
        const x = xAt(index);
        return <g key={`${event.index}-${event.label}`}>
          <line className="obs-event" x1={x} x2={x} y1={pad.top + 4} y2={pad.top + plotH}/>
          <path className="obs-event-mark" d={`M${x} ${pad.top - 4} l4.5 4.5 l-4.5 4.5 l-4.5 -4.5 Z`}/>
        </g>;
      })}
      {path && <path className="obs-line" d={path}/>}
      {knowns.filter(point => point.id === selected).map(point => <circle key={point.id} className="obs-dot-selected" cx={xAt(points.indexOf(point))} cy={yAt(point.value!)} r={4}/>)}
      {points.map((point, index) => {
        const known = point.value !== null && Number.isFinite(point.value);
        const spacing = points.length > 1 ? plotW / (points.length - 1) : 60;
        const hitWidth = Math.min(Math.max(spacing, 24), 60);
        return <rect
          key={point.id}
          className="obs-hit"
          x={xAt(index) - hitWidth / 2}
          y={pad.top + 4}
          width={hitWidth}
          height={plotH - 4}
          tabIndex={0}
          role={onSelect && known ? "button" : "img"}
          aria-pressed={point.id === selected || undefined}
          aria-label={known ? `${point.label}：${point.value}` : `${point.label}：未知`}
          onClick={onSelect && known ? () => onSelect!(point.id) : undefined}
          onKeyDown={onSelect && known ? event => { if (event.key === "Enter" || event.key === " ") { event.preventDefault(); onSelect!(point.id); } } : undefined}
        />;
      })}
    </svg>}
  </div>;
}
