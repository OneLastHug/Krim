import { obsCategories, obsCategoryNames } from "../observability/obs-contracts";
import { obsPalette } from "../observability/obs-palette";
import type { SessionContextState } from "../hooks/use-session-context";
import { Button } from "./ui/button";

export const contextCount = (value: number | null) => value === null ? "—" : value >= 1000 ? `${(value / 1000).toLocaleString(undefined, { maximumFractionDigits: 1 })}k` : value.toLocaleString();

export function ContextSummary({ context, onTrace, dark = document.documentElement.classList.contains("dark") }: { context: SessionContextState; onTrace?: () => void; dark?: boolean }) {
  const { manifest, used, limit } = context;
  const ratio = used !== null && limit ? used / limit : null;
  const rows = obsCategories.map(category => ({ category, bytes: manifest?.items.filter(item => item.category === category).reduce((sum, item) => sum + item.input_bytes, 0) ?? 0 }));
  const known = rows.reduce((sum, item) => sum + item.bytes, 0);
  if (manifest && known < manifest.input_bytes) rows.find(item => item.category === "unknown")!.bytes += manifest.input_bytes - known;
  const total = Math.max(manifest?.input_bytes ?? 0, known);
  return <div className="context-summary" aria-label="上下文用量与来源">
    <div className="g-context-overview"><strong>{ratio === null ? "上下文用量" : `${(ratio * 100).toFixed(1)}% 已用`}</strong><span>{contextCount(used)} / {contextCount(limit)} tokens</span></div>
    {context.loading && <p className="dev-context-note" role="status">正在读取上一完成轮次…</p>}
    {context.error && <div role="status" className="dev-context-note">{context.error}<Button variant="ghost" size="sm" onClick={context.refresh}>重试</Button></div>}
    {!context.turnId && <p className="dev-context-note">发送首条消息后统计各来源占比。</p>}
    {manifest && <>
      <svg className="context-composition" viewBox="0 0 300 10" preserveAspectRatio="none" role="img" aria-label="上下文输入字节构成">
        {rows.map((item, index) => <rect key={item.category} x={total ? rows.slice(0, index).reduce((sum, row) => sum + row.bytes, 0) / total * 300 : 0} y="0" width={total ? Math.max(0, item.bytes / total * 300 - 1) : 0} height="10" fill={dark ? obsPalette[item.category].darkColor : obsPalette[item.category].color}><title>{obsCategoryNames[item.category]}：{item.bytes.toLocaleString()} 字节</title></rect>)}
      </svg>
      <table className="g-context-table" aria-label="上下文来源明细"><tbody>{rows.filter(item => item.bytes > 0).map(item => <tr key={item.category}><td><svg aria-hidden="true" width="8" height="8"><rect width="8" height="8" fill={dark ? obsPalette[item.category].darkColor : obsPalette[item.category].color}/></svg>{obsCategoryNames[item.category]}</td><td>{contextCount(item.bytes)} B</td><td>{total ? (item.bytes / total * 100).toFixed(1) : "0"}%</td></tr>)}</tbody></table>
      <p className="dev-context-note">最新完成轮次 · tokens 为估算值；来源占比按输入字节计算。{manifest.items_truncated ? "部分来源已截断。" : ""}</p>
    </>}
    {used !== null && limit !== null && <p className="dev-context-note">距自动整理 {contextCount(Math.max(0, limit - used))} tokens</p>}
    {!!context.turnId && onTrace && <Button variant="ghost" size="sm" onClick={onTrace}>查看完整上下文</Button>}
  </div>;
}
