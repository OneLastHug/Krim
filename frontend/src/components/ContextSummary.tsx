import type { ObsCategory } from "../observability/obs-contracts";
import { obsPalette } from "../observability/obs-palette";
import type { SessionContextState } from "../hooks/use-session-context";
import { Button } from "./ui/button";

export const contextCount = (value: number | null) => value === null ? "—" : value >= 1000 ? `${(value / 1000).toLocaleString(undefined, { maximumFractionDigits: 1 })}k` : value.toLocaleString();
const labels: Record<ObsCategory, string> = { system: "系统消息", tools: "工具定义", files: "项目文件", tool_result: "工具结果", memory: "记忆文件", history: "对话历史", summary: "历史摘要", unknown: "未归属" };

export function ContextSummary({ context, onTrace, compactView = false, dark = document.documentElement.classList.contains("dark") }: { context: SessionContextState; onTrace?: () => void; dark?: boolean; compactView?: boolean }) {
  const { manifest, used, limit } = context;
  const ratio = used !== null && limit ? used / limit : null;
  const tokenMode = !!manifest && manifest.estimated_tokens !== null && manifest.items.every(item => item.estimated_tokens !== null);
  const order: ObsCategory[] = ["files", "tool_result", "history", "tools", "system", "memory", "summary", "unknown"];
  const rows = order.map(category => {
    const items = manifest?.items.filter(item => item.category === category) ?? [];
    return { category, bytes: items.reduce((sum, item) => sum + item.input_bytes, 0), tokens: items.reduce((sum, item) => sum + (item.estimated_tokens ?? 0), 0) };
  });
  const knownBytes = rows.reduce((sum, item) => sum + item.bytes, 0), knownTokens = rows.reduce((sum, item) => sum + item.tokens, 0);
  const unknown = rows.find(item => item.category === "unknown")!;
  if (manifest && knownBytes < manifest.input_bytes) unknown.bytes += manifest.input_bytes - knownBytes;
  if (tokenMode && used !== null && knownTokens < used) unknown.tokens += used - knownTokens;
  const totalBytes = Math.max(manifest?.input_bytes ?? 0, knownBytes);
  const totalTokens = Math.max(used ?? 0, knownTokens);
  const budget = limit ?? totalTokens;
  const color = (category: ObsCategory) => dark ? obsPalette[category].darkColor : obsPalette[category].color;
  return <div className="context-summary" aria-label="上下文用量与来源">
    <div className="g-context-overview"><strong>{ratio === null ? "上下文用量" : `${(ratio * 100).toFixed(1)}% 已用`}</strong><span title="tokens（服务端估算）">{contextCount(used)} / {contextCount(limit)}</span></div>
    {context.loading && <p className="dev-context-note" role="status">正在读取上一完成轮次…</p>}
    {context.error && <div role="status" className="dev-context-note">{context.error}<Button variant="ghost" size="sm" onClick={context.refresh}>重试</Button></div>}
    {!context.turnId && <p className="dev-context-note">发送首条消息后统计各来源占比。</p>}
    {manifest && <>
      <svg className="context-composition dev-context-bar" viewBox="0 0 300 7" preserveAspectRatio="none" role="img" aria-label={tokenMode ? "上下文来源占窗口容量；右侧灰色为剩余空间" : "来源 token 明细未提供，字节明细见下表"}>
        <title>{tokenMode ? "服务端估算 token 用量与剩余窗口" : "来源 token 明细未提供；下表按字节列出来源"}</title>
        {tokenMode ? rows.map((item, index) => <rect key={item.category} data-context-category={item.category} x={budget ? rows.slice(0, index).reduce((sum, row) => sum + row.tokens, 0) / budget * 300 : 0} y="0" width={budget ? item.tokens / budget * 300 : 0} height="7" fill={color(item.category)} stroke="var(--background)" strokeWidth="1" vectorEffect="non-scaling-stroke"><title>{labels[item.category]}：{contextCount(item.tokens)} tokens（估算）</title></rect>) : used !== null && limit ? <rect x="0" y="0" width={Math.min(1, used / limit) * 300} height="7" fill={color("unknown")}/> : null}
      </svg>
      <table className="g-context-table" aria-label={tokenMode ? "上下文来源明细，按估算 token 占已用量" : "上下文来源明细，按输入字节"}><tbody>{rows.filter(item => item.bytes > 0 || item.tokens > 0).map(item => <tr key={item.category}><td><svg className="obs-swatch" aria-hidden="true" width="8" height="8"><rect width="8" height="8" rx="2" fill={color(item.category)}/></svg>{labels[item.category]}</td><td title={`${item.bytes.toLocaleString()} 字节`}>{tokenMode ? contextCount(item.tokens) : `${contextCount(item.bytes)} B`}</td><td>{tokenMode ? totalTokens ? (item.tokens / totalTokens * 100).toFixed(1) : "0" : totalBytes ? (item.bytes / totalBytes * 100).toFixed(1) : "0"}%</td></tr>)}</tbody></table>
      {(!compactView || !tokenMode || manifest.items_truncated) && <p className="dev-context-note">{tokenMode ? "tokens 为服务端估算值。" : "来源 token 明细未提供，表格按输入字节计算。"}{manifest.items_truncated ? "部分来源已截断。" : ""}</p>}
    </>}
    {!compactView && used !== null && limit !== null && <p className="dev-context-note">距自动整理 {contextCount(Math.max(0, limit - used))} tokens</p>}
    {!compactView && !!context.turnId && onTrace && <Button variant="ghost" size="sm" onClick={onTrace}>查看完整上下文</Button>}
  </div>;
}
