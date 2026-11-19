import { formatCost, formatCount, formatDuration, formatPercent, formatTime } from "./obs-format";
import { ObsEmpty, ObsFact, ObsPageHead, ObsScopeNote, ObsStatusPill, type ObsViewProps } from "./obs-ui";
import type { ObservationsIndexItem } from "./obs-contracts";

/** 概览页：对当前已加载页的有界摘要。所有计数都来自返回页，明确范围；
 * 没有质量评分数据，不放质量列。 */

function attention(item: ObservationsIndexItem): string[] {
  const reasons: string[] = [];
  if (item.status === "failed") reasons.push("运行失败");
  if (item.status === "interrupted") reasons.push("已中断");
  if (item.projection_status === "unavailable") reasons.push(`事实不可投影（${item.projection_code ?? "原因未知"}）`);
  if (item.usage === null) reasons.push("无用量记录");
  else if ((item.steps_missing_usage ?? 0) > 0) reasons.push(`${item.steps_missing_usage} 步缺用量`);
  if (item.manifest_status === "missing") reasons.push("无上下文清单");
  if (item.manifest_status === "partial") reasons.push("清单部分缺失");
  return reasons;
}

export function OverviewView({ state, onState, data }: ObsViewProps) {
  if (!data.ready) return <ObsEmpty title="连接工作区后读取观测记录。"/>;
  if (data.loading && !data.items.length) return <ObsEmpty title="正在读取观测记录…"/>;
  if (data.error) return <ObsEmpty title="暂时无法读取观测记录">{data.error}</ObsEmpty>;
  const items = data.items;
  if (!items.length) return <ObsEmpty title="此工作区还没有可观测的运行记录">发起会话任务后，观测会在这里出现。</ObsEmpty>;

  const withUsage = items.filter(item => item.usage !== null);
  const knownTotal = (key: "input_tokens" | "output_tokens" | "total_tokens" | "cached_input_tokens") => {
    const values = withUsage.map(item => item.usage?.[key]).filter((value): value is number => value !== null && value !== undefined);
    return values.length ? values.reduce((sum, value) => sum + value, 0) : null;
  };
  const totals = { input: knownTotal("input_tokens"), output: knownTotal("output_tokens"),
    cached: knownTotal("cached_input_tokens"), total: knownTotal("total_tokens") };
  const tokenCount = (value: number | null) => value === null ? "未知" : formatCount(value);
  const durations = items.map(item => item.duration_ms).filter((value): value is number => value !== null).sort((a, b) => a - b);
  const percentile = (fraction: number) => durations.length ? durations[Math.min(durations.length - 1, Math.floor(durations.length * fraction))]! : null;
  const attentionItems = items.map(item => ({ item, reasons: attention(item) })).filter(entry => entry.reasons.length > 0);

  return <div className="obs-stack">
    <ObsPageHead
      title="概览"
      subtitle={`已加载 ${formatCount(items.length)} 条运行（有界页）`}
      actions={<button type="button" className="obs-button" onClick={data.refresh}>刷新</button>}
    />
    <section className="obs-panel obs-metric-strip">
      <div className="obs-stat">
        <span className="obs-stat-label">运行</span>
        <strong className="obs-stat-value">{formatCount(items.length)}</strong>
        <span className="obs-stat-note">{formatCount(withUsage.length)} 条有用量</span>
      </div>
      <div className="obs-stat">
        <span className="obs-stat-label">技术失败</span>
        <strong className="obs-stat-value">{formatCount(items.filter(item => item.status === "failed" || item.status === "aborted").length)}</strong>
        <span className="obs-stat-note">中断单列 {formatCount(items.filter(item => item.status === "interrupted").length)} 条</span>
      </div>
      <div className="obs-stat">
        <span className="obs-stat-label">输入 token</span>
        <strong className="obs-stat-value">{tokenCount(totals.input)}</strong>
        <span className="obs-stat-note">缓存读取 {tokenCount(totals.cached)}</span>
      </div>
      <div className="obs-stat">
        <span className="obs-stat-label">输出 token</span>
        <strong className="obs-stat-value">{tokenCount(totals.output)}</strong>
        <span className="obs-stat-note">已报告合计 {tokenCount(totals.total)}</span>
      </div>
      <div className="obs-stat">
        <span className="obs-stat-label">耗时 p50 / p95</span>
        <strong className="obs-stat-value">{formatDuration(percentile(0.5))}</strong>
        <span className="obs-stat-note">p95 {formatDuration(percentile(0.95))} · {formatCount(durations.length)} 个有效样本</span>
      </div>
      <div className="obs-stat">
        <span className="obs-stat-label">费用（USD）</span>
        <strong className="obs-stat-value">{formatCost(null, "unpriced")}</strong>
        <span className="obs-stat-note">缺少价格与汇率记录</span>
      </div>
    </section>
    <section className="obs-panel obs-panel-pad">
      <div className="obs-panel-head"><h3>需要关注</h3><span className="obs-caption">按可验证原因列出，可叠加</span></div>
      {attentionItems.length === 0 && <p className="obs-caption">当前已加载页没有满足关注条件的运行。</p>}
      <ul className="obs-attention-list">
        {attentionItems.slice(0, 12).map(({ item, reasons }) => <li key={item.run_id}>
          <div className="obs-attention-head">
            <ObsStatusPill status={item.status}/>
            <strong className="obs-truncate">{item.session_title || item.session_id}</strong>
            <span className="obs-caption obs-mono">{item.run_id.slice(0, 10)}</span>
            <span className="obs-caption">{formatTime(item.created_at)}</span>
          </div>
          <p className="obs-caption">{reasons.join("；")}</p>
          <div className="obs-actions">
            <button type="button" className="obs-button obs-button-small" onClick={() => onState({ ...state, page: "runs", run: item.run_id, session: item.session_id, turn: item.turn_id, request: "", base: "", item: "" })}>查看执行</button>
            {(item.compaction_passes ?? 0) > 0 && <button type="button" className="obs-button obs-button-small" onClick={() => onState({ ...state, page: "context", session: item.session_id, turn: item.turn_id, request: "", base: "", item: "", mode: "content" })}>查看上下文</button>}
          </div>
        </li>)}
      </ul>
      {attentionItems.length > 12 && <p className="obs-caption">其余 {attentionItems.length - 12} 条满足关注条件，可在运行页筛选查看。</p>}
    </section>
    <section className="obs-panel obs-panel-pad">
      <div className="obs-panel-head"><h3>数据覆盖</h3><span className="obs-caption">仅当前已加载页</span></div>
      <div className="obs-fact-grid">
        <ObsFact label="上下文清单">{formatCount(items.filter(item => item.manifest_status === "present").length)} 完整 / {formatCount(items.filter(item => item.manifest_status === "partial").length)} 部分 / {formatCount(items.filter(item => item.manifest_status === "missing").length)} 缺失</ObsFact>
        <ObsFact label="用量记录">{formatCount(withUsage.length)} / {formatCount(items.length)} 条运行</ObsFact>
        <ObsFact label="压缩 pass">{formatCount(items.reduce((sum, item) => sum + (item.compaction_passes ?? 0), 0))} 个</ObsFact>
        <ObsFact label="可投影">{formatCount(items.filter(item => item.projection_status === "ok").length)} / {formatCount(items.length)} 条</ObsFact>
        <ObsFact label="缓存读取占比">{totals.input !== null && totals.input > 0 && totals.cached !== null ? formatPercent(totals.cached, totals.input) : "—"}</ObsFact>
        <ObsFact label="时长口径">{items.filter(item => item.duration_source === "mono").length} 条单调时钟 / {items.filter(item => item.duration_source === "wall_clock").length} 条墙钟</ObsFact>
      </div>
      <ObsScopeNote>概览数字不外推到全工作区；翻页、筛选或切换会话会改变样本范围。</ObsScopeNote>
    </section>
  </div>;
}
