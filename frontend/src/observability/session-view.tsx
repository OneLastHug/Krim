import { formatClock, formatCost, formatCount, formatDuration, formatReportedTokens } from "./obs-format";
import { groupBySession } from "./use-observations";
import { ObsEmpty, ObsPageHead, ObsScopeNote, ObsStatusPill, type ObsViewProps } from "./obs-ui";

/** 会话页：已加载轮次的分组视图。只读，不放输入框；
 * 累计 token 标注为“已加载轮次合计”，不当作窗口占用。 */

export function SessionView({ state, onState, data }: ObsViewProps) {
  if (!data.ready) return <ObsEmpty title="连接工作区后读取观测记录。"/>;
  if (data.loading && !data.items.length) return <ObsEmpty title="正在读取观测记录…"/>;
  if (data.error) return <ObsEmpty title="暂时无法读取观测记录">{data.error}</ObsEmpty>;
  const groups = groupBySession(data.items);
  if (!groups.length) return <ObsEmpty title="此工作区还没有可观测的运行记录">发起会话任务后，观测会在这里出现。</ObsEmpty>;

  return <div className="obs-stack">
    <ObsPageHead title="会话" subtitle="按会话分组的轮次与请求事实"/>
    <ObsScopeNote>只展示当前已加载页内的轮次；翻页或筛选会改变范围。</ObsScopeNote>
    {groups.map(group => {
      const known = (key: "input_tokens" | "output_tokens") => {
        const values = group.items.map(item => item.usage?.[key]).filter((value): value is number => value !== null && value !== undefined);
        return values.length ? values.reduce((sum, value) => sum + value, 0) : null;
      };
      const input = known("input_tokens"), output = known("output_tokens");
      const missingUsage = group.items.filter(item => item.usage === null).length;
      const compactions = group.items.reduce((sum, item) => sum + (item.compaction_passes ?? 0), 0);
      return <section key={group.session_id} className="obs-panel obs-panel-pad">
        <div className="obs-panel-head">
          <h3>{group.session_title || group.session_id}</h3>
          <div className="obs-actions">
            <span className="obs-caption">{formatCount(group.items.length)} 轮 · 累计输入 {formatReportedTokens(input)} / 输出 {formatReportedTokens(output)}（已报告部分）</span>
            <button type="button" className="obs-button obs-button-small" onClick={() => onState({ ...state, page: "context", session: group.session_id, turn: group.items[0]!.turn_id, request: "", base: "", item: "", mode: "content" })}>查看上下文</button>
          </div>
        </div>
        {missingUsage > 0 && <p className="obs-caption" role="status">{missingUsage} 轮缺少供应商用量，token 合计不含这些轮次。</p>}
        {compactions > 0 && <p className="obs-caption">上下文压缩 {compactions} 次。</p>}
        <ol className="obs-turn-list">
          {group.items.map(item => <li key={item.turn_id} className="obs-turn">
            <div className="obs-turn-head">
              <span className="obs-mono obs-turn-id" title={item.turn_id}>{item.turn_id.slice(0, 10)}</span>
              <ObsStatusPill status={item.status}/>
              <span className="obs-caption">{formatClock(item.created_at)} · {formatDuration(item.duration_ms)}</span>
            </div>
            <div className="obs-turn-facts">
              <span>请求 {formatCount(item.model_requests)} · 工具 {formatCount(item.tool_calls)} · 压缩 {formatCount(item.compaction_passes)}</span>
              <span>token {item.usage ? `in ${formatReportedTokens(item.usage.input_tokens)} / out ${formatReportedTokens(item.usage.output_tokens)}` : "用量未知"}</span>
              <span>费用 {formatCost(item.cost, item.cost_status)}</span>
            </div>
            <div className="obs-actions">
              <button type="button" className="obs-button obs-button-small" onClick={() => onState({ ...state, page: "runs", run: item.run_id, session: item.session_id, turn: item.turn_id, request: "", base: "", item: "" })}>执行详情</button>
              <button type="button" className="obs-button obs-button-small" onClick={() => onState({ ...state, page: "context", session: item.session_id, turn: item.turn_id, request: "", base: "", item: "", mode: "content" })}>上下文</button>
            </div>
          </li>)}
        </ol>
      </section>;
    })}
  </div>;
}
