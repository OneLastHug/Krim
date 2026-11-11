import { useMemo } from "react";
import { traceDuration, traceSpanRows, type TraceSpan } from "../lib/trace-contracts";
import { obsFilterStatuses, obsStatusNames, type ObsRequestRecord, type ObsToolRecord } from "./obs-contracts";
import { downloadObservationCsv } from "./export";
import { formatBytes, formatClock, formatCost, formatCount, formatDuration, formatTime, formatReportedTokens } from "./obs-format";
import { obsSeries } from "./obs-palette";
import { ObsEmpty, ObsPageHead, ObsScopeNote, ObsStatusPill, type ObsViewProps } from "./obs-ui";

/** 运行页：有界列表 + 详情。时间线为真实 trace span 的同级排布；
 * 模型请求与工具都是 run 根 span 的子级，工具不画进模型请求区间。 */

function spanKind(span: TraceSpan, requests: ObsRequestRecord[], tools: ObsToolRecord[]): "model" | "tool" | "root" | "other" {
  if (requests.some(request => request.id === span.span_id)) return "model";
  if (tools.some(tool => tool.id === span.span_id)) return "tool";
  return span.parent_span_id === null ? "root" : "other";
}

export function RunsView({ state, onState, dark, data, workspace }: ObsViewProps) {
  const items = data.items;
  const detail = data.detail;
  const selected = state.run ? items.find(item => item.run_id === state.run) ?? null : null;

  const spanRows = useMemo(() => detail ? traceSpanRows(detail.trace.spans) : [], [detail]);
  const timeline = useMemo(() => {
    if (!detail || !spanRows.length) return null;
    const bounds = spanRows.reduce<{ min: number; max: number }>((acc, { span }) => ({
      min: Math.min(acc.min, span.start),
      max: Math.max(acc.max, span.end ?? span.start),
    }), { min: Infinity, max: -Infinity });
    if (!Number.isFinite(bounds.min) || bounds.max <= bounds.min) return null;
    return { ...bounds, span: (value: number) => Math.min(100, Math.max(0, (value - bounds.min) / (bounds.max - bounds.min) * 100)) };
  }, [detail, spanRows]);
  const selectedSpan = detail?.trace.spans.find(span => span.span_id === state.item) ?? null;
  const selectedRequest = detail?.requests.find(request => request.id === state.item) ?? null;
  const selectedTool = detail?.tools.find(tool => tool.id === state.item) ?? null;

  if (!data.ready) return <ObsEmpty title="连接工作区后读取观测记录。"/>;
  if (data.loading && !items.length) return <ObsEmpty title="正在读取观测记录…"/>;
  if (data.error) return <ObsEmpty title="暂时无法读取观测记录">{data.error}</ObsEmpty>;

  return <div className="obs-stack">
    <ObsPageHead
      title="运行"
      subtitle="从会话轮次进入执行、请求与工具事实"
      actions={<>
        <select aria-label="按状态筛选" value={state.status} onChange={event => onState({ ...state, status: event.target.value, run: "", item: "" })}>
          <option value="">全部状态</option>
          {obsFilterStatuses.map(status => <option key={status} value={status}>{obsStatusNames[status]}</option>)}
        </select>
        <button type="button" className="obs-button" disabled={!data.canPrevious} onClick={() => { data.previous(); }}>上一页</button>
        <button type="button" className="obs-button" disabled={!data.canNext} onClick={() => { data.next(); }}>下一页</button>
        <button type="button" className="obs-button" onClick={data.refresh}>刷新</button>
        <button type="button" className="obs-button" onClick={() => downloadObservationCsv(workspace, data.items)}>导出当前页 CSV</button>
      </>}
    />
    {!items.length && <ObsEmpty title="没有符合筛选条件的运行记录">调整状态筛选或刷新。</ObsEmpty>}
    {items.length > 0 && <div className={selected ? "obs-run-layout has-selection" : "obs-run-layout"}>
      <section className="obs-panel obs-run-list" aria-label="运行列表">
        {items.map(item => <button key={item.run_id} type="button"
          className={item.run_id === state.run ? "obs-run-row active" : "obs-run-row"}
          onClick={() => onState({ ...state, run: item.run_id, session: item.session_id, turn: item.turn_id, request: "", base: "", item: "" })}>
          <span className="obs-run-row-top">
            <strong>{item.session_title || item.session_id}</strong>
            <ObsStatusPill status={item.status}/>
          </span>
          <span className="obs-run-row-meta">
            <span className="obs-mono">{item.run_id.slice(0, 10)}</span>
            <span>{formatClock(item.created_at)}</span>
            <span>{formatDuration(item.duration_ms)}</span>
          </span>
        </button>)}
        <p className="obs-caption obs-list-note">已加载 {items.length} 条 · 只读有界页</p>
      </section>
      {selected && <section className="obs-panel obs-panel-pad obs-run-detail">
        {!detail && data.detailLoading && <p className="obs-caption" role="status">正在读取运行观测详情…</p>}
        {!detail && !!data.detailError && <p className="obs-caption" role="alert">{data.detailError}</p>}
        {detail && <>
          <header className="obs-detail-head">
            <div>
              <h3>{selected.session_title || selected.session_id}</h3>
              <p className="obs-caption obs-mono">{detail.run.run_id} · key {detail.run.request_key}</p>
            </div>
            <ObsStatusPill status={selected.status}/>
            <button type="button" className="obs-button obs-button-small" onClick={() => onState({ ...state, run: "", item: "" })}>返回列表</button>
          </header>
          <div className="obs-fact-grid">
            <div className="obs-fact"><span>开始</span><strong>{formatTime(detail.run.created_at)}</strong></div>
            <div className="obs-fact"><span>结束</span><strong>{formatTime(detail.run.finished_at)}</strong></div>
            <div className="obs-fact"><span>总耗时</span><strong>{formatDuration(detail.run.duration_ms)}（{detail.run.duration_source === "mono" ? "单调时钟" : "墙钟差值"}）</strong></div>
            <div className="obs-fact"><span>模型 / 工具</span><strong>{formatCount(detail.requests.length)} / {formatCount(detail.tools.length)}</strong></div>
            <div className="obs-fact"><span>输入 token</span><strong>{formatReportedTokens(detail.usage.totals?.input_tokens ?? null)}</strong></div>
            <div className="obs-fact"><span>输出 token</span><strong>{formatReportedTokens(detail.usage.totals?.output_tokens ?? null)}</strong></div>
            <div className="obs-fact"><span>缓存读取</span><strong>{detail.usage.totals?.cached_input_tokens == null ? "未知" : formatReportedTokens(detail.usage.totals.cached_input_tokens)}</strong></div>
            <div className="obs-fact"><span>费用</span><strong>{formatCost(detail.cost, detail.cost_status)}</strong></div>
            <div className="obs-fact"><span>清单覆盖</span><strong>{detail.coverage.manifest_status}</strong></div>
            <div className="obs-fact"><span>用量覆盖</span><strong>{detail.usage.steps_with_usage} 步有值{detail.usage.steps_missing_usage > 0 ? ` · ${detail.usage.steps_missing_usage} 步缺失` : ""}</strong></div>
          </div>
          {detail.configuration && <section className="obs-run-configuration">
            <h4 className="obs-section-label">运行配置</h4>
            <div className="obs-fact-grid">
              <div className="obs-fact"><span>目标</span><strong>{detail.configuration.key} · {detail.configuration.version}</strong></div>
              <div className="obs-fact"><span>Provider 行为版本</span><strong>{detail.configuration.provider_behavior_version ?? "未记录"}</strong></div>
              <div className="obs-fact"><span>模型</span><strong>{detail.configuration.model ?? "未记录"}</strong></div>
              <div className="obs-fact"><span>推理 / thinking</span><strong>{detail.configuration.reasoning_effort ?? "—"} / {detail.configuration.thinking ?? "—"}</strong></div>
              <div className="obs-fact"><span>endpoint 指纹</span><strong className="obs-mono">{detail.configuration.endpoint_fingerprint ?? "未记录"}</strong></div>
              <div className="obs-fact"><span>prompt 指纹</span><strong className="obs-mono">{detail.configuration.prompt_fingerprint ?? "未记录"}</strong></div>
              <div className="obs-fact"><span>最大输出 token</span><strong>{detail.configuration.max_output_tokens === null ? "未记录" : formatCount(detail.configuration.max_output_tokens)}</strong></div>
            </div>
            <p className="obs-caption">配置指纹记录本次运行使用的版本。</p>
          </section>}
          {!detail.usage.usage_complete && <p className="obs-caption" role="status">供应商用量尚不完整，当前显示已报告部分。</p>}
          {selected.projection_status === "unavailable" && <p className="obs-caption" role="status">索引聚合不可投影：{selected.projection_code ?? "原因未知"}；执行事实仍可查看。</p>}
          <h4 className="obs-section-label">执行时间线</h4>
          {timeline && <div className="obs-timeline" role="group" aria-label="执行时间线">
            <div className="obs-timeline-scale"><span>0</span><span>{traceDuration(timeline.min, timeline.max)}</span></div>
            {spanRows.map(({ span, depth }) => {
              const kind = spanKind(span, detail.requests, detail.tools);
              const bad = !["ok", "completed", "allow"].includes(span.status) && kind !== "root";
              const series = kind === "model" ? obsSeries.model : obsSeries.tool;
              const color = bad ? "var(--destructive)" : kind === "model" ? dark ? series.darkColor : series.color : dark ? series.darkColor : series.color;
              const left = timeline.span(span.start);
              const right = timeline.span(span.end ?? span.start);
              return <button key={span.span_id} type="button"
                className={state.item === span.span_id ? "obs-timeline-row active" : "obs-timeline-row"}
                onClick={() => onState({ ...state, item: span.span_id }, true)}>
                <span className="obs-timeline-name" style={{ paddingInlineStart: depth * 12 }}>{span.kind}</span>
                <span className="obs-timeline-track"><i style={{ left: left + "%", width: Math.max(0.8, right - left) + "%", background: color }}/></span>
                <span className="obs-timeline-duration">{traceDuration(span.start, span.end)}</span>
              </button>;
            })}
          </div>}
          {detail && !timeline && <p className="obs-caption">此运行没有可排布的时间区间。</p>}
          <div className="obs-inspector">
            {selectedRequest && <RequestInspector request={selectedRequest} onContext={id => onState({ ...state, page: "context", request: id, base: "", item: "" })}/>}
            {selectedTool && <ToolInspector tool={selectedTool} onContext={id => onState({ ...state, page: "context", request: id, base: "", item: "" })}/>}
            {!selectedRequest && !selectedTool && selectedSpan && <div>
              <h4 className="obs-section-label">{selectedSpan.kind}</h4>
              <div className="obs-fact-grid">
                <div className="obs-fact"><span>Span</span><strong className="obs-mono">{selectedSpan.span_id}</strong></div>
                <div className="obs-fact"><span>状态</span><strong>{selectedSpan.status}</strong></div>
                <div className="obs-fact"><span>区间</span><strong>{traceDuration(selectedSpan.start, selectedSpan.end)}</strong></div>
              </div>
              <details className="obs-raw">
                <summary>span 属性</summary>
                <pre>{JSON.stringify(selectedSpan.attributes, null, 2).slice(0, 4000)}</pre>
              </details>
            </div>}
            {!selectedRequest && !selectedTool && !selectedSpan && <p className="obs-caption">在时间线或列表中选择一个节点查看事实。</p>}
          </div>
        </>}
      </section>}
    </div>}
    <ObsScopeNote>列表与聚合仅覆盖当前已加载页；使用状态筛选与翻页缩小或移动范围。</ObsScopeNote>
  </div>;
}

function RequestInspector({ request, onContext }: { request: ObsRequestRecord; onContext: (id: string) => void }) {
  const usage = request.usage?.usage ?? null;
  return <div>
    <h4 className="obs-section-label">模型请求 · step {request.step_index} · {request.purpose}</h4>
    <div className="obs-fact-grid">
      <div className="obs-fact"><span>结果</span><strong>{request.status}</strong></div>
      <div className="obs-fact"><span>耗时</span><strong>{formatDuration(request.duration_ms)}（{request.duration_source === "mono" ? "单调时钟" : "墙钟差值"}）</strong></div>
      <div className="obs-fact"><span>首内容事件</span><strong>{request.timing?.first_content_ms === null || request.timing === null ? "未记录" : formatDuration(request.timing.first_content_ms)}</strong></div>
      <div className="obs-fact"><span>首文本事件</span><strong>{request.timing?.first_text_ms === null || request.timing === null ? "未记录" : formatDuration(request.timing.first_text_ms)}</strong></div>
      <div className="obs-fact"><span>输入 token</span><strong>{formatReportedTokens(usage?.input_tokens ?? null)}</strong></div>
      <div className="obs-fact"><span>输出 token</span><strong>{formatReportedTokens(usage?.output_tokens ?? null)}</strong></div>
      <div className="obs-fact"><span>上报次数</span><strong>{request.usage ? `${request.usage.usage_reports} 次${request.usage.usage_final ? " · 含final" : ""}` : "无用量事件"}</strong></div>
      <div className="obs-fact"><span>清单</span><strong>{request.manifest ? `${formatBytes(request.manifest.input_bytes)} · ${request.manifest.items.length} 项` : request.manifest_status}</strong></div>
    </div>
    {request.manifest && <button type="button" className="obs-button obs-button-small obs-section-gap" onClick={() => onContext(request.id)}>查看上下文分布</button>}
  </div>;
}

function ToolInspector({ tool, onContext }: { tool: ObsToolRecord; onContext: (id: string) => void }) {
  return <div>
    <h4 className="obs-section-label">工具调用 · {tool.name}</h4>
    <div className="obs-fact-grid">
      <div className="obs-fact"><span>结果</span><strong>{tool.status}{tool.code ? ` · ${tool.code}` : ""}</strong></div>
      <div className="obs-fact"><span>审批决定</span><strong>{tool.decision ?? "未记录"}</strong></div>
      <div className="obs-fact"><span>执行器已启动</span><strong>{tool.executor_started === null ? "未知" : tool.executor_started ? "是" : "否"}</strong></div>
      <div className="obs-fact"><span>结果已产出</span><strong>{tool.result_available === null ? "未知" : tool.result_available ? "是" : "否"}</strong></div>
      <div className="obs-fact"><span>耗时</span><strong>{formatDuration(tool.duration_ms)}（{tool.duration_source === "mono" ? "单调时钟" : "墙钟差值"}）</strong></div>
      <div className="obs-fact"><span>step</span><strong>{tool.step_index}</strong></div>
    </div>
    <p className="obs-caption obs-mono">{tool.provider_call_id ? `provider ${tool.provider_call_id}` : tool.call_id ? `call ${tool.call_id}` : "调用 id 未记录"}</p>
    {tool.source && <section className="obs-tool-source">
      <h4 className="obs-section-label">已读取的历史来源</h4>
      <div className="obs-fact-grid">
        <div className="obs-fact"><span>历史文件版本</span><strong className="obs-mono">{tool.source.revision_id}</strong></div>
        <div className="obs-fact"><span>文件路径</span><strong className="obs-mono">{tool.source.path}</strong></div>
        <div className="obs-fact"><span>字节范围</span><strong>{formatCount(tool.source.offset)}–{formatCount(tool.source.offset + tool.source.length)} / {formatCount(tool.source.total_size)}</strong></div>
        <div className="obs-fact"><span>文件 SHA-256</span><strong className="obs-mono">{tool.source.sha256}</strong></div>
      </div>
      <p className="obs-caption">此处保留调用当时的文件版本和读取范围。</p>
    </section>}
    <p className="obs-caption">工具结果作为 tool_result 来源出现在对应模型请求上下文中。</p>
    <button type="button" className="obs-button obs-button-small obs-section-gap" onClick={() => onContext("")}>打开所属轮次上下文</button>
  </div>;
}
