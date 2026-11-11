import { useMemo, useState } from "react";
import {
  diffManifests,
  manifestBytesSum,
  obsCategories,
  obsCategoryNames,
  type ObsCategory,
  type ObsManifest,
  type ObsManifestItem,
  type ObsRequestRecord,
  type ObsTurnObservations,
} from "./obs-contracts";
import { ObsSparkline, ObsTreemap, type TreemapEntry } from "./obs-charts";
import { formatBytes, formatDeltaBytes, formatDuration, formatPercent, formatReportedTokens, formatTokens } from "./obs-format";
import { obsPalette } from "./obs-palette";
import type { ObsState } from "./obs-route";
import { groupBySession } from "./use-observations";
import { ObsEmpty, ObsNotice, ObsPageHead, ObsScopeNote, type ObsViewProps } from "./obs-ui";

/** 上下文页：manifest 驱动的来源分布、明细、相邻请求差分与真实压缩记录。
 * 图形面积以 input_bytes 为准确分母；窗口模式以估算 token 对齐已知窗口，窗口未知时不画空余；
 * 缓存只是计量维度，不产生额外面积。 */

function swatch(category: ObsCategory, dark: boolean): string {
  const entry = obsPalette[category];
  return dark ? entry.darkColor : entry.color;
}

/** 后端 label 为通用英文名；按类别给出中文名，工具名等专名原样保留。 */
const genericLabels: Record<string, string> = {
  "System message": "系统消息", "User message": "用户消息", "Assistant message": "助手消息",
  "Tool result": "工具结果", "Tools": "工具定义", "Summary": "历史摘要",
  "unknown:request-envelope": "请求信封（未归属）",
};

function itemLabel(item: ObsManifestItem): string {
  return genericLabels[item.label] ?? item.label;
}

const sourceLabel = (item: ObsManifestItem): string => {
  const parts: string[] = [];
  if (item.message_index !== null) parts.push(`消息 #${item.message_index}`);
  if (item.tool_index !== null) parts.push(`工具 #${item.tool_index}`);
  if (item.call_id) parts.push(`call ${item.call_id}`);
  if (item.source_refs?.length) parts.push(item.source_refs.slice(0, 3).map(ref => `${ref.turn_id ? `轮次 ${ref.turn_id.slice(0, 8)}` : "来源"}${ref.run_id ? ` · run ${ref.run_id.slice(0, 8)}` : ""}`).join(" · "));
  return parts.join(" · ") || "未归属";
};

function buildContentEntries(manifest: ObsManifest, hidden: Set<ObsCategory>): TreemapEntry[] {
  const groups = new Map<ObsCategory, { bytes: number; items: { key: string; label: string; bytes: number }[] }>();
  for (const item of manifest.items) {
    if (hidden.has(item.category)) continue;
    const group = groups.get(item.category) ?? { bytes: 0, items: [] };
    group.bytes += item.input_bytes;
    group.items.push({ key: item.key, label: itemLabel(item), bytes: item.input_bytes });
    groups.set(item.category, group);
  }
  const entries: TreemapEntry[] = [];
  for (const category of obsCategories) {
    const group = groups.get(category);
    if (!group) continue;
    entries.push({
      id: `cat:${category}`,
      name: obsCategoryNames[category],
      value: group.bytes,
      kind: "group",
      category,
      children: group.items
        .sort((a, b) => b.bytes - a.bytes)
        .map(item => ({ id: item.key, name: item.label, value: item.bytes, kind: "item" as const, category, sublabel: formatBytes(item.bytes) })),
    });
  }
  return entries;
}

function buildWindowEntries(manifest: ObsManifest, hidden: Set<ObsCategory>): { entries: TreemapEntry[]; overflow: boolean } {
  const window = manifest.model_context_window;
  if (window === null) return { entries: [], overflow: false };
  const reserved = manifest.reserved_output_tokens ?? 0;
  const visible = manifest.items.filter(item => !hidden.has(item.category));
  const children = visible
    .map(item => ({ id: item.key, name: itemLabel(item), value: item.estimated_tokens ?? 0, kind: "item" as const, category: item.category, sublabel: formatTokens(item.estimated_tokens) }))
    .sort((a, b) => b.value - a.value);
  const entries: TreemapEntry[] = [];
  for (const category of obsCategories) {
    const items = children.filter(item => item.category === category);
    if (!items.length) continue;
    entries.push({ id: `cat:${category}`, name: obsCategoryNames[category], value: items.reduce((sum, item) => sum + item.value, 0), kind: "group", category, children: items });
  }
  const used = entries.reduce((sum, entry) => sum + entry.value, 0);
  const overflow = used + reserved > window;
  if (reserved > 0) entries.push({ id: "ghost:reserved", name: "预留输出", value: reserved, kind: "ghost", sublabel: formatTokens(reserved) });
  const free = window - used - reserved;
  if (free > 0 && !overflow) entries.push({ id: "ghost:free", name: "可用空间", value: free, kind: "ghost", sublabel: formatTokens(free) });
  return { entries, overflow };
}

function ManifestSummary({ request }: { request: ObsRequestRecord }) {
  const manifest = request.manifest;
  const usage = request.usage?.usage ?? null;
  if (!manifest) {
    return <ObsEmpty title="此请求没有可用的上下文清单">{request.manifest_status === "invalid" ? "清单结构损坏，无法展示来源分布。" : "采集未记录该请求的清单（旧数据）。"}</ObsEmpty>;
  }
  const window = manifest.model_context_window;
  const cached = usage?.cached_input_tokens ?? null;
  const written = usage?.cache_write_input_tokens ?? null;
  const providerInput = usage?.input_tokens ?? null;
  return <>
    <div className="obs-context-top">
      <div>
        <p className="obs-metric-label">已发送输入 <span className="obs-pill">字节精确</span></p>
        <p className="obs-big">{formatBytes(manifest.input_bytes)}<span className="obs-suffix">{formatTokens(manifest.estimated_tokens)} token（估算）</span></p>
        <p className="obs-caption">
          {window !== null
            ? <>窗口 {formatTokens(window)} · 预留输出 {formatTokens(manifest.reserved_output_tokens)} · 预算 {manifest.input_budget_bytes === null ? "未知" : formatBytes(manifest.input_budget_bytes)}</>
            : <>模型窗口未知，只展示已发送输入，不估算剩余空间{manifest.input_budget_bytes !== null ? ` · 输入预算 ${formatBytes(manifest.input_budget_bytes)}` : ""}</>}
        </p>
      </div>
      <div>
        <p className="obs-metric-label">提示词缓存</p>
        <p className="obs-big">{cached === null || providerInput === null ? "未知" : formatPercent(cached, providerInput)}<span className="obs-suffix">{cached === null ? "供应商未报告" : "缓存读取"}</span></p>
        <div className="obs-meter" aria-hidden="true"><span style={{ width: cached === null || providerInput === null || !(providerInput > 0) ? 0 : Math.min(100, cached / providerInput * 100) + "%" }}/></div>
        <p className="obs-caption">{cached === null ? "供应商未报告缓存用量" : `${formatReportedTokens(cached)} 读取 · 缓存写入 ${formatReportedTokens(written)}`}</p>
      </div>
    </div>
    {manifest.items_truncated && <ObsNotice tone="warn" title={`清单有 ${manifest.omitted_items ?? "多"} 项已合并`}>“其他来源”包含其余条目的合计；具体引用未保留。</ObsNotice>}
    {!manifest.items_truncated && manifestBytesSum(manifest) !== manifest.input_bytes && <ObsNotice tone="warn" title="分项字节合计与总量不一致">差异 {formatDeltaBytes(manifest.input_bytes - manifestBytesSum(manifest))}，以请求总量为准展示。</ObsNotice>}
  </>;
}

function ContextSelection({ data, state, onState, requests }: {
  data: ObsViewProps["data"];
  state: ObsState;
  onState: ObsViewProps["onState"];
  requests: ObsRequestRecord[];
}) {
  const groups = groupBySession(data.items);
  const turns = data.items.filter(item => !state.session || item.session_id === state.session);
  return <section className="obs-panel obs-panel-pad">
    <div className="obs-panel-head"><h3>选择上下文对象</h3></div>
    <div className="obs-actions">
      <label className="obs-field"><span>会话</span><select aria-label="选择会话" value={state.session} onChange={event => onState({ ...state, session: event.target.value, turn: "", request: "", base: "", item: "" })}>
        {groups.map(group => <option key={group.session_id} value={group.session_id}>{group.session_title || group.session_id}</option>)}
        {state.session && !groups.some(group => group.session_id === state.session) && <option value={state.session}>{state.session}</option>}
      </select></label>
      <label className="obs-field"><span>轮次</span><select aria-label="选择轮次" value={state.turn} onChange={event => onState({ ...state, turn: event.target.value, request: "", base: "", item: "" })}>
        {turns.map(item => <option key={item.turn_id} value={item.turn_id}>{item.session_title} · {item.run_id.slice(0, 8)}</option>)}
        {state.turn && !turns.some(item => item.turn_id === state.turn) && <option value={state.turn}>{state.turn}</option>}
      </select></label>
      {requests.length > 0 && <label className="obs-field"><span>模型请求</span><select aria-label="选择模型请求" value={state.request} onChange={event => onState({ ...state, request: event.target.value, base: "", item: "" })}>
        {requests.map((item, index) => <option key={item.id} value={item.id}>请求 {index + 1} · {item.purpose}</option>)}
      </select></label>}
    </div>
  </section>;
}

export function ContextView({ state, onState, dark, data }: ObsViewProps) {
  const [selectedCategory, setSelectedCategory] = useState<ObsCategory | "all">("all");
  const [query, setQuery] = useState("");
  const [onlyChanges, setOnlyChanges] = useState(true);
  const detail = data.detail;
  const requests = detail?.requests.filter(request => request.manifest !== null) ?? [];
  const request = requests.find(item => item.id === state.request) ?? requests[0] ?? null;
  const baseRequest = state.base ? requests.find(item => item.id === state.base) ?? null : null;
  const manifest = request?.manifest ?? null;
  const compare = baseRequest !== null && request !== null && baseRequest !== request;

  const sessionItems = useMemo(() => {
    const groups = groupBySession(data.items);
    if (detail && !groups.some(group => group.session_id === detail.session_id)) {
      groups.push({ session_id: detail.session_id, session_title: detail.session_id, items: [] });
    }
    return groups;
  }, [data.items, detail]);
  const turnItems = useMemo(() => data.items.filter(item => item.session_id === state.session), [data.items, state.session]);
  const turnOptions = useMemo(() => {
    const options = turnItems.map(item => ({ turn_id: item.turn_id, run_id: item.run_id, created_at: item.created_at }));
    if (detail && detail.session_id === state.session && !options.some(item => item.turn_id === detail.turn_id)) {
      options.push({ turn_id: detail.turn_id, run_id: detail.run.run_id, created_at: detail.run.created_at });
    }
    return options;
  }, [turnItems, detail, state.session]);

  if (data.loading && !data.items.length && !detail) return <ObsEmpty title="正在读取观测记录…"/>;
  if (data.error && !detail) return <ObsEmpty title="暂时无法读取观测记录">{data.error}</ObsEmpty>;
  if (!data.items.length && !detail) {
    return <ObsEmpty title={state.session ? "此会话没有已加载的运行记录" : "此工作区还没有可观测的运行记录"}>
      {state.session ? "清除会话筛选或翻页查看更多。" : "发起会话任务后，观测会在这里出现。"}
    </ObsEmpty>;
  }
  if (!detail) {
    return <div className="obs-stack">
      <ContextSelection data={data} state={state} onState={onState} requests={[]}/>
      <ObsEmpty title={data.detailError || "正在读取轮次观测详情…"}>{data.detailLoading ? "" : "选择其他轮次或返回会话列表。"}</ObsEmpty>
    </div>;
  }
  if (!requests.length) {
    return <ObsEmpty title="此轮没有带上下文清单的模型请求">{detail.coverage.manifest_status === "missing" ? "采集未记录清单（旧数据），执行与用量仍可查看。" : "清单缺失或损坏。"}</ObsEmpty>;
  }

  const requestIndex = request ? requests.indexOf(request) : -1;
  const contentEntries = manifest ? buildContentEntries(manifest, new Set()) : [];
  const windowBuild = manifest && manifest.model_context_window !== null ? buildWindowEntries(manifest, new Set()) : null;
  const mode = state.mode === "window" && windowBuild ? "window" : "content";
  const activeEntries = mode === "window" ? windowBuild!.entries : contentEntries;
  const diff = compare && manifest && baseRequest?.manifest ? diffManifests(baseRequest.manifest, manifest) : [];
  const growthPoints = turnItems.map(item => ({ id: item.turn_id, label: `轮 ${item.turn_id.slice(0, 6)}`, value: item.usage?.input_tokens ?? null }));
  const growthEvents = turnItems
    .map((item, index) => ({ index, item }))
    .filter(({ item }) => (item.compaction_passes ?? 0) > 0)
    .map(({ index, item }) => ({ index, label: `压缩 · 轮 ${item.turn_id.slice(0, 6)}` }));
  const visibleRows = manifest
    ? manifest.items
      .filter(item => selectedCategory === "all" || item.category === selectedCategory)
      .filter(item => !query || itemLabel(item).toLowerCase().includes(query.toLowerCase()) || item.key.toLowerCase().includes(query.toLowerCase()))
      .sort((a, b) => b.input_bytes - a.input_bytes)
    : [];

  return <div className="obs-stack">
    <ObsPageHead
      title={compare ? "比较上下文" : "上下文"}
      subtitle={`${detail.session_id} · 轮 ${detail.turn_id.slice(0, 8)} · ${detail.run.status}`}
      actions={<>
        <select aria-label="选择会话" value={state.session} onChange={event => onState({ ...state, session: event.target.value, turn: "", request: "", base: "", item: "" })}>
          {sessionItems.map(group => <option key={group.session_id} value={group.session_id}>{group.session_title || group.session_id} · {group.items.length} 轮</option>)}
        </select>
        <select aria-label="选择轮次" value={state.turn} onChange={event => onState({ ...state, turn: event.target.value, request: "", base: "", item: "" })}>
          {turnOptions.map(item => <option key={item.turn_id} value={item.turn_id}>{item.run_id.slice(0, 8)} · {item.created_at ? new Date(item.created_at * 1000).toLocaleTimeString("zh-CN", { hour12: false }) : ""}</option>)}
        </select>
        <select aria-label="选择模型请求" value={request?.id ?? ""} onChange={event => onState({ ...state, request: event.target.value, base: "", item: "" })}>
          {requests.map(item => <option key={item.id} value={item.id}>请求 {requests.indexOf(item) + 1} · {item.purpose} · {item.manifest ? formatBytes(item.manifest.input_bytes) : "无清单"}</option>)}
        </select>
        <button type="button" className="obs-button" disabled={requestIndex <= 0} onClick={() => onState({ ...state, request: requests[requestIndex - 1]!.id, base: "", item: "" })}>上一请求</button>
        <button type="button" className="obs-button" disabled={requestIndex < 0 || requestIndex >= requests.length - 1} onClick={() => onState({ ...state, request: requests[requestIndex + 1]!.id, base: "", item: "" })}>下一请求</button>
        <button type="button" className="obs-button" disabled={requestIndex <= 0} aria-pressed={compare || undefined} onClick={() => onState({ ...state, base: compare ? "" : requests[Math.max(0, requestIndex - 1)]!.id, item: "" })}>与上一请求比较</button>
      </>}
    />
    {state.status && <ObsScopeNote>当前生效状态筛选：{state.status}（在运行页修改）。</ObsScopeNote>}
    {compare && baseRequest?.manifest && manifest && <>
      <section className="obs-panel obs-panel-pad">
        <div className="obs-panel-head">
          <h3>比较上下文</h3>
          <select aria-label="基准请求" value={baseRequest.id} onChange={event => onState({ ...state, base: event.target.value })}>
            {requests.filter(item => item.id !== request.id).map(item => <option key={item.id} value={item.id}>基准：请求 {requests.indexOf(item) + 1}</option>)}
          </select>
          <label className="obs-check"><input type="checkbox" checked={onlyChanges} onChange={event => setOnlyChanges(event.target.checked)}/>只看变化</label>
        </div>
        <p className="obs-caption">输入 {formatBytes(baseRequest.manifest.input_bytes)} → {formatBytes(manifest.input_bytes)}（{formatDeltaBytes(manifest.input_bytes - baseRequest.manifest.input_bytes)}）；比较以 key 对齐、content_hash 判等。</p>
        <div className="obs-split">
          <div>
            <p className="obs-compare-caption">基准 · 请求 {requests.indexOf(baseRequest) + 1} · {formatBytes(baseRequest.manifest.input_bytes)}</p>
            <ObsTreemap entries={buildContentEntries(baseRequest.manifest, new Set())} height={220} dark={dark} selected={null} label="基准请求来源分布"/>
          </div>
          <div>
            <p className="obs-compare-caption">当前 · 请求 {requestIndex + 1} · {formatBytes(manifest.input_bytes)}</p>
            <ObsTreemap entries={contentEntries} height={220} dark={dark} selected={null} label="当前请求来源分布"/>
          </div>
        </div>
        <ObsDiffTable rows={diff} onlyChanges={onlyChanges}/>
      </section>
    </>}
    {!compare && manifest && <section className="obs-panel obs-panel-pad">
      <ManifestSummary request={request!}/>
      <div className="obs-panel-head">
        <h3>内容来源{mode === "window" ? "（窗口 · 估算token刻度）" : "（内容 · 字节刻度）"}</h3>
        <div className="obs-segmented" role="group" aria-label="分布图范围">
          <button type="button" className={mode === "content" ? "active" : ""} onClick={() => onState({ ...state, mode: "content", item: "" }, true)}>内容</button>
          <button type="button" className={mode === "window" ? "active" : ""} disabled={manifest.model_context_window === null} onClick={() => onState({ ...state, mode: "window", item: "" }, true)}>窗口</button>
        </div>
      </div>
      {windowBuild?.overflow && <ObsNotice tone="warn" title="估算 token 超出已知窗口">分类估算合计加预留输出超过窗口，可用空间不再展示。</ObsNotice>}
      <ObsTreemap
        entries={activeEntries}
        height={320}
        dark={dark}
        selected={state.item}
        onSelect={id => onState({ ...state, item: id }, true)}
        label="上下文来源分布，完整数值见下方明细"
      />
      <div className="obs-legend">
        {obsCategories.map(category => {
          const bytes = manifest.items.filter(item => item.category === category).reduce((sum, item) => sum + item.input_bytes, 0);
          return <button key={category} type="button" className={selectedCategory === category ? "active" : ""} aria-pressed={selectedCategory === category}
            onClick={() => setSelectedCategory(selectedCategory === category ? "all" : category)}>
            <i className="obs-swatch" style={{ background: swatch(category, dark) }} aria-hidden="true"/>{obsCategoryNames[category]}
            <span className="obs-legend-value">{formatBytes(bytes)}</span>
          </button>;
        })}
        <span className="obs-caption">点图例选择明细分类，不改变分布图分母；{mode === "window" ? "窗口模式按估算 token。" : "内容模式按字节。"}</span>
      </div>
      <hr className="obs-divider"/>
      <div className="obs-detail-toolbar">
        <h4>内容明细 <span className="obs-caption">{visibleRows.length} / {manifest.items.length} 项</span></h4>
        <div className="obs-category-tabs">
          <button type="button" className={selectedCategory === "all" ? "active" : ""} onClick={() => setSelectedCategory("all")}>全部</button>
          {obsCategories.map(category => (
            <button key={category} type="button" className={selectedCategory === category ? "active" : ""} onClick={() => setSelectedCategory(category)}>{obsCategoryNames[category]}</button>
          ))}
        </div>
        <input className="obs-search" aria-label="搜索内容名称或来源" placeholder="搜索名称 / key" value={query} onChange={event => setQuery(event.target.value)}/>
      </div>
      <ObsItemTable rows={visibleRows} total={manifest.input_bytes} selected={state.item} dark={dark} onLocate={id => onState({ ...state, item: id }, true)} onTimeline={callId => {
        // manifest.call_id 实为 provider 协议 id：优先 tools[].provider_call_id，兼容回退 tools[].call_id。
        const tool = detail.tools.find(entry => entry.provider_call_id === callId) ?? detail.tools.find(entry => entry.call_id === callId);
        if (tool) onState({ ...state, page: "runs", run: detail.run.run_id, item: tool.id, request: "", base: "" });
        else onState({ ...state, page: "runs", run: detail.run.run_id, request: "", base: "", item: "" });
      }} onSource={turnId => onState({ ...state, page: "runs", turn: turnId, run: "", request: "", base: "", item: "" })}/>
      <div className="obs-section-gap">
        <div className="obs-panel-head">
          <h4>会话输入增长（已加载 {turnItems.length} 轮）</h4>
          <span className="obs-caption">当前请求高亮；压缩记录以标记显示</span>
        </div>
        <ObsSparkline points={growthPoints} events={growthEvents} selected={detail.turn_id} onSelect={id => onState({ ...state, turn: id, request: "", base: "", item: "" })} ariaLabel="会话输入随轮次变化"/>
      </div>
    </section>}
    <CompactionsPanel detail={detail} request={request} state={state} onState={onState}/>
  </div>;
}

function ObsDiffTable({ rows, onlyChanges }: { rows: ReturnType<typeof diffManifests>; onlyChanges: boolean }) {
  const visible = rows.filter(row => !onlyChanges || row.change !== "kept");
  const changeNames: Record<string, string> = { added: "新增", removed: "移除", changed: "变化", kept: "保留" };
  if (!rows.length) return <p className="obs-caption">两个请求没有可比较的清单。</p>;
  return <div className="obs-table-wrap">
    <table>
      <thead><tr><th>内容</th><th>变化</th><th className="obs-numeric">基准</th><th className="obs-numeric">当前</th><th className="obs-numeric">差额</th><th>判据</th></tr></thead>
      <tbody>
        {visible.map(row => <tr key={row.key}>
          <td>{row.label}<span className="obs-table-sub">{row.key}</span></td>
          <td><span className={`obs-pill ${row.change === "added" ? "obs-pill-ok" : row.change === "removed" ? "obs-pill-bad" : row.change === "changed" ? "obs-pill-warn" : ""}`}>{changeNames[row.change]}</span></td>
          <td className="obs-numeric">{row.left ? formatBytes(row.left_bytes) : "—"}</td>
          <td className="obs-numeric">{row.right ? formatBytes(row.right_bytes) : "—"}</td>
          <td className="obs-numeric">{formatDeltaBytes(row.delta_bytes)}</td>
          <td className="obs-caption">{row.change === "kept" ? "hash 相同" : row.left && row.right ? "hash 或类别变化" : row.right ? "仅当前请求包含" : "仅基准请求包含"}</td>
        </tr>)}
        {!visible.length && <tr><td colSpan={6}>两个请求内容一致。</td></tr>}
      </tbody>
    </table>
  </div>;
}

function ObsItemTable({ rows, total, selected, dark, onLocate, onTimeline, onSource }: {
  rows: ObsManifestItem[];
  total: number;
  selected: string;
  dark: boolean;
  onLocate: (key: string) => void;
  onTimeline: (callId: string) => void;
  onSource: (turnId: string) => void;
}) {
  if (!rows.length) return <p className="obs-caption">当前筛选没有匹配的内容条目。</p>;
  return <div className="obs-table-wrap">
    <table>
      <thead><tr><th>内容</th><th>类别</th><th className="obs-numeric">字节</th><th className="obs-numeric">占比</th><th className="obs-numeric">估算token</th><th>来源</th><th></th></tr></thead>
      <tbody>
        {rows.map(item => <tr key={item.key} className={selected === item.key ? "obs-row-selected" : ""} onClick={() => onLocate(item.key)}>
          <td>{itemLabel(item)}<span className="obs-table-sub obs-mono">{item.key}</span></td>
          <td><span className="obs-icon-text"><i className="obs-swatch" style={{ background: swatch(item.category, dark) }} aria-hidden="true"/>{obsCategoryNames[item.category]}</span></td>
          <td className="obs-numeric">{formatBytes(item.input_bytes)}</td>
          <td className="obs-numeric">{formatPercent(item.input_bytes, total)}</td>
          <td className="obs-numeric">{formatTokens(item.estimated_tokens)}</td>
          <td className="obs-caption obs-truncate" title={sourceLabel(item)}>{sourceLabel(item)}</td>
          <td>{item.call_id && <button type="button" className="obs-button obs-button-small" onClick={event => { event.stopPropagation(); onTimeline(item.call_id!); }}>定位调用</button>}
            {item.source_refs?.find(ref => ref.turn_id) && <button type="button" className="obs-button obs-button-small" onClick={event => { event.stopPropagation(); onSource(item.source_refs!.find(ref => ref.turn_id)!.turn_id!); }}>定位来源</button>}</td>
        </tr>)}
      </tbody>
    </table>
  </div>;
}

function CompactionsPanel({ detail, request, state, onState }: {
  detail: ObsTurnObservations;
  request: ObsRequestRecord | null;
  state: ObsState;
  onState: (next: ObsState, replace?: boolean) => void;
}) {
  const compactions = detail.compactions;
  const coverage = detail.coverage;
  const changes = request?.manifest?.compaction_changes ?? [];
  const compactionByPass = new Map(compactions.map(pass => [pass.pass_index, pass]));
  return <>
    {changes.length > 0 && <section className="obs-panel obs-panel-pad">
      <div className="obs-panel-head"><h3>压缩前后上下文（本次请求记录）</h3><span className="obs-caption">同一业务组合口径，非摘要请求输入</span></div>
      <div className="obs-table-wrap">
        <table>
          <thead><tr><th>Pass</th><th className="obs-numeric">压缩前</th><th className="obs-numeric">压缩后</th><th className="obs-numeric">变化</th><th>计数口径</th><th>摘要请求</th><th>核验</th></tr></thead>
          <tbody>
            {changes.map(change => <tr key={change.pass_index}>
              <td>#{change.pass_index}</td>
              <td className="obs-numeric">{formatBytes(change.before_input_bytes)}</td>
              <td className="obs-numeric">{formatBytes(change.after_input_bytes)}</td>
              <td className="obs-numeric">{formatDeltaBytes(change.after_input_bytes - change.before_input_bytes)}</td>
              <td className="obs-caption">{change.counter_version ?? "未知"}</td>
              <td>{change.summary_step_index !== null && <button type="button" className="obs-link" onClick={() => {
                const target = detail.requests.find(entry => entry.step_index === change.summary_step_index);
                if (target) onState({ ...state, request: target.id, base: "", item: "" });
              }}>请求 step {change.summary_step_index}</button>}{change.summary_step_index === null && "—"}</td>
              <td className="obs-caption obs-mono obs-truncate" title={change.output_hash ?? ""}>{change.output_hash ? `output ${change.output_hash.slice(0, 12)}…` : compactionByPass.get(change.pass_index)?.output_hash ? "哈希随 pass" : "哈希未知"}</td>
            </tr>)}
          </tbody>
        </table>
      </div>
    </section>}
    <section className="obs-panel obs-panel-pad">
      <div className="obs-panel-head"><h3>压缩记录</h3>{compactions.length > 0 && <span className="obs-caption">{coverage.complete_passes} / {coverage.passes} 个 pass 完成</span>}</div>
      {!compactions.length && <p className="obs-caption">本轮未进行上下文压缩。</p>}
      {compactions.length > 0 && <>
        {coverage.covered_turns !== null && <p className="obs-caption">最近完成 pass 覆盖 {coverage.covered_turns} / {coverage.history_turns ?? "?"} 轮历史；摘要正文不返回，仅核验哈希与字节数。</p>}
        <div className="obs-table-wrap">
          <table>
            <thead><tr><th>Pass</th><th>状态</th><th>覆盖轮次</th><th className="obs-numeric">摘要字节</th><th>摘要请求</th><th className="obs-numeric">耗时</th><th>核验</th></tr></thead>
            <tbody>
              {compactions.map(pass => <tr key={pass.pass_index}>
                <td>#{pass.pass_index}</td>
                <td><span className={`obs-pill ${pass.status === "complete" ? "obs-pill-ok" : "obs-pill-warn"}`}>{pass.status}</span></td>
                <td>{pass.covered_turns === null ? "未知" : `${pass.covered_turns} 轮${pass.covered_turn_ids?.length ? `（首 ${pass.covered_turn_ids[0]!.slice(0, 8)}…）` : ""}`}</td>
                <td className="obs-numeric">{formatBytes(pass.summary_bytes)}</td>
                <td>{pass.summary_step_index !== null && <button type="button" className="obs-link" onClick={() => {
                  const target = detail.requests.find(entry => entry.step_index === pass.summary_step_index);
                  if (target) onState({ ...state, request: target.id, base: "", item: "" });
                }}>请求 step {pass.summary_step_index}</button>}{pass.summary_step_index === null && "—"}</td>
                <td className="obs-numeric">{pass.finished_at !== null ? formatDuration((pass.finished_at - pass.created_at) * 1000) : "—"}</td>
                <td className="obs-caption obs-mono obs-truncate" title={pass.output_hash ?? ""}>{pass.output_hash ? `output ${pass.output_hash.slice(0, 12)}…` : "哈希未知"}</td>
              </tr>)}
            </tbody>
          </table>
        </div>
      </>}
    </section>
  </>;
}
