import { obsCategories, obsCategoryNames } from "./obs-contracts";
import { formatCount } from "./obs-format";
import { ObsEmpty, ObsPageHead, ObsScopeNote } from "./obs-ui";
import type { ObsViewProps } from "./obs-ui";

/** 数据状态页：当前已加载数据的来源覆盖与口径说明。无假数据、无隐藏补充。 */

export function StatusView({ data }: ObsViewProps) {
  if (!data.ready) return <ObsEmpty title="连接工作区后读取观测记录。"/>;
  const items = data.items;
  const missingProjection = items.filter(item => item.projection_status === "unavailable");
  const nonConserving = items.filter(item => item.usage !== null).length;
  const manifestChecked = items.reduce((count, item) => count + (item.manifest_requests ?? 0), 0);
  const scope = data.page?.scope;

  return <div className="obs-stack">
    <ObsPageHead
      title="数据状态"
      subtitle="观测数据的来源覆盖与已知限制"
      actions={<button type="button" className="obs-button" onClick={data.refresh}>刷新</button>}
    />
    {data.loading && <p className="obs-caption" role="status">正在读取…</p>}
    {data.error && <p className="obs-caption" role="alert">{data.error}</p>}
    {scope && <section className="obs-panel obs-panel-pad">
      <h3>当前读取范围</h3>
      <div className="obs-fact-grid">
        <div className="obs-fact"><span>工作区</span><strong className="obs-mono">{scope.workspace_id}</strong></div>
        <div className="obs-fact"><span>角色</span><strong>{scope.role}</strong></div>
        <div className="obs-fact"><span>页大小</span><strong>{scope.limit}</strong></div>
        <div className="obs-fact"><span>计数口径</span><strong>{scope.counted === "returned_page" ? "仅返回页" : scope.counted}</strong></div>
        <div className="obs-fact"><span>状态筛选</span><strong>{scope.status ?? "全部"}</strong></div>
        <div className="obs-fact"><span>已加载</span><strong>{formatCount(items.length)} 条</strong></div>
      </div>
      <ObsScopeNote>统计范围为当前返回页；翻页会切换这组记录。</ObsScopeNote>
    </section>}
    <section className="obs-panel obs-panel-pad">
      <h3>来源覆盖</h3>
      <div className="obs-fact-grid">
        <div className="obs-fact"><span>可投影运行</span><strong>{formatCount(items.length - missingProjection.length)} / {formatCount(items.length)}</strong></div>
        <div className="obs-fact"><span>有用量运行</span><strong>{formatCount(nonConserving)} / {formatCount(items.length)}</strong></div>
        <div className="obs-fact"><span>有效上下文清单</span><strong>{formatCount(manifestChecked)} 份请求</strong></div>
        <div className="obs-fact"><span>压缩 pass</span><strong>{formatCount(items.reduce((sum, item) => sum + (item.compaction_passes ?? 0), 0))} 个</strong></div>
      </div>
    </section>
    <section className="obs-panel obs-panel-pad">
      <h3>口径说明</h3>
      <ul className="obs-caption-list">
        <li>来源面积按请求的规范 JSON 字节计算，token 分项按字节分配估算。</li>
        <li>合并的小项计入“其他来源”，图形与明细的总量一致。</li>
        <li>缓存读取和写入是输入用量的子项，不增加来源面积。</li>
        <li>模型窗口未知时展示内容构成；已记录的输出预留单列显示。</li>
        <li>费用使用 USD。缺少价格或汇率记录时显示“未计价”。</li>
        <li>新模型请求使用单调时钟测量；历史记录采用事件时间差。首内容与首文本记录在供应商事件到达时。</li>
        <li>来源类别：{obsCategories.map(category => obsCategoryNames[category]).join(" / ")}。</li>
        <li>压缩前后比较采用同一业务上下文的字节计量；旧记录可能缺少这一测量。</li>
      </ul>
    </section>
    {missingProjection.length > 0 && <section className="obs-panel obs-panel-pad">
      <h3>不可投影的运行</h3>
      <ul className="obs-caption-list">
        {missingProjection.map(item => <li key={item.run_id}><span className="obs-mono">{item.run_id}</span> · {item.projection_code ?? "原因未知"}</li>)}
      </ul>
    </section>}
  </div>;
}
