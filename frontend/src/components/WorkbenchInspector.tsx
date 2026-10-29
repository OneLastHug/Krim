import type { Actor } from "../lib/session-contracts";
import type { WorkbenchView } from "../lib/workbench-route";

export function WorkbenchInspector({ view, objectId, identity }: { view: WorkbenchView; objectId: string; identity: Actor | null }) {
  const objectLabel = view === "chat" ? "对话" : view === "run" ? "运行" : view === "eval" ? "评测" : "对象";
  return <aside className="workbench-inspector" aria-label="对象详情">
    <header className="inspector-heading"><div><span className="inspector-kicker">当前上下文</span><h2>详情</h2></div></header>
    <section className="inspector-section" aria-labelledby="inspector-object-heading">
      <h3 id="inspector-object-heading">{objectId ? `${objectLabel}身份` : "尚未选择对象"}</h3>
      {objectId ? <code className="inspector-object-id" data-ui-data>{objectId}</code> : <p>选择一项记录后，这里显示它的定位信息。</p>}
      {view === "chat" && <p>工具调用与文件产物可在对应消息中展开查看。</p>}
      {view === "eval" && <p>当前入口承载已有的旧版评测能力。</p>}
    </section>
    <section className="inspector-section" aria-labelledby="inspector-identity-heading">
      <h3 id="inspector-identity-heading">当前身份</h3>
      {identity ? <>
        <p className="inspector-principal" title={identity.principal_id}>{identity.principal_id}</p>
        <ul className="inspector-workspaces">{identity.workspaces.map(workspace => <li key={workspace.workspace_id}><span>{workspace.workspace_id}</span><span>{workspace.role === "admin" ? "管理员" : workspace.role === "viewer" ? "只读" : "成员"}</span></li>)}</ul>
        {identity.legacy && <p>运行记录与评测使用旧版接口。</p>}
      </> : <p>连接后显示身份与工作区能力。</p>}
    </section>
  </aside>;
}
