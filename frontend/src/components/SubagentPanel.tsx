import { useState } from "react";
import { ArrowUpRight, Check, ChevronRight, Clock3, FileText, GitFork, LoaderCircle, MoreHorizontal, Shield, X } from "lucide-react";
import type { SessionState } from "../hooks/use-session";
import type { useSubagents } from "../hooks/use-subagents";
import { memberTitle, subagentState, type SubagentMember } from "../lib/subagent-contracts";
import { openDevelopmentPanel } from "../lib/workbench-events";
import { projectTurn, type ExecutionPresentationSource } from "../lib/session-presentation";
import { Button } from "./ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "./ui/dialog";
import { MessageMarkdown } from "./messages/MessageMarkdown";
import { useCopyFeedback } from "../hooks/use-copy-feedback";
import { CopyFeedback } from "./messages/CopyFeedback";
import { ToolCallCard } from "./ToolCallCard";
import { runReason } from "../lib/copy";
import "./subagent-panel.css";

export type SubagentsState = ReturnType<typeof useSubagents>;
const labels = { running: "运行中", approval: "等待批准", completed: "已完成", error: "失败", idle: "空闲", closed: "已关闭" };

export function SubagentStatus({ member }: { member: SubagentMember }) {
  const state = subagentState(member);
  const Icon = state === "running" ? LoaderCircle : state === "approval" ? Shield : state === "completed" ? Check : state === "error" ? X : Clock3;
  return <Icon aria-hidden="true" className={state === "running" ? "sa-spin" : undefined}/>;
}

export function SubagentSummary({ state, chat }: { state: SubagentsState; chat: SessionState }) {
  if (!state.available || (!state.members.length && !state.error)) return null;
  const running = state.members.filter(member => subagentState(member) === "running").length;
  const approvals = state.members.filter(member => subagentState(member) === "approval").length;
  return <div className="sa-task-strip"><button type="button" className="sa-task-toggle" onClick={() => openDevelopmentPanel(chat, "subagents")}><GitFork aria-hidden="true"/><strong>后台任务</strong><span>{state.error && !state.members.length ? "暂时无法读取" : running ? `${running} 运行中${approvals ? ` · ${approvals} 等待批准` : ""}` : approvals ? `${approvals} 等待批准` : `${state.members.length} 个子代理`}</span><ChevronRight aria-hidden="true"/></button></div>;
}

function textOf(value: unknown): string {
  if (typeof value === "string") return value;
  if (!value || typeof value !== "object" || Array.isArray(value)) return "";
  const data = value as Record<string, unknown>;
  return typeof data.text === "string" ? data.text : typeof data.content === "string" ? data.content : "";
}
function artifactPaths(value: unknown): string[] {
  if (!value || typeof value !== "object" || Array.isArray(value)) return [];
  const data = value as Record<string, unknown>;
  const values = Array.isArray(data.artifacts) ? data.artifacts : [];
  return values.flatMap(item => {
    const path = typeof item === "string" ? item : item && typeof item === "object" && "path" in item ? (item as { path: unknown }).path : null;
    return typeof path === "string" && path.length <= 4096 && !path.startsWith("/") && !path.includes("\\") && !path.includes("\0") && !path.split("/").includes("..") ? [path] : [];
  });
}

export function SubagentApprovals({ state, chat }: { state: SubagentsState; chat: SessionState }) {
  return <>{state.approvalItems.map(({ agent, approval }) => <section className="sa-pending-approval" key={`${agent.agent_id}:${approval.request_id}`} data-subagent-id={agent.agent_id}>
    <button type="button" className="sa-approval-source" onClick={() => { state.select(agent.agent_id); openDevelopmentPanel(chat, "subagents", agent.agent_id); }}><GitFork aria-hidden="true"/>{memberTitle(agent)}<ArrowUpRight aria-hidden="true"/></button>
    <ToolCallCard tool={{ type: "tool", key: approval.request_id, callId: approval.call_id, stepIndex: 0, name: approval.name, state: "approval", arguments: approval.arguments, approval, resultAvailable: false }} canApprove={chat.canApprove && chat.session?.status === "open"} canAllowSession={!!chat.execution?.policy?.project_id && chat.execution.policy.project_access === "write"} busy={state.busy} pending={false} onInspect={() => undefined} onDecision={(allow, scope) => { void state.approve(agent.agent_id, approval, allow, scope); }}/>
  </section>)}{state.approvalError && <div className="sa-load-error" role="status"><span>{state.approvalError}</span><Button variant="outline" size="sm" onClick={state.reload}>重试读取</Button></div>}{state.approvalReceipt && <p className="sa-action-receipt" role="status">{state.approvalReceipt}</p>}</>;
}

export function SubagentPanel({ state, chat, onOpenFile, onShowApproval }: { state: SubagentsState; chat: SessionState; onOpenFile: (path: string, title: string) => void; onShowApproval: () => void }) {
  const feedback = useCopyFeedback();
  const [listOpen, setListOpen] = useState(true), [showDetails, setShowDetails] = useState(false);
  const [draft, setDraft] = useState({ id: "", text: "" }), [wake, setWake] = useState<"auto" | "queue_only">("auto");
  const [confirm, setConfirm] = useState<{ id: string; action: "interrupt" | "close"; activationId: string | null; taskId: string | null } | null>(null);
  const member = state.members.find(item => item.agent_id === state.selectedId) ?? null;
  const detail = state.detail?.agent.agent_id === member?.agent_id ? state.detail : null;
  const text = draft.id === member?.agent_id ? draft.text : "";
  const stateName = member ? subagentState(member) : "idle";
  const canWrite = chat.canWrite && chat.session?.status === "open" && !chat.session.archived_at && !chat.session.deleted_at;
  const execution: ExecutionPresentationSource | null = detail?.run ? { run_id: detail.run.id, status: detail.run.status === "completed" ? "completed" : detail.run.status === "running" ? "running" : detail.run.status === "queued" ? "queued" : detail.run.status === "interrupted" ? "interrupted" : "failed", transcript: null } : null;
  const items = execution ? projectTurn(execution, detail?.events ?? [], detail?.approvals ?? []) : [];
  const assignment = detail?.messages.find(message => message.kind === "task" && message.task_id === member?.current_task_id);
  const outputs = detail?.results ?? [];
  const paths = [...new Set(outputs.flatMap(result => artifactPaths(result.output)))];
  async function send() {
    if (!member || !text.trim() || state.busy || !canWrite) return;
    const id = member.agent_id, sent = text;
    if (await state.send(sent.trim(), wake)) setDraft(previous => previous.id === id && previous.text === sent ? { id, text: "" } : previous);
  }
  return <section className="sa-panel" aria-label="后台子代理">
    <div className="sa-agents-head"><button type="button" aria-expanded={listOpen} onClick={() => setListOpen(value => !value)}><ChevronRight className={listOpen ? "is-open" : undefined} aria-hidden="true"/><span>当前会话</span><span>{state.members.length}</span></button></div>
    {listOpen && <div className="sa-agent-list" role="list" aria-label="后台子代理列表">{state.members.map(agent => <div role="listitem" key={agent.agent_id}><button type="button" className={`sa-agent-item${member?.agent_id === agent.agent_id ? " is-selected" : ""}`} onClick={() => { state.select(agent.agent_id); setConfirm(null); }}><SubagentStatus member={agent}/><span><strong>{memberTitle(agent)}</strong><small>{agent.path}</small></span><em data-state={subagentState(agent)}>{labels[subagentState(agent)]}</em></button></div>)}{state.hasMore && <Button variant="ghost" size="sm" disabled={state.loading} onClick={() => void state.loadMore()}>加载更多</Button>}</div>}
    {(state.error || state.detailError) && <div className="sa-load-error" role="status"><span>{state.detailError || state.error}</span><Button variant="outline" size="sm" onClick={state.reload}>重试读取</Button></div>}
    {state.loading && !state.members.length && <p className="sa-empty" role="status">正在读取后台任务…</p>}
    {!state.loading && !member && <p className="sa-empty">{state.members.length ? "选择一个子代理查看输出。" : "当前会话没有后台子代理。"}</p>}
    {member && <div className="sa-agent-detail" key={member.agent_id}>
      <div className="sa-detail-heading"><div><h2>{memberTitle(member)}</h2><span>{member.path}</span></div><button type="button" className="sa-detail-toggle" aria-label="查看模型与工作区" aria-expanded={showDetails} onClick={() => setShowDetails(value => !value)}><MoreHorizontal aria-hidden="true"/></button><Button variant="outline" size="sm" disabled={!canWrite || state.busy || !["running", "approval"].includes(stateName)} onClick={() => setConfirm({ id: member.agent_id, action: "interrupt", activationId: member.activation?.activation_id ?? null, taskId: member.current_task_id })}>中断</Button><button type="button" className="icon-button small" aria-label="关闭此子代理及后代" disabled={!canWrite || state.busy || stateName === "closed"} onClick={() => setConfirm({ id: member.agent_id, action: "close", activationId: member.activation?.activation_id ?? null, taskId: member.current_task_id })}><X aria-hidden="true"/></button></div>
      {showDetails && <dl className="sa-metadata"><dt>配置</dt><dd>{member.profile}</dd><dt>后端</dt><dd>{member.provider}</dd><dt>工作区</dt><dd>{member.workdir ?? "共享工作区"}</dd></dl>}
      <div className="sa-agent-transcript">
        {state.detailLoading && !detail && <p role="status">正在读取子代理输出…</p>}
        {assignment && textOf(assignment.body) && <p className="sa-assignment">{textOf(assignment.body)}</p>}
        {items.map(item => item.type === "assistant" ? <div className="sa-agent-output" key={item.key}><MessageMarkdown text={item.text} onCopy={feedback.copy}/></div> : item.approval ? <button type="button" key={item.key} className="sa-return-approval" onClick={onShowApproval}><Shield aria-hidden="true"/>在主对话中批准此操作<ArrowUpRight aria-hidden="true"/></button> : <ToolCallCard key={item.key} tool={item} canApprove={false} busy={false} pending={false} onDecision={() => undefined} onInspect={() => undefined}/>)}
        {!items.some(item => item.type === "assistant") && outputs.map(result => textOf(result.output) ? <div className="sa-agent-output" key={result.result_id}><MessageMarkdown text={textOf(result.output)} onCopy={feedback.copy}/></div> : null)}
        {detail && !items.length && !outputs.length && <p className="sa-empty">{stateName === "approval" ? "等待主会话批准工具操作。" : stateName === "running" ? "正在执行，尚未生成输出。" : "还没有已保存的执行输出。"}</p>}
        {detail?.events_truncated && <p className="sa-empty">执行记录较长，当前显示最近一段。</p>}
        {detail?.run?.code && <p className="sa-action-receipt">{detail.run.code === "cancelled" ? "已停止" : runReason(detail.run.code)} <code>{detail.run.code}</code></p>}
        {detail?.messages.filter(message => message.kind !== "task").map(message => <div className="sa-sent-message" key={message.id}><small>后续消息{message.wake === "queue_only" ? " · 仅排队" : ""}</small><p>{textOf(message.body)}</p></div>)}
        {paths.map(path => <button type="button" className="sa-artifact" key={path} onClick={() => onOpenFile(path, path.split("/").at(-1) ?? path)}><FileText aria-hidden="true"/><span>{path}</span><ArrowUpRight aria-hidden="true"/></button>)}
        {stateName === "approval" && !items.some(item => item.type === "tool" && item.approval) && <button type="button" className="sa-return-approval" onClick={onShowApproval}><Shield aria-hidden="true"/>在主对话中批准此操作<ArrowUpRight aria-hidden="true"/></button>}
      </div>
      <form className="sa-agent-compose" onSubmit={event => { event.preventDefault(); void send(); }}><label className="sr-only" htmlFor="subagent-message">给子代理发送后续消息</label><textarea id="subagent-message" rows={1} placeholder={stateName === "closed" ? "此子代理已关闭" : "给此子代理补充信息…"} disabled={!canWrite || stateName === "closed" || state.busy} value={text} onChange={event => setDraft({ id: member.agent_id, text: event.target.value })}/><div><select aria-label="子代理消息投递方式" disabled={!canWrite || stateName === "closed" || state.busy} value={wake} onChange={event => setWake(event.target.value === "queue_only" ? "queue_only" : "auto")}><option value="auto">自动唤醒</option><option value="queue_only">仅排队</option></select><Button size="sm" variant="outline" type="submit" disabled={!canWrite || stateName === "closed" || state.busy || !text.trim()}>发送</Button></div>{state.receipt && <p role="status">{state.receipt}</p>}</form>
    </div>}
    <CopyFeedback {...feedback}/>
    <Dialog open={!!confirm && confirm.id === member?.agent_id} onOpenChange={open => { if (!open) setConfirm(null); }}><DialogContent className="dev-dialog sa-confirm" aria-describedby="subagent-confirm-description"><DialogHeader><DialogTitle>{confirm?.action === "interrupt" ? "中断当前子任务？" : "关闭子代理及其后代？"}</DialogTitle></DialogHeader><p id="subagent-confirm-description">{confirm?.action === "interrupt" ? `停止“${member ? memberTitle(member) : ""}”的当前执行。历史和已有产出保留，可继续发送后续任务。` : `停止“${member ? memberTitle(member) : ""}”及其后代。历史和结果保留。`}</p><div className="dialog-actions"><Button variant="outline" disabled={state.busy} onClick={() => setConfirm(null)}>取消</Button><Button variant="outline" className="sa-danger" disabled={!canWrite || state.busy} onClick={async () => { if (confirm && await state.control(confirm.action, { agentId: confirm.id, activationId: confirm.activationId, taskId: confirm.taskId })) setConfirm(null); }}>{state.busy ? "处理中…" : confirm?.action === "interrupt" ? "中断任务" : "关闭子代理"}</Button></div>{state.receipt && !/^(已发送|已批准|已拒绝)/.test(state.receipt) && <p role="status">{state.receipt}</p>}</DialogContent></Dialog>
  </section>;
}
