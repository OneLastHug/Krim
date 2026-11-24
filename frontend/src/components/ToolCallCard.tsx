import { useEffect, useId, useState } from "react";
import type { ApprovalScope } from "../lib/execution-policy";
import { ArrowUpRight, ChevronRight, LoaderCircle, Shield, Wrench, FileText, Terminal, Search, Plug } from "lucide-react";
import { runReason } from "../lib/copy";
import { displayToolName } from "../lib/tool-names";
import type { ToolPresentation, ToolState } from "../lib/session-presentation";
import { ToolProvenance } from "./ToolProvenance";
import { Button } from "./ui/button";
import { toolKind, toolSummary, formatToolValue } from "../lib/tool-display";

const labels: Record<ToolState, string> = {
  preparing: "准备调用", approval: "等待审批", running: "等待结果", ok: "已完成",
  denied: "已拒绝", failed: "失败", cancelled: "已取消", unknown: "结果未确认",
};

function valueText(value: unknown) {
  try { return JSON.stringify(value, null, 2) ?? "未提供"; }
  catch { return "内容暂时无法显示"; }
}

function ApprovalTime({ expiresAt }: { expiresAt: number }) {
  const secondsLeft = () => Math.max(0, Math.ceil(expiresAt - Date.now() / 1000));
  const [remaining, setRemaining] = useState(secondsLeft);
  useEffect(() => {
    const update = () => setRemaining(Math.max(0, Math.ceil(expiresAt - Date.now() / 1000)));
    update();
    const timer = window.setInterval(update, 1000);
    return () => window.clearInterval(timer);
  }, [expiresAt]);
  const duration = `${String(Math.floor(remaining / 60)).padStart(2, "0")}:${String(remaining % 60).padStart(2, "0")}`;
  return <span className="approval-expiry" role="timer" aria-label="审批剩余时间" aria-live="off">{remaining > 0 ? `审批剩余 ${duration}` : "等待更新审批状态"}</span>;
}

function missingResult(tool: ToolPresentation) {
  if (tool.state === "preparing" || tool.state === "running") return "尚未收到结果";
  if (tool.executorStarted === false) return "工具未执行";
  if (tool.state === "failed") return "工具调用失败，未取得结果";
  if (tool.state === "cancelled") return "调用已取消，未取得结果";
  return "结果未确认";
}

export function ToolCallCard({ tool, canApprove, canAllowSession = false, busy, pending, onDecision, onInspect, onOpenSubagent }: {
  tool: ToolPresentation;
  canApprove: boolean;
  canAllowSession?: boolean;
  busy: boolean;
  pending: boolean;
  onDecision: (allow: boolean, scope?: ApprovalScope) => void;
  onInspect: () => void;
  onOpenSubagent?: (agentId: string) => void;
}) {
  // 显示用名称：旧 dev_* 名映射为新名；事件与审批记录中的原始 name 不变。
  const toolName = displayToolName(tool.name);
  const Icon = ({ file: FileText, terminal: Terminal, search: Search, mcp: Plug, other: Wrench })[toolKind(tool.name)];
  const summary = toolSummary(tool.arguments);
  const result = tool.result && typeof tool.result === "object" && !Array.isArray(tool.result) ? tool.result as Record<string, unknown> : null;
  const value = result?.value && typeof result.value === "object" && !Array.isArray(result.value) ? result.value as Record<string, unknown> : result;
  const agentId = tool.name === "spawn_agent" && tool.state === "ok" && typeof value?.agent_id === "string" ? value.agent_id : null;
  const [raw, setRaw] = useState(false);
  const scopeId = useId();
  const approvalIdentity = tool.approval ? JSON.stringify([tool.approval.run_id, tool.approval.request_id, tool.approval.call_id, tool.approval.tool_hash, tool.approval.arguments_hash]) : "";
  const [choice, setChoice] = useState<{ identity: string; scope: ApprovalScope }>({ identity: "", scope: "once" });
  const selectedScope = canAllowSession && choice.identity === approvalIdentity ? choice.scope : "once";
  if (tool.approval) return <section className="approval chat-approval" data-request-id={tool.approval.request_id} data-risk={tool.risk === "write" ? "write" : tool.risk === "read" ? "read" : "unknown"} aria-label="等待审批">
    <div className="approval-title"><Shield aria-hidden="true" /><strong data-ui-data>{toolName}</strong><span>等待审批</span></div>
    <ToolProvenance value={{ source: tool.source, risk: tool.risk ?? "unknown" }} />
    {tool.approval.expires_at !== null && <ApprovalTime expiresAt={tool.approval.expires_at} />}
    <pre data-ui-data className="approval-arguments" tabIndex={0} aria-label={`${toolName} 的调用参数`}>{valueText(tool.approval.arguments)}</pre>
    {canAllowSession && <div className="approval-scope"><label htmlFor={scopeId}>允许范围</label>
      <select id={scopeId} value={selectedScope} disabled={!canApprove || busy || pending}
        onChange={event => setChoice({ identity: approvalIdentity, scope: event.target.value === "session" ? "session" : "once" })}>
        <option value="once">仅本次调用</option><option value="session">本会话的相同调用</option>
      </select></div>}
    <div className="approval-actions">
      <Button disabled={!canApprove || busy || pending} onClick={() => selectedScope === "session" ? onDecision(true, "session") : onDecision(true)} aria-label={selectedScope === "session" ? `允许 ${toolName} 本会话相同调用` : `允许 ${toolName} 本次调用`}>{selectedScope === "session" ? "允许本会话相同调用" : "允许本次调用"}</Button>
      <Button variant="outline" disabled={!canApprove || busy || pending} onClick={() => onDecision(false)} aria-label={`拒绝 ${toolName} 本次调用`}>拒绝</Button>
      {pending && <span role="status">正在确认决定</span>}
      {!canApprove && <span>当前身份没有审批权限</span>}
    </div>
  </section>;

  return <details className="chat-tool" data-state={tool.state} data-call-id={tool.callId ?? tool.providerCallId}>
    <summary onClick={event => { if (!(event.currentTarget.parentElement as HTMLDetailsElement).open) onInspect(); }}><ChevronRight className="chat-tool-chevron" aria-hidden="true" />{tool.state === "running" ? <LoaderCircle className="chat-tool-spinner" aria-hidden="true" /> : <Icon aria-hidden="true" />}<span className="chat-tool-name" data-ui-data>{toolName}</span>{summary && <span className="chat-tool-target" title={summary}>{summary}</span>}<span className="chat-tool-meta">{tool.code === "approval_expired" ? "审批已过期" : labels[tool.state]}</span></summary>
    <div className="chat-tool-body">
      <div className="chat-tool-bar"><span>{tool.source || tool.risk ? <ToolProvenance value={{ source: tool.source, risk: tool.risk }} /> : "来源未提供"}</span><Button variant="ghost" size="sm" className="g-info-link" aria-pressed={raw} onClick={() => setRaw(value => !value)}>{raw ? "查看格式化摘要" : "查看原始 JSON"}</Button></div>
      {raw ? <pre className="chat-tool-output" data-ui-data tabIndex={0}>{valueText({ name: tool.name, arguments: tool.arguments, output: tool.resultAvailable ? tool.result : undefined })}</pre> : tool.resultAvailable ? <pre className="chat-tool-output" data-ui-data tabIndex={0}>{formatToolValue(tool.result)}</pre> : <p className="detail-empty">{missingResult(tool)}</p>}
      {agentId && onOpenSubagent && <Button variant="ghost" size="sm" className="sa-open-agent" onClick={() => onOpenSubagent(agentId)}>查看子代理<ArrowUpRight aria-hidden="true"/></Button>}
      {tool.code && <p className="chat-tool-error" data-ui-data>{tool.code === "invalid_input" ? "工具参数无效" : runReason(tool.code)} <code>{tool.code}</code></p>}
    </div>
  </details>;
}
