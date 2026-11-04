import { useEffect, useState } from "react";
import { ChevronRight, LoaderCircle, Shield, Wrench } from "lucide-react";
import { runReason } from "../lib/copy";
import type { ToolPresentation, ToolState } from "../lib/session-presentation";
import { ToolProvenance } from "./ToolProvenance";
import { Button } from "./ui/button";

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

export function ToolCallCard({ tool, canApprove, busy, pending, onDecision, onInspect }: {
  tool: ToolPresentation;
  canApprove: boolean;
  busy: boolean;
  pending: boolean;
  onDecision: (allow: boolean) => void;
  onInspect: () => void;
}) {
  if (tool.approval) return <section className="approval chat-approval" data-request-id={tool.approval.request_id} data-risk={tool.risk === "write" ? "write" : tool.risk === "read" ? "read" : "unknown"} aria-label="等待审批">
    <div className="approval-title"><Shield aria-hidden="true" /><strong data-ui-data>{tool.name}</strong><span>等待审批</span></div>
    <ToolProvenance value={{ source: tool.source, risk: tool.risk ?? "unknown" }} />
    <ApprovalTime expiresAt={tool.approval.expires_at} />
    <pre data-ui-data className="approval-arguments" tabIndex={0} aria-label={`${tool.name} 的调用参数`}>{valueText(tool.approval.arguments)}</pre>
    <div className="approval-actions">
      <Button disabled={!canApprove || busy || pending} onClick={() => onDecision(true)} aria-label={`允许 ${tool.name} 本次调用`}>允许本次调用</Button>
      <Button variant="outline" disabled={!canApprove || busy || pending} onClick={() => onDecision(false)} aria-label={`拒绝 ${tool.name} 本次调用`}>拒绝</Button>
      {pending && <span role="status">正在确认决定</span>}
      {!canApprove && <span>当前身份没有审批权限</span>}
    </div>
  </section>;

  return <details className="chat-tool" data-state={tool.state} data-call-id={tool.callId ?? tool.providerCallId}>
    <summary onClick={event => { if (!(event.currentTarget.parentElement as HTMLDetailsElement).open) onInspect(); }}><ChevronRight className="chat-tool-chevron" aria-hidden="true" />{tool.state === "running" ? <LoaderCircle className="chat-tool-spinner" aria-hidden="true" /> : <Wrench aria-hidden="true" />}<strong data-ui-data>{tool.name}</strong><span>{tool.code === "approval_expired" ? "审批已过期" : labels[tool.state]}</span></summary>
    <div className="chat-tool-body">
      <ToolProvenance value={{ source: tool.source, risk: tool.risk }} />
      <div className="chat-tool-section"><span>参数</span><pre data-ui-data tabIndex={0}>{valueText(tool.arguments)}</pre></div>
      {tool.resultAvailable ? <div className="chat-tool-section"><span>结果</span><pre data-ui-data tabIndex={0}>{valueText(tool.result)}</pre></div> : <p className="detail-empty">{missingResult(tool)}</p>}
      {tool.code && <p className="chat-tool-error" data-ui-data>{tool.code === "invalid_input" ? "工具参数无效" : runReason(tool.code)} <code>{tool.code}</code></p>}
    </div>
  </details>;
}
