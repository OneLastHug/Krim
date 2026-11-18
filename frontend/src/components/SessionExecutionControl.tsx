import { useEffect, useRef, useState } from "react";
import { ChevronDown } from "lucide-react";
import type { SessionExecutionState } from "../hooks/use-execution-policy";
import { modeDescriptions, modeLabels, type ExecutionMode } from "../lib/execution-policy";
import { PopoverMenu } from "./popover-menu";
import { Button } from "./ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "./ui/dialog";
import "./session-execution.css";

const environmentLabels: Record<string, string> = {
  unprovisioned: "未准备", creating: "准备中", starting: "启动中", ready: "运行中",
  stopping: "停止中", stopped: "已停止", recovering: "恢复中", failed: "异常", unavailable: "不可用",
};
const modes: ExecutionMode[] = ["manual", "auto", "bypass"];

/** Replace only the existing permissions placeholder; retain its icon and class. */
export function SessionExecutionControl({ execution, canChange }: {
  execution: SessionExecutionState; canChange: boolean;
}) {
  const [open, setOpen] = useState(false);
  const trigger = useRef<HTMLButtonElement>(null);
  const identity = execution.identity;
  useEffect(() => setOpen(false), [identity]);
  const policy = execution.policy;
  const readonly = policy?.project_access === "read";
  const busy = execution.changing || execution.clearing;
  async function choose(mode: ExecutionMode) {
    if (await execution.changeMode(mode)) setOpen(false);
  }
  return <>
    <button type="button" ref={trigger} className="control-chip" onClick={() => setOpen(value => !value)}
      aria-label={policy ? `执行模式：${modeLabels[policy.mode]}${readonly ? "，项目只读" : ""}` : "执行模式"}
      title={policy ? `执行模式：${modeLabels[policy.mode]}，仅影响当前会话` : "读取当前会话执行模式"}
      aria-haspopup="menu" aria-expanded={open}>
      {readonly ? "只读 · " : ""}{policy ? modeLabels[policy.mode] : "权限"}<ChevronDown aria-hidden="true" />
    </button>
    <PopoverMenu open={open} anchor={trigger.current} placement="above" className="session-execution-menu" onClose={() => setOpen(false)} label="执行模式">
      <p className="session-execution-hint">仅影响当前会话</p>
      {execution.loading && !policy && <p role="status" className="session-execution-hint">正在读取执行模式</p>}
      {modes.map(mode => <button key={mode} type="button" role="menuitemradio" className="menu-item"
        aria-checked={policy?.mode === mode} disabled={!canChange || !policy?.project_id || busy}
        onClick={() => void choose(mode)}>{modeLabels[mode]}{policy?.mode === mode ? "（当前）" : ""}</button>)}
      {policy && <p className="session-execution-hint">{modeDescriptions[policy.mode]}</p>}
      {readonly && <p className="session-execution-hint">项目只读：不能修改文件或运行 Shell。切换模式不会增加项目权限。</p>}
      {policy && !policy.project_id && <p className="session-execution-hint">此历史会话尚未绑定项目，保留原有确认方式。</p>}
      {execution.changing && <p className="session-execution-hint" role="status">正在保存，当前模式尚未更改</p>}
      {execution.error && <div className="session-execution-error" role="alert"><p>{execution.error}</p>
        <Button type="button" variant="outline" size="sm" disabled={busy} onClick={() => void (execution.hasPendingMode ? execution.retryMode() : execution.reload())}>
          {execution.hasPendingMode ? "重试原请求" : "重新读取"}</Button></div>}
      {(execution.ruleCount > 0 || execution.hasPendingClear) && <div className="session-execution-rules">
        <p className="session-execution-hint">本会话已保存 {execution.ruleCount} 条允许规则。</p>
        <Button type="button" variant="outline" size="sm" disabled={!canChange || busy} onClick={() => void execution.clearRules()}>
          {execution.clearing ? "正在清理" : execution.hasPendingClear ? "重试原清理请求" : "清除本会话允许规则"}</Button>
      </div>}
      {execution.clearError && <p className="session-execution-error" role="alert">{execution.clearError}</p>}
    </PopoverMenu>
    <Dialog open={execution.projectOpen} onOpenChange={value => { if (!value) execution.closeProject(); }}>
      <DialogContent className="session-project-permission-dialog" aria-describedby={undefined}>
        <DialogHeader><DialogTitle>项目详情</DialogTitle></DialogHeader>
        {!execution.project && !execution.projectError && <p role="status">正在读取项目详情</p>}
        {execution.project && <dl className="session-project-permission-grid">
          <dt>项目名称</dt><dd>{execution.project.title}</dd>
          <dt>创建者</dt><dd>{execution.project.is_creator ? "当前账号" : execution.project.owner_id}</dd>
          <dt>我的权限</dt><dd>{execution.project.project_access === "read" ? "只读" : execution.project.is_creator ? "写入 · 创建者" : "写入"}</dd>
          <dt>沙箱状态</dt><dd>{environmentLabels[execution.project.sandbox_status] ?? "状态未知"}</dd>
        </dl>}
        {execution.projectError && <div className="session-execution-error" role="alert"><p>{execution.projectError}</p>
          <Button type="button" variant="outline" size="sm" onClick={() => void execution.openProject()}>重新读取</Button></div>}
        <div className="dialog-actions"><Button type="button" variant="outline" onClick={execution.closeProject}>关闭</Button></div>
      </DialogContent>
    </Dialog>
  </>;
}
