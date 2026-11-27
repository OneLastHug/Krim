import type { ReactNode } from "react";
import { ChevronRight, FileText, LoaderCircle } from "lucide-react";
import type { ToolPresentation } from "../lib/session-presentation";
import { fileGroupSummary, type FileToolGroup } from "../lib/file-tool-groups";
import "./file-tool-group.css";

export function FileToolGroupCard({ group, renderTool }: {
  group: FileToolGroup;
  renderTool: (tool: ToolPresentation) => ReactNode;
}) {
  const summary = fileGroupSummary(group.tools);
  const liveNumbers = summary.liveAdded !== null || summary.liveRemoved !== null;
  const showAdded = summary.counted > 0 || summary.liveAdded !== null;
  const showRemoved = summary.counted > 0 || summary.liveRemoved !== null;
  const added = summary.added + (summary.liveAdded ?? 0);
  const removed = summary.removed + (summary.liveRemoved ?? 0);
  const unrecorded = summary.uncounted ? `；${summary.uncounted} 次修改未记录行数` : "";
  return <details className="chat-tool chat-file-group" data-state={summary.state} data-file-path={group.path}>
    <summary>
      <ChevronRight className="chat-tool-chevron" aria-hidden="true" />
      {summary.active ? <LoaderCircle className="chat-tool-spinner" aria-hidden="true" /> : <FileText aria-hidden="true" />}
      <span className="chat-tool-name chat-file-path" title={group.path} data-ui-data>{group.path}</span>
      <span className="chat-file-operations">{summary.operations}</span>
      {(showAdded || showRemoved) && <span className="chat-file-changes" aria-label={liveNumbers
        ? `新增 ${added} 行，删除 ${removed} 行，含生成中的实时预览（未确认）${unrecorded}`
        : `累计新增 ${summary.added} 行，删除 ${summary.removed} 行${unrecorded}`}
        title={liveNumbers
          ? `生成中的增删行数为实时预览${summary.livePartial ? "，部分行数暂未生成" : ""}${summary.counted > 0 ? "；已完成的调用计入合计" : ""}`
          : `各次已完成修改的增删行数合计${summary.uncounted ? `；另有 ${summary.uncounted} 次历史修改未记录行数` : ""}`}>
        {showAdded && <span className="chat-file-added">+{added}</span>}
        {showRemoved && <span className="chat-file-removed">−{removed}</span>}
      </span>}
      {summary.uncounted > 0 && <span className="chat-file-unrecorded" title="旧调用结果未记录实际增删行数">{summary.counted > 0 || liveNumbers ? "部分行数未记录" : "行数未记录"}</span>}
      <span className="chat-tool-meta" role="status">{summary.status}</span>
    </summary>
    <div className="chat-file-calls">{group.tools.map(renderTool)}</div>
  </details>;
}
