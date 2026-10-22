import { useState } from "react";
import type { SessionState } from "../hooks/use-session";
import { useWorkspace } from "../hooks/use-workspace";
import { type Completion, type InputSnapshotRequest } from "../lib/workspace-contracts";
import { workspaceCopy } from "../lib/copy";
import { ArtifactView } from "./ArtifactView";
import { Button } from "./ui/button";
import { Label } from "./ui/label";

const initialCompletion: Completion = { profile: "python-unittest/1", required_paths: [], conditions: [] };
export function WorkspacePanel({ chat, apiKey, active, fence }: { chat: SessionState; apiKey: string;
  active: boolean; fence: { current: number } }) {
  const work = useWorkspace(chat, apiKey, active, fence);
  const [text, setText] = useState("");
  const [completion, setCompletion] = useState(JSON.stringify(initialCompletion, null, 2));
  const [input, setInput] = useState(JSON.stringify({ schema_version: 1, request_key: "",
    expected_head_revision: 0, files: [] }, null, 2));
  const [error, setError] = useState("");
  const available = chat.actor?.target_keys?.includes("target-workspace");
  const disabled = !active || !available || !chat.canWrite || chat.session?.status !== "open" || work.busy;
  return <section className="workspace-panel" aria-label="工作区任务">
    <h3>{workspaceCopy.title}</h3>{!available && <p role="status">{workspaceCopy.unavailable}</p>}
    <p role="status">{work.notice || error}</p>
    {work.pending.map(intent => <div className="workspace-pending" key={intent.id}>
      <span>{intent.kind === "turn" ? "工作Turn" : "输入快照"}结果未确认</span>
      <Button variant="outline" disabled={work.busy} onClick={() => void work.recover(intent)}>{workspaceCopy.find}</Button>
    </div>)}
    {work.known.map(record => <div key={record.id} className="workspace-known">
      <span data-ui-data>{record.kind === "input" ? "已确认输入" : "已确认Turn"} · {record.request_key}</span>
      <Button variant="outline" onClick={() => void work.recoverConfirmed(record)}>读取原工作对象</Button>
    </div>)}
    <form onSubmit={event => { event.preventDefault(); setError(""); try {
      const value = JSON.parse(completion) as Completion; void work.submit(text, value);
    } catch { setError("完成条件JSON无效"); } }}>
      <Label htmlFor="workspace-text">工作请求</Label><textarea id="workspace-text" value={text}
        onChange={event => setText(event.target.value)} maxLength={32768} disabled={disabled} />
      <Label htmlFor="workspace-completion">完成条件</Label><textarea id="workspace-completion"
        value={completion} onChange={event => setCompletion(event.target.value)} disabled={disabled} />
      <Button type="submit" disabled={disabled || !text.trim()}>{workspaceCopy.send}</Button>
    </form>
    <details><summary>输入快照</summary><form onSubmit={event => { event.preventDefault(); setError("");
      try { const value = JSON.parse(input) as InputSnapshotRequest;
        if (!value.request_key) value.request_key = crypto.randomUUID(); void work.input(value);
      } catch { setError("输入快照JSON无效"); } }}>
      <Label htmlFor="workspace-input">输入文件与预期版本</Label><textarea id="workspace-input"
        value={input} onChange={event => setInput(event.target.value)} disabled={disabled} />
      <Button type="submit" disabled={disabled}>提交输入快照</Button>
    </form></details>
    <Button variant="ghost" onClick={work.refresh}>刷新工作结果</Button>
    {work.view && <div className="workspace-facts"><p>工作阶段：{work.view.phase}</p>
      <h4>计划</h4><pre data-ui-data>{JSON.stringify(work.view.plan, null, 2)}</pre>
      <h4>实际命令与工具</h4>{work.view.effects.map(effect => <p key={effect.call_id} data-ui-data>
        {effect.kind} · {effect.phase} · exit={effect.exit_code ?? "unknown"} · {effect.cleanup}</p>)}
      <h4>摘要</h4>{work.view.compactions.map(summary => <details key={String(summary.pass_index)}>
        <summary>历史摘要 · {String(summary.status)}</summary><pre data-ui-data>{String(summary.summary ?? "")}</pre>
      </details>)}
      <p role="status">核验：{work.view.verification?.state ?? "pending"}</p>
      <pre data-ui-data>{JSON.stringify(work.view.verification?.results ?? [], null, 2)}</pre>
    </div>}
    {work.artifact && <ArtifactView value={work.artifact} scope={work.scope()} apiKey={apiKey}
      capture={work.capture} />}
  </section>;
}
