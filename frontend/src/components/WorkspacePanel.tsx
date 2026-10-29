import { useEffect, useRef, useState, type ChangeEvent, type FormEvent } from "react";
import type { SessionState } from "../hooks/use-session";
import { useWorkspace } from "../hooks/use-workspace";
import { request } from "../api";
import { artifactPath, parseArtifact } from "../lib/workspace-contracts";
import { workBase } from "../lib/workspace-requests";
import { createCompletion, createInputSnapshot, createWorkspaceTurn,
  validateTextFiles, type CompletionConditionDraft, type TextFileDraft } from "../lib/workspace-form";
import { workspaceCopy } from "../lib/copy";
import { ArtifactView } from "./ArtifactView";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
import { Textarea } from "./ui/textarea";
import "./workspace-panel.css";

type Draft = {
  identity: string;
  text: string;
  files: TextFileDraft[];
  selectedFileId: string | null;
  expectedHeadRevision: string;
  requiredPaths: string[];
  requiredPathInput: string;
  conditions: CompletionConditionDraft[];
};

function emptyDraft(identity: string): Draft {
  return { identity, text: "", files: [], selectedFileId: null,
    expectedHeadRevision: "", requiredPaths: [], requiredPathInput: "", conditions: [] };
}

function errorText(error: unknown): string {
  if (error instanceof Error && error.message !== "工作区合同无效") return error.message;
  return "请检查路径、文件大小、重复项和完成条件数量";
}

export function WorkspacePanel({ chat, apiKey, active, fence }: { chat: SessionState; apiKey: string;
  active: boolean; fence: { current: number } }) {
  const work = useWorkspace(chat, apiKey, active, fence);
  const available = chat.actor?.target_keys?.includes("target-workspace") ?? false;
  const canReadWorkspace = active && available && Boolean(chat.actor && chat.id &&
    chat.session?.target_key === "target-workspace");
  const canUseWorkspace = active && available && chat.canWrite && chat.session?.status === "open";
  const disabled = !canUseWorkspace || work.busy;
  const identity = JSON.stringify([chat.actor?.principal_id, chat.actor?.generation, chat.workspace,
    chat.id, chat.session?.target_key, apiKey, active, chat.canWrite, chat.session?.status,
    available, fence.current]);
  const identityRef = useRef(identity);
  identityRef.current = identity;
  const headReadController = useRef<AbortController | null>(null);
  const [headReadOwner, setHeadReadOwner] = useState<string | null>(null);
  const readingHead = headReadOwner === identity;
  useEffect(() => {
    headReadController.current?.abort();
    headReadController.current = null;
    setHeadReadOwner(null);
    identityRef.current = identity;
    return () => {
      headReadController.current?.abort();
      headReadController.current = null;
      if (identityRef.current === identity) identityRef.current = "";
    };
  }, [identity]);
  const [storedDraft, setStoredDraft] = useState(() => emptyDraft(identity));
  const draft = storedDraft.identity === identity ? storedDraft : emptyDraft(identity);
  const draftRef = useRef(draft);
  draftRef.current = draft;
  const [feedback, setFeedback] = useState({ identity, error: "" });
  const error = feedback.identity === identity ? feedback.error : "";
  function setError(message: string, owner = identity) {
    if (identityRef.current === owner) setFeedback({ identity: owner, error: message });
  }
  const [readStatus, setReadStatus] = useState({ identity, reading: false });
  const readingFiles = readStatus.identity === identity && readStatus.reading;
  function setReadingFiles(reading: boolean, owner = identity) {
    if (identityRef.current === owner) setReadStatus({ identity: owner, reading });
  }
  const selectedFile = draft.files.find(file => file.id === draft.selectedFileId) ?? draft.files[0] ?? null;

  function current(owner: string) {
    return identityRef.current === owner && active && canUseWorkspace;
  }
  function currentRead(owner: string) {
    return identityRef.current === owner && canReadWorkspace;
  }
  async function readCurrentHead() {
    if (!currentRead(identity) || readingHead) return;
    const owner = identity, captureWorkspace = work.capture(), controller = new AbortController();
    headReadController.current?.abort();
    headReadController.current = controller;
    setHeadReadOwner(owner);
    try {
      const scope = work.scope();
      const head = parseArtifact(await request(workBase(scope) + "/artifacts/head", apiKey,
        undefined, controller.signal));
      if (!currentRead(owner) || !captureWorkspace() || controller.signal.aborted) return;
      if (head.session_id !== scope.session_id) throw new Error("当前文件版本身份不符");
      setStoredDraft(previous => {
        if (!currentRead(owner) || !captureWorkspace() || controller.signal.aborted) return previous;
        const base = previous.identity === owner ? previous : emptyDraft(owner);
        return { ...base, expectedHeadRevision: String(head.revision) };
      });
      setError("");
    } catch {
      if (currentRead(owner) && captureWorkspace() && !controller.signal.aborted)
        setError("无法读取当前工作区文件版本；请检查身份与读取权限后重试", owner);
    } finally {
      if (headReadController.current === controller) headReadController.current = null;
      if (identityRef.current === owner) setHeadReadOwner(null);
    }
  }
  function updateDraft(update: (value: Draft) => Draft, owner = identity) {
    if (!current(owner)) return;
    setStoredDraft(previous => {
      if (!current(owner)) return previous;
      const base = previous.identity === owner ? previous : emptyDraft(owner);
      return { ...update(base), identity: owner };
    });
    setError("");
  }
  function updateFile(fileId: string, change: Partial<TextFileDraft>) {
    if (disabled) return;
    updateDraft(value => ({ ...value, files: value.files.map(file =>
      file.id === fileId ? { ...file, ...change } : file) }));
  }
  function addBlankFile() {
    if (disabled || draft.files.length >= 32) return;
    let suffix = draft.files.length + 1;
    let path = `new-file-${suffix}.txt`;
    while (draft.files.some(file => file.path === path)) path = `new-file-${++suffix}.txt`;
    const file = { id: crypto.randomUUID(), path, content: "" };
    updateDraft(value => ({ ...value, files: [...value.files, file], selectedFileId: file.id }));
  }
  async function importFiles(event: ChangeEvent<HTMLInputElement>) {
    const input = event.currentTarget;
    const selected = Array.from(input.files ?? []);
    input.value = "";
    if (!selected.length || !canUseWorkspace) return;
    const owner = identity;
    const captureWorkspace = work.capture();
    const startingDraft = draftRef.current;
    const existing = startingDraft.identity === owner ? startingDraft.files : [];
    if (existing.length + selected.length > 32) {
      setError("最多可提交 32 个文件");
      return;
    }
    if (selected.some(file => file.size > 32768)) {
      setError("单个文件不能超过 32 KiB");
      return;
    }
    setReadingFiles(true, owner);
    try {
      const imported = await Promise.all(selected.map(async file => ({
        id: crypto.randomUUID(),
        path: (file as File & { webkitRelativePath?: string }).webkitRelativePath || file.name,
        content: await file.text(),
      })));
      if (!current(owner) || !captureWorkspace()) return;
      const latest = draftRef.current.identity === owner ? draftRef.current : emptyDraft(owner);
      const nextFiles = [...latest.files, ...imported];
      validateTextFiles(nextFiles);
      updateDraft(value => ({ ...value, files: [...value.files, ...imported],
        selectedFileId: imported[0]?.id ?? value.selectedFileId }), owner);
    } catch (reason) {
      if (current(owner)) setError(errorText(reason));
    } finally {
      if (identityRef.current === owner) setReadingFiles(false, owner);
    }
  }
  function removeFile(fileId: string) {
    if (disabled) return;
    updateDraft(value => {
      const files = value.files.filter(file => file.id !== fileId);
      return { ...value, files, selectedFileId: value.selectedFileId === fileId ? files[0]?.id ?? null : value.selectedFileId };
    });
  }
  function addRequiredPath() {
    if (disabled) return;
    const path = draft.requiredPathInput;
    try {
      artifactPath(path);
      if (draft.requiredPaths.includes(path)) throw new Error(`文件路径重复：${path}`);
      if (draft.requiredPaths.length + draft.conditions.length >= 16) throw new Error("必需路径与完成条件合计最多 16 项");
      updateDraft(value => ({ ...value, requiredPaths: [...value.requiredPaths, path], requiredPathInput: "" }));
    } catch (reason) { setError(errorText(reason)); }
  }
  function addCondition() {
    if (disabled) return;
    if (draft.requiredPaths.length + draft.conditions.length >= 16) {
      setError("必需路径与完成条件合计最多 16 项");
      return;
    }
    const id = `condition-${crypto.randomUUID().slice(0, 8)}`;
    const condition: CompletionConditionDraft = { uid: crypto.randomUUID(), id, kind: "json_equal",
      path: "", pointer: "", expectedText: "null", symbolsText: "" };
    updateDraft(value => ({ ...value, conditions: [...value.conditions, condition] }));
  }
  function updateCondition(uid: string, change: Partial<CompletionConditionDraft>) {
    if (disabled) return;
    updateDraft(value => ({ ...value, conditions: value.conditions.map(condition =>
      condition.uid === uid ? { ...condition, ...change } : condition) }));
  }
  function removeRequiredPath(path: string) {
    if (disabled) return;
    updateDraft(value => ({ ...value, requiredPaths: value.requiredPaths.filter(item => item !== path) }));
  }
  function removeCondition(uid: string) {
    if (disabled) return;
    updateDraft(value => ({ ...value, conditions: value.conditions.filter(item => item.uid !== uid) }));
  }
  async function submitSnapshot(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!canUseWorkspace || work.busy) return;
    const owner = identity;
    const captureWorkspace = work.capture();
    try {
      const revisionText = draft.expectedHeadRevision.trim();
      if (!revisionText) throw new Error("请填写当前工作区文件版本；对话轮次不是文件版本");
      const revision = Number(revisionText);
      if (!Number.isSafeInteger(revision) || revision < 0)
        throw new Error("输入版本必须是非负整数");
      const request = await createInputSnapshot(draft.files, revision, crypto.randomUUID(),
        () => current(owner) && captureWorkspace());
      if (!current(owner) || !captureWorkspace() || work.busy) return;
      void work.input(request);
    } catch (reason) { if (current(owner)) setError(errorText(reason)); }
  }
  function submitTurn(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!canUseWorkspace || work.busy || !chat.session) return;
    try {
      const completion = createCompletion(draft.requiredPaths, draft.conditions);
      createWorkspaceTurn(draft.text, completion, chat.session.revision);
      work.submit(draft.text, completion);
    } catch (reason) { setError(errorText(reason)); }
  }
  return <section className="workspace-panel" aria-label="工作区任务">
    <h3>{workspaceCopy.title}</h3>
    {!available && <p role="status">{workspaceCopy.unavailable}</p>}
    {work.notice && <p role="status">{work.notice}</p>}
    {error && <p className="workspace-form-error" role="alert">{error}</p>}
    {work.pending.map(intent => <div className="workspace-pending" key={intent.id}>
      <span>{intent.kind === "turn" ? "工作任务" : "输入快照"}结果未确认</span>
      <Button variant="outline" disabled={work.busy} onClick={() => void work.recover(intent)}>{workspaceCopy.find}</Button>
    </div>)}
    {work.known.map(record => <div key={record.id} className="workspace-known">
      <span data-ui-data>{record.kind === "input" ? "已确认输入" : "已确认任务"} · {record.request_key}</span>
      <Button variant="outline" disabled={work.busy} onClick={() => void work.recoverConfirmed(record)}>读取原工作对象</Button>
    </div>)}

    <form id="workspace-input-form" className="workspace-form workspace-section" onSubmit={event => void submitSnapshot(event)}>
      <h4>输入文件</h4>
      <div className="workspace-revision-field">
        <Label htmlFor="workspace-input-revision">输入版本</Label>
        <Input id="workspace-input-revision" type="number" min="0" step="1" inputMode="numeric"
          value={draft.expectedHeadRevision} onChange={event => updateDraft(value =>
            ({ ...value, expectedHeadRevision: event.target.value }))} disabled={disabled}
          aria-describedby="workspace-input-revision-help" />
        <p id="workspace-input-revision-help" className="workspace-field-help">
          填写当前工作区文件版本号；此版本独立于对话轮次，未知时请先确认，不要猜测。
        </p>
        <div className="workspace-version-actions">
          <Button type="button" variant="outline" disabled={!canReadWorkspace || readingHead}
            aria-busy={readingHead} onClick={() => void readCurrentHead()}>读取当前输入版本</Button>
          {readingHead && <span role="status">正在读取当前工作区版本</span>}
        </div>
      </div>
      <div className="workspace-import-field">
        <Label htmlFor="workspace-file-import">导入文本文件</Label>
        <Input id="workspace-file-import" type="file" multiple disabled={disabled || readingFiles}
          onChange={event => void importFiles(event)} />
      </div>
      <div className="workspace-file-actions">
        <Button type="button" variant="outline" disabled={disabled || draft.files.length >= 32} onClick={addBlankFile}>添加空文件</Button>
        <span role="status">{draft.files.length} / 32 个文件，{validateTextFilesSafe(draft.files)} / 32768 字节</span>
      </div>
      {readingFiles && <p role="status">正在读取文件</p>}
      <div className="workspace-file-workbench">
        <div role="group" aria-label="输入文件列表">
          {draft.files.length ? <ul className="workspace-file-list">{draft.files.map(file => <li key={file.id}>
            <button type="button" className="workspace-file-select" aria-pressed={selectedFile?.id === file.id}
              onClick={() => { if (!disabled) updateDraft(value => ({ ...value, selectedFileId: file.id })); }}>
              <span>{file.path || "未命名文件"}</span>
              {selectedFile?.id === file.id && <span className="workspace-selected-label">已选</span>}
            </button>
          </li>)}</ul> : <p className="workspace-empty-files">尚未添加文件</p>}
        </div>
        {selectedFile && <div className="workspace-file-editor">
          <div className="workspace-file-editor-heading">
            <Label htmlFor={`workspace-file-path-${selectedFile.id}`}>相对路径</Label>
            <Button type="button" variant="ghost" disabled={disabled} aria-label={`移除 ${selectedFile.path || "未命名文件"}`}
              onClick={() => removeFile(selectedFile.id)}>移除文件</Button>
          </div>
          <Input id={`workspace-file-path-${selectedFile.id}`} value={selectedFile.path}
            onChange={event => updateFile(selectedFile.id, { path: event.target.value })}
            placeholder="src/example.py" disabled={disabled} />
          <Label htmlFor={`workspace-file-content-${selectedFile.id}`}>文件内容</Label>
          <Textarea id={`workspace-file-content-${selectedFile.id}`} className="workspace-file-content"
            value={selectedFile.content} onChange={event => updateFile(selectedFile.id, { content: event.target.value })}
            maxLength={32768} spellCheck={false} disabled={disabled} />
        </div>}
      </div>
      <Button type="submit" disabled={disabled || readingFiles}>提交输入快照</Button>
    </form>

    <form id="workspace-turn-form" className="workspace-form workspace-section" onSubmit={submitTurn}>
      <h4>工作请求</h4>
      <Label htmlFor="workspace-text">任务内容</Label>
      <Textarea id="workspace-text" value={draft.text} onChange={event => updateDraft(value =>
        ({ ...value, text: event.target.value }))} maxLength={32768} disabled={disabled} />
      <section className="workspace-checks" aria-labelledby="workspace-required-heading">
        <div className="workspace-list-heading">
          <h5 id="workspace-required-heading">必须存在的路径</h5>
          <span>{draft.requiredPaths.length} / 16 项（与完成条件合计）</span>
        </div>
        <div className="workspace-add-path">
          <Label htmlFor="workspace-required-path">相对路径</Label>
          <div className="workspace-inline-edit">
            <Input id="workspace-required-path" value={draft.requiredPathInput}
              onChange={event => updateDraft(value => ({ ...value, requiredPathInput: event.target.value }))}
              placeholder="src/result.py" disabled={disabled} />
            <Button type="button" variant="outline" disabled={disabled || draft.requiredPaths.length + draft.conditions.length >= 16}
              onClick={addRequiredPath}>添加路径</Button>
          </div>
        </div>
        {draft.requiredPaths.length > 0 && <ul className="workspace-check-list">{draft.requiredPaths.map(path => <li key={path}>
          <span className="workspace-check-mark">必须存在</span><code>{path}</code>
          <Button type="button" variant="ghost" disabled={disabled} aria-label={`移除必需路径 ${path}`}
            onClick={() => removeRequiredPath(path)}>移除</Button>
        </li>)}</ul>}
      </section>
      <section className="workspace-checks" aria-labelledby="workspace-conditions-heading">
        <div className="workspace-list-heading">
          <h5 id="workspace-conditions-heading">完成条件</h5>
          <span>{draft.conditions.length} 项</span>
        </div>
        {draft.conditions.length > 0 && <ol className="workspace-condition-list">{draft.conditions.map((condition, index) => <li key={condition.uid}>
          <fieldset className="workspace-condition">
            <legend>条件 {index + 1}</legend>
            <div className="workspace-condition-title">
              <span>{condition.id || "未命名条件"}</span>
              <Button type="button" variant="ghost" disabled={disabled} aria-label={`移除条件 ${condition.id || index + 1}`}
                onClick={() => removeCondition(condition.uid)}>移除条件</Button>
            </div>
            <Label htmlFor={`condition-id-${condition.uid}`}>条件名称</Label>
            <Input id={`condition-id-${condition.uid}`} value={condition.id} maxLength={128}
              onChange={event => updateCondition(condition.uid, { id: event.target.value })} disabled={disabled} />
            <Label htmlFor={`condition-kind-${condition.uid}`}>检查方式</Label>
            <select id={`condition-kind-${condition.uid}`} value={condition.kind} disabled={disabled}
              onChange={event => updateCondition(condition.uid, { kind: event.target.value as CompletionConditionDraft["kind"] })}>
              <option value="json_equal">JSON 值相等</option>
              <option value="ast_symbols">Python 符号存在</option>
            </select>
            <Label htmlFor={`condition-path-${condition.uid}`}>检查文件</Label>
            <Input id={`condition-path-${condition.uid}`} value={condition.path}
              onChange={event => updateCondition(condition.uid, { path: event.target.value })}
              placeholder="src/result.py" disabled={disabled} />
            {condition.kind === "json_equal" ? <>
              <Label htmlFor={`condition-pointer-${condition.uid}`}>JSON Pointer</Label>
              <Input id={`condition-pointer-${condition.uid}`} value={condition.pointer}
                onChange={event => updateCondition(condition.uid, { pointer: event.target.value })}
                placeholder="/result/status" disabled={disabled} />
              <Label htmlFor={`condition-expected-${condition.uid}`}>预期 JSON 值</Label>
              <Textarea id={`condition-expected-${condition.uid}`} className="workspace-expected-json"
                value={condition.expectedText} onChange={event => updateCondition(condition.uid,
                  { expectedText: event.target.value })} disabled={disabled} />
            </> : <>
              <Label htmlFor={`condition-symbols-${condition.uid}`}>Python 符号（每行一个）</Label>
              <Textarea id={`condition-symbols-${condition.uid}`} className="workspace-symbols"
                value={condition.symbolsText} onChange={event => updateCondition(condition.uid,
                  { symbolsText: event.target.value })} placeholder="main\nResult" disabled={disabled} />
            </>}
          </fieldset>
        </li>)}</ol>}
        <Button type="button" variant="outline" disabled={disabled || draft.requiredPaths.length + draft.conditions.length >= 16}
          onClick={addCondition}>添加完成条件</Button>
      </section>
      <Button type="submit" disabled={disabled || !draft.text.trim()}>{workspaceCopy.send}</Button>
    </form>
    <Button variant="ghost" disabled={!active} onClick={() => { if (active) work.refresh(); }}>刷新工作结果</Button>
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

function validateTextFilesSafe(files: TextFileDraft[]): number {
  try { return validateTextFiles(files); }
  catch { return files.reduce((total, file) => total + new TextEncoder().encode(file.content).byteLength, 0); }
}
