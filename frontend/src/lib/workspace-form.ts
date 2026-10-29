import {
  artifactPath, parseInputSnapshot, parseWorkspaceTurn, sha256,
  type Completion, type CompletionCondition, type InputFile, type InputSnapshotRequest,
  type Json, type WorkspaceTurnRequest,
} from "./workspace-contracts";

export interface TextFileDraft { id: string; path: string; content: string }
export interface CompletionConditionDraft {
  uid: string;
  id: string;
  kind: CompletionCondition["kind"];
  path: string;
  pointer: string;
  expectedText: string;
  symbolsText: string;
}

const utf8 = (value: string) => new TextEncoder().encode(value);

function base64(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

export function validateTextFiles(files: TextFileDraft[]): number {
  if (files.length > 32) throw new Error("最多可提交 32 个文件");
  const paths = new Set<string>();
  let total = 0;
  for (const file of files) {
    try { artifactPath(file.path); }
    catch { throw new Error(`相对路径无效：${file.path || "（空）"}`); }
    if (paths.has(file.path)) throw new Error(`文件路径重复：${file.path}`);
    paths.add(file.path);
    const size = utf8(file.content).byteLength;
    if (size > 32768) throw new Error(`单个文件不能超过 32 KiB：${file.path}`);
    total += size;
  }
  if (total > 32768) throw new Error("文件总大小不能超过 32 KiB");
  return total;
}

export async function createInputSnapshot(files: TextFileDraft[], expectedHeadRevision: number,
  requestKey = crypto.randomUUID(), isCurrent: () => boolean = () => true): Promise<InputSnapshotRequest> {
  if (!Number.isSafeInteger(expectedHeadRevision) || expectedHeadRevision < 0)
    throw new Error("输入版本必须是非负整数");
  validateTextFiles(files);
  const encoded: InputFile[] = [];
  for (const file of files) {
    if (!isCurrent()) throw new Error("工作区身份已切换");
    const bytes = utf8(file.content);
    const digest = await sha256(bytes);
    if (!isCurrent()) throw new Error("工作区身份已切换");
    encoded.push({ path: file.path, encoding: "base64", data: base64(bytes), sha256: digest });
  }
  return parseInputSnapshot({ schema_version: 1, request_key: requestKey,
    expected_head_revision: expectedHeadRevision, files: encoded });
}

export function createCompletion(requiredPaths: string[], drafts: CompletionConditionDraft[]): Completion {
  const conditions: CompletionCondition[] = drafts.map(draft => {
    let expected: Json = null;
    let symbols: string[] = [];
    if (draft.kind === "json_equal") {
      try { expected = JSON.parse(draft.expectedText) as Json; }
      catch { throw new Error(`条件「${draft.id || "未命名"}」的预期值不是有效 JSON`); }
    } else {
      symbols = draft.symbolsText.split(/\r?\n/).map(value => value.trim()).filter(Boolean);
    }
    return { id: draft.id, kind: draft.kind, path: draft.path,
      pointer: draft.kind === "json_equal" ? draft.pointer : "", expected, symbols };
  });
  const value: Completion = { profile: "python-unittest/1", required_paths: [...requiredPaths], conditions };
  parseWorkspaceTurn({ schema_version: 1, request_key: crypto.randomUUID(), text: "条件检查",
    expected_revision: 0, input: { mode: "head_at_dispatch", revision_id: null }, completion: value });
  return value;
}

export function createWorkspaceTurn(text: string, completion: Completion,
  expectedRevision: number): WorkspaceTurnRequest {
  return parseWorkspaceTurn({ schema_version: 1, request_key: crypto.randomUUID(), text,
    expected_revision: expectedRevision,
    input: { mode: "head_at_dispatch", revision_id: null }, completion });
}
