/** 与 Python 公开 HTTP/SSE 合约同步；浏览器不绕过服务端权限。 */
export interface TopicView {
  id: string;
  title: string;
  created_at: string;
  updated_at: string;
  active_branches: Record<string, string | null>;
  locale: "zh-CN" | "en-US";
}
export interface LocaleView { requested: string; effective: "zh-CN" | "en-US"; fallback_chain: string[]; }
export interface MessageView {
  id: string;
  topic_id: string;
  run_id: string;
  sequence: number;
  role: "system" | "user" | "assistant" | "tool";
  content: string;
  details: Record<string, unknown>;
  created_at: string;
  parent_id: string | null;
}
export interface MentionRef {
  id: string;
  kind: "text" | "file" | "resource";
  label: string;
  content: string;
  metadata: Record<string, unknown>;
}
export interface MentionOccurrence { mention_id: string; start: number; end: number; }
export interface ContextTag { name: string; content: string; metadata: Record<string, unknown>; }
export interface PromptContext { mentions: MentionRef[]; context_tags: ContextTag[]; }
export interface MessageTreeView {
  topic: TopicView;
  messages: MessageView[];
  active_path: string[];
}
export interface RunView {
  id: string;
  topic_id: string;
  input_message_id: string;
  status: "queued" | "running" | "completed" | "failed" | "aborted" | "interrupted";
  error: string | null;
  error_code: string | null;
}
export interface StreamFrame {
  stream_id: string;
  seq: number;
  kind: "start" | "chunk" | "end" | "error";
  data: Record<string, unknown> | null;
}
export interface CompactResponse {
  topic_id: string;
  status: "proposed" | "completed" | "unchanged";
  persisted: boolean;
  branch_key: string;
  source_message_ids: string[];
  summary: string | null;
  reason: string | null;
  omitted_count: number;
  messages: MessageView[];
}
export interface InteractionView {
  id: string;
  run_id: string;
  tool_name: string;
  kind: "approval" | "question";
  payload: Record<string, unknown>;
  status: "pending" | "approved" | "denied" | "answered" | "cancelled" | "expired";
  response: Record<string, unknown> | null;
  expires_at: number;
}
export interface AgentDescriptor {
  name: string;
  description: string;
  risk: "read" | "write";
  max_turns: number;
  exposed_as_tool: boolean;
}
export interface AgentView {
  agent_id: string;
  name: string;
  topic_id: string;
  status: string;
  current_run_id: string | null;
  turn: number;
  result: { text: string; code: string | null; error: string | null } | null;
}
export interface WorkflowRunView {
  run_id: string;
  status: "queued" | "running" | "paused" | "completed" | "failed" | "aborted";
  epoch: number;
  current_node: string | null;
  output: unknown;
  node_statuses: Record<string, string>;
  node_outputs: Record<string, Record<string, unknown>>;
  error: string | null;
  code: string | null;
  event_count: number;
}
export interface WorkflowEvent { run_id: string; seq: number; epoch: number; type: string; node_id: string | null; data: unknown; }
export interface WorkflowPort { name: string; json_schema?: Record<string, unknown>; required?: boolean; multiple?: boolean; }
export interface WorkflowGraph { nodes: Array<{ id: string; kind: string; config: Record<string, unknown>; developer_only?: boolean; input_ports?: WorkflowPort[] | null; output_ports?: WorkflowPort[] | null }>; edges: Array<{ source: string; target: string; source_port?: string; target_port?: string; when?: string | null }>; output_node?: string | null; }
export interface HealthView { provider: string; service: string; }

let apiKey = "";
export function setApiKey(key: string): void { apiKey = key; }
export function errorText(error: unknown): string {
  return error instanceof Error ? error.message : "操作失败，请检查连接与运行状态";
}
function headers(): Headers {
  const value = new Headers({ "Content-Type": "application/json" });
  if (apiKey) value.set("Authorization", `Bearer ${apiKey}`);
  return value;
}
export async function requestJson<T>(path: string, init: RequestInit = {}): Promise<T> {
  const merged = headers();
  new Headers(init.headers).forEach((value, key) => merged.set(key, value));
  const response = await fetch(path, { ...init, headers: merged });
  const value: unknown = await response.json().catch(() => null);
  if (!response.ok) {
    const body = value as { error?: { message?: string } } | null;
    throw new Error(body?.error?.message || `HTTP ${response.status}`);
  }
  if (value === null) throw new Error("服务没有返回有效的数据，请检查连接");
  return value as T;
}
export async function consumeEvents(
  runId: string,
  fromSeq: number,
  onFrame: (frame: StreamFrame) => void,
  signal?: AbortSignal,
): Promise<number> {
  const response = await fetch(`/v1/runs/${encodeURIComponent(runId)}/events?from_seq=${fromSeq}`, {
    headers: headers(), signal,
  });
  if (!response.ok) {
    const body = await response.json() as { error?: { message?: string } };
    throw new Error(body.error?.message || `HTTP ${response.status}`);
  }
  if (!response.body) throw new Error("浏览器未提供流式响应");
  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let buffer = "";
  let cursor = fromSeq;
  let terminal = false;
  try {
    while (true) {
      const { value, done } = await reader.read();
      buffer += decoder.decode(value ?? new Uint8Array(), { stream: !done });
      if (buffer.length > 262144) throw new Error("流帧超过大小限制");
      const blocks = buffer.split("\n\n");
      buffer = blocks.pop() ?? "";
      for (const block of blocks) {
        if (block.startsWith(":")) continue;
        let id = "", kind = "";
        const data: string[] = [];
        for (const line of block.split("\n")) {
          if (line.startsWith("data:")) data.push(line.slice(5).trimStart());
          if (line.startsWith("id:")) id = line.slice(3).trim();
          if (line.startsWith("event:")) kind = line.slice(6).trim();
        }
        if (!data.length) continue;
        const raw = data.join("\n");
        if (new TextEncoder().encode(raw).length > 131072) throw new Error("流事件超过大小限制");
        const frame = JSON.parse(raw) as StreamFrame;
        if (frame.stream_id !== runId || frame.seq !== cursor || id !== String(cursor) || kind !== frame.kind) {
          throw new Error("流事件关联或序号不正确");
        }
        cursor += 1;
        onFrame(frame);
        if (frame.kind === "end" || frame.kind === "error") {
          terminal = true;
          await reader.cancel();
          return cursor;
        }
      }
      if (done) break;
    }
  } finally { reader.releaseLock(); }
  if (!terminal) throw new Error("连接已断开，可从已保存的游标继续读取");
  return cursor;
}
export async function consumeWorkflowEvents(runId: string, fromSeq: number, onEvent: (event: WorkflowEvent) => void): Promise<number> {
  const path = "/v1/workflows/runs/" + encodeURIComponent(runId);
  const response = await fetch(path + "/events?from_seq=" + fromSeq, { headers: headers() });
  if (!response.ok) throw new Error("Workflow 请求失败，请检查权限、状态或游标");
  if (!response.body) throw new Error("浏览器未提供 Workflow 流");
  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let buffer = "", cursor = fromSeq, epoch = -1;
  try {
    while (true) {
      const { value, done } = await reader.read();
      buffer += decoder.decode(value ?? new Uint8Array(), { stream: !done });
      const blocks = buffer.split("\n\n");
      buffer = blocks.pop() ?? "";
      if (buffer.length > 262144) throw new Error("Workflow 流帧超过大小限制");
      for (const block of blocks) {
        let id = "", kind = "";
        const lines: string[] = [];
        for (const line of block.split("\n")) {
          if (line.startsWith("data:")) lines.push(line.slice(5).trimStart());
          if (line.startsWith("id:")) id = line.slice(3).trim();
          if (line.startsWith("event:")) kind = line.slice(6).trim();
        }
        if (!lines.length) continue;
        const data = lines.join("\n");
        if (new TextEncoder().encode(data).length > 131072) throw new Error("Workflow 流事件超过大小限制");
        const event = JSON.parse(data) as WorkflowEvent;
        if (event.run_id !== runId || event.seq !== cursor || id !== String(cursor) || kind !== event.type
          || !Number.isSafeInteger(event.epoch) || event.epoch < epoch) {
          throw new Error("Workflow 事件关联、代次或游标不正确");
        }
        epoch = event.epoch;
        cursor += 1;
        onEvent(event);
        if (event.type === "end") { await reader.cancel(); return cursor; }
      }
      if (done) break;
    }
  } finally { reader.releaseLock(); }
  const current = await requestJson<WorkflowRunView>(path);
  if (["queued", "running"].includes(current.status) || current.event_count !== cursor) {
    throw new Error("Workflow 流连接已断开，可从游标继续");
  }
  return cursor;
}
