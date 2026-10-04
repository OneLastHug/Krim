import { HostClient, isHostEnvelope, type HostEnvelope, type HostJson } from "./host.js";
import { requestJson, setApiKey, type StreamFrame } from "./api.js";

const element = (id: string): HTMLElement => document.getElementById(id)!;
const input = (id: string): HTMLInputElement => element(id) as HTMLInputElement;
const output = (value: HostJson): void => { element("host-output").textContent = JSON.stringify(value, null, 2); };
const frame = element("agent-frame") as HTMLIFrameElement;
const panel = element("host-panel");
let client: HostClient | null = null;
let sessionId: string | null = null;
let apiKey = "";
let pump: AbortController | null = null;
let open = true;

function status(message: string, ready = false): void {
  element("host-status").textContent = message;
  element("host-status").className = ready ? "badge ready" : "badge";
}
function target(): HostClient {
  if (!client || !frame.contentWindow || !frame.isConnected) throw new Error("iframe 尚未就绪或已销毁");
  return client;
}
function object(data: HostJson): Record<string, HostJson> {
  if (data === null || typeof data !== "object" || Array.isArray(data)) throw new Error("Host 参数需要对象");
  return data;
}
async function perform(channel: string, data: HostJson): Promise<HostJson> {
  if (channel === "host/sidebar") {
    const value = object(data).open;
    if (typeof value !== "boolean") throw new Error("侧栏状态无效");
    open = value; panel.hidden = !open;
    element("host-toggle").textContent = open ? "隐藏侧栏" : "打开侧栏";
    return { open };
  }
  const result = await target().call(channel, data);
  if (channel === "host/resize") {
    const width = object(result).width;
    if (typeof width !== "number") throw new Error("宽度回执无效");
    frame.style.width = `${width}px`;
  }
  return result;
}
async function call(channel: string, data: HostJson = null): Promise<void> {
  try { output(await perform(channel, data)); status("iframe 已连接", true); }
  catch (error) { status(error instanceof Error ? error.message : "Host 调用失败"); }
}
async function relay(envelope: HostEnvelope): Promise<void> {
  if (!sessionId || envelope.kind === "cancel") return;
  let result: HostJson;
  try { result = { ok: true, data: await perform(envelope.channel, envelope.data), error: null }; }
  catch { result = { ok: false, data: null, error: `Host 信道处理失败: ${envelope.channel}` }; }
  if (envelope.kind === "notify") return;
  await requestJson(`/v1/host/sessions/${sessionId}/reply`, { method: "POST", body: JSON.stringify({ protocol_version: 2, kind: "reply", id: envelope.id, channel: envelope.channel, data: result }) });
}
async function consumeCommands(identifier: string, controller: AbortController): Promise<void> {
  const headers = new Headers(); if (apiKey) headers.set("Authorization", `Bearer ${apiKey}`);
  const response = await fetch(`/v1/host/sessions/${identifier}/events?side=app`, { headers, signal: controller.signal });
  if (!response.ok || !response.body) throw new Error("Host 控制订阅失败");
  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let buffer = "", cursor = 0;
  try {
    while (true) {
      const next = await reader.read();
      buffer += decoder.decode(next.value ?? new Uint8Array(), { stream: !next.done });
      if (buffer.length > 262144) throw new Error("Host 控制流超过大小边界");
      const blocks = buffer.split("\n\n"); buffer = blocks.pop() ?? "";
      for (const block of blocks) {
        if (block.startsWith(":")) continue;
        const line = block.split("\n").find((value) => value.startsWith("data: "));
        if (!line) continue;
        const value = JSON.parse(line.slice(6)) as StreamFrame;
        if (value.stream_id !== identifier || value.seq !== cursor || value.kind !== "chunk" || !isHostEnvelope(value.data)) throw new Error("Host 控制流关联或序号无效");
        cursor += 1;
        if (value.data.kind !== "call" && value.data.kind !== "notify" && value.data.kind !== "cancel") throw new Error("Host 控制方向无效");
        void relay(value.data).catch(() => undefined);
      }
      if (next.done) break;
    }
  } finally { reader.releaseLock(); }
}
async function connect(): Promise<void> {
  try {
    apiKey = input("host-key").value; setApiKey(apiKey);
    const connected = await target().call("host/set-key", { key: apiKey });
    if (object(connected).connected !== true) throw new Error("iframe 未连接业务服务");
    if (!sessionId) {
      const created = await requestJson<{ id: string }>("/v1/host/sessions", { method: "POST" });
      sessionId = created.id; element("host-session").textContent = created.id;
      pump = new AbortController();
      void consumeCommands(created.id, pump).catch((error: unknown) => { if (!pump?.signal.aborted) status(error instanceof Error ? error.message : "Host 控制断开"); });
    }
    await requestJson(`/v1/host/sessions/${sessionId}/ready`, { method: "POST" });
    input("host-key").value = ""; output(connected); status("iframe 已连接", true);
  } catch (error) { status(error instanceof Error ? error.message : "连接失败"); }
}
function bind(): void {
  if (!frame.contentWindow || !frame.isConnected) return;
  client?.dispose(); client = new HostClient(frame.contentWindow, location.origin);
  client.on("host/state", (data) => {
    if (sessionId) void requestJson(`/v1/host/sessions/${sessionId}/notify`, { method: "POST", body: JSON.stringify({ protocol_version: 2, kind: "notify", id: crypto.randomUUID(), channel: "host/state", data }) }).catch(() => undefined);
  });
  client.on("link/navigate", (data) => {
    if (!sessionId) { output(data); status("收到链接通知", true); return; }
    void requestJson(`/v1/host/sessions/${sessionId}/notify`, { method: "POST", body: JSON.stringify({ protocol_version: 2, kind: "notify", id: crypto.randomUUID(), channel: "link/navigate", data }) }).then(() => { output(data); status("收到链接通知", true); }).catch(() => status("链接通知被服务拒绝"));
  });
  void client.call("host/get-state").then(() => status("iframe 已就绪", true)).catch(() => status("等待 iframe 就绪"));
}
frame.addEventListener("load", bind);
if (frame.contentDocument?.readyState === "complete") bind();
element("host-connect").addEventListener("click", () => void connect());
element("host-state").addEventListener("click", () => void call("host/get-state"));
element("host-dark").addEventListener("click", () => void call("host/theme", { theme: "dark" }));
element("host-locale-apply").addEventListener("click", () => void call("host/set-locale", { locale: (element("host-locale") as HTMLSelectElement).value }));
element("host-resize").addEventListener("click", () => void call("host/resize", { width: 960 }));
element("host-replace").addEventListener("click", () => void call("host/prompt", { value: input("host-prompt").value, mode: "replace" }));
element("host-append").addEventListener("click", () => void call("host/prompt", { value: input("host-prompt").value, mode: "append" }));
element("host-toggle").addEventListener("click", () => void call("host/sidebar", { open: !open }));
element("host-destroy").addEventListener("click", () => {
  client?.dispose(); client = null; pump?.abort(); frame.remove();
  if (sessionId) void requestJson(`/v1/host/sessions/${sessionId}`, { method: "DELETE" }).catch(() => undefined);
  status("iframe 已销毁");
});
