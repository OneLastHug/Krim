import { expect, test, beforeEach, afterEach, vi } from "vitest";
import { RequestError } from "../src/api";
import { readTurnStream, StreamProtocolError, StreamUnavailableError, type RunStreamEvent } from "../src/lib/session-stream";

const encoder = new TextEncoder();
const frame = (seq: number, kind = "text", runId = "run-1", text = "片段" + seq) =>
  "id: " + seq + "\nevent: " + kind + "\ndata: " + JSON.stringify({ run_id: runId, seq, kind, data: { text } }) + "\n\n";
const terminalFrame = (seq: number, runId = "run-1") => frame(seq, "terminal", runId, "done");

function sseOK(body: ReadableStream<Uint8Array>, contentType = "text/event-stream") {
  return new Response(body, { status: 200, headers: { "Content-Type": contentType } });
}

function streamOf(chunks: (string | Uint8Array)[], options: { hold?: boolean; onCancel?: () => void } = {}) {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(typeof chunk === "string" ? encoder.encode(chunk) : chunk);
      if (!options.hold) controller.close();
    },
    cancel() { options.onCancel?.(); },
  });
}

// abort 时以 AbortError 打断悬挂 read，模拟真实 fetch 的信号取消；cancel 钩子用于核对清理。
function holdingStream(signal: AbortSignal, chunks: string[], onCancel?: () => void) {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
      signal.addEventListener("abort", () => controller.error(new DOMException("The operation was aborted.", "AbortError")), { once: true });
    },
    cancel() { onCancel?.(); },
  });
}

let respond: (path: string) => Response = () => { throw new Error("未配置响应"); };
const calls: { path: string; init: RequestInit }[] = [];

beforeEach(() => {
  calls.length = 0;
  vi.stubGlobal("fetch", (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({ path: String(input), init: init ?? {} });
    return respond(String(input));
  });
});
afterEach(() => vi.unstubAllGlobals());

const read = (path = "/v2/w/s/t/stream?from_seq=0", signal = new AbortController().signal, onEvent?: (event: RunStreamEvent) => void) =>
  readTurnStream(path, "key-1", "run-1", 0, signal, onEvent ?? (() => {}));

test("frames split across chunks and across multi-byte UTF-8 arrive as complete events", async () => {
  const bytes = encoder.encode(frame(0, "text", "run-1", "你好，流式世界"));
  const head = encoder.encode('id: 0\nevent: text\ndata: {"run_id":"run-1","seq":0,"kind":"text","data":{"text":"').length + 1;
  const seen: RunStreamEvent[] = [];
  respond = () => sseOK(streamOf([bytes.slice(0, head), bytes.slice(head, head + 5), bytes.slice(head + 5)]));
  const result = await read("/v2/w/s/t/stream?from_seq=0", new AbortController().signal, event => seen.push(event));
  expect(seen).toHaveLength(1);
  expect((seen[0]!.data as { text: string }).text).toBe("你好，流式世界");
  expect(result).toEqual({ cursor: 1, terminal: false });
});

test("CRLF and lone CR terminate lines and a CRLF split across chunks stays one line", async () => {
  const crlf = "id: 0\r\nevent: text\r\ndata: " + JSON.stringify({ run_id: "run-1", seq: 0, kind: "text", data: { text: "a" } }) + "\r\n\r\n";
  const cr = "id: 1\revent: text\rdata: " + JSON.stringify({ run_id: "run-1", seq: 1, kind: "text", data: { text: "b" } }) + "\r\r";
  const mixed = 'id: 2\nevent: text\ndata: {"run_id":"run-1","seq":2,"kind":"text","data":{"text":"c"}}\r';
  const tail = '\n\nid: 3\nevent: text\ndata: {"run_id":"run-1","seq":3,"kind":"text","data":{"text":"d"}}\r\n\r\n';
  const seen: RunStreamEvent[] = [];
  respond = () => sseOK(streamOf([crlf + cr, mixed, tail]));
  const result = await read("/v2/w/s/t/stream?from_seq=0", new AbortController().signal, event => seen.push(event));
  expect(seen.map(event => (event.data as { text: string }).text)).toEqual(["a", "b", "c", "d"]);
  expect(result).toEqual({ cursor: 4, terminal: false });
});

test("multi-line data joins with newlines, comments and heartbeats produce no events", async () => {
  const split = 'id: 0\nevent: text\ndata: {"run_id":"run-1","seq":0,"kind":"text",\n: 校验注释\ndata: "data":{"text":"ok"}}\n\n';
  const seen: RunStreamEvent[] = [];
  respond = () => sseOK(streamOf([": heartbeat\n\n", split, ": heartbeat\n\n: heartbeat\n\n"]));
  const result = await read("/v2/w/s/t/stream?from_seq=0", new AbortController().signal, event => seen.push(event));
  expect(seen).toHaveLength(1);
  expect((seen[0]!.data as { text: string }).text).toBe("ok");
  expect(result).toEqual({ cursor: 1, terminal: false });
});

test("request carries Bearer key, Accept header, redirect error and the given signal", async () => {
  respond = () => sseOK(streamOf([frame(0), terminalFrame(1)]));
  const controller = new AbortController();
  await read("/v2/w/s/t/stream?from_seq=0", controller.signal);
  expect(calls).toHaveLength(1);
  const headers = calls[0]!.init.headers as Record<string, string>;
  expect(calls[0]!.path).toBe("/v2/w/s/t/stream?from_seq=0");
  expect(headers.Authorization).toBe("Bearer key-1");
  expect(headers.Accept).toBe("text/event-stream");
  expect(calls[0]!.init.redirect).toBe("error");
  expect(calls[0]!.init.signal).toBe(controller.signal);
});

test.each([
  ["序号跳号", [frame(0), frame(2)]],
  ["序号重复", [frame(0), frame(0)]],
  ["run_id不符", [frame(0, "text", "other-run")]],
  ["event与kind不一致", ["id: 0\nevent: text\ndata: " + JSON.stringify({ run_id: "run-1", seq: 0, kind: "tool_start" }) + "\n\n"]],
  ["id与seq不一致", ["id: 3\nevent: text\ndata: " + JSON.stringify({ run_id: "run-1", seq: 0, kind: "text" }) + "\n\n"]],
  ["缺少id", ["event: text\ndata: " + JSON.stringify({ run_id: "run-1", seq: 0, kind: "text" }) + "\n\n"]],
  ["data不是JSON", ["id: 0\nevent: text\ndata: not-json\n\n"]],
  ["kind不是字符串", ["id: 0\nevent: text\ndata: " + JSON.stringify({ run_id: "run-1", seq: 0, kind: 5 }) + "\n\n"]],
])("protocol violations fail loudly: %s", async (_name, chunks) => {
  const seen: RunStreamEvent[] = [];
  respond = () => sseOK(streamOf(chunks as string[]));
  await expect(read("/v2/w/s/t/stream?from_seq=0", new AbortController().signal, event => seen.push(event))).rejects.toThrow(StreamProtocolError);
  expect(seen.length).toBeLessThanOrEqual(1);
});

test("unsupported endpoints allow polling fallback while auth and server errors keep their status", async () => {
  const json = (status: number, code: string) => new Response(JSON.stringify({ error: { code } }), { status, headers: { "Content-Type": "application/json" } });
  respond = () => json(404, "not_found");
  await expect(read()).rejects.toThrow(StreamUnavailableError);
  respond = () => json(405, "method_not_allowed");
  await expect(read()).rejects.toThrow(StreamUnavailableError);
  respond = () => json(501, "not_implemented");
  await expect(read()).rejects.toThrow(StreamUnavailableError);
  respond = () => json(401, "unauthorized");
  let error: unknown = await read().catch(value => value);
  expect(error).toBeInstanceOf(RequestError);
  expect(error).not.toBeInstanceOf(StreamUnavailableError);
  expect((error as RequestError).status).toBe(401);
  expect((error as RequestError).code).toBe("unauthorized");
  respond = () => json(403, "permission_denied");
  error = await read().catch(value => value);
  expect((error as RequestError).status).toBe(403);
  expect((error as RequestError).code).toBe("permission_denied");
  respond = () => json(500, "storage_unavailable");
  error = await read().catch(value => value);
  expect((error as RequestError).status).toBe(500);
  expect((error as RequestError).code).toBe("storage_unavailable");
  respond = () => sseOK(streamOf([frame(0)]), "application/json");
  await expect(read()).rejects.toThrow(StreamUnavailableError);
});

test("gateway HTML errors keep status as execution_error and non-SSE bodies are cancelled", async () => {
  let cancelled = 0;
  respond = () => new Response("<html>502 Bad Gateway</html>", { status: 502, headers: { "Content-Type": "text/html" } });
  let error: unknown = await read().catch(value => value);
  expect(error).toBeInstanceOf(RequestError);
  expect(error).not.toBeInstanceOf(StreamUnavailableError);
  expect((error as RequestError).status).toBe(502);
  expect((error as RequestError).code).toBe("execution_error");
  respond = () => new Response(null, { status: 401 });
  error = await read().catch(value => value);
  expect((error as RequestError).status).toBe(401);
  expect((error as RequestError).code).toBe("execution_error");
  const body = streamOf(["{}"], { hold: true, onCancel: () => { cancelled++; } });
  respond = () => sseOK(body, "application/json");
  await expect(read()).rejects.toThrow(StreamUnavailableError);
  expect(cancelled).toBe(1);
});

test("SSE error frames map to RequestError without retry", async () => {
  const errorFrame = (code: string) => "event: error\ndata: " + JSON.stringify({ code }) + "\n\n";
  const seen: RunStreamEvent[] = [];
  respond = () => sseOK(streamOf([frame(0), errorFrame("unauthorized")]));
  let error: unknown = await read("/v2/w/s/t/stream?from_seq=0", new AbortController().signal, event => seen.push(event)).catch(value => value);
  expect(seen).toHaveLength(1);
  expect(error).toBeInstanceOf(RequestError);
  expect((error as RequestError).status).toBe(401);
  respond = () => sseOK(streamOf([errorFrame("schema_upgrade_required")]));
  error = await read().catch(value => value);
  expect((error as RequestError).status).toBe(503);
  expect((error as RequestError).code).toBe("schema_upgrade_required");
  respond = () => sseOK(streamOf([errorFrame("Bad Code!")]));
  error = await read().catch(value => value);
  expect((error as RequestError).status).toBe(503);
  expect((error as RequestError).code).toBe("execution_error");
  respond = () => sseOK(streamOf([errorFrame("permission_denied")]));
  error = await read().catch(value => value);
  expect((error as RequestError).status).toBe(403);
});

test("EOF discards an incomplete trailing frame and reports the confirmed cursor", async () => {
  const seen: RunStreamEvent[] = [];
  respond = () => sseOK(streamOf([frame(0) + frame(1), 'id: 2\nevent: text\ndata: {"run_id":"run-1","seq":2,"ki']));
  const result = await read("/v2/w/s/t/stream?from_seq=0", new AbortController().signal, event => seen.push(event));
  expect(seen).toHaveLength(2);
  expect(seen.map(event => event.seq)).toEqual([0, 1]);
  expect(result).toEqual({ cursor: 2, terminal: false });
});

test("EOF resolves a trailing lone CR so an already complete frame is not lost", async () => {
  const seen: RunStreamEvent[] = [];
  const body = frame(0) + "id: 1\revent: text\rdata: " + JSON.stringify({ run_id: "run-1", seq: 1, kind: "text", data: { text: "尾帧" } }) + "\r\r";
  respond = () => sseOK(streamOf([body]));
  const result = await read("/v2/w/s/t/stream?from_seq=0", new AbortController().signal, event => seen.push(event));
  expect(seen).toHaveLength(2);
  expect((seen[1]!.data as { text: string }).text).toBe("尾帧");
  expect(result).toEqual({ cursor: 2, terminal: false });
});

test("invalid UTF-8 bytes fail loudly instead of becoming replacement text", async () => {
  const seen: RunStreamEvent[] = [];
  const bytes = Uint8Array.from([...encoder.encode('id: 1\nevent: text\ndata: {"run_id":"run-1","seq":1,"kind":"text","data":{"text":"坏"}'), 0xff, 0x0a, 0x0a]);
  respond = () => sseOK(streamOf([frame(0), bytes]));
  await expect(read("/v2/w/s/t/stream?from_seq=0", new AbortController().signal, event => seen.push(event))).rejects.toThrow(StreamProtocolError);
  expect(seen).toHaveLength(1);
});

test("a multi-byte character truncated at EOF is discarded without losing earlier frames", async () => {
  const seen: RunStreamEvent[] = [];
  const prefix = encoder.encode('data: {"run_id":"run-1"');
  const tail = new Uint8Array([...prefix, 0xe4, 0xbd]);
  respond = () => sseOK(streamOf([frame(0), tail]));
  const result = await read("/v2/w/s/t/stream?from_seq=0", new AbortController().signal, event => seen.push(event));
  expect(seen).toHaveLength(1);
  expect(result).toEqual({ cursor: 1, terminal: false });
});

test("terminal frame returns immediately, cancels the reader and ignores trailing bytes", async () => {
  let cancelled = 0;
  const seen: RunStreamEvent[] = [];
  respond = () => sseOK(streamOf([frame(0) + frame(1) + terminalFrame(2) + "data: 残留", frame(3)], { hold: true, onCancel: () => { cancelled++; } }));
  const result = await read("/v2/w/s/t/stream?from_seq=0", new AbortController().signal, event => seen.push(event));
  expect(seen.map(event => event.kind)).toEqual(["text", "text", "terminal"]);
  expect(result).toEqual({ cursor: 3, terminal: true });
  expect(cancelled).toBe(1);
});

test("abort cancels a hanging read and cleans up the reader", async () => {
  let cancelled = 0;
  const controller = new AbortController();
  const seen: RunStreamEvent[] = [];
  const body = holdingStream(controller.signal, [frame(0)], () => { cancelled++; });
  respond = () => sseOK(body);
  const pending = read("/v2/w/s/t/stream?from_seq=0", controller.signal, event => {
    seen.push(event);
    controller.abort();
  });
  await expect(pending).rejects.toMatchObject({ name: "AbortError" });
  expect(seen).toHaveLength(1);
  expect(seen[0]!.kind).toBe("text");
  // 流被中止出错后源 cancel 钩子按规范不再触发，但锁必须已在 finally 中释放。
  expect(body.locked).toBe(false);
  expect(cancelled).toBe(0);
});

test("abort rejects and cancels even when the custom stream never reacts to the signal", async () => {
  let cancelled = 0;
  const controller = new AbortController();
  const body = new ReadableStream<Uint8Array>({ start() { /* 永不产出，也不监听信号 */ }, cancel() { cancelled++; } });
  respond = () => sseOK(body);
  const pending = read("/v2/w/s/t/stream?from_seq=0", controller.signal);
  const timer = setTimeout(() => controller.abort(), 0);
  const outcome: unknown = await pending.then(() => "resolved", error => error);
  clearTimeout(timer);
  expect((outcome as Error).name).toBe("AbortError");
  expect(cancelled).toBe(1);
  expect(body.locked).toBe(false);
});
