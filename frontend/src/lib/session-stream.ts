import { object, RequestError } from "../api.ts";

export type RunStreamEvent = Record<string, unknown> & { run_id: string; seq: number; kind: string };

export class StreamUnavailableError extends Error {
  constructor(message?: string) { super(message ?? "流式接口不可用"); this.name = "StreamUnavailableError"; }
}
export class StreamProtocolError extends Error {
  constructor(message?: string) { super(message ?? "流式事件格式无效"); this.name = "StreamProtocolError"; }
}

// 终止判据与服务端约定一致：kind 为 "terminal" 的完整帧，回调后立即结束，不等待连接关闭。
// EOF 不代表运行终态，只说明本次连接结束；EOF 残留的不完整尾帧被丢弃（末尾孤立 CR 按
// 行结束处理，避免丢失已完整帧），调用方按返回的 cursor 续读。帧 id 与 JSON seq、SSE
// event 与 JSON kind、run_id 与预期运行不一致，序号跳号或重复，UTF-8 无效字节，都直接
// 抛 StreamProtocolError，不静默吞掉。
export async function readTurnStream(path: string, apiKey: string, runId: string, fromSeq: number, signal: AbortSignal, onEvent: (event: RunStreamEvent) => void): Promise<{ cursor: number; terminal: boolean }> {
  if (!Number.isSafeInteger(fromSeq) || fromSeq < 0) throw new StreamProtocolError("起始序号无效");
  const response = await fetch(path, { headers: { Accept: "text/event-stream", Authorization: "Bearer " + apiKey }, redirect: "error", signal });
  // 不可用分支不读body，主动放弃以免遗留连接；cancel 不等待可能永不完成的自定义流。
  const dropBody = () => {
    if (!response.body) return;
    try { void response.body.cancel().catch(() => { /* 已关闭 */ }); } catch { /* 已释放 */ }
  };
  if (!response.ok) {
    if (response.status === 404 || response.status === 405 || response.status === 501) {
      dropBody();
      throw new StreamUnavailableError("流式接口不可用（" + response.status + "）");
    }
    // 网关HTML或空body等非JSON错误：保持原HTTP状态并按 execution_error，让5xx可重试、401/403仍停止。
    let code = "execution_error";
    try {
      const err = object(object(await response.json()).error);
      if (/^[a-z_]{1,64}$/.test(String(err.code))) code = String(err.code);
    } catch { /* 非JSON错误体 */ }
    throw new RequestError(response.status, code);
  }
  if ((response.headers.get("Content-Type") ?? "").split(";")[0]!.trim().toLowerCase() !== "text/event-stream") {
    dropBody();
    throw new StreamUnavailableError("流式响应内容类型无效");
  }
  if (!response.body) throw new StreamUnavailableError("流式响应缺少内容");
  const reader = response.body.getReader();
  // fatal 解码避免无效字节被替换字符伪装成正文；流式解码让跨块多字节保持完整。
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let buffer = "", dataLines: string[] = [], eventName = "", eventId: string | null = null, next = fromSeq, stopped = false;
  // fetch 的信号联动不覆盖自定义流或被桩替换的响应，这里自挂监听：取消悬挂 read，尽快以
  // AbortError 结束；finally 里移除监听并 cancel、releaseLock。
  let rejectInterruption: ((reason: unknown) => void) | null = null;
  const interruption = new Promise<never>((_, reject) => { rejectInterruption = reject; });
  const onAbort = () => {
    signal.removeEventListener("abort", onAbort);
    // 先拒绝 race 再 cancel：cancel 会把悬挂 read 兑现为 done，顺序颠倒会变成正常返回。
    rejectInterruption?.(signal.reason instanceof Error ? signal.reason : new DOMException("读取已中止", "AbortError"));
    try { void reader.cancel().catch(() => { /* 已出错或已释放 */ }); } catch { /* 已释放 */ }
  };
  signal.addEventListener("abort", onAbort);
  if (signal.aborted) onAbort();

  function invalid(message: string): never { throw new StreamProtocolError(message); }

  function failure(payload: string): RequestError {
    let parsed: unknown;
    try { parsed = JSON.parse(payload); } catch { return invalid("错误帧数据不是有效JSON"); }
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return invalid("错误帧结构无效");
    const code = String((parsed as Record<string, unknown>).code ?? "");
    if (code === "unauthorized") return new RequestError(401, "unauthorized");
    if (code === "permission_denied") return new RequestError(403, "permission_denied");
    return new RequestError(503, /^[a-z_]{1,64}$/.test(code) ? code : "execution_error");
  }

  function dispatch(event: string, id: string | null, payload: string): boolean {
    if (event === "error") throw failure(payload);
    let parsed: unknown;
    try { parsed = JSON.parse(payload); } catch { return invalid("事件数据不是有效JSON"); }
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return invalid("事件结构无效");
    const value = parsed as Record<string, unknown>;
    if (typeof value.kind !== "string") return invalid("事件kind无效");
    if (event !== value.kind) return invalid("事件类型与kind不一致");
    if (typeof value.run_id !== "string" || value.run_id !== runId) return invalid("事件身份不匹配");
    if (id === null || !/^\d+$/.test(id)) return invalid("事件id无效");
    const seq = Number(id);
    if (!Number.isSafeInteger(seq) || typeof value.seq !== "number" || !Number.isSafeInteger(value.seq) || value.seq !== seq) return invalid("事件序号与id不一致");
    if (value.seq !== next) return invalid("事件序号不连续");
    onEvent(value as RunStreamEvent);
    next = value.seq + 1;
    return value.kind === "terminal";
  }

  // 行结束符接受 LF、CRLF 与单独 CR；缓冲区末尾的孤立 CR 先按 CRLF 前半等待下一块，
  // EOF 时（atEnd）不再可能构成 CRLF，按行结束处理。
  function lineBreak(text: string, atEnd = false): number {
    for (let index = 0; index < text.length; index++) {
      const char = text[index];
      if (char === "\n") return index;
      if (char === "\r") return index + 1 < text.length || atEnd ? index : -1;
    }
    return -1;
  }

  function accept(line: string): boolean {
    if (line === "") return flush();
    if (line[0] === ":") return false; // 注释与心跳不产生事件
    const colon = line.indexOf(":");
    const name = colon < 0 ? line : line.slice(0, colon);
    let value = colon < 0 ? "" : line.slice(colon + 1);
    if (value[0] === " ") value = value.slice(1);
    if (name === "data") dataLines.push(value);
    else if (name === "event") eventName = value;
    else if (name === "id") eventId = value; // retry 与未知字段忽略
    return false;
  }

  function flush(): boolean {
    if (!dataLines.length) { eventName = ""; eventId = null; return false; }
    const event = eventName, id = eventId, payload = dataLines.join("\n");
    dataLines = []; eventName = ""; eventId = null;
    return dispatch(event, id, payload);
  }

  function append(bytes: Uint8Array): void {
    try { buffer += decoder.decode(bytes, { stream: true }); }
    catch { return invalid("事件编码无效"); }
  }

  function consume(end: number): void {
    const width = buffer[end] === "\r" && buffer[end + 1] === "\n" ? 2 : 1;
    const line = buffer.slice(0, end);
    buffer = buffer.slice(end + width);
    if (accept(line)) stopped = true;
  }

  try {
    while (!stopped) {
      const chunk = await Promise.race([reader.read(), interruption]);
      if (chunk.done) break;
      append(chunk.value);
      for (let end = lineBreak(buffer); end >= 0 && !stopped; end = lineBreak(buffer)) consume(end);
    }
    if (!stopped) {
      try { buffer += decoder.decode(); } catch { /* 末尾残留字节无效，随不完整尾帧丢弃 */ }
      for (let end = lineBreak(buffer, true); end >= 0 && !stopped; end = lineBreak(buffer, true)) consume(end);
    }
  } finally {
    signal.removeEventListener("abort", onAbort);
    try { await reader.cancel(); } catch { /* 连接已关闭或已出错 */ }
    try { reader.releaseLock(); } catch { /* 锁已释放 */ }
  }
  if (stopped) return { cursor: next, terminal: true };
  return { cursor: next, terminal: false };
}
