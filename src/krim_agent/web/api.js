let apiKey = "";
export function setApiKey(key) { apiKey = key; }
export function errorText(error) {
    return error instanceof Error ? error.message : "操作失败，请检查连接与运行状态";
}
function headers() {
    const value = new Headers({ "Content-Type": "application/json" });
    if (apiKey)
        value.set("Authorization", `Bearer ${apiKey}`);
    return value;
}
export class ApiError extends Error {
    status;
    code;
    constructor(message, status, code) {
        super(message);
        this.status = status;
        this.code = code;
        this.name = "ApiError";
    }
}
async function readJson(path, init) {
    const merged = headers();
    new Headers(init.headers).forEach((value, key) => merged.set(key, value));
    const response = await fetch(path, { ...init, headers: merged });
    let value;
    try {
        value = await response.json();
    }
    catch {
        throw new ApiError(`HTTP ${response.status} · 服务没有返回有效的数据，请检查连接`, response.status, null);
    }
    if (!response.ok) {
        const body = value;
        throw new ApiError(typeof body?.error?.message === "string" ? body.error.message : `HTTP ${response.status}`, response.status, typeof body?.error?.code === "string" ? body.error.code : null);
    }
    return value;
}
export async function requestJson(path, init = {}) {
    const value = await readJson(path, init);
    if (value === null)
        throw new Error("服务没有返回有效的数据，请检查连接");
    return value;
}
/** 只有明确允许 null 的查询使用此入口；空正文或无效 JSON 仍拒绝。 */
export async function requestJsonNullable(path, init = {}) {
    return await readJson(path, init);
}
export async function consumeEvents(runId, fromSeq, onFrame, signal) {
    const response = await fetch(`/v1/runs/${encodeURIComponent(runId)}/events?from_seq=${fromSeq}`, {
        headers: headers(), signal,
    });
    if (!response.ok) {
        const body = await response.json();
        throw new Error(body.error?.message || `HTTP ${response.status}`);
    }
    if (!response.body)
        throw new Error("浏览器未提供流式响应");
    const reader = response.body.getReader();
    const decoder = new TextDecoder("utf-8", { fatal: true });
    let buffer = "";
    let cursor = fromSeq;
    let terminal = false;
    try {
        while (true) {
            const { value, done } = await reader.read();
            buffer += decoder.decode(value ?? new Uint8Array(), { stream: !done });
            if (buffer.length > 262144)
                throw new Error("流帧超过大小限制");
            const blocks = buffer.split("\n\n");
            buffer = blocks.pop() ?? "";
            for (const block of blocks) {
                if (block.startsWith(":"))
                    continue;
                let id = "", kind = "";
                const data = [];
                for (const line of block.split("\n")) {
                    if (line.startsWith("data:"))
                        data.push(line.slice(5).trimStart());
                    if (line.startsWith("id:"))
                        id = line.slice(3).trim();
                    if (line.startsWith("event:"))
                        kind = line.slice(6).trim();
                }
                if (!data.length)
                    continue;
                const raw = data.join("\n");
                if (new TextEncoder().encode(raw).length > 131072)
                    throw new Error("流事件超过大小限制");
                const frame = JSON.parse(raw);
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
            if (done)
                break;
        }
    }
    finally {
        reader.releaseLock();
    }
    if (!terminal)
        throw new Error("连接已断开，可从已保存的游标继续读取");
    return cursor;
}
export async function consumeWorkflowEvents(runId, fromSeq, onEvent) {
    const path = "/v1/workflows/runs/" + encodeURIComponent(runId);
    const response = await fetch(path + "/events?from_seq=" + fromSeq, { headers: headers() });
    if (!response.ok)
        throw new Error("Workflow 请求失败，请检查权限、状态或游标");
    if (!response.body)
        throw new Error("浏览器未提供 Workflow 流");
    const reader = response.body.getReader();
    const decoder = new TextDecoder("utf-8", { fatal: true });
    let buffer = "", cursor = fromSeq, epoch = -1;
    try {
        while (true) {
            const { value, done } = await reader.read();
            buffer += decoder.decode(value ?? new Uint8Array(), { stream: !done });
            const blocks = buffer.split("\n\n");
            buffer = blocks.pop() ?? "";
            if (buffer.length > 262144)
                throw new Error("Workflow 流帧超过大小限制");
            for (const block of blocks) {
                let id = "", kind = "";
                const lines = [];
                for (const line of block.split("\n")) {
                    if (line.startsWith("data:"))
                        lines.push(line.slice(5).trimStart());
                    if (line.startsWith("id:"))
                        id = line.slice(3).trim();
                    if (line.startsWith("event:"))
                        kind = line.slice(6).trim();
                }
                if (!lines.length)
                    continue;
                const data = lines.join("\n");
                if (new TextEncoder().encode(data).length > 131072)
                    throw new Error("Workflow 流事件超过大小限制");
                const event = JSON.parse(data);
                if (event.run_id !== runId || event.seq !== cursor || id !== String(cursor) || kind !== event.type
                    || !Number.isSafeInteger(event.epoch) || event.epoch < epoch) {
                    throw new Error("Workflow 事件关联、代次或游标不正确");
                }
                epoch = event.epoch;
                cursor += 1;
                onEvent(event);
                if (event.type === "end") {
                    await reader.cancel();
                    return cursor;
                }
            }
            if (done)
                break;
        }
    }
    finally {
        reader.releaseLock();
    }
    const current = await requestJson(path);
    if (["queued", "running"].includes(current.status) || current.event_count !== cursor) {
        throw new Error("Workflow 流连接已断开，可从游标继续");
    }
    return cursor;
}
export async function consumeMultiplex(sources, onFrame, signal) {
    const response = await fetch("/v1/runs/events", { method: "POST", headers: headers(), body: JSON.stringify({ sources }), signal });
    if (!response.ok)
        throw new Error(`HTTP ${response.status}`);
    if (!response.body)
        throw new Error("浏览器未提供流式响应");
    const expected = new Map(sources.map((source) => [source.run_id, source.next_seq]));
    const topics = new Map(sources.map((source) => [source.run_id, source.topic_id]));
    const reader = response.body.getReader();
    const decoder = new TextDecoder("utf-8", { fatal: true });
    let buffer = "";
    try {
        while (true) {
            const { value, done } = await reader.read();
            buffer += decoder.decode(value ?? new Uint8Array(), { stream: !done });
            const blocks = buffer.split("\n\n");
            buffer = blocks.pop() ?? "";
            if (buffer.length > 524288)
                throw new Error("多路流帧超过大小限制");
            for (const block of blocks) {
                if (block.startsWith(":"))
                    continue;
                let id = "", kind = "";
                const lines = [];
                for (const line of block.split("\n")) {
                    if (line.startsWith("data:"))
                        lines.push(line.slice(5).trimStart());
                    if (line.startsWith("id:"))
                        id = line.slice(3).trim();
                    if (line.startsWith("event:"))
                        kind = line.slice(6).trim();
                }
                if (!lines.length)
                    continue;
                const raw = lines.join("\n");
                if (new TextEncoder().encode(raw).length > 262144)
                    throw new Error("多路流事件超过大小限制");
                const item = JSON.parse(raw);
                const frame = item.frame;
                const next = expected.get(frame.stream_id);
                if (item.type !== "run-frame" || kind !== "run-frame" || item.topic_id !== topics.get(frame.stream_id)
                    || next === undefined || frame.seq !== next || id !== `${frame.stream_id}:${frame.seq}`)
                    throw new Error("多路流来源或序号不正确");
                expected.set(frame.stream_id, next + 1);
                onFrame(item);
                if (frame.kind === "end" || frame.kind === "error")
                    expected.delete(frame.stream_id);
            }
            if (!expected.size) {
                await reader.cancel();
                return;
            }
            if (done)
                break;
        }
    }
    finally {
        reader.releaseLock();
    }
    if (expected.size)
        throw new Error("连接已断开，可从已保存的游标继续读取");
}
