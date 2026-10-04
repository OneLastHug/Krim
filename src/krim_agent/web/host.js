export const HOST_PROTOCOL_VERSION = 2;
const isRecord = (value) => typeof value === "object" && value !== null && !Array.isArray(value);
export function isHostJson(value, depth = 0) {
    if (depth > 32)
        return false;
    if (value === null || typeof value === "string" || typeof value === "boolean")
        return true;
    if (typeof value === "number")
        return Number.isFinite(value) && Math.abs(value) <= Number.MAX_SAFE_INTEGER;
    if (Array.isArray(value))
        return value.length <= 2048 && value.every((item) => isHostJson(item, depth + 1));
    if (!isRecord(value))
        return false;
    return Object.keys(value).length <= 256 && Object.values(value).every((item) => isHostJson(item, depth + 1));
}
export function isHostEnvelope(value) {
    if (!isRecord(value) || value.protocol_version !== HOST_PROTOCOL_VERSION)
        return false;
    if (!Object.keys(value).every((key) => ["protocol_version", "kind", "id", "channel", "data"].includes(key)))
        return false;
    if (!["call", "reply", "notify", "cancel"].includes(String(value.kind)))
        return false;
    if (typeof value.id !== "string" || value.id.length < 1 || value.id.length > 128)
        return false;
    if (typeof value.channel !== "string" || value.channel.length < 1 || value.channel.length > 128)
        return false;
    if (!isHostJson(value.data))
        return false;
    try {
        return new TextEncoder().encode(JSON.stringify(value)).length <= 65536;
    }
    catch {
        return false;
    }
}
function replyValue(value) {
    if (!isRecord(value) || typeof value.ok !== "boolean")
        return null;
    if (!Object.keys(value).every((key) => ["ok", "data", "error"].includes(key)))
        return null;
    const data = value.data ?? null, error = value.error ?? null;
    if (!isHostJson(data) || (error !== null && typeof error !== "string"))
        return null;
    if (value.ok && error !== null)
        return null;
    if (!value.ok && (typeof error !== "string" || !error || data !== null))
        return null;
    return { ok: value.ok, data, error };
}
function requestId() { return crypto.randomUUID(); }
export class HostClient {
    target;
    targetOrigin;
    timeoutMs;
    source;
    pending = new Map();
    handlers = new Map();
    onMessageBound;
    disposed = false;
    constructor(target, targetOrigin, timeoutMs = 10_000, source = window) {
        this.target = target;
        this.targetOrigin = targetOrigin;
        this.timeoutMs = timeoutMs;
        this.source = source;
        if (targetOrigin !== source.location.origin || timeoutMs <= 0 || timeoutMs > 300_000)
            throw new Error("Host UI 只接受明确同源目标和有效超时");
        this.onMessageBound = (event) => {
            const envelope = event.data;
            if (event.source !== this.target || event.origin !== this.targetOrigin || !isHostEnvelope(envelope))
                return;
            if (envelope.kind === "notify") {
                this.handlers.get(envelope.channel)?.(envelope.data);
                return;
            }
            if (envelope.kind !== "reply")
                return;
            const item = this.pending.get(envelope.id), result = replyValue(envelope.data);
            if (!item || item.channel !== envelope.channel || !result)
                return;
            this.pending.delete(envelope.id);
            this.source.clearTimeout(item.timer);
            if (!result.ok)
                item.reject(new Error(result.error || "Host 请求失败"));
            else
                item.resolve(result.data);
        };
        source.addEventListener("message", this.onMessageBound);
    }
    on(channel, handler) {
        if (this.handlers.has(channel))
            throw new Error("Host 通知已注册");
        this.handlers.set(channel, handler);
        return () => { if (this.handlers.get(channel) === handler)
            this.handlers.delete(channel); };
    }
    async call(channel, data = null) {
        if (this.disposed)
            throw new Error("HostClient 已销毁");
        if (this.pending.size >= 128)
            throw new Error("Host 待处理请求超过上限");
        const envelope = { protocol_version: 2, kind: "call", id: requestId(), channel, data };
        if (!isHostEnvelope(envelope))
            throw new Error("Host 请求超过协议边界");
        return await new Promise((resolve, reject) => {
            const timer = this.source.setTimeout(() => {
                this.pending.delete(envelope.id);
                this.target.postMessage({ ...envelope, kind: "cancel", data: null }, this.targetOrigin);
                reject(new Error(`Host 请求超时: ${channel}`));
            }, this.timeoutMs);
            this.pending.set(envelope.id, { channel, resolve: (value) => resolve(value), reject, timer });
            this.target.postMessage(envelope, this.targetOrigin);
        });
    }
    notify(channel, data = null) {
        if (this.disposed)
            throw new Error("HostClient 已销毁");
        const envelope = { protocol_version: 2, kind: "notify", id: requestId(), channel, data };
        if (!isHostEnvelope(envelope))
            throw new Error("Host 通知超过协议边界");
        this.target.postMessage(envelope, this.targetOrigin);
    }
    dispose() {
        if (this.disposed)
            return;
        this.disposed = true;
        this.source.removeEventListener("message", this.onMessageBound);
        this.handlers.clear();
        for (const item of this.pending.values()) {
            this.source.clearTimeout(item.timer);
            item.reject(new Error("HostClient 已销毁"));
        }
        this.pending.clear();
    }
}
export class HostEndpoint {
    parent;
    parentOrigin;
    source;
    handlers = new Map();
    seen = new Set();
    cancelled = new Set();
    onMessageBound;
    disposed = false;
    constructor(parent = window.parent, parentOrigin = window.location.origin, source = window) {
        this.parent = parent;
        this.parentOrigin = parentOrigin;
        this.source = source;
        if (parentOrigin !== source.location.origin)
            throw new Error("Host iframe 只接受同源宿主");
        this.onMessageBound = (event) => {
            const envelope = event.data;
            if (this.disposed || event.source !== this.parent || event.origin !== this.parentOrigin || !isHostEnvelope(envelope))
                return;
            if (envelope.kind === "cancel") {
                this.cancelled.add(envelope.id);
                return;
            }
            if (envelope.kind !== "call" && envelope.kind !== "notify")
                return;
            if (this.seen.has(envelope.id) || this.seen.size >= 2048) {
                if (envelope.kind === "call")
                    this.reply(envelope, false, null, "Host 请求重复或超过上限");
                return;
            }
            this.seen.add(envelope.id);
            const handler = this.handlers.get(envelope.channel);
            void Promise.resolve().then(() => {
                if (this.cancelled.has(envelope.id) || this.disposed)
                    throw new Error("Host 调用已取消");
                if (!handler)
                    throw new Error("未注册 Host 信道");
                return handler(envelope.data);
            }).then((data) => { if (envelope.kind === "call")
                this.reply(envelope, true, data); })
                .catch(() => { if (envelope.kind === "call")
                this.reply(envelope, false, null, `Host 信道处理失败: ${envelope.channel}`); });
        };
        source.addEventListener("message", this.onMessageBound);
    }
    on(channel, handler) {
        if (this.handlers.has(channel))
            throw new Error(`重复 Host 信道: ${channel}`);
        this.handlers.set(channel, handler);
        return () => { if (this.handlers.get(channel) === handler)
            this.handlers.delete(channel); };
    }
    announceReady(data = { protocol_version: 2 }) { this.notify("host/ready", data); }
    notify(channel, data) {
        if (this.parent === this.source || this.disposed)
            return;
        const envelope = { protocol_version: 2, kind: "notify", id: requestId(), channel, data };
        if (isHostEnvelope(envelope))
            this.parent.postMessage(envelope, this.parentOrigin);
    }
    dispose() { this.disposed = true; this.source.removeEventListener("message", this.onMessageBound); this.handlers.clear(); this.seen.clear(); this.cancelled.clear(); }
    reply(request, ok, data, error = null) {
        if (this.disposed || this.cancelled.has(request.id))
            return;
        const envelope = { protocol_version: 2, kind: "reply", id: request.id, channel: request.channel, data: { ok, data: ok ? data : null, error: ok ? null : error } };
        if (isHostEnvelope(envelope))
            this.parent.postMessage(envelope, this.parentOrigin);
    }
}
