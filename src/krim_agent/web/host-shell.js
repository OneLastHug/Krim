import { HostClient, isHostEnvelope } from "./host.js";
import { requestJson, setApiKey } from "./api.js";
const element = (id) => document.getElementById(id);
const input = (id) => element(id);
const output = (value) => { element("host-output").textContent = JSON.stringify(value, null, 2); };
const frame = element("agent-frame");
const panel = element("host-panel");
let client = null;
let sessionId = null;
let apiKey = "";
let pump = null;
let open = true;
function status(message, ready = false) {
    element("host-status").textContent = message;
    element("host-status").className = ready ? "badge ready" : "badge";
}
function target() {
    if (!client || !frame.contentWindow || !frame.isConnected)
        throw new Error("iframe 尚未就绪或已销毁");
    return client;
}
function object(data) {
    if (data === null || typeof data !== "object" || Array.isArray(data))
        throw new Error("Host 参数需要对象");
    return data;
}
async function perform(channel, data) {
    if (channel === "host/sidebar") {
        const value = object(data).open;
        if (typeof value !== "boolean")
            throw new Error("侧栏状态无效");
        open = value;
        panel.hidden = !open;
        element("host-toggle").textContent = open ? "隐藏侧栏" : "打开侧栏";
        return { open };
    }
    const result = await target().call(channel, data);
    if (channel === "host/resize") {
        const width = object(result).width;
        if (typeof width !== "number")
            throw new Error("宽度回执无效");
        frame.style.width = `${width}px`;
    }
    return result;
}
async function call(channel, data = null) {
    try {
        output(await perform(channel, data));
        status("iframe 已连接", true);
    }
    catch (error) {
        status(error instanceof Error ? error.message : "Host 调用失败");
    }
}
async function relay(envelope) {
    if (!sessionId || envelope.kind === "cancel")
        return;
    let result;
    try {
        result = { ok: true, data: await perform(envelope.channel, envelope.data), error: null };
    }
    catch {
        result = { ok: false, data: null, error: `Host 信道处理失败: ${envelope.channel}` };
    }
    if (envelope.kind === "notify")
        return;
    await requestJson(`/v1/host/sessions/${sessionId}/reply`, { method: "POST", body: JSON.stringify({ protocol_version: 2, kind: "reply", id: envelope.id, channel: envelope.channel, data: result }) });
}
async function consumeCommands(identifier, controller) {
    const headers = new Headers();
    if (apiKey)
        headers.set("Authorization", `Bearer ${apiKey}`);
    const response = await fetch(`/v1/host/sessions/${identifier}/events?side=app`, { headers, signal: controller.signal });
    if (!response.ok || !response.body)
        throw new Error("Host 控制订阅失败");
    const reader = response.body.getReader();
    const decoder = new TextDecoder("utf-8", { fatal: true });
    let buffer = "", cursor = 0;
    try {
        while (true) {
            const next = await reader.read();
            buffer += decoder.decode(next.value ?? new Uint8Array(), { stream: !next.done });
            if (buffer.length > 262144)
                throw new Error("Host 控制流超过大小边界");
            const blocks = buffer.split("\n\n");
            buffer = blocks.pop() ?? "";
            for (const block of blocks) {
                if (block.startsWith(":"))
                    continue;
                const line = block.split("\n").find((value) => value.startsWith("data: "));
                if (!line)
                    continue;
                const value = JSON.parse(line.slice(6));
                if (value.stream_id !== identifier || value.seq !== cursor || value.kind !== "chunk" || !isHostEnvelope(value.data))
                    throw new Error("Host 控制流关联或序号无效");
                cursor += 1;
                if (value.data.kind !== "call" && value.data.kind !== "notify" && value.data.kind !== "cancel")
                    throw new Error("Host 控制方向无效");
                void relay(value.data).catch(() => undefined);
            }
            if (next.done)
                break;
        }
    }
    finally {
        reader.releaseLock();
    }
}
async function connect() {
    try {
        apiKey = input("host-key").value;
        setApiKey(apiKey);
        const connected = await target().call("host/set-key", { key: apiKey });
        if (object(connected).connected !== true)
            throw new Error("iframe 未连接业务服务");
        if (!sessionId) {
            const created = await requestJson("/v1/host/sessions", { method: "POST" });
            sessionId = created.id;
            element("host-session").textContent = created.id;
            pump = new AbortController();
            void consumeCommands(created.id, pump).catch((error) => { if (!pump?.signal.aborted)
                status(error instanceof Error ? error.message : "Host 控制断开"); });
        }
        await requestJson(`/v1/host/sessions/${sessionId}/ready`, { method: "POST" });
        await target().call("host/set-session", { session_id: sessionId });
        input("host-key").value = "";
        output(connected);
        status("iframe 已连接", true);
    }
    catch (error) {
        status(error instanceof Error ? error.message : "连接失败");
    }
}
function bind() {
    if (!frame.contentWindow || !frame.isConnected)
        return;
    client?.dispose();
    client = new HostClient(frame.contentWindow, location.origin);
    client.on("host/state", (data) => {
        if (sessionId)
            void requestJson(`/v1/host/sessions/${sessionId}/notify`, { method: "POST", body: JSON.stringify({ protocol_version: 2, kind: "notify", id: crypto.randomUUID(), channel: "host/state", data }) }).catch(() => undefined);
    });
    client.on("link/navigate", (data) => {
        if (!sessionId) {
            output(data);
            status("收到链接通知", true);
            return;
        }
        void requestJson(`/v1/host/sessions/${sessionId}/notify`, { method: "POST", body: JSON.stringify({ protocol_version: 2, kind: "notify", id: crypto.randomUUID(), channel: "link/navigate", data }) }).then(() => { output(data); status("收到链接通知", true); }).catch(() => status("链接通知被服务拒绝"));
    });
    client.on("changeList/item/click", (data) => {
        if (sessionId)
            void requestJson(`/v1/host/sessions/${sessionId}/notify`, { method: "POST", body: JSON.stringify({ protocol_version: 2, kind: "notify", id: crypto.randomUUID(), channel: "changeList/item/click", data }) }).catch(() => status("变更点击通知被服务拒绝"));
    });
    for (const channel of ["checkpoint/create", "checkpoint/rollback", "checkpoint/canRollback", "changeList/invoke", "host/callback"]) {
        client.onCall(channel, async (data, requestId) => {
            if (!sessionId)
                throw new Error("Python Host session 尚未建立");
            if (channel === "changeList/invoke") {
                if (data === null || typeof data !== "object" || Array.isArray(data) || typeof data.topic_id !== "string")
                    throw new Error("变更执行需要话题 ID");
                const { topic_id: topicId, ...business } = data;
                const response = await requestJson(`/v1/topics/${encodeURIComponent(topicId)}/change-list/invoke`, { method: "POST", body: JSON.stringify({ ...business, host_session_id: sessionId }) });
                return response;
            }
            const response = await requestJson(`/v1/host/sessions/${sessionId}/requests`, { method: "POST", body: JSON.stringify({ envelope: { protocol_version: 2, kind: "call", id: requestId, channel, data }, timeout_seconds: 10 }) });
            if (!response.ok)
                throw new Error(response.error || "宿主执行失败");
            return response.data;
        });
        client.onCancel(channel, (requestId) => {
            if (sessionId)
                void requestJson(`/v1/host/sessions/${sessionId}/cancel/${encodeURIComponent(requestId)}`, { method: "POST" }).catch(() => undefined);
        });
    }
    void client.call("host/get-state").then(() => status("iframe 已就绪", true)).catch(() => status("等待 iframe 就绪"));
}
frame.addEventListener("load", bind);
if (frame.contentDocument?.readyState === "complete")
    bind();
element("host-connect").addEventListener("click", () => void connect());
element("host-state").addEventListener("click", () => void call("host/get-state"));
element("host-dark").addEventListener("click", () => void call("host/theme", { theme: "dark" }));
element("host-locale-apply").addEventListener("click", () => void call("host/set-locale", { locale: element("host-locale").value }));
element("host-resize").addEventListener("click", () => void call("host/resize", { width: 960 }));
element("host-replace").addEventListener("click", () => void call("host/prompt", { value: input("host-prompt").value, mode: "replace" }));
element("host-append").addEventListener("click", () => void call("host/prompt", { value: input("host-prompt").value, mode: "append" }));
element("host-toggle").addEventListener("click", () => void call("host/sidebar", { open: !open }));
element("host-destroy").addEventListener("click", () => {
    client?.dispose();
    client = null;
    pump?.abort();
    frame.remove();
    if (sessionId)
        void requestJson(`/v1/host/sessions/${sessionId}`, { method: "DELETE" }).catch(() => undefined);
    status("iframe 已销毁");
});
