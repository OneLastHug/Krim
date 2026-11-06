import { act, StrictMode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { App } from "../src/App";
(globalThis as typeof globalThis & {
    IS_REACT_ACT_ENVIRONMENT: boolean;
}).IS_REACT_ACT_ENVIRONMENT = true;
let root: Root;
let container: HTMLDivElement;
let calls: {
    path: string;
    method: string;
    body: Record<string, unknown> | undefined;
    key: string;
}[] = [];
const view = (id: string) => ({ id, request_key: "test-" + id, status: "completed", trace_id: "trace-" + id, root_span_id: "span-" + id, created_at: 1, finished_at: 2, code: null, stop_requested: false });
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });
function read(path: string) {
    const id = path.split("/")[3] ?? "A";
    if (path.includes("/by-key/"))
        return json({ ...view("A"), request_key: decodeURIComponent(path.split("/").at(-1)!) });
    if (path.endsWith("/events?from_seq=0"))
        return json([]);
    if (path.endsWith("/approvals"))
        return json([]);
    if (path.endsWith("/result"))
        return json({ run: view(id), text: id, tools: [], events: [], model_steps: 0 });
    if (path.endsWith("/trace"))
        return json({ run_id: id, trace_id: "trace-" + id, complete: true, spans: [], reasons: [] });
    return json(view(id));
}
const identity = { principal_id: "local", generation: 1, legacy: true, workspaces: [{ workspace_id: "local", role: "admin", approve: true }] };
let identityRead = false;
function intercept(handler: (path: string, options: RequestInit) => Promise<Response> | Response) {
    identityRead = false;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => {
        const path = String(input);
        calls.push({ path, method: options.method ?? "GET", body: options.body ? JSON.parse(String(options.body)) : undefined, key: new Headers(options.headers).get("Authorization") ?? "" });
        if (path === "/v2/me") {
            identityRead = true;
            return json(identity);
        }
        return handler(path, options);
    }));
}
async function mount() {
    await act(async () => root.render(<StrictMode><App /></StrictMode>));
    await until(() => expect(identityRead).toBe(true));
    await act(async () => { await new Promise(r => setTimeout(r, 0)); });
}
async function until(check: () => void) { let error: unknown; for (let i = 0; i < 60; i++) {
    try {
        check();
        return;
    }
    catch (e) {
        error = e;
    }
    await act(async () => { await new Promise(r => setTimeout(r, 10)); });
} throw error; }
function button(label: string) { const found = [...document.querySelectorAll("button")].find(b => b.textContent?.trim() === label); expect(found, label).toBeTruthy(); return found!; }
function ariaButton(label: string) { const found = document.querySelector(`[aria-label="${label}"]`) as HTMLButtonElement | null; expect(found, label).toBeTruthy(); return found!; }
function menuItem(label: string) { const found = [...document.querySelectorAll<HTMLButtonElement>("[role='menuitem']")].find(b => b.textContent?.trim() === label); expect(found, label).toBeTruthy(); return found!; }
// 连接入口在侧栏账号菜单：账号菜单 → 「切换账号」→ Dialog；登录先 GET /v2/me 验证再应用。
async function connectKey(key: string) {
    await act(async () => ariaButton("账号菜单").click());
    await until(() => expect(menuItem("切换账号")).toBeTruthy());
    await act(async () => menuItem("切换账号").click());
    await type("api-key", key);
    await act(async () => document.getElementById("connect-save")!.click());
}
async function click(label: string) { await act(async () => button(label).click()); }
async function type(id: string, value: string) { await act(async () => { const input = document.getElementById(id) as HTMLInputElement | HTMLTextAreaElement; expect(input).toBeTruthy(); Object.getOwnPropertyDescriptor(input.tagName === "TEXTAREA" ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype, "value")!.set!.call(input, value); input.dispatchEvent(new Event("input", { bubbles: true })); }); }
beforeEach(() => { localStorage.clear(); history.replaceState(null, "", "/ui/?view=run"); calls = []; container = document.createElement("div"); document.body.append(container); root = createRoot(container); intercept((p) => read(p)); });
afterEach(async () => { await act(async () => root.unmount()); container.remove(); vi.unstubAllGlobals(); });
test("真实App StrictMode及重挂没有自动POST", async () => { await mount(); await act(async () => root.unmount()); root = createRoot(container); await mount(); expect(calls.filter(c => c.method === "POST")).toEqual([]); expect(container.textContent).toContain("暂无任务"); });
test("双击创建固定原键并只有一次POST", async () => {
    let release!: (r: Response) => void;
    const held = new Promise<Response>(r => { release = r; });
    intercept((path, options) => options.method === "POST" ? held : read(path));
    await mount();
    await type("prompt", "/add 2 3");
    await act(async () => { button("创建任务").click(); button("创建任务").click(); });
    expect(calls.filter(c => c.method === "POST")).toHaveLength(1);
    const submitted = calls.find(c => c.method === "POST")!.body!;
    expect(submitted.text).toBe("/add 2 3");
    expect(typeof submitted.request_key).toBe("string");
    expect(JSON.parse(localStorage.getItem("krim-agent-intent")!)).toEqual(submitted);
    await act(async () => release(json({ ...view("A"), request_key: submitted.request_key })));
    await until(() => expect(document.getElementById("result")?.textContent).toBe("A"));
});
test("未知intent在404及重挂后保留原输入原键，只能查找", async () => {
    const intent = { text: "/echo unresolved", request_key: "original-key", target: "demo" };
    localStorage.setItem("krim-agent-intent", JSON.stringify(intent));
    intercept(() => json({ error: { code: "not_found" } }, 404));
    await mount();
    expect((document.getElementById("prompt") as HTMLTextAreaElement).value).toBe(intent.text);
    await click("查找任务");
    expect(calls.every(c => c.method === "GET")).toBe(true);
    expect(JSON.parse(localStorage.getItem("krim-agent-intent")!)).toEqual(intent);
    await act(async () => root.unmount());
    root = createRoot(container);
    await mount();
    expect(localStorage.getItem("krim-agent-intent")).toBe(JSON.stringify(intent));
    expect(calls.filter(c => c.method === "POST")).toEqual([]);
});
test("历史GET、换ID、迟到回程和卸载均不POST且不覆盖当前对象", async () => {
    let release!: (r: Response) => void;
    const held = new Promise<Response>(r => { release = r; });
    intercept((path) => path === "/v1/runs/A" ? held : read(path));
    history.replaceState(null, "", "/ui/?view=run#A");
    await mount();
    await until(() => expect(calls.some(c => c.path === "/v1/runs/A")).toBe(true));
    await act(async () => { location.hash = "B"; window.dispatchEvent(new HashChangeEvent("hashchange")); });
    await until(() => expect(document.getElementById("result")?.textContent).toBe("B"));
    await act(async () => release(json(view("A"))));
    expect(document.getElementById("result")?.textContent).toBe("B");
    expect(calls.filter(c => c.method === "POST")).toEqual([]);
});
test("换密钥中止旧读取，晚回程不覆盖新认证视图，Tab不写入", async () => {
    let release!: (r: Response) => void;
    const held = new Promise<Response>(r => { release = r; });
    intercept((path, options) => path === "/v1/runs/A" && !new Headers(options.headers).get("Authorization") ? held : read(path));
    history.replaceState(null, "", "/ui/?view=run#A");
    await mount();
    await until(() => expect(calls.some(c => c.path === "/v1/runs/A")).toBe(true));
    await connectKey("new-key");
    await until(() => expect(document.getElementById("result")?.textContent).toBe("A"));
    expect(calls.some(c => c.path === "/v1/runs/A" && c.key === "Bearer new-key")).toBe(true);
    await act(async () => release(json({ ...view("A"), status: "running", finished_at: null })));
    expect(document.getElementById("status")?.textContent).toBe("已完成");
    await click("工具");
    await click("Trace");
    expect(calls.filter(c => c.method === "POST")).toEqual([]);
    expect([...Array(localStorage.length)].map((_, i) => localStorage.getItem(localStorage.key(i)!)).join(" ")).not.toContain("new-key");
});
test("审批双击只发一次，未知回程重试保留原键原决定与绑定", async () => {
    let committed = false;
    const approval = { run_id: "A", request_id: "req-A", name: "echo", arguments: { text: "bound" }, call_id: "call-A", arguments_hash: "args-A", tool_hash: "tool-A" };
    intercept((path, options) => {
        if (options.method === "POST") {
            const posted = calls.filter(c => c.method === "POST");
            if (posted.length === 1)
                return Promise.reject(new TypeError("lost approval return"));
            committed = true;
            return json({ request_id: approval.request_id, allow: true, replayed: false });
        }
        if (path === "/v1/runs/A")
            return json({ ...view("A"), status: committed ? "completed" : "running", finished_at: committed ? 2 : null });
        if (path.endsWith("/approvals"))
            return json(committed ? [] : [approval]);
        return read(path);
    });
    history.replaceState(null, "", "/ui/?view=run#A");
    await mount();
    await until(() => expect(document.querySelector("#approval")).not.toBeNull());
    await act(async () => { button("允许").click(); button("允许").click(); });
    expect(calls.filter(c => c.method === "POST")).toHaveLength(1);
    const original = calls.find(c => c.method === "POST")!.body!;
    expect(original).toMatchObject({ allow: true, call_id: "call-A", arguments_hash: "args-A", tool_hash: "tool-A" });
    expect(JSON.parse(localStorage.getItem("krim-agent-approval-req-A")!)).toEqual(original);
    await click("拒绝");
    expect(calls.filter(c => c.method === "POST")).toHaveLength(1);
    expect(container.textContent).toContain("审批决定不一致");
    await click("允许");
    await until(() => expect(document.getElementById("status")?.textContent).toBe("已完成"));
    expect(calls.filter(c => c.method === "POST")).toHaveLength(2);
    expect(calls.filter(c => c.method === "POST")[1]!.body).toEqual(original);
});
test("取消双击只请求一次，并展示实际读回的终态", async () => {
    let stopped = false;
    intercept((path, options) => {
        if (options.method === "POST") {
            expect(path).toBe("/v1/runs/A/cancel");
            stopped = true;
            return json({ ...view("A"), status: "aborted" });
        }
        if (path === "/v1/runs/A")
            return json({ ...view("A"), status: stopped ? "aborted" : "running", finished_at: stopped ? 2 : null });
        if (path.endsWith("/result"))
            return json({ run: { ...view("A"), status: "aborted" }, text: "", tools: [], events: [] });
        return read(path);
    });
    history.replaceState(null, "", "/ui/?view=run#A");
    await mount();
    await until(() => expect(document.getElementById("status")?.textContent).toBe("执行中"));
    await act(async () => { button("停止").click(); button("停止").click(); });
    await until(() => expect(document.getElementById("status")?.textContent).toBe("已停止"));
    expect(calls.filter(c => c.method === "POST")).toHaveLength(1);
});
test("卸载期间创建回程不清未知intent，不产生第二次POST", async () => {
    let release!: (r: Response) => void;
    const held = new Promise<Response>(r => { release = r; });
    intercept((path, options) => options.method === "POST" ? held : read(path));
    await mount();
    await type("prompt", "/echo original");
    await click("创建任务");
    const original = localStorage.getItem("krim-agent-intent")!;
    await act(async () => root.unmount());
    const body = JSON.parse(original);
    await act(async () => release(json({ ...view("A"), request_key: body.request_key })));
    expect(localStorage.getItem("krim-agent-intent")).toBe(original);
    root = createRoot(container);
    await mount();
    expect(calls.filter(c => c.method === "POST")).toHaveLength(1);
    expect((document.getElementById("prompt") as HTMLTextAreaElement).value).toBe("/echo original");
});


test("创建等待中换密钥后未知回程仍锁定原输入并显示查找",async()=>{
 let reject!:(e:Error)=>void;const held=new Promise<Response>((_resolve,r)=>{reject=r;});
 intercept((_path,options)=>options.method==="POST"?held:json({error:{code:"not_found"}},404));
 await mount();await type("prompt","/echo original");await click("创建任务");const original=localStorage.getItem("krim-agent-intent");
 await connectKey("changed-key");
 await act(async()=>reject(new TypeError("lost return")));
 await until(()=>expect([...container.querySelectorAll("button")].some(b=>b.textContent?.trim()==="查找任务")).toBe(true));
 expect(localStorage.getItem("krim-agent-intent")).toBe(original);
 expect((document.getElementById("prompt") as HTMLTextAreaElement).value).toBe("/echo original");
 expect((document.getElementById("prompt") as HTMLTextAreaElement).readOnly).toBe(true);
 expect(container.textContent).toContain("创建结果未确认");
 await click("查找任务");expect(calls.filter(c=>c.method==="POST")).toHaveLength(1);expect(localStorage.getItem("krim-agent-intent")).toBe(original);
});


test("修正读取密钥后只读成功，不保留旧认证错误",async()=>{
 intercept((path,options)=>!new Headers(options.headers).get("Authorization")?json({error:{code:"unauthorized"}},401):read(path));
 history.replaceState(null,"","/ui/?view=run#A");await mount();await until(()=>expect(container.textContent).toContain("密钥无效"));
 await connectKey("correct-key");
 await until(()=>expect(document.getElementById("result")?.textContent).toBe("A"));
 expect(container.textContent).not.toContain("密钥无效");expect(calls.filter(c=>c.method==="POST")).toEqual([]);
});
