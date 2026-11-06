import { act, StrictMode, useState } from "react";
import { createRoot, type Root } from "react-dom/client";
import { beforeEach, afterEach, expect, test, vi } from "vitest";
import { useAccountIdentity } from "../src/hooks/use-account-identity";
import { RunAccess } from "../src/components/RunAccess";
import { AppSidebar } from "../src/components/AppSidebar";
import type { Actor } from "../src/lib/session-contracts";

const actor: Actor = { principal_id: "alice", generation: 1, legacy: false, workspaces: [{ workspace_id: "workspace-a", role: "admin", approve: true }] };
const profile = (id: string, name: string) => ({ principal_id: id, display_name: name, status: "active", platform_role: "member", generation: 1, revision: 1, legacy: false, key: null, workspaces: [{ workspace_id: "workspace-a", display_name: "个人工作区", role: "admin", approve: true, personal: true }] });
const connection = { profile_id: "connection-demo", display_name: "团队网关", mode: "api_gateway", target_key: "demo", target_fingerprint: "a".repeat(64) };
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });
let host: HTMLDivElement, root: Root;
beforeEach(() => {
  host = document.createElement("div"); document.body.append(host); root = createRoot(host);
  (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); vi.unstubAllGlobals(); });
async function until(check: () => void) { let failure: unknown; for (let i = 0; i < 60; i++) { try { check(); return; } catch (error) { failure = error; } await act(async () => { await new Promise(r => setTimeout(r, 0)); }); } throw failure; }
function Probe({ apiKey, identity = actor, fingerprint = "a".repeat(64) }: { apiKey: string; identity?: Actor; fingerprint?: string }) {
  const data = useAccountIdentity({ apiKey, identity, targetKey: "demo", targetFingerprint: fingerprint, onUnauthorized: () => {} });
  return <output>{data.profile?.display_name ?? ""}|{data.connection?.display_name ?? ""}|{data.connectionError}</output>;
}

test("旧账号资料和接入响应不能覆盖新密钥对应的账号", async () => {
  const pending: (() => void)[] = [];
  vi.stubGlobal("fetch", vi.fn((path: string, options: RequestInit) => {
    const old = (options.headers as Record<string, string>).Authorization === "Bearer old-key";
    const payload = path.includes("connection-profile") ? connection : profile(old ? "alice" : "bob", old ? "旧账号" : "新账号");
    return old ? new Promise(resolve => pending.push(() => resolve(json(payload)))) : Promise.resolve(json(payload));
  }));
  await act(async () => root.render(<StrictMode><Probe apiKey="old-key"/></StrictMode>));
  await act(async () => root.render(<StrictMode><Probe apiKey="new-key" identity={{ ...actor, principal_id: "bob" }}/></StrictMode>));
  await until(() => expect(host.textContent).toContain("新账号|团队网关"));
  await act(async () => { pending.forEach(resolve => resolve()); await Promise.resolve(); });
  expect(host.textContent).toContain("新账号"); expect(host.textContent).not.toContain("旧账号");
});

test("网关指纹不匹配时不展示错误的接入名称", async () => {
  vi.stubGlobal("fetch", vi.fn(async (path: string) => json(path.includes("connection-profile") ? connection : profile("alice", "账号甲"))));
  await act(async () => root.render(<Probe apiKey="key" fingerprint={"b".repeat(64)}/>));
  await until(() => expect(host.textContent).toContain("账号甲"));
  expect(host.textContent).not.toContain("团队网关");
});

test("一次性轮换结果展示期间不以旧密钥刷新身份", async () => {
  const unauthorized = vi.fn();
  let expired = false;
  const fetcher = vi.fn(async (path: string) => expired ? json({ error: { code: "unauthorized" } }, 401) : json(path.includes("connection-profile") ? connection : profile("alice", "账号甲")));
  vi.stubGlobal("fetch", fetcher);
  function Paused({ paused }: { paused: boolean }) {
    const value = useAccountIdentity({ apiKey: "old-key", identity: actor, targetKey: "demo", onUnauthorized: unauthorized, paused });
    return <output>{value.profile?.display_name}</output>;
  }
  await act(async () => root.render(<Paused paused={false}/>));
  await until(() => expect(host.textContent).toBe("账号甲"));
  const before = fetcher.mock.calls.length; expired = true;
  await act(async () => root.render(<Paused paused/>));
  await act(async () => window.dispatchEvent(new Event("focus")));
  expect(fetcher.mock.calls.length).toBe(before);
  expect(unauthorized).not.toHaveBeenCalled();
  expect(host.textContent).toBe("账号甲");
});

test("外部授权版本变化时先重读身份，不把新资料绑定到旧主体快照", async () => {
  const stale = vi.fn();
  vi.stubGlobal("fetch", vi.fn(async (path: string) => json(path.includes("connection-profile") ? connection : { ...profile("alice", "更新后的账号"), generation: 2 })));
  function Changed() {
    const data = useAccountIdentity({ apiKey: "key", identity: actor, targetKey: "demo", onUnauthorized: () => {}, onIdentityStale: stale });
    return <output>{data.profile?.display_name}</output>;
  }
  await act(async () => root.render(<Changed/>));
  await until(() => expect(stale).toHaveBeenCalledOnce());
  expect(host.textContent).not.toContain("更新后的账号");
});

test("账号菜单展示可读名称，顶部查找对话、底部设置且不重复local", async () => {
  const search = vi.fn();
  await act(async () => root.render(<AppSidebar active="chat" identity={actor} identityLoading={false} identityError="" accountName="账号甲" gatewayName="团队网关" accountAdmin={false} chat={null} onNavigate={() => {}} onOpenSearch={search} onConnect={() => {}} onCreate={() => {}} theme="bright" onThemeSelect={() => {}} readingSize={14} onReadingSizeSelect={() => {}}/>));
  expect(host.querySelector(".sidebar-buttons [aria-label='查找对话']")).toBeTruthy();
  expect(host.querySelector(".sidebar-buttons [aria-label='设置']")).toBeNull();
  expect(host.querySelector(".sidebar-account [aria-label='设置']")).toBeTruthy();
  expect(host.querySelector(".account-identity-line")?.textContent).toBe("账号甲·团队网关");
  expect(host.textContent).not.toContain("workspace-a");
  await act(async () => (host.querySelector("[aria-label='查找对话']") as HTMLButtonElement).click());
  expect(search).toHaveBeenCalledOnce();
});

test("登录失败不切换当前密钥，关闭后丢弃输入值", async () => {
  const changed = vi.fn();
  vi.stubGlobal("fetch", vi.fn(async () => json({ error: { code: "unauthorized" } }, 401)));
  function Access() {
    const [open, setOpen] = useState(true);
    return <><button id="reopen" onClick={() => setOpen(true)}>重新打开</button><RunAccess apiKey="" onKey={changed} onOpen={() => {}} keyOpen={open} setKeyOpen={setOpen} taskOpen={false} setTaskOpen={() => {}}/></>;
  }
  await act(async () => root.render(<Access/>));
  const input = document.querySelector<HTMLInputElement>("#api-key")!;
  await act(async () => { Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(input, "invalid-key"); input.dispatchEvent(new Event("input", { bubbles: true })); });
  await act(async () => (document.querySelector("#connect-save") as HTMLButtonElement).click());
  await until(() => expect(document.body.textContent).toContain("密钥无效或账号已停用"));
  expect(changed).not.toHaveBeenCalled();
  const cancel = [...document.querySelectorAll<HTMLButtonElement>("button")].find(button => button.textContent === "取消")!;
  await act(async () => cancel.click());
  await until(() => expect(document.querySelector("#api-key")).toBeNull());
  await act(async () => (document.querySelector("#reopen") as HTMLButtonElement).click());
  await until(() => expect(document.querySelector<HTMLInputElement>("#api-key")?.value).toBe(""));
});
