import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { AccountSettings, type AccountSettingsProps, type AccountTab } from "../src/components/AccountSettings";
import type { AccountProfile, AccountView } from "../src/lib/accounts";

// AccountSettings 契约：mock fetch 返回 lib/accounts 真实 JSON 结构；
// 覆盖 member 无管理权限、结果未知不自动重发、一次性密钥关闭即清除、
// 切换凭据丢弃旧响应、编辑 expected_revision、资料 409、轮换 onKeyPrepared/onKeyChange 协议、
// 服务端 403 最后管理员保护。
let root: Root, host: HTMLDivElement;
interface Call { path: string; method: string; key: string; body: Record<string, unknown> | undefined }
let calls: Call[] = [];
let respond: (path: string, method: string, body: Record<string, unknown> | undefined, key: string) => Response | Promise<Response> = () => new Response("{}", { status: 404 });

const keyView = (over: Record<string, unknown> = {}) => ({
  key_id: "key_a1b2c3d4e5f60718", masked: "krim1.key_a1b2c3d4e5f60718…9f2e", credential_version: 1,
  status: "active", weak: false, issued_at: 1761000000, revoked_at: null, last_used_at: null, ...over,
});
const personalWs = () => ({ workspace_id: "ws-personal", display_name: "个人工作区", role: "admin", approve: true, personal: true });
const sharedWs = () => ({ workspace_id: "ws-shared", display_name: "研发工作区", role: "viewer", approve: false, personal: false });
const profileOf = (over: Record<string, unknown> = {}): AccountProfile => ({
  principal_id: "acct-admin", display_name: "Lenovo", status: "active", platform_role: "platform_admin",
  generation: 1, revision: 5, legacy: false, key: keyView(), workspaces: [personalWs()], ...over,
} as AccountProfile);
const adminProfile = () => profileOf();
const memberProfile = () => profileOf({ principal_id: "acct-member", display_name: "张宁", platform_role: "member", revision: 3 });
const accountOf = (over: Record<string, unknown> = {}): AccountView => ({
  principal_id: "acct-2", display_name: "项目体验", status: "active", platform_role: "member",
  generation: 1, revision: 7, legacy: false, created_at: 1760900000, updated_at: 1761000000,
  has_active_key: false, key: null, ...over,
} as AccountView);
const membershipsOf = () => ({ principal_id: "acct-2", generation: 1, memberships: [personalWs(), sharedWs()] });
const pageOf = (items: AccountView[]) => ({ items, offset: 0, total: items.length });
const issuedOf = (token: string | null) => ({
  principal_id: "acct-2",
  key: keyView({ key_id: "key_0123456789abcdef", masked: "krim1.key_0123456789abcdef…ab12", credential_version: 2 }),
  token, replayed: false, revoked_key_id: null,
});
const updatedOf = () => ({
  account: { ...accountOf(), display_name: "新名字", revision: 8, updated_at: 1761000100 }, replayed: false,
});
const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const notFound = () => json({ error: { code: "not_found" } }, 404);

const baseProps = (over: Partial<AccountSettingsProps> = {}): AccountSettingsProps => ({
  apiKey: "admin-key",
  identity: { principal_id: "acct-admin", generation: 1, legacy: false, workspaces: [] },
  profile: adminProfile(),
  active: true,
  tab: "accounts" as AccountTab,
  onTabChange: vi.fn(),
  onProfileChange: vi.fn(),
  onKeyPrepared: vi.fn(),
  onKeyChange: vi.fn(),
  onLogout: vi.fn(),
  onConnect: vi.fn(),
  ...over,
});
let props: AccountSettingsProps = baseProps();

beforeEach(() => {
  localStorage.clear(); sessionStorage.clear();
  calls = [];
  host = document.createElement("div"); document.body.append(host); root = createRoot(host);
  (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, options: RequestInit = {}) => {
    const path = String(input);
    const method = options.method ?? "GET";
    const key = new Headers(options.headers).get("Authorization") ?? "";
    const body = options.body ? JSON.parse(String(options.body)) as Record<string, unknown> : undefined;
    calls.push({ path, method, key, body });
    return respond(path, method, body, key);
  }));
});
afterEach(async () => {
  await act(async () => root.unmount());
  host.remove(); vi.unstubAllGlobals();
});
async function mount(overrides: Partial<AccountSettingsProps> = {}) {
  props = baseProps(overrides);
  await act(async () => root.render(<AccountSettings {...props} />));
}
async function rerender(overrides: Partial<AccountSettingsProps>) {
  props = { ...props, ...overrides };
  await act(async () => root.render(<AccountSettings {...props} />));
}
async function until(check: () => void) {
  let error: unknown;
  for (let attempt = 0; attempt < 120; attempt++) {
    try { check(); return; } catch (value) { error = value; }
    await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); });
  }
  throw error;
}
async function typeInput(element: HTMLInputElement, value: string) {
  await act(async () => {
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(element, value);
    element.dispatchEvent(new Event("input", { bubbles: true }));
  });
}
async function click(where: () => HTMLButtonElement | undefined) {
  const target = where();
  if (!target) throw new Error("目标按钮不存在");
  await act(async () => target.click());
}
const byText = (text: string) => [...document.querySelectorAll<HTMLButtonElement>("button")].find(b => b.textContent?.trim() === text);
const dialogButton = (text: string) =>
  [...document.querySelectorAll<HTMLButtonElement>("[data-slot='dialog-content'] button")].find(b => b.textContent?.trim() === text);
const menuItem = (text: string) =>
  [...document.querySelectorAll<HTMLButtonElement>("[role='menuitem']")].find(b => b.textContent?.trim() === text);
const drain = async () => { await act(async () => { for (let i = 0; i < 25; i++) await new Promise(resolve => setTimeout(resolve, 0)); }); };

test("迁移账号没有个人工作区时不伪造自有管理员权限", async () => {
  const account = accountOf();
  respond = (path, method) => {
    if (path.startsWith("/v2/admin/accounts?")) return json(pageOf([account]));
    if (path === "/v2/admin/accounts/acct-2" && method === "GET") return json(account);
    if (path === "/v2/admin/accounts/acct-2/memberships") return json({ principal_id: "acct-2", generation: 1, memberships: [sharedWs()] });
    return notFound();
  };
  await mount();
  await until(() => expect(host.querySelector('[aria-label="查看 项目体验"]')).toBeTruthy());
  await click(() => host.querySelector<HTMLButtonElement>('[aria-label="查看 项目体验"]') ?? undefined);
  await until(() => expect(document.body.textContent).toContain("研发工作区"));
  expect(document.querySelector('[role="dialog"]')?.textContent).not.toContain("账号自有工作区");
});

test("member账号看不到管理入口，accounts/events均无管理请求", async () => {
  respond = notFound;
  await mount({ profile: memberProfile(), tab: "accounts" });
  expect(host.textContent).toContain("没有账号管理权限");
  expect(host.querySelectorAll(".account-tab")).toHaveLength(1);
  await rerender({ tab: "events" });
  expect(host.textContent).toContain("没有账号管理权限");
  await drain();
  expect(calls.filter(value => value.path.startsWith("/v2/admin"))).toHaveLength(0);
});

test("签发结果未知时不自动重发，原request_key可查询管理记录", async () => {
  respond = (path, method) => {
    if (path.startsWith("/v2/admin/accounts?") && method === "GET") return json(pageOf([accountOf()]));
    if (path === "/v2/admin/accounts/acct-2/key/issue" && method === "POST") throw new TypeError("网络中断");
    if (path.startsWith("/v2/admin/account-events")) return json({ items: [], offset: 0, total: 0 });
    return notFound();
  };
  await mount();
  await act(async () => host.querySelector<HTMLButtonElement>('[aria-label="项目体验的操作"]')!.click());
  await until(() => expect(menuItem("签发密钥")).toBeTruthy());
  await click(() => menuItem("签发密钥"));
  await until(() => expect(dialogButton("签发密钥")).toBeTruthy());
  await click(() => dialogButton("签发密钥"));
  await until(() => expect(calls.filter(value => value.path.endsWith("/key/issue"))).toHaveLength(1));
  const requestKey = String(calls.find(value => value.path.endsWith("/key/issue"))!.body!.request_key);
  expect(requestKey).toMatch(/^[A-Za-z0-9_.:-]{1,128}$/);
  await until(() => expect(document.body.textContent).toContain("操作结果尚未确认"));
  await drain();
  expect(calls.filter(value => value.path.endsWith("/key/issue"))).toHaveLength(1);
  await click(() => dialogButton("查询原操作结果"));
  await until(() => expect(document.body.textContent).toContain("没有找到该请求的完成记录"));
  const query = calls.find(value => value.path.startsWith("/v2/admin/account-events"));
  expect(query!.path).toContain(`request_key=${requestKey}`);
  expect(calls.filter(value => value.path.endsWith("/key/issue"))).toHaveLength(1);
});

test("一次性密钥只在modal内存中：关闭后从页面与存储中消失", async () => {
  const token = `krim1.key_0123456789abcdef.${"A".repeat(43)}`;
  respond = (path, method) => {
    if (path.startsWith("/v2/admin/accounts?") && method === "GET") return json(pageOf([accountOf()]));
    if (path === "/v2/admin/accounts/acct-2/key/issue" && method === "POST") return json(issuedOf(token));
    return notFound();
  };
  const local = baseProps();
  props = local;
  await act(async () => root.render(<AccountSettings {...props} />));
  await act(async () => host.querySelector<HTMLButtonElement>('[aria-label="项目体验的操作"]')!.click());
  await until(() => expect(menuItem("签发密钥")).toBeTruthy());
  await click(() => menuItem("签发密钥"));
  await until(() => expect(dialogButton("签发密钥")).toBeTruthy());
  await click(() => dialogButton("签发密钥"));
  await until(() => expect(document.body.textContent).toContain(token));
  expect(local.onKeyPrepared).not.toHaveBeenCalled();
  expect(localStorage.length).toBe(0);
  expect(sessionStorage.length).toBe(0);
  await click(() => dialogButton("已保存，关闭"));
  expect(document.body.textContent).not.toContain(token);
  expect(localStorage.length).toBe(0);
  expect(sessionStorage.length).toBe(0);
});

test("切换凭据后旧列表响应被丢弃，不渲染旧数据且以新凭据重新读取", async () => {
  let release!: () => void;
  const held = new Promise<Response>(resolve => { release = () => resolve(json(pageOf([accountOf({ display_name: "旧账号" })]))); });
  respond = (path, method, _body, key) => {
    if (path.startsWith("/v2/admin/accounts?") && method === "GET") {
      return key === "Bearer key-b" ? json(pageOf([accountOf({ display_name: "新账号" })])) : held;
    }
    return notFound();
  };
  await mount({ apiKey: "key-a" });
  await until(() => expect(calls.some(value => value.key === "Bearer key-a")).toBe(true));
  await rerender({ apiKey: "key-b" });
  await until(() => expect(calls.some(value => value.key === "Bearer key-b")).toBe(true));
  await act(async () => { release(); await held; });
  await drain();
  expect(host.textContent).not.toContain("旧账号");
  expect(host.textContent).toContain("新账号");
  const listCalls = calls.filter(value => value.path.startsWith("/v2/admin/accounts?"));
  expect(listCalls.every(value => value.key === "Bearer key-a" || value.key === "Bearer key-b")).toBe(true);
});

test("账号详情编辑按当前revision携带expected_revision，仅改名不触发权限PUT", async () => {
  const listed = accountOf();
  respond = (path, method) => {
    if (path.startsWith("/v2/admin/accounts?") && method === "GET") return json(pageOf([listed]));
    if (path === "/v2/admin/accounts/acct-2" && method === "GET") return json(listed);
    if (path === "/v2/admin/accounts/acct-2/memberships" && method === "GET") return json(membershipsOf());
    if (path === "/v2/admin/accounts/acct-2" && method === "PATCH") return json(updatedOf());
    return notFound();
  };
  await mount();
  await act(async () => host.querySelector<HTMLButtonElement>('[aria-label="查看 项目体验"]')!.click());
  await until(() => expect((document.getElementById("account-edit-name") as HTMLInputElement | null)?.value).toBe("项目体验"));
  await typeInput(document.getElementById("account-edit-name") as HTMLInputElement, "新名字");
  await click(() => byText("保存更改"));
  await until(() => expect(calls.filter(value => value.path === "/v2/admin/accounts/acct-2" && value.method === "PATCH")).toHaveLength(1));
  const body = calls.find(value => value.path === "/v2/admin/accounts/acct-2" && value.method === "PATCH")!.body!;
  expect(body.expected_revision).toBe(7);
  expect(body.display_name).toBe("新名字");
  expect(String(body.request_key)).toMatch(/^[A-Za-z0-9_.:-]{1,128}$/);
  expect(body.platform_role).toBeUndefined();
  expect(calls.some(value => value.path.endsWith("/memberships") && value.method === "PUT")).toBe(false);
  await until(() => expect(host.textContent).toContain("新名字"));
});

test("资料更名携带expected_revision；409展示冲突且重新读取，不静默覆盖", async () => {
  respond = (path, method) => {
    if (path === "/v2/me/profile" && method === "PATCH") return json({ error: { code: "revision_conflict" } }, 409);
    return notFound();
  };
  const local = baseProps({ tab: "profile" as AccountTab });
  props = local;
  await act(async () => root.render(<AccountSettings {...props} />));
  await until(() => expect((document.getElementById("account-display-name") as HTMLInputElement | null)?.value).toBe("Lenovo"));
  await typeInput(document.getElementById("account-display-name") as HTMLInputElement, "改名字");
  await click(() => byText("保存更改"));
  await until(() => expect(calls.filter(value => value.path === "/v2/me/profile" && value.method === "PATCH")).toHaveLength(1));
  const body = calls.find(value => value.path === "/v2/me/profile" && value.method === "PATCH")!.body!;
  expect(body.expected_revision).toBe(5);
  expect(body.display_name).toBe("改名字");
  expect(String(body.request_key)).toBeTruthy();
  await until(() => expect(document.body.textContent).toContain("资料已在其他页面更新"));
  expect((document.getElementById("account-display-name") as HTMLInputElement).value).toBe("改名字");
  expect(local.onProfileChange).not.toHaveBeenCalled();
  await click(() => byText("重新读取"));
  expect(local.onProfileChange).toHaveBeenCalledTimes(1);
});

test("self rotate成功先onKeyPrepared，modal关闭时onKeyChange并移除明文", async () => {
  const token = `krim1.key_fedcba9876543210.${"B".repeat(43)}`;
  respond = (path, method) => {
    if (path === "/v2/me/key/rotate" && method === "POST") {
      return json({
        principal_id: "acct-admin",
        key: keyView({ key_id: "key_fedcba9876543210", masked: "krim1.key_fedcba9876543210…fed9", credential_version: 2 }),
        token, replayed: false, revoked_key_id: "key_a1b2c3d4e5f60718",
      });
    }
    return notFound();
  };
  const local = baseProps({ tab: "profile" as AccountTab });
  props = local;
  await act(async () => root.render(<AccountSettings {...props} />));
  await until(() => expect(byText("轮换密钥")).toBeTruthy());
  await click(() => byText("轮换密钥"));
  await until(() => expect(dialogButton("轮换密钥")).toBeTruthy());
  await click(() => dialogButton("轮换密钥"));
  await until(() => expect(document.body.textContent).toContain(token));
  expect(local.onKeyPrepared).toHaveBeenCalledTimes(1);
  expect(local.onKeyPrepared).toHaveBeenCalledWith(token);
  expect(local.onKeyChange).not.toHaveBeenCalled();
  expect(calls.find(value => value.path === "/v2/me/key/rotate")!.body!.request_key).toBeTruthy();
  await click(() => dialogButton("已保存，关闭"));
  expect(local.onKeyChange).toHaveBeenCalledTimes(1);
  expect(local.onKeyChange).toHaveBeenCalledWith(token);
  expect(document.body.textContent).not.toContain(token);
});

test("停用最后管理员由服务端403拒绝，按文案展示保护提示", async () => {
  const listed = accountOf({ status: "active", has_active_key: true, key: keyView() });
  respond = (path, method) => {
    if (path.startsWith("/v2/admin/accounts?") && method === "GET") return json(pageOf([listed]));
    if (path === "/v2/admin/accounts/acct-2" && method === "PATCH") return json({ error: { code: "permission_denied" } }, 403);
    return notFound();
  };
  await mount();
  await act(async () => host.querySelector<HTMLButtonElement>('[aria-label="项目体验的操作"]')!.click());
  await until(() => expect(menuItem("停用账号")).toBeTruthy());
  await click(() => menuItem("停用账号"));
  await until(() => expect(dialogButton("停用账号")).toBeTruthy());
  await click(() => dialogButton("停用账号"));
  await until(() => expect(calls.some(value => value.path === "/v2/admin/accounts/acct-2" && value.method === "PATCH")).toBe(true));
  const body = calls.find(value => value.path === "/v2/admin/accounts/acct-2" && value.method === "PATCH")!.body!;
  expect(body.status).toBe("disabled");
  expect(body.expected_revision).toBe(7);
  await until(() => expect(document.body.textContent).toContain("至少保留一个可登录的管理员账号"));
  expect(dialogButton("取消")).toBeTruthy();
});

test("对自己的账号签发密钥走self协议：onKeyPrepared立即回调，关闭才onKeyChange", async () => {
  const token = `krim1.key_1212121212121212.${"C".repeat(43)}`;
  const listed = accountOf({ principal_id: "acct-admin", display_name: "Lenovo", platform_role: "platform_admin", has_active_key: true, key: keyView() });
  respond = (path, method) => {
    if (path.startsWith("/v2/admin/accounts?") && method === "GET") return json(pageOf([listed]));
    if (path === "/v2/admin/accounts/acct-admin/key/issue" && method === "POST") {
      return json({ ...issuedOf(token), principal_id: "acct-admin" });
    }
    return notFound();
  };
  const local = baseProps();
  props = local;
  await act(async () => root.render(<AccountSettings {...props} />));
  await act(async () => host.querySelector<HTMLButtonElement>('[aria-label="Lenovo的操作"]')!.click());
  await until(() => expect(menuItem("重新签发密钥")).toBeTruthy());
  await click(() => menuItem("重新签发密钥"));
  await until(() => expect(dialogButton("重新签发")).toBeTruthy());
  await click(() => dialogButton("重新签发"));
  await until(() => expect(document.body.textContent).toContain(token));
  expect(local.onKeyPrepared).toHaveBeenCalledTimes(1);
  expect(local.onKeyPrepared).toHaveBeenCalledWith(token);
  expect(local.onKeyChange).not.toHaveBeenCalled();
  expect(local.onLogout).not.toHaveBeenCalled();
  await click(() => dialogButton("已保存，关闭"));
  expect(local.onKeyChange).toHaveBeenCalledTimes(1);
  expect(local.onKeyChange).toHaveBeenCalledWith(token);
  expect(local.onLogout).not.toHaveBeenCalled();
  expect(document.body.textContent).not.toContain(token);
});

test("详情对话框吊销自己的密钥成功后立即onLogout", async () => {
  const listed = accountOf({ principal_id: "acct-admin", display_name: "Lenovo", platform_role: "platform_admin", has_active_key: true, key: keyView() });
  respond = (path, method) => {
    if (path.startsWith("/v2/admin/accounts?") && method === "GET") return json(pageOf([listed]));
    if (path === "/v2/admin/accounts/acct-admin" && method === "GET") return json(listed);
    if (path === "/v2/admin/accounts/acct-admin/memberships" && method === "GET") return json({ principal_id: "acct-admin", generation: 1, memberships: [personalWs()] });
    if (path === "/v2/me/key/revoke" && method === "POST") {
      return json({ principal_id: "acct-admin", key: keyView({ status: "revoked", revoked_at: 1761000200 }), generation: 2, replayed: false });
    }
    return notFound();
  };
  const local = baseProps();
  props = local;
  await act(async () => root.render(<AccountSettings {...props} />));
  await act(async () => host.querySelector<HTMLButtonElement>('[aria-label="查看 Lenovo"]')!.click());
  await until(() => expect(byText("吊销")).toBeTruthy());
  await click(() => byText("吊销"));
  await until(() => expect(dialogButton("吊销密钥")).toBeTruthy());
  await click(() => dialogButton("吊销密钥"));
  await until(() => expect(local.onLogout).toHaveBeenCalledTimes(1));
  // 自己的吊销按 self 协议走 /v2/me/key/revoke，不再以失效 key 请求管理接口
  const revokes = calls.filter(value => value.path === "/v2/me/key/revoke" && value.method === "POST");
  expect(revokes).toHaveLength(1);
  expect(revokes[0]!.body!.request_key).toBeTruthy();
  expect(calls.some(value => value.path === "/v2/admin/accounts/acct-admin/key/revoke")).toBe(false);
  expect(local.onKeyChange).not.toHaveBeenCalled();
});

test("编辑自己账号的角色变更成功后触发onProfileChange让App重读身份", async () => {
  const listed = accountOf({ principal_id: "acct-admin", display_name: "Lenovo", platform_role: "platform_admin", has_active_key: true, key: keyView() });
  respond = (path, method) => {
    if (path.startsWith("/v2/admin/accounts?") && method === "GET") return json(pageOf([listed]));
    if (path === "/v2/admin/accounts/acct-admin" && method === "GET") return json(listed);
    if (path === "/v2/admin/accounts/acct-admin/memberships" && method === "GET") return json({ principal_id: "acct-admin", generation: 1, memberships: [personalWs()] });
    if (path === "/v2/admin/accounts/acct-admin" && method === "PATCH") {
      return json({ account: { ...listed, platform_role: "member", revision: 6 }, replayed: false });
    }
    return notFound();
  };
  const local = baseProps();
  props = local;
  await act(async () => root.render(<AccountSettings {...props} />));
  await act(async () => host.querySelector<HTMLButtonElement>('[aria-label="查看 Lenovo"]')!.click());
  await until(() => expect((document.getElementById("account-edit-name") as HTMLInputElement | null)?.value).toBe("Lenovo"));
  await act(async () => {
    const select = document.querySelector<HTMLSelectElement>("[data-slot='dialog-content'] select[aria-label='平台角色']")!;
    Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, "value")!.set!.call(select, "member");
    select.dispatchEvent(new Event("change", { bubbles: true }));
  });
  await click(() => byText("保存更改"));
  await until(() => expect(calls.some(value => value.path === "/v2/admin/accounts/acct-admin" && value.method === "PATCH")).toBe(true));
  const body = calls.find(value => value.path === "/v2/admin/accounts/acct-admin" && value.method === "PATCH")!.body!;
  expect(body.platform_role).toBe("member");
  expect(local.onProfileChange).toHaveBeenCalledTimes(1);
});

test("外部profile更新到达时保留未保存草稿并标记冲突，主动重新读取才丢弃", async () => {
  respond = notFound;
  await mount({ tab: "profile" as AccountTab });
  await until(() => expect((document.getElementById("account-display-name") as HTMLInputElement | null)?.value).toBe("Lenovo"));
  await typeInput(document.getElementById("account-display-name") as HTMLInputElement, "草稿名字");
  // focus 类外部更新：revision 变化且服务端名称不同 → 草稿保留并提示冲突，不静默覆盖
  await rerender({ profile: { ...adminProfile(), display_name: "外部改名", revision: 6 } });
  expect((document.getElementById("account-display-name") as HTMLInputElement).value).toBe("草稿名字");
  await until(() => expect(document.body.textContent).toContain("资料已在其他页面更新"));
  const save = byText("保存更改")!;
  expect(save.disabled).toBe(false);
  // 用户主动重新读取：清草稿并以服务端为准
  await click(() => byText("重新读取"));
  expect(props.onProfileChange).toHaveBeenCalledTimes(1);
  await rerender({ profile: { ...adminProfile(), display_name: "外部改名", revision: 7 } });
  await until(() => expect((document.getElementById("account-display-name") as HTMLInputElement)?.value).toBe("外部改名"));
  expect(document.body.textContent).not.toContain("资料已在其他页面更新");
});

test("rename结果未确认禁用重提交：可读资料核对，管理员可原键查询", async () => {
  respond = (path, method) => {
    if (path === "/v2/me/profile" && method === "PATCH") throw new TypeError("网络中断");
    if (path.startsWith("/v2/admin/account-events")) return json({ items: [], offset: 0, total: 0 });
    return notFound();
  };
  const local = baseProps({ tab: "profile" as AccountTab });
  props = local;
  await act(async () => root.render(<AccountSettings {...props} />));
  await until(() => expect((document.getElementById("account-display-name") as HTMLInputElement | null)?.value).toBe("Lenovo"));
  await typeInput(document.getElementById("account-display-name") as HTMLInputElement, "改名字");
  await click(() => byText("保存更改"));
  await until(() => expect(calls.filter(value => value.path === "/v2/me/profile" && value.method === "PATCH")).toHaveLength(1));
  await until(() => expect(document.body.textContent).toContain("重新保存已暂时禁用"));
  const save = byText("保存更改")!;
  expect(save.disabled).toBe(true);
  await drain();
  expect(calls.filter(value => value.path === "/v2/me/profile" && value.method === "PATCH")).toHaveLength(1);
  await click(() => byText("查询原操作结果"));
  await until(() => expect(document.body.textContent).toContain("没有找到该请求的完成记录"));
  const query = calls.find(value => value.path.startsWith("/v2/admin/account-events"));
  expect(query!.path).toContain("request_key=");
  expect(save.disabled).toBe(true);
  await click(() => byText("重新读取资料"));
  expect(local.onProfileChange).toHaveBeenCalledTimes(1);
});
