import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { Memory, type MemoryAdapter } from "../src/components/Memory";
import type { Memory as MemoryValue, MemoryPageQuery } from "../src/lib/memory";

let root: Root;
let host: HTMLDivElement;
const item = (over: Partial<MemoryValue> = {}): MemoryValue => ({
  id: "memory-one", body: "喜欢清茶", revision: 2, content_hash: "a".repeat(64), source_ref: "用户手记",
  workspace_id: null, project_id: null, source_kind: "user", memory_kind: "preference", authorization_turn_id: null,
  created_at: 1761000000, updated_at: 1761000100, principal_id: "owner.main", ...over,
});
let adapter: MemoryAdapter;
let pageCalls: { offset?: number; limit?: number; query?: string }[];
beforeEach(() => {
  pageCalls = [];
  host = document.createElement("div"); document.body.append(host); root = createRoot(host);
  (globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  adapter = {
  list: vi.fn(async (query: MemoryPageQuery) => { pageCalls.push(query); return { items: [item()], next_offset: null }; }),
    get: vi.fn(async id => item({ id })),
    create: vi.fn(async () => ({ id: "memory-new", revision: 1, content_hash: "b".repeat(64) })),
    update: vi.fn(async () => ({ id: "memory-one", revision: 3, content_hash: "c".repeat(64) })),
    remove: vi.fn(async () => ({ id: "memory-one", removed: true })),
    preference: vi.fn(async () => ({ enabled: false, revision: 0 })),
    setPreference: vi.fn(async enabled => ({ enabled, revision: 1 })),
  };
});
afterEach(async () => {
  await act(async () => root.unmount());
  host.remove(); vi.restoreAllMocks();
});
async function mount() {
  await act(async () => root.render(<Memory scope={{ kind: "account" }} adapter={adapter} />));
}
async function until(check: () => void) {
  let error: unknown;
  for (let attempt = 0; attempt < 120; attempt++) {
    try { check(); return; } catch (value) { error = value; }
    await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); });
  }
  throw error;
}
async function click(element: HTMLElement | null) {
  if (!element) throw new Error("记忆操作按钮不存在");
  await act(async () => element.click());
}
async function typeInput(element: HTMLInputElement | HTMLTextAreaElement, value: string) {
  await act(async () => {
    const prototype = element instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
    Object.getOwnPropertyDescriptor(prototype, "value")!.set!.call(element, value);
    element.dispatchEvent(new Event("input", { bubbles: true }));
  });
}

 test("账户记忆列表默认不启用偏好，允许搜索、查看、编辑、删除和显式开启", async () => {
  await mount();
  await until(() => expect(host.textContent).toContain("喜欢清茶"));
  expect(host.querySelector<HTMLInputElement>('[aria-label="允许模型使用记忆"]')?.checked).toBe(false);
  expect(adapter.setPreference).not.toHaveBeenCalled();
  expect(host.textContent).toContain("来源：用户手记");

  const search = host.querySelector<HTMLInputElement>('[aria-label="搜索记忆"]')!;
  await typeInput(search, "清茶");
  await click([...host.querySelectorAll<HTMLButtonElement>("button")].find(button => button.textContent?.trim() === "搜索") ?? null);
  await until(() => expect(pageCalls.some(call => call.query === "清茶")).toBe(true));

  await click(host.querySelector<HTMLButtonElement>('[aria-label^="查看记忆"]'));
  await until(() => expect(host.textContent).toContain("创建于"));
  await click([...host.querySelectorAll<HTMLButtonElement>("button")].find(button => button.textContent?.trim() === "编辑") ?? null);
  const editor = host.querySelector<HTMLTextAreaElement>("#memory-edit-memory-one")!;
  await typeInput(editor, "更喜欢淡茶");
  await click([...host.querySelectorAll<HTMLButtonElement>("button")].find(button => button.textContent?.trim() === "保存更改") ?? null);
  await until(() => expect(adapter.update).toHaveBeenCalledTimes(1));
  const update = vi.mocked(adapter.update).mock.calls[0]!;
  expect(update[0]).toBe("memory-one");
  expect(update[1].body).toBe("更喜欢淡茶");
  expect(update[1].expected_revision).toBe(2);
  expect(update[1].expected_content_hash).toBe("a".repeat(64));
  await until(() => expect(adapter.get).toHaveBeenCalledTimes(2));

  const toggle = host.querySelector<HTMLInputElement>('[aria-label="允许模型使用记忆"]')!;
  await click(toggle);
  await until(() => expect(adapter.setPreference).toHaveBeenCalledTimes(1));
  const setPreference = adapter.setPreference;
  if (!setPreference) throw new Error("缺少记忆偏好测试适配器");
  expect(vi.mocked(setPreference).mock.calls[0]![0]).toBe(true);
  expect(vi.mocked(setPreference).mock.calls[0]![1]).toBe(0);

  await click([...host.querySelectorAll<HTMLButtonElement>("button")].find(button => button.textContent?.includes("新增记忆")) ?? null);
  await typeInput(host.querySelector<HTMLTextAreaElement>("#memory-create-body")!, "项目里使用简洁称呼");
  await click([...host.querySelectorAll<HTMLButtonElement>("button")].find(button => button.textContent?.trim() === "保存记忆") ?? null);
  await until(() => expect(adapter.create).toHaveBeenCalledTimes(1));
  const create = vi.mocked(adapter.create).mock.calls[0]![0];
  expect(create.body).toBe("项目里使用简洁称呼");
  expect(create.request_key).toMatch(/^[!-~]{1,128}$/);

  await click([...host.querySelectorAll<HTMLButtonElement>("button")].find(button => button.textContent?.trim() === "删除") ?? null);
  expect(host.querySelector('[aria-label="确认删除记忆"]')).toBeTruthy();
  await click([...host.querySelectorAll<HTMLButtonElement>("button")].find(button => button.textContent?.trim() === "确认删除") ?? null);
  await until(() => expect(adapter.remove).toHaveBeenCalledTimes(1));
  const removal = vi.mocked(adapter.remove).mock.calls[0]!;
  expect(removal[0]).toBe("memory-one");
  expect(removal[1].expected_revision).toBe(2);
  expect(removal[1].expected_content_hash).toBe("a".repeat(64));
});

test("项目记忆适配器收到项目范围且支持加载更多", async () => {
  vi.mocked(adapter.list).mockImplementation(async (query: MemoryPageQuery) => {
    pageCalls.push(query);
    return query.offset === 0 ? { items: [item()], next_offset: 1 } : { items: [item(), item({ id: "memory-two", body: "另一条偏好" })], next_offset: null };
  });
  await act(async () => root.render(<Memory scope={{ kind: "project", workspace_id: "ws-project", project_id: "project-one" }} adapter={adapter} />));
  await until(() => expect(host.textContent).toContain("喜欢清茶"));
  expect(vi.mocked(adapter.list).mock.calls[0]![1]).toEqual({ workspace_id: "ws-project", project_id: "project-one" });
  await click([...host.querySelectorAll<HTMLButtonElement>("button")].find(button => button.textContent?.trim() === "加载更多") ?? null);
  await until(() => expect(adapter.list).toHaveBeenCalledTimes(2));
  expect(vi.mocked(adapter.list).mock.calls[1]![0].offset).toBe(1);
  await until(() => expect(host.querySelectorAll(".memory-item")).toHaveLength(2));
  expect(host.querySelector('[aria-label="允许模型使用记忆"]')).toBeNull();
});

test("并发详情读取只应用当前条目，旧项目记忆草稿在切换范围时清除", async () => {
  let releaseOld!: (value: MemoryValue) => void;
  const oldDetail = new Promise<MemoryValue>(resolve => { releaseOld = resolve; });
  vi.mocked(adapter.list).mockResolvedValue({
    items: [item(), item({ id: "memory-two", body: "另一条偏好" })], next_offset: null,
  });
  vi.mocked(adapter.get).mockImplementation(id => id === "memory-one"
    ? oldDetail
    : Promise.resolve(item({ id, body: "第二条详情" })));
  await mount();
  await until(() => expect(host.querySelectorAll(".memory-item")).toHaveLength(2));
  await click(host.querySelector<HTMLButtonElement>('[aria-label="查看记忆 喜欢清茶"]'));
  await click(host.querySelector<HTMLButtonElement>('[aria-label="查看记忆 另一条偏好"]'));
  await until(() => expect(host.textContent).toContain("第二条详情"));
  await act(async () => { releaseOld(item({ body: "迟到的第一条详情" })); await oldDetail; });
  expect(host.textContent).toContain("第二条详情");
  expect(host.textContent).not.toContain("迟到的第一条详情");

  await click([...host.querySelectorAll<HTMLButtonElement>("button")].find(button => button.textContent?.includes("新增记忆")) ?? null);
  await typeInput(host.querySelector<HTMLTextAreaElement>("#memory-create-body")!, "账号私有草稿");
  await typeInput(host.querySelector<HTMLInputElement>('[aria-label="搜索记忆"]')!, "账号私有搜索");
  await act(async () => root.render(<Memory scope={{ kind: "project", workspace_id: "ws-new", project_id: "project-new" }} adapter={adapter} />));
  await until(() => expect(vi.mocked(adapter.list).mock.calls.some(call => call[1]?.workspace_id === "ws-new")).toBe(true));
  expect(host.textContent).not.toContain("账号私有草稿");
  expect(host.querySelector<HTMLInputElement>('[aria-label="搜索记忆"]')?.value).toBe("");
  expect(host.querySelector("#memory-create-body")).toBeNull();
});

test("切换记忆范围后迟到的创建响应不会刷新新范围", async () => {
  let releaseCreate!: (value: unknown) => void;
  const delayedCreate = new Promise<unknown>(resolve => { releaseCreate = resolve; });
  vi.mocked(adapter.create).mockImplementation(() => delayedCreate);
  await mount();
  await until(() => expect(host.textContent).toContain("喜欢清茶"));
  await click([...host.querySelectorAll<HTMLButtonElement>("button")].find(button => button.textContent?.includes("新增记忆")) ?? null);
  await typeInput(host.querySelector<HTMLTextAreaElement>("#memory-create-body")!, "账号范围草稿");
  await click([...host.querySelectorAll<HTMLButtonElement>("button")].find(button => button.textContent?.trim() === "保存记忆") ?? null);
  await until(() => expect(adapter.create).toHaveBeenCalledTimes(1));
  await act(async () => root.render(<Memory scope={{ kind: "project", workspace_id: "ws-new", project_id: "project-new" }} adapter={adapter} />));
  await until(() => expect(vi.mocked(adapter.list).mock.calls.some(call => call[1]?.workspace_id === "ws-new")).toBe(true));
  await act(async () => { releaseCreate({ id: "account-only", revision: 1, content_hash: "b".repeat(64) }); await delayedCreate; });
  await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); });
  expect(adapter.list).toHaveBeenCalledTimes(2);
  expect(host.textContent).not.toContain("账号范围草稿");
});
