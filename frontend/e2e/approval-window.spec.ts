import { randomUUID } from "node:crypto";
import { expect, test, type Page } from "@playwright/test";

const headers = { Authorization: "Bearer formal-workbench-admin-browser-key" };
const keys = new WeakMap<Page, { session: string; turn: string }>();

async function openSession(page: Page) {
  await page.goto("/ui/");
  await page.locator(".access-connect").click();
  await page.locator("#api-key").fill("formal-workbench-admin-browser-key");
  await page.locator("#connect-save").click();
  await expect(page.locator(".topbar-status")).toContainText("已连接");
  const limits = await page.request.get("/test/tool-counts", { headers });
  expect(limits.ok()).toBe(true);
  expect((await limits.json()).limits).toEqual({ run: 3, tool: 1, approval: 300 });
  await page.getByRole("button", { name: "新建对话", exact: true }).click();
  await page.locator("#session-title").fill(`审批与工具结果-${randomUUID().slice(0, 8)}`);
  await page.getByRole("dialog").locator("button[type=submit]").click();
  await expect(page.locator("#chat-prompt")).toBeEnabled();
}
async function send(page: Page, text: string) {
  await page.locator("#chat-prompt").fill(text);
  await page.locator(".chat-send").click();
  const last = page.locator(".chat-turn").last();
  await expect(last.locator(".chat-user-text")).toHaveText(text);
  const session = new URL(page.url()).searchParams.get("session")!;
  const turn = (await last.getAttribute("data-turn-id"))!;
  expect(session).toBeTruthy(); expect(turn).toBeTruthy();
  keys.set(page, { session, turn });
  return `/v2/workspaces/local/sessions/${session}/turns/${turn}`;
}
async function status(page: Page, path: string) {
  const response = await page.request.get(path, { headers });
  expect(response.ok()).toBe(true);
  return (await response.json()).status as string;
}
test.afterEach(async ({ page }) => {
  const owned = keys.get(page);
  if (!owned) return;
  const sessionPath = `/v2/workspaces/local/sessions/${owned.session}`;
  const turnPath = `${sessionPath}/turns/${owned.turn}`;
  const response = await page.request.get(turnPath, { headers });
  if (response.ok() && ["queued", "running"].includes((await response.json()).status)) {
    const session = await page.request.get(sessionPath, { headers });
    const revision = (await session.json()).revision as number;
    const cancelled = await page.request.post(`${turnPath}/cancel`, { headers, data: { request_key: randomUUID(), expected_revision: revision } });
    expect(cancelled.ok()).toBe(true);
  }
});

test("审批等待超过工具及整轮执行时限后仍能批准，未找到笔记正常完成", async ({ page }, info) => {
  await openSession(page);
  const path = await send(page, "/note context");
  await expect(page.locator(".chat-approval")).toBeVisible();
  await expect(page.getByRole("timer", { name: "审批剩余时间" })).toContainText(/审批剩余 0[45]:\d{2}/);
  const events = await page.request.get(`${path}/events?from_seq=0`, { headers });
  const request = (await events.json() as { kind: string; at: number; data: { expires_at: number } }[]).find(event => event.kind === "approval_request")!;
  expect(request).toBeTruthy();
  expect(request.data.expires_at - request.at).toBeGreaterThan(299);
  expect(request.data.expires_at - request.at).toBeLessThanOrEqual(300);
  await page.locator(".chat-approval").screenshot({ path: info.outputPath("approval-five-minutes.png") });
  // 模拟用户阅读：真实等待超过本机fixture的1秒工具和3秒整轮执行预算。
  await page.waitForTimeout(4200);
  expect(await status(page, path)).toBe("running");
  await expect(page.getByRole("button", { name: "允许 lookup_note 本次调用", exact: true })).toBeEnabled();
  await page.getByRole("button", { name: "允许 lookup_note 本次调用", exact: true }).click();
  await expect.poll(() => status(page, path)).toBe("completed");
  const result = await page.request.get(`${path}/result`, { headers });
  const facts = await result.json();
  expect(facts.tools).toHaveLength(1);
  expect(facts.tools[0]).toMatchObject({ name: "lookup_note", status: "ok", result_available: true, value: { found: false, key: "context" } });
  const turn = page.locator(".chat-turn").last();
  await turn.locator(".chat-tool summary").click();
  await expect(turn.locator(".chat-tool-section pre").last()).toContainText('"found": false');
  await expect(turn.locator(".chat-answer")).toContainText("found");
  await expect(turn.locator(".chat-turn-notice")).toHaveCount(0);
  await expect(turn.locator(".chat-turn-meta > [role=status]")).toHaveCount(0);
  await expect(turn.getByRole("button", { name: "停止此轮", exact: true })).toHaveCount(0);
  await turn.screenshot({ path: info.outputPath("note-not-found-completed.png") });
});

test("真正的工具异常显示执行失败，保持与读取故障不同", async ({ page }, info) => {
  await openSession(page);
  const path = await send(page, "/echo synthetic-tool-error");
  await expect.poll(() => status(page, path)).toBe("failed");
  const turn = page.locator(".chat-turn").last();
  await expect(turn.locator(".chat-turn-notice")).toContainText("工具执行失败");
  await expect(turn.locator(".chat-turn-notice code")).toHaveText("tool_error");
  await turn.locator(".chat-tool summary").click();
  await expect(turn.locator(".detail-empty")).toHaveText("工具调用失败，未取得结果");
  await expect(turn).not.toContainText("读取失败，请刷新");
  await turn.screenshot({ path: info.outputPath("actual-tool-failure.png") });
});

test("等待审批期间仍可取消，执行器不进入", async ({ page }) => {
  await openSession(page);
  const before = (await (await page.request.get("/test/tool-counts", { headers })).json()).executions.length;
  const path = await send(page, "/note context");
  await expect(page.locator(".chat-approval")).toBeVisible();
  await page.locator("#chat-stop").click();
  await expect.poll(() => status(page, path)).toBe("cancelled");
  await expect(page.locator(".chat-approval")).toHaveCount(0);
  const after = await (await page.request.get("/test/tool-counts", { headers })).json();
  expect(after.executions).toHaveLength(before);
});
