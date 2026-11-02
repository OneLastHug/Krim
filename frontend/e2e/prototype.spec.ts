import { mkdir } from "node:fs/promises";
import { resolve } from "node:path";
import { expect, test, type Page } from "@playwright/test";

const baseURL = "http://127.0.0.1:5182";
const screenshotDirectory = resolve(import.meta.dirname, "..", "..", ".cache", "ui-prototype", "screenshots");

type BrowserObservations = {
  pageErrors: string[];
  apiRequests: string[];
  crossOriginRequests: string[];
  crossOriginWebSockets: string[];
};

const observations = new WeakMap<Page, BrowserObservations>();

async function saveScreenshot(page: Page, name: string) {
  await page.screenshot({ path: resolve(screenshotDirectory, name), animations: "disabled" });
}

async function openPrototype(page: Page) {
  await page.goto("/prototype.html", { waitUntil: "domcontentloaded" });
  await expect(page.locator(".p-app")).toBeVisible();
}

async function expectNoHorizontalOverflow(page: Page) {
  const widths = await page.evaluate(() => ({
    viewport: window.innerWidth,
    document: document.documentElement.scrollWidth,
    app: document.querySelector<HTMLElement>(".p-app")?.scrollWidth ?? 0,
  }));
  expect(widths.document, `document wider than ${widths.viewport}px viewport`).toBeLessThanOrEqual(widths.viewport);
  expect(widths.app, `prototype app wider than ${widths.viewport}px viewport`).toBeLessThanOrEqual(widths.viewport);
}

async function selectSession(page: Page, title: string) {
  await page.locator(".p-session-button").filter({ hasText: title }).first().click();
}

async function openSidebar(page: Page) {
  const expand = page.getByRole("button", { name: "展开侧栏" });
  if (await expand.isVisible()) await expand.click();
  await expect(page.getByRole("complementary", { name: "会话导航" })).toBeVisible();
}

async function closeMobileSidebar(page: Page) {
  const backdrop = page.getByRole("button", { name: "关闭导航背景" });
  if (await backdrop.count()) await backdrop.click({ position: { x: 300, y: 420 } });
}

test.beforeAll(async () => {
  await mkdir(screenshotDirectory, { recursive: true });
});

test.beforeEach(async ({ page }) => {
  const state: BrowserObservations = {
    pageErrors: [],
    apiRequests: [],
    crossOriginRequests: [],
    crossOriginWebSockets: [],
  };
  observations.set(page, state);
  page.on("pageerror", error => state.pageErrors.push(error.message));
  page.on("request", request => {
    const url = new URL(request.url());
    if (url.origin !== baseURL) state.crossOriginRequests.push(`${request.resourceType()} ${request.url()}`);
    if (request.resourceType() === "fetch" || request.resourceType() === "xhr") {
      state.apiRequests.push(`${request.resourceType()} ${request.method()} ${request.url()}`);
    }
  });
  page.on("websocket", socket => {
    const url = new URL(socket.url());
    if (url.host !== new URL(baseURL).host) state.crossOriginWebSockets.push(socket.url());
  });
});

test.afterEach(async ({ page }, testInfo) => {
  const state = observations.get(page)!;
  await testInfo.attach("browser-observations.json", {
    body: Buffer.from(JSON.stringify(state, null, 2)),
    contentType: "application/json",
  });
  expect(state.pageErrors, "uncaught page errors").toEqual([]);
  // Vite module loads and its same-origin HMR websocket are not API calls.
  expect(state.apiRequests, "business API fetch/XHR requests").toEqual([]);
  expect(state.crossOriginRequests, "cross-origin HTTP requests").toEqual([]);
  expect(state.crossOriginWebSockets, "cross-origin websocket connections").toEqual([]);
});

test("1440px new session keeps a compact single-line composer", async ({ page }) => {
  await openPrototype(page);
  await expect(page.getByRole("heading", { name: "新会话" })).toBeVisible();
  await expect(page.getByRole("complementary", { name: "会话导航" })).toBeVisible();
  const prompt = page.getByRole("textbox", { name: "发送消息" });
  await expect(prompt).toBeVisible();
  const measurements = await prompt.evaluate(element => ({
    fontSize: getComputedStyle(element).fontSize,
    height: element.getBoundingClientRect().height,
    rows: (element as HTMLTextAreaElement).rows,
  }));
  expect(measurements.fontSize).toBe("12px");
  expect(measurements.rows).toBe(1);
  expect(measurements.height).toBeGreaterThanOrEqual(22);
  expect(measurements.height).toBeLessThanOrEqual(26);
  await expectNoHorizontalOverflow(page);
  await saveScreenshot(page, "1440-new-session.png");
});

test("Markdown formatting and timeout.py preview open and close", async ({ page }) => {
  await openPrototype(page);
  await selectSession(page, "工具调用与超时处理");
  await expect(page.locator(".p-markdown strong")).toContainText("调用时限、取消传播、结果归属");
  await expect(page.getByRole("region", { name: "表格" })).toBeVisible();
  await expect(page.locator(".p-markdown table th")).toHaveCount(3);
  await expect(page.locator(".p-code-block pre code")).toContainText("asyncio.timeout");
  await saveScreenshot(page, "1440-markdown.png");

  await page.locator(".p-reference-files button").filter({ hasText: "timeout.py" }).click();
  const preview = page.locator('aside[aria-label="文件预览"]');
  await expect(preview).toContainText("src/runtime/timeout.py");
  await expect(preview).toContainText("async def call_with_timeout");
  await saveScreenshot(page, "1440-file-panel.png");
  await page.getByRole("button", { name: "关闭文件预览" }).click();
  await expect(preview).toHaveCount(0);
});

test("drafts stay per conversation and Enter honors newline and composition", async ({ page }) => {
  await openPrototype(page);
  const prompt = page.getByRole("textbox", { name: "发送消息" });
  await prompt.fill("草稿A");
  await page.locator(".p-new-button").click();
  await prompt.fill("草稿B");

  const newSessionButtons = page.locator(".p-session-button").filter({ hasText: "新会话" });
  await expect(newSessionButtons).toHaveCount(2);
  await newSessionButtons.nth(1).click();
  await expect(prompt).toHaveValue("草稿A");
  await newSessionButtons.nth(0).click();
  await expect(prompt).toHaveValue("草稿B");
  await newSessionButtons.nth(1).click();

  await prompt.fill("换行第一行");
  await prompt.press("Shift+Enter");
  await expect(prompt).toHaveValue("换行第一行\n");
  await prompt.fill("输入中的草稿");
  await prompt.dispatchEvent("compositionstart", { data: "中" });
  await prompt.press("Enter");
  await expect(page.locator(".p-turn")).toHaveCount(0);
  await expect(prompt).toHaveValue(/输入中的草稿/);
  await prompt.dispatchEvent("compositionend", { data: "中" });

  await prompt.fill("Enter 发送验证");
  await prompt.press("Enter");
  await expect(page.locator(".p-user-text")).toHaveText("Enter 发送验证");
});

test("stopping freezes generated text and regeneration completes", async ({ page }) => {
  await openPrototype(page);
  const prompt = page.getByRole("textbox", { name: "发送消息" });
  await prompt.fill("继续展开取消后如何处理已经产生的结果");
  await prompt.press("Enter");
  await expect(page.locator(".p-stream-cursor")).toBeVisible();
  await page.getByRole("button", { name: "停止生成" }).click();
  await expect(page.locator(".p-stopped")).toContainText("已停止");
  const stoppedText = await page.locator(".p-markdown").innerText();
  await page.waitForTimeout(300);
  await expect(page.locator(".p-markdown")).toHaveText(stoppedText);
  await expect(page.locator(".p-stream-cursor")).toHaveCount(0);
  await saveScreenshot(page, "1440-stopped-generation.png");

  await page.locator(".p-stopped").getByRole("button", { name: "重新生成" }).click();
  await expect(page.getByRole("button", { name: "重新生成回答" })).toBeVisible({ timeout: 10_000 });
  await expect(page.locator(".p-stream-cursor")).toHaveCount(0);
});

test("session menu renames, pins, archives, and search closes with Escape", async ({ page }) => {
  await openPrototype(page);
  const initialTitle = "把 Agent 拆开来看";
  let row = page.locator(".p-session").filter({ hasText: initialTitle });
  await row.hover();
  await row.getByRole("button", { name: `${initialTitle} 的操作` }).click();
  await expect(page.getByRole("menuitem", { name: "重命名" })).toBeVisible();
  await saveScreenshot(page, "1440-session-menu.png");
  await page.getByRole("menuitem", { name: "重命名" }).click();
  await page.getByLabel("会话名称").fill("菜单操作验证");
  await page.getByRole("button", { name: "保存" }).click();

  row = page.locator(".p-session").filter({ hasText: "菜单操作验证" });
  await row.hover();
  await row.getByRole("button", { name: "菜单操作验证 的操作" }).click();
  await page.getByRole("menuitem", { name: "置顶" }).click();
  await expect(page.locator(".p-session-group").first()).toContainText("菜单操作验证");

  row = page.locator(".p-session").filter({ hasText: "菜单操作验证" });
  await row.hover();
  await row.getByRole("button", { name: "菜单操作验证 的操作" }).click();
  await page.getByRole("menuitem", { name: "归档会话" }).click();
  await expect(page.locator(".p-session").filter({ hasText: "菜单操作验证" })).toHaveCount(0);

  await page.keyboard.press("Control+k");
  const search = page.getByRole("textbox", { name: "搜索会话" });
  await expect(search).toBeVisible();
  await search.fill("菜单操作验证");
  await expect(page.getByRole("button", { name: /菜单操作验证/ })).toHaveCount(0);
  await page.getByLabel("包含已归档").check();
  await expect(page.getByRole("button", { name: /菜单操作验证/ })).toContainText("已归档，点击恢复");
  await page.keyboard.press("Escape");
  await expect(search).toHaveCount(0);
  await page.keyboard.press("Control+k");
  await page.getByRole("textbox", { name: "搜索会话" }).fill("菜单操作验证");
  await page.getByRole("button", { name: /菜单操作验证/ }).click();
  await expect(page.getByRole("heading", { name: "菜单操作验证" })).toBeVisible();
  await page.getByRole("button", { name: "上一个会话" }).click();
  await expect(page.getByRole("heading", { name: "新会话", exact: true })).toBeVisible();
});

test("code copy keeps its text and streaming respects reading position", async ({ page, context }) => {
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  await openPrototype(page);
  await selectSession(page, "评测结果应该怎么看");
  const code = await page.locator(".p-code-block pre code").innerText();
  await page.getByRole("button", { name: "复制代码" }).click();
  await expect(page.getByRole("button", { name: "复制代码" })).toContainText("已复制");
  const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
  expect(clipboardText.replace(/\r\n/g, "\n")).toBe(code);

  await selectSession(page, "工具调用与超时处理");
  await page.getByRole("textbox", { name: "发送消息" }).fill("继续解释超时");
  await page.getByRole("button", { name: "发送消息", exact: true }).click();
  await expect(page.locator(".p-stream-cursor")).toBeVisible();
  const scroll = page.locator(".p-scroll");
  await scroll.evaluate(element => { element.scrollTop = 0; });
  await expect(page.getByRole("button", { name: "回到底部" })).toBeVisible();
  await page.waitForTimeout(180);
  expect(await scroll.evaluate(element => element.scrollTop)).toBe(0);
  await page.getByRole("button", { name: "回到底部" }).click();
  await expect.poll(() => scroll.evaluate(element => element.scrollHeight - element.clientHeight - element.scrollTop)).toBeLessThan(2);
  await page.getByRole("button", { name: "停止生成" }).click();
});

test("1280px conversation renders and reading font can be changed", async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 900 });
  await openPrototype(page);
  await selectSession(page, "评测结果应该怎么看");
  await expect(page.locator(".p-markdown")).toBeVisible();
  await expectNoHorizontalOverflow(page);
  await saveScreenshot(page, "1280-conversation.png");

  await page.getByRole("button", { name: "阅读设置" }).click();
  await page.locator(".p-font-options label").filter({ hasText: "16 px" }).click();
  await expect(page.getByRole("radio", { name: "16 px" })).toBeChecked();
  await expect(page.locator(".p-markdown")).toHaveCSS("font-size", "16px");
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).toHaveCount(0);
});

test("390px sidebar, session menu, conversation, and file preview fit viewport", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await openPrototype(page);
  await openSidebar(page);
  await expectNoHorizontalOverflow(page);
  await saveScreenshot(page, "390-sidebar.png");
  await closeMobileSidebar(page);

  await openSidebar(page);
  await selectSession(page, "工具调用与超时处理");
  await expect(page.locator(".p-markdown")).toBeVisible();
  await expectNoHorizontalOverflow(page);
  await saveScreenshot(page, "390-conversation.png");

  await openSidebar(page);
  const row = page.locator(".p-session").filter({ hasText: "工具调用与超时处理" });
  await row.getByRole("button", { name: "工具调用与超时处理 的操作" }).click();
  const menu = page.locator(".p-menu");
  await expect(menu).toBeVisible();
  const menuBox = await menu.boundingBox();
  expect(menuBox).not.toBeNull();
  expect(menuBox!.x).toBeGreaterThanOrEqual(0);
  expect(menuBox!.x + menuBox!.width).toBeLessThanOrEqual(390);
  await saveScreenshot(page, "390-session-menu.png");
  await page.keyboard.press("Escape");
  await closeMobileSidebar(page);

  await page.locator(".p-reference-files button").filter({ hasText: "timeout.py" }).click();
  const preview = page.locator('aside[aria-label="文件预览"]');
  await expect(preview).toBeVisible();
  const previewBox = await preview.boundingBox();
  expect(previewBox).not.toBeNull();
  expect(previewBox!.x).toBeGreaterThanOrEqual(0);
  expect(previewBox!.x + previewBox!.width).toBeLessThanOrEqual(390);
  await expectNoHorizontalOverflow(page);
  await saveScreenshot(page, "390-file-panel.png");
});
