import { mkdir } from "node:fs/promises";
import { resolve } from "node:path";
import { expect, test, type Page } from "@playwright/test";

const baseURL = "http://127.0.0.1:5182";
const screenshotDirectory = resolve(import.meta.dirname, "..", "..", ".cache", "ui-prototype", "linux-fidelity");

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

async function openExamples(page: Page) {
  await page.getByRole("button", { name: "应用菜单" }).click();
  await page.getByRole("menuitem", { name: "打开示例" }).click();
  await expect(page.locator(".p-session-button").filter({ hasText: "工具调用与超时处理" })).toBeVisible();
}

async function selectSession(page: Page, title: string) {
  const session = page.locator(".p-session-button").filter({ hasText: title }).first();
  if (!(await session.count())) await openExamples(page);
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

function observePage(page: Page) {
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
}

test.beforeAll(async () => {
  await mkdir(screenshotDirectory, { recursive: true });
});

test.beforeEach(async ({ page }) => {
  observePage(page);
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
  await expect(page.getByRole("heading", { name: "What's up next?" })).toBeVisible();
  await expect(page.getByRole("complementary", { name: "会话导航" })).toBeVisible();
  await expect(page.locator(".p-session-button")).toHaveCount(0);
  const prompt = page.getByRole("textbox", { name: "发送消息" });
  await expect(prompt).toBeVisible();
  const measurements = await prompt.evaluate(element => ({
    fontSize: getComputedStyle(element).fontSize,
    height: element.getBoundingClientRect().height,
    rows: (element as HTMLTextAreaElement).rows,
    composerHeight: element.closest(".p-composer")?.getBoundingClientRect().height ?? 0,
  }));
  expect(measurements.fontSize).toBe("14px");
  expect(measurements.rows).toBe(1);
  expect(measurements.height).toBeGreaterThanOrEqual(20);
  expect(measurements.height).toBeLessThanOrEqual(22);
  expect(measurements.composerHeight).toBe(46);
  await expectNoHorizontalOverflow(page);
  await saveScreenshot(page, "1440-new-session.png");
});

test("1280x900 and 390x844 empty layouts are captured", async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 900 });
  await openPrototype(page);
  await saveScreenshot(page, "1280-empty.png");
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.getByRole("textbox", { name: "发送消息" })).toBeVisible();
  await saveScreenshot(page, "390-empty.png");
});

test("global navigation stays fixed as the sidebar collapses", async ({ page }) => {
  await openPrototype(page);
  const search = page.getByRole("button", { name: "搜索会话" }).last();
  const before = await search.boundingBox();
  await page.getByRole("button", { name: "收起侧栏" }).click();
  await expect(page.locator(".p-main")).toHaveCSS("grid-column-start", "2");
  const after = await search.boundingBox();
  expect(before).not.toBeNull();
  expect(after).not.toBeNull();
  expect(after!.x).toBe(before!.x);
  const composer = await page.locator(".p-composer").boundingBox();
  expect(composer).not.toBeNull();
  expect(composer!.x + composer!.width / 2).toBeCloseTo(720, 0);
  await saveScreenshot(page, "1440-sidebar-collapsed.png");
  await page.getByRole("button", { name: "展开侧栏" }).click();
  await expect(page.locator(".p-sidebar")).toBeVisible();
});

test("permission and model menus expose the local choices", async ({ page }) => {
  await openPrototype(page);
  const modeButton = page.getByRole("button", { name: "权限模式" });
  await modeButton.click();
  const modeMenu = page.locator(".p-menu");
  await expect(modeMenu.getByRole("menuitem", { name: /Ask permissions/ })).toBeVisible();
  await expect(modeMenu.getByRole("menuitem", { name: /Accept edits/ })).toContainText("2");
  const modeBounds = await modeMenu.boundingBox();
  expect(modeBounds).not.toBeNull();
  expect(modeBounds!.x).toBeGreaterThanOrEqual(0);
  expect(modeBounds!.x + modeBounds!.width).toBeLessThanOrEqual(1440);
  await saveScreenshot(page, "1440-permission-menu.png");
  await modeMenu.getByRole("menuitem", { name: /Ask permissions/ }).click();
  await expect(modeButton).toContainText("Ask permissions");

  const modelButton = page.getByRole("button", { name: "选择模型（本地样例，不发起请求）" });
  await modelButton.click();
  const modelMenu = page.locator(".p-menu");
  await expect(modelMenu.getByRole("menuitem", { name: /GPT-6 Luna/ })).toBeVisible();
  await saveScreenshot(page, "1440-model-menu.png");
});

test("representative viewports and zoom-equivalent layouts stay aligned", async ({ browser }) => {
  const scenarios = [
    { name: "1280x800", width: 1280, height: 800, deviceScaleFactor: 1 },
    { name: "1440x900", width: 1440, height: 900, deviceScaleFactor: 1 },
    { name: "1920x1080", width: 1920, height: 1080, deviceScaleFactor: 1 },
    { name: "1280@80-equivalent", width: 1600, height: 1000, deviceScaleFactor: 0.8 },
    { name: "1280@150-equivalent", width: 853, height: 533, deviceScaleFactor: 1.5 },
    { name: "1920@125-equivalent", width: 1536, height: 864, deviceScaleFactor: 1.25 },
    { name: "390x844", width: 390, height: 844, deviceScaleFactor: 1 },
    { name: "768x1024", width: 768, height: 1024, deviceScaleFactor: 1 },
  ];
  for (const scenario of scenarios) {
    const context = await browser.newContext({
      baseURL,
      viewport: { width: scenario.width, height: scenario.height },
      deviceScaleFactor: scenario.deviceScaleFactor,
    });
    const page = await context.newPage();
    observePage(page);
    await openPrototype(page);
    await expectNoHorizontalOverflow(page);
    const geometry = await page.evaluate(() => {
      const rect = (selector: string) => {
        const element = document.querySelector<HTMLElement>(selector);
        const box = element?.getBoundingClientRect();
        return box ? { x: box.x, y: box.y, width: box.width, height: box.height, right: box.right, bottom: box.bottom } : null;
      };
      return {
        viewport: { width: innerWidth, height: innerHeight },
        topbar: rect(".p-topbar"), sidebar: rect(".p-sidebar"), main: rect(".p-main"), composer: rect(".p-composer"),
        documentWidth: document.documentElement.scrollWidth,
      };
    });
    expect(geometry.documentWidth, scenario.name).toBeLessThanOrEqual(scenario.width);
    expect(geometry.composer?.right, scenario.name).toBeLessThanOrEqual(scenario.width);
    expect(geometry.composer?.bottom, scenario.name).toBeLessThanOrEqual(scenario.height);
    if (scenario.width > 700) {
      expect(geometry.sidebar?.width, scenario.name).toBeCloseTo(288, 0);
      expect(geometry.main?.x, scenario.name).toBe(304);
      expect(Math.abs((geometry.composer!.x + geometry.composer!.width / 2) - (geometry.main!.x + geometry.main!.width / 2)), scenario.name).toBeLessThanOrEqual(1);
    } else {
      expect(geometry.main?.x, scenario.name).toBe(0);
      expect(geometry.composer?.x, scenario.name).toBeGreaterThan(0);
    }

    const prompt = page.getByRole("textbox", { name: "发送消息" });
    await prompt.focus();
    await expect(prompt).toBeFocused();
    await prompt.fill("本地布局检查");
    await expect(prompt).toHaveValue("本地布局检查");
    await prompt.fill("");

    await page.getByRole("button", { name: "权限模式" }).click();
    const modeMenu = page.locator(".p-menu");
    await expect(modeMenu.getByRole("menuitem", { name: /Ask permissions/ })).toBeVisible();
    const modeBounds = await modeMenu.boundingBox();
    expect(modeBounds, scenario.name).not.toBeNull();
    expect(modeBounds!.x, scenario.name).toBeGreaterThanOrEqual(0);
    expect(modeBounds!.x + modeBounds!.width, scenario.name).toBeLessThanOrEqual(scenario.width);
    await page.keyboard.press("Escape");

    await page.getByRole("button", { name: "选择模型（本地样例，不发起请求）" }).click();
    const modelMenu = page.locator(".p-menu");
    await expect(modelMenu.getByRole("menuitem", { name: /GPT-6 Luna/ })).toBeVisible();
    const modelBounds = await modelMenu.boundingBox();
    expect(modelBounds, scenario.name).not.toBeNull();
    expect(modelBounds!.x, scenario.name).toBeGreaterThanOrEqual(0);
    expect(modelBounds!.x + modelBounds!.width, scenario.name).toBeLessThanOrEqual(scenario.width);
    await page.keyboard.press("Escape");

    if (scenario.width === 390) {
      await openSidebar(page);
      const sidebar = page.locator(".p-sidebar");
      const sidebarBounds = await sidebar.boundingBox();
      expect(sidebarBounds).not.toBeNull();
      await expect.poll(() => sidebar.evaluate(element => element.getBoundingClientRect().x)).toBeGreaterThanOrEqual(0);
      const settledBounds = await sidebar.boundingBox();
      expect(settledBounds).not.toBeNull();
      expect(settledBounds!.x + settledBounds!.width).toBeLessThanOrEqual(scenario.width);
      await closeMobileSidebar(page);
    }
    const state = observations.get(page)!;
    expect(state.pageErrors, scenario.name).toEqual([]);
    expect(state.apiRequests, scenario.name).toEqual([]);
    expect(state.crossOriginRequests, scenario.name).toEqual([]);
    expect(state.crossOriginWebSockets, scenario.name).toEqual([]);
    await context.close();
  }
});

test("Markdown formatting and timeout.py preview open and close", async ({ page }, testInfo) => {
  await openPrototype(page);
  await selectSession(page, "工具调用与超时处理");
  await expect(page.locator(".p-markdown")).toContainText("A tool timeout means the operation did not return before its time limit");
  await expect(page.getByRole("region", { name: "表格" })).toBeVisible();
  await expect(page.locator(".p-markdown table th")).toHaveCount(2);
  await expect(page.locator(".p-markdown table tbody tr")).toHaveCount(3);
  await expect(page.locator(".p-code-block pre code")).toContainText("run_tool(timeout=30)");
  await expect(page.locator(".p-code-bar")).toHaveCount(0);
  const session = await page.context().newCDPSession(page);
  const renderedFonts: Record<string, string[]> = {};
  try {
    await session.send("DOM.enable");
    await session.send("CSS.enable");
    const { root } = await session.send("DOM.getDocument");
    for (const [label, selector] of Object.entries({ navigation: ".p-session-button span", body: ".p-markdown p", code: ".p-code-block pre code" })) {
      const { nodeId } = await session.send("DOM.querySelector", { nodeId: root.nodeId, selector });
      const { fonts } = await session.send("CSS.getPlatformFontsForNode", { nodeId });
      renderedFonts[label] = fonts.filter(font => font.glyphCount > 0).map(font => font.familyName);
      expect(renderedFonts[label]!.length).toBeGreaterThan(0);
      expect(renderedFonts[label]).not.toContain("Times New Roman");
      expect(renderedFonts[label]).not.toContain("SimSun");
      expect(renderedFonts[label]).not.toContain("Courier New");
    }
    await testInfo.attach("rendered-fonts.json", { body: Buffer.from(JSON.stringify(renderedFonts)), contentType: "application/json" });
  } finally {
    await session.detach();
  }
  const markdownGeometry = await page.locator(".p-markdown").evaluate(element => {
    const table = element.querySelector("table")!;
    const header = table.querySelector("th")!;
    const cell = table.querySelector("td")!;
    const codeBlock = element.querySelector(".p-code-block")!;
    const markdownStyle = getComputedStyle(element);
    const codeBox = codeBlock.getBoundingClientRect();
    return {
      fontSize: markdownStyle.fontSize,
      lineHeight: Number.parseFloat(markdownStyle.lineHeight),
      tableWidth: table.getBoundingClientRect().width,
      borderSpacing: getComputedStyle(table).borderSpacing,
      headerBackground: getComputedStyle(header).backgroundColor,
      cellBackground: getComputedStyle(cell).backgroundColor,
      codeWidth: codeBox.width,
      codeHeight: codeBox.height,
    };
  });
  expect(markdownGeometry.fontSize).toBe("14px");
  expect(markdownGeometry.lineHeight).toBeCloseTo(20, 0);
  expect(markdownGeometry.tableWidth).toBeCloseTo(960, 0);
  expect(markdownGeometry.borderSpacing).toBe("2px");
  expect(markdownGeometry.headerBackground).not.toBe(markdownGeometry.cellBackground);
  expect(markdownGeometry.codeWidth).toBeLessThan(360);
  expect(markdownGeometry.codeHeight).toBeGreaterThanOrEqual(110);
  expect(markdownGeometry.codeHeight).toBeLessThanOrEqual(125);
  const threadBox = await page.locator(".p-thread").boundingBox();
  const userBubble = await page.locator(".p-user-message").first().boundingBox();
  expect(threadBox).not.toBeNull();
  expect(userBubble).not.toBeNull();
  expect(userBubble!.width).toBeLessThanOrEqual(720);
  expect(Math.abs(userBubble!.x + userBubble!.width - (threadBox!.x + threadBox!.width))).toBeLessThanOrEqual(1);
  await expect(page.locator(".p-user-text")).toHaveCSS("font-size", "14px");
  await page.locator(".p-scroll").evaluate(element => { element.scrollTop = 0; });
  await saveScreenshot(page, "1440-markdown.png");

  await page.getByRole("button", { name: "打开文件预览" }).click();
  const preview = page.locator('aside[aria-label="文件预览"]');
  await expect(preview).toContainText("src/runtime/timeout.py");
  await expect(preview).toContainText("async def call_with_timeout");
  await saveScreenshot(page, "1440-file-panel.png");
  await preview.getByRole("button", { name: "关闭文件预览" }).click();
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
  await expect.poll(() => page.locator(".p-composer").evaluate(element => element.getBoundingClientRect().height)).toBeGreaterThan(46);
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

test("existing drafts reflow on resize, sidebar collapse, and file preview", async ({ page }) => {
  await openPrototype(page);
  const prompt = page.getByRole("textbox", { name: "发送消息" });
  const draft = "请核对工具调用的超时处理，保留草稿并重新计算自动换行。".repeat(5);
  await prompt.fill(draft);
  const initialHeight = await prompt.evaluate(element => element.clientHeight);
  await page.setViewportSize({ width: 768, height: 1024 });
  await expect.poll(() => prompt.evaluate(element => element.clientHeight)).toBeGreaterThan(initialHeight);
  await expect.poll(() => prompt.evaluate(element => element.scrollHeight - element.clientHeight)).toBeLessThanOrEqual(1);
  await expect(prompt).toHaveValue(draft);
  await page.setViewportSize({ width: 1440, height: 900 });
  await expect.poll(() => prompt.evaluate(element => element.clientHeight)).toBe(initialHeight);
  await page.getByRole("button", { name: "收起侧栏" }).click();
  await expect.poll(() => prompt.evaluate(element => element.scrollHeight - element.clientHeight)).toBeLessThanOrEqual(1);
  await page.getByRole("button", { name: "展开侧栏" }).click();
  await page.getByRole("button", { name: "打开文件预览" }).click();
  await expect.poll(() => prompt.evaluate(element => element.clientHeight)).toBeGreaterThan(initialHeight);
  await expect(prompt).toHaveValue(draft);
  await expectNoHorizontalOverflow(page);
});

test("tablet file preview overlays without squeezing the conversation", async ({ page }) => {
  await page.setViewportSize({ width: 768, height: 1024 });
  await openPrototype(page);
  await selectSession(page, "工具调用与超时处理");
  const chatWidth = await page.locator(".p-chat").evaluate(element => element.clientWidth);
  await page.getByRole("button", { name: "打开文件预览" }).click();
  const preview = page.locator(".p-preview");
  await expect(preview).toHaveCSS("position", "absolute");
  await expect(page.locator(".p-chat")).toHaveAttribute("inert", "");
  expect(await page.locator(".p-chat").evaluate(element => element.clientWidth)).toBe(chatWidth);
  const panel = await preview.boundingBox();
  expect(panel!.width).toBeGreaterThan(350);
  expect(panel!.x + panel!.width).toBeLessThanOrEqual(768);
  await saveScreenshot(page, "768-file-panel.png");
  await preview.getByRole("button", { name: "关闭文件预览" }).click();
  await expect(page.getByRole("textbox", { name: "发送消息" })).toBeFocused();
  await expectNoHorizontalOverflow(page);
});

test("short viewports keep long drafts and menus inside the window", async ({ page }) => {
  await page.setViewportSize({ width: 640, height: 360 });
  await openPrototype(page);
  await page.getByRole("textbox", { name: "发送消息" }).fill("多行草稿\n".repeat(30));
  const composer = await page.locator(".p-composer").boundingBox();
  expect(composer!.y).toBeGreaterThanOrEqual(36);
  expect(composer!.y + composer!.height).toBeLessThan(360);
  await page.getByRole("button", { name: "权限模式" }).click();
  const menu = await page.locator(".p-menu").boundingBox();
  expect(menu!.y).toBeGreaterThanOrEqual(0);
  expect(menu!.y + menu!.height).toBeLessThanOrEqual(360);
  await expectNoHorizontalOverflow(page);
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
  await openExamples(page);
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
  await expect(page.getByRole("heading", { name: "工具调用与超时处理" })).toBeVisible();
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
  await expect.poll(() => scroll.evaluate(element => element.scrollHeight - element.clientHeight)).toBeGreaterThan(48);
  await scroll.evaluate(element => { element.scrollTop = 0; element.dispatchEvent(new Event("scroll", { bubbles: true })); });
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

  await page.getByRole("button", { name: "自定义" }).click();
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
  await selectSession(page, "评测结果应该怎么看");
  await expect(page.locator(".p-markdown")).toBeVisible();
  const codeScroller = page.locator(".p-code-block pre");
  await expect.poll(() => codeScroller.evaluate(element => element.scrollWidth > element.clientWidth)).toBe(true);
  await expectNoHorizontalOverflow(page);
  await saveScreenshot(page, "390-conversation.png");

  await openSidebar(page);
  await selectSession(page, "工具调用与超时处理");
  const tableScroller = page.locator(".p-table-scroll");
  await expect.poll(() => tableScroller.evaluate(element => element.scrollWidth > element.clientWidth)).toBe(true);
  await expectNoHorizontalOverflow(page);

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

  await page.getByRole("button", { name: "打开文件预览" }).click();
  const preview = page.locator('aside[aria-label="文件预览"]');
  await expect(preview).toBeVisible();
  const previewBox = await preview.boundingBox();
  expect(previewBox).not.toBeNull();
  expect(previewBox!.x).toBeGreaterThanOrEqual(0);
  expect(previewBox!.x + previewBox!.width).toBeLessThanOrEqual(390);
  await expectNoHorizontalOverflow(page);
  await saveScreenshot(page, "390-file-panel.png");
});
