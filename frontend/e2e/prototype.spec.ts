import { mkdir } from "node:fs/promises";
import { resolve } from "node:path";
import { expect, test, type Page } from "@playwright/test";

const baseURL = "http://127.0.0.1:5182";
const screenshotDirectory = resolve(import.meta.dirname, "..", "..", ".cache", "ui-prototype", "typography-20261103");

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

async function waitForPrototypeFonts(page: Page) {
  await page.evaluate(async () => {
    await document.fonts.ready;
    await Promise.all([
      document.fonts.load('14px "Geist Variable"', "Krim Agent Latin"),
      document.fonts.load('14px "Noto Sans SC Variable"', "中文字体检查"),
      document.fonts.load('14px "Geist Mono Variable"', "const value = 1;"),
    ]);
    await document.fonts.ready;
  });
}

async function getRenderedFontFamilies(page: Page, selector: string) {
  const session = await page.context().newCDPSession(page);
  try {
    await session.send("DOM.enable");
    await session.send("CSS.enable");
    const { root } = await session.send("DOM.getDocument");
    const { nodeId } = await session.send("DOM.querySelector", { nodeId: root.nodeId, selector });
    const { fonts } = await session.send("CSS.getPlatformFontsForNode", { nodeId });
    return fonts.filter(font => font.glyphCount > 0).map(font => font.familyName);
  } finally {
    await session.detach();
  }
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
  await waitForPrototypeFonts(page);
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
    await waitForPrototypeFonts(page);
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
  await expect(page.locator(".message-markdown")).toContainText("A tool timeout means the operation did not return before its time limit");
  await expect(page.getByRole("region", { name: "表格" })).toBeVisible();
  await expect(page.locator(".message-markdown table th")).toHaveCount(2);
  await expect(page.locator(".message-markdown table tbody tr")).toHaveCount(3);
  await expect(page.locator(".message-code-block pre code")).toContainText("run_tool(timeout=30)");
  await expect(page.locator(".message-code-block pre")).toHaveAttribute("tabindex", "0");
  await expect(page.locator(".message-code-block pre")).toHaveAttribute("aria-label", "代码片段");
  await expect(page.locator(".p-code-bar")).toHaveCount(0);
  await waitForPrototypeFonts(page);
  const session = await page.context().newCDPSession(page);
  const renderedFonts: Record<string, string[]> = {};
  try {
    await session.send("DOM.enable");
    await session.send("CSS.enable");
    const { root } = await session.send("DOM.getDocument");
    for (const [label, selector] of Object.entries({ navigation: ".p-session-button span", body: ".message-markdown p", code: ".message-code-block pre code" })) {
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
  expect(renderedFonts.body).toContain("Geist");
  expect(renderedFonts.code).toContain("Geist Mono");
  const markdownGeometry = await page.locator(".message-markdown").evaluate(element => {
    const table = element.querySelector("table")!;
    const header = table.querySelector("th")!;
    const cell = table.querySelector("td")!;
    const codeBlock = element.querySelector(".message-code-block")!;
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

test("complex Markdown keeps safe structure and copies fixed code text", async ({ page, context }, testInfo) => {
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  await page.setViewportSize({ width: 1440, height: 900 });
  await openPrototype(page);
  await openExamples(page);
  await selectSession(page, "请求超时后的安全重试");
  const markdown = page.locator(".message-markdown");
  await expect(markdown.locator("h1")).toHaveText("请求超时后的安全重试");
  await expect(markdown.locator("ol > li > ul > li")).toHaveCount(2);
  await expect(markdown.locator("blockquote blockquote")).toHaveCount(1);
  await expect(markdown.locator("table th").nth(0)).toHaveCSS("text-align", "left");
  await expect(markdown.locator("table th").nth(1)).toHaveCSS("text-align", "center");
  await expect(markdown.locator("table th").nth(2)).toHaveCSS("text-align", "right");
  await expect(markdown.locator("img, script")).toHaveCount(0);
  const htmlExecution = await page.evaluate(() => ({
    image: (window as Window & { __markdownHtmlRan?: boolean }).__markdownHtmlRan,
    script: (window as Window & { __markdownScriptRan?: boolean }).__markdownScriptRan,
  }));
  expect(htmlExecution).toEqual({ image: undefined, script: undefined });
  const requestId = "req_20261103_7f2a9c0d4e138b6a90c4e51f83d2a7b6f0c19e48a6d2f5b1c7039a4d8e62f0c1b9a5d3e7f6142c8b0a6d9e3f1c5b7a2d8e4f690c1a3b5d7e9f20468c1a5b3d7e9f0124";
  const longLink = markdown.locator("a").filter({ hasText: requestId });
  await expect(longLink).toHaveText(requestId);
  const longHref = await longLink.getAttribute("href");
  expect(longHref).toBe(`/prototype.html?request_id=${requestId}`);
  expect(new URL(longHref!, baseURL).origin).toBe(new URL(baseURL).origin);
  const legacyText = markdown.getByText("旧版接口说明", { exact: true });
  await expect(legacyText).toHaveCount(1);
  expect(await legacyText.evaluate(element => element.tagName.toLowerCase())).toBe("span");
  expect(await legacyText.evaluate(element => element.closest("a"))).toBeNull();
  await expect(markdown.locator("a").filter({ hasText: "旧版接口说明" })).toHaveCount(0);
  await page.locator(".p-scroll").evaluate(element => { element.scrollTop = 0; });
  await saveScreenshot(page, "markdown-top-1440.png");
  await page.locator(".p-scroll").evaluate(element => { element.scrollTop = element.scrollHeight; });
  await saveScreenshot(page, "markdown-precision-1440.png");

  await page.setViewportSize({ width: 390, height: 844 });
  await expectNoHorizontalOverflow(page);
  const longLinkBounds = await longLink.boundingBox();
  expect(longLinkBounds).not.toBeNull();
  expect(longLinkBounds!.height).toBeGreaterThan(20);
  expect(longLinkBounds!.x + longLinkBounds!.width).toBeLessThanOrEqual(390);
  await saveScreenshot(page, "markdown-precision-390.png");

  const codeBlocks = page.locator(".message-code-block");
  await expect(codeBlocks).toHaveCount(2);
  const unknownCode = codeBlocks.nth(0).locator("pre code");
  await expect(unknownCode).toHaveAttribute("class", /language-request-log-unregistered/);
  await expect(unknownCode.locator("span[class*='hljs-']")).toHaveCount(0);
  const unclosedCode = codeBlocks.nth(1).locator("pre code");
  await expect(unclosedCode).toContainText(`2026-11-03T10:14:33Z request_id=${requestId} retry_start=accepted`);
  await expect(unclosedCode).not.toHaveAttribute("class", /language-/);
  await waitForPrototypeFonts(page);
  expect(await getRenderedFontFamilies(page, ".message-markdown p")).toContain("Noto Sans SC Thin");
  expect(await getRenderedFontFamilies(page, ".message-code-block pre code")).toContain("Geist Mono");
  await testInfo.attach("complex-markdown-fonts.json", {
    body: Buffer.from(JSON.stringify({
      paragraph: await getRenderedFontFamilies(page, ".message-markdown p"),
      code: await getRenderedFontFamilies(page, ".message-code-block pre code"),
    })),
    contentType: "application/json",
  });

  const expectedUnknownSource = [
    `    GET /v1/runs/trace?request_id="${requestId}" status=504 message="<gateway timeout>" retries=1 & source="api"`,
    `\tattempt=2 outcome="confirmed-not-applied" elapsed_ms=281`,
    `\tresponse="Retry-After: 2s"; note="same request id"`,
    "",
  ].join("\n");
  const expectedUnclosedSource = [
    `2026-11-03T10:14:31Z request_id=${requestId} retry_check=status_not_applied`,
    `2026-11-03T10:14:33Z request_id=${requestId} retry_start=accepted`,
    "",
  ].join("\n");
  const normalizeCrlf = (value: string) => value.replace(/\r\n/g, "\n");
  const copyButtons = page.getByRole("button", { name: "复制代码" });
  await copyButtons.nth(0).click();
  await expect(page.locator(".p-toast")).toContainText("已复制");
  expect(normalizeCrlf(await page.evaluate(() => navigator.clipboard.readText()))).toBe(normalizeCrlf(expectedUnknownSource));
  await copyButtons.nth(1).click();
  await expect(page.locator(".p-toast")).toContainText("已复制");
  expect(normalizeCrlf(await page.evaluate(() => navigator.clipboard.readText()))).toBe(normalizeCrlf(expectedUnclosedSource));
});

test("stopping a partial code fence leaves it readable and copyable", async ({ page, context }) => {
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  await openPrototype(page);
  const prompt = page.getByRole("textbox", { name: "发送消息" });
  await prompt.fill("Explain a safe tool timeout retry in detail.");
  await prompt.press("Enter");
  const codeBlock = page.locator(".message-code-block").first();
  const code = codeBlock.locator("pre code");
  await expect(codeBlock).toBeVisible({ timeout: 10_000 });
  await expect(code).toContainText("try:", { timeout: 10_000 });
  await page.getByRole("button", { name: "停止生成" }).click();
  await expect(page.locator(".p-stopped")).toContainText("已停止");
  const partialSource = await code.textContent();
  expect(partialSource).toContain("try:");
  await codeBlock.getByRole("button", { name: "复制代码" }).click();
  await expect(page.locator(".p-toast")).toContainText("已复制");
  const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
  expect(clipboardText.replace(/\r\n/g, "\n")).toBe(partialSource!.replace(/\r\n/g, "\n"));
});

test("code copy failure stays visible and is announced", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await openPrototype(page);
  await openExamples(page);
  await selectSession(page, "请求超时后的安全重试");
  await page.evaluate(() => {
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText: () => Promise.reject(new DOMException("Clipboard denied", "NotAllowedError")) },
    });
  });
  const button = page.getByRole("button", { name: "复制代码" }).first();
  await button.click();
  await expect(page.locator(".p-toast")).toBeVisible();
  await expect(page.locator(".p-toast")).toContainText("复制失败，请选择文字后复制");
  await expect(page.locator(".p-toast")).toHaveAttribute("data-tone", "error");
  await expect(page.locator(".p-toast")).toHaveAttribute("aria-live", "polite");
  await expect(button).toHaveAttribute("aria-label", "复制代码");
  await expect(button).toHaveAttribute("title", "复制失败");
  await expect(button.locator("svg.lucide-check")).toHaveCount(0);
  await expect(page.locator(".p-toast")).toHaveCSS("opacity", "1");
  await saveScreenshot(page, "copy-failed-1440.png");
});

test("code copy works with Enter, Space, and a narrow touch target", async ({ browser, context, page }) => {
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  await openPrototype(page);
  await openExamples(page);
  await selectSession(page, "请求超时后的安全重试");
  const copyButtons = page.getByRole("button", { name: "复制代码" });
  await copyButtons.nth(0).focus();
  await page.keyboard.press("Enter");
  await expect(page.locator(".p-toast")).toContainText("已复制");
  await copyButtons.nth(1).focus();
  await page.keyboard.press("Space");
  await expect(page.locator(".p-toast")).toContainText("已复制");

  const mobileContext = await browser.newContext({
    baseURL,
    viewport: { width: 390, height: 844 },
    hasTouch: true,
    isMobile: true,
  });
  const mobilePage = await mobileContext.newPage();
  observePage(mobilePage);
  try {
    await mobileContext.grantPermissions(["clipboard-read", "clipboard-write"]);
    await openPrototype(mobilePage);
    await openSidebar(mobilePage);
    await openExamples(mobilePage);
    await selectSession(mobilePage, "请求超时后的安全重试");
    const touchButton = mobilePage.getByRole("button", { name: "复制代码" }).first();
    const bounds = await touchButton.boundingBox();
    expect(bounds).not.toBeNull();
    expect(bounds!.width).toBeGreaterThanOrEqual(40);
    expect(bounds!.height).toBeGreaterThanOrEqual(40);
    const scrollport = mobilePage.locator(".message-code-block pre").first();
    const scrollBounds = await scrollport.boundingBox();
    expect(scrollBounds).not.toBeNull();
    expect(scrollBounds!.x + scrollBounds!.width).toBeLessThanOrEqual(bounds!.x);
    expect(await scrollport.evaluate(element => element.scrollWidth)).toBeGreaterThan(scrollBounds!.width);
    await touchButton.tap();
    await expect(mobilePage.locator(".p-toast")).toContainText("已复制");
    const state = observations.get(mobilePage)!;
    expect(state.pageErrors).toEqual([]);
    expect(state.apiRequests).toEqual([]);
    expect(state.crossOriginRequests).toEqual([]);
    expect(state.crossOriginWebSockets).toEqual([]);
  } finally {
    await mobileContext.close();
  }
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
  await waitForPrototypeFonts(page);
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
  await waitForPrototypeFonts(page);
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
  await waitForPrototypeFonts(page);
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
  await waitForPrototypeFonts(page);
  await prompt.press("Enter");
  await expect(page.locator(".p-stream-cursor")).toBeVisible();
  await page.getByRole("button", { name: "停止生成" }).click();
  await expect(page.locator(".p-stopped")).toContainText("已停止");
  const stoppedText = await page.locator(".message-markdown").innerText();
  await page.waitForTimeout(300);
  await expect(page.locator(".message-markdown")).toHaveText(stoppedText);
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
  const code = await page.locator(".message-code-block pre code").innerText();
  await page.getByRole("button", { name: "复制代码" }).click();
  await expect(page.getByRole("button", { name: "复制代码" })).toContainText("已复制");
  const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
  expect(clipboardText.replace(/\r\n/g, "\n")).toBe(code);

  await selectSession(page, "工具调用与超时处理");
  await page.getByRole("textbox", { name: "发送消息" }).fill("继续解释超时");
  await waitForPrototypeFonts(page);
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
  await expect(page.locator(".message-markdown")).toBeVisible();
  await expectNoHorizontalOverflow(page);
  await saveScreenshot(page, "1280-conversation.png");

  await page.getByRole("button", { name: "自定义" }).click();
  await page.locator(".p-font-options label").filter({ hasText: "16 px" }).click();
  await expect(page.getByRole("radio", { name: "16 px" })).toBeChecked();
  await expect(page.locator(".message-markdown")).toHaveCSS("font-size", "16px");
  await expect(page.locator(".message-code-block pre code")).toHaveCSS("font-size", "15px");
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await selectSession(page, "工具调用与超时处理");
  await expect(page.locator(".message-markdown table")).toHaveCSS("font-size", "16px");
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
  await expect(page.locator(".message-markdown")).toBeVisible();
  const codeScroller = page.locator(".message-code-block pre");
  await expect.poll(() => codeScroller.evaluate(element => element.scrollWidth > element.clientWidth)).toBe(true);
  await expectNoHorizontalOverflow(page);
  await saveScreenshot(page, "390-conversation.png");

  await openSidebar(page);
  await selectSession(page, "工具调用与超时处理");
  const tableScroller = page.locator(".message-table-scroll");
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
