import { chromium, expect, test, type Page, type TestInfo } from "@playwright/test";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const baseURL = "http://127.0.0.1:5182";
const expectedOuterSize = { width: 1440, height: 900 };
const defaultLayoutViewportWidth = 1410;

const zoomScenarios = [
  { percent: 100, factor: 1 },
  { percent: 125, factor: 1.25 },
  { percent: 150, factor: 1.5 },
  { percent: 80, factor: 0.8 },
] as const;

type Rectangle = { x: number; y: number; width: number; height: number; right: number; bottom: number };
type EmptyLayout = {
  viewport: {
    innerWidth: number;
    innerHeight: number;
    outerWidth: number;
    outerHeight: number;
    devicePixelRatio: number;
    screenWidth: number;
    screenHeight: number;
    visualViewport: { width: number; height: number; scale: number } | null;
  };
  document: { clientWidth: number; scrollWidth: number };
  app: { clientWidth: number; scrollWidth: number } | null;
  rectangles: Record<"topbar" | "sidebar" | "main" | "composer" | "markdown" | "preview", Rectangle | null>;
  sessionCount: number;
};

type BrowserObservations = {
  requestedZoom: { percent: number; factor: number; zoomLevel: number };
  launch: {
    channel: string;
    headless: boolean;
    viewport: null;
    deviceScaleFactor: "not set";
    forceDeviceScaleFactor: "not set";
    windowSizeArgument: string;
    preference: { partition: { default_zoom_level: Record<string, number> } };
  };
  userAgent: string | null;
  emptyLayout: EmptyLayout | null;
  conversationLayout: unknown;
  draftLayout: unknown;
  previewLayout: unknown;
  closedPreviewLayout: unknown;
  pageErrors: string[];
  apiRequests: string[];
  crossOriginRequests: string[];
  crossOriginWebSockets: string[];
};

async function measureLayout(page: Page): Promise<EmptyLayout> {
  return page.evaluate(() => {
    const rect = (selector: string): Rectangle | null => {
      const element = document.querySelector<HTMLElement>(selector);
      if (!element) return null;
      const bounds = element.getBoundingClientRect();
      return {
        x: bounds.x,
        y: bounds.y,
        width: bounds.width,
        height: bounds.height,
        right: bounds.right,
        bottom: bounds.bottom,
      };
    };
    const app = document.querySelector<HTMLElement>(".p-app");
    const visualViewport = window.visualViewport;
    return {
      viewport: {
        innerWidth,
        innerHeight,
        outerWidth,
        outerHeight,
        devicePixelRatio,
        screenWidth: screen.width,
        screenHeight: screen.height,
        visualViewport: visualViewport ? {
          width: visualViewport.width,
          height: visualViewport.height,
          scale: visualViewport.scale,
        } : null,
      },
      document: {
        clientWidth: document.documentElement.clientWidth,
        scrollWidth: document.documentElement.scrollWidth,
      },
      app: app ? { clientWidth: app.clientWidth, scrollWidth: app.scrollWidth } : null,
      rectangles: {
        topbar: rect(".p-topbar"),
        sidebar: rect(".p-sidebar"),
        main: rect(".p-main"),
        composer: rect(".p-composer"),
        markdown: rect(".p-markdown"),
        preview: rect(".p-preview"),
      },
      sessionCount: document.querySelectorAll(".p-session-button").length,
    };
  });
}

function expectNoHorizontalOverflow(layout: EmptyLayout, label: string) {
  expect(layout.document.scrollWidth, `${label}: document scrollWidth`).toBeLessThanOrEqual(layout.document.clientWidth);
  expect(layout.app?.scrollWidth, `${label}: app scrollWidth`).toBeLessThanOrEqual(layout.app?.clientWidth ?? 0);
}

function expectRectangleInsideViewport(rectangle: Rectangle | null, layout: EmptyLayout, label: string) {
  expect(rectangle, `${label}: rectangle exists`).not.toBeNull();
  const viewportWidth = layout.viewport.visualViewport?.width ?? layout.viewport.innerWidth;
  const viewportHeight = layout.viewport.visualViewport?.height ?? layout.viewport.innerHeight;
  expect(rectangle!.x, `${label}: left`).toBeGreaterThanOrEqual(-1);
  expect(rectangle!.y, `${label}: top`).toBeGreaterThanOrEqual(-1);
  expect(rectangle!.right, `${label}: right`).toBeLessThanOrEqual(viewportWidth + 1);
  expect(rectangle!.bottom, `${label}: bottom`).toBeLessThanOrEqual(viewportHeight + 1);
}

function expectHorizontalBoundsInsideViewport(rectangle: { x: number; right: number } | null, layout: EmptyLayout, label: string) {
  expect(rectangle, `${label}: horizontal bounds exist`).not.toBeNull();
  const viewportWidth = layout.viewport.visualViewport?.width ?? layout.viewport.innerWidth;
  expect(rectangle!.x, `${label}: left`).toBeGreaterThanOrEqual(-1);
  expect(rectangle!.right, `${label}: right`).toBeLessThanOrEqual(viewportWidth + 1);
}

function observePage(page: Page, observations: BrowserObservations) {
  page.on("pageerror", error => observations.pageErrors.push(error.message));
  page.on("request", request => {
    const url = new URL(request.url());
    if (url.origin !== baseURL) observations.crossOriginRequests.push(`${request.resourceType()} ${request.url()}`);
    if (request.resourceType() === "fetch" || request.resourceType() === "xhr") {
      observations.apiRequests.push(`${request.resourceType()} ${request.method()} ${request.url()}`);
    }
  });
  page.on("websocket", socket => {
    const url = new URL(socket.url());
    if (url.host !== new URL(baseURL).host) observations.crossOriginWebSockets.push(socket.url());
  });
}

async function verifyNativeZoom(percent: number, factor: number, testInfo: TestInfo) {
  const userDataDir = await mkdtemp(join(tmpdir(), "krim-native-zoom-"));
  const zoomLevel = Math.log(factor) / Math.log(1.2);
  const preference = { partition: { default_zoom_level: { x: zoomLevel } } };
  const preferencesPath = join(userDataDir, "Default", "Preferences");
  const observations: BrowserObservations = {
    requestedZoom: { percent, factor, zoomLevel },
    launch: {
      channel: "msedge",
      headless: true,
      viewport: null,
      deviceScaleFactor: "not set",
      forceDeviceScaleFactor: "not set",
      windowSizeArgument: "--window-size=1440,900",
      preference,
    },
    userAgent: null,
    emptyLayout: null,
    conversationLayout: null,
    draftLayout: null,
    previewLayout: null,
    closedPreviewLayout: null,
    pageErrors: [],
    apiRequests: [],
    crossOriginRequests: [],
    crossOriginWebSockets: [],
  };
  let context: Awaited<ReturnType<typeof chromium.launchPersistentContext>> | undefined;

  try {
    await mkdir(join(userDataDir, "Default"), { recursive: true });
    await writeFile(preferencesPath, JSON.stringify(preference));
    context = await chromium.launchPersistentContext(userDataDir, {
      channel: "msedge",
      headless: true,
      viewport: null,
      args: ["--window-size=1440,900"],
    });
    const page = context.pages()[0] ?? await context.newPage();
    observePage(page, observations);
    await page.goto(`${baseURL}/prototype.html`, { waitUntil: "domcontentloaded" });
    await expect(page.locator(".p-app")).toBeVisible();
    await expect(page.getByRole("heading", { name: "What's up next?" })).toBeVisible();
    await expect(page.getByRole("complementary", { name: "会话导航" })).toBeVisible();
    await expect(page.locator(".p-session-button")).toHaveCount(0);
    observations.userAgent = await page.evaluate(() => navigator.userAgent);

    const emptyLayout = await measureLayout(page);
    observations.emptyLayout = emptyLayout;
    expect(emptyLayout.viewport.outerWidth, `${percent}% outerWidth`).toBe(expectedOuterSize.width);
    expect(emptyLayout.viewport.outerHeight, `${percent}% outerHeight`).toBe(expectedOuterSize.height);
    expect(emptyLayout.viewport.devicePixelRatio, `${percent}% devicePixelRatio`).toBeCloseTo(factor, 2);
    expect(emptyLayout.viewport.innerWidth * factor, `${percent}% innerWidth follows browser zoom`).toBeCloseTo(defaultLayoutViewportWidth, 0);
    expect(emptyLayout.viewport.visualViewport, `${percent}% visualViewport exists`).not.toBeNull();
    expect(emptyLayout.viewport.visualViewport!.scale, `${percent}% visualViewport scale`).toBe(1);
    expect(emptyLayout.viewport.visualViewport!.width * factor, `${percent}% visualViewport width follows browser zoom`).toBeCloseTo(defaultLayoutViewportWidth, 0);
    expectRectangleInsideViewport(emptyLayout.rectangles.topbar, emptyLayout, `${percent}% empty topbar`);
    expectRectangleInsideViewport(emptyLayout.rectangles.sidebar, emptyLayout, `${percent}% empty sidebar`);
    expectRectangleInsideViewport(emptyLayout.rectangles.main, emptyLayout, `${percent}% empty main`);
    expectRectangleInsideViewport(emptyLayout.rectangles.composer, emptyLayout, `${percent}% empty composer`);
    expect(emptyLayout.rectangles.sidebar!.width, `${percent}% sidebar width`).toBeCloseTo(288, 0);
    expect(emptyLayout.rectangles.main!.x, `${percent}% main offset`).toBeCloseTo(304, 0);
    expectNoHorizontalOverflow(emptyLayout, `${percent}% empty state`);

    await page.getByRole("button", { name: "应用菜单" }).click();
    await page.getByRole("menuitem", { name: "打开示例" }).click();
    const example = page.locator(".p-session-button").filter({ hasText: "工具调用与超时处理" }).first();
    await expect(example).toBeVisible();
    await example.click();
    await expect(page.locator(".p-markdown").first()).toContainText("A tool timeout means the operation did not return before its time limit");
    const conversationLayout = await measureLayout(page);
    observations.conversationLayout = conversationLayout;
    const markdownBox = await page.locator(".p-markdown").first().boundingBox();
    const threadBox = await page.locator(".p-thread").boundingBox();
    expect(markdownBox, `${percent}% markdown bounds`).not.toBeNull();
    expect(threadBox, `${percent}% thread bounds`).not.toBeNull();
    expect(markdownBox!.x, `${percent}% markdown left`).toBeGreaterThanOrEqual(threadBox!.x - 1);
    expect(markdownBox!.x + markdownBox!.width, `${percent}% markdown right`).toBeLessThanOrEqual(threadBox!.x + threadBox!.width + 1);
    expectHorizontalBoundsInsideViewport({ x: markdownBox!.x, right: markdownBox!.x + markdownBox!.width }, conversationLayout, `${percent}% markdown`);
    expectNoHorizontalOverflow(conversationLayout, `${percent}% conversation`);

    await page.getByRole("button", { name: "权限模式" }).click();
    const permissionMenu = page.locator(".p-menu");
    await expect(permissionMenu.getByRole("menuitem", { name: /Ask permissions/ })).toBeVisible();
    const permissionMenuBox = await permissionMenu.boundingBox();
    const permissionLayout = await measureLayout(page);
    expectRectangleInsideViewport(permissionMenuBox ? {
      ...permissionMenuBox,
      right: permissionMenuBox.x + permissionMenuBox.width,
      bottom: permissionMenuBox.y + permissionMenuBox.height,
    } : null, permissionLayout, `${percent}% permission menu`);
    await page.keyboard.press("Escape");
    await expect(permissionMenu).toHaveCount(0);

    const prompt = page.getByRole("textbox", { name: "发送消息" });
    const draft = "请核对工具调用的超时处理，保留草稿并重新计算自动换行。".repeat(5);
    await prompt.fill(draft);
    await expect(prompt).toHaveValue(draft);
    const draftLayout = await measureLayout(page);
    observations.draftLayout = draftLayout;
    const promptBox = await prompt.boundingBox();
    expectRectangleInsideViewport(promptBox ? {
      ...promptBox,
      right: promptBox.x + promptBox.width,
      bottom: promptBox.y + promptBox.height,
    } : null, draftLayout, `${percent}% long draft input`);
    expectRectangleInsideViewport(draftLayout.rectangles.composer, draftLayout, `${percent}% long draft composer`);
    expectNoHorizontalOverflow(draftLayout, `${percent}% long draft`);

    await page.locator(".p-conversation-header").getByRole("button", { name: "打开文件预览" }).click();
    const preview = page.locator('aside[aria-label="文件预览"]');
    await expect(preview).toContainText("src/runtime/timeout.py");
    const previewBox = await preview.boundingBox();
    const previewLayout = await measureLayout(page);
    observations.previewLayout = previewLayout;
    expectRectangleInsideViewport(previewBox ? {
      ...previewBox,
      right: previewBox.x + previewBox.width,
      bottom: previewBox.y + previewBox.height,
    } : null, previewLayout, `${percent}% file preview`);
    expect(await prompt.inputValue(), `${percent}% draft survives opening preview`).toBe(draft);
    expectNoHorizontalOverflow(previewLayout, `${percent}% file preview open`);

    await preview.getByRole("button", { name: "关闭文件预览" }).click();
    await expect(preview).toHaveCount(0);
    await expect(prompt).toBeFocused();
    await expect(prompt).toHaveValue(draft);
    const closedPreviewLayout = await measureLayout(page);
    observations.closedPreviewLayout = closedPreviewLayout;
    expectRectangleInsideViewport(closedPreviewLayout.rectangles.composer, closedPreviewLayout, `${percent}% composer after closing preview`);
    expectNoHorizontalOverflow(closedPreviewLayout, `${percent}% file preview closed`);

    expect(observations.pageErrors, `${percent}% uncaught page errors`).toEqual([]);
    expect(observations.apiRequests, `${percent}% business API fetch/XHR requests`).toEqual([]);
    expect(observations.crossOriginRequests, `${percent}% cross-origin HTTP requests`).toEqual([]);
    expect(observations.crossOriginWebSockets, `${percent}% cross-origin WebSocket connections`).toEqual([]);
  } finally {
    try {
      await context?.close();
    } finally {
      try {
        await testInfo.attach("native-zoom-observations.json", {
          body: Buffer.from(JSON.stringify(observations, null, 2)),
          contentType: "application/json",
        });
      } finally {
        await rm(userDataDir, { recursive: true, force: true });
      }
    }
  }
}

for (const scenario of zoomScenarios) {
  test(`native browser zoom ${scenario.percent}% preserves prototype layout and interactions`, async ({}, testInfo) => {
    await verifyNativeZoom(scenario.percent, scenario.factor, testInfo);
  });
}
