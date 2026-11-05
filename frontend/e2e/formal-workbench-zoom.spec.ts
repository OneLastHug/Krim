import { chromium, expect, test, type Page, type TestInfo } from "@playwright/test";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { resolve, join } from "node:path";

const baseURL = process.env.KRIM_FORMAL_BASE_URL ?? "http://127.0.0.1:8879";
const origin = new URL(baseURL).origin;
const artifactRoot = resolve(import.meta.dirname, "..", "..", ".cache", "formal-workbench", "e2e");
const ADMIN_KEY = "formal-workbench-admin-browser-key";
const expectedOuterSize = { width: 1440, height: 900 };
const defaultLayoutViewportWidth = 1410;
const zoomScenarios = [
  { percent: 80, factor: 0.8 },
  { percent: 100, factor: 1 },
  { percent: 125, factor: 1.25 },
  { percent: 150, factor: 1.5 },
] as const;

type BrowserObservations = {
  requestedZoom: { percent: number; factor: number; zoomLevel: number };
  preference: { partition: { default_zoom_level: Record<string, number> } };
  viewport: Record<string, number | null> | null;
  pageErrors: string[];
  crossOriginRequests: string[];
};

function observePage(page: Page, observations: BrowserObservations) {
  page.on("pageerror", error => observations.pageErrors.push(error.message));
  page.on("request", request => {
    const url = new URL(request.url());
    if (url.origin !== origin) observations.crossOriginRequests.push(`${request.resourceType()} ${request.url()}`);
  });
}

async function runNativeZoom(percent: number, factor: number, testInfo: TestInfo) {
  const profileRoot = resolve(artifactRoot, "native-zoom-profiles");
  await mkdir(profileRoot, { recursive: true });
  const userDataDir = await mkdtemp(join(profileRoot, `zoom-${percent}-`));
  const zoomLevel = Math.log(factor) / Math.log(1.2);
  const preference = { partition: { default_zoom_level: { x: zoomLevel } } };
  const preferencePath = join(userDataDir, "Default", "Preferences");
  const observations: BrowserObservations = {
    requestedZoom: { percent, factor, zoomLevel },
    preference,
    viewport: null,
    pageErrors: [],
    crossOriginRequests: [],
  };
  let context: Awaited<ReturnType<typeof chromium.launchPersistentContext>> | undefined;

  try {
    await mkdir(join(userDataDir, "Default"), { recursive: true });
    await writeFile(preferencePath, JSON.stringify(preference), "utf-8");
    context = await chromium.launchPersistentContext(userDataDir, {
      channel: "msedge",
      headless: true,
      viewport: null,
      args: ["--window-size=1440,900"],
    });
    const page = context.pages()[0] ?? await context.newPage();
    observePage(page, observations);
    await page.goto("/ui/", { waitUntil: "domcontentloaded" });
    await expect(page.locator(".workbench-shell")).toBeVisible();
    const viewport = await page.evaluate(() => ({
      innerWidth,
      innerHeight,
      outerWidth,
      outerHeight,
      devicePixelRatio,
      visualViewportWidth: window.visualViewport?.width ?? null,
      visualViewportHeight: window.visualViewport?.height ?? null,
      visualViewportScale: window.visualViewport?.scale ?? null,
    }));
    observations.viewport = viewport;
    expect(viewport.outerWidth, `${percent}% native outer width`).toBe(expectedOuterSize.width);
    expect(viewport.outerHeight, `${percent}% native outer height`).toBe(expectedOuterSize.height);
    expect(viewport.devicePixelRatio, `${percent}% native zoom factor`).toBeCloseTo(factor, 2);
    expect(viewport.innerWidth * factor, `${percent}% layout viewport width`).toBeCloseTo(defaultLayoutViewportWidth, 0);
    expect(viewport.visualViewportScale).toBe(1);

    await page.getByRole("button", { name: "密钥身份" }).click();
    await page.getByRole("menuitem", { name: "连接 / 切换密钥" }).click();
    await page.locator("#api-key").fill(ADMIN_KEY);
    await page.locator("#connect-save").click();
    await expect(page.locator(".sidebar-account .account-sub")).toContainText("已连接");
    await page.locator(".sidebar-buttons").getByRole("button", { name: "新建对话" }).click();
    const title = `formal-zoom-${percent}-${Date.now()}`;
    await page.locator("#session-title").fill(title);
    await page.getByRole("button", { name: "创建对话" }).click();
    const prompt = page.locator("#chat-prompt");
    await expect(prompt).toBeVisible();
    await expect(prompt).toBeEnabled();

    const before = await page.evaluate(async key => {
      const response = await fetch("/test/tool-counts", { headers: { Authorization: `Bearer ${key}` } });
      return (await response.json() as { executions: unknown[] }).executions.length;
    }, ADMIN_KEY);
    await prompt.fill("/note welcome");
    await prompt.press("Enter");
    const approval = page.locator(".chat-approval");
    await expect(approval).toBeVisible();
    const draft = `zoom ${percent}% 输入保持可编辑`;
    await prompt.fill(draft);
    await expect(prompt).toHaveValue(draft);
    const promptRect = await prompt.boundingBox();
    const approvalRect = await approval.boundingBox();
    expect(promptRect).not.toBeNull();
    expect(approvalRect).not.toBeNull();
    expect(promptRect!.x).toBeGreaterThanOrEqual(-1);
    expect(promptRect!.x + promptRect!.width).toBeLessThanOrEqual(viewport.innerWidth + 1);
    expect(promptRect!.y + promptRect!.height).toBeLessThanOrEqual(viewport.innerHeight + 1);
    expect(approvalRect!.x).toBeGreaterThanOrEqual(-1);
    expect(approvalRect!.x + approvalRect!.width).toBeLessThanOrEqual(viewport.innerWidth + 1);

    await approval.getByRole("button", { name: "允许 lookup_note 本次调用" }).click();
    await expect.poll(async () => page.evaluate(async key => {
      const response = await fetch("/test/tool-counts", { headers: { Authorization: `Bearer ${key}` } });
      return (await response.json() as { executions: unknown[] }).executions.length;
    }, ADMIN_KEY), { timeout: 10_000 }).toBe(before + 1);
    await expect(prompt).toHaveValue(draft);
    expect(observations.pageErrors, `${percent}% uncaught errors`).toEqual([]);
    expect(observations.crossOriginRequests, `${percent}% cross-origin requests`).toEqual([]);
  } finally {
    try {
      await context?.close();
    } finally {
      try {
        await testInfo.attach("formal-native-zoom-observations.json", {
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
  test(`正式工作台原生页面缩放 ${scenario.percent}% 保持会话、审批与输入可用`, async ({}, testInfo) => {
    await runNativeZoom(scenario.percent, scenario.factor, testInfo);
  });
}
