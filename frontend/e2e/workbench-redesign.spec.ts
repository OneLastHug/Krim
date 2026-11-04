import { expect, test, type Page } from "@playwright/test";
import { attachJsonEvidence, screenshotStep } from "./step-screenshot.js";

type Theme = "warm" | "bright" | "dark";
const themeLabels: Record<Theme, string> = { warm: "暖纸", bright: "浅色", dark: "深色" };
async function selectTheme(page: Page, theme: Theme) {
  await page.getByRole("button", { name: "选择显示主题", exact: true }).click();
  await page.locator(".theme-options").getByRole("button", { name: new RegExp("^" + themeLabels[theme]) }).click();
  await expect(page.locator("html")).toHaveAttribute("data-theme", theme);
}
async function openNavigation(page: Page) {
  const menu = page.getByRole("button", { name: "打开导航", exact: true });
  if (await menu.isVisible()) {
    await menu.click();
    await expect(page.locator(".mobile-drawer-content .primary-navigation")).toBeVisible();
  }
}
for (const theme of ["warm", "bright", "dark"] as const) {
  for (const viewport of [
    { name: "桌面", slug: "desktop", width: 1440, height: 900, mobile: false, title: 0 },
    { name: "手机", slug: "mobile", width: 390, height: 844, mobile: true, title: 1 },
    { name: "平板", slug: "tablet", width: 1024, height: 768, mobile: false, title: 2 },
    { name: "小屏手机", slug: "small-mobile", width: 375, height: 812, mobile: true, title: 3 },
  ]) {
    const { mobile } = viewport;
    test(`P1：${themeLabels[theme]}${viewport.name}对话与独立Trace`, async ({ page }, info) => {
      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      const caseName = `P1-三主题-${theme}-${viewport.slug}`;
      const step = (stepName: string, action: () => Promise<void>) => screenshotStep(page, info, { caseName, stepName, theme }, action);
      const titles: Record<Theme, string[]> = { warm: ["订单金额核对", "发票金额核对", "采购金额核对", "报销金额核对"], bright: ["收款金额核对", "货款金额核对", "结算金额核对", "退款金额核对"], dark: ["账单金额核对", "运费金额核对", "对账金额核对", "付款金额核对"] };
      const title = titles[theme][viewport.title]!;
      await step("选择外观并核对单一侧栏", async () => {
        await page.goto("/ui/");
        await selectTheme(page, theme);
        expect(await page.locator(".desktop-sidebar").count()).toBe(mobile ? 0 : 1);
        expect(await page.locator(".chat-main .chat-heading").count()).toBe(0);
      });
      await step("新建会话后返回消息区", async () => {
        await openNavigation(page);
        await page.getByRole("button", { name: "新建对话", exact: true }).click();
        await page.locator("#session-title").fill(title);
        await page.getByRole("dialog", { name: "新建对话", exact: true }).getByRole("button", { name: "创建对话", exact: true }).click();
        await expect(page.locator("#chat-prompt")).toBeEnabled();
        await expect(page.locator(".topbar-title h1")).toHaveText(title);
        await expect(page.getByRole("dialog", { name: "主导航与对话", exact: true })).not.toBeVisible();
      });
      await step("完成一次实际工具任务而不显示评测内容", async () => {
        await page.locator("#chat-prompt").fill("/add 125 75");
        await page.locator(".chat-compose-actions").getByRole("button", { name: "发送", exact: true }).click();
        await expect(page.locator(".chat-answer").last()).toHaveText("200");
        await expect(page.locator(".chat-transcript").getByRole("button", { name: "评测此结果", exact: true })).toHaveCount(0);
        await expect(page.locator(".chat-transcript .mono")).toHaveCount(0);
        await expect(page.locator(".chat-transcript")).not.toContainText("工具与 Trace");
        const composer = await page.locator(".chat-composer").boundingBox();
        expect(composer).toBeTruthy();
        expect(composer!.y).toBeGreaterThanOrEqual(0);
        expect(composer!.y + composer!.height).toBeLessThanOrEqual(viewport.height);
        expect(composer!.y + composer!.height).toBeGreaterThan(viewport.height - 50);
        expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
      });
      await step("刷新保留主题和原会话", async () => {
        await page.reload();
        await expect(page.locator("html")).toHaveAttribute("data-theme", theme);
        await expect(page.locator(".topbar-title h1")).toHaveText(title);
        await expect(page.locator(".chat-answer").last()).toHaveText("200");
      });
      await step("按需展开工具摘要", async () => {
        const turn = page.locator(".chat-turn").last();
        const tool = turn.locator(".chat-tool").first();
        await expect(tool).toContainText("add");
        await tool.locator("summary").click();
        await expect(tool.locator(".chat-tool-section pre").first()).toContainText("left");
        await turn.getByRole("button", { name: "更多操作", exact: true }).click();
        await expect(turn.locator(".chat-detail")).toBeVisible();
      });
      await step("进入独立Trace读取原运行", async () => {
        await page.getByRole("button", { name: "查看 Trace", exact: true }).click();
        await expect(page.locator(".topbar-title h1")).toHaveText("Trace");
        await expect(page.locator(".trace-detail-heading h2")).toHaveText(title);
        await expect(page.locator(".trace-span-node").first()).toBeVisible();
        const url = new URL(page.url());
        const query = new URLSearchParams({ limit: "20", session_id: url.searchParams.get("session")!, turn_id: url.searchParams.get("turn")! });
        const response = await page.request.get(`/v2/workspaces/local/traces?${query}`);
        expect(response.ok()).toBe(true);
        const result = await response.json();
        expect(result.items).toHaveLength(1);
        expect(result.items[0].status).toBe("completed");
        await attachJsonEvidence(info, "精确轮次Trace索引", result);
        expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
      });
      await step("只读返回原会话", async () => {
        const methods: string[] = [];
        const observe = (request: import("@playwright/test").Request) => { if (/\/v[1-4]\//.test(request.url())) methods.push(request.method()); };
        page.on("request", observe);
        await page.getByRole("button", { name: "返回对话", exact: true }).click();
        await expect(page.locator(".topbar-title h1")).toHaveText(title);
        await expect(page.locator(".chat-answer").last()).toHaveText("200");
        expect(methods.every(method => method === "GET")).toBe(true);
        page.off("request", observe);
      });
    });
  }
}
