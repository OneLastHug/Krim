import { expect, test } from "@playwright/test";
import { attachJsonEvidence, screenshotStep } from "./step-screenshot.js";

async function selectTheme(page: import("@playwright/test").Page, theme: "warm" | "bright" | "dark") {
  const labels = { warm: "暖纸", bright: "浅色", dark: "深色" };
  await page.getByRole("button", { name: "选择显示主题", exact: true }).click();
  await page.locator(".theme-options").getByRole("button", { name: new RegExp("^" + labels[theme]) }).click();
  await expect(page.locator("html")).toHaveAttribute("data-theme", theme);
}

// 阶段用例使用真实本机服务，不拦截或替换业务接口。
test("P0：真实任务、工具事实和输入拒绝", async ({ page }, info) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  const step = (name: string, action: () => Promise<void>) => screenshotStep(page, info,
    { caseName: "P0-真实运行", stepName: name, theme: "warm" }, action);
  await step("打开运行入口", async () => {
    await page.goto("/ui/?view=run");
    await expect(page.locator("#prompt")).toBeVisible();
    await expect(page.getByRole("heading", { name: "运行记录", level: 1 })).toBeVisible();
  });
  await step("提交加法任务并等待完成", async () => {
    await page.locator("#prompt").fill("/add 2 3");
    await page.locator("#send").click();
    await expect(page.locator("#status")).toHaveText("已完成");
    await expect(page.locator("#result")).toHaveText("5");
  });
  await step("核对真实持久结果", async () => {
    const id = await page.locator("#run-id").textContent();
    expect(id).toBeTruthy();
    const response = await page.request.get(`/v1/runs/${encodeURIComponent(id!)}/result`);
    expect(response.ok()).toBe(true);
    const facts = await response.json();
    expect(facts.run.status).toBe("completed");
    expect(facts.text).toBe("5");
    expect(facts.tools.some((tool: { name: string; result_available: boolean }) => tool.name === "add" && tool.result_available)).toBe(true);
    await attachJsonEvidence(info, "持久运行结果.json", facts);
  });
  await step("展开工具执行结果", async () => {
    await page.getByRole("tab", { name: "工具", exact: true }).click();
    await expect(page.locator("#tools")).toContainText("add");
    await expect(page.locator("#tools")).toContainText("已完成");
  });
  await step("空输入被明确拒绝", async () => {
    await page.locator("#prompt").fill("");
    await page.locator("#send").click();
    await expect(page.locator(".composer .notice")).toContainText("任务内容无效");
  });
});

test("P1：桌面对话、详情与明暗主题持久化", async ({ page }, info) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  let theme: "warm" | "dark" = "warm";
  const step = (name: string, action: () => Promise<void>) => screenshotStep(page, info,
    { caseName: "P1-桌面工作台", stepName: name, theme }, action);
  await step("默认进入对话工作台", async () => {
    await page.goto("/ui/");
    await selectTheme(page, "warm");
    await expect(page.getByRole("button", { name: "新建对话", exact: true })).toBeEnabled();
    await expect(page.locator(".workbench-globalbar .primary-navigation")).toBeVisible();
    expect(await page.locator(".desktop-sidebar").count()).toBe(1);
    await expect(page.locator(".chat-main .chat-heading")).toHaveCount(0);
  });
  await step("填写新建对话", async () => {
    await page.getByRole("button", { name: "新建对话", exact: true }).click();
    await page.locator("#session-title").fill("工作台阶段演示");
    await expect(page.getByRole("dialog")).toBeVisible();
  });
  await step("创建并选中对话", async () => {
    await page.getByRole("dialog").getByRole("button", { name: "创建对话", exact: true }).click();
    await expect(page.locator("#chat-prompt")).toBeEnabled();
    await expect(page.locator(".topbar-title h1")).toHaveText("工作台阶段演示");
  });
  await step("发送消息并读取真实答复", async () => {
    await page.locator("#chat-prompt").fill("/echo 工作台已连通");
    await page.locator(".chat-compose-actions").getByRole("button", { name: "发送", exact: true }).click();
    await expect.poll(async () => {
      const sessionId = new URL(page.url()).searchParams.get("session");
      const turnId = await page.locator(".chat-turn").last().getAttribute("data-turn-id");
      expect(sessionId).toBeTruthy(); expect(turnId).toBeTruthy();
      const response = await page.request.get(`/v2/workspaces/local/sessions/${encodeURIComponent(sessionId!)}/turns/${encodeURIComponent(turnId!)}`);
      expect(response.ok()).toBe(true);
      const turn = await response.json();
      expect(turn.id).toBe(turnId); expect(turn.session_id).toBe(sessionId);
      return turn.status;
    }).toBe("completed");
    await expect(page.locator(".chat-answer").last()).toHaveText("工作台已连通");
  });
  await step("打开右侧对象详情", async () => {
    await page.getByRole("button", { name: "打开详情面板", exact: true }).click();
    await expect(page.locator(".inspector-desktop")).toBeVisible();
    await expect(page.locator(".inspector-object-id")).not.toBeEmpty();
  });
  theme = "dark";
  await step("切换深色主题", async () => {
    await selectTheme(page, "dark");
    await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  });
  await step("刷新后保留主题和对话", async () => {
    await page.reload();
    await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
    await expect(page.locator(".topbar-title h1")).toHaveText("工作台阶段演示");
    await expect(page.locator(".chat-answer").last()).toHaveText("工作台已连通");
  });
});

test("P1：窄屏导航、详情抽屉与键盘退出", async ({ page }, info) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const step = (name: string, action: () => Promise<void>) => screenshotStep(page, info,
    { caseName: "P1-手机工作台", stepName: name, theme: "warm" }, action);
  await step("打开窄屏工作台", async () => {
    await page.goto("/ui/");
    await selectTheme(page, "warm");
    await expect(page.getByRole("button", { name: "打开导航", exact: true })).toBeVisible();
  });
  await step("展开导航抽屉", async () => {
    await page.getByRole("button", { name: "打开导航", exact: true }).click();
    const drawer = page.getByRole("dialog", { name: "主导航与对话", exact: true });
    await expect(drawer).toBeVisible();
    const navigation = drawer.locator(".primary-navigation");
    await expect(navigation).toBeVisible();
    const bounds = await drawer.boundingBox();
    expect(bounds).toBeTruthy();
    expect(bounds!.x).toBeGreaterThanOrEqual(0);
    expect(bounds!.y).toBeGreaterThanOrEqual(0);
    expect(bounds!.x + bounds!.width).toBeLessThanOrEqual(390);
    expect(bounds!.y + bounds!.height).toBeLessThanOrEqual(844);
    await expect(navigation.getByRole("button", { name: "导航到设置", exact: true })).toBeInViewport();
  });
  await step("通过抽屉进入设置", async () => {
    await page.getByRole("dialog", { name: "主导航与对话", exact: true }).locator(".primary-navigation").getByRole("button", { name: "导航到设置", exact: true }).click();
    await expect(page.getByRole("heading", { name: "设置", level: 1 })).toBeVisible();
    await expect(page.getByRole("dialog", { name: "主导航与对话", exact: true })).not.toBeVisible();
  });
  await step("展开窄屏对象详情", async () => {
    await page.getByRole("button", { name: "打开详情面板", exact: true }).click();
    const inspector = page.getByRole("dialog", { name: "对象详情", exact: true });
    await expect(inspector).toBeVisible();
    const bounds = await inspector.boundingBox();
    expect(bounds).toBeTruthy();
    expect(bounds!.x).toBeGreaterThanOrEqual(0);
    expect(bounds!.y).toBeGreaterThanOrEqual(0);
    expect(bounds!.x + bounds!.width).toBeLessThanOrEqual(390);
    expect(bounds!.y + bounds!.height).toBeLessThanOrEqual(844);
  });
  await step("键盘关闭并检查无整页横溢", async () => {
    await page.keyboard.press("Escape");
    await expect(page.getByRole("dialog", { name: "对象详情", exact: true })).not.toBeVisible();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  });
});
