import { randomUUID } from "node:crypto";
import { expect, test, type Page, type TestInfo } from "@playwright/test";

const baseURL = process.env.KRIM_FORMAL_BASE_URL ?? "http://127.0.0.1:8879";
const origin = new URL(baseURL).origin;
const ADMIN_KEY = "formal-workbench-admin-browser-key";

type Theme = "bright" | "warm" | "dark";
const themes: Record<Theme, string> = { bright: "浅色", warm: "暖纸", dark: "深色" };
const desktopViewport = { width: 1440, height: 900 };
const mobileViewport = { width: 390, height: 844 };
const shortViewport = { width: 960, height: 480 };

type Rect = { x: number; y: number; width: number; height: number; right: number; bottom: number };
type DialogGeometry = {
  dialog: Rect;
  actions: Rect;
  input: Rect;
  select: Rect;
  labelControlGaps: number[];
  helpGap: number;
  fieldGap: number;
  formRowGap: number;
  fieldRowGaps: number[];
  helpId: string;
  describedBy: string[];
  workspaceDisabled: boolean;
  nativeSelect: boolean;
  chevronCount: number;
  viewport: { width: number; height: number };
  coarsePointer: boolean;
};

async function openNavigationSurface(page: Page) {
  const drawer = page.getByRole("dialog", { name: "主导航与对话", exact: true });
  if (!await drawer.isVisible()) {
    const mobileMenu = page.getByRole("button", { name: "打开导航", exact: true });
    if (await mobileMenu.isVisible()) { await mobileMenu.click(); await expect(drawer).toBeVisible(); }
  }
  return drawer;
}

async function selectTheme(page: Page, theme: Theme) {
  const drawer = await openNavigationSurface(page);
  const surface = await drawer.isVisible() ? drawer : page.locator(".desktop-sidebar");
  await surface.getByRole("button", { name: "设置", exact: true }).click();
  await page.locator(".settings-popover .theme-options").getByRole("button", { name: new RegExp(`^${themes[theme]}`) }).click();
  await expect(page.locator("html")).toHaveAttribute("data-theme", theme);
  // 主题选择后恢复抽屉开合状态，避免遮挡后续主区操作。
  if (await drawer.isVisible()) { await page.keyboard.press("Escape"); await expect(drawer).not.toBeVisible(); }
}

async function connect(page: Page) {
  const drawer = await openNavigationSurface(page);
  await page.getByRole("button", { name: "密钥身份" }).click();
  await page.getByRole("menuitem", { name: "连接 / 切换密钥" }).click();
  await page.locator("#api-key").fill(ADMIN_KEY);
  await page.locator("#connect-save").click();
  await expect(page.locator(".sidebar-account .account-sub")).toContainText("已连接");
  return drawer;
}

async function openCreateDialog(page: Page, theme: Theme) {
  await page.goto("/ui/", { waitUntil: "domcontentloaded" });
  await selectTheme(page, theme);
  // 连接会按需打开移动端抽屉；随后从侧栏按钮新建对话（移动端抽屉仍开着）。
  await connect(page);
  await page.locator(".sidebar-buttons").getByRole("button", { name: "新建对话", exact: true }).click();
  await expect(page.locator(".session-create-dialog")).toBeVisible();
  await expect(page.locator(".session-create-form")).toBeVisible();
}

async function readyForMeasurement(page: Page) {
  await page.evaluate(async () => {
    await document.fonts.ready;
    const popup = document.querySelector<HTMLElement>(".session-create-dialog");
    if (!popup) throw new Error("缺少新建对话弹窗");
    await Promise.all(popup.getAnimations({ subtree: true }).map(animation => animation.finished.catch(() => undefined)));
    await new Promise<void>(resolve => requestAnimationFrame(() => requestAnimationFrame(() => resolve())));
  });
}

async function inspectDialog(page: Page): Promise<DialogGeometry> {
  return page.evaluate(() => {
    const required = <T extends Element>(selector: string, root: ParentNode = document): T => {
      const element = root.querySelector<T>(selector);
      if (!element) throw new Error(`缺少对话弹窗验收元素：${selector}`);
      return element;
    };
    const box = (element: Element): Rect => {
      const rect = element.getBoundingClientRect();
      return { x: rect.x, y: rect.y, width: rect.width, height: rect.height, right: rect.right, bottom: rect.bottom };
    };
    const px = (value: string) => Number.parseFloat(value);
    const dialog = required<HTMLElement>(".session-create-dialog");
    const form = required<HTMLElement>(".session-create-form", dialog);
    const fields = Array.from(form.querySelectorAll<HTMLElement>(".session-create-field"));
    if (fields.length !== 2) throw new Error(`新建对话字段数量应为 2，实际为 ${fields.length}`);

    const titleLabel = required<HTMLLabelElement>('label[for="session-title"]', fields[0]!);
    const titleInput = required<HTMLInputElement>("#session-title", fields[0]!);
    const targetLabel = required<HTMLLabelElement>('label[for="chat-target"]', fields[1]!);
    const targetWrapper = required<HTMLElement>(".session-target-select", fields[1]!);
    const targetSelect = required<HTMLSelectElement>("select#chat-target", targetWrapper);
    const help = required<HTMLElement>("#session-target-help", fields[1]!);
    const actions = required<HTMLElement>(".dialog-actions", form);
    const targetOption = required<HTMLOptionElement>('option[value="target-workspace"]', targetSelect);
    const titleBox = box(titleInput);
    const targetBox = box(targetSelect);
    const fieldBoxes = fields.map(box);

    return {
      dialog: box(dialog),
      actions: box(actions),
      input: titleBox,
      select: targetBox,
      labelControlGaps: [
        box(titleInput).y - box(titleLabel).bottom,
        box(targetWrapper).y - box(targetLabel).bottom,
      ],
      helpGap: box(help).y - targetBox.bottom,
      fieldGap: fieldBoxes[1]!.y - fieldBoxes[0]!.bottom,
      formRowGap: px(getComputedStyle(form).rowGap),
      fieldRowGaps: fields.map(field => px(getComputedStyle(field).rowGap)),
      helpId: help.id,
      describedBy: (targetSelect.getAttribute("aria-describedby") ?? "").split(/\s+/).filter(Boolean),
      workspaceDisabled: targetOption.disabled,
      nativeSelect: targetSelect.tagName === "SELECT",
      chevronCount: targetWrapper.querySelectorAll("svg").length,
      viewport: { width: innerWidth, height: innerHeight },
      coarsePointer: matchMedia("(pointer: coarse)").matches,
    };
  });
}

function withinOnePixel(actual: number, expected: number, label: string) {
  expect(Math.abs(actual - expected), label).toBeLessThanOrEqual(1);
}

function insideViewport(rect: Rect, viewport: { width: number; height: number }, label: string) {
  expect(rect.x, `${label} 左边界`).toBeGreaterThanOrEqual(-1);
  expect(rect.y, `${label} 上边界`).toBeGreaterThanOrEqual(-1);
  expect(rect.right, `${label} 右边界`).toBeLessThanOrEqual(viewport.width + 1);
  expect(rect.bottom, `${label} 下边界`).toBeLessThanOrEqual(viewport.height + 1);
}

async function expectDialogGeometry(page: Page, touch: boolean) {
  await readyForMeasurement(page);
  const geometry = await inspectDialog(page);
  const expectedControlHeight = touch ? 40 : 36;
  const expectedDialogWidth = Math.min(420, geometry.viewport.width - 32);

  withinOnePixel(geometry.dialog.width, expectedDialogWidth, "弹窗宽度");
  withinOnePixel(geometry.input.height, expectedControlHeight, "标题输入框高度");
  withinOnePixel(geometry.select.height, expectedControlHeight, "目标原生选择框高度");
  withinOnePixel(geometry.input.x, geometry.select.x, "输入框与选择框左对齐");
  withinOnePixel(geometry.input.right, geometry.select.right, "输入框与选择框右对齐");
  withinOnePixel(geometry.input.width, geometry.select.width, "输入框与选择框等宽");
  withinOnePixel(geometry.input.height, geometry.select.height, "输入框与选择框等高");
  for (const [index, gap] of geometry.labelControlGaps.entries()) {
    withinOnePixel(gap, 8, `第 ${index + 1} 个字段的标签与控件间距`);
  }
  withinOnePixel(geometry.formRowGap, 18, "表单网格间距");
  withinOnePixel(geometry.fieldGap, 18, "两个字段组间距");
  for (const [index, gap] of geometry.fieldRowGaps.entries()) {
    withinOnePixel(gap, 8, `第 ${index + 1} 个字段网格间距`);
  }

  expect(geometry.helpGap, "目标说明位于选择框下方且不重叠").toBeGreaterThanOrEqual(7);
  expect(geometry.describedBy, "目标选择框通过 aria-describedby 关联说明").toContain(geometry.helpId);
  expect(geometry.workspaceDisabled, "未登记的工作区目标仍受禁用门禁保护").toBe(true);
  expect(geometry.nativeSelect, "目标控件保持原生 select").toBe(true);
  expect(geometry.chevronCount, "目标选择框保留下拉图标").toBeGreaterThan(0);
  expect(geometry.coarsePointer, "手机场景启用触控指针").toBe(touch);
  insideViewport(geometry.dialog, geometry.viewport, "弹窗");
  insideViewport(geometry.actions, geometry.viewport, "弹窗操作按钮");

  const actionsInsideDialog = await page.locator(".session-create-dialog .dialog-actions").evaluate(actions => {
    const dialog = actions.closest(".session-create-dialog");
    if (!dialog) return false;
    const parent = dialog.getBoundingClientRect();
    const child = actions.getBoundingClientRect();
    return child.left >= parent.left - 1 && child.top >= parent.top - 1
      && child.right <= parent.right + 1 && child.bottom <= parent.bottom + 1;
  });
  expect(actionsInsideDialog, "弹窗操作按钮位于弹窗内部").toBe(true);

  // 原生 select 通过标准 selectOption 操作，不展开仿生自定义菜单。
  await page.locator("#chat-target").selectOption("demo");
  await expect(page.locator("#chat-target")).toHaveValue("demo");
  return geometry;
}

async function captureOpenDialog(page: Page, testInfo: TestInfo, theme: Theme, viewportName: string) {
  await page.locator(".session-create-dialog").screenshot({
    path: testInfo.outputPath(`session-create-${theme}-${viewportName}-dialog.png`),
    animations: "disabled",
  });
  await page.screenshot({
    path: testInfo.outputPath(`session-create-${theme}-${viewportName}-page.png`),
    fullPage: true,
    animations: "disabled",
  });
}

async function createConversation(page: Page, title: string) {
  const responsePromise = page.waitForResponse(response => {
    const request = response.request();
    const url = new URL(response.url());
    const pathname = url.pathname;
    return url.origin === origin && request.method() === "POST"
      && pathname.startsWith("/v2/workspaces/") && pathname.endsWith("/sessions");
  });
  await page.locator("#session-title").fill(title);
  await page.getByRole("button", { name: "创建对话", exact: true }).click();
  const response = await responsePromise;
  expect(response.ok(), "新建对话 POST 成功").toBe(true);
  const requestBody = response.request().postDataJSON() as Record<string, unknown>;
  expect(requestBody).toMatchObject({ title, target_key: "demo" });
  const created = await response.json() as { id: string; title: string; target_key: string };
  expect(created).toMatchObject({ title, target_key: "demo" });
  await expect(page.locator("#chat-prompt")).toBeVisible();
  await expect(page.locator("#chat-prompt")).toBeEnabled();
  await expect(page.locator(".chat-toolbar .toolbar-title h1")).toHaveText(title);
  await expect.poll(() => new URL(page.url()).searchParams.get("session")).toBe(created.id);
}

test.describe("桌面对话弹窗", () => {
  test.use({ viewport: desktopViewport });

  for (const theme of ["bright", "warm", "dark"] as const) {
    test(`${themes[theme]}主题 1440×900`, async ({ page }, testInfo) => {
      await openCreateDialog(page, theme);
      await expectDialogGeometry(page, false);
      await captureOpenDialog(page, testInfo, theme, "1440x900");
    });
  }

  test("960×480 短窗口仍将弹窗与按钮留在视口内", async ({ page }, testInfo) => {
    await page.setViewportSize(shortViewport);
    await openCreateDialog(page, "bright");
    await expectDialogGeometry(page, false);
    await captureOpenDialog(page, testInfo, "bright", "960x480");
  });

  test("桌面通过真实创建请求进入可用对话", async ({ page }) => {
    await openCreateDialog(page, "bright");
    await expectDialogGeometry(page, false);
    await createConversation(page, `session-create-desktop-${randomUUID().slice(0, 8)}`);
  });
});

test.describe("手机对话弹窗", () => {
  test.use({ viewport: mobileViewport, isMobile: true, hasTouch: true });

  for (const theme of ["bright", "warm", "dark"] as const) {
    test(`${themes[theme]}主题 390×844`, async ({ page }, testInfo) => {
      await openCreateDialog(page, theme);
      await expectDialogGeometry(page, true);
      await captureOpenDialog(page, testInfo, theme, "390x844");
    });
  }

  test("手机通过真实创建请求进入可用对话", async ({ page }) => {
    await openCreateDialog(page, "bright");
    await expectDialogGeometry(page, true);
    await createConversation(page, `session-create-mobile-${randomUUID().slice(0, 8)}`);
  });
});
