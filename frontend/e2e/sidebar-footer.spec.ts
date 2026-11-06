import { expect, test } from "@playwright/test";

const themes = { warm: "暖纸", bright: "浅色", dark: "深色" } as const;

for (const mobile of [false, true]) for (const [theme, label] of Object.entries(themes)) {
  test(`侧栏底部单条通栏分隔线：${label}${mobile ? "手机" : "桌面"}`, async ({ page }) => {
    await page.setViewportSize(mobile ? { width: 375, height: 812 } : { width: 1440, height: 900 });
    await page.goto("/ui/");
    if (mobile) await page.getByRole("button", { name: "打开导航", exact: true }).click();
    const sidebar = page.getByRole("complementary", { name: "Krim Agent 工作区侧栏" });
    await expect(sidebar.locator(".account-sub")).toContainText("已连接");
    await sidebar.getByRole("button", { name: "设置", exact: true }).click();
    await page.locator(".settings-popover .theme-options").getByRole("button", { name: label, exact: true }).click();
    await expect(page.locator("html")).toHaveAttribute("data-theme", theme);

    const account = sidebar.locator(".sidebar-account");
    const extra = sidebar.locator(".sidebar-footer-extra");
    await expect(account).toHaveCSS("border-top-width", "1px");
    await expect(account).toHaveCSS("border-top-style", "solid");
    await expect(extra).toHaveCSS("border-top-width", "0px");
    await expect(extra).toHaveCSS("padding-top", "0px");
    const bounds = await sidebar.boundingBox();
    const footerBounds = await account.boundingBox();
    expect(bounds).not.toBeNull();
    expect(footerBounds).not.toBeNull();
    expect(footerBounds!.x).toBeCloseTo(bounds!.x, 1);
    expect(footerBounds!.width).toBeCloseTo(bounds!.width, 1);
    expect(await extra.evaluate(element => element.getBoundingClientRect().height)).toBe(0);

    await account.getByRole("button", { name: "密钥身份", exact: true }).click();
    await expect(page.getByRole("menu", { name: "密钥身份", exact: true })).toBeVisible();
    await page.keyboard.press("Escape");
    await account.getByRole("button", { name: "查找对话", exact: true }).click();
    await expect(page.getByRole("dialog", { name: "查找对话", exact: true })).toBeVisible();
  });
}
