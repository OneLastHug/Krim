import { expect, test } from "@playwright/test";

const actor = { principal_id: "hover-user", generation: 1, legacy: true, target_keys: ["demo"], workspaces: [{ workspace_id: "hover-workspace", role: "admin", approve: true }] };
const session = (id: string, title: string) => ({ id, title, workspace_id: "hover-workspace", owner_id: "hover-user", create_key: "create-" + id, target_key: "demo", target_fingerprint: "0".repeat(64), context_policy: { max_bytes: 131072, max_messages: 256, reserved_output_tokens: 2048, counter_version: "approx-utf8/1" }, revision: 0, status: "open", dispatch_paused: false, created_at: 1, closed_at: null });
const selected = session("hover-a", "仓库解析技术栈");
const other = session("hover-b", "项目消息队列与后端架构");

for (const [theme, label] of [["bright", "浅色"], ["warm", "暖纸"], ["dark", "深色"]] as const) {
  test(`会话行完整底色包住操作按钮：${label}`, async ({ page }) => {
    // Exercise the production DOM/CSS with read-only fixtures; do not modify service data.
    const writes: string[] = [];
    await page.route("**/v2/**", async route => {
      const path = new URL(route.request().url()).pathname;
      if (route.request().method() !== "GET") { writes.push(path); await route.abort(); return; }
      let body: unknown = [];
      let status = 200;
      if (path === "/v2/me") body = actor;
      else if (path === "/v2/me/profile") { status = 409; body = { error: { code: "account_management_unavailable" } }; }
      else if (path.endsWith("/connection-profile")) body = { profile_id: "connection-demo", display_name: "本地模式", mode: "local", target_key: "demo", target_fingerprint: "0".repeat(64) };
      else if (path.endsWith("/sessions")) body = [selected, other];
      else if (path.endsWith("/sessions/hover-a")) body = selected;
      else if (path.endsWith("/sessions/hover-b")) body = other;
      await route.fulfill({ status, contentType: "application/json", body: JSON.stringify(body) });
    });
    await page.route("**/healthz", route => route.fulfill({ contentType: "application/json", body: JSON.stringify({ http_enabled: false, product_eval_enabled: false, evaluation_automation_enabled: false }) }));
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto("/ui/?view=chat&workspace=hover-workspace&session=hover-a");
    const row = page.locator(".chat-session-row").filter({ has: page.getByRole("button", { name: selected.title, exact: true }) });
    const title = row.locator(".session-select");
    const action = row.getByRole("button", { name: "重命名当前对话", exact: true });
    await expect(action).toBeVisible();
    await page.locator(".sidebar-account").getByRole("button", { name: "设置", exact: true }).click();
    await page.locator(".settings-popover").getByRole("button", { name: label, exact: true }).click();
    await expect(page.locator("html")).toHaveAttribute("data-theme", theme);
    await page.evaluate(() => document.fonts.ready.then(() => undefined));

    await title.hover();
    await expect(title).toHaveCSS("background-color", "rgba(0, 0, 0, 0)");
    const outer = await row.evaluate(element => getComputedStyle(element).backgroundColor);
    expect(outer).not.toBe("rgba(0, 0, 0, 0)");
    await expect(action).toHaveCSS("background-color", "rgba(0, 0, 0, 0)");
    const before = await row.boundingBox();
    expect(before!.height).toBeCloseTo(38, 0);
    await expect(row.locator("small, time")).toHaveCount(0);
    await expect(title).not.toHaveAttribute("aria-describedby", /session-created/);

    await action.hover();
    expect(await row.evaluate(element => getComputedStyle(element).backgroundColor)).toBe(outer);
    const inner = await action.evaluate(element => getComputedStyle(element).backgroundColor);
    expect(inner).not.toBe("rgba(0, 0, 0, 0)");
    expect(inner).not.toBe(outer);
    const rowBox = (await row.boundingBox())!, actionBox = (await action.boundingBox())!;
    expect(rowBox).toEqual(before);
    expect(actionBox.x).toBeGreaterThan(rowBox.x);
    expect(actionBox.y).toBeGreaterThan(rowBox.y);
    expect(actionBox.x + actionBox.width).toBeLessThan(rowBox.x + rowBox.width);
    expect(actionBox.y + actionBox.height).toBeLessThan(rowBox.y + rowBox.height);
    await page.screenshot({ path: test.info().outputPath(`session-row-${theme}.png`) });
    await row.screenshot({ path: test.info().outputPath(`session-row-detail-${theme}.png`) });

    await action.focus();
    expect(await action.evaluate(element => getComputedStyle(element).backgroundColor)).not.toBe("rgba(0, 0, 0, 0)");
    await action.click();
    await expect(page.getByRole("dialog", { name: "重命名对话", exact: true })).toBeVisible();
    expect(new URL(page.url()).searchParams.get("session")).toBe("hover-a");
    await page.keyboard.press("Escape");
    const otherRow = page.locator(".chat-session-row").filter({ has: page.getByRole("button", { name: other.title, exact: true }) });
    await otherRow.locator(".session-select").hover();
    expect(await otherRow.evaluate(element => getComputedStyle(element).backgroundColor)).not.toBe("rgba(0, 0, 0, 0)");
    await expect(otherRow.locator(".session-select")).toHaveCSS("background-color", "rgba(0, 0, 0, 0)");
    expect(writes).toEqual([]);
  });
}
