import { expect, test, type Locator, type Page } from "@playwright/test";
const headers = { Authorization: "Bearer browser-test-key" };
async function open(page: Page): Promise<void> {
  await page.goto("/ui/"); await page.getByLabel("服务访问密钥", { exact: true }).fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.locator("#connection")).toContainText("本地服务已连接");
  await expect(page.locator("#mcp-resources .mcp-resource")).toHaveCount(3);
}
function card(page: Page, name = "browser_mcp"): Locator { return page.locator(`.mcp-resource[data-name="${name}"]`); }
async function control(page: Page, row: Locator, label: string): Promise<void> {
  page.once("dialog", dialog => dialog.accept()); await row.getByRole("button", { name: label, exact: true }).click();
}
async function disable(page: Page, name: string): Promise<void> {
  const view = await (await page.request.get(`/v1/mcp/servers/${name}`, { headers })).json();
  if (view.profile?.enabled) await page.request.put(`/v1/mcp/servers/${name}`, { headers, data: {
    expected_revision: view.profile.revision, resource_hash: view.resource_hash || view.profile.resource_hash, enabled: false,
  } });
}

test("公开引用启停连接、纯文本和Locale保持真实状态", async ({ page }) => {
  await open(page); const row = card(page);
  try {
    await expect(row).toContainText('<img src=x onerror="window.mcpExecuted=1">');
    expect(await row.locator("img").count()).toBe(0);
    expect(await page.evaluate(() => (window as unknown as { mcpExecuted?: number }).mcpExecuted)).toBeUndefined();
    await control(page, row, "启用MCP资源"); await expect(row.getByRole("button", { name: "连接MCP资源", exact: true })).toBeEnabled();
    await control(page, row, "连接MCP资源"); await expect(row.locator("strong")).toContainText("MCP已连接");
    await expect(row.locator(".mcp-resource-status")).toContainText("browser_mcp__echo");
    await control(page, row, "断开MCP连接"); await expect(row.locator("strong")).toContainText("MCP未连接");
    await page.locator("#locale-select").selectOption("en-US");
    await expect(page.locator("#mcp-panel h2")).toHaveText("MCP resources");
    await expect(row.getByRole("button", { name: "Disable MCP resource", exact: true })).toBeEnabled();
    await control(page, row, "Disable MCP resource"); await expect(row.getByRole("button", { name: "Enable MCP resource", exact: true })).toBeEnabled();
  } finally { await disable(page, "browser_mcp"); }
});

test("丢失启用响应后只读找回，刷新不重新PUT或自动连接", async ({ page }) => {
  await open(page); const row = card(page); let writes = 0;
  try {
    await page.route("**/v1/mcp/servers/browser_mcp", async route => {
      if (route.request().method() === "PUT") { writes += 1; await route.fetch(); await route.abort("failed"); }
      else await route.continue();
    });
    await control(page, row, "启用MCP资源"); await expect(row.locator(".mcp-pending")).toBeVisible();
    await expect(row.getByRole("button", { name: "连接MCP资源", exact: true })).toBeDisabled();
    const pending = await page.evaluate(() => sessionStorage.getItem("krim-mcp-pending"));
    expect(pending).toContain("browser_mcp"); expect(pending).not.toContain("trusted");
    await row.getByRole("button", { name: "查询原MCP状态", exact: true }).click();
    await expect(row.getByRole("button", { name: "连接MCP资源", exact: true })).toBeEnabled();
    expect(writes).toBe(1);
    const before = await (await page.request.post("/_test/mcp", { headers, data: { name: "browser_mcp", action: "status" } })).json();
    await open(page); const after = await (await page.request.post("/_test/mcp", { headers, data: { name: "browser_mcp", action: "status" } })).json();
    expect(after.starts).toBe(before.starts); expect(writes).toBe(1);
  } finally { await page.unroute("**/v1/mcp/servers/browser_mcp"); await disable(page, "browser_mcp"); }
});

test("慢连接可断开、服务失败不污染其他资源且不回显秘密", async ({ page }) => {
  await page.request.post("/_test/mcp", { headers, data: { name: "browser_slow_mcp", action: "block" } });
  await open(page); const slow = card(page, "browser_slow_mcp"), failed = card(page, "browser_fail_mcp");
  try {
    await control(page, slow, "启用MCP资源"); await control(page, slow, "连接MCP资源");
    await expect(slow.locator("strong")).toContainText("MCP连接中");
    await control(page, slow, "断开MCP连接"); await expect(slow.locator("strong")).toContainText("MCP未连接");
    await control(page, failed, "启用MCP资源"); await control(page, failed, "连接MCP资源");
    await expect(failed.locator("strong")).toContainText("MCP连接失败");
    await expect(failed).not.toContainText("private-browser-secret");
    await expect(slow.locator("strong")).toContainText("MCP未连接");
  } finally {
    await page.request.post("/_test/mcp", { headers, data: { name: "browser_slow_mcp", action: "release" } });
    await disable(page, "browser_slow_mcp"); await disable(page, "browser_fail_mcp");
  }
});
