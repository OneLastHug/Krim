import { expect, test, type Page } from "@playwright/test";
import { mkdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

const headers = { Authorization: "Bearer browser-test-key" };

async function capture(page: Page, folder: string, name: string) {
  await page.screenshot({ path: resolve(folder, name + ".png"), fullPage: true });
  const dimensions = await page.evaluate(() => ({
    width: innerWidth, scrollWidth: document.documentElement.scrollWidth,
    catalogFont: getComputedStyle(document.querySelector(".tool-catalog")!).fontSize,
  }));
  expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.width);
  expect(dimensions.catalogFont).toBe("12px");
  await writeFile(resolve(folder, name + ".json"), JSON.stringify(dimensions, null, 2));
}

test("HTTP/MCP实际来源、真实审批与结果", async ({ page }, info) => {
  const folder = resolve(process.env.KRIM_WEB_TOOLS_EVIDENCE!, info.project.name);
  await mkdir(folder, { recursive: true });
  const errors: string[] = [];
  page.on("pageerror", error => errors.push(String(error)));
  await page.goto("/ui/?view=chat");
  await page.locator("button.access-connect").click();
  await page.locator("#api-key").fill("browser-test-key");
  await page.locator("#connect-save").click();
  const mobile = page.getByRole("button", { name: "打开导航", exact: true });
  if (await mobile.isVisible()) await mobile.click();
  await page.getByRole("button", { name: "新建对话", exact: true }).click();
  await page.locator("#session-title").fill("来源验证 " + info.project.name);
  await page.getByRole("button", { name: "创建对话", exact: true }).click();
  await expect(page.locator(".topbar-title h1")).toContainText("来源验证");
  await expect(page.getByRole("dialog")).toHaveCount(0);
  const catalog = page.locator(".tool-catalog");
  await expect(catalog).toBeVisible();
  await catalog.locator(":scope > summary").click();
  const search = catalog.locator("article").filter({ hasText: "web_search" });
  await expect(search).toContainText("MCP");
  await expect(search).toContainText("fixture / echo");
  await expect(catalog.locator("article").filter({ hasText: "web_fetch" })).toContainText("HTTP");
  await search.getByText("参数 Schema", { exact: true }).click();
  await expect(search.locator("pre").first()).toContainText('"text"');
  await capture(page, folder, "01-sources");
  await search.getByText("参数 Schema", { exact: true }).click();
  await catalog.locator(":scope > summary").click();

  await page.locator("#chat-prompt").fill("MCP browser " + info.project.name);
  await page.locator(".chat-composer button[type=submit]").click();
  const approval = page.getByRole("button", { name: "允许 web_search 本次调用", exact: true });
  await expect(approval).toBeVisible();
  await expect(page.locator(".chat-approval").last()).toContainText("fixture / echo");
  await expect(page.locator(".chat-approval").last()).toContainText("只读");
  await capture(page, folder, "02-approval");
  await approval.click();
  await expect(page.locator(".chat-turn").last().locator(".chat-answer"))
    .toContainText("MCP browser " + info.project.name);
  await expect(page.locator(".chat-turn").last().locator(".chat-turn-meta > [role=status]"))
    .toHaveCount(0);
  await capture(page, folder, "03-mcp-result");
  const session = new URL(page.url()).searchParams.get("session")!;
  const turn = await page.locator(".chat-turn").last().getAttribute("data-turn-id");
  const factsResponse = await page.request.get(
    `/v2/workspaces/local/sessions/${session}/turns/${turn}/result`, { headers });
  expect(factsResponse.ok()).toBe(true);
  const facts = await factsResponse.json();
  expect(facts.run.status).toBe("completed");
  expect(facts.tools[0].name).toBe("web_search");
  expect(facts.tools[0].value).toEqual({ echo: "MCP browser " + info.project.name });
  await writeFile(resolve(folder, "mcp-facts.json"), JSON.stringify(facts, null, 2));

  await page.locator("#chat-prompt").fill("fetch https://example.com/article");
  await page.locator(".chat-composer button[type=submit]").click();
  const fetchApproval = page.getByRole("button", { name: "允许 web_fetch 本次调用", exact: true });
  await expect(fetchApproval).toBeVisible();
  await fetchApproval.click();
  await expect(page.locator(".chat-turn").last().locator(".chat-answer"))
    .toContainText("HTTP article from local fixture");
  await expect(page.locator(".chat-turn").last().locator(".chat-turn-meta > [role=status]"))
    .toHaveCount(0);
  await capture(page, folder, "04-http-result");
  const countsResponse = await page.request.get("/test/tool-counts", { headers });
  const counts = await countsResponse.json();
  expect(counts.search).toBe(0);
  expect(counts.extract).toBeGreaterThan(0);
  expect(errors).toEqual([]);
  await writeFile(resolve(folder, "result.json"), JSON.stringify({ counts, errors }, null, 2));
});
