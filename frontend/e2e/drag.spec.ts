import { pythonExecutable, pythonFixtureArgs } from "./python-client.js";
import { test as base, expect } from "@playwright/test";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const test = base.extend({
  request: async ({ playwright, baseURL }, use) => {
    const context = await playwright.request.newContext({ baseURL, extraHTTPHeaders: { Authorization: "Bearer browser-test-key" } });
    await use(context); await context.dispose();
  },
});

test("原生拖放先预览，drop 后创建 MentionRef，脚本/HTML 保持文本", async ({ page, request }) => {
  const topic = await (await request.post("/v1/topics", { data: { title: `Drag-${Date.now()}` } })).json();
  await page.goto(`/ui/?topic=${topic.id}`); await page.locator("#api-key").fill("browser-test-key"); await page.locator("#key-form button").click(); await expect(page.locator("#connection")).toHaveClass(/ready/);
  const composer = page.locator("#composer");
  await page.locator("#prompt").fill("😀 ");
  await composer.evaluate((element) => {
    const data = new DataTransfer(); data.setData("text/plain", "拖放文本 <img id='drop-html'>");
    element.dispatchEvent(new DragEvent("dragenter", { bubbles: true, dataTransfer: data }));
  });
  await expect(page.locator("#drag-preview")).toContainText("拖入文本");
  await composer.evaluate((element) => {
    const data = new DataTransfer(); data.setData("text/plain", "拖放文本 <img id='drop-html'>");
    element.dispatchEvent(new DragEvent("drop", { bubbles: true, dataTransfer: data }));
  });
  await expect(page.locator("#drag-preview")).toBeHidden();
  await page.locator(".prompt-context-panel summary").click();
  await expect(page.locator("#mention-list")).toContainText("拖入文本");
  await expect(page.locator("#mention-list")).not.toContainText("drop-html");
  const context = (await (await request.get(`/v1/topics/${topic.id}/prompt-context`)).json()).mentions;
  expect(context).toHaveLength(1); expect(context[0].content).toContain("拖放文本");
  await expect(page.locator("#prompt")).toHaveValue("😀 @拖入文本");
  expect((await (await request.get(`/v1/topics/${topic.id}/messages`)).json()).messages).toHaveLength(0);
  const accepted = page.waitForResponse((response) => response.url().endsWith("/messages") && response.request().method() === "POST");
  await page.locator("#send").click(); const run = await (await accepted).json();
  await expect(page.locator("#send")).toBeEnabled();
  const saved = (await (await request.get(`/v1/runs/${run.id}/messages`)).json()).messages;
  expect(saved[0].content).toBe("😀 @拖入文本"); expect(saved[0].details.prompt_context.mentions[0]).toMatchObject({ id: context[0].id, start: 2, end: 7 });
  await expect(page.locator("#drop-html")).toHaveCount(0);
});

test("拖入非文本 File 被拒绝且不会写入目录", async ({ page, request }) => {
  const topic = await (await request.post("/v1/topics", { data: { title: `Drag reject-${Date.now()}` } })).json();
  await page.goto(`/ui/?topic=${topic.id}`); await page.locator("#api-key").fill("browser-test-key"); await page.locator("#key-form button").click(); await expect(page.locator("#connection")).toHaveClass(/ready/);
  await page.locator("#composer").evaluate((element) => {
    const data = new DataTransfer(); data.items.add(new File([new Uint8Array([0, 1, 2])], "binary.bin", { type: "application/octet-stream" }));
    element.dispatchEvent(new DragEvent("drop", { bubbles: true, dataTransfer: data }));
  });
  await expect(page.locator("#notice")).toContainText("附件为空、过大或媒体类型不允许");
  expect((await (await request.get(`/v1/topics/${topic.id}/prompt-context`)).json()).mentions).toHaveLength(0);
});

test("明确 UTF-8 File 在 drop 后才读取正文，预览阶段不写入", async ({ page, request }) => {
  const topic = await (await request.post("/v1/topics", { data: { title: `Text File-${Date.now()}` } })).json();
  await page.goto(`/ui/?topic=${topic.id}`); await page.locator("#api-key").fill("browser-test-key"); await page.locator("#key-form button").click(); await expect(page.locator("#connection")).toHaveClass(/ready/);
  const transfer = await page.evaluateHandle(() => { const data = new DataTransfer(); data.items.add(new File(["😀 文本文件原文 <script>literal</script>"], "notes.md", { type: "text/markdown" })); return data; });
  await page.locator("#composer").dispatchEvent("dragenter", { dataTransfer: transfer });
  await expect(page.locator("#drag-preview")).toBeVisible();
  expect((await (await request.get(`/v1/topics/${topic.id}/prompt-context`)).json()).mentions).toHaveLength(0);
  await page.locator("#composer").dispatchEvent("dragleave", { dataTransfer: transfer }); await expect(page.locator("#drag-preview")).toBeHidden();
  await page.locator("#composer").dispatchEvent("drop", { dataTransfer: transfer });
  await expect(page.locator("#prompt")).toHaveValue("@notes.md");
  const context = (await (await request.get(`/v1/topics/${topic.id}/prompt-context`)).json()).mentions;
  expect(context[0]).toMatchObject({ kind: "file", label: "notes.md", content: "😀 文本文件原文 <script>literal</script>", metadata: { filename: "notes.md", media_type: "text/markdown" } });
  expect((await (await request.get(`/v1/topics/${topic.id}/messages`)).json()).messages).toHaveLength(0);
  await transfer.dispose();
});

test("URI drop 只保存安全 URL 字面值且拒绝脚本地址", async ({ page, request }) => {
  const topic = await (await request.post("/v1/topics", { data: { title: `URI Drop-${Date.now()}` } })).json();
  await page.goto(`/ui/?topic=${topic.id}`); await page.locator("#api-key").fill("browser-test-key"); await page.locator("#key-form button").click(); await expect(page.locator("#connection")).toHaveClass(/ready/);
  await page.locator("#composer").evaluate((element) => { const data = new DataTransfer(); data.setData("text/uri-list", "javascript:alert(1)"); element.dispatchEvent(new DragEvent("drop", { bubbles: true, dataTransfer: data })); });
  await expect(page.locator("#notice")).toHaveClass(/error/);
  expect((await (await request.get(`/v1/topics/${topic.id}/prompt-context`)).json()).mentions).toHaveLength(0);
  await page.locator("#composer").evaluate((element) => { const data = new DataTransfer(); data.setData("text/uri-list", "https://example.invalid/source#part"); element.dispatchEvent(new DragEvent("drop", { bubbles: true, dataTransfer: data })); });
  await expect(page.locator("#prompt")).toHaveValue("@拖入链接");
  const context = (await (await request.get(`/v1/topics/${topic.id}/prompt-context`)).json()).mentions;
  expect(context[0]).toMatchObject({ kind: "resource", content: "https://example.invalid/source#part" });
});

test("Python Host 经 iframe 预览/离开/drop 获得实际持久 MentionRef", async ({ page, request, baseURL }) => {
  const topic = await (await request.post("/v1/topics", { data: { title: `Host Drag-${Date.now()}` } })).json();
  await page.goto("/ui/host.html"); await page.locator("#host-key").fill("browser-test-key"); await page.locator("#host-connect").click(); await expect(page.locator("#host-status")).toContainText("iframe 已连接");
  const frame = page.frameLocator("#agent-frame"); await frame.locator(".topic-button").filter({ hasText: topic.title }).click(); await expect(frame.locator("#topic-heading")).toHaveText(topic.title);
  const sid = await page.locator("#host-session").textContent();
  const result = await promisify(execFile)(pythonExecutable, [...pythonFixtureArgs("host_drag_client.py"), baseURL!, sid!], { env: { ...process.env, KRIM_AGENT_API_KEY: "browser-test-key" }, timeout: 20_000 });
  const value = JSON.parse(result.stdout); expect(value.topic_id).toBe(topic.id);
  const context = (await (await request.get(`/v1/topics/${topic.id}/prompt-context`)).json()).mentions;
  expect(context[0]).toMatchObject({ id: value.mention_id, content: "Python 明确提供的拖放正文" });
  await expect(frame.locator("#prompt")).toHaveValue("@Host 引用"); await expect(frame.locator("#drag-preview")).toBeHidden();
});
