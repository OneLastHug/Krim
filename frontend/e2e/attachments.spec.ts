import { test as base, expect } from "@playwright/test";

const test = base.extend({
  request: async ({ playwright, baseURL }, use) => {
    const context = await playwright.request.newContext({ baseURL, extraHTTPHeaders: { Authorization: "Bearer browser-test-key" } });
    await use(context); await context.dispose();
  },
});

test("上传允许的二进制附件，插入 Mention，下载字节并删除资源", async ({ page, request }) => {
  const topic = await (await request.post("/v1/topics", { data: { title: `Attachment-${Date.now()}` } })).json();
  await page.goto(`/ui/?topic=${topic.id}`); await page.locator("#api-key").fill("browser-test-key"); await page.locator("#key-form button").click(); await expect(page.locator("#connection")).toHaveClass(/ready/);
  await page.locator("#attachment-file").setInputFiles({ name: "pixel.png", mimeType: "image/png", buffer: Buffer.from([137, 80, 78, 71, 13, 10, 26, 10, 0, 1, 2, 3]) });
  await page.locator("#attachment-upload").click(); await expect(page.locator("#attachment-list")).toContainText("pixel.png"); await expect(page.locator("#prompt")).toHaveValue("@pixel.png");
  const attachments = (await (await request.get(`/v1/topics/${topic.id}/attachments`)).json()); expect(attachments).toHaveLength(1); expect(attachments[0]).toMatchObject({ filename: "pixel.png", media_type: "image/png", byte_size: 12 });
  const content = await (await request.get(`/v1/topics/${topic.id}/attachments/${attachments[0].id}/content`)).json(); expect(Buffer.from(content.content_base64, "base64")).toEqual(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10, 0, 1, 2, 3]));
  const saved = (await (await request.get(`/v1/topics/${topic.id}/prompt-context`)).json()).mentions[0]; expect(saved.metadata).toMatchObject({ attachment_id: attachments[0].id, sha256: attachments[0].sha256 });
  await page.locator("#attachment-list button", { hasText: "删除附件" }).click(); await expect(page.locator("#attachment-list")).not.toContainText("pixel.png"); expect((await (await request.get(`/v1/topics/${topic.id}/attachments`)).json())).toHaveLength(0);
  expect((await (await request.get(`/v1/topics/${topic.id}/messages`)).json()).messages).toHaveLength(0);
});

test("拒绝任意二进制 MIME 和跨话题 attachment_id", async ({ page, request }) => {
  const one = await (await request.post("/v1/topics", { data: { title: `Attachment reject-${Date.now()}` } })).json(); const two = await (await request.post("/v1/topics", { data: { title: `Attachment other-${Date.now()}` } })).json();
  await page.goto(`/ui/?topic=${one.id}`); await page.locator("#api-key").fill("browser-test-key"); await page.locator("#key-form button").click(); await expect(page.locator("#connection")).toHaveClass(/ready/);
  await page.locator("#attachment-file").setInputFiles({ name: "binary.bin", mimeType: "application/octet-stream", buffer: Buffer.from([0, 1, 2]) }); await page.locator("#attachment-upload").click(); await expect(page.locator("#notice")).toHaveClass(/error/); expect((await (await request.get(`/v1/topics/${one.id}/attachments`)).json())).toHaveLength(0);
  const response = await request.post(`/v1/topics/${one.id}/attachments`, { data: { filename: "ok.txt", media_type: "text/plain", content_base64: Buffer.from("ok").toString("base64"), idempotency_key: "cross" } }); const attachment = await response.json();
  const cross = await request.post(`/v1/topics/${two.id}/mentions`, { data: { kind: "file", label: "ok.txt", content: "[附件]", metadata: { attachment_id: attachment.id } } }); expect(cross.status()).toBe(404);
});

test("原生 PNG drop 预览不写入，drop 后存储字节并绑定发送快照", async ({ page, request }) => {
  const topic = await (await request.post("/v1/topics", { data: { title: `Binary drop-${Date.now()}` } })).json();
  await page.goto(`/ui/?topic=${topic.id}`); await page.locator("#api-key").fill("browser-test-key"); await page.locator("#key-form button").click(); await expect(page.locator("#connection")).toHaveClass(/ready/);
  const transfer = await page.evaluateHandle(() => { const data = new DataTransfer(); data.items.add(new File([new Uint8Array([137, 80, 78, 71, 13, 10, 26, 10, 0, 1, 2, 3])], "drop.png", { type: "image/png" })); return data; });
  await page.locator("#composer").dispatchEvent("dragenter", { dataTransfer: transfer }); await expect(page.locator("#drag-preview")).toBeVisible(); expect((await (await request.get(`/v1/topics/${topic.id}/attachments`)).json())).toHaveLength(0);
  await page.locator("#composer").dispatchEvent("drop", { dataTransfer: transfer }); await expect(page.locator("#prompt")).toHaveValue("@drop.png");
  const attachment = (await (await request.get(`/v1/topics/${topic.id}/attachments`)).json())[0]; expect(attachment.byte_size).toBe(12);
  const accepted = page.waitForResponse((response) => response.url().endsWith("/messages") && response.request().method() === "POST"); await page.locator("#send").click(); const run = await (await accepted).json(); await expect(page.locator("#send")).toBeEnabled();
  const history = (await (await request.get(`/v1/runs/${run.id}/messages`)).json()).messages; expect(history[0].details.prompt_context.mentions[0].metadata).toMatchObject({ attachment_id: attachment.id, sha256: attachment.sha256, size: 12 });
  expect(history[0].details.prompt_nodes).toEqual([{ type: "mention", mention_id: history[0].details.prompt_context.mentions[0].id, label: "drop.png" }]);
  expect(JSON.stringify(history[0].details)).not.toContain("content_base64"); await transfer.dispose();
});
