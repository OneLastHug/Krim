import { test, expect } from "@playwright/test";

test("公开多路 SSE 保持各 Run 的序号和话题关联", async ({ playwright, baseURL }) => {
  const request = await playwright.request.newContext({ baseURL, extraHTTPHeaders: { Authorization: "Bearer browser-test-key" } });
  try {
    const topics = [] as Array<{ id: string; title: string }>;
    for (let index = 0; index < 2; index++) topics.push(await (await request.post("/v1/topics", { data: { title: `多路-${Date.now()}-${index}` } })).json());
    const runs = [] as Array<{ id: string; topic_id: string }>;
    for (const [index, topic] of topics.entries()) runs.push(await (await request.post(`/v1/topics/${topic.id}/messages`, { data: { text: `后台 ${index}`, idempotency_key: `mux-${index}` } })).json());
    const response = await request.post("/v1/runs/events", { data: { sources: runs.map((run) => ({ run_id: run.id, topic_id: run.topic_id, next_seq: 0 })) } });
    expect(response.ok()).toBeTruthy();
    const frames = (await response.text()).split("\n\n").filter((block) => block.includes("event: run-frame")).map((block) => JSON.parse(block.split("data: ")[1].split("\n")[0])) as Array<{ topic_id: string; frame: { stream_id: string; seq: number; kind: string } }>;
    expect(new Set(frames.map((item) => item.frame.stream_id)).size).toBe(2);
    for (const run of runs) {
      const source = frames.filter((item) => item.frame.stream_id === run.id);
      expect(source.map((item) => item.frame.seq)).toEqual([...Array(source.length).keys()]);
      expect(source.every((item) => item.topic_id === run.topic_id)).toBeTruthy();
      expect(source.at(-1)?.frame.kind).toBe("end");
    }
  } finally { await request.dispose(); }
});

test("后台生成时可切换话题并独立停止", async ({ page, playwright, baseURL }) => {
  const api = await playwright.request.newContext({ baseURL, extraHTTPHeaders: { Authorization: "Bearer browser-test-key" } });
  const topics = [] as Array<{ id: string; title: string }>;
  const runs = [] as Array<{ id: string }>;
  try {
    for (let i = 0; i < 2; i++) topics.push(await (await api.post("/v1/topics", { data: { title: `后台独立-${Date.now()}-${i}` } })).json());
    for (const topic of topics) runs.push(await (await api.post(`/v1/topics/${topic.id}/messages`, { data: { text: "x".repeat(4000), idempotency_key: `bg-${topic.id}` } })).json());
    await page.goto(`/ui/?topic=${topics[0].id}`);
    await page.getByLabel("服务访问密钥").fill("browser-test-key");
    await page.getByRole("button", { name: "连接", exact: true }).click();
    await expect(page.locator("#topic-heading")).toHaveText(topics[0].title);
    await expect(page.locator("#prompt")).toBeDisabled();
    await page.locator(".topic-button").filter({ hasText: topics[1].title }).click();
    await expect(page.locator("#topic-heading")).toHaveText(topics[1].title);
    await expect(page.locator("#run-stop")).toBeEnabled();
    await page.locator("#run-stop").click();
    await expect.poll(async () => (await (await api.get(`/v1/runs/${runs[1].id}`)).json()).status).toBe("aborted");
    expect((await (await api.get(`/v1/runs/${runs[0].id}`)).json()).status).toBe("running");
    await page.locator(".topic-button").filter({ hasText: topics[0].title }).click();
    await expect(page.locator("#topic-heading")).toHaveText(topics[0].title);
    await expect(page.locator("#prompt")).toBeDisabled();
  } finally {
    for (const run of runs) await api.post(`/v1/runs/${run.id}/stop`);
    await api.dispose();
  }
});
