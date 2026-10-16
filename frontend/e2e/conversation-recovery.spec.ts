import { expect, test } from "@playwright/test";
import { conversationFacts, conversationGate, conversationState, openConversation, recoveryHeaders, seedConversation } from "./conversation-recovery-fixtures.js";

test("无工作区原组审阅/取消确认/恢复/原键刷新/显式新输入保持历史与草稿", async ({ page }) => {
  const agent = await seedConversation(page), posts: string[] = [];
  page.on("request", request => { if (request.method() === "POST" && request.url().includes("/conversation-recovery?")) posts.push(request.postData()!); });
  let { card, panel } = await openConversation(page, agent);
  await expect(panel.locator(".conversation-recovery-preview")).toHaveAttribute("data-available", "true");
  await expect(panel.locator(".conversation-pending-calls li")).toHaveCount(1); await expect(panel.locator(".conversation-answered-calls li")).toHaveCount(1);
  await expect(panel).toContainText("原运行中断"); expect(agent.session.permissions.workspace).toBeFalsy();
  const input = card.getByLabel("contextworker 后续任务", { exact: true }); await input.fill("保留恢复前草稿");
  page.once("dialog", dialog => void dialog.dismiss()); await panel.getByRole("button", { name: "确认恢复上下文", exact: true }).click();
  await expect(panel.getByRole("button", { name: "确认恢复上下文", exact: true })).toBeEnabled(); expect(posts).toHaveLength(0);
  const before = await conversationFacts(page, agent.agent_id); expect(before.provider_calls).toBe(0); expect(before.messages).toBe(before.original_messages);
  let confirmation = ""; page.once("dialog", dialog => { confirmation = dialog.message(); void dialog.accept(); });
  await panel.getByRole("button", { name: "确认恢复上下文", exact: true }).click();
  await expect(panel.locator(".conversation-recovery-record")).toContainText("上下文已恢复"); await expect(panel).toHaveAttribute("data-pending", "false");
  const state = await conversationState(page, agent.agent_id), record = state.record;
  expect(confirmation).toContain(record.source.source_path_hash); expect(confirmation).toContain(`r${record.source.session_revision}`);
  expect(Object.keys(JSON.parse(posts[0]!)).sort()).toEqual(["expected_path_hash", "expected_session_revision", "request_key", "source_run_id"]);
  await expect(input).toHaveValue("保留恢复前草稿");
  expect(await conversationFacts(page, agent.agent_id)).toMatchObject({ messages: before.original_messages + 1, provider_calls: 0, original_run_status: "interrupted", original_frames_same: true, turn: 1 });
  ({ card, panel } = await openConversation(page, agent)); await expect(panel.locator(".conversation-recovery-record")).toHaveAttribute("data-message-id", record.recovery_message_id); expect(posts).toHaveLength(1);
  await card.getByLabel("contextworker 后续任务", { exact: true }).fill("新的明确只读输入"); await card.getByRole("button", { name: "继续任务", exact: true }).click(); await expect(card.locator(".agent-result")).toContainText("新的明确只读输入");
  const after = await conversationFacts(page, agent.agent_id); expect(after).toMatchObject({ provider_calls: 1, turn: 2, original_frames_same: true });
  await panel.getByLabel("会话恢复请求键", { exact: true }).fill(record.request.request_key); await panel.getByRole("button", { name: "查询会话恢复历史", exact: true }).click();
  await expect(panel.locator(".conversation-recovery-record")).toHaveAttribute("data-message-id", record.recovery_message_id);
  expect(await conversationFacts(page, agent.agent_id)).toEqual(after); expect(posts).toHaveLength(1);
});

test("完整上下文不提供恢复写入", async ({ page }) => {
  const response = await page.request.post("/v1/agents", { headers: recoveryHeaders, data: { name: "default" } }); expect(response.ok()).toBeTruthy();
  const agent = await response.json(), { panel } = await openConversation(page, agent);
  await expect(panel).toContainText("当前上下文完整，无需恢复"); await expect(panel.getByRole("button", { name: "确认恢复上下文", exact: true })).toBeDisabled();
});

test("恢复写入中禁用新输入，断线刷新只查询原键等待同一真实提交", async ({ page }) => {
  const agent = await seedConversation(page), writes: string[] = [];
  page.on("request", request => { if (request.method() === "POST" && request.url().includes("/conversation-recovery?")) writes.push(request.postData()!); });
  let { card, panel } = await openConversation(page, agent); await expect(panel.locator(".conversation-recovery-preview")).toHaveAttribute("data-available", "true");
  await conversationGate(page, agent.agent_id, "hold"); page.once("dialog", dialog => void dialog.accept()); await panel.getByRole("button", { name: "确认恢复上下文", exact: true }).click();
  await conversationGate(page, agent.agent_id, "entered"); await expect(card.getByRole("button", { name: "继续任务", exact: true })).toBeDisabled();
  const blocked = await page.request.post(`/v1/agents/${agent.agent_id}/execute`, { headers: recoveryHeaders, data: { prompt: "不能插入新任务", idempotency_key: "browser-overlap" } }); expect(blocked.status()).toBe(409);
  const pending = (await conversationState(page, agent.agent_id)).pending; expect(pending.request.request_key).toBeTruthy();
  ({ card, panel } = await openConversation(page, agent)); await expect(panel).toHaveAttribute("data-pending", "true"); await expect(card.getByRole("button", { name: "继续任务", exact: true })).toBeDisabled(); expect(writes).toHaveLength(1);
  await conversationGate(page, agent.agent_id, "release");
  await expect.poll(async () => await (await page.request.get(`/v1/agents/${agent.agent_id}/conversation-recovery/by-key?request_key=${pending.request.request_key}`, { headers: recoveryHeaders })).json()).not.toBeNull();
  await panel.getByRole("button", { name: "查询原会话恢复请求", exact: true }).click(); await expect(panel.locator(".conversation-recovery-record")).toContainText("上下文已恢复");
  await expect(panel).toHaveAttribute("data-pending", "false"); expect(writes).toHaveLength(1); expect(await conversationFacts(page, agent.agent_id)).toMatchObject({ provider_calls: 0, turn: 1 });
});

for (const locale of ["zh-CN", "en-US"]) test(`${locale}420px纯文本审阅，不把工具引用当HTML`, async ({ page }) => {
  await page.setViewportSize({ width: 420, height: 900 }); const agent = await seedConversation(page, { unsafe_text: true });
  const { panel } = await openConversation(page, agent, locale); await expect(panel.locator(".conversation-recovery-preview")).toHaveAttribute("data-available", "true");
  await expect(panel.locator(".conversation-pending-calls")).toContainText("<img"); await expect(panel.locator("img")).toHaveCount(0); expect(await page.evaluate(() => (window as unknown as { krimInjected?: boolean }).krimInjected)).toBeUndefined();
  await expect(panel.getByRole("button", { name: locale === "en-US" ? "Confirm context recovery" : "确认恢复上下文", exact: true })).toBeEnabled();
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(420); expect(await panel.evaluate(node => node.scrollWidth <= node.clientWidth)).toBeTruthy();
  await test.info().attach(`conversation-${locale}-420`, { body: await panel.screenshot(), contentType: "image/png" });
});

test("关闭重开会话保留输入与原组，关闭时不能确认恢复", async ({ page }) => {
  const agent = await seedConversation(page), { card, panel } = await openConversation(page, agent);
  await expect(panel.getByRole("button", { name: "确认恢复上下文", exact: true })).toBeEnabled();
  await card.getByLabel("contextworker 后续任务", { exact: true }).fill("关闭前草稿"); await panel.getByLabel("会话恢复请求键", { exact: true }).fill("保留原键输入");
  await card.getByRole("button", { name: "关闭会话", exact: true }).click(); await expect(card).toContainText("会话关闭");
  await expect(panel.getByRole("button", { name: "确认恢复上下文", exact: true })).toBeDisabled();
  await expect(panel.getByLabel("会话恢复请求键", { exact: true })).toHaveValue("保留原键输入");
  await card.getByRole("button", { name: "重开会话", exact: true }).click(); await expect(card).toContainText("会话开启"); await panel.getByRole("button", { name: "读取会话恢复快照", exact: true }).click();
  await expect(panel.getByRole("button", { name: "确认恢复上下文", exact: true })).toBeEnabled(); await expect(card.getByLabel("contextworker 后续任务", { exact: true })).toHaveValue("关闭前草稿");
  const facts = await conversationFacts(page, agent.agent_id); expect(facts.messages).toBe(facts.original_messages); expect(facts.provider_calls).toBe(0);
});

test("真实原信号量排队禁止恢复，另一活模型仍可按原恢复键读历史", async ({ page }) => {
  const running = await seedConversation(page), queued = await seedConversation(page);
  async function preview(agentId: string) { return await (await page.request.get(`/v1/agents/${agentId}/conversation-recovery/preview`, { headers: recoveryHeaders })).json(); }
  const first = await preview(running.agent_id), target = await preview(queued.agent_id);
  const request = { source_run_id: first.source_run_id, expected_path_hash: first.source_path_hash, expected_session_revision: first.session_revision, request_key: "active-history-key" };
  const applied = await page.request.post(`/v1/agents/${running.agent_id}/conversation-recovery?confirmed=true`, { headers: recoveryHeaders, data: request }); expect(applied.status()).toBe(202); const record = await applied.json();
  await conversationGate(page, running.agent_id, "hold-model");
  try {
    const started = await page.request.post(`/v1/agents/${running.agent_id}/start`, { headers: recoveryHeaders, data: { prompt: "受控的新输入", idempotency_key: "active-history-model" } }); expect(started.status()).toBe(202); await conversationGate(page, running.agent_id, "model-entered");
    const second = await page.request.post(`/v1/agents/${queued.agent_id}/start`, { headers: recoveryHeaders, data: { prompt: "等待原信号量", idempotency_key: "actual-queued-model" } }); expect(second.status()).toBe(202);
    await expect.poll(async () => (await (await page.request.get(`/v1/agents/${queued.agent_id}`, { headers: recoveryHeaders })).json()).status).toBe("queued");
    const { panel } = await openConversation(page, queued); await expect(panel).toContainText("Agent正在执行、排队或会话已关闭"); await expect(panel.getByRole("button", { name: "确认恢复上下文", exact: true })).toBeDisabled();
    const denied = await page.request.post(`/v1/agents/${queued.agent_id}/conversation-recovery?confirmed=true`, { headers: recoveryHeaders, data: { source_run_id: target.source_run_id, expected_path_hash: target.source_path_hash, expected_session_revision: target.session_revision, request_key: "queued-denied-key" } }); expect(denied.status()).toBe(409);
    const runningPanel = page.locator(`article.agent-card[data-agent-id="${running.agent_id}"] .conversation-recovery`); await runningPanel.locator("summary").click();
    await runningPanel.getByLabel("会话恢复请求键", { exact: true }).fill(request.request_key); await runningPanel.getByRole("button", { name: "查询会话恢复历史", exact: true }).click();
    await expect(runningPanel.locator(".conversation-recovery-record")).toHaveAttribute("data-message-id", record.recovery_message_id); expect((await conversationFacts(page, running.agent_id)).provider_calls).toBe(1);
  } finally { await conversationGate(page, running.agent_id, "release-model"); }
});
