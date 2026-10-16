import { expect, test } from "@playwright/test";
import { conversationFacts, conversationState, openConversation, recoveryHeaders, seedConversation } from "./conversation-recovery-fixtures.js";

for (const fault of ["agent", "topic", "revision", "private", "answered"]) test(`浏览器拒绝源快照${fault}且不发送恢复`, async ({ page }) => {
  const agent = await seedConversation(page), posts: string[] = [];
  page.on("request", request => { if (request.method() === "POST" && request.url().includes("/conversation-recovery?")) posts.push(request.url()); });
  await page.route("**/conversation-recovery/preview", async route => {
    const response = await route.fetch(), value = await response.json();
    if (fault === "agent") value.agent_id = "another-agent";
    if (fault === "topic") value.topic_id = "another-topic";
    if (fault === "revision") value.session_revision++;
    if (fault === "private") value.owner_id = "private-owner";
    if (fault === "answered") value.answered_calls[0].message_id = "another-message";
    await route.fulfill({ response, json: value });
  });
  const { panel } = await openConversation(page, agent);
  await expect(panel.locator(".conversation-recovery-notice")).toContainText("会话恢复回程与原事实不一致"); await expect(panel.getByRole("button", { name: "确认恢复上下文", exact: true })).toBeDisabled(); expect(posts).toHaveLength(0);
  const facts = await conversationFacts(page, agent.agent_id); expect(facts.messages).toBe(facts.original_messages); expect(facts.provider_calls).toBe(0);
});

for (const fault of ["post-id", "get-key", "get-hash", "get-done", "get-json", "get-private", "get-error"]) test(`浏览器拒绝${fault}且原意图由可信原键回读后才释放`, async ({ page }) => {
  const agent = await seedConversation(page), posts: string[] = [];
  page.on("request", request => { if (request.method() === "POST" && request.url().includes("/conversation-recovery?")) posts.push(request.url()); });
  const routePattern = fault === "post-id" ? "**/conversation-recovery?confirmed=true" : "**/conversation-recovery/by-key?*";
  await page.route(routePattern, async route => {
    const response = await route.fetch();
    if (fault === "get-json") { await route.fulfill({ response, body: "{broken" }); return; }
    if (fault === "get-error") { await route.fulfill({ status: 409, json: { error: { code: "conversation_recovery_corrupt", message: "原查询暂不可核实" } } }); return; }
    const value = await response.json();
    if (fault === "post-id") value.recovery_message_id = "00000000-0000-5000-8000-000000000000";
    if (fault === "get-key") value.request.request_key = "different-key";
    if (fault === "get-hash") value.request.expected_path_hash = "f".repeat(64);
    if (fault === "get-done") value.status = "done";
    if (fault === "get-private") value.owner_id = "private-owner";
    await route.fulfill({ response, json: value });
  });
  const { card, panel } = await openConversation(page, agent); await expect(panel.getByRole("button", { name: "确认恢复上下文", exact: true })).toBeEnabled();
  page.once("dialog", dialog => void dialog.accept()); await panel.getByRole("button", { name: "确认恢复上下文", exact: true }).click();
  await expect(panel.locator(".conversation-recovery-notice")).toContainText("原会话恢复请求尚未确认");
  await expect(panel.getByRole("button", { name: "读取会话恢复快照", exact: true })).toBeEnabled();
  await expect(panel).toHaveAttribute("data-pending", "true"); await expect(card.getByRole("button", { name: "继续任务", exact: true })).toBeDisabled();
  const pending = (await conversationState(page, agent.agent_id)).pending; expect(pending.rejected).toBe(false);
  await page.unroute(routePattern); await panel.getByRole("button", { name: "查询原会话恢复请求", exact: true }).click();
  await expect(panel.locator(".conversation-recovery-record")).toContainText("上下文已恢复"); await expect(panel).toHaveAttribute("data-pending", "false"); expect(posts).toHaveLength(1);
  const facts = await conversationFacts(page, agent.agent_id); expect(facts.messages).toBe(facts.original_messages + 1); expect(facts.provider_calls).toBe(0);
});

test("未发出的丢请求和合法null保持原键，刷新不自动重发", async ({ page }) => {
  const agent = await seedConversation(page), posts: string[] = [];
  page.on("request", request => { if (request.method() === "POST" && request.url().includes("/conversation-recovery?")) posts.push(request.url()); });
  await page.route("**/conversation-recovery?confirmed=true", route => route.abort("failed"));
  let { panel } = await openConversation(page, agent); await expect(panel.getByRole("button", { name: "确认恢复上下文", exact: true })).toBeEnabled();
  page.once("dialog", dialog => void dialog.accept()); await panel.getByRole("button", { name: "确认恢复上下文", exact: true }).click(); await expect(panel.locator(".conversation-recovery-notice")).toContainText("原会话恢复请求尚未确认");
  const pending = (await conversationState(page, agent.agent_id)).pending;
  ({ panel } = await openConversation(page, agent)); await expect(panel).toHaveAttribute("data-pending", "true"); await expect(panel.getByRole("button", { name: "查询原会话恢复请求", exact: true })).toBeEnabled();
  expect((await conversationState(page, agent.agent_id)).pending.request).toEqual(pending.request); expect(posts).toHaveLength(1);
  const facts = await conversationFacts(page, agent.agent_id); expect(facts.messages).toBe(facts.original_messages); expect(facts.provider_calls).toBe(0);
});

for (const fault of ["json", "version", "identity", "private", "oversize"]) test(`浏览器坏存储${fault}停止恢复写入`, async ({ page }) => {
  const agent = await seedConversation(page), posts: string[] = [];
  const identity = { agent_id: agent.agent_id, session_id: agent.session_id, topic_id: agent.topic_id };
  const value: Record<string, unknown> = { version: 1, ...identity, pending: null, record: null };
  if (fault === "version") value.version = 2;
  if (fault === "identity") value.session_id = "another-session";
  if (fault === "private") value.owner_id = "private-owner";
  const raw = fault === "json" ? "{bad" : fault === "oversize" ? "x".repeat(131073) : JSON.stringify(value);
  await page.addInitScript(({ id, raw }) => localStorage.setItem(`krim-conversation-recovery:${id}`, raw), { id: agent.agent_id, raw });
  page.on("request", request => { if (request.method() === "POST" && request.url().includes("/conversation-recovery?")) posts.push(request.url()); });
  const { panel } = await openConversation(page, agent); await expect(panel).toContainText("会话恢复记录不可用，写操作已停用"); await expect(panel.getByRole("button", { name: "确认恢复上下文", exact: true })).toBeDisabled(); expect(posts).toHaveLength(0);
});

test("localStorage静默丢写在POST前停住", async ({ page }) => {
  const agent = await seedConversation(page), posts: string[] = [];
  await page.addInitScript(() => { const original = Storage.prototype.setItem; Storage.prototype.setItem = function (key, value) { if (!key.startsWith("krim-conversation-recovery:")) original.call(this, key, value); }; });
  page.on("request", request => { if (request.method() === "POST" && request.url().includes("/conversation-recovery?")) posts.push(request.url()); });
  const { panel } = await openConversation(page, agent); await expect(panel.getByRole("button", { name: "确认恢复上下文", exact: true })).toBeEnabled();
  page.once("dialog", dialog => void dialog.accept()); await panel.getByRole("button", { name: "确认恢复上下文", exact: true }).click();
  await expect(panel).toContainText("会话恢复记录不可用，写操作已停用"); expect(posts).toHaveLength(0); const facts = await conversationFacts(page, agent.agent_id); expect(facts.messages).toBe(facts.original_messages);
});

test("后端版本已推进会拒绝旧确认，原键查无记录才释放明确拒绝意图", async ({ page }) => {
  const agent = await seedConversation(page), { panel } = await openConversation(page, agent);
  await expect(panel.getByRole("button", { name: "确认恢复上下文", exact: true })).toBeEnabled();
  await page.route("**/conversation-recovery?confirmed=true", async route => {
    const original = route.request().postDataJSON();
    const result = await page.request.post(`/v1/agents/${agent.agent_id}/conversation-recovery?confirmed=true`, { headers: recoveryHeaders, data: { ...original, request_key: "host-advanced-original" } }); expect(result.status()).toBe(202);
    await route.continue();
  });
  page.once("dialog", dialog => void dialog.accept()); await panel.getByRole("button", { name: "确认恢复上下文", exact: true }).click(); await expect(panel).toHaveAttribute("data-pending", "true");
  await expect(panel.getByRole("button", { name: "读取会话恢复快照", exact: true })).toBeEnabled(); expect((await conversationState(page, agent.agent_id)).pending.rejected).toBe(true);
  await panel.getByRole("button", { name: "查询原会话恢复请求", exact: true }).click(); await expect(panel).toHaveAttribute("data-pending", "false"); await expect(panel).toContainText("当前上下文完整，无需恢复");
  const facts = await conversationFacts(page, agent.agent_id); expect(facts.messages).toBe(facts.original_messages + 1); expect(facts.provider_calls).toBe(0);
});

test("已观察到另一个页面的原意图时不能用旧缓存覆盖或再POST", async ({ page }) => {
  const agent = await seedConversation(page), { panel } = await openConversation(page, agent), posts: string[] = [];
  await expect(panel.getByRole("button", { name: "确认恢复上下文", exact: true })).toBeEnabled();
  const source = await (await page.request.get(`/v1/agents/${agent.agent_id}/conversation-recovery/preview`, { headers: recoveryHeaders })).json();
  const state = { version: 1, agent_id: agent.agent_id, session_id: agent.session_id, topic_id: agent.topic_id, record: null, pending: { source, request: { source_run_id: source.source_run_id, expected_path_hash: source.source_path_hash, expected_session_revision: source.session_revision, request_key: "other-page-original-key" }, rejected: false } };
  await page.evaluate(({ id, state }) => localStorage.setItem(`krim-conversation-recovery:${id}`, JSON.stringify(state)), { id: agent.agent_id, state });
  page.on("request", request => { if (request.method() === "POST" && request.url().includes("/conversation-recovery?")) posts.push(request.url()); });
  page.once("dialog", dialog => void dialog.accept()); await panel.getByRole("button", { name: "确认恢复上下文", exact: true }).click();
  await expect(panel).toContainText("会话恢复记录不可用，写操作已停用"); expect(posts).toHaveLength(0); expect(await conversationState(page, agent.agent_id)).toEqual(state);
  const facts = await conversationFacts(page, agent.agent_id); expect(facts.messages).toBe(facts.original_messages); expect(facts.provider_calls).toBe(0);
});
