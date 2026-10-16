import { expect, type Page } from "@playwright/test";
import { ensureChineseUi } from "./session-fixtures.js";
export const recoveryHeaders = { Authorization: "Bearer browser-test-key" };
export async function seedConversation(page: Page, options = {}) {
  const response = await page.request.post("/_test/conversation-recovery", { headers: recoveryHeaders, data: options });
  expect(response.ok(), await response.text()).toBeTruthy(); return await response.json();
}
export async function openConversation(page: Page, agent: { agent_id: string; topic_id: string }, locale = "zh-CN") {
  await page.goto(`/ui/?topic=${agent.topic_id}`);
  await page.getByLabel("服务访问密钥", { exact: true }).fill("browser-test-key");
  await page.getByRole("button", { name: "连接", exact: true }).click(); await ensureChineseUi(page);
  if (locale !== "zh-CN") await page.locator("#locale-select").selectOption(locale);
  const card = page.locator(`article.agent-card[data-agent-id="${agent.agent_id}"]`), panel = card.locator(".conversation-recovery");
  await panel.locator("summary").click(); return { card, panel };
}
export async function conversationFacts(page: Page, agentId: string) {
  const response = await page.request.get(`/_test/conversation-recovery/${agentId}/facts`, { headers: recoveryHeaders });
  expect(response.ok(), await response.text()).toBeTruthy(); return await response.json();
}
export async function conversationGate(page: Page, agentId: string, action: string) {
  const response = await page.request.post(`/_test/conversation-recovery/${agentId}/gate`, { headers: recoveryHeaders, data: { action } });
  expect(response.ok(), await response.text()).toBeTruthy();
}
export async function conversationState(page: Page, agentId: string) { return await page.evaluate(id => JSON.parse(localStorage.getItem(`krim-conversation-recovery:${id}`)!), agentId); }
