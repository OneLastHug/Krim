import { expect, test } from "@playwright/test";
import { conversationState, openConversation, recoveryHeaders } from "./conversation-recovery-fixtures.js";

test("真模型效果强杀后浏览器恢复同源unknown，新输入只读，正常新进程重启全部消费者0旧重放", async ({ page }) => {
  const initialResponse = await page.request.get("/_test/conversation-process", { headers: recoveryHeaders }); expect(initialResponse.ok(), await initialResponse.text()).toBeTruthy();
  const original = await initialResponse.json(), writes: string[] = [];
  expect(original).toMatchObject({ phase: "cold", original_sources_same: true, original_messages_same: true, original_events_same: true, original_run_status: "interrupted", workspace_phase: "uncertain", binary_hex: "00ff", replays: 0, new_query_calls: 0, natural_effect_lease_expiry: true }); expect(original.pid).not.toBe(original.producer_pid);
  page.on("request", request => { if (request.method() === "POST" && request.url().includes("/conversation-recovery?")) writes.push(request.postData()!); });
  let { card, panel } = await openConversation(page, original); await expect(panel.locator(".conversation-recovery-preview")).toHaveAttribute("data-available", "true"); await expect(panel).toContainText(original.source_run_id);
  page.once("dialog", dialog => void dialog.accept()); await panel.getByRole("button", { name: "确认恢复上下文", exact: true }).click();
  await expect(panel.locator(".conversation-recovery-record")).toContainText("上下文已恢复"); const record = (await conversationState(page, original.agent_id)).record;
  const factsPath = `/_test/conversation-process?consumers=true&request_key=${record.request.request_key}`;
  let response = await page.request.get(factsPath, { headers: recoveryHeaders }); expect(response.ok(), await response.text()).toBeTruthy();
  let actual = await response.json(); expect(actual).toMatchObject({ recovery_message_id: record.recovery_message_id, inode: original.inode, operation_id: original.operation_id, plan_id: original.plan_id, consumers_same: true, replays: 0, new_query_calls: 0, workspace_phase: "uncertain" });
  await card.getByLabel(`${original.name} 后续任务`, { exact: true }).fill("只读核实原unknown，禁止重发旧执行"); await card.getByRole("button", { name: "继续任务", exact: true }).click(); await expect(card.locator(".agent-result")).toContainText("浏览器显式新轮次只读原unknown");
  response = await page.request.get(factsPath, { headers: recoveryHeaders }); expect(response.ok(), await response.text()).toBeTruthy(); actual = await response.json(); expect(actual).toMatchObject({ new_query_calls: 2, turn: 2, original_sources_same: true, original_events_same: true, original_messages_same: true, replays: 0, workspace_phase: "uncertain" });
  const stopped = await page.request.post("/_test/conversation-process/restart", { headers: recoveryHeaders }); expect(stopped.ok()).toBeTruthy();
  await expect.poll(async () => { try { const response = await page.request.get("/_test/conversation-process", { headers: recoveryHeaders }); return response.ok() ? (await response.json()).phase : "unavailable"; } catch { return "unavailable"; } }, { timeout: 15_000 }).toBe("warm");
  ({ card, panel } = await openConversation(page, original)); await expect(panel.locator(".conversation-recovery-record")).toHaveAttribute("data-message-id", record.recovery_message_id); await expect(panel).toContainText("当前上下文完整，无需恢复");
  response = await page.request.get(factsPath, { headers: recoveryHeaders }); expect(response.ok(), await response.text()).toBeTruthy(); const warm = await response.json(); expect(warm.pid).not.toBe(actual.pid);
  expect(warm).toMatchObject({ phase: "warm", recovery_message_id: record.recovery_message_id, inode: original.inode, binary_hex: "00ff", turn: 2, new_query_calls: 0, replays: 0, consumers_same: true, original_sources_same: true, original_messages_same: true, original_events_same: true, workspace_phase: "uncertain" }); expect(writes).toHaveLength(1);
  await test.info().attach("conversation-recovery-process-proof", { body: JSON.stringify({ original, cold: actual, warm }, null, 2), contentType: "application/json" });
});
