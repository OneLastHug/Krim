import { expect, test, type Locator, type Page, type TestInfo } from "@playwright/test";
import { attachJsonEvidence, screenshotStep } from "./step-screenshot.js";

type Theme = "warm" | "bright" | "dark";
async function selectTheme(page: Page, theme: Theme) {
  const labels: Record<Theme, string> = { warm: "暖纸", bright: "亮彩", dark: "深色" };
  await page.getByRole("button", { name: "选择显示主题", exact: true }).click();
  await page.locator(".theme-options").getByRole("button", { name: new RegExp("^" + labels[theme]) }).click();
  await expect(page.locator("html")).toHaveAttribute("data-theme", theme);
}
async function openDisclosure(page: Page, label: string): Promise<Locator> {
  const details = page.locator("details.product-eval-editor").filter({ hasText: label });
  await expect(details).toHaveCount(1);
  if (await details.getAttribute("open") === null) await details.locator("summary").click();
  return details;
}

const scope = "/v4/workspaces/local/eval";
function steps(page: Page, info: TestInfo, name: string, theme: Theme = "warm") {
  return (label: string, action: () => Promise<void>) => screenshotStep(page, info,
    { caseName: name, stepName: label, theme }, action);
}
async function startTask(page: Page, info: TestInfo, target: string) {
  const launchDetails = await openDisclosure(page, "新建评测");
  await page.locator("#product-target").selectOption(target);
  await page.getByRole("checkbox", { name: /Krim 执行结果/ }).check();
  const launch = launchDetails.getByRole("button", { name: "启动评测", exact: true });
  await expect(launch).toBeEnabled();
  const created = page.waitForResponse(response => response.url().endsWith(scope + "/tasks") && response.request().method() === "POST");
  await launch.click();
  const response = await created;
  expect(response.status()).toBe(202);
  const task = await response.json();
  await attachJsonEvidence(info, `创建评测-${target}.json`, task);
  await expect(page.locator(".product-eval-task-facts")).toContainText(task.id);
  await expect(page.getByRole("button", { name: "查看报告", exact: true })).toBeVisible({ timeout: 20000 });
  return task;
}

test("P3：多样本冻结、真实执行与版本退化比较", async ({ page }, info) => {
  test.setTimeout(90000);
  await page.setViewportSize({ width: 1440, height: 1000 });
  const step = steps(page, info, "P3-多样本与版本比较");
  const name = `browser-${Date.now()}`;
  let baselineId = "", candidateId = "";
  await step("打开工作区数据集", async () => {
    await page.goto("/ui/?view=datasets&workspace=local");
    await selectTheme(page, "warm");
    await expect(page.getByRole("heading", { name: "已冻结的数据集" })).toBeVisible();
    await expect(page.locator("#product-dataset-name")).toBeHidden();
  });
  await step("填写人工样本并加入列表", async () => {
    await openDisclosure(page, "新建数据集");
    await expect(page.getByRole("button", { name: "加入样本", exact: true })).toBeEnabled();
    await page.locator("#product-dataset-name").fill(name);
    await page.locator("#product-dataset-version").fill("v1");
    await page.locator("#product-instruction").fill("/add 2.5 3.5");
    await page.locator("#product-input-file").fill("/add 2.5 3.5");
    await page.locator("#product-expected").fill("6");
    await page.getByRole("button", { name: "加入样本", exact: true }).click();
    await expect(page.getByRole("list", { name: "样本草稿" }).locator("li")).toHaveCount(1);
  });
  await step("补充第二个独立参考", async () => {
    await page.locator("#product-instruction").fill("/add 3.5 4.5");
    await page.locator("#product-input-file").fill("/add 3.5 4.5");
    await page.locator("#product-expected").fill("8");
    await page.getByRole("button", { name: "加入样本", exact: true }).click();
    await expect(page.getByRole("list", { name: "样本草稿" }).locator("li")).toHaveCount(2);
  });
  await step("冻结真实数据集版本", async () => {
    await page.getByRole("button", { name: "冻结 2 个样本", exact: true }).click();
    const row = page.locator(".product-eval-list-row").filter({ hasText: name });
    await expect(row.getByRole("cell").nth(2)).toHaveText("2");
    const saved = await page.request.get(`${scope}/datasets/${name}/v1`);
    expect(saved.ok()).toBe(true);
    const data = await saved.json();
    expect(data.cases).toHaveLength(2);
    expect(Object.fromEntries(data.cases.map((item: { instruction: string; reference: { expected: string } }) => [item.instruction, item.reference.expected]))).toEqual({ "/add 2.5 3.5": "6", "/add 3.5 4.5": "8" });
    await attachJsonEvidence(info, "冻结的人工数据集.json", data);
  });
  await step("选择数据集与真实运行目标", async () => {
    await page.getByRole("button", { name: "导航到评测", exact: true }).click();
    await expect(page.getByRole("heading", { name: "评测任务" })).toBeVisible();
    const launchDetails = await openDisclosure(page, "新建评测");
    await page.locator("#product-dataset-choice").selectOption(`${name}/v1`);
    await page.locator("#product-target").selectOption("krim-script-v2");
    await page.getByRole("checkbox", { name: /Krim 执行结果/ }).check();
    await expect(launchDetails.getByRole("button", { name: "启动评测", exact: true })).toBeEnabled();
  });
  await step("执行精度正确版本并读取报告", async () => {
    baselineId = (await startTask(page, info, "krim-script-v2")).id;
    await page.getByRole("button", { name: "查看报告", exact: true }).click();
    await expect(page.locator(".product-eval-report-row")).toHaveCount(2);
    await expect(page.locator(".product-eval-report-summary")).toContainText("100%");
    await expect(page.locator(".product-eval-report-row.is-failed")).toHaveCount(0);
    await page.locator(".product-eval-report").scrollIntoViewIfNeeded();
  });
  await step("执行整数解析版本并显示失败样本", async () => {
    candidateId = (await startTask(page, info, "krim-script-v1")).id;
    await page.getByRole("button", { name: "查看报告", exact: true }).click();
    await expect(page.locator(".product-eval-report-row.is-failed")).toHaveCount(2);
    await page.locator(".product-eval-report").scrollIntoViewIfNeeded();
  });
  await step("比较两份持久报告并定位退化", async () => {
    await page.locator("#product-baseline").selectOption(baselineId);
    await page.locator("#product-candidate-task").selectOption(candidateId);
    const result = page.waitForResponse(response => response.url().endsWith(scope + "/tasks/compare"));
    await page.getByRole("button", { name: "比较基线与候选", exact: true }).click();
    const compared = await (await result).json();
    expect(compared.status).toBe("fail");
    expect(compared.reasons.length).toBeGreaterThan(0);
    await expect(page.locator(".product-eval-comparison")).toContainText("比较未通过");
    await attachJsonEvidence(info, "真实退化比较.json", compared);
  });
  await step("刷新深链接恢复当前任务", async () => {
    expect(new URL(page.url()).searchParams.get("product_eval_id")).toBe(candidateId);
    await page.reload();
    await expect(page.locator(".product-eval-task-facts")).toContainText(candidateId);
    await page.getByRole("button", { name: "查看报告", exact: true }).click();
    await expect(page.locator(".product-eval-report-row.is-failed")).toHaveCount(2);
    await page.locator(".product-eval-report").scrollIntoViewIfNeeded();
  });
});

test("P3：窄屏深色报告与刷新恢复", async ({ page }, info) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const step = steps(page, info, "P3-窄屏报告", "dark");
  const response = await page.request.get(`${scope}/tasks?offset=0&limit=20`);
  expect(response.ok()).toBe(true);
  const tasks = await response.json();
  const task = tasks.find((row: { status: string; report_hash: string | null }) => row.status === "completed" && row.report_hash);
  expect(task).toBeTruthy();
  await step("打开持久任务深链接", async () => {
    await page.goto(`/ui/?view=eval&workspace=local&product_eval_id=${encodeURIComponent(task.id)}`);
    await expect(page.locator(".product-eval-task-facts")).toContainText(task.id);
    await selectTheme(page, "dark");
    await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  });
  await step("读取窄屏报告并核对布局", async () => {
    await page.getByRole("button", { name: "查看报告", exact: true }).click();
    await expect(page.locator(".product-eval-report-row").first()).toBeVisible();
    await page.locator(".product-eval-report").scrollIntoViewIfNeeded();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
    expect(await page.locator(".product-eval-workbench").count()).toBe(1);
    expect(await page.locator(".product-eval-report-row").evaluateAll(rows => rows.every(row => {
      const title = row.querySelector("header strong")!.getBoundingClientRect();
      const card = row.getBoundingClientRect();
      return title.left >= card.left && title.right <= card.right;
    }))).toBe(true);
  });
  await step("刷新后保留深色主题与任务", async () => {
    await page.reload();
    await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
    await expect(page.locator(".product-eval-task-facts")).toContainText(task.id);
    await page.getByRole("button", { name: "查看报告", exact: true }).click();
    await expect(page.locator(".product-eval-report-row").first()).toBeVisible();
    await page.locator(".product-eval-report").scrollIntoViewIfNeeded();
  });
});


test("P3：对话结果直达候选、人工确认与连续报告", async ({ page, context }, info) => {
  test.setTimeout(90000);
  await page.setViewportSize({ width: 1440, height: 1000 });
  const step = steps(page, info, "P3-来源到连续评测");
  const name = `source-${Date.now()}`;
  let runId = "", taskId = "";
  await step("创建来源对话", async () => {
    await page.goto("/ui/");
    await selectTheme(page, "warm");
    await page.getByRole("button", { name: "新建对话", exact: true }).click();
    await page.locator("#session-title").fill("人工参考来源");
    await page.getByRole("dialog").getByRole("button", { name: "创建对话", exact: true }).click();
    await expect(page.locator("#chat-prompt")).toBeEnabled();
  });
  await step("运行真实加法任务", async () => {
    await page.locator("#chat-prompt").fill("/add 2.5 3.5");
    await page.locator(".chat-compose-actions").getByRole("button", { name: "发送", exact: true }).click();
    await expect.poll(async () => {
      const sessionId = new URL(page.url()).searchParams.get("session");
      const turnId = await page.locator(".chat-turn").last().getAttribute("data-turn-id");
      expect(sessionId).toBeTruthy(); expect(turnId).toBeTruthy();
      const response = await page.request.get(`/v2/workspaces/local/sessions/${encodeURIComponent(sessionId!)}/turns/${encodeURIComponent(turnId!)}`);
      expect(response.ok()).toBe(true);
      const turn = await response.json();
      expect(turn.id).toBe(turnId); expect(turn.session_id).toBe(sessionId);
      return turn.status;
    }).toBe("completed");
    await expect(page.locator(".chat-answer").last()).toHaveText("6");
    await expect(page.locator(".chat-turn").last().getByRole("button", { name: "更多操作", exact: true })).toBeVisible();
    await expect(page.getByRole("button", { name: "评测此结果", exact: true })).toHaveCount(0);
  });
  await step("直接进入来源候选而不复制标识", async () => {
    const last = page.locator(".chat-turn").last();
    await last.getByRole("button", { name: "更多操作", exact: true }).click();
    await expect(last.locator(".chat-detail")).toContainText("工具摘要");
    await last.getByRole("button", { name: "查看 Trace", exact: true }).click();
    await expect(page.locator(".topbar-title h1")).toHaveText("Trace");
    await expect(page.locator(".trace-span-node").first()).toBeVisible();
    await page.getByRole("button", { name: "加入数据集", exact: true }).click();
    runId = new URL(page.url()).searchParams.get("source_run") ?? "";
    expect(runId).toBeTruthy();
    await expect(page.locator("#product-source-run")).toHaveValue(runId);
    await page.reload();
    await expect(page.locator("#product-source-run")).toHaveValue(runId);
  });
  await step("导入候选并检查资格", async () => {
    await page.getByRole("button", { name: "导入候选", exact: true }).click();
    await expect(page.getByRole("list", { name: "导入的候选" }).locator("li")).toHaveCount(1);
    await page.getByRole("button", { name: "补充参考", exact: true }).click();
    await expect(page.locator("#product-candidate-instruction")).toBeVisible();
  });
  await step("明确填写人工参考与初态", async () => {
    await expect(page.locator("#product-confirmed-dataset-name")).toBeVisible();
    await expect(page.locator("#product-confirmed-dataset-version")).toBeVisible();
    await expect(page.locator("#product-dataset-name")).toBeHidden();
    await page.locator("#product-confirmed-dataset-name").fill(name);
    await page.locator("#product-confirmed-dataset-version").fill("v1");
    await page.locator("#product-candidate-instruction").fill("/add 2.5 3.5");
    await page.locator("#product-candidate-input").fill("/add 2.5 3.5");
    await page.locator("#product-candidate-expected").fill("6");
    await page.locator("#product-candidate-reason").fill("人工核对算式 2.5 + 3.5 = 6");
    await page.getByRole("button", { name: "确认人工参考", exact: true }).click();
    await expect(page.getByRole("button", { name: "冻结连续评测版本", exact: true })).toBeVisible();
  });
  await step("冻结带人工确认记录的连续版本", async () => {
    await expect(page.locator("#product-confirmed-dataset-name")).toHaveValue(name);
    await expect(page.locator("#product-confirmed-dataset-version")).toHaveValue("v1");
    await page.getByRole("button", { name: "冻结连续评测版本", exact: true }).click();
    await expect(page.locator(".product-eval-list-row").filter({ hasText: name })).toBeVisible();
  });
  await step("清除浏览器缓存后从服务器恢复版本", async () => {
    await context.clearCookies();
    await page.evaluate(() => localStorage.clear());
    await page.reload();
    await page.getByRole("button", { name: "导航到评测", exact: true }).click();
    await openDisclosure(page, "新建评测");
    await page.locator("#product-dataset-choice").selectOption(`${name}/v1`);
    await expect(page.locator(".product-eval-selected-meta")).toContainText("已包含人工确认记录");
    await page.locator("#product-target").selectOption("krim-script-v2");
    await expect(page.getByRole("button", { name: "启动评测", exact: true })).toBeEnabled();
  });
  await step("运行连续评测并读取真实评分", async () => {
    taskId = (await startTask(page, info, "krim-script-v2")).id;
    await page.getByRole("button", { name: "查看报告", exact: true }).click();
    await expect(page.locator(".product-eval-report-row")).toHaveCount(1);
    await expect(page.locator(".product-eval-report-row.is-failed")).toHaveCount(0);
    await expect(page.locator(".product-eval-report-summary")).toContainText("100%");
    const report = await page.request.get(`${scope}/tasks/${taskId}/report`);
    expect(report.ok()).toBe(true);
    const body = await report.json();
    expect(body.rows[0].quality).toBe("passed");
    await attachJsonEvidence(info, "真实来源连续报告.json", body);
    await page.locator(".product-eval-report").scrollIntoViewIfNeeded();
  });
});
