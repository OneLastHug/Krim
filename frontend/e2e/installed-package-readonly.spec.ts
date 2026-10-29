import { expect, test } from "@playwright/test";
import { attachJsonEvidence, screenshotStep } from "./step-screenshot.js";

test.skip(process.env.KRIM_INSTALLED_PACKAGE !== "1", "需要项目外已安装的候选包服务");
test("P6：安装包冷重启后只读恢复任务与评测", async ({ page }, info) => {
  const methods: string[] = [];
  page.on("request", request => { if (/\/v[1-4]\//.test(request.url())) methods.push(request.method()); });
  const step = (name: string, action: () => Promise<void>) => screenshotStep(page, info,
    { caseName: "P6-安装包冷恢复", stepName: name, theme: "light" }, action);
  const response = await page.request.get("/v4/workspaces/local/eval/tasks?offset=0&limit=20");
  expect(response.ok()).toBe(true);
  const tasks = await response.json();
  expect(tasks.length).toBeGreaterThan(0);
  const task = tasks.find((row: { status: string; report_hash: string | null }) => row.status === "completed" && row.report_hash);
  expect(task).toBeTruthy();
  await attachJsonEvidence(info, "冷重启-持久评测目录.json", tasks);
  await step("打开原持久任务而不新建", async () => {
    await page.goto(`/ui/?view=eval&workspace=local&product_eval_id=${encodeURIComponent(task.id)}`);
    await expect(page.locator(".product-eval-task-facts")).toContainText(task.id);
  });
  await step("读取原报告和完整进度", async () => {
    await page.getByRole("button", { name: "查看报告", exact: true }).click();
    await expect(page.locator(".product-eval-report-row").first()).toBeVisible();
    await page.locator(".product-eval-report").scrollIntoViewIfNeeded();
    expect(methods.every(method => method === "GET")).toBe(true);
    const report = await page.request.get(`/v4/workspaces/local/eval/tasks/${encodeURIComponent(task.id)}/report`);
    expect(report.ok()).toBe(true);
    const body = await report.json();
    expect(body.report_hash).toBe(task.report_hash);
    await attachJsonEvidence(info, "冷重启-同一原报告.json", body);
  });
});
