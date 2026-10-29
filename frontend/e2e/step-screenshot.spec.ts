import { readFile, stat } from "node:fs/promises";
import { expect, test } from "@playwright/test";
import { attachJsonEvidence, screenshotStep } from "./step-screenshot.js";

test("API evidence is persisted independently of the reporter without overwriting prior results", async ({}, info) => {
  const value = { status: 409, body: { error: { code: "artifact_invalid" } } };
  const path = await attachJsonEvidence(info, "拒绝回执.json", value);
  expect(JSON.parse(await readFile(path, "utf8"))).toEqual(value);
  await expect(attachJsonEvidence(info, "拒绝回执.json", { status: 200 })).rejects.toMatchObject({ code: "EEXIST" });
  expect(JSON.parse(await readFile(path, "utf8"))).toEqual(value);
});

test("indexes ordered Chinese steps, duplicate titles, action failures and screenshot failures", async ({ page }, testInfo) => {
  await page.emulateMedia({ colorScheme: "dark" });
  const caseName = "截图-helper-示例";
  const step = (stepName: string, action: () => Promise<void>) =>
    screenshotStep(page, testInfo, { caseName, stepName, fullPage: true }, action);

  await step("准备中文页面", async () => {
    await page.setViewportSize({ width: 640, height: 480 });
    await page.setContent(`
      <main>
        <h1>Screenshot helper fixture</h1>
        <label>输入 <input id="entry"></label>
        <p data-sensitive>synthetic-private-value</p>
      </main>
    `);
    await expect(page.getByRole("heading", { name: "Screenshot helper fixture" })).toBeVisible();
  });
  await step("填写中文输入", async () => {
    await page.locator("#entry").fill("第一次中文输入");
  });
  await step("重复标题", async () => {
    await page.locator("#entry").fill("重复步骤一");
  });
  await step("重复标题", async () => {
    await page.locator("#entry").fill("重复步骤二");
  });

  await expect(step("动作失败现场", async () => {
    await page.locator("#entry").fill("保留失败现场");
    throw new Error("synthetic-action-failure");
  })).rejects.toThrow("synthetic-action-failure");

  await expect(step("截图失败现场", async () => {
    await page.close();
  })).rejects.toThrow();

  const indexPath = testInfo.outputPath("step-screenshots", "index.json");
  const records = JSON.parse(await readFile(indexPath, "utf8")) as Array<Record<string, unknown>>;
  expect(records.map(record => record.sequence)).toEqual([1, 2, 3, 4, 5, 6]);
  expect(records.map(record => record.step)).toEqual([
    "准备中文页面",
    "填写中文输入",
    "重复标题",
    "重复标题",
    "动作失败现场",
    "截图失败现场",
  ]);
  expect(records.map(record => record.status)).toEqual([
    "passed", "passed", "passed", "passed", "failed", "failed",
  ]);
  expect(records[0]).toMatchObject({
    case: caseName,
    viewport: { width: 640, height: 480 },
    theme: "dark",
    nodeVersion: process.version,
  });
  expect(records[0]!.runtimeRevision).not.toBe("git-unavailable");
  expect(["clean", "dirty", "unknown"]).toContain(records[0]!.workingTree);
  expect(records[0]!.screenshot).toContain("runtime-");
  expect(records[0]!.screenshot).toContain(String(records[0]!.workingTree));
  expect(records[0]!.maskSelectors).toContain("[data-sensitive]");

  const repeatedScreenshots = [records[2]!.screenshot, records[3]!.screenshot].map(String);
  expect(new Set(repeatedScreenshots).size).toBe(2);
  expect(repeatedScreenshots.every(name => name.includes("重复标题"))).toBe(true);
  for (const record of records.slice(0, 5)) {
    expect((await stat(testInfo.outputPath("step-screenshots", String(record.screenshot)))).size)
      .toBeGreaterThan(0);
  }
  await expect(stat(testInfo.outputPath("step-screenshots", String(records[5]!.screenshot))))
    .rejects.toThrow();
});
