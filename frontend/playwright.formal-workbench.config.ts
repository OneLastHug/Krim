import { resolve } from "node:path";
import { defineConfig } from "@playwright/test";

const artifactRoot = resolve(import.meta.dirname, "..", ".cache", "formal-workbench", "e2e");
const baseURL = process.env.KRIM_FORMAL_BASE_URL ?? "http://127.0.0.1:8879";

export default defineConfig({
  testDir: "./e2e",
  testMatch: ["formal-workbench.spec.ts", "formal-workbench-zoom.spec.ts", "model-budget-errors.spec.ts"],
  fullyParallel: false,
  workers: 1,
  retries: 0,
  timeout: 45_000,
  expect: { timeout: 5_000 },
  outputDir: resolve(artifactRoot, "playwright-output"),
  reporter: [
    ["line"],
    ["json", { outputFile: resolve(artifactRoot, "test-results.json") }],
  ],
  use: {
    baseURL,
    browserName: "chromium",
    channel: "msedge",
    headless: true,
    viewport: { width: 1440, height: 900 },
    screenshot: "off",
    video: "off",
    trace: "off",
    actionTimeout: 5_000,
  },
});
