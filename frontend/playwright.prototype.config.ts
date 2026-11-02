import { resolve } from "node:path";
import { defineConfig } from "@playwright/test";

const artifactRoot = resolve(import.meta.dirname, "..", ".cache", "ui-prototype");

export default defineConfig({
  testDir: "./e2e",
  testMatch: ["prototype.spec.ts"],
  fullyParallel: false,
  workers: 1,
  retries: 0,
  timeout: 20_000,
  expect: { timeout: 5_000 },
  outputDir: resolve(artifactRoot, "playwright-output"),
  reporter: [
    ["line"],
    ["json", { outputFile: resolve(artifactRoot, "test-results.json") }],
  ],
  use: {
    baseURL: "http://127.0.0.1:5182",
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
