import { resolve } from "node:path";
import { defineConfig } from "@playwright/test";

const root = resolve(import.meta.dirname, "..", ".cache", "approval-window", "browser");
export default defineConfig({
  testDir: "./e2e", testMatch: "approval-window.spec.ts", fullyParallel: false,
  workers: 1, retries: 0, timeout: 30_000, expect: { timeout: 5_000 },
  outputDir: resolve(root, "output"),
  reporter: [["line"], ["json", { outputFile: resolve(root, "results.json") }]],
  use: { baseURL: process.env.KRIM_APPROVAL_BASE_URL ?? "http://127.0.0.1:8884", browserName: "chromium", channel: "msedge", headless: true, viewport: { width: 1280, height: 900 }, trace: "off", screenshot: "off" },
});
