import { randomUUID } from "node:crypto";
import { resolve } from "node:path";
import { defineConfig } from "@playwright/test";

const root = resolve(import.meta.dirname, "..");
const evidence = resolve(root, ".cache/agent-core/web-tools-browser", randomUUID());
process.env.KRIM_WEB_TOOLS_EVIDENCE = evidence;
export default defineConfig({
  testDir: "./e2e",
  testMatch: "web-tools.spec.ts",
  outputDir: resolve(evidence, "traces"),
  workers: 1,
  fullyParallel: false,
  timeout: 45_000,
  expect: { timeout: 12_000 },
  reporter: [["line"]],
  use: {
    baseURL: "http://localhost:8878",
    trace: "retain-on-failure",
    headless: true,
  },
  projects: [
    { name: "desktop", use: { viewport: { width: 1280, height: 900 } } },
    { name: "mobile", use: { viewport: { width: 390, height: 844 } } },
  ],
});
