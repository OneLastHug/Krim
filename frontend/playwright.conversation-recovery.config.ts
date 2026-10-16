import { defineConfig, devices } from "@playwright/test";
import { pythonExecutable, pythonFixtureArgs } from "./e2e/python-client.js";
const quote = (value: string) => "'" + value.replaceAll("'", "'\\''") + "'";
export default defineConfig({
  testDir: "./e2e", testMatch: ["conversation-recovery.spec.ts", "conversation-recovery-boundaries.spec.ts"],
  timeout: 30_000, expect: { timeout: 10_000 }, fullyParallel: false, workers: 1, reporter: [["line"]],
  use: { baseURL: "http://127.0.0.1:8881", trace: "retain-on-failure", ...devices["Desktop Chrome"] },
  webServer: { command: [pythonExecutable, ...pythonFixtureArgs("conversation_recovery_browser_server.py")].map(quote).join(" "),
    url: "http://127.0.0.1:8881/healthz", reuseExistingServer: false, timeout: 30_000 },
});
