import { defineConfig, devices } from "@playwright/test";
import { pythonExecutable, pythonFixtureArgs } from "./e2e/python-client.js";
const quote = (value: string) => "'" + value.replaceAll("'", "'\\''") + "'";
export default defineConfig({
  testDir: "./e2e", testMatch: "conversation-recovery-process.spec.ts", timeout: 45_000,
  expect: { timeout: 10_000 }, workers: 1, reporter: [["line"]],
  use: { baseURL: "http://127.0.0.1:8883", trace: "retain-on-failure", ...devices["Desktop Chrome"] },
  webServer: { command: [pythonExecutable, ...pythonFixtureArgs("conversation_recovery_browser_process_server.py")].map(quote).join(" "),
    env: { KRIM_AGENT_API_KEY: "browser-test-key" }, url: "http://127.0.0.1:8883/healthz", reuseExistingServer: false, timeout: 60_000 },
});
