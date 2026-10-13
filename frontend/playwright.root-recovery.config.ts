import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e", testMatch: "generated-root-recovery.spec.ts", timeout: 30_000,
  expect: { timeout: 10_000 }, workers: 1, reporter: [["line"]],
  use: { baseURL: "http://127.0.0.1:8877", trace: "retain-on-failure", ...devices["Desktop Chrome"] },
  webServer: {
    command: "../.venv/bin/python ../tests/fixtures/generated_root_browser_recovery_server.py",
    url: "http://127.0.0.1:8877/healthz", reuseExistingServer: false, timeout: 60_000,
  },
});
