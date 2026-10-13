import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  testIgnore: "generated-root-recovery.spec.ts",
  timeout: 30_000,
  expect: { timeout: 10_000 },
  fullyParallel: false,
  workers: 1,
  reporter: [["line"]],
  use: {
    baseURL: "http://127.0.0.1:8876",
    trace: "retain-on-failure",
    ...devices["Desktop Chrome"],
  },
  webServer: {
    command: "../.venv/bin/python ../tests/fixtures/browser_ui_server.py",
    url: "http://127.0.0.1:8876/healthz",
    reuseExistingServer: false,
    timeout: 30_000,
  },
});
