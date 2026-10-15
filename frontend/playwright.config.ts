import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  testIgnore: ["generated-root-recovery.spec.ts", "directory-checkpoint-recovery.spec.ts"],
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
    env: {
      KRIM_TEST_EXECUTION_TREE: "1",
      KRIM_TEST_MODEL_CHECKPOINTS: "1",
      KRIM_TEST_MODEL_RESTORES: "1",
      KRIM_TEST_DIRECTORY_CHECKPOINTS: "1",
    },
    command: "../.venv/bin/python ../tests/fixtures/browser_ui_server.py",
    url: "http://127.0.0.1:8876/healthz",
    reuseExistingServer: false,
    timeout: 30_000,
  },
});
