import { defineConfig, devices } from "@playwright/test";
import { existsSync } from "node:fs";

export default defineConfig({
  testDir: "./tests/browser",
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  workers: 1,
  reporter: "list",
  use: { baseURL: "http://127.0.0.1:4173", trace: "retain-on-failure" },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: [
    {
      command: "npm run preview",
      url: "http://127.0.0.1:4173",
      reuseExistingServer: false,
      gracefulShutdown: { signal: "SIGTERM" as const, timeout: 30000 },
      env:
        process.env.MAINTAI_M4_INTEGRATION === "1"
          ? { MAINTAI_BUSINESS_PROXY_ORIGIN: "http://127.0.0.1:18031" }
          : {},
    },
    ...(process.env.MAINTAI_M4_INTEGRATION === "1"
      ? [
          {
            command: `${process.env.MAINTAI_TEST_PYTHON ?? (existsSync("../.venv/bin/python") ? "../.venv/bin/python" : "python3")} -B tests/provider/server.py`,
            url: "http://127.0.0.1:18033/health/ready",
            reuseExistingServer: false,
            timeout: 90000,
            gracefulShutdown: { signal: "SIGTERM" as const, timeout: 30000 },
          },
        ]
      : []),
  ],
});
