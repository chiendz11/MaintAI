import { defineConfig, devices } from "@playwright/test";
import { existsSync } from "node:fs";
const sensorPython =
  process.env.MAINTAI_SENSOR_PYTHON ??
  (existsSync("../.venv/bin/python") ? "../.venv/bin/python" : "python3");

export default defineConfig({
  testDir: "./tests/browser",
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  workers: process.env.CI ? 2 : undefined,
  reporter: "list",
  use: { baseURL: "http://127.0.0.1:4174", trace: "retain-on-failure" },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: [
    {
      command: "npm run preview",
      url: "http://127.0.0.1:4174",
      reuseExistingServer: false,
    },
    {
      command: `${sensorPython} tests/provider/server.py`,
      url: "http://127.0.0.1:8006/health/ready",
      reuseExistingServer: false,
    },
  ],
});
