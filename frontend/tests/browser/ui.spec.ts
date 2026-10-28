import { expect, test } from "@playwright/test";
import { readFileSync } from "node:fs";

test("built UI works under the production Nginx content security policy", async ({
  page,
}) => {
  const config = readFileSync("nginx.conf", "utf8");
  const policy = /add_header Content-Security-Policy "([^"]+)"/.exec(
    config,
  )![1];
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.route("**/*", async (route) => {
    const response = await route.fetch();
    await route.fulfill({
      response,
      headers: { ...response.headers(), "content-security-policy": policy },
    });
  });
  await page.goto("/machines/M-001");
  await expect(
    page.getByRole("heading", { name: "Latest Processing" }),
  ).toBeVisible();
  expect(errors).toEqual([]);
});

test("demo Fleet opens independent telemetry prediction and processing panels", async ({
  page,
}, testInfo) => {
  await page.goto("/machines");
  await expect(
    page.getByText("Dữ liệu mô phỏng", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "Tổng quan thiết bị" }),
  ).toBeVisible();
  await page.getByRole("link", { name: "Xem M-001" }).click();
  await expect(
    page.getByRole("heading", { name: "Live Telemetry" }),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "Latest Valid Prediction" }),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "Latest Processing" }),
  ).toBeVisible();
  await expect(page.getByText("demo-model", { exact: true })).toBeVisible();
  await expect(page.getByText("CONNECTED", { exact: true })).toBeVisible();
  await page.screenshot({
    path: testInfo.outputPath("machine-desktop.png"),
    fullPage: true,
  });
});

test("direct machine input and encoded deep links work without a Fleet selection", async ({
  page,
}) => {
  await page.goto("/machines");
  await page.getByLabel("ID máy").fill("M-002");
  await page.getByRole("button", { name: "Mở thiết bị" }).click();
  await expect(page).toHaveURL(/\/machines\/M-002$/);
  await page.reload();
  await expect(
    page.getByRole("heading", { name: "Live Telemetry" }),
  ).toBeVisible();
  await expect(page.getByText("demo-model", { exact: true })).toBeVisible();
});

test("missing machine is an error, never a fake normal snapshot", async ({
  page,
}) => {
  await page.goto("/machines/missing");
  await expect(page.getByRole("alert")).toContainText("Không tìm thấy máy");
  await expect(page.getByText("demo-model", { exact: true })).toHaveCount(0);
});

test("mobile keeps source labels readable without horizontal viewport overflow", async ({
  page,
}, testInfo) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/machines/M-001");
  await expect(
    page.getByRole("heading", { name: "Latest Processing" }),
  ).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  await page.screenshot({
    path: testInfo.outputPath("machine-mobile.png"),
    fullPage: true,
  });
});
