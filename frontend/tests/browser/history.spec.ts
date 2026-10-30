import { expect, test } from "@playwright/test";

test("demo history keeps raw receipts, UTC source labels and CSV download on mobile", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/machines/M-001");
  await page.getByRole("button", { name: "Lịch sử", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Lịch sử xử lý", exact: true }),
  ).toBeVisible();
  await expect(page.getByText("res_fixture_1", { exact: true })).toBeVisible();
  await page.getByLabel("Nguồn lịch sử").selectOption("RAW");
  await page.getByRole("button", { name: "Áp dụng", exact: true }).click();
  await expect(
    page.getByText("sensor.readings:0:1", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("sensor.readings:0:0", { exact: true }),
  ).toBeVisible();
  const download = page.waitForEvent("download");
  await page.getByRole("button", { name: "Xuất CSV", exact: true }).click();
  expect((await download).suggestedFilename()).toBe("maintai-history.csv");
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});
