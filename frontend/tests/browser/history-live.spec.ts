import { expect, test } from "@playwright/test";

test.skip(
  process.env.MAINTAI_M6_INTEGRATION !== "1",
  "Requires approved M6 DB/Kafka/MinIO fixture",
);
const issuer = "http://127.0.0.1:18033";
test("PKCE history reads original terminal outcomes and downloads snapshot CSV", async ({
  page,
  request,
}) => {
  await page.route("**/runtime-config.json", (route) =>
    route.fulfill({
      json: {
        mode: "live",
        apiBaseUrl: "/api/v1",
        freshMs: 2000,
        staleMs: 5000,
        auth: {
          authority: issuer,
          clientId: "business-ui-fixture",
          scope: "openid profile",
          audience: "business-test",
        },
      },
    }),
  );
  await page.goto("/machines");
  await page.getByRole("button", { name: "Đăng nhập", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Tổng quan thiết bị" }),
  ).toBeVisible();
  const completed = await request.post(issuer + "/fixture/publish", {
    data: { kind: "completed" },
  });
  expect(completed.ok()).toBe(true);
  const source = await completed.json();
  const invalid = await request.post(issuer + "/fixture/publish", {
    data: { kind: "invalid" },
  });
  expect(invalid.ok()).toBe(true);
  const invalidSource = await invalid.json();
  const unavailable = await request.post(issuer + "/fixture/publish", {
    data: { kind: "unavailable" },
  });
  expect(unavailable.ok()).toBe(true);
  const unavailableSource = await unavailable.json();
  await page.getByLabel("ID máy").fill("M-001");
  await page.getByRole("button", { name: "Mở thiết bị" }).click();
  await page.getByRole("button", { name: "Lịch sử", exact: true }).click();
  const history = page.locator(".history-panel");
  await expect(
    history.getByText(source.event_id, { exact: true }),
  ).toBeVisible();
  await expect(
    history
      .locator("tr")
      .filter({ hasText: source.event_id })
      .getByText("PREDICTION_COMPLETED", { exact: true }),
  ).toBeVisible();
  await expect(
    history
      .locator("tr")
      .filter({ hasText: invalidSource.event_id })
      .getByText("DATA_INVALID", { exact: true }),
  ).toBeVisible();
  await expect(
    history
      .locator("tr")
      .filter({ hasText: unavailableSource.event_id })
      .getByText("INFERENCE_UNAVAILABLE", { exact: true }),
  ).toBeVisible();
  const download = page.waitForEvent("download");
  await history.getByRole("button", { name: "Xuất CSV", exact: true }).click();
  expect((await download).suggestedFilename()).toBe("maintai-history.csv");
  await history
    .getByRole("button", { name: "Xem biểu đồ", exact: true })
    .click();
  await expect(
    history.getByRole("img", { name: "Biểu đồ risk_score" }),
  ).toBeVisible();
  await history.getByLabel("Nguồn lịch sử").selectOption("RAW");
  await history.getByRole("button", { name: "Áp dụng", exact: true }).click();
  await expect(
    history.getByText(source.event_id, { exact: true }),
  ).toBeVisible();
  await expect(history).toContainText(source.sequence_number);
  await history.getByLabel("Chỉ số").selectOption("rotational_speed_rpm");
  await history
    .getByRole("button", { name: "Xem biểu đồ", exact: true })
    .click();
  await expect(
    history.getByRole("img", { name: "Biểu đồ rotational_speed_rpm" }),
  ).toBeVisible();
  await history
    .getByRole("button", { name: "Tạo Parquet job", exact: true })
    .click();
  await expect(history.getByText("SUCCEEDED", { exact: true })).toBeVisible({
    timeout: 15000,
  });
  const artifact = page.waitForEvent("download");
  await history
    .getByRole("button", { name: "Tải tệp export", exact: true })
    .click();
  const parquet = await artifact;
  expect(parquet.suggestedFilename()).toBe("maintai-history.parquet");
  const { readFileSync } = await import("node:fs");
  const bytes = readFileSync((await parquet.path())!);
  expect(bytes.subarray(0, 4).toString()).toBe("PAR1");
});
