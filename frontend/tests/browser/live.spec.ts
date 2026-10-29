import { expect, test } from "@playwright/test";

test.skip(
  process.env.MAINTAI_M4_INTEGRATION !== "1",
  "Requires approved local PostgreSQL/Kafka M4 fixture",
);
const issuer = "http://127.0.0.1:18033";
test.beforeEach(async ({ page }) => {
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
});

test("PKCE login, real Kafka projections and scoped SSE render in the browser before M5", async ({
  page,
  request,
}) => {
  let bearer = "";
  page.on("request", (request) => {
    if (request.url().includes("/api/v1/"))
      bearer = request.headers().authorization ?? bearer;
  });
  await page.goto("/machines");
  await page.getByRole("button", { name: "Đăng nhập", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Tổng quan thiết bị" }),
  ).toBeVisible();
  if (process.env.MAINTAI_M5_INTEGRATION !== "1")
    await expect(page.getByRole("alert")).toContainText(
      "Không thể tải dữ liệu",
    );
  await page.getByLabel("ID máy").fill("M-001");
  await page.getByRole("button", { name: "Mở thiết bị" }).click();
  await expect(page.getByText("CONNECTED", { exact: true })).toBeVisible();
  const response = await request.post(issuer + "/fixture/publish", {
    data: { kind: "completed" },
  });
  expect(response.ok()).toBe(true);
  const result = await response.json();
  await expect(
    page.getByText(result.event_id, { exact: true }).first(),
  ).toBeVisible();
  await expect(page.getByText("fixture", { exact: true })).toBeVisible();
  await expect(
    page.getByText(result.sequence_number, { exact: true }),
  ).toBeVisible();
  if (process.env.MAINTAI_M5_INTEGRATION !== "1")
    await expect(
      page.getByText("Thông tin bổ sung chưa khả dụng", { exact: true }),
    ).toBeVisible();
  expect(bearer.startsWith("Bearer ")).toBe(true);
  expect(
    await page.evaluate(() =>
      JSON.stringify({ ...localStorage, ...sessionStorage }),
    ),
  ).not.toContain(bearer.slice(7));
  const alertsBefore =
    process.env.MAINTAI_M5_INTEGRATION === "1"
      ? await request
          .get("http://127.0.0.1:18031/api/v1/alerts", {
            headers: { Authorization: bearer },
          })
          .then((r) => r.json())
      : null;
  const historical = await page.locator(".prediction-result").innerText();
  await request.post(issuer + "/fixture/publish", {
    data: { kind: "invalid" },
  });
  await expect(
    page.getByText("INVALID", { exact: true }).first(),
  ).toBeVisible();
  await expect(page.getByText(/Based on older data/)).toBeVisible();
  expect(await page.locator(".prediction-result").innerText()).toBe(historical);
  await request.post(issuer + "/fixture/publish", {
    data: { kind: "unavailable" },
  });
  await expect(page.getByText(/Inference unavailable/)).toBeVisible();
  if (alertsBefore) {
    const after = await request
      .get("http://127.0.0.1:18031/api/v1/alerts", {
        headers: { Authorization: bearer },
      })
      .then((r) => r.json());
    expect(after.total).toBe(alertsBefore.total);
    expect(after.items.map((v: { status: string }) => v.status)).toEqual(
      alertsBefore.items.map((v: { status: string }) => v.status),
    );
  }
  await request.post(issuer + "/fixture/restart-listeners");
  await expect(page.getByText("CONNECTED", { exact: true })).toBeVisible({
    timeout: 15000,
  });
  await expect(page.getByText(/Inference unavailable/)).toBeVisible();
  await page.getByRole("button", { name: "Đăng xuất" }).click();
  await expect(
    page.getByRole("button", { name: "Đăng nhập", exact: true }),
  ).toBeVisible();
  await expect(page.getByText("fixture", { exact: true })).toHaveCount(0);
});
