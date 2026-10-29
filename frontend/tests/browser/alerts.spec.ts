import { expect, test } from "@playwright/test";

const issuer = "http://127.0.0.1:18033";
test.skip(
  process.env.MAINTAI_M5_INTEGRATION !== "1",
  "Requires isolated full M5 pipeline",
);
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
test("real HTTP Ingestion creates Alert, technician resolves it and records feedback", async ({
  page,
  request,
}) => {
  await page.goto("/machines");
  await page.getByRole("button", { name: "Đăng nhập", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Tổng quan thiết bị" }),
  ).toBeVisible();
  const result = await request.post(issuer + "/fixture/publish", {
    data: { kind: "alert" },
  });
  expect(result.ok()).toBe(true);
  await page.getByRole("button", { name: "Cảnh báo", exact: true }).click();
  await expect(
    page.getByRole("cell", { name: "OPEN", exact: true }).first(),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Chi tiết", exact: true })
    .first()
    .click();
  await expect(
    page.getByText((await result.json()).event_id, { exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Tiếp nhận", exact: true }).click();
  await expect(
    page.getByRole("option", { name: "Bắt đầu xử lý", exact: true }),
  ).toBeAttached();
  await page
    .getByRole("button", { name: "Bắt đầu xử lý", exact: true })
    .click();
  await page.getByLabel("Thao tác cảnh báo").selectOption("resolve");
  await page
    .getByLabel("Kết quả xử lý")
    .fill("Đã kiểm tra và xử lý trong demo");
  await page
    .getByRole("button", { name: "Hoàn tất xử lý", exact: true })
    .click();
  await expect(
    page.getByText("Cảnh báo đã đóng. Không thể chuyển trạng thái thêm."),
  ).toBeVisible();
  await page.getByRole("button", { name: "Gửi feedback", exact: true }).click();
  await expect(page.getByRole("status")).toContainText("Đã ghi nhận feedback");
  await expect(
    page.getByText("RESOLVED", { exact: true }).first(),
  ).toBeVisible();
});
test("viewer can read evidence but cannot mutate alerts", async ({ page }) => {
  await page.route("**/runtime-config.json", (route) =>
    route.fulfill({
      json: {
        mode: "live",
        apiBaseUrl: "/api/v1",
        freshMs: 2000,
        staleMs: 5000,
        auth: {
          authority: issuer,
          clientId: "business-ui-fixture-viewer",
          scope: "openid profile",
          audience: "business-test",
        },
      },
    }),
  );
  await page.goto("/alerts");
  await page.getByRole("button", { name: "Đăng nhập", exact: true }).click();
  await page.getByRole("button", { name: "Cảnh báo", exact: true }).click();
  await page
    .getByRole("button", { name: "Chi tiết", exact: true })
    .first()
    .click();
  await expect(
    page.getByText("Bạn có quyền xem cảnh báo.", { exact: false }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Gửi feedback", exact: true }),
  ).toHaveCount(0);
});

test("lost mutation responses are reconciled without replaying POST", async ({
  page,
  request,
}) => {
  await page.goto("/machines");
  await page.getByRole("button", { name: "Đăng nhập", exact: true }).click();
  expect(
    (
      await request.post(issuer + "/fixture/publish", {
        data: { kind: "alert" },
      })
    ).ok(),
  ).toBe(true);
  await page.getByRole("button", { name: "Cảnh báo", exact: true }).click();
  await page.getByLabel("Trạng thái cảnh báo").selectOption("OPEN");
  await page
    .getByRole("button", { name: "Chi tiết", exact: true })
    .first()
    .click();
  let actions = 0,
    feedback = 0;
  await page.route("**/api/v1/alerts/*/ack", async (route) => {
    actions++;
    await route.fetch();
    await route.abort("failed");
  });
  await page.getByRole("button", { name: "Tiếp nhận", exact: true }).click();
  await expect(page.getByRole("status")).toContainText(
    "Kết quả thao tác chưa xác định",
  );
  await expect(
    page.getByRole("option", { name: "Bắt đầu xử lý", exact: true }),
  ).toBeAttached();
  expect(actions).toBe(1);
  await page.route("**/api/v1/alerts/*/feedback", async (route) => {
    feedback++;
    await route.fetch();
    await route.abort("failed");
  });
  await page.getByRole("button", { name: "Gửi feedback", exact: true }).click();
  await expect(page.getByRole("status")).toContainText(
    "Kết quả gửi feedback chưa xác định",
  );
  expect(feedback).toBe(1);
});
