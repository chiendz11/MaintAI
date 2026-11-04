import { expect, test } from "@playwright/test";
const issuer = "http://127.0.0.1:18033";
test.skip(
  process.env.MAINTAI_M7_INTEGRATION !== "1",
  "Requires M7 real Business/DB/S3 and fixture RAG",
);
test.beforeEach(async ({ page, request }) => {
  await request.post(issuer + "/fixture/rag-mode", {
    data: { value: "GROUNDED" },
  });
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
});
test("technician requests a durable brief and sees citations from fixture RAG", async ({
  page,
  request,
}) => {
  await request.post(issuer + "/fixture/publish", { data: { kind: "alert" } });
  await page.getByRole("button", { name: "Cảnh báo", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "Chi tiết", exact: true }).first(),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Chi tiết", exact: true })
    .first()
    .click();
  await page
    .getByRole("button", { name: "Tạo khuyến nghị", exact: true })
    .click();
  await expect(
    page.getByRole("heading", { name: "Nguồn trích dẫn" }),
  ).toBeVisible({ timeout: 20000 });
  await expect(page.getByText("GROUNDED", { exact: true })).toBeVisible();
});
test("Monitoring reads a verified object report without fabricating heartbeat", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole("button", { name: "Monitoring", exact: true }).click();
  await expect(
    page.getByText("Checkpoint thành công: Chưa có nguồn heartbeat", {
      exact: true,
    }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Xem report", exact: true }).first(),
  ).toBeVisible({ timeout: 15000 });
  await page
    .getByRole("button", { name: "Xem report", exact: true })
    .first()
    .click();
  await expect(page.locator(".table-scroll")).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  await expect(page.locator("pre.evidence-json")).toContainText(
    '"schema_version": "1.0.0"',
  );
});

test("insufficient evidence completes without a fabricated checklist", async ({
  page,
  request,
}) => {
  await request.post(issuer + "/fixture/rag-mode", {
    data: { value: "INSUFFICIENT_EVIDENCE" },
  });
  await request.post(issuer + "/fixture/publish", { data: { kind: "alert" } });
  await page.getByRole("button", { name: "Cảnh báo", exact: true }).click();
  await page
    .getByRole("button", { name: "Chi tiết", exact: true })
    .first()
    .click();
  await page
    .getByLabel("Loại yêu cầu Copilot")
    .selectOption("TECHNICIAN_CHECKLIST");
  await page
    .getByRole("button", { name: "Tạo khuyến nghị", exact: true })
    .click();
  await expect(
    page.getByText("Chưa đủ bằng chứng để đưa ra checklist.", { exact: true }),
  ).toBeVisible({ timeout: 20000 });
  await expect(
    page.getByRole("heading", { name: "Nguồn trích dẫn" }),
  ).toHaveCount(0);
});

test("an unknown POST is reconciled and retried with the same key", async ({
  page,
  request,
}) => {
  await request.post(issuer + "/fixture/publish", { data: { kind: "alert" } });
  await page.getByRole("button", { name: "Cảnh báo", exact: true }).click();
  await page
    .getByRole("button", { name: "Chi tiết", exact: true })
    .first()
    .click();
  await page
    .getByLabel("Loại yêu cầu Copilot")
    .selectOption("ALERT_EXPLANATION");
  const keys: string[] = [];
  await page.route("**/api/v1/alerts/*/brief", async (route) => {
    if (route.request().method() !== "POST") return route.continue();
    keys.push(route.request().headers()["idempotency-key"]);
    if (keys.length === 1) {
      await route.fetch();
      await route.abort("failed");
    } else await route.continue();
  });
  await page
    .getByRole("button", { name: "Tạo khuyến nghị", exact: true })
    .click();
  await expect(
    page.getByRole("button", { name: "Thử lại cùng yêu cầu", exact: true }),
  ).toBeVisible();
  expect(keys).toHaveLength(1);
  await page
    .getByRole("button", { name: "Thử lại cùng yêu cầu", exact: true })
    .click();
  await expect(
    page.getByRole("heading", { name: "Nguồn trích dẫn" }),
  ).toBeVisible({ timeout: 20000 });
  expect(keys).toHaveLength(2);
  expect(keys[0]).toBe(keys[1]);
});

test("RAG unavailable leaves Alert actions available", async ({
  page,
  request,
}) => {
  await request.post(issuer + "/fixture/rag-mode", {
    data: { value: "UNAVAILABLE" },
  });
  await request.post(issuer + "/fixture/publish", { data: { kind: "alert" } });
  await page.getByRole("button", { name: "Cảnh báo", exact: true }).click();
  await page
    .getByRole("button", { name: "Chi tiết", exact: true })
    .first()
    .click();
  await page
    .getByLabel("Loại yêu cầu Copilot")
    .selectOption("MAINTENANCE_BRIEF");
  await page
    .getByRole("button", { name: "Tạo khuyến nghị", exact: true })
    .click();
  await page.getByLabel("Thao tác cảnh báo").selectOption("escalate");
  await page.getByRole("button", { name: "Chuyển cấp", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "Bắt đầu xử lý", exact: true }),
  ).toBeVisible();
  await expect(page.getByText("UNAVAILABLE", { exact: true })).toBeVisible({
    timeout: 20000,
  });
});
