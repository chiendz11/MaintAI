import { expect, test } from "@playwright/test";

test.skip(
  process.env.MAINTAI_M8_INTEGRATION !== "1",
  "Requires approved M8 local services",
);
test("original Storage/Processing rebuild resyncs both API replicas without replaying Alerts", async ({
  browser,
  request,
}) => {
  const issuer = "http://127.0.0.1:18033";
  const contexts = await Promise.all([
    browser.newContext(),
    browser.newContext(),
  ]);
  try {
    const pages = await Promise.all(
      contexts.map((context) => context.newPage()),
    );
    const reads = [0, 0];
    for (const [index, page] of pages.entries()) {
      await page.route("**/runtime-config.json", (route) =>
        route.fulfill({
          json: {
            mode: "live",
            apiBaseUrl: index === 0 ? "/api/v1" : "/replica-2/api/v1",
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
      page.on("response", (response) => {
        if (response.url().endsWith("/M-001/live")) reads[index]++;
      });
      await page.goto("/machines/M-001");
      await page
        .getByRole("button", { name: "Đăng nhập", exact: true })
        .click();
      await page.getByLabel("ID máy").fill("M-001");
      await page.getByRole("button", { name: "Mở thiết bị" }).click();
      await expect(page.getByText("CONNECTED", { exact: true })).toBeVisible();
    }
    const published = await request.post(issuer + "/fixture/publish", {
      data: { kind: "completed" },
    });
    expect(published.ok()).toBe(true);
    const source = await published.json();
    for (const page of pages)
      await expect(
        page.getByText(source.event_id, { exact: true }).first(),
      ).toBeVisible();
    const before = [...reads];
    const rebuilt = await request.post(issuer + "/fixture/rebuild", {
      timeout: 45000,
    });
    expect(rebuilt.ok(), await rebuilt.text()).toBe(true);
    expect((await rebuilt.json()).business_effects_unchanged).toBe(true);
    for (const [index, page] of pages.entries()) {
      await expect.poll(() => reads[index]).toBeGreaterThan(before[index]);
      await expect(
        page.getByText(source.event_id, { exact: true }).first(),
      ).toBeVisible();
    }
  } finally {
    await Promise.all(contexts.map((context) => context.close()));
  }
});
