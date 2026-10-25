import { expect, test } from "@playwright/test";

test("live UI controls the real Sensor provider and survives browser reload", async ({
  page,
  request,
}) => {
  // Only public UI config is intercepted; all Control requests reach Python over HTTP.
  await page.route("**/runtime-config.json", (route) =>
    route.fulfill({
      json: {
        mode: "live",
        apiBaseUrl: "http://127.0.0.1:8006",
        pollIntervalMs: 5000,
        requestTimeoutMs: 2000,
        staleAfterMs: 20000,
        auth: { mode: "none" },
        links: {},
      },
    }),
  );
  await page.goto("/");
  await expect(page.getByText("LIVE API", { exact: true })).toBeVisible();
  await expect(page.getByText(/Bởi local-operator|Bởi system/)).toBeVisible();
  await page.getByRole("button", { name: "Resume", exact: true }).click();
  await expect
    .poll(async () =>
      (
        await request.get(
          "http://127.0.0.1:8006/api/v1/simulator/machines/M-001/control",
        )
      )
        .json()
        .then((s) => s.stream_state),
    )
    .toBe("RUNNING");
  await page.getByRole("button", { name: "Pause", exact: true }).click();
  await expect
    .poll(async () =>
      (
        await request.get(
          "http://127.0.0.1:8006/api/v1/simulator/machines/M-001/control",
        )
      )
        .json()
        .then((s) => s.stream_state),
    )
    .toBe("PAUSED");
  await page
    .getByRole("button", { name: "Kịch bản Hồi phục", exact: true })
    .click();
  await expect
    .poll(async () =>
      (
        await request.get(
          "http://127.0.0.1:8006/api/v1/simulator/machines/M-001/control",
        )
      )
        .json()
        .then((s) => s.scenario),
    )
    .toBe("recovery");
  await page.reload();
  await expect(page.getByText(/Bởi local-operator/)).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Pause", exact: true }),
  ).toBeDisabled();
  await expect(
    page.getByRole("button", { name: "Kịch bản Hồi phục", exact: true }),
  ).toHaveClass(/selected/);
  const peer = await request.get(
    "http://127.0.0.1:8006/api/v1/simulator/machines/M-002/control",
  );
  expect((await peer.json()).control_version).toBe(0);
});
