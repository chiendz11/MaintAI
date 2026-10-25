import { expect, test, type Page } from "@playwright/test";
import type { ControlState } from "../../src/types";

async function live(page: Page) {
  const machines: ControlState[] = [1, 2].map((i) => ({
    machine_id: `M-00${i}`,
    stream_state: "RUNNING",
    scenario: "normal",
    control_version: 1,
    updated_at: new Date().toISOString(),
    last_controlled_by: "fixture-operator",
    source: "SIMULATOR_CONTROL",
  }));
  const mutations: {
    key: string;
    body: { expected_control_version: number; scenario?: string };
    path: string;
  }[] = [];
  await page.route("**/runtime-config.json", (route) =>
    route.fulfill({
      json: {
        mode: "live",
        apiBaseUrl: "",
        pollIntervalMs: 5000,
        requestTimeoutMs: 1000,
        staleAfterMs: 20000,
        auth: { mode: "none" },
        links: {},
      },
    }),
  );
  await page.route("**/api/v1/simulator/machines**", async (route) => {
    const request = route.request();
    const parts = new URL(request.url()).pathname.split("/");
    if (parts.length === 5)
      return route.fulfill({
        json: { items: machines, as_of: new Date().toISOString() },
      });
    const index = machines.findIndex(
      (m) => m.machine_id === decodeURIComponent(parts[5]),
    );
    if (request.method() === "GET")
      return route.fulfill({ json: machines[index] });
    const body = request.postDataJSON();
    mutations.push({
      key: request.headers()["idempotency-key"],
      body,
      path: parts[6],
    });
    machines[index] = {
      ...machines[index],
      control_version: machines[index].control_version + 1,
      stream_state:
        parts[6] === "pause"
          ? "PAUSED"
          : parts[6] === "resume"
            ? "RUNNING"
            : machines[index].stream_state,
      scenario: body.scenario ?? machines[index].scenario,
      updated_at: new Date().toISOString(),
    };
    return route.fulfill({ json: machines[index] });
  });
  return { machines, mutations };
}

test("demo operates independently and never calls the live control/data plane", async ({
  page,
}) => {
  const apiCalls: string[] = [];
  page.on("request", (request) => {
    if (request.url().includes("/api/")) apiCalls.push(request.url());
  });
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "Sensor Simulator." }),
  ).toBeVisible();
  await expect(page.getByText("Bạn đang ở chế độ demo")).toBeVisible();
  await expect(page.locator(".machine-card")).toHaveCount(5);
  await page.getByRole("button", { name: "Pause", exact: true }).click();
  await expect(page.locator(".control-state-row")).toContainText("PAUSED");
  await expect(
    page.locator(".machine-card").filter({ hasText: "M-002" }),
  ).toContainText("RUNNING");
  await page.getByRole("button", { name: /Kịch bản Hồi phục/ }).click();
  await expect(
    page.getByRole("button", { name: /Kịch bản Hồi phục/ }),
  ).toHaveAttribute("aria-pressed", "true");
  await expect(page.locator(".control-state-row")).toContainText("PAUSED");
  await page.reload();
  await expect(page.locator(".control-state-row")).toContainText("PAUSED");
  await expect(
    page.getByRole("button", { name: /Kịch bản Hồi phục/ }),
  ).toHaveAttribute("aria-pressed", "true");
  expect(apiCalls).toEqual([]);
});

test("live sends contract commands and shows unavailable telemetry instead of mock data", async ({
  page,
}) => {
  const { mutations } = await live(page);
  await page.goto("/");
  await expect(page.getByText("Chưa có sensor preview từ API")).toBeVisible();
  await expect(page.locator(".telemetry-cell strong").first()).toHaveText("—");
  await expect(page.getByRole("slider")).toHaveCount(0);
  await page.getByRole("button", { name: "Pause", exact: true }).click();
  await expect(page.locator(".control-state-row")).toContainText("PAUSED");
  expect(mutations[0].body).toEqual({ expected_control_version: 1 });
  expect(mutations[0].key).toBeTruthy();
  await page.getByRole("button", { name: /Kịch bản Suy giảm/ }).click();
  await expect(
    page.getByRole("button", { name: /Kịch bản Suy giảm/ }),
  ).toHaveAttribute("aria-pressed", "true");
  expect(mutations[1].body).toEqual({
    expected_control_version: 2,
    scenario: "degradation",
  });
});

test("timeout remains unknown after GET and reload; retry keeps the same command", async ({
  page,
}) => {
  await live(page);
  const sent: { key: string; body: unknown }[] = [];
  await page.route(
    "**/api/v1/simulator/machines/M-001/pause",
    async (route) => {
      sent.push({
        key: route.request().headers()["idempotency-key"],
        body: route.request().postDataJSON(),
      });
      if (sent.length === 1) return route.abort("connectionfailed");
      return route.fulfill({
        json: {
          machine_id: "M-001",
          stream_state: "PAUSED",
          scenario: "normal",
          control_version: 2,
          updated_at: new Date().toISOString(),
          last_controlled_by: "fixture-operator",
          source: "SIMULATOR_CONTROL",
        },
      });
    },
  );
  await page.goto("/");
  await page.getByRole("button", { name: "Pause", exact: true }).click();
  await expect(
    page.getByText("Chưa rõ kết quả", { exact: true }),
  ).toBeVisible();
  await expect(page.locator(".control-state-row")).toContainText("RUNNING");
  await expect(
    page.getByRole("button", { name: "Pause", exact: true }),
  ).toBeDisabled();
  await page.reload();
  await expect(
    page.getByRole("button", { name: "Thử lại cùng mã lệnh" }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Đọc lại trạng thái", exact: true })
    .click();
  await expect(
    page.getByText("Chưa rõ kết quả", { exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Thử lại cùng mã lệnh" }).click();
  await expect(page.getByText("Đã xác nhận", { exact: true })).toBeVisible();
  expect(sent).toHaveLength(2);
  expect(sent[1]).toEqual(sent[0]);
});

test("409 reconciles changed version and waits for a fresh operator action", async ({
  page,
}) => {
  const { machines } = await live(page);
  let requests = 0;
  await page.route("**/api/v1/simulator/machines/M-001/pause", (route) => {
    requests++;
    machines[0].control_version = 9;
    machines[0].scenario = "critical";
    return route.fulfill({
      status: 409,
      json: {
        error: {
          code: "CONTROL_VERSION_CONFLICT",
          message: "Conflict",
          retryable: false,
        },
      },
    });
  });
  await page.goto("/");
  await page.getByRole("button", { name: "Pause", exact: true }).click();
  await expect(page.locator(".control-version")).toHaveText("v9");
  await expect(page.getByText("Xung đột", { exact: true })).toBeVisible();
  await expect(
    page.getByRole("button", { name: /Kịch bản Critical/ }),
  ).toHaveAttribute("aria-pressed", "true");
  expect(requests).toBe(1);
});

test("authorization failure never becomes success or an automatic retry", async ({
  page,
}) => {
  await live(page);
  let requests = 0;
  await page.route("**/api/v1/simulator/machines/M-001/pause", (route) => {
    requests++;
    return route.fulfill({
      status: 403,
      json: {
        error: { code: "FORBIDDEN", message: "Forbidden", retryable: false },
      },
    });
  });
  await page.goto("/");
  await page.getByRole("button", { name: "Pause", exact: true }).click();
  await expect(page.getByText("Bị từ chối", { exact: true })).toBeVisible();
  await expect(page.locator(".control-state-row")).toContainText("RUNNING");
  await expect(
    page.getByRole("button", { name: "Pause", exact: true }),
  ).toBeDisabled();
  expect(requests).toBe(1);
});

test("machine search and keyboard control work on mobile without page overflow", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");
  await expect(page.locator(".machine-card")).toHaveCount(5);
  await page.getByRole("textbox", { name: "Tìm máy" }).fill("M-004");
  await expect(page.locator(".machine-card")).toHaveCount(1);
  await page.locator(".machine-card").focus();
  await page.keyboard.press("Enter");
  await expect(page.locator(".control-state-row")).toContainText("PAUSED");
  await page.getByRole("button", { name: "Resume", exact: true }).focus();
  await page.keyboard.press("Enter");
  await expect(page.locator(".control-state-row")).toContainText("RUNNING");
  await page.getByRole("button", { name: "Cấu hình kết nối" }).click();
  await expect(page.getByRole("dialog")).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).not.toBeVisible();
  const overflow = await page.evaluate(
    () => document.documentElement.scrollWidth > innerWidth,
  );
  expect(overflow).toBe(false);
});

test("live OIDC waits for login and never falls back to a demo on API failure", async ({
  page,
}) => {
  let calls = 0;
  await page.route("**/runtime-config.json", (route) =>
    route.fulfill({
      json: {
        mode: "live",
        apiBaseUrl: "",
        pollIntervalMs: 5000,
        requestTimeoutMs: 1000,
        staleAfterMs: 20000,
        auth: {
          mode: "oidc",
          authority: "https://identity.example.com",
          clientId: "simulator-ui",
          scope: "openid profile simulator:control",
        },
        links: {},
      },
    }),
  );
  page.on("request", (request) => {
    if (request.url().includes("/api/v1/")) calls++;
  });
  await page.goto("/");
  await expect(
    page.getByRole("button", { name: "Đăng nhập", exact: true }),
  ).toBeVisible();
  await expect(page.locator(".machine-card")).toHaveCount(0);
  expect(calls).toBe(0);
  await expect(page.getByText("Bạn đang ở chế độ demo")).toHaveCount(0);
});

test("OIDC login uses code + S256 PKCE with a public client", async ({
  page,
}) => {
  await page.route("**/runtime-config.json", (route) =>
    route.fulfill({
      json: {
        mode: "live",
        apiBaseUrl: "",
        pollIntervalMs: 5000,
        requestTimeoutMs: 1000,
        staleAfterMs: 20000,
        auth: {
          mode: "oidc",
          authority: "https://identity.example.com",
          clientId: "simulator-ui",
          scope: "openid profile simulator:control",
        },
        links: {},
      },
    }),
  );
  await page.route(
    "https://identity.example.com/.well-known/openid-configuration",
    (route) =>
      route.fulfill({
        headers: { "Access-Control-Allow-Origin": "http://127.0.0.1:4174" },
        json: {
          issuer: "https://identity.example.com",
          authorization_endpoint: "https://identity.example.com/authorize",
          token_endpoint: "https://identity.example.com/token",
          jwks_uri: "https://identity.example.com/jwks",
          response_types_supported: ["code"],
          code_challenge_methods_supported: ["S256"],
        },
      }),
  );
  await page.route("https://identity.example.com/authorize**", (route) =>
    route.fulfill({
      contentType: "text/html",
      body: "<h1>Fixture identity provider</h1>",
    }),
  );
  await page.goto("/");
  await page.getByRole("button", { name: "Đăng nhập", exact: true }).click();
  await expect(page).toHaveURL(/identity\.example\.com\/authorize/);
  const params = new URL(page.url()).searchParams;
  expect(params.get("response_type")).toBe("code");
  expect(params.get("code_challenge_method")).toBe("S256");
  expect(params.get("code_challenge")?.length).toBeGreaterThan(30);
  expect(params.get("redirect_uri")).toBe(
    "http://127.0.0.1:4174/auth/callback",
  );
  expect(params.has("client_secret")).toBe(false);
  expect(params.has("access_token")).toBe(false);
});
