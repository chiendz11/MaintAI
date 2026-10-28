import { expect, it, vi } from "vitest";
import { BusinessApi, ApiError } from "../../src/api/client";
import { watchMachine } from "../../src/realtime/sse";

const snapshot = JSON.stringify({
  machine_id: "cell/live",
  telemetry: null,
  latest_valid_prediction: null,
  latest_processing: null,
  as_of: "2026-10-28T00:00:00Z",
});

it("retries a temporarily saturated SSE admission after backoff", async () => {
  vi.useFakeTimers();
  let calls = 0;
  const api = new BusinessApi(
    "https://business.test/api/v1",
    async () => "token",
    async () => {
      calls++;
      return new Response(
        '{"error":{"code":"RATE_LIMITED","retryable":true}}',
        { status: 429 },
      );
    },
  );
  const stop = watchMachine(api, "M-001", {
    state: () => {},
    connection: () => {},
    error: () => {},
  });
  try {
    await vi.advanceTimersByTimeAsync(1200);
    expect(calls).toBe(2);
  } finally {
    stop();
    await vi.advanceTimersByTimeAsync(1);
    vi.useRealTimers();
  }
});

it("signals session loss on server401 without falling back to another identity", async () => {
  let expired = false;
  const api = new BusinessApi(
    "https://business.test/api/v1",
    async () => "token",
    async () =>
      new Response('{"error":{"code":"UNAUTHORIZED"}}', { status: 401 }),
    () => {
      expired = true;
    },
  );
  await expect(
    api.live("M-001", new AbortController().signal),
  ).rejects.toMatchObject({ status: 401 });
  expect(expired).toBe(true);
});

it("bounds a REST request that never returns headers", async () => {
  vi.useFakeTimers();
  const caller = new AbortController();
  let failure: unknown;
  let pending: Promise<unknown> | undefined;
  try {
    const api = new BusinessApi(
      "https://business.test/api/v1",
      async () => "token",
      async (_input, init) =>
        new Promise((_resolve, reject) => {
          init?.signal?.addEventListener(
            "abort",
            () => reject(new DOMException("Aborted", "AbortError")),
            { once: true },
          );
        }),
    );
    pending = api.live("M-001", caller.signal).catch((error) => {
      failure = error;
    });
    await vi.advanceTimersByTimeAsync(10001);
    expect(failure).toMatchObject({ status: 504 });
  } finally {
    caller.abort();
    await pending;
    vi.useRealTimers();
  }
});

it("opens authenticated stream before snapshot and aborts requests on cleanup", async () => {
  const calls: Array<{ url: string; token: string | null }> = [];
  let streamCancelled = false;
  const request: typeof fetch = async (input, init) => {
    const url = String(input);
    calls.push({ url, token: new Headers(init?.headers).get("Authorization") });
    if (url.includes("/events/stream"))
      return new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode(": connected\n\n"));
          },
          cancel() {
            streamCancelled = true;
          },
        }),
        { headers: { "Content-Type": "text/event-stream" } },
      );
    return new Response(snapshot, {
      headers: { "Content-Type": "application/json" },
    });
  };
  const api = new BusinessApi(
    "https://business.test/api/v1",
    async () => "own-token",
    request,
  );
  let synced = false;
  const stop = watchMachine(api, "cell/live", {
    state: () => {
      synced = true;
    },
    connection: () => {},
    error: () => {},
  });
  await new Promise((resolve) => setTimeout(resolve, 20));
  expect(calls[0].url).toContain("/events/stream?machine_id=cell%2Flive");
  expect(calls[1].url).toContain("/machines/cell%2Flive/live");
  expect(calls.every((call) => call.token === "Bearer own-token")).toBe(true);
  expect(synced).toBe(true);
  stop();
  await new Promise((resolve) => setTimeout(resolve, 10));
  expect(streamCancelled).toBe(true);
});

it("does not retry unauthorized streams or turn failure into fixture success", async () => {
  let calls = 0;
  const api = new BusinessApi(
    "https://business.test/api/v1",
    async () => "expired",
    async () => {
      calls++;
      return new Response(
        '{"error":{"code":"UNAUTHORIZED","retryable":false}}',
        { status: 401 },
      );
    },
  );
  let failure: unknown;
  const stop = watchMachine(api, "M-001", {
    state: () => {},
    connection: () => {},
    error: (error) => {
      failure = error;
    },
  });
  await new Promise((resolve) => setTimeout(resolve, 30));
  expect(calls).toBe(1);
  expect(failure).toBeInstanceOf(ApiError);
  stop();
});

it("rejects dot-only IDs before browser URL normalization can request another resource", async () => {
  let called = false;
  const api = new BusinessApi(
    "https://business.test/api/v1",
    async () => "token",
    async () => {
      called = true;
      return new Response(snapshot.replace("cell/live", ".."), {
        headers: { "Content-Type": "application/json" },
      });
    },
  );
  await expect(api.live("..", new AbortController().signal)).rejects.toThrow(
    /định danh/i,
  );
  expect(called).toBe(false);
});
