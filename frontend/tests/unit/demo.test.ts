import { expect, it, vi } from "vitest";
import { DemoApi } from "../../src/api/demo";
import type { LiveEvent } from "../../src/types/live";

it("shows initial no-prediction and delayed completion in explicit demo mode", async () => {
  vi.useFakeTimers();
  const api = new DemoApi();
  const controller = new AbortController();
  const events: LiveEvent[] = [];
  let stream: Promise<void> | undefined;
  try {
    const initial = await api.live("M-003", controller.signal);
    expect(initial.latest_valid_prediction).toBeNull();
    expect(initial.latest_processing?.processing_status).toBe("PENDING");
    stream = api.stream(
      "M-003",
      controller.signal,
      () => {},
      (event) => events.push(event),
    );
    expect(
      (await api.live("M-003", controller.signal)).latest_valid_prediction,
    ).toBeNull();
    await vi.advanceTimersByTimeAsync(1000);
    expect(events.some((event) => event.type === "prediction.completed")).toBe(
      true,
    );
  } finally {
    controller.abort();
    await stream;
    vi.useRealTimers();
  }
});
