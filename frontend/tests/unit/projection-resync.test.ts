import { expect, it, vi } from "vitest";
import { BusinessApi } from "../../src/api/client";
import { watchMachine } from "../../src/realtime/sse";
import type { LiveSnapshot } from "../../src/types/live";

function snapshot(sequence: number | null): LiveSnapshot {
  return {
    machine_id: "M-001",
    as_of: "2026-11-04T00:00:00Z",
    latest_processing: null,
    latest_valid_prediction: null,
    telemetry:
      sequence === null
        ? null
        : {
            event_id: `e-${sequence}`,
            stream_epoch: "fixture",
            sequence_number: BigInt(sequence),
            server_accepted_at: "2026-11-04T00:00:00Z",
            received_at: "2026-11-04T00:00:00Z",
            features_raw: {},
            validation_status: "UNVERIFIED",
          },
  };
}

it("periodic authoritative REST repairs a missed resync while SSE remains open", async () => {
  vi.useFakeTimers();
  let reads = 0;
  const states: LiveSnapshot[] = [];
  const api = {
    live: async () => snapshot(++reads === 1 ? 99 : null),
    stream: async (
      _machine: string,
      signal: AbortSignal,
      ready: () => void,
    ) => {
      ready();
      await new Promise<void>((resolve) =>
        signal.addEventListener("abort", () => resolve(), { once: true }),
      );
    },
  } as unknown as import("../../src/api/client").MachineApi;
  const stop = watchMachine(api, "M-001", {
    state: (value) => states.push(value),
    connection() {},
    error() {},
  });
  try {
    await vi.advanceTimersByTimeAsync(10);
    expect(states.at(-1)?.telemetry?.sequence_number).toBe(99n);
    await vi.advanceTimersByTimeAsync(30000);
    expect(states.at(-1)?.telemetry).toBeNull();
  } finally {
    stop();
    await vi.advanceTimersByTimeAsync(1);
    vi.useRealTimers();
  }
});

it("buffers live updates received during an authoritative REST refresh", async () => {
  vi.useFakeTimers();
  let reads = 0;
  let finish: (value: LiveSnapshot) => void;
  let liveEvent: (value: import("../../src/types/live").LiveEvent) => void;
  const states: LiveSnapshot[] = [];
  const api = {
    live: async () =>
      ++reads === 1
        ? snapshot(99)
        : new Promise<LiveSnapshot>((resolve) => {
            finish = resolve;
          }),
    stream: async (
      _machine: string,
      signal: AbortSignal,
      ready: () => void,
      event: typeof liveEvent,
    ) => {
      liveEvent = event;
      ready();
      await new Promise<void>((resolve) =>
        signal.addEventListener("abort", () => resolve(), { once: true }),
      );
    },
  } as unknown as import("../../src/api/client").MachineApi;
  const stop = watchMachine(api, "M-001", {
    state: (value) => states.push(value),
    connection() {},
    error() {},
  });
  try {
    await vi.advanceTimersByTimeAsync(30010);
    liveEvent!({
      schema_version: "1.0.0",
      type: "telemetry.received",
      machine_id: "M-001",
      occurred_at: "2026-11-04T00:00:00Z",
      data: snapshot(2).telemetry!,
    });
    finish!(snapshot(1));
    await vi.advanceTimersByTimeAsync(1);
    expect(states.at(-1)?.telemetry?.sequence_number).toBe(2n);
  } finally {
    stop();
    await vi.advanceTimersByTimeAsync(1);
    vi.useRealTimers();
  }
});

it("resync and a reconnect replace old generation cursors with a REST snapshot", async () => {
  vi.useFakeTimers();
  let reads = 0;
  let controller: ReadableStreamDefaultController<Uint8Array>;
  const states: LiveSnapshot[] = [];
  const api = new BusinessApi(
    "https://business.test/api/v1",
    async () => "token",
    async (input) => {
      if (String(input).includes("/events/stream"))
        return new Response(
          new ReadableStream({
            start(value) {
              controller = value;
            },
          }),
          { headers: { "Content-Type": "text/event-stream" } },
        );
      const sequence = ++reads === 1 ? 99 : 1;
      return new Response(
        JSON.stringify({
          machine_id: "M-001",
          as_of: "2026-11-04T00:00:00Z",
          telemetry: {
            event_id: `e-${sequence}`,
            stream_epoch: "fixture",
            sequence_number: sequence,
            server_accepted_at: "2026-11-04T00:00:00Z",
            received_at: "2026-11-04T00:00:00Z",
            features_raw: {},
            validation_status: "UNVERIFIED",
          },
          latest_processing: null,
          latest_valid_prediction: null,
        }),
        { headers: { "Content-Type": "application/json" } },
      );
    },
  );
  const stop = watchMachine(api, "M-001", {
    state: (value) => states.push(value),
    connection() {},
    error() {},
  });
  try {
    await vi.advanceTimersByTimeAsync(10);
    expect(states.at(-1)?.telemetry?.sequence_number).toBe(99n);
    const envelope = {
      schema_version: "1.0.0",
      type: "projection.resync",
      machine_id: null,
      occurred_at: "2026-11-04T00:00:00Z",
      data: { generation_id: "new", updated_at: "2026-11-04T00:00:00Z" },
    };
    controller!.enqueue(
      new TextEncoder().encode(
        `event: projection.resync\ndata: ${JSON.stringify(envelope)}\n\n`,
      ),
    );
    await vi.advanceTimersByTimeAsync(1500);
    expect(reads).toBe(2);
    expect(states.at(-1)?.telemetry?.sequence_number).toBe(1n);
    // A missed notification is recovered by the same reset on a later reconnect.
    controller!.close();
    await vi.advanceTimersByTimeAsync(1500);
    expect(reads).toBe(3);
    expect(states.at(-1)?.telemetry?.sequence_number).toBe(1n);
  } finally {
    stop();
    await vi.advanceTimersByTimeAsync(1);
    vi.useRealTimers();
  }
});
