import { expect, it } from "vitest";
import { BusinessApi } from "../../src/api/client";
import { DemoApi } from "../../src/api/demo";
import { decodeEvents } from "../../src/api/codec";
import { UnknownOutcome } from "../../src/realtime/alerts";

it("never retries action or feedback POST when the server outcome is unknown", async () => {
  let calls = 0;
  const request: typeof fetch = async () => {
    calls++;
    throw new TypeError("response lost");
  };
  const api = new BusinessApi("/api/v1", async () => "token", request);
  const signal = new AbortController().signal;
  await expect(
    api.action("a", "ack", { actor_id: "tech" }, signal),
  ).rejects.toBeInstanceOf(UnknownOutcome);
  expect(calls).toBe(1);
  try {
    await api.feedback(
      "a",
      {
        technician_verdict: "INCONCLUSIVE",
        recommendation_feedback: "NOT_AVAILABLE",
      },
      signal,
    );
  } catch (error) {
    expect(error).toBeInstanceOf(UnknownOutcome);
    expect((error as UnknownOutcome).feedback).toBe(true);
  }
  expect(calls).toBe(2);
});
it("decodes Alert SSE without applying source-sequence assumptions", async () => {
  const value = {
    schema_version: "1.0.0",
    type: "alert.updated",
    machine_id: "M-001",
    occurred_at: "2026-10-28T00:00:00Z",
    data: {
      alert_id: "a",
      machine_id: "M-001",
      severity: "HIGH",
      status: "ACKNOWLEDGED",
      updated_at: "2026-10-28T00:00:00Z",
    },
  };
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(
        new TextEncoder().encode(
          "event: alert.updated\ndata: " + JSON.stringify(value) + "\n\n",
        ),
      );
      controller.close();
    },
  });
  const events = [];
  for await (const event of decodeEvents(stream)) events.push(event);
  expect(events).toEqual([value]);
});
it("explicit demo supports lifecycle, terminal guards and feedback independently of live", async () => {
  const api = new DemoApi(),
    signal = new AbortController().signal;
  const page = await api.alerts({}, signal);
  const id = page.items[0].alert_id;
  await api.action(id, "ack", { actor_id: "demo-technician" }, signal);
  await expect(
    api.action(id, "ack", { actor_id: "demo-technician" }, signal),
  ).rejects.toMatchObject({ status: 409 });
  await api.action(
    id,
    "resolve",
    { actor_id: "demo-technician", resolution: "Fixed" },
    signal,
  );
  expect((await api.alert(id, signal)).alert.status).toBe("RESOLVED");
  expect(
    (
      await api.feedback(
        id,
        {
          technician_verdict: "CONFIRMED_ISSUE",
          recommendation_feedback: "USEFUL",
        },
        signal,
      )
    ).alert_id,
  ).toBe(id);
});
