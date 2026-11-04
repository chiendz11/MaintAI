import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import { parseCopilot, M7Client, BriefUnknown } from "../../src/api/m7-client";

it("validates all canonical Copilot business outcomes", () => {
  for (const name of [
    "grounded",
    "insufficient-evidence",
    "validation-failed",
    "pending",
    "not_requested",
    "unavailable",
  ]) {
    const raw = readFileSync(
      new URL(
        `../../../contracts/v1.3/examples/http/backend/copilot-${name}.json`,
        import.meta.url,
      ),
      "utf8",
    );
    expect(parseCopilot(raw).copilot_status).toBeTruthy();
  }
  expect(() =>
    parseCopilot('{"copilot_status":"AVAILABLE","brief":null}'),
  ).toThrow();
});
it("unknown POST preserves its idempotency key without an automatic retry", async () => {
  const calls: string[] = [];
  const client = new M7Client(async (_path, _signal, _body, headers) => {
    calls.push(headers?.["Idempotency-Key"] ?? "");
    throw new Error("lost response");
  });
  await expect(
    client.generate(
      "alert/slash",
      "MAINTENANCE_BRIEF",
      "same-key",
      new AbortController().signal,
    ),
  ).rejects.toBeInstanceOf(BriefUnknown);
  expect(calls).toEqual(["same-key"]);
});
it("accepts brief and global drift invalidations without treating them as telemetry", async () => {
  const { decodeEvents } = await import("../../src/api/codec");
  const data = {
    schema_version: "1.0.0",
    type: "drift.updated",
    machine_id: null,
    occurred_at: "2026-11-03T00:00:00Z",
    data: { report_id: "r", updated_at: "2026-11-03T00:00:00Z" },
  };
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(
        new TextEncoder().encode(
          `event: drift.updated\ndata: ${JSON.stringify(data)}\n\n`,
        ),
      );
      controller.close();
    },
  });
  const events = [];
  for await (const item of decodeEvents(body)) events.push(item);
  expect(events).toEqual([data]);
});
